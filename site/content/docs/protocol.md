+++
title = "Wire protocol"
description = "The AWS Session Manager wire format: the 120-byte binary header, MessageId byte order, acknowledgements and retransmission, smux port forwarding, and KMS session encryption."
weight = 3
+++

## Establishing a session

`ssm:StartSession` returns three things: a session ID, a `wss://` stream URL for
the Amazon Message Gateway Service, and a token.

<figure class="diagram">
<svg class="seq" viewBox="0 0 718 446" width="718" height="446" role="img" aria-labelledby="t-Establ d-Establ" xmlns="http://www.w3.org/2000/svg">
<title id="t-Establ">Establishing an SSM session</title>
<desc id="d-Establ">The client calls StartSession on the SSM API, receives a session ID, stream URL and token, opens a WebSocket to the Message Gateway, authenticates with the token in the open message, and then completes a three-message handshake with the SSM agent.</desc>
<defs>
<marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
<path d="M0 0 L10 5 L0 10 z" fill="currentColor"/></marker>
</defs>
<g fill="none" stroke="currentColor" stroke-width="1.2">
<line x1="74" y1="38" x2="74" y2="402" stroke-dasharray="3 4" opacity=".35"/>
<line x1="264" y1="38" x2="264" y2="402" stroke-dasharray="3 4" opacity=".35"/>
<line x1="454" y1="38" x2="454" y2="402" stroke-dasharray="3 4" opacity=".35"/>
<line x1="644" y1="38" x2="644" y2="402" stroke-dasharray="3 4" opacity=".35"/>
</g>
<rect x="8" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="74" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">Client</text>
<rect x="198" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="264" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">SSM API</text>
<rect x="388" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="454" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">Message Gateway</text>
<rect x="578" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="644" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">SSM agent</text>
<line x1="77" y1="78" x2="259" y2="78" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="169" y="71" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">StartSession(target, document)</text>
<line x1="261" y1="118" x2="79" y2="118" stroke="currentColor" stroke-width="1.4" stroke-dasharray="5 4" marker-end="url(#ah)"/>
<text x="169" y="111" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">SessionId, StreamUrl, TokenValue</text>
<line x1="77" y1="158" x2="449" y2="158" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="264" y="151" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">WebSocket upgrade (StreamUrl)</text>
<line x1="77" y1="198" x2="449" y2="198" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="264" y="191" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">OpenDataChannelInput { TokenValue, ClientId, … }</text>
<line x1="451" y1="238" x2="79" y2="238" stroke="currentColor" stroke-width="1.4" stroke-dasharray="5 4" marker-end="url(#ah)"/>
<text x="264" y="231" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">start_publication</text>
<line x1="641" y1="278" x2="79" y2="278" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="359" y="271" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">HandshakeRequest</text>
<line x1="77" y1="318" x2="639" y2="318" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="359" y="311" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">HandshakeResponse</text>
<line x1="641" y1="358" x2="79" y2="358" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="359" y="351" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">HandshakeComplete</text>
<rect x="8" y="410" width="702" height="24" rx="5" fill="currentColor" opacity=".07"/>
<text x="359" y="426" text-anchor="middle" font-size="11.5" font-weight="600" fill="currentColor">session is ready</text>
</svg>
</figure>

The token goes in the **open message**, never in the URL. The reference plugin
does the same, and the reason matters: a URL is recorded by proxies, connection
traces and crash reports; a WebSocket payload is not.


## Message format

Every binary frame is a 120-byte header followed by a payload. All integers are
big-endian.

| Offset | Size | Field | Notes |
|---:|---:|:---|:---|
| 0 | 4 | `HeaderLength` | Always 116 — the header size *excluding* this field |
| 4 | 32 | `MessageType` | ASCII, space-padded |
| 36 | 4 | `SchemaVersion` | Always 1 |
| 40 | 8 | `CreatedDate` | Unix milliseconds |
| 48 | 8 | `SequenceNumber` | Per direction, starts at 0 |
| 56 | 8 | `Flags` | `SYN` = 1, `FIN` = 2 |
| 64 | 16 | `MessageId` | UUID — see below |
| 80 | 32 | `PayloadDigest` | SHA-256 of the payload |
| 112 | 4 | `PayloadType` | See the table below |
| 116 | 4 | `PayloadLength` | |
| 120 | … | `Payload` | |

### The MessageId trap

The SSM agent stores UUIDs the way Java does — as two `long`s — and writes the
**least**-significant half first. That is not RFC 4122 order.

```text
RFC 4122:  [ MSB 0..8 ][ LSB 8..16 ]
On the wire: [ LSB 8..16 ][ MSB 0..8 ]
```

Getting this wrong is quiet rather than loud: the agent cannot match your
acknowledgements to its messages, so it retransmits everything forever and the
session appears to hang. `binary_protocol.rs` pins the byte layout with a test
for exactly this reason.

### Message types

| Type | Direction | Meaning |
|:---|:---|:---|
| `input_stream_data` | client → agent | Keystrokes, control payloads |
| `output_stream_data` | agent → client | Output, handshake, exit codes |
| `acknowledge` | both | Confirms one sequence number |
| `channel_closed` | agent → client | The session is over; a JSON body's `Output` field says why |
| `start_publication` | gateway → client | The client may start sending |
| `pause_publication` | gateway → client | The client should stop sending |

### Payload types

| # | Name | Encrypted when KMS is on |
|---:|:---|:---:|
| 0 | `Undefined` | |
| 1 | `Output` | ✅ both directions |
| 2 | `Error` | |
| 3 | `Size` | |
| 4 | `Parameter` | |
| 5 | `HandshakeRequest` | |
| 6 | `HandshakeResponse` | |
| 7 | `HandshakeComplete` | |
| 8 | `EncChallengeRequest` | |
| 9 | `EncChallengeResponse` | |
| 10 | `Flag` | |
| 11 | `StdErr` | ✅ inbound |
| 12 | `ExitCode` | ✅ inbound |

Handshake and control payloads always travel in the clear: they carry the key
agreement itself.


## Integrity

`PayloadDigest` is a SHA-256 over the payload exactly as it appears on the wire —
after encryption, if session encryption is on.

This implementation **verifies it** and rejects a mismatch, matching
`ClientMessage.Validate()` in the reference plugin. A message whose digest does
not match is a message we cannot trust, and delivering it would put corrupt
bytes into a terminal or a forwarded TCP connection.

Two documented exemptions, both taken from the reference implementation:

- A zero-length payload has no digest to check.
- `start_publication` and `pause_publication` skip validation entirely; the
  gateway sends them with an empty payload and a zeroed digest.

The digest covers the payload and nothing else. A corrupted timestamp, sequence
number, flag or message ID is not detectable at this layer — the format has no
header checksum. Integrity and authenticity for the channel as a whole come from
TLS and the SigV4-authenticated session token, not from this field.


## Reliable delivery

The channel runs over TLS/TCP, so bytes are not lost in transit. The *agent*
can still drop a message when its buffers are full, and it signals successful
processing at the application layer rather than the transport layer. Both sides
therefore sequence and retransmit.

```text
client                                agent
  │── input_stream_data seq=0 ────────►│
  │◄─ acknowledge       seq=0 ─────────│   RTT sample
  │── input_stream_data seq=1 ────────►│
  │                (no ack)            │
  │   … RTO elapses …                  │
  │── input_stream_data seq=1 ────────►│   retransmit
  │◄─ acknowledge       seq=1 ─────────│   no RTT sample (Karn's algorithm)
```

**Inbound**, the client applies the agent's in-order contract:

| Received | Action |
|:---|:---|
| `seq == expected` | Deliver, acknowledge, then drain anything the gap was blocking |
| `seq > expected` | Buffer and acknowledge, so the agent stops resending it |
| `seq < expected` | Already processed — drop **without** acknowledging |

That last row is not an oversight. A second acknowledgement for a retired
sequence number confuses the agent's own buffer accounting.

**Outbound**, only the head of the queue is ever retransmitted: the agent
processes the stream strictly in order, so resending later messages while the
head is still missing cannot make progress.

An acknowledgement always carries `SequenceNumber = 0` and `Flags = SYN | FIN`
in its header; the sequence number being acknowledged lives in the JSON payload.

### One counter, one sender

There is a **single** outbound sequence counter, shared by everything the client
sends as `input_stream_data` — caller data, terminal-size updates, the handshake
response and the encryption challenge response alike. The reference plugin does
the same: all of them go through `SendInputDataMessage`, which increments
`StreamDataSequenceNumber`.

That matters because those messages originate in different tasks: handshake
traffic is produced while *reading* from the socket, caller data while draining
the send queue. Allocating a sequence number, recording the message for
retransmission and enqueuing it must therefore be one critical section. Split it
up and two messages get the same number while the next is never used — the agent
then waits forever for a message that will never arrive, and the session hangs
with no error reported anywhere. This crate holds a mutex across the whole
outbound path for that reason, and an integration test asserts that every
sequence number is issued exactly once.

### Timers

| Value | Setting | Reference plugin |
|:---|:---|:---|
| Retransmit scan | 100 ms | `ResendSleepInterval` |
| Initial RTO | 200 ms | `DefaultTransmissionTimeout` |
| RTO | Jacobson/Karels, clamped to 50 ms – 30 s | fixed |
| Give up after | 3000 attempts | `ResendMaxAttempt` |
| Buffer depth | 10 000 messages each way | `{In,Out}goingMessageBufferCapacity` |

The adaptive RTO is a deliberate divergence: the reference implementation uses a
fixed 200 ms, which retransmits constantly on a link whose round trip exceeds it.


## Chunking

Outbound data is split into `payload_chunk_size` messages, default 1024 bytes to
match `config.StreamDataPayloadSize`.

Measured against a live agent (3.3.3572.0), 8 KiB and 32 KiB chunks are accepted
and perform identically — latency is dominated by the round trip, not by
chunking. The conservative default is about interoperability with older agents,
not throughput. Raising it is reasonable for bulk port-forward traffic:

```rust
SessionBuilder::new("i-…").payload_chunk_size(16 * 1024)
```

Chunking is invisible to the peer: the far side sees one ordered byte stream, so
higher layers such as smux framing reassemble across chunk boundaries.


## Liveness

The reference plugin pings every five minutes and never checks for a reply, so a
silently dead connection can hang a session indefinitely.

This implementation pings every 30 seconds and judges liveness on **any** inbound
frame — data, pong, or control. A busy session is therefore never mistaken for a
dead one, and a genuinely dead one is detected within `idle_timeout` (120 s by
default) with `CloseReason::PeerUnresponsive`.

Note what this does and does not catch: it detects a dead network, a dropped NAT
entry, a suspended host. It does not detect an agent process that is wedged but
whose WebSocket library still answers pings — same limitation as TCP keepalive.


## Port forwarding and smux

Port-forwarding sessions carry many TCP connections over one WebSocket, framed
with [xtaci/smux](https://github.com/xtaci/smux) v1.

```text
 ┌────────┬────────┬──────────────┬──────────────────┐
 │ ver(1) │ cmd(1) │ length(2, LE)│ stream_id(4, LE) │
 ├────────┴────────┴──────────────┴──────────────────┤
 │                payload (length bytes)              │
 └────────────────────────────────────────────────────┘

 cmd: SYN=0  FIN=1  PSH=2  NOP=3
```

Client-initiated streams use odd IDs. Two consequences worth knowing:

**smux v1 has no flow control** — that arrived in v2. A consumer that stops
reading cannot backpressure the sender, so a stalled stream is evicted rather
than allowed to block every other stream on the session.

**There is no resynchronisation marker.** A bad version byte or an over-long
length field means the byte stream is not smux at this offset, and there is no
way to find where the next frame starts. Guessing would deliver corrupt bytes to
a real TCP connection, so the whole multiplexer fails instead.

The agent only speaks smux for documents whose session properties set
`type: LocalPortForwarding` — that is, `AWS-StartPortForwardingSession` and
`AWS-StartPortForwardingSessionToRemoteHost`. `AWS-StartSSHSession` looks like
port forwarding but is a plain byte stream carrying SSH's own protocol.

Keep-alive NOP frames are **off** by default, matching the reference plugin: SSM
enforces its own idle timeout, and synthetic keep-alives defeat it and leave
forgotten tunnels open indefinitely.


## Session encryption

When the account's Session Manager preferences enable "Encrypt session data",
the agent requests a `KMSEncryption` action during the handshake.

<figure class="diagram">
<svg class="seq" viewBox="0 0 528 446" width="528" height="446" role="img" aria-labelledby="t-KMS se d-KMS se" xmlns="http://www.w3.org/2000/svg">
<title id="t-KMS se">KMS session key agreement</title>
<desc id="d-KMS se">The agent requests KMS encryption. The client calls GenerateDataKey for 64 bytes, returns the ciphertext blob in its handshake response, and the agent decrypts the same blob to derive the matching key pair. An encryption challenge then proves both sides agree.</desc>
<defs>
<marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
<path d="M0 0 L10 5 L0 10 z" fill="currentColor"/></marker>
</defs>
<g fill="none" stroke="currentColor" stroke-width="1.2">
<line x1="74" y1="38" x2="74" y2="402" stroke-dasharray="3 4" opacity=".35"/>
<line x1="264" y1="38" x2="264" y2="402" stroke-dasharray="3 4" opacity=".35"/>
<line x1="454" y1="38" x2="454" y2="402" stroke-dasharray="3 4" opacity=".35"/>
</g>
<rect x="8" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="74" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">Client</text>
<rect x="198" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="264" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">AWS KMS</text>
<rect x="388" y="2" width="132" height="34" rx="6" fill="none" stroke="currentColor" stroke-width="1.2" opacity=".55"/>
<text x="454" y="23" text-anchor="middle" font-size="12.5" font-weight="600" fill="currentColor">SSM agent</text>
<line x1="451" y1="78" x2="79" y2="78" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="264" y="71" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">HandshakeRequest { KMSEncryption, KMSKeyId }</text>
<line x1="77" y1="118" x2="259" y2="118" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="169" y="111" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">GenerateDataKey(64 bytes, context)</text>
<line x1="261" y1="158" x2="79" y2="158" stroke="currentColor" stroke-width="1.4" stroke-dasharray="5 4" marker-end="url(#ah)"/>
<text x="169" y="151" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">plaintext key, ciphertext blob</text>
<line x1="77" y1="198" x2="449" y2="198" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="264" y="191" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">HandshakeResponse { KMSCipherTextKey }</text>
<line x1="451" y1="238" x2="269" y2="238" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="359" y="231" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">Decrypt(ciphertext blob)</text>
<line x1="267" y1="278" x2="449" y2="278" stroke="currentColor" stroke-width="1.4" stroke-dasharray="5 4" marker-end="url(#ah)"/>
<text x="359" y="271" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">same plaintext key</text>
<line x1="451" y1="318" x2="79" y2="318" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="264" y="311" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">EncChallengeRequest</text>
<line x1="77" y1="358" x2="449" y2="358" stroke="currentColor" stroke-width="1.4" marker-end="url(#ah)"/>
<text x="264" y="351" text-anchor="middle" font-size="11.5" fill="currentColor" opacity=".85">EncChallengeResponse</text>
<rect x="8" y="410" width="512" height="24" rx="5" fill="currentColor" opacity=".07"/>
<text x="264" y="426" text-anchor="middle" font-size="11.5" font-weight="600" fill="currentColor">both sides proved they derived the same key</text>
</svg>
</figure>

The 64-byte plaintext key is split in half. The client uses the **first** half to
decrypt and the **last** half to encrypt; the agent applies them the other way
round, giving each direction its own AES-256-GCM key. Each message carries a
fresh 12-byte random nonce, prepended to the ciphertext.

### The base64 trap

The agent and the reference plugin are Go, and Go's `encoding/json` renders a
`[]byte` as a **base64 string**. Rust's `serde` renders a `Vec<u8>` as an *array
of numbers*. Three handshake fields are declared `[]byte` on the Go side:

| Field | Message |
|:---|:---|
| `KMSCipherTextKey` | `HandshakeResponse`, `KMSEncryption` action result |
| `Challenge` | `EncChallengeRequest` |
| `Challenge` | `EncChallengeResponse` |

Encode them the natural serde way and the agent cannot parse the handshake
response, so encryption never comes up — and because the agent reports it as a
generic handshake failure, the cause is not obvious from either end. This crate
pins the base64 form with tests in `handshake.rs` for exactly that reason. It is
the same class of trap as the [MessageId byte order](#the-messageid-trap): a
silent, cross-language encoding mismatch that no type checker catches.

The encryption context is
`{"aws:ssm:SessionId": …, "aws:ssm:TargetId": …}` and must match on both sides,
which is why an attached session (`Session::attach`) cannot use encryption — it
never learns the target ID.

Both principals need KMS grants: `kms:GenerateDataKey` for the caller and
`kms:Decrypt` for the target's instance profile.

Built without the `kms` feature, the action is failed with an explicit message
rather than silently downgrading. An account that mandated encryption must never
end up with a plaintext session because a client was compiled without a flag.


## Divergences from the reference plugin

Everything here is a deliberate, tested choice, not an accident of porting.

| Area | Reference plugin | This crate | Why |
|:---|:---|:---|:---|
| Liveness | Ping every 5 min, no reply check | Ping every 30 s, idle deadline on any inbound frame | A silently dead connection otherwise hangs forever |
| Retransmit timeout | Fixed 200 ms | Jacobson/Karels adaptive | A fixed 200 ms retransmits constantly on slow links |
| Outgoing buffer full | Drop the oldest unacknowledged message | Refuse the new one and apply backpressure | Dropping the head abandons a message the agent is still waiting for and stalls the stream permanently |
| Chunk size | Fixed 1024 | Configurable, default 1024 | Bulk transfers benefit; the default stays interoperable |
| Terminal input | Platform-specific | Raw byte passthrough | Preserves modified keys, mouse reporting and application cursor mode |
