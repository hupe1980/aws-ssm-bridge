+++
title = "Security"
description = "The aws-ssm-bridge threat model: credential handling, endpoint validation, message integrity, session encryption, resource limits, and what is explicitly not defended against."
weight = 4
+++

## Where the trust actually comes from

Session Manager's security properties are AWS's, not this library's:

- **Authentication** is SigV4 on `ssm:StartSession`, plus the short-lived token
  the API returns.
- **Authorisation** is IAM, including session-document conditions and the
  target's own instance profile.
- **Confidentiality in transit** is TLS to the Message Gateway.
- **Audit** is CloudTrail, plus optional S3/CloudWatch session logging.

This crate's job is to not undermine any of that. The rest of this page is about
the places where a client can, and what is done about them.


## Credential handling

The `StartSession` token is the one secret this crate holds. It is a bearer
credential for the data channel until the session ends.

| Protection | Detail |
|:---|:---|
| Never in a URL | The token goes in the data-channel open message, matching the reference plugin. A URL is recorded by proxies, connection traces and crash reports; a WebSocket payload is not |
| Scrubbed from memory | Held in `Zeroizing` buffers from the API response through to the open message |
| Never logged | Stream URLs are sanitised before they reach a log line, case-insensitively |
| Not duplicated | The open-message struct has no `Clone` and no derived `Debug` |

Key material for session encryption gets the same treatment: `SessionCrypto`
implements neither `Clone` nor a derived `Debug`, so a stray `dbg!` cannot print
session keys into a log aggregator.


## Endpoint validation

The data channel refuses to connect anywhere that is not an AWS SSM messages
endpoint:

```text
wss://ssmmessages.<region>.amazonaws.com/…
wss://ssmmessages-fips.<region>.amazonaws.com/…
wss://ssmmessages.<region>.amazonaws.com.cn/…
wss://vpce-….ssmmessages.<region>.vpce.amazonaws.com/…     ← PrivateLink
```

The check requires a whole DNS label to be the service name, so
`evil-ssmmessages.amazonaws.com` and `ssmmessages.attacker.com` are both
rejected, while the PrivateLink form — where the service name sits in a middle
label — still works.

This is defence in depth, not a primary control: the URL comes from an
SDK-authenticated API response. It exists so that a tampered or replayed
`StartSession` response cannot redirect the session token to a host of the
attacker's choosing.

`EndpointPolicy::AllowAny` disables it. That exists for pointing tests at a local
mock gateway; using it on untrusted input removes the guard entirely.


## Message integrity

`PayloadDigest` is a SHA-256 over the payload as it appears on the wire. This
implementation **verifies it and rejects mismatches**, matching
`ClientMessage.Validate()` in the reference plugin.

A message whose digest does not match is a message that cannot be trusted.
Delivering it anyway would put corrupt bytes into a terminal or a forwarded TCP
connection, where they are indistinguishable from something the remote actually
sent.

> [!WARNING]
> The digest covers **the payload only**. A corrupted timestamp, sequence number,
> flag or message ID is not detectable at this layer — the wire format has no
> header checksum. This is a data-integrity check against corruption, not an
> authenticity mechanism; authenticity comes from TLS and the session token.

Two exemptions, both from the reference implementation: a zero-length payload has
no digest to check, and `start_publication` / `pause_publication` skip validation
entirely.


## Session encryption

When the account's Session Manager preferences require it, payloads are
encrypted end-to-end with AES-256-GCM under a KMS-derived data key, so the
Message Gateway carries only ciphertext. See
[the protocol page](@/docs/protocol.md#session-encryption) for the key agreement.

The important property is what happens when it *cannot* be done:

> [!IMPORTANT]
> A client that cannot negotiate encryption **fails the handshake**. It never
> silently downgrades to plaintext. An account that mandated encrypted sessions
> must not end up with an unencrypted one because a client was built without a
> feature flag.

This holds in all three cases: the `kms` feature is off, no KMS client is
available (an attached session), or the KMS call itself fails. The error names
the missing permission.


## Resource limits

A malfunctioning or hostile peer should not be able to exhaust the client.

| Limit | Value | Purpose |
|:---|:---|:---|
| Max message payload | 10 MiB | A declared length is checked *before* allocating |
| Reorder buffer | 10 000 messages | Bounded; a full buffer stops acknowledging so the agent backs off |
| Unacknowledged buffer | 10 000 messages | Bounded; a full buffer applies backpressure to the caller |
| Output subscriber queue | 8192 messages | A subscriber that overflows is evicted, not the session |
| Forwarded connections | 100, configurable | Excess connections are refused rather than queued |
| Idle deadline | 120 s | A silent connection is closed rather than held open |

There is deliberately **no inbound message rate limiter**. An earlier version had
one, and it was actively harmful: dropping an inbound stream-data message without
acknowledging it makes the agent retransmit it up to 3000 times, so a brief burst
turned into a retransmit storm and a dead session. Bounded buffers give the same
protection without breaking the protocol.


## Memory safety

`unsafe_code = "forbid"` in `[lints.rust]`, so any `unsafe` block fails the build
rather than merely warning. There is none anywhere, including in the Python
bindings.

The parsers that touch network bytes are fuzzed:

```sh
cargo fuzz run fuzz_binary_protocol   # the 120-byte header parser
cargo fuzz run fuzz_handshake         # agent handshake JSON
cargo fuzz run fuzz_acknowledge       # acknowledgement payloads
```

The bar is: arbitrary bytes in, `Result` out, never a panic. A panic in a network
parser is a remotely triggerable denial of service.


## What this does not protect against

Stated plainly, because a security page that only lists wins is not useful.

- **A compromised target.** If the instance is owned, the session gives it a
  channel to you. Session encryption protects the transport, not the peer.
- **Over-broad IAM.** `ssm:StartSession` on `*` is a remote-shell grant to your
  whole fleet. Scope it, and use session-document conditions.
- **A malicious local process.** Anything running as your user can read the
  token from memory or hijack the terminal.
- **Header tampering by a TLS-terminating middlebox.** Only the payload is
  digest-covered; the transport is trusted for the rest.
- **A wedged agent that still answers pings.** Liveness detection catches a dead
  network, not a hung process whose WebSocket library is still responsive — the
  same limitation as TCP keepalive.
- **Traffic analysis.** Sizes and timings of encrypted payloads are visible to
  the gateway.


## Deployment checklist

- [ ] Scope `ssm:StartSession` to specific targets and documents, not `*`.
- [ ] Enable session logging to S3 or CloudWatch for an audit trail.
- [ ] Turn on "Encrypt session data" in Session Manager preferences, and grant
      `kms:GenerateDataKey` to callers and `kms:Decrypt` to instance profiles.
- [ ] Pass a `reason` when starting sessions — it lands in CloudTrail.
- [ ] Use VPC endpoints so session traffic never leaves your VPC.
- [ ] Bind port forwarders to `127.0.0.1`, never `0.0.0.0`, unless you mean to
      share the tunnel with your whole network.
- [ ] Keep dependencies current: `cargo audit`, `cargo deny check`.
- [ ] Alert on `ssm_sessions_ended_total` climbing without matching starts.


## Reporting a vulnerability

Open a [security advisory](https://github.com/hupe1980/aws-ssm-bridge/security/advisories/new)
rather than a public issue.
