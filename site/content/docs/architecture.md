+++
title = "Architecture"
description = "How aws-ssm-bridge is layered: the data channel's five tasks, session lifetime, queue policies, concurrency choices and the testing approach."
weight = 2
+++

## Layers

```text
  ┌───────────────────────────────────────────────────────────┐
  │  InteractiveShell   PortForwarder   SessionPool           │  application
  │                     ReconnectingSession                   │
  ├───────────────────────────────────────────────────────────┤
  │  Session · SessionManager · SessionBuilder · documents    │  API
  ├───────────────────────────────────────────────────────────┤
  │  connection  ── the data channel and its five tasks       │  transport
  │  channels · ack · crypto · handshake                      │
  ├───────────────────────────────────────────────────────────┤
  │  binary_protocol  ── the 120-byte wire format             │  framing
  └───────────────────────────────────────────────────────────┘

  mux (smux v1) sits beside the API layer: it is a framing protocol
  that runs *inside* a session's byte stream, not underneath it.
```

Each module owns one concern and depends only downwards.

| Module | Responsibility |
|:---|:---|
| `binary_protocol` | Encode and decode messages; verify digests |
| `ack` | Sequencing, retransmission, RTT estimation, reordering |
| `crypto` | KMS data-key negotiation and AES-256-GCM |
| `handshake` | The three-message agent handshake |
| `channels` | Fan-out of output to subscribers |
| `connection` | WebSocket transport and message routing |
| `session` | Lifecycle, configuration, the AWS API |
| `mux` | smux v1 framing for port forwarding |


## The data channel

`connection::connect` splits the WebSocket and spawns five tasks:

```text
                   ┌──────────────┐
  Session::send ──►│ command task │──┐
                   └──────────────┘  │
                   ┌──────────────┐  ├──► writer task ──► WebSocket sink
  retransmit ─────►│  scheduler   │──┤
                   └──────────────┘  │
                   ┌──────────────┐  │
  heartbeat ──────►│    pinger    │──┘
                   └──────────────┘

  WebSocket stream ──► reader task ──┬──► output fan-out (consumers)
                                     ├──► acknowledgements (writer task)
                                     └──► handshake / control handling
```

Two rules make this tractable:

**Exactly one task owns the sink.** Everything else enqueues on a bounded
channel. That gives backpressure without a mutex on the hot path, and it means
the reader never waits on the writer — the deadlock that a single shared socket
invites.

**Exactly one task at a time numbers a message.** *Two* tasks produce
`input_stream_data`: the command task streaming caller data, and the reader task
answering the handshake and the encryption challenge. They share one sequence
counter, so allocating a number, recording the message for retransmission and
enqueuing it is a single critical section behind an async mutex. See
[one counter, one sender](@/docs/protocol.md#one-counter-one-sender) for what breaks
otherwise.

**Every task exits on close, and every failure path closes.** A dead peer, a
protocol violation, a socket error: all of them call `SessionCore::close` and
wake everyone. There is no state in which the session looks alive but nothing is
running.

### Queue policies

The right behaviour when a queue fills differs per producer, and each choice is
deliberate:

| Producer | Full-queue policy | Why |
|:---|:---|:---|
| Caller data | Wait | Backpressure is correct; dropping user input is not |
| Acknowledgements | Drop with a warning | The agent retransmits; blocking the reader would stall everything |
| Keep-alive pings | Skip | A saturated writer is itself proof of life |
| Retransmissions | Wait, racing shutdown | The timer was already reset, so the send must happen |


## Session lifetime

`SessionCore` is the single shared truth, held by the `Session` handle and by
every task driving its channel.

```text
              ┌──────────────────────────────┐
              │        SessionCore           │
              │                              │
   Session ──►│  sendable   (+ notify)       │◄── reader task
   handle     │  closed     (+ notify)       │◄── writer task
              │  close_reason                │◄── heartbeat
              │  output fan-out              │◄── retransmit
              │  crypto, agent version       │
              └──────────────────────────────┘
```

`close()` records the **first** reason and latches. Later closes — including the
`terminate()` that follows a transport failure — do not overwrite it, so the
original cause survives to be reported.

Everything that could park is released on close: waiters on readiness, on buffer
space, on the output stream. A caller can never be left waiting for a signal that
will never come.

### The `Notify` ordering rule

`tokio::sync::Notify::notify_waiters` only wakes futures that are *already*
registered. Every wait in this crate therefore registers before checking the
flag:

```rust
let notified = self.closed_notify.notified();   // register first
if self.is_closed() {                           // then check
    return;
}
notified.await;
```

The other order loses any close that lands between the check and the await, and
the task waits forever. This pattern appears in `SessionCore`, `ShutdownSignal`
and `mux`; it is the single most common way to get this wrong.


## Output fan-out

Several readers may want the same byte stream: a terminal, a log tap, the smux
demultiplexer. Each gets its own bounded queue.

An earlier design used a `broadcast` channel, which silently drops the oldest
messages when a receiver falls behind. For terminal output that is ugly; for
smux framing it is fatal, because a dropped chunk desynchronises the frame parser
and every subsequent byte is garbage attributed to the wrong stream.

So a subscriber that fills its queue is **evicted**, and its stream reports
`lagged()`. An explicit, observable failure beats silent corruption. The session
and every other subscriber continue unaffected.


## Port forwarding

```text
  psql :15432 ──► TcpListener ──► smux stream 1 ─┐
  psql :15432 ──► TcpListener ──► smux stream 3 ─┼─► one session ──► agent ──► db:5432
  psql :15432 ──► TcpListener ──► smux stream 5 ─┘
```

Each accepted connection gets its own smux stream, so concurrent connections
neither block nor corrupt each other. `SmuxStream` implements `AsyncRead` and
`AsyncWrite`, so it plugs straight into `tokio::io::copy`.

### Half-close

Each direction is copied to EOF and then **half-closed**, rather than tearing the
whole connection down as soon as either direction ends.

This matters more than it sounds. Many protocols — HTTP without keep-alive, `nc`,
anything shell-piped — signal "request finished" by closing one direction and
then reading the reply. Collapsing both directions at once truncates that reply.


## Reconnection

```text
   ReconnectingSession
        │
        ├── output fan-out ──────────────► your stream (survives reconnects)
        │        ▲
        │        │ pump
        │   ┌────┴─────┐   closed()   ┌──────────┐
        └──►│ Session  │─────────────►│supervisor│──► new Session
            └──────────┘              └──────────┘
```

The supervisor waits on `closed()`, checks `CloseReason::is_recoverable()`, and
rebuilds with full-jitter exponential backoff. Your `OutputStream` is attached to
the wrapper's fan-out, not to any individual session, so it spans the gap.

Only recoverable failures trigger a rebuild. Reconnecting after a clean
`AgentClosed` would resurrect a session an operator deliberately ended.


## Concurrency choices

**`std::sync::Mutex` for short critical sections, `tokio::sync::Mutex` only
where one is genuinely held across an await.** The buffers in `ack` and the
subscriber list in `channels` hold no awaits, so an async mutex would add
allocation and scheduling for nothing. The outbound sequence counter is the one
place that must span an await — the enqueue is part of the critical section —
and it uses the async mutex accordingly. The `await_holding_lock` clippy lint is
denied to keep the distinction honest.

**Poisoning is always recovered.** `lock().unwrap_or_else(|e| e.into_inner())`.
A panic in an unrelated task should not cascade into every other task through a
poisoned mutex.

**`OnceLock` for write-once state.** The negotiated cipher and agent version are
set exactly once during the handshake and read on every message.

**`Notify` waiters are enqueued before the flag they guard is checked.**
Constructing a `Notified` future does *not* enqueue it — the first poll does, or
`enable()` up front. Every "register, then check the latch, then await" site
therefore calls `enable()` first. Without it, a `close()` or `shutdown()` landing
between the check and the first poll is delivered to nobody and the task parks
forever: a rare hang with no error and nothing in the logs.


## Testing

| Layer | Approach |
|:---|:---|
| Framing | Unit tests pinning byte offsets, plus proptests for round-trip and corruption detection |
| Reliability | Unit tests that manipulate timers directly rather than sleeping |
| End to end | A mock Message Gateway speaking the real binary protocol |
| Parsers | `cargo-fuzz` over everything that touches network bytes |

The mock gateway is the interesting one. It can misbehave on demand — close
after the handshake, go silent, reorder messages, refuse to acknowledge, close
with an explanatory `Output` body, send a base64 encryption challenge, or skip
the handshake like a pre-2.3 agent — so the failure paths are exercised rather
than assumed:

```rust
let gateway = MockGateway::start(Behaviour::GoSilent).await;
let session = Session::attach("s", gateway.url(), "t", config()).await?;
session.wait_ready().await?;

session.closed().await;   // the idle timeout must fire
assert!(matches!(session.close_reason(), Some(CloseReason::PeerUnresponsive { .. })));
```

`Session::attach` exists for this — and for the real use case it also serves:
opening a channel whose `StartSession` call happened somewhere else, such as a
broker service that holds the IAM permissions.
