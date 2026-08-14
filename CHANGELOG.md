# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version stays below `1.0.0`, a minor bump may contain breaking changes.

## [0.5.0] — 2026-08-14

A substantial rework of the public API. Almost every type moved, was renamed, or
changed shape, so treat this as a rewrite rather than an upgrade: read the
[migration notes](#migrating-from-040) below before bumping.

The theme of the release is making session lifetime unambiguous. In 0.4.0 a
session exposed a `SessionState` you had to poll, and several failure paths left
a handle that looked alive with nothing behind it. Now a session is either
running or closed, every way it can end resolves `Session::closed()` and records
a `CloseReason`, and the layers above — the port forwarder, the pool,
reconnection — are built on that one signal.

### Added

- **KMS session encryption** (`kms` feature, on by default). AES-256-GCM
  end-to-end under a KMS-derived data key, for accounts whose Session Manager
  preferences require encrypted sessions. Built without the feature, a session
  that demands encryption fails the handshake with an actionable error instead of
  silently running in plaintext.
- **`CloseReason`** and `Session::closed()`, replacing polled state. Every close
  path records why: `Terminated`, `AgentClosed { exit_code, detail }`,
  `PeerUnresponsive`, `DeliveryFailed`, `Transport`, `Protocol`.
  `CloseReason::is_recoverable()` says whether a reconnect could plausibly help.
- **Dead-peer detection.** The connection now sends keep-alive pings and judges
  liveness on any inbound frame, closing with `PeerUnresponsive` once
  `idle_timeout` elapses. 0.4.0 had no idle deadline at all, so a black-holed
  connection — socket open, nothing coming back — hung indefinitely.
- **`NonInteractiveCommand` document**, for running a command with no pty and
  collecting its output; the session ends when the command exits and
  `Session::exit_code()` reports the status.
- **`SessionType` now lives in `documents`** and is derived from the document
  rather than set by hand, so `PortForwarder` can reject a session that was not
  started with a port-forwarding document instead of hanging on output it cannot
  parse as smux frames.
- **`EndpointPolicy`**, which refuses any data-channel URL that is not an AWS SSM
  messages endpoint. Accepts the regional, FIPS, China and PrivateLink hostname
  forms; `AllowAny` exists for pointing tests at a local mock.
- **`Session::attach`**, for opening a channel whose `StartSession` call happened
  elsewhere — a broker service holding the IAM permissions, or a test harness.
- **Adaptive retransmission.** RTO is now estimated with Jacobson/Karels
  (RFC 6298) and clamped to 50 ms–30 s, with Karn's algorithm excluding
  retransmitted messages from the estimate. The reference plugin uses a fixed
  200 ms, which retransmits constantly on any link slower than that.
- **`SessionConfig::payload_chunk_size`**, so bulk port-forward traffic can trade
  acknowledgement granularity for throughput. Defaults to the reference plugin's
  1024 bytes.
- **`extension-module` feature**, split out of `python` so
  `cargo test --features python` links against libpython instead of aborting.
  `maturin` enables it when building a wheel.
- **Python**: `PortForwarder` and `run_command`.
- **Documentation site** at
  [hupe1980.github.io/aws-ssm-bridge](https://hupe1980.github.io/aws-ssm-bridge/),
  built with Zola from `site/`.

### Changed

- **`Error` is now a flat enum.** 0.4.0 nested `SessionError`, `ProtocolError`
  and `TransportError` inside it, so matching a failure meant matching through
  two levels. There is now one variant per failure domain, plus
  `Error::is_retriable()` which classifies AWS service codes rather than
  retrying everything.
- **Output fan-out no longer drops data silently.** 0.4.0 used a `broadcast`
  channel, which discards the oldest messages when a receiver falls behind. For
  terminal output that is ugly; for the smux framing layer behind a port forward
  it is fatal, because a dropped chunk desynchronises the frame parser and every
  later byte is attributed to the wrong stream. Each subscriber now gets its own
  bounded queue, and one that overflows is evicted with `OutputStream::lagged()`
  set — an observable failure instead of silent corruption.
- **Outgoing buffer applies backpressure instead of evicting.** When the
  unacknowledged buffer is full, `send` waits. 0.4.0 followed the reference
  plugin in dropping the oldest entry, which abandons a message the agent is
  still waiting for and stalls the stream permanently.
- **Metrics**: `register_metrics()` → `metrics::register()`, and `get_metrics()`
  is no longer public. `ssm_message_retransmissions_total` →
  `ssm_retransmissions_total`. `ssm_active_sessions` and
  `ssm_session_errors_total` are gone; `ssm_sessions_ended_total`,
  `ssm_rtt_seconds` and `ssm_handshake_seconds` are new.
- **Minimum supported Rust version is 1.94.1**, up from a declared 1.81. This is
  set by the AWS SDK — `aws-config` and the `aws-sdk-*` crates declare 1.94.1,
  and `aws-sigv4` pulls in a dependency whose manifest needs edition2024 — not by
  this crate's own code. The 1.81 claim had not been accurate for some time.
- **Default features are now `["interactive", "kms"]`**, up from
  `["interactive"]`.
- **Python exceptions renamed** to a consistent hierarchy under `SsmError`:
  `AwsSsmBridgeError` → `SsmError`, `SsmSessionError` → `SsmClosedError`,
  `SsmAwsSdkError` → `SsmAwsError`. `SsmCryptoError` is new.
- **Examples renamed** to match their subject: `shell_session` → `shell`,
  `port_forwarding` → `port_forward`, `interactive_shell` → `interactive`,
  `session_pool`/`multiple_sessions` → `fleet`, `metrics_session` → `metrics`.

### Removed

- **`protocol` module.** Its contents moved to `binary_protocol` (wire types) and
  `documents` (`SessionType`).
- **`rate_limit` module** (`RateLimiter`, `RateLimitConfig`, `RateLimitResult`).
  Rate-limiting inbound protocol messages was actively harmful: dropping a
  stream-data message without acknowledging it makes the agent retransmit up to
  3000 times, so a brief burst became a retransmit storm and a dead session.
  Bounded buffers give the same protection without breaking the protocol.
- **`retry` module** (`RetryConfig`, `RetryStrategy`). The AWS SDK already
  retries API calls; `ReconnectConfig` covers session-level retry, and
  `Error::is_retriable()` covers the classification.
- **`tracing_ext` module.** The crate emits ordinary `tracing` spans and events;
  a bespoke layer on top of that was indirection without a payoff.
- **`SessionState`**, replaced by `CloseReason` plus `is_ready()` / `is_closed()`.
- **`Terminal`, `TerminalConfig`, `TerminalInput`.** Terminal handling is now
  `RawModeGuard` + `TerminalReader`, which forward stdin verbatim rather than
  decoding and re-encoding key events — the old design could not round-trip
  mode-dependent sequences, so arrow keys broke inside `vim`.
- **`ShutdownGuard`** and **`ReconnectStats`**.
- **`SessionBuilder::document_name`, `parameter` and `session_type`**, and
  **`PortForwardingSessionBuilder`**. A document and its parameters could
  previously be set independently and disagree; a typed document carries both.
- **Python**: `SessionConfig`, `SessionType`, `InteractiveConfig` and
  `run_shell`. Session options are keyword arguments on `start_session`, and
  `run_command` replaces `run_shell`.

### Fixed

Both entries below are silent data loss: 0.4.0 discarded bytes without telling
anyone. If you ran port forwarding on 0.4.0 under load, this is the reason to
upgrade.

- **A slow output consumer lost bytes with no indication.** The `broadcast`
  fan-out dropped the oldest messages once a receiver fell behind. Behind a port
  forward that desynchronises the smux frame parser, so subsequent bytes are
  handed to the wrong TCP connection — corruption indistinguishable from the
  remote having sent something else. See the fan-out entry under *Changed*.
- **A full outgoing buffer abandoned an unacknowledged message.** Evicting the
  oldest entry drops a message the agent is still waiting for, stalling that
  direction of the stream permanently. `send` now waits instead.

### Known limitations

- KMS session encryption is new in this release and has been verified against the
  reference implementation's wire format and in tests, but not yet against a live
  agent with "Encrypt session data" enabled. Reports welcome.
- A wedged agent whose WebSocket library still answers pings is not detected —
  the same limitation as TCP keepalive.

### Migrating from 0.4.0

| 0.4.0 | 0.5.0 |
|:---|:---|
| `session.state()` | `session.is_ready()`, `session.is_closed()`, `session.close_reason()` |
| Poll for termination | `session.closed().await` |
| `Error::Session(SessionError::…)` | `Error::SessionClosed(…)` and siblings |
| `register_metrics(r)` | `metrics::register(r)` |
| `protocol::SessionType` | `documents::SessionType` |
| `protocol::MessageType` | `binary_protocol::MessageType` |
| `RetryStrategy`, `RateLimiter` | removed — see above |
| `TerminalInput` | `TerminalReader` |
| `builder.build_config()` | `builder.into_config()` |
| Python `run_shell(...)` | `run_command(...)` |
| Python `AwsSsmBridgeError` | `SsmError` |

`SessionBuilder` loses the escape hatches that let a document and its parameters
disagree. `document_name`, `parameter` and `session_type` are gone; pass a typed
document instead, which is where the session type now comes from:

```rust
// 0.4.0
SessionBuilder::new(target)
    .document_name("AWS-StartPortForwardingSession")
    .parameter("portNumber", vec!["5432".into()])
    .session_type(SessionType::Port)

// 0.5.0
SessionBuilder::new(target).document(PortForwardingSession::new(5432))
```

## [0.4.0] and earlier

Released before this changelog was kept. See the
[commit history](https://github.com/hupe1980/aws-ssm-bridge/commits/main) and the
[release tags](https://github.com/hupe1980/aws-ssm-bridge/releases).

[0.5.0]: https://github.com/hupe1980/aws-ssm-bridge/releases/tag/v0.5.0
