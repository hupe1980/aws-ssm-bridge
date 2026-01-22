---
layout: default
title: Architecture
nav_order: 5
---

# Architecture
{: .no_toc }

Internal design and implementation details.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

`aws-ssm-bridge` is a flat-module Rust library implementing the AWS SSM Session Manager protocol.

```
┌─────────────────────────────────────────────────────────────┐
│           Python Bindings (PyO3 + pyo3-asyncio)            │
├─────────────────────────────────────────────────────────────┤
│  Public API: SessionManager, Session, SessionBuilder       │
├─────────────────────────────────────────────────────────────┤
│  Connection: WebSocket, Routing, Retransmit, Metrics       │
├─────────────────────────────────────────────────────────────┤
│  Protocol: Binary (116B header), JSON, Handshake, ACK      │
├─────────────────────────────────────────────────────────────┤
│  AWS SDK: SSM Client, Credentials, StartSession API        │
└─────────────────────────────────────────────────────────────┘
```

## Module Structure

```
src/
├── lib.rs              # Public API re-exports
├── binary_protocol.rs  # 116-byte binary header, SHA-256 digest
├── protocol.rs         # Message types, JSON payloads, SessionType
├── session.rs          # Session lifecycle, state machine
├── connection.rs       # WebSocket, message routing, retransmit scheduler
├── ack.rs              # ACK tracking, RTT (Jacobson/Karels), buffers
├── handshake.rs        # 3-phase handshake protocol
├── builder.rs          # Fluent SessionBuilder API
├── port_forward.rs     # TCP tunneling
├── documents.rs        # Type-safe SSM document definitions
├── rate_limit.rs       # Token bucket rate limiting
├── metrics.rs          # Pluggable observability hooks
├── shutdown.rs         # Graceful shutdown coordination
├── pool.rs             # Session pool for concurrent sessions
├── reconnect.rs        # Auto-reconnection with exponential backoff
├── tracing_ext.rs      # Structured tracing with spans
├── interactive.rs      # Terminal raw mode, shell
├── terminal.rs         # Cross-platform terminal handling
├── channels.rs         # Broadcast multiplexer (fan-out)
├── retry.rs            # Exponential backoff, circuit breaker
├── errors.rs           # Error types
├── aws_client.rs       # AWS SDK wrapper
└── python/             # PyO3 bindings
    ├── mod.rs          # Module exports
    └── session.rs      # Python wrappers
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Flat modules** | Industry standard (tokio, serde), simpler imports |
| **Broadcast channels** | Multiple output consumers, proper fan-out |
| **`tokio-tungstenite`** | AWS SDK uses Tokio, mature WebSocket impl |
| **`bytes::Bytes`** | Zero-copy, reference-counted buffers |
| **`aws-lc-rs`** | AWS's libcrypto, hardware-accelerated SHA-256 |
| **`tokio::sync::Notify`** | Efficient shutdown signaling |
| **`forbid(unsafe_code)`** | Memory safety guarantee |

## Data Flow

### Session Creation

```
Application
    │
    ▼
SessionManager::new().await
    │  (loads AWS credentials)
    ▼
SessionConfig
    │
    ▼
AWS SSM StartSession API
    │  (returns: session_id, stream_url, token)
    ▼
Session::new()
    │
    ├─▶ ConnectionManager::connect()
    │       │
    │       ├─▶ WebSocket to wss://ssmmessages.{region}.amazonaws.com
    │       │
    │       ├─▶ Handshake (open_data_channel → request → response → complete)
    │       │
    │       └─▶ Spawn: message_loop, retransmit_scheduler
    │
    └─▶ Return Session handle
```

### Message Flow (Outgoing)

```
session.send(data)
    │
    ▼
ManagerCommand::SendData
    │  (via mpsc channel)
    ▼
ConnectionManager
    │
    ├─▶ Create StreamingMessage (seq, payload)
    │
    ├─▶ OutgoingMessageBuffer::add()
    │       (track for retransmit)
    │
    ├─▶ Serialize: binary header (116B) + payload
    │
    ├─▶ Metrics: counter(messages_sent), counter(bytes_sent)
    │
    └─▶ WebSocket::send(Binary)
```

### Message Flow (Incoming)

```
WebSocket::recv()
    │
    ▼
StreamingMessage::deserialize()
    │
    ├─▶ Metrics: counter(messages_received), counter(bytes_received)
    │
    ▼
Sequence Number Check
    │
    ├─▶ seq < expected: DUPLICATE → silently drop (no ACK)
    │
    ├─▶ seq == expected: IN-ORDER → ACK, process, drain buffered
    │       │
    │       └─▶ ChannelMultiplexer::broadcast(payload)
    │               │
    │               └─▶ Application receives via session.output()
    │
    └─▶ seq > expected: OUT-OF-ORDER → buffer, ACK (non-sequential)
```

### Retransmission Flow

```
tokio::interval(200ms)
    │
    ▼
OutgoingMessageBuffer::get_retransmit_candidates()
    │
    ├─▶ For each unacked message past RTO:
    │       │
    │       ├─▶ Retransmit counter++
    │       │
    │       ├─▶ If counter > 3000 (5 min): signal termination
    │       │
    │       ├─▶ Else: WebSocket::send(Binary)
    │       │
    │       └─▶ Metrics: counter(retransmissions)
    │
    └─▶ (continues)
```

## Concurrency Model

- **Fully async**: Built on Tokio runtime
- **Message passing**: `mpsc` channels (unbounded for commands, bounded for data)
- **Shared state**: `Arc<RwLock<T>>` / `Arc<Mutex<T>>` where necessary
- **Task-based**: Each session spawns:
  - Message processing loop
  - Retransmit scheduler
  - (Optional) Output stream consumers

### Session Tasks

```
Session
    │
    ├─▶ ConnectionManager::run()
    │       │
    │       ├─▶ WebSocket recv loop
    │       │
    │       ├─▶ Command handler (send, terminate)
    │       │
    │       └─▶ Retransmit scheduler (200ms interval)
    │
    └─▶ ChannelMultiplexer
            │
            └─▶ Broadcast channels for output
```

## Error Handling

### Error Types

```rust
pub enum Error {
    Aws(AwsError),
    WebSocket(WebSocketError),
    Protocol(ProtocolError),
    Session(SessionError),
    Transport(TransportError),
    Config(String),
    Timeout,
    Cancelled,
    InvalidState(String),
    Io(std::io::Error),
}
```

### Error Propagation

- **Rust**: `Result<T, Error>` consistently
- **Python**: Converted to `PyRuntimeError`, `PyValueError`, etc.
- **Retryable vs Fatal**: Handled by retry.rs circuit breaker

## Security Architecture

| Component | Protection |
|-----------|------------|
| **Credentials** | Redacted `Debug` impl, skip in tracing |
| **URLs** | SSRF protection, AWS endpoint validation |
| **Input** | Bounds checking, max sizes enforced |
| **Memory** | `forbid(unsafe_code)` |
| **Transport** | TLS required (WSS only) |

## Performance Characteristics

| Metric | Value |
|--------|-------|
| **Serialization** | 460+ MiB/s (16KB payloads) |
| **Deserialization** | 500+ MiB/s (16KB payloads) |
| **Base64** | 3.97 GiB/s (SIMD-accelerated) |
| **SHA-256** | 205-333 MiB/s (aws-lc-rs) |
| **Memory per session** | ~1-2 MB |
| **CPU** | Minimal, mostly IO-bound |

## Extension Points

1. **MetricsRecorder**: Implement for Prometheus/OTel/StatsD
2. **SessionPool**: Custom pool policies
3. **ReconnectConfig**: Custom backoff strategies

## Python Bindings Architecture

```
Python asyncio
    │
    ▼
pyo3-asyncio::future_into_py()
    │
    ▼
Tokio runtime (shared)
    │
    ▼
Rust async functions
    │
    ▼
Result<T, Error> → PyResult<T>
```

### Python API

| Class | Methods |
|-------|---------|
| `SessionManager` | `new()`, `start_session()`, `terminate_session()` |
| `Session` | `send()`, `output()`, `terminate()`, `wait_for_ready()`, `is_ready()` |
| `SessionConfig` | Constructor with all options |
| `OutputStream` | Async iterator (`async for chunk in stream`) |

### Context Manager Support

```python
async with await manager.start_session("i-xxx") as session:
    await session.send(b"ls\n")
# Automatically terminated
```
