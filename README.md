# aws-ssm-bridge

A Rust library implementing the AWS Systems Manager (SSM) Session Manager protocol with Python bindings.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.81%2B-orange)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org)
[![unsafe](https://img.shields.io/badge/unsafe-forbidden-red)](Cargo.toml)

---

## ⚠️ Disclaimer

**This project is not affiliated with, endorsed by, or sponsored by Amazon Web Services, Inc. or any of its affiliates.**

This is an independent implementation of the SSM Session Manager protocol.

---

## Overview

Unlike the [official AWS Session Manager Plugin](https://github.com/aws/session-manager-plugin) (a CLI binary written in Go), `aws-ssm-bridge` is a **library** designed for embedding in your applications.

### Features

- **Binary Protocol**: Full 116-byte AWS header, SHA-256 digest validation
- **Reliable Delivery**: Sequence tracking, ACK/retransmission, RTT estimation (Jacobson/Karels)
- **Interactive Shell**: Raw terminal mode, resize handling (SIGWINCH)
- **Port Forwarding**: TCP tunneling via `PortForwarder`
- **Python Bindings**: Async support via PyO3, type stubs included
- **Security**: `#![forbid(unsafe_code)]`, rate limiting, SSRF protection

---

## Installation

### Rust

```toml
[dependencies]
aws-ssm-bridge = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Python

```bash
pip install aws-ssm-bridge
```

---

## Quick Start

### Interactive Shell

```rust
use aws_ssm_bridge::interactive::{InteractiveShell, InteractiveConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = InteractiveConfig::default();
    let mut shell = InteractiveShell::new(config)?;
    
    // Connect and run interactive session
    // Handles raw mode, resize (SIGWINCH), signals (Ctrl+C/D/Z)
    shell.connect("i-0123456789abcdef0").await?;
    shell.run().await?;
    Ok(())
}
```

### Programmatic Session

```rust
use aws_ssm_bridge::{SessionManager, SessionConfig};
use futures::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let manager = SessionManager::new().await?;
    
    let mut session = manager.start_session(SessionConfig {
        target: "i-0123456789abcdef0".into(),
        ..Default::default()
    }).await?;
    
    let mut output = session.output();
    tokio::spawn(async move {
        while let Some(data) = output.next().await {
            print!("{}", String::from_utf8_lossy(&data));
        }
    });
    
    session.send(b"hostname\n").await?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    session.terminate().await?;
    
    Ok(())
}
```

### Port Forwarding

```rust
use aws_ssm_bridge::{SessionManager, PortForwardConfig, PortForwarder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let manager = SessionManager::new().await?;
    
    let forwarder = PortForwarder::new(&manager, PortForwardConfig {
        target: "i-0123456789abcdef0".into(),
        local_port: 8080,
        remote_port: 80,
        ..Default::default()
    }).await?;
    
    println!("Forwarding localhost:8080 -> remote:80");
    forwarder.wait().await?;
    Ok(())
}
```

### Python

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    manager = await SessionManager.new()
    
    async with await manager.start_session(target="i-0123456789abcdef0") as session:
        await session.send(b"hostname\n")
        async for chunk in await session.output():
            print(chunk.decode(), end="")

asyncio.run(main())
```

### Type-Safe Documents

Use type-safe document wrappers instead of magic strings:

```rust
use aws_ssm_bridge::{SessionBuilder, documents::*};

// Port forwarding to instance
let session = SessionBuilder::new("i-xxx")
    .document(PortForwardingSession::builder()
        .remote_port(3306)
        .local_port(13306)
        .build())
    .build().await?;

// Port forwarding through bastion to RDS
let session = SessionBuilder::new("i-bastion")
    .document(PortForwardingToRemoteHost::new("mydb.rds.amazonaws.com", 3306))
    .build().await?;

// SSH over Session Manager
let session = SessionBuilder::new("i-xxx")
    .document(SshSession::new())
    .build().await?;

// Interactive command execution
let session = SessionBuilder::new("i-xxx")
    .document(InteractiveCommand::new("top"))
    .build().await?;
```

---

## Documentation

- [Getting Started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [Security](docs/security.md)
- [Binary Protocol](docs/binary_protocol.md)
- [Protocol Flow](docs/protocol_flow.md)
- [Python Bindings](docs/python.md)

---

## Examples

### Rust Examples (`examples/`)

| Example | Description |
|---------|-------------|
| `interactive_shell.rs` | Full interactive shell with raw mode, resize, signals |
| `shell_session.rs` | Programmatic shell session (send commands, read output) |
| `port_forwarding.rs` | TCP port forwarding through SSM |
| `session_pool.rs` | Managing multiple concurrent sessions |
| `reconnecting.rs` | Auto-reconnection with exponential backoff |
| `metrics_session.rs` | Session with observability hooks |

Run with: `cargo run --example interactive_shell -- i-0123456789abcdef0`

### Python Examples (`python_examples/`)

| Example | Description |
|---------|-------------|
| `interactive_shell.py` | Full interactive shell with raw terminal mode |
| `shell_session.py` | Basic shell session with context manager |
| `port_forwarding.py` | TCP port forwarding |
| `multiple_sessions.py` | Concurrent sessions to multiple instances |

Run with: `python python_examples/interactive_shell.py i-0123456789abcdef0`

---

## Architecture

```
src/
├── lib.rs              # Public API
├── binary_protocol.rs  # 116-byte header, SHA-256
├── session.rs          # Session lifecycle
├── connection.rs       # WebSocket, retransmit
├── ack.rs              # ACK tracking, RTT
├── handshake.rs        # 3-phase handshake
├── port_forward.rs     # TCP tunneling
├── rate_limit.rs       # Token bucket
└── python/             # PyO3 bindings
```

---

## Security

- `#![forbid(unsafe_code)]`
- SSRF protection (AWS endpoint validation)
- Rate limiting (configurable token bucket)
- TLS required (WSS only)
- AWS transport encryption (all SSM traffic is encrypted)

See [Security Documentation](docs/security.md) for threat model and details.

---

## License

MIT License. See [LICENSE](LICENSE).
