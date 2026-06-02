---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Prerequisites

- **AWS Credentials** with SSM permissions
- **Target** with SSM agent installed (EC2, ECS, or on-premises)
- **Rust 1.81+** or **Python 3.8+**

### Required IAM Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["ssm:StartSession", "ssm:TerminateSession"],
        "Resource": "*"
    }]
}
```

---

## Installation

### Rust

```toml
[dependencies]
aws-ssm-bridge = "0.4"
tokio = { version = "1", features = ["full"] }
```

### Python

```bash
pip install aws-ssm-bridge
```

---

## Usage

### Interactive Shell

```rust
use aws_ssm_bridge::interactive::{InteractiveShell, InteractiveConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = InteractiveConfig::default();
    let mut shell = InteractiveShell::new(config)?;

    // Handles raw mode, resize (SIGWINCH), signals (Ctrl+C/D/Z)
    shell.connect("i-0123456789abcdef0").await?;
    shell.run().await?;
    Ok(())
}
```

### Port Forwarding

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use aws_ssm_bridge::{SessionBuilder, PortForwardConfig, PortForwarder,
                     ShutdownSignal, install_signal_handlers};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let shutdown = ShutdownSignal::new();
    install_signal_handlers(shutdown.clone());

    // Remote port belongs in the session document, not PortForwardConfig.
    let session = Arc::new(
        SessionBuilder::new("i-0123456789abcdef0")
            .port_forward(80)
            .build()
            .await?
    );

    // bind() binds the local TCP port immediately; local_addr() returns the
    // actual address (useful when port 0 was requested for an OS-assigned port).
    let forwarder = PortForwarder::bind(PortForwardConfig {
        local_addr: "127.0.0.1:8080".parse::<SocketAddr>()?,
        ..Default::default()
    }).await?;
    println!("Forwarding {} -> remote:80", forwarder.local_addr());
    forwarder.forward(session, shutdown).await?;
    Ok(())
}
```

### Python Async

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    manager = await SessionManager.new()
    
    async with await manager.start_session(target="i-0123456789abcdef0") as session:
        await session.send(b"whoami\n")
        async for chunk in session.output():
            print(chunk.decode(), end="")

asyncio.run(main())
```

---

## Configuration

### Timeouts

```rust
use aws_ssm_bridge::SessionConfig;
use std::time::Duration;

let config = SessionConfig {
    target: "i-xxx".into(),
    connect_timeout: Duration::from_secs(30),
    idle_timeout: Duration::from_secs(20 * 60),
    max_duration: Some(Duration::from_secs(3600)),
    ..Default::default()
};
```

### Feature Flags

```toml
[dependencies]
aws-ssm-bridge = { version = "0.4", default-features = false, features = ["interactive"] }
```

| Feature | Description | Default |
|:--------|:------------|:-------:|
| `interactive` | Terminal handling | ✅ |
| `python` | Python bindings | ❌ |

---

## Next Steps

- [Architecture](architecture) - Design overview
- [Security](security) - Threat model and mitigations
- [Binary Protocol](binary_protocol) - Wire format details
- [Protocol Flow](protocol_flow) - Sequence diagrams
