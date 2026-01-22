---
layout: home
title: Home
nav_order: 1
description: "Rust library implementing AWS SSM Session Manager protocol with Python bindings"
permalink: /
---

# aws-ssm-bridge
{: .fs-9 }

A Rust implementation of the AWS Systems Manager (SSM) Session Manager protocol.
{: .fs-6 .fw-300 }

[Get Started](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[GitHub](https://github.com/hupe1980/aws-ssm-bridge){: .btn .fs-5 .mb-4 .mb-md-0 }

---

{: .warning }
> **Disclaimer**: This project is **not affiliated with, endorsed by, or sponsored by Amazon Web Services, Inc.** This is an independent implementation based on protocol analysis.

---

## Overview

`aws-ssm-bridge` provides a native Rust implementation of the AWS SSM Session Manager protocol, enabling programmatic access to EC2 instances, ECS containers, and on-premises servers without SSH or open inbound ports.

### Why This Library?

The official [AWS Session Manager Plugin](https://github.com/aws/session-manager-plugin) is a CLI binary written in Go. This library is designed for **embedding** in your applications:

- **Interactive shells** with proper terminal handling
- **Port forwarding** via TCP tunneling
- **Session pooling** for connection reuse
- **Async Python bindings** via PyO3

---

## Capabilities

| Feature | Description |
|:--------|:------------|
| **Binary Protocol** | Full 116-byte AWS header, SHA-256 digest validation |
| **Reliable Delivery** | Sequence tracking, ACK/retransmission, RTT estimation |
| **Interactive Shell** | Raw terminal mode, resize handling (SIGWINCH) |
| **Port Forwarding** | TCP tunneling via `PortForwarder` |
| **Python** | Async support, context managers, type stubs |

---

## Quick Example

### Rust

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

### Python

```python
import asyncio
from aws_ssm_bridge import AsyncSession

async def main():
    async with AsyncSession("i-0123456789abcdef0") as session:
        await session.send("hostname\n")
        async for chunk in session.output():
            print(chunk.decode(), end="")

asyncio.run(main())
```

---

## Security Properties

| Property | Implementation |
|:---------|:---------------|
| Memory safety | `#![forbid(unsafe_code)]` |
| SSRF protection | AWS endpoint validation |
| Rate limiting | Token bucket (configurable) |
| TLS required | WSS only (no unencrypted WebSocket) |

[Security Documentation →](security)

---

## License

MIT License. See [LICENSE](https://github.com/hupe1980/aws-ssm-bridge/blob/main/LICENSE).
