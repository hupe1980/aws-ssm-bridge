# aws-ssm-bridge

A Rust implementation of the AWS Systems Manager **Session Manager** protocol,
with async Python bindings.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94.1%2B-orange)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org)
[![unsafe](https://img.shields.io/badge/unsafe-forbidden-success)](Cargo.toml)

> **Not affiliated with AWS.** This is an independent implementation of a
> documented-by-observation protocol, not endorsed or sponsored by Amazon Web
> Services, Inc.

---

## What this is for

The official [`session-manager-plugin`][plugin] is a CLI binary: you shell out to
it, hand it JSON on `argv`, and parse whatever it prints. That is fine for a
terminal and awkward for everything else.

`aws-ssm-bridge` is a **library**. Open sessions, stream bytes and forward ports
from inside your own async application — no subprocess, no plugin to install, no
output scraping.

```rust
use aws_ssm_bridge::SessionBuilder;
use futures_util::StreamExt;

let session = SessionBuilder::new("i-0123456789abcdef0").start().await?;
session.wait_ready().await?;

let mut output = session.output();
session.send(&b"uname -a\r"[..]).await?;

while let Some(chunk) = output.next().await {
    print!("{}", String::from_utf8_lossy(&chunk));
}
session.terminate().await?;
```

---

## Install

```sh
cargo add aws-ssm-bridge
cargo add tokio --features rt-multi-thread,macros
```

```sh
pip install aws-ssm-bridge
```

Requires the same IAM permissions as the official plugin: `ssm:StartSession` on
the target, and `ssm:TerminateSession` on your own sessions.

---

## Capabilities

| | |
|---|---|
| **Shell and command sessions** | Interactive shells, `AWS-StartInteractiveCommand`, `AWS-StartNonInteractiveCommand` |
| **Port forwarding** | smux-multiplexed, many concurrent TCP connections over one session |
| **KMS session encryption** | AES-256-GCM end-to-end, for accounts that mandate encrypted sessions |
| **Interactive terminal** | Raw byte passthrough, SIGWINCH resize, panic-safe restore |
| **Reconnection** | Durable output stream across reconnects, full-jitter backoff |
| **Pooling** | Bounded concurrent sessions with automatic reaping |
| **Observability** | `tracing` spans throughout, pluggable metrics recorder |
| **Python** | Full async API, type stubs, context managers |

Verified against a live SSM agent (3.3.3572.0): shell sessions, handshake,
six concurrent multiplexed TCP streams, and clean teardown.

---

## Guided tour

### Shell session

```rust
use aws_ssm_bridge::SessionBuilder;
use futures_util::StreamExt;

let session = SessionBuilder::new("i-0123456789abcdef0")
    .region("eu-central-1")
    .reason("incident 4711")     // recorded in CloudTrail
    .start()
    .await?;

let mut output = session.output();   // subscribe *before* sending
session.wait_ready().await?;
session.send(&b"df -h\r"[..]).await?;
```

Send `\r`, not `\n`: a remote pty maps carriage return to newline, but Windows
shells behind winpty do not accept a bare line feed.

### Port forwarding

```rust
use std::sync::Arc;
use aws_ssm_bridge::{
    documents::PortForwardingToRemoteHost, install_signal_handlers,
    PortForwardConfig, PortForwarder, SessionBuilder, ShutdownSignal,
};

let shutdown = ShutdownSignal::new();
install_signal_handlers(shutdown.clone());

let session = Arc::new(
    SessionBuilder::new("i-0123456789abcdef0")
        .document(PortForwardingToRemoteHost::new("db.internal", 5432))
        .start()
        .await?,
);

let forwarder = PortForwarder::bind(PortForwardConfig {
    local_addr: "127.0.0.1:15432".parse()?,
    ..Default::default()
})
.await?;

println!("psql -h 127.0.0.1 -p {}", forwarder.local_addr().port());
forwarder.forward(session, shutdown).await?;
```

Each accepted connection becomes its own smux stream inside one WebSocket, so
concurrent connections neither block nor corrupt each other.

### Typed documents

```rust
use aws_ssm_bridge::documents::*;

PortForwardingSession::new(3306)                            // port on the instance
PortForwardingToRemoteHost::new("db.internal", 5432)        // through the instance
SshSession::new()                                           // ssh ProxyCommand transport
InteractiveCommand::new("top")                              // with a pty
NonInteractiveCommand::new("systemctl status nginx")        // without a pty
```

### Python

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    manager = await SessionManager.new(region="eu-central-1")
    async with await manager.start_session("i-0123456789abcdef0") as session:
        await session.send(b"uname -a\r")
        async for chunk in session.output():
            print(chunk.decode(errors="replace"), end="")

asyncio.run(main())
```

---

## Session lifetime

A session is either running or closed. **Every** way it can end — a clean
`terminate()`, the agent hanging up, a dead network, a protocol violation —
resolves `Session::closed()` and records a `CloseReason`.

```rust
tokio::select! {
    () = session.closed() => eprintln!("gone: {}", session.close_reason().unwrap()),
    result = do_work(&session) => result?,
}
```

That one guarantee is what makes the layers above it work: the port forwarder
stops accepting when the tunnel dies, the pool reaps dead entries, and
`ReconnectingSession` knows when to rebuild. There is no state in which the
handle looks alive but nothing is running.

Reconnection restores *connectivity*, not continuity — a new session is a new
process on the target, so shell state and anything printed while disconnected
are gone.

---

## Feature flags

| Feature | Default | Effect |
|---|:---:|---|
| `interactive` | ✅ | `terminal` and `InteractiveShell`; pulls in `crossterm` |
| `kms` | ✅ | KMS session encryption; pulls in `aws-sdk-kms` and `aes-gcm` |
| `python` | — | PyO3 bindings |
| `extension-module` | — | Link the bindings as a Python extension module; set by `maturin` when building a wheel |

Built without `kms`, a session whose account mandates encryption fails the
handshake with an explicit error instead of quietly running in plaintext.

`extension-module` is deliberately separate from `python`: it leaves the CPython
symbols for the interpreter to resolve at load time, which is right for a wheel
and fatal for a test binary. Because `--all-features` would enable it, name the
features you want instead — `--features interactive,kms` is what CI runs.

---

## Examples

| Rust | |
|---|---|
| `cargo run --example shell -- i-… "uname -a"` | Run a command, print the output |
| `cargo run --example interactive -- i-…` | Full interactive shell |
| `cargo run --example port_forward -- i-… 5432 127.0.0.1:15432` | TCP tunnel |
| `cargo run --example reconnecting -- i-…` | Survive a dropped connection |
| `cargo run --example fleet -- "uptime" i-… i-…` | One command, many instances |
| `cargo run --example metrics -- i-…` | Wire up the metrics hooks |

Python equivalents live in [`python_examples/`](python_examples).

---

## Documentation

Full documentation: **[hupe1980.github.io/aws-ssm-bridge](https://hupe1980.github.io/aws-ssm-bridge/)**

| | |
|---|---|
| [Getting started](https://hupe1980.github.io/aws-ssm-bridge/docs/getting-started/) | Credentials, your first session, port forwarding, every `CloseReason` |
| [Architecture](https://hupe1980.github.io/aws-ssm-bridge/docs/architecture/) | How it is layered, and why the non-obvious parts are that way |
| [Wire protocol](https://hupe1980.github.io/aws-ssm-bridge/docs/protocol/) | The binary format, reliability, smux, KMS encryption |
| [Security](https://hupe1980.github.io/aws-ssm-bridge/docs/security/) | Threat model, and what is explicitly not defended against |
| [Python API](https://hupe1980.github.io/aws-ssm-bridge/docs/python/) | The full async binding surface |
| [API reference](https://docs.rs/aws-ssm-bridge) | Every type and method, on docs.rs |
| [Changelog](CHANGELOG.md) | What changed, and how to migrate |

The site is built with [Zola](https://www.getzola.org) from [`site/`](site);
`just site` serves it locally.

---

## Security

- `unsafe_code = "forbid"` for the whole crate, so any `unsafe` fails the build.
- The session token travels only in the data-channel open message — never in a
  URL, where proxies and traces would record it.
- SHA-256 payload digests are **verified**, matching the reference
  implementation; a mismatch ends the session rather than delivering corrupt
  bytes.
- The data channel refuses any endpoint that is not an AWS SSM messages host.
- KMS session encryption is AES-256-GCM with a per-message nonce, and a client
  that cannot negotiate it fails the handshake rather than downgrading.

See the [security model](https://hupe1980.github.io/aws-ssm-bridge/docs/security/) for the threat model and what is
explicitly *not* defended against.

---

## Development

```sh
just                # list every recipe
just check          # what CI runs: fmt, clippy, tests, docs
just matrix         # every feature combination
just bench          # framing and reliability micro-benchmarks
just fuzz           # cargo-fuzz over the network-facing parsers
just python         # build and install the wheel
just site           # serve the documentation site locally
just release-check  # everything above, plus MSRV, --locked, publish dry run
```

The MSRV is set by the AWS SDK, not by this crate's own code. `just msrv`
verifies it against the committed lockfile — which is the only way the check
means anything, since an unlocked resolve pulls in dependencies that need a
newer toolchain than users actually get.

---

## License

MIT. See [LICENSE](LICENSE).

[plugin]: https://github.com/aws/session-manager-plugin
