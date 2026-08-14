+++
title = "Getting started"
description = "Install aws-ssm-bridge, resolve AWS credentials, open your first Session Manager session, forward a port, and understand every CloseReason."
weight = 1
+++

## Prerequisites

| | |
|:---|:---|
| A target | An EC2 instance, `mi-` managed instance, or ECS Exec task with the SSM agent running and registered |
| Credentials | Resolved through the standard AWS chain: environment, `~/.aws/config`, SSO, or instance metadata |
| IAM | `ssm:StartSession` on the target; `ssm:TerminateSession` on your own sessions |

If `aws ssm start-session --target i-…` works, this crate will too. If it does
not, fix that first — the failure will be identical and the AWS CLI reports it
more legibly.

Additional permissions for optional features:

| Feature | Also needs |
|:---|:---|
| KMS session encryption | `kms:GenerateDataKey` for you, `kms:Decrypt` for the target's instance profile |
| Port forwarding | `ssm:StartSession` on `AWS-StartPortForwardingSession*` documents |


## Install

```toml
# Cargo.toml
[dependencies]
aws-ssm-bridge = "0.5"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
futures-util = "0.3"
```

```sh
pip install aws-ssm-bridge
```


## Your first session

```rust
use aws_ssm_bridge::SessionBuilder;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> aws_ssm_bridge::Result<()> {
    let session = SessionBuilder::new("i-0123456789abcdef0")
        .region("eu-central-1")
        .start()
        .await?;

    // Subscribe before sending. Output produced before you subscribe is not
    // replayed, so subscribing afterwards can miss the start of the response.
    let mut output = session.output();
    session.wait_ready().await?;

    session.send(&b"uname -a\r"[..]).await?;

    while let Some(chunk) = output.next().await {
        print!("{}", String::from_utf8_lossy(&chunk));
    }

    session.terminate().await
}
```

### Two things that surprise people

**Send `\r`, not `\n`.** A remote pty maps carriage return to newline, but
Windows shells behind winpty do not accept a bare line feed. `\r` works
everywhere.

**A shell never closes its output.** The loop above runs until the session ends.
For a request/response pattern, race the read against a timeout:

```rust
loop {
    tokio::select! {
        chunk = output.next() => match chunk {
            Some(chunk) => print!("{}", String::from_utf8_lossy(&chunk)),
            None => break,
        },
        () = session.closed() => break,
        _ = tokio::time::sleep(Duration::from_secs(2)) => break,
    }
}
```

Or use `NonInteractiveCommand`, which ends its own session when the command
exits.


## Running a single command

```rust
use aws_ssm_bridge::{documents::NonInteractiveCommand, SessionBuilder};

let session = SessionBuilder::new("i-0123456789abcdef0")
    .document(NonInteractiveCommand::new("systemctl is-active nginx"))
    .start()
    .await?;

session.wait_ready().await?;
session.closed().await;                  // the command ended the session
println!("exit code: {:?}", session.exit_code());
```


## Port forwarding

```rust
use std::sync::Arc;
use aws_ssm_bridge::{
    install_signal_handlers, PortForwardConfig, PortForwarder, SessionBuilder, ShutdownSignal,
};

let shutdown = ShutdownSignal::new();
install_signal_handlers(shutdown.clone());

let session = Arc::new(
    SessionBuilder::new("i-0123456789abcdef0")
        .port_forward(5432)
        .start()
        .await?,
);

let forwarder = PortForwarder::bind(PortForwardConfig {
    local_addr: "127.0.0.1:15432".parse().unwrap(),
    ..Default::default()
})
.await?;

println!("listening on {}", forwarder.local_addr());
forwarder.forward(session, shutdown).await?;   // runs until Ctrl-C or the session ends
```

To reach a third host *through* the instance — an RDS endpoint from a bastion —
use `PortForwardingToRemoteHost::new("db.internal", 5432)` instead of
`.port_forward(…)`.

`PortForwarder` accepts only port-forwarding sessions. Handing it a shell session
returns `Error::Config` immediately, because the agent does not speak smux there
and the tunnel would silently hang.


## Interactive shell

```rust
use aws_ssm_bridge::InteractiveShell;

let exit_code = InteractiveShell::new(Default::default())
    .run("i-0123456789abcdef0")
    .await?;
std::process::exit(exit_code.unwrap_or(0));
```

Needs a real terminal on stdin and stdout; it returns `Error::Config` on a pipe
rather than failing later inside raw-mode setup.


## Reacting to the session ending

```rust
tokio::select! {
    () = session.closed() => {
        eprintln!("session ended: {}", session.close_reason().unwrap());
    }
    result = do_work(&session) => result?,
}
```

`CloseReason` distinguishes what happened, and `is_recoverable()` says whether
a retry could plausibly help:

| Reason | Recoverable | Typical cause |
|:---|:---:|:---|
| `Terminated` | — | You called `terminate()` |
| `AgentClosed` | — | The remote process exited, or an operator ended the session |
| `PeerUnresponsive` | ✅ | Network died; nothing arrived within the idle timeout |
| `DeliveryFailed` | ✅ | The agent stopped acknowledging |
| `Transport` | ✅ | The WebSocket failed |
| `Protocol` | — | The peer violated the protocol, or needs something unsupported |

`AgentClosed` carries the two things worth acting on — the remote process's exit
status, and the agent's own explanation when it sent one:

```rust
if let Some(CloseReason::AgentClosed { exit_code, detail }) = session.close_reason() {
    // detail is e.g. Some("Connection refused by 10.0.0.7:5432") for a port
    // forward whose target is down.
    eprintln!("agent closed: {detail:?}, exit {exit_code:?}");
}
```


## Surviving disconnects

```rust
use aws_ssm_bridge::{ReconnectConfig, ReconnectingSession};

let session = ReconnectingSession::connect("i-0123456789abcdef0", ReconnectConfig::default())
    .await?;

let mut output = session.output();   // this stream outlives each session
session.send(&b"tail -f /var/log/syslog\r"[..]).await?;
```

Reconnection restores connectivity, **not continuity**. A new session is a new
process on the target: the working directory, environment, running jobs and
anything printed while disconnected are gone. Treat every
`ReconnectEvent::Reconnected` as a fresh shell.


## Many instances at once

```rust
use aws_ssm_bridge::{PoolConfig, SessionPool};

let pool = SessionPool::new(PoolConfig { max_sessions: 25, ..Default::default() }).await?;
let session = pool.start("i-0123456789abcdef0").await?;   // -> Arc<Session>
// …
pool.shutdown().await;    // terminates everything concurrently
```


## Logging

```rust
tracing_subscriber::fmt()
    .with_env_filter("aws_ssm_bridge=debug")
    .init();
```

`RUST_LOG=aws_ssm_bridge=debug` works too. In an interactive shell, log to
**stderr** — stdout belongs to the remote terminal, and interleaved log lines
corrupt the display.


## Troubleshooting

| Symptom | Cause |
|:---|:---|
| `TargetNotConnected` | The SSM agent is not running, or the instance has no route to the SSM endpoints |
| `AccessDeniedException` | Missing `ssm:StartSession`, or a session-document condition in the policy |
| Handshake times out | The agent connected but never replied — usually a KMS problem; check the logs for the `KMSEncryption` action |
| `not an AWS SSM messages endpoint` | The stream URL was not an AWS host. Set `endpoint_policy` to `AllowAny` only if you are deliberately testing against a mock |
| `PortForwarder needs a port-forwarding session` | The session was started without a forwarding document |
| Nothing arrives after sending | You subscribed with `output()` after `send()`; subscribe first |
