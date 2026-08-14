+++
title = "Python"
description = "The async Python API for AWS Session Manager: SessionManager, Session, output streaming, port forwarding, interactive shells, errors and type stubs."
weight = 5
+++

## Install

```sh
pip install aws-ssm-bridge
```

Wheels are `abi3` for CPython 3.8 and later. The Rust core, including terminal
handling and KMS session encryption, is compiled in.

Building from source:

```sh
pip install maturin
maturin develop --release
```


## Quick start

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    manager = await SessionManager.new(region="eu-central-1")

    # The context manager waits for the agent handshake on entry and terminates
    # the session on exit, including when the body raises.
    async with await manager.start_session("i-0123456789abcdef0") as session:
        await session.send(b"uname -a\r")
        async for chunk in session.output():
            print(chunk.decode(errors="replace"), end="")

asyncio.run(main())
```

Send `\r`, not `\n`. A remote pty maps carriage return to newline; Windows shells
behind winpty do not accept a bare line feed.


## `SessionManager`

Build one and reuse it — the SSM client owns a connection pool and a credential
cache, both worth sharing.

```python
manager = await SessionManager.new()                      # ambient region
manager = await SessionManager.new(region="us-east-1")    # pinned
```

| Method | Purpose |
|:---|:---|
| `start_session(target, document_name=None, parameters=None, reason=None, ready_timeout=30.0)` | Shell, or any document |
| `start_port_forward(target, remote_port, reason=None)` | Forward a port on the instance |
| `start_remote_port_forward(target, host, remote_port, reason=None)` | Forward through the instance |
| `terminate_session(session_id)` | Terminate by ID, without a session object |

`target` may be an instance ID, an `mi-` managed instance, an `ecs:` task, or an
ARN.


## `Session`

```python
session = await manager.start_session("i-0123456789abcdef0")

await session.wait_ready()
await session.send(b"df -h\r")
await session.send_terminal_size(120, 40)
await session.terminate()
```

| Property | |
|:---|:---|
| `id` | The AWS session ID |
| `target` | What it connects to |
| `agent_version` | SSM agent version, after the handshake |
| `banner` | The agent's login banner, if any |
| `exit_code` | Remote process exit status, once it exits |
| `is_encrypted` | Whether KMS session encryption is active |
| `is_ready` / `is_closed` | Current state |
| `close_reason` | Why it ended, or `None` while open |

### Reading output

`output()` returns an async iterator. Subscribe **before** sending — output
produced earlier is not replayed.

```python
stream = session.output()
await session.send(b"ls -la\r")

async for chunk in stream:
    print(chunk.decode(errors="replace"), end="")
```

A shell never closes its output, so that loop runs until the session ends. For
request/response, bound it:

```python
async def drain():
    async for chunk in stream:
        print(chunk.decode(errors="replace"), end="")

try:
    await asyncio.wait_for(drain(), timeout=3.0)
except asyncio.TimeoutError:
    pass
```

Or use `AWS-StartNonInteractiveCommand`, which ends its own session:

```python
session = await manager.start_session(
    "i-0123456789abcdef0",
    document_name="AWS-StartNonInteractiveCommand",
    parameters={"command": ["systemctl is-active nginx"]},
)
await session.wait_ready()
await session.wait_closed()
print("exit code:", session.exit_code)
```


## Port forwarding

```python
from aws_ssm_bridge import PortForwarder, SessionManager

manager = await SessionManager.new()
session = await manager.start_port_forward("i-0123456789abcdef0", 5432)

# Bind first: a port conflict then fails immediately, and port 0 resolves to a
# real port you can print.
forwarder = await PortForwarder.bind("127.0.0.1:15432")
print(f"psql -h 127.0.0.1 -p {forwarder.port}")

try:
    await forwarder.forward(session)      # runs until the session ends
finally:
    await session.terminate()
```

Reaching a third host through the instance:

```python
session = await manager.start_remote_port_forward(
    "i-0123456789abcdef0", "db.cluster-abc.eu-central-1.rds.amazonaws.com", 5432
)
```

Call `forwarder.stop()` from a signal handler or another task to release the
port. A `PortForwarder` is single-use; bind a new one to forward again.


## Interactive shell

```python
import asyncio, sys
from aws_ssm_bridge import InteractiveShell

exit_code = asyncio.run(InteractiveShell().run("i-0123456789abcdef0"))
sys.exit(exit_code or 0)
```

Raw terminal mode, verbatim byte passthrough, SIGWINCH resize, and restoration on
every exit path. Requires a real terminal on stdin and stdout; raises
`ValueError` on a pipe.


## Convenience helpers

```python
from aws_ssm_bridge import connect, run_command

session = await connect("i-0123456789abcdef0")               # one-call shell
output  = await run_command("i-0123456789abcdef0", "uptime") # no pty, returns str
```

Fine for scripts. For anything that opens several sessions, build a
`SessionManager` and reuse it.


## Errors

```text
SsmError
 ├── SsmAwsError         an AWS API call failed
 ├── SsmProtocolError    the peer violated the protocol
 ├── SsmTransportError   the WebSocket failed
 ├── SsmClosedError      the session is no longer usable
 ├── SsmCryptoError      KMS session encryption failed
 └── SsmTimeoutError     an operation ran out of time
```

Invalid arguments raise `ValueError`; unsupported operations raise
`RuntimeError`.

```python
from aws_ssm_bridge import SsmAwsError, SsmError

try:
    session = await manager.start_session("i-0123456789abcdef0")
except SsmAwsError as e:
    print("AWS refused:", e)      # no permission, target offline, throttled
except SsmError as e:
    print("session failed:", e)
```


## Concurrency

Sessions are independent, so fan out with `asyncio.gather`. Use
`return_exceptions=True` so one unreachable instance does not cancel the rest.

```python
async def run_on(manager, target, command):
    session = await manager.start_session(
        target,
        document_name="AWS-StartNonInteractiveCommand",
        parameters={"command": [command]},
    )
    try:
        await session.wait_ready()
        return b"".join([c async for c in session.output()]).decode(errors="replace")
    finally:
        await session.terminate()

manager = await SessionManager.new()
results = await asyncio.gather(
    *(run_on(manager, t, "uptime") for t in targets),
    return_exceptions=True,
)
```


## Logging

```python
from aws_ssm_bridge import configure_logging

configure_logging("debug")   # off, error, warn, info, debug, trace
```

Writes to stderr. `RUST_LOG` still applies and takes precedence. The global
subscriber installs once per process; later calls are ignored.


## Type checking

The package ships `py.typed` and full stubs, so mypy and pyright see real types:

```python
from aws_ssm_bridge import Session, SessionManager

async def open_shell(manager: SessionManager, target: str) -> Session:
    return await manager.start_session(target)
```


## Examples

| File | |
|:---|:---|
| [`shell.py`](https://github.com/hupe1980/aws-ssm-bridge/blob/main/python_examples/shell.py) | Run a command and print the output |
| [`interactive.py`](https://github.com/hupe1980/aws-ssm-bridge/blob/main/python_examples/interactive.py) | Full interactive shell |
| [`port_forward.py`](https://github.com/hupe1980/aws-ssm-bridge/blob/main/python_examples/port_forward.py) | TCP tunnel |
| [`fleet.py`](https://github.com/hupe1980/aws-ssm-bridge/blob/main/python_examples/fleet.py) | One command across many instances |
