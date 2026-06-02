---
layout: default
title: Python
nav_order: 4
---

# Python Bindings
{: .no_toc }

Async Python bindings via PyO3 with full type hints.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Installation

```bash
pip install aws-ssm-bridge
```

---

## Quick Start

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    # Create session manager
    manager = await SessionManager.new()
    
    # Use context manager for automatic cleanup
    async with await manager.start_session(target="i-0123456789abcdef0") as session:
        await session.send(b"whoami\n")
        
        async for chunk in session.output():
            print(chunk.decode(), end="")

asyncio.run(main())
```

---

## Session Manager

The `SessionManager` handles AWS authentication and session lifecycle:

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    # Create manager (loads AWS credentials from default chain)
    manager = await SessionManager.new()
    
    # Or specify a region explicitly
    manager = await SessionManager.new(region="us-west-2")
    
    # Start session with options
    session = await manager.start_session(
        target="i-0123456789abcdef0",
        region="us-west-2",                    # Optional
        session_type="standard_stream",        # Optional
        reason="Debugging issue #123",         # Optional (for audit)
    )
    
    # Wait for session to be ready
    if await session.wait_for_ready(timeout_secs=30.0):
        print(f"Connected: {session.id}")  # id is a sync property
        print(f"Ready: {session.is_ready()}")  # is_ready() is sync
        await session.send(b"hostname\n")
        async for chunk in session.output():
            print(chunk.decode(), end="")
    
    # Always terminate when done
    await session.terminate()

asyncio.run(main())
```

---

## Context Manager

Use the async context manager for automatic cleanup:

```python
async with await manager.start_session(target="i-xxx") as session:
    await session.send(b"ls -la\n")
    async for chunk in session.output():
        print(chunk.decode(), end="")
# Session automatically terminated on exit
```

---

## Interactive Shell

Full terminal support with raw mode, resize, and signal handling:

```python
import asyncio
from aws_ssm_bridge import InteractiveShell, InteractiveConfig, run_shell

async def main():
    # Option 1: Quick one-liner
    await run_shell("i-0123456789abcdef0")
    
    # Option 2: Full control
    config = InteractiveConfig(
        show_banner=True,
        send_initial_size=True,
        forward_signals=True,
    )
    shell = InteractiveShell(config)
    await shell.connect("i-0123456789abcdef0")
    await shell.run()  # Blocks until Ctrl+D

asyncio.run(main())
```

Features:
- Raw terminal mode (no echo, immediate input)
- Terminal resize detection (SIGWINCH)
- Signal forwarding (Ctrl+C, Ctrl+D, Ctrl+Z)
- Automatic terminal restoration on exit/crash

---

## Multiple Sessions

Run commands on multiple instances concurrently:

```python
import asyncio
from aws_ssm_bridge import SessionManager

async def run_command(manager, instance_id: str, command: str) -> str:
    async with await manager.start_session(target=instance_id) as session:
        await session.send(f"{command}\n".encode())
        output = []
        async for chunk in session.output():
            output.append(chunk.decode())
            if len(output) > 10:
                break
        return "".join(output)

async def main():
    manager = await SessionManager.new()
    instances = ["i-aaa", "i-bbb", "i-ccc"]
    results = await asyncio.gather(*[
        run_command(manager, i, "hostname") for i in instances
    ], return_exceptions=True)
    
    for inst, result in zip(instances, results):
        if isinstance(result, Exception):
            print(f"{inst}: ERROR - {result}")
        else:
            print(f"{inst}: {result.strip()}")

asyncio.run(main())
```

---

## Session Types

AWS SSM Session Manager supports 3 session types:

```python
# Shell session (default) - standard interactive shell
shell = await manager.start_session(
    target="i-xxx",
    session_type="standard_stream",
)

# Port forwarding session (AWS-StartPortForwardingSession)
port = await manager.start_session(
    target="i-xxx",
    session_type="port",
    parameters={"portNumber": ["3306"], "localPortNumber": ["13306"]},
)

# Interactive commands (AWS-StartInteractiveCommand)
cmd = await manager.start_session(
    target="i-xxx",
    session_type="interactive_commands",
    parameters={"command": ["top"]},
)
```

For custom documents like `AWS-StartSSHSession` or `AWS-StartPortForwardingSessionToRemoteHost`:

```python
# SSH over Session Manager
ssh = await manager.start_session(
    target="i-xxx",
    document_name="AWS-StartSSHSession",
    parameters={"portNumber": ["22"]},
)

# Port forward through bastion to RDS
rds = await manager.start_session(
    target="i-bastion",
    document_name="AWS-StartPortForwardingSessionToRemoteHost",
    parameters={
        "host": ["mydb.cluster-xxx.us-east-1.rds.amazonaws.com"],
        "portNumber": ["3306"],
        "localPortNumber": ["13306"],
    },
)
```

---

## Type Hints

Full `.pyi` stubs are included for IDE autocomplete:

```python
from aws_ssm_bridge import SessionManager, Session, OutputStream

async def example(manager: SessionManager) -> None:
    session: Session = await manager.start_session(target="i-xxx")
    
    # id and is_ready() are synchronous — no await needed
    session_id: str = session.id
    ready: bool = session.is_ready()
    
    stream: OutputStream = session.output()
    
    async for chunk in stream:
        data: bytes = chunk
        print(data.decode())
```
