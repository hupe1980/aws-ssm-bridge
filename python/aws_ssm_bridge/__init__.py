"""Async Python bindings for the AWS Systems Manager Session Manager protocol.

Open sessions, stream bytes and forward ports from Python, without the
`session-manager-plugin` binary or a subprocess.

    import asyncio
    from aws_ssm_bridge import SessionManager

    async def main():
        manager = await SessionManager.new()
        async with await manager.start_session("i-0123456789abcdef0") as session:
            await session.send(b"uname -a\\r")
            async for chunk in session.output():
                print(chunk.decode(errors="replace"), end="")

    asyncio.run(main())

Send ``\\r``, not ``\\n``, for Enter: a remote pty maps carriage return to
newline, but Windows shells behind winpty do not accept a bare line feed.

Credentials come from the standard AWS chain -- environment variables,
``~/.aws/config``, SSO, or instance metadata.
"""

# The wheel is abi3-py38, so `str | None` in a signature would raise at import
# time on Python 3.8 and 3.9. Deferring annotation evaluation keeps the modern
# spelling working everywhere.
from __future__ import annotations

from ._internal import (
    InteractiveShell,
    OutputStream,
    PortForwarder,
    Session,
    SessionManager,
    SsmAwsError,
    SsmClosedError,
    SsmCryptoError,
    SsmError,
    SsmProtocolError,
    SsmTimeoutError,
    SsmTransportError,
    __version__,
    configure_logging,
)

__all__ = [
    "SessionManager",
    "Session",
    "OutputStream",
    "PortForwarder",
    "InteractiveShell",
    "configure_logging",
    "connect",
    "run_command",
    "SsmError",
    "SsmAwsError",
    "SsmProtocolError",
    "SsmTransportError",
    "SsmClosedError",
    "SsmCryptoError",
    "SsmTimeoutError",
    "__version__",
]


async def connect(target: str, *, region: str | None = None, reason: str | None = None) -> Session:
    """Open a shell session in one call.

    Convenient for scripts. Starting several sessions? Build one
    :class:`SessionManager` and reuse it, so they share a connection pool and
    credential cache.

    The returned session is *not* waited on; call ``await session.wait_ready()``
    or use it as an async context manager before sending.
    """
    manager = await SessionManager.new(region=region)
    return await manager.start_session(target, reason=reason)


async def run_command(
    target: str,
    command: str,
    *,
    region: str | None = None,
    timeout: float = 30.0,
) -> str:
    """Run one command and return everything it printed.

    Uses ``AWS-StartNonInteractiveCommand``, so there is no pty and no shell
    prompt in the output. The session ends when the command exits; anything
    still streaming after ``timeout`` seconds is discarded.
    """
    import asyncio

    manager = await SessionManager.new(region=region)
    session = await manager.start_session(
        target,
        document_name="AWS-StartNonInteractiveCommand",
        parameters={"command": [command]},
    )

    async def collect() -> str:
        chunks = []
        async for chunk in session.output():
            chunks.append(chunk)
        return b"".join(chunks).decode(errors="replace")

    try:
        await session.wait_ready()
        return await asyncio.wait_for(collect(), timeout=timeout)
    except asyncio.TimeoutError:
        return ""
    finally:
        await session.terminate()
