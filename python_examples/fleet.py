"""Run one command across several instances at once.

    python python_examples/fleet.py "uptime" i-0123456789abcdef0 i-0fedcba9876543210
"""

import asyncio
import sys

from aws_ssm_bridge import SessionManager, SsmError, configure_logging


async def run_on(manager: SessionManager, target: str, command: str) -> str:
    session = await manager.start_session(
        target,
        document_name="AWS-StartNonInteractiveCommand",
        parameters={"command": [command]},
    )
    try:
        await session.wait_ready()
        chunks = []

        async def drain() -> None:
            async for chunk in session.output():
                chunks.append(chunk)

        # A non-interactive command ends its own session, but cap the wait so
        # one wedged instance cannot stall the whole fleet.
        try:
            await asyncio.wait_for(drain(), timeout=30.0)
        except asyncio.TimeoutError:
            pass
        return b"".join(chunks).decode(errors="replace")
    finally:
        await session.terminate()


async def main() -> None:
    configure_logging("warn")
    command, *targets = sys.argv[1:]

    # One manager for the whole fleet: the SSM client owns a connection pool and
    # a credential cache, both worth sharing.
    manager = await SessionManager.new()

    # return_exceptions keeps one unreachable instance from cancelling the rest.
    results = await asyncio.gather(
        *(run_on(manager, target, command) for target in targets),
        return_exceptions=True,
    )

    for target, result in zip(targets, results):
        print(f"=== {target} ===")
        if isinstance(result, SsmError):
            print(f"failed: {result}")
        elif isinstance(result, BaseException):
            raise result
        else:
            print(result.strip())


asyncio.run(main())
