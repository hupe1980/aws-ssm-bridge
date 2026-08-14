"""Run one command in a shell session and print its output.

    python python_examples/shell.py i-0123456789abcdef0 "uname -a"
"""

import asyncio
import sys

from aws_ssm_bridge import SessionManager, configure_logging


async def main() -> None:
    configure_logging("info")
    target = sys.argv[1]
    command = sys.argv[2] if len(sys.argv) > 2 else "uname -a"

    manager = await SessionManager.new()

    # The context manager waits for the agent handshake on entry and terminates
    # the session on exit, including when the body raises.
    async with await manager.start_session(target, reason="python shell example") as session:
        print(f"session {session.id} ready (agent {session.agent_version})")

        # Send \r, not \n: the remote pty maps carriage return to newline.
        await session.send(f"{command}\r".encode())

        async def drain() -> None:
            async for chunk in session.output():
                sys.stdout.write(chunk.decode(errors="replace"))
                sys.stdout.flush()

        # The shell never closes its output on its own, so stop reading after a
        # quiet period rather than waiting forever.
        try:
            await asyncio.wait_for(drain(), timeout=3.0)
        except asyncio.TimeoutError:
            pass


asyncio.run(main())
