"""Forward a local port to a port on the instance.

    python python_examples/port_forward.py i-0123456789abcdef0 22 127.0.0.1:18022
"""

import asyncio
import sys

from aws_ssm_bridge import PortForwarder, SessionManager, configure_logging


async def main() -> None:
    configure_logging("info")
    target = sys.argv[1]
    remote_port = int(sys.argv[2])
    local_addr = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1:0"

    manager = await SessionManager.new()
    session = await manager.start_port_forward(target, remote_port)

    # Bind first so a port conflict fails immediately, and so the chosen port
    # can be printed before any traffic flows.
    forwarder = await PortForwarder.bind(local_addr)
    print(f"forwarding {forwarder.address} -> instance:{remote_port} (Ctrl-C to stop)")

    try:
        await forwarder.forward(session)
    except (KeyboardInterrupt, asyncio.CancelledError):
        forwarder.stop()
    finally:
        print("closing:", session.close_reason)
        await session.terminate()


asyncio.run(main())
