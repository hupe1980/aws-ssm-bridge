"""
Example: Managing multiple concurrent sessions

This example demonstrates how to manage multiple SSM sessions concurrently.
"""

import asyncio
from aws_ssm_bridge import SessionManager


async def run_session(manager, instance_id, region, session_num):
    """Run a single session"""
    print(f"[Session {session_num}] Starting session to {instance_id}")

    session = await manager.start_session(
        target=instance_id,
        region=region,
        session_type="standard_stream",
        reason=f"Test session {session_num}",
    )

    print(f"[Session {session_num}] Started: {session.id}")

    # Send a command
    await session.send(f"echo 'Hello from session {session_num}!'\n".encode())

    # Wait a moment
    await asyncio.sleep(1)

    # Terminate
    await session.terminate()
    await session.wait_terminated()

    print(f"[Session {session_num}] Terminated")


async def main():
    # Configuration
    instances = [
        ("i-1234567890abcdef0", "us-east-1"),
        ("i-0987654321fedcba0", "us-east-1"),
        ("i-abcdef1234567890", "us-west-2"),
    ]

    print(f"Starting {len(instances)} concurrent sessions...\n")

    # Create session manager
    manager = await SessionManager.new()

    # Run all sessions concurrently
    tasks = [
        run_session(manager, instance_id, region, i + 1)
        for i, (instance_id, region) in enumerate(instances)
    ]

    await asyncio.gather(*tasks)

    print("\n✓ All sessions completed")


if __name__ == "__main__":
    asyncio.run(main())
