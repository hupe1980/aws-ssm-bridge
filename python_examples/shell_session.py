"""
Example: Basic shell session with AWS SSM

This example demonstrates how to start a shell session with an EC2 instance
using AWS SSM Session Manager.

Uses the async context manager for automatic cleanup.
"""

import asyncio
import sys
from aws_ssm_bridge import SessionManager


async def main():
    if len(sys.argv) < 2:
        print("Usage: python shell_session.py <instance-id> [region]")
        sys.exit(1)

    instance_id = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"Starting SSM session to {instance_id}")
    if region:
        print(f"Region: {region}")

    # Create session manager
    manager = await SessionManager.new()

    # Use async context manager for automatic cleanup
    async with await manager.start_session(
        target=instance_id,
        region=region,
        session_type="standard_stream",
    ) as session:
        print(f"✓ Session started: {session.id}")
        print(f"  State: {await session.state()}")
        print(f"  Ready: {session.is_ready()}")

        # Send a simple command
        print("\nSending command: echo 'Hello from Python!'")
        await session.send(b"echo 'Hello from Python!'\n")

        # Wait a moment for output
        await asyncio.sleep(2)

    # Session automatically terminated when exiting context
    print("✓ Session terminated successfully")


if __name__ == "__main__":
    asyncio.run(main())
