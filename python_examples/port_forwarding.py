"""
Example: Port forwarding with AWS SSM

This example demonstrates how to set up port forwarding to an EC2 instance
using AWS SSM Session Manager.
"""

import asyncio
import sys
from aws_ssm_bridge import SessionManager


async def main():
    if len(sys.argv) < 4:
        print("Usage: python port_forwarding.py <instance-id> <remote-port> <local-port> [region]")
        print("Example: python port_forwarding.py i-1234567890abcdef0 3306 13306 us-east-1")
        sys.exit(1)

    instance_id = sys.argv[1]
    remote_port = sys.argv[2]
    local_port = sys.argv[3]
    region = sys.argv[4] if len(sys.argv) > 4 else None

    print(f"Starting port forwarding session")
    print(f"  Instance: {instance_id}")
    print(f"  Remote port: {remote_port}")
    print(f"  Local port: {local_port}")
    if region:
        print(f"  Region: {region}")

    # Create session manager
    manager = await SessionManager.new()

    # Start a port forwarding session
    session = await manager.start_session(
        target=instance_id,
        region=region,
        session_type="port",
        document_name="AWS-StartPortForwardingSession",
        parameters={
            "portNumber": [remote_port],
            "localPortNumber": [local_port],
        },
    )

    print(f"\n✓ Port forwarding session started: {session.id}")
    
    # Wait for the session to be ready
    print("  Waiting for session to be ready...")
    if not await session.wait_for_ready(timeout_secs=30.0):
        print("  ERROR: Session did not become ready in time")
        await session.terminate()
        sys.exit(1)
    
    print(f"  You can now connect to localhost:{local_port}")

    # Wait for session state
    state = await session.state()
    print(f"  State: {state}")

    # Keep the session alive
    print("\nPress Ctrl+C to terminate the session...")
    try:
        await session.wait_terminated()
    except KeyboardInterrupt:
        print("\n\nTerminating session...")
        await session.terminate()
        await session.wait_terminated()

    print("✓ Session terminated")


if __name__ == "__main__":
    asyncio.run(main())
