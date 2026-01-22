#!/usr/bin/env python3
"""
Example: Interactive shell session with AWS SSM

This example demonstrates a true interactive shell session with an EC2 instance,
with proper terminal handling (raw mode, stdin/stdout streaming).

Features:
- Raw terminal mode (no echo, immediate input)
- Terminal resize detection (SIGWINCH on Unix)
- Signal forwarding (Ctrl+C, Ctrl+D, Ctrl+Z)
- Automatic terminal restoration on exit/crash

Usage:
    python interactive_shell.py <instance-id> [region]

Example:
    python interactive_shell.py i-0123456789abcdef0
    python interactive_shell.py i-0123456789abcdef0 us-west-2
"""

import asyncio
import sys

from aws_ssm_bridge import InteractiveShell, InteractiveConfig, run_shell


async def main():
    if len(sys.argv) < 2:
        print("Interactive SSM Shell")
        print()
        print(f"Usage: {sys.argv[0]} <instance-id>")
        print()
        print("Examples:")
        print(f"  {sys.argv[0]} i-0123456789abcdef0")
        print()
        print("Controls:")
        print("  Ctrl+D  - Exit session")
        print("  Ctrl+C  - Send interrupt signal")
        print("  Ctrl+Z  - Suspend current process")
        sys.exit(1)

    instance_id = sys.argv[1]

    # Option 1: Quick one-liner (uses default config)
    # await run_shell(instance_id)

    # Option 2: Full control with custom configuration
    config = InteractiveConfig(
        show_banner=True,         # Show session ID on connect
        send_initial_size=True,   # Send terminal dimensions on connect
        forward_signals=True,     # Forward Ctrl+C/Z to remote
    )

    shell = InteractiveShell(config)

    # Connect to instance
    await shell.connect(instance_id)

    # Run interactive session (blocks until Ctrl+D or session closes)
    # Terminal is automatically restored on exit, even on panic
    await shell.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\033[33mInterrupted.\033[0m")
