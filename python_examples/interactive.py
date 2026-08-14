"""A full interactive shell, equivalent to `aws ssm start-session`.

    python python_examples/interactive.py i-0123456789abcdef0
"""

import asyncio
import sys

from aws_ssm_bridge import InteractiveShell


async def main() -> int:
    shell = InteractiveShell(reason="python interactive example")
    exit_code = await shell.run(sys.argv[1])
    return exit_code or 0


sys.exit(asyncio.run(main()))
