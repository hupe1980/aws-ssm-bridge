"""
AWS SSM Bridge - Python bindings for AWS Systems Manager Session Manager protocol.

This package provides a high-level Python interface to the aws-ssm-bridge Rust library,
enabling secure, high-performance connections to EC2 instances via AWS Systems Manager.

Example (context manager - recommended):
    >>> import asyncio
    >>> from aws_ssm_bridge import SessionManager
    >>> 
    >>> async def main():
    ...     manager = await SessionManager.new()
    ...     async with await manager.start_session("i-1234567890abcdef0") as session:
    ...         await session.send(b"hostname\\n")
    ...         async for chunk in await session.output():
    ...             print(chunk.decode(), end='')
    ...     # Session automatically terminated
    >>> 
    >>> asyncio.run(main())

Example (manual):
    >>> async def main():
    ...     manager = await SessionManager.new()
    ...     session = await manager.start_session("i-1234567890abcdef0")
    ...     try:
    ...         await session.send(b"hostname\\n")
    ...     finally:
    ...         await session.terminate()

Features:
    - Full AWS SSM binary protocol implementation
    - Async/await API with streaming output
    - Async context manager for automatic cleanup
    - Automatic retry with exponential backoff
    - Secure: zeroized keys, constant-time comparisons
    - High performance: Rust core with zero-copy where possible
"""

from typing import Dict, List, Optional

from ._internal import (
    SessionManager,
    Session,
    SessionConfig,
    SessionType,
    OutputStream,
    InteractiveShell,
    InteractiveConfig,
    run_shell,
    __version__,
)


async def connect(
    target: str,
    region: Optional[str] = None,
    session_type: Optional[str] = None,
    document_name: Optional[str] = None,
    parameters: Optional[Dict[str, List[str]]] = None,
    reason: Optional[str] = None,
) -> Session:
    """
    Convenience function to quickly connect to an instance.
    
    Creates a SessionManager and starts a session in one call.
    For multiple sessions, prefer creating a SessionManager directly.
    
    Args:
        target: Instance ID to connect to (e.g., 'i-1234567890abcdef0')
        region: AWS region (uses default if not specified)
        session_type: Type of session ('standard_stream', 'port', etc.)
        document_name: SSM document for custom sessions
        parameters: Document parameters
        reason: Audit reason for session
    
    Returns:
        Session: Connected session ready for use
    
    Example:
        >>> session = await connect("i-1234567890abcdef0")
        >>> await session.send(b"whoami\\n")
        >>> await session.terminate()
    """
    manager = await SessionManager.new()
    return await manager.start_session(
        target=target,
        region=region,
        session_type=session_type,
        document_name=document_name,
        parameters=parameters,
        reason=reason,
    )


__all__ = [
    "SessionManager",
    "Session", 
    "SessionConfig",
    "SessionType",
    "OutputStream",
    "InteractiveShell",
    "InteractiveConfig",
    "run_shell",
    "connect",
    "__version__",
]
