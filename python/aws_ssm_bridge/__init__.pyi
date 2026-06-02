"""
Type stubs for aws_ssm_bridge Python bindings.

This module provides type hints for IDE autocomplete and static type checking.
"""

from typing import AsyncIterator, Dict, List, Optional

__version__: str
"""Package version string."""


def configure_logging(level: str = "warn") -> None:
    """
    Configure logging verbosity for the Rust backend.

    Call this before creating a SessionManager to control log output.
    Note: logging can only be configured once per process.

    Alternatively, set the RUST_LOG environment variable (e.g. RUST_LOG=debug).

    Args:
        level: Log level - "off", "error", "warn", "info", "debug", or "trace".
               Defaults to "warn".
    """
    ...


class SessionType:
    """Session type enumeration for AWS SSM Session Manager."""
    
    STANDARD_STREAM: str
    """Standard interactive shell session."""
    
    PORT: str
    """Port forwarding session (AWS-StartPortForwardingSession)."""
    
    INTERACTIVE_COMMANDS: str
    """Interactive command execution (AWS-StartInteractiveCommand)."""
    
    def __init__(self, session_type: str) -> None:
        """
        Create a SessionType from string.
        
        Args:
            session_type: One of 'standard_stream', 'port', 'interactive_commands'
        
        Raises:
            ValueError: If session_type is not valid
        """
        ...


class SessionConfig:
    """Configuration for an SSM session."""
    
    @property
    def target(self) -> str:
        """Target instance ID (e.g., 'i-1234567890abcdef0')."""
        ...
    
    @property
    def region(self) -> Optional[str]:
        """AWS region (e.g., 'us-east-1')."""
        ...
    
    def __init__(
        self,
        target: str,
        region: Optional[str] = None,
        session_type: Optional[SessionType] = None,
        document_name: Optional[str] = None,
        parameters: Optional[Dict[str, List[str]]] = None,
        reason: Optional[str] = None,
    ) -> None:
        """
        Create session configuration.
        
        Args:
            target: Instance ID to connect to (required)
            region: AWS region (uses default if not specified)
            session_type: Type of session (default: STANDARD_STREAM)
            document_name: SSM document name for custom sessions
            parameters: Document parameters as dict of string lists
            reason: Audit reason for the session
        
        Example:
            >>> config = SessionConfig(
            ...     target="i-1234567890abcdef0",
            ...     region="us-west-2",
            ...     reason="Debugging production issue #123"
            ... )
        """
        ...


class OutputStream:
    """
    Async iterator for session output data.
    
    Yields chunks of bytes from the remote session's stdout/stderr.
    
    Example:
        >>> async for chunk in session.output():
        ...     print(chunk.decode('utf-8'), end='')
    """
    
    def __aiter__(self) -> "OutputStream":
        """Return self as async iterator."""
        ...
    
    async def __anext__(self) -> bytes:
        """
        Get next chunk of output.
        
        Returns:
            bytes: Next chunk of output data
        
        Raises:
            StopAsyncIteration: When stream is exhausted
        """
        ...


class Session:
    """
    An active SSM session.
    
    Represents a connection to a remote EC2 instance via SSM.
    Supports async context manager for automatic cleanup.
    
    Example (context manager - recommended):
        >>> async with await manager.start_session("i-xxx") as session:
        ...     await session.send(b"ls -la\\n")
        ...     async for chunk in session.output():
        ...         print(chunk.decode(), end='')
        ... # Session automatically terminated
    
    Example (manual):
        >>> session = await manager.start_session("i-xxx")
        >>> try:
        ...     await session.send(b"ls -la\\n")
        ... finally:
        ...     await session.terminate()
    """
    
    async def __aenter__(self) -> "Session":
        """
        Async context manager entry.
        
        Waits for the session to be ready before returning.
        """
        ...
    
    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool:
        """
        Async context manager exit.
        
        Terminates the session gracefully.
        """
        ...
    
    @property
    def id(self) -> str:
        """Session ID assigned by AWS (synchronous — no await needed)."""
        ...
    
    async def state(self) -> str:
        """
        Get current session state.
        
        Returns:
            One of: 'initializing', 'connected', 'disconnecting', 'terminated'
        """
        ...
    
    def is_ready(self) -> bool:
        """
        Check if the session is ready to send data (synchronous — no await needed).
        
        The session is ready once the SSM agent has completed the handshake
        and sent the start_publication message.
        
        Returns:
            bool: True if ready to send, False otherwise
        """
        ...
    
    async def wait_for_ready(self, timeout_secs: float = 30.0) -> bool:
        """
        Wait for the session to become ready.
        
        Blocks until the session is ready or timeout expires.
        Call this after start_session() before sending data.
        
        Args:
            timeout_secs: Maximum time to wait in seconds (default: 30)
        
        Returns:
            bool: True if ready, False if timeout expired
        
        Example:
            >>> session = await manager.start_session("i-xxx")
            >>> if await session.wait_for_ready(timeout_secs=10):
            ...     await session.send(b"whoami\\n")
        """
        ...
    
    def output(self) -> OutputStream:
        """
        Get output stream for reading session output.

        This is a synchronous method — no ``await`` needed.
        The returned :class:`OutputStream` is an async iterator.

        Returns:
            OutputStream: Async iterator yielding output bytes

        Example:
            >>> stream = session.output()
            >>> async for chunk in stream:
            ...     print(chunk.decode(), end='')
        """
        ...
    
    async def send(self, data: bytes) -> None:
        """
        Send data to the session (stdin).
        
        Args:
            data: Bytes to send to the remote session
        
        Raises:
            RuntimeError: If session is not in 'connected' state
        
        Example:
            >>> await session.send(b"echo hello\\n")
            >>> await session.send("ls -la\\n".encode())
        """
        ...
    
    async def terminate(self) -> None:
        """
        Terminate the session gracefully.
        
        Sends termination signal and waits for cleanup.
        Call this when done with the session.
        
        Example:
            >>> await session.terminate()
        """
        ...
    
    async def wait_terminated(self) -> None:
        """
        Wait for session to fully terminate.
        
        Blocks until the session reaches 'terminated' state.
        Useful after calling terminate() to ensure cleanup.
        """
        ...


class SessionManager:
    """
    Factory for creating and managing SSM sessions.
    
    Handles AWS authentication and session lifecycle.
    Uses the default AWS credential chain (environment, config files, IAM role).
    
    Example:
        >>> import asyncio
        >>> from aws_ssm_bridge import SessionManager
        >>> 
        >>> async def main():
        ...     manager = await SessionManager.new()
        ...     session = await manager.start_session("i-1234567890abcdef0")
        ...     if await session.wait_for_ready():
        ...         await session.send(b"hostname\\n")
        ...         async for chunk in session.output():
        ...             print(chunk.decode(), end='')
        ...     await session.terminate()
        >>> 
        >>> asyncio.run(main())
    """
    
    @staticmethod
    async def new(region: Optional[str] = None) -> "SessionManager":
        """
        Create a new session manager.
        
        Initializes AWS SDK and loads credentials from the default chain.
        
        Args:
            region: AWS region override. If provided, all sessions created by
                this manager will default to this region. If ``None``, the
                standard AWS region resolution order is used (env vars,
                config files, instance metadata).
        
        Returns:
            SessionManager: Ready to create sessions
        
        Raises:
            RuntimeError: If AWS credentials cannot be loaded
        
        Example:
            >>> manager = await SessionManager.new()
            >>> # or with explicit region:
            >>> manager = await SessionManager.new(region="eu-west-1")
        """
        ...
    
    async def start_session(
        self,
        target: str,
        region: Optional[str] = None,
        session_type: Optional[str] = None,
        document_name: Optional[str] = None,
        parameters: Optional[Dict[str, List[str]]] = None,
        reason: Optional[str] = None,
    ) -> Session:
        """
        Start a new SSM session to a target instance.
        
        Args:
            target: Instance ID (e.g., 'i-1234567890abcdef0')
            region: AWS region (uses default if not specified)
            session_type: One of 'standard_stream', 'port', 'interactive_commands'
            document_name: SSM document for custom sessions
            parameters: Document parameters
            reason: Audit reason for session
        
        Returns:
            Session: Connected session ready for use
        
        Raises:
            RuntimeError: If session creation fails
            ValueError: If target or parameters are invalid
        
        Example:
            >>> session = await manager.start_session(
            ...     target="i-1234567890abcdef0",
            ...     region="us-west-2",
            ...     reason="Investigating disk space issue"
            ... )
        """
        ...
    
    async def start_session_with_config(
        self,
        config: SessionConfig,
    ) -> Session:
        """
        Start a new SSM session from a SessionConfig object.
        
        Prefer this when reusing configurations or when type-safe config
        construction via ``SessionConfig(...)`` is desired.
        
        Args:
            config: Pre-built session configuration
        
        Returns:
            Session: Connected session ready for use
        
        Raises:
            RuntimeError: If session creation fails
            ValueError: If configuration is invalid
        
        Example:
            >>> cfg = SessionConfig("i-1234567890abcdef0", region="us-west-2")
            >>> session = await manager.start_session_with_config(cfg)
        """
        ...
    
    async def terminate_session(self, session_id: str) -> None:
        """
        Terminate a session by its ID.
        
        Args:
            session_id: Session ID to terminate
        
        Raises:
            RuntimeError: If termination fails
        """
        ...


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
        >>> if await session.wait_for_ready():
        ...     await session.send(b"whoami\\n")
        >>> await session.terminate()
    """
    ...


class InteractiveConfig:
    """
    Configuration for interactive shell sessions.
    
    Example:
        >>> config = InteractiveConfig(
        ...     show_banner=True,
        ...     send_initial_size=True,
        ...     forward_signals=True,
        ... )
    """
    
    @property
    def show_banner(self) -> bool:
        """Whether to show session banner on connect."""
        ...
    
    @property
    def send_initial_size(self) -> bool:
        """Whether to send terminal size on connect."""
        ...
    
    @property
    def forward_signals(self) -> bool:
        """Whether to forward Ctrl+C/Z as signals."""
        ...
    
    def __init__(
        self,
        show_banner: bool = True,
        send_initial_size: bool = True,
        forward_signals: bool = True,
    ) -> None:
        """
        Create interactive shell configuration.
        
        Args:
            show_banner: Show session ID banner on connect
            send_initial_size: Send terminal dimensions on connect
            forward_signals: Forward Ctrl+C/Z to remote
        """
        ...
    
    @staticmethod
    def default() -> "InteractiveConfig":
        """Create configuration with default settings."""
        ...


class InteractiveShell:
    """
    Interactive shell session with full terminal support.
    
    Provides a complete interactive shell experience with:
    - Raw terminal mode (no echo, immediate input)
    - Terminal resize detection (SIGWINCH on Unix)
    - Signal forwarding (Ctrl+C, Ctrl+D, Ctrl+Z)
    - Automatic terminal restoration on exit/crash
    
    Example:
        >>> import asyncio
        >>> from aws_ssm_bridge import InteractiveShell, InteractiveConfig
        >>> 
        >>> async def main():
        ...     config = InteractiveConfig.default()
        ...     shell = InteractiveShell(config)
        ...     await shell.connect("i-0123456789abcdef0")
        ...     await shell.run()  # Blocks until Ctrl+D
        >>> 
        >>> asyncio.run(main())
    """
    
    def __init__(self, config: Optional[InteractiveConfig] = None) -> None:
        """
        Create a new interactive shell.
        
        Args:
            config: Shell configuration (uses defaults if not specified)
        
        Raises:
            RuntimeError: If terminal cannot be initialized
        """
        ...
    
    async def connect(self, target: str) -> None:
        """
        Connect to an EC2 instance.
        
        Args:
            target: Instance ID (e.g., "i-0123456789abcdef0")
        
        Raises:
            RuntimeError: If connection fails
        """
        ...
    
    async def run(self) -> None:
        """
        Run the interactive session.
        
        Blocks until:
        - User presses Ctrl+D (EOF)
        - Session is closed by remote
        - An error occurs
        
        Terminal is automatically restored on exit.
        
        Raises:
            RuntimeError: If not connected or session error
        """
        ...
    
    async def is_connected(self) -> bool:
        """Check if connected to an instance."""
        ...


async def run_shell(target: str) -> None:
    """
    Convenience function for quick interactive session.
    
    Creates an InteractiveShell with default config, connects, and runs.
    This is the simplest way to start an interactive shell.
    
    Args:
        target: Instance ID (e.g., "i-0123456789abcdef0")
    
    Example:
        >>> import asyncio
        >>> from aws_ssm_bridge import run_shell
        >>> asyncio.run(run_shell("i-0123456789abcdef0"))
    """
    ...


__all__ = [
    "SessionType",
    "SessionConfig", 
    "Session",
    "SessionManager",
    "OutputStream",
    "InteractiveConfig",
    "InteractiveShell",
    "run_shell",
    "connect",
    "__version__",
]
