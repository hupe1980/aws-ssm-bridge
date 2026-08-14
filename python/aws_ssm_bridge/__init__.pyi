"""Type stubs for aws_ssm_bridge."""

from types import TracebackType
from typing import AsyncIterator, Mapping, Sequence

__version__: str

class SsmError(Exception):
    """Base class for every error this library raises."""

class SsmAwsError(SsmError):
    """An AWS API call failed."""

class SsmProtocolError(SsmError):
    """The peer violated the SSM protocol."""

class SsmTransportError(SsmError):
    """The WebSocket transport failed."""

class SsmClosedError(SsmError):
    """The session has ended and can no longer be used."""

class SsmCryptoError(SsmError):
    """KMS session encryption failed."""

class SsmTimeoutError(SsmError):
    """An operation did not complete in time."""

def configure_logging(level: str = "warn") -> None:
    """Route the library's tracing output to stderr.

    Levels: ``off``, ``error``, ``warn``, ``info``, ``debug``, ``trace``.
    ``RUST_LOG`` still applies and takes precedence.
    """

class OutputStream:
    """An async iterator over a session's output."""

    def __aiter__(self) -> AsyncIterator[bytes]: ...
    async def __anext__(self) -> bytes: ...

class Session:
    """An open SSM session."""

    @property
    def id(self) -> str:
        """The AWS session ID."""

    @property
    def target(self) -> str:
        """The instance, task or ARN this session connects to."""

    @property
    def agent_version(self) -> str | None:
        """SSM agent version, once the handshake has run."""

    @property
    def banner(self) -> str | None:
        """The agent's login banner, if it sent one."""

    @property
    def exit_code(self) -> int | None:
        """Exit status of the remote process, once it has exited."""

    @property
    def is_encrypted(self) -> bool:
        """Whether session data is encrypted end-to-end with a KMS-derived key."""

    @property
    def is_ready(self) -> bool:
        """Whether the agent handshake has completed."""

    @property
    def is_closed(self) -> bool:
        """Whether the session has ended."""

    @property
    def close_reason(self) -> str | None:
        """Why the session ended, or ``None`` while it is still open."""

    async def wait_ready(self) -> None:
        """Wait for the agent handshake to finish."""

    async def wait_closed(self) -> None:
        """Wait for the session to end, for whatever reason."""

    async def send(self, data: bytes) -> None:
        """Send bytes to the remote process's standard input.

        Send ``\\r``, not ``\\n``, for Enter. Large buffers are chunked
        automatically, and data sent before the handshake completes is held
        rather than dropped.
        """

    async def send_terminal_size(self, cols: int, rows: int) -> None:
        """Tell the remote pty the terminal was resized."""

    def output(self) -> OutputStream:
        """Subscribe to the session's output.

        Every subscriber sees the stream from the moment it subscribes; earlier
        output is not replayed. Subscribe before sending.
        """

    async def terminate(self) -> None:
        """End the session and release it on the AWS side. Idempotent."""

    async def __aenter__(self) -> Session: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool: ...

class SessionManager:
    """Starts SSM sessions. Build one and reuse it."""

    @staticmethod
    async def new(region: str | None = None) -> SessionManager:
        """Build a manager, optionally pinned to a region."""

    async def start_session(
        self,
        target: str,
        document_name: str | None = None,
        parameters: Mapping[str, Sequence[str]] | None = None,
        reason: str | None = None,
        ready_timeout: float = 30.0,
    ) -> Session:
        """Start a session.

        ``target`` may be an instance ID, an ``mi-`` managed instance, an
        ``ecs:`` task, or an ARN. Omit ``document_name`` for a plain shell.
        """

    async def start_port_forward(
        self, target: str, remote_port: int, reason: str | None = None
    ) -> Session:
        """Start a session forwarding a port on the instance itself."""

    async def start_remote_port_forward(
        self, target: str, host: str, remote_port: int, reason: str | None = None
    ) -> Session:
        """Start a session forwarding through the instance to ``host``."""

    async def terminate_session(self, session_id: str) -> None:
        """Terminate a session by ID."""

class PortForwarder:
    """Forwards a local TCP port over a port-forwarding session."""

    @staticmethod
    async def bind(local_addr: str = "127.0.0.1:0", max_connections: int = 100) -> PortForwarder:
        """Bind a local address. Port ``0`` lets the OS choose."""

    @property
    def address(self) -> str:
        """The bound address, with the OS-assigned port resolved."""

    @property
    def port(self) -> int:
        """The bound port."""

    async def forward(self, session: Session) -> None:
        """Accept and forward connections until the session ends or ``stop``."""

    def stop(self) -> None:
        """Stop forwarding and release the local port."""

class InteractiveShell:
    """Runs a full interactive shell against the local terminal."""

    def __init__(
        self,
        show_banner: bool = True,
        region: str | None = None,
        reason: str | None = None,
    ) -> None: ...
    async def run(self, target: str) -> int | None:
        """Connect and run until the session ends.

        Returns the remote process's exit code when the agent reported one.
        Raises ``ValueError`` if stdin and stdout are not a terminal.
        """

async def connect(
    target: str, *, region: str | None = None, reason: str | None = None
) -> Session:
    """Open a shell session in one call."""

async def run_command(
    target: str, command: str, *, region: str | None = None, timeout: float = 30.0
) -> str:
    """Run one command without a pty and return its output."""
