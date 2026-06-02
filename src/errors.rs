//! Error types for aws-ssm-bridge

use std::fmt;

/// Result type alias for aws-ssm-bridge operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the library
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// AWS SDK errors.
    ///
    /// `code` is the typed error code from the AWS API (e.g. `"ThrottlingException"`),
    /// extracted via [`ProvideErrorMetadata`][aws_smithy_types::error::metadata::ProvideErrorMetadata].
    /// It is `None` for non-service errors (timeouts, dispatch failures) and for
    /// errors constructed directly from a plain message string.
    #[error("AWS SDK error: {message}")]
    AwsSdk {
        /// Human-readable error details (debug representation of the SDK error).
        message: String,
        /// Typed API error code, if available.
        code: Option<String>,
    },

    /// Session errors
    #[error("Session error: {0}")]
    Session(#[from] SessionError),

    /// Protocol errors
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Transport errors
    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Invalid state error
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Timeout error
    #[error("Operation timed out")]
    Timeout,

    /// Cancelled error
    #[error("Operation was cancelled")]
    Cancelled,
}

/// Session-specific errors
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// Session not found
    #[error("Session not found: {0}")]
    NotFound(String),

    /// Session already exists
    #[error("Session already exists: {0}")]
    AlreadyExists(String),

    /// Session terminated
    #[error("Session terminated: {reason}")]
    Terminated {
        /// The reason for termination
        reason: String,
    },

    /// Invalid session state
    #[error("Invalid session state: expected {expected}, got {actual}")]
    InvalidState {
        /// The expected state
        expected: String,
        /// The actual state
        actual: String,
    },

    /// Session initialization failed
    #[error("Session initialization failed: {0}")]
    InitializationFailed(String),
}

/// Protocol-specific errors
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// Invalid message format
    #[error("Invalid message format: {0}")]
    InvalidMessage(String),

    /// Unknown message type
    #[error("Unknown message type: {0}")]
    UnknownMessageType(String),

    /// Invalid sequence number
    #[error("Invalid sequence number: expected {expected}, got {actual}")]
    InvalidSequence {
        /// The expected sequence number
        expected: u64,
        /// The actual sequence number received
        actual: u64,
    },

    /// Message framing error
    #[error("Message framing error: {0}")]
    Framing(String),

    /// Unsupported protocol version
    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(String),

    /// Checksum mismatch
    #[error("Checksum mismatch")]
    ChecksumMismatch,

    /// Feature required by the remote agent is not implemented in this client.
    ///
    /// This is a hard error: the session cannot continue without the feature.
    /// For example, the SSM agent may mandate KMS session encryption which this
    /// client does not implement.
    #[error("Unsupported feature required by agent: {0}")]
    UnsupportedFeature(String),
}

/// Transport-specific errors
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// WebSocket error
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Connection closed
    #[error("Connection closed: {reason}")]
    ConnectionClosed {
        /// The reason for connection closure
        reason: String,
    },

    /// Connection failed
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Channel error
    #[error("Channel error: {0}")]
    Channel(String),

    /// Heartbeat timeout
    #[error("Heartbeat timeout")]
    HeartbeatTimeout,
}

impl Error {
    /// Construct an `AwsSdk` error from a plain message string with no typed error code.
    ///
    /// Use this for response-validation errors (e.g. missing fields in an API
    /// response) where no SDK `SdkError` is available.  Real SDK errors should
    /// be converted via the `From<SdkError<E, R>>` impl, which preserves the
    /// typed error code for accurate retriability classification.
    pub(crate) fn aws_sdk_msg(message: impl Into<String>) -> Self {
        Error::AwsSdk { message: message.into(), code: None }
    }

    /// Check if error is retriable
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::Timeout => true,
            Error::Transport(TransportError::HeartbeatTimeout) => true,
            Error::Transport(TransportError::ConnectionFailed(_)) => true,
            Error::Transport(TransportError::WebSocket(_)) => true,
            // Only retry *transient* AWS errors — permanent failures (AccessDenied,
            // InvalidInstanceId, TargetNotConnected) must propagate immediately.
            Error::AwsSdk { code, message } => {
                // Primary: typed error code from ProvideErrorMetadata (exact match).
                // This is reliable and version-stable.
                if let Some(code) = code {
                    return matches!(
                        code.as_str(),
                        "ThrottlingException"
                            | "Throttling"
                            | "ThrottledExceptions"
                            | "TooManyRequestsException"
                            | "RequestThrottled"
                            | "RequestThrottledException"
                            | "ProvisionedThroughputExceededException"
                            | "TransactionInProgressException"
                            | "ServiceUnavailableException"
                            | "ServiceUnavailable"
                            | "InternalServerError"
                            | "InternalFailure"
                            | "RequestTimeout"
                            | "RequestTimeoutException"
                    );
                }
                // Fallback: substring match on the debug message for hand-constructed
                // errors that have no typed code (e.g. aws_sdk_msg()).
                let lower = message.to_lowercase();
                lower.contains("throttling")
                    || lower.contains("serviceunavailable")
                    || lower.contains("internalservererror")
                    || lower.contains("internalfailure")
                    || lower.contains("requesttimeout")
            }
            _ => false,
        }
    }

    /// Check if error is fatal (session should be terminated)
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Error::Session(SessionError::Terminated { .. })
                | Error::Transport(TransportError::ConnectionClosed { .. })
                | Error::Session(SessionError::InvalidState { .. })
        )
    }

    /// Check if error indicates the connection is shutting down or already closed.
    ///
    /// Used to downgrade send errors to `debug!` level during graceful shutdown,
    /// avoiding spurious `error!` log lines.
    pub fn is_shutdown_related(&self) -> bool {
        match self {
            Error::Transport(TransportError::ConnectionClosed { .. }) => true,
            // Only treat Channel errors whose message indicates the channel was
            // *closed* (connection shutting down) as shutdown-related.  Other
            // Channel messages (e.g. "lossless subscriber channel full") signal
            // overload conditions and must remain visible as real errors.
            Error::Transport(TransportError::Channel(msg)) => msg.to_lowercase().contains("closed"),
            Error::Transport(TransportError::WebSocket(msg)) => {
                let lower = msg.to_lowercase();
                lower.contains("closed") || lower.contains("closing")
            }
            _ => false,
        }
    }
}

// Implement conversion from AWS SDK errors.
// The `ProvideErrorMetadata` bound lets us extract the typed error code (e.g.
// "ThrottlingException") so `is_retriable()` can do an exact code match instead
// of fragile substring matching on the debug string.
impl<E, R> From<aws_smithy_runtime_api::client::result::SdkError<E, R>> for Error
where
    E: fmt::Debug + aws_smithy_types::error::metadata::ProvideErrorMetadata,
    R: fmt::Debug,
{
    fn from(err: aws_smithy_runtime_api::client::result::SdkError<E, R>) -> Self {
        use aws_smithy_types::error::metadata::ProvideErrorMetadata;
        let code = err.code().map(str::to_owned);
        Error::AwsSdk {
            message: format!("{:?}", err),
            code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: construct an AwsSdk error with a typed error code (simulates a real SDK error).
    fn sdk_code(code: &str) -> Error {
        Error::AwsSdk { message: format!("{code}: request details"), code: Some(code.to_owned()) }
    }

    /// Helper: construct an AwsSdk error with no typed code (simulates a hand-constructed error).
    fn sdk_msg(msg: &str) -> Error {
        Error::AwsSdk { message: msg.to_owned(), code: None }
    }

    #[test]
    fn test_error_is_retriable() {
        // Retriable errors
        assert!(Error::Timeout.is_retriable());
        assert!(Error::Transport(TransportError::HeartbeatTimeout).is_retriable());
        assert!(Error::Transport(TransportError::ConnectionFailed("test".into())).is_retriable());
        assert!(Error::Transport(TransportError::WebSocket("test".into())).is_retriable());

        // Typed code path — permanent AWS errors must NOT be retried
        assert!(!sdk_code("AccessDeniedException").is_retriable());
        assert!(!sdk_code("InvalidInstanceId").is_retriable());
        assert!(!sdk_code("TargetNotConnected").is_retriable());

        // Typed code path — transient AWS errors MUST be retried
        assert!(sdk_code("ThrottlingException").is_retriable());
        assert!(sdk_code("Throttling").is_retriable());
        assert!(sdk_code("TooManyRequestsException").is_retriable());
        assert!(sdk_code("ServiceUnavailableException").is_retriable());
        assert!(sdk_code("InternalServerError").is_retriable());
        assert!(sdk_code("InternalFailure").is_retriable());
        assert!(sdk_code("RequestTimeout").is_retriable());

        // Fallback string path (no typed code) — permanent errors must NOT be retried
        assert!(!sdk_msg("AccessDeniedException: ...").is_retriable());
        assert!(!sdk_msg("InvalidInstanceId: ...").is_retriable());
        assert!(!sdk_msg("TargetNotConnected: ...").is_retriable());

        // Fallback string path — transient errors MUST be retried
        assert!(sdk_msg("ThrottlingException: rate exceeded").is_retriable());
        assert!(sdk_msg("ServiceUnavailableException: service down").is_retriable());
        assert!(sdk_msg("InternalServerError: internal failure").is_retriable());

        // Non-retriable errors
        assert!(!Error::Cancelled.is_retriable());
        assert!(!Error::Config("bad config".into()).is_retriable());
        assert!(!Error::InvalidState("invalid".into()).is_retriable());
        assert!(!Error::Session(SessionError::NotFound("sess".into())).is_retriable());
    }

    #[test]
    fn test_error_is_fatal() {
        // Fatal errors
        let terminated = Error::Session(SessionError::Terminated {
            reason: "test".to_string(),
        });
        assert!(terminated.is_fatal());

        let conn_closed = Error::Transport(TransportError::ConnectionClosed {
            reason: "closed".to_string(),
        });
        assert!(conn_closed.is_fatal());

        let invalid_state = Error::Session(SessionError::InvalidState {
            expected: "Running".into(),
            actual: "Terminated".into(),
        });
        assert!(invalid_state.is_fatal());

        // Non-fatal errors
        assert!(!Error::Timeout.is_fatal());
        assert!(!Error::Cancelled.is_fatal());
        assert!(!sdk_msg("error").is_fatal());
    }

    #[test]
    fn test_error_display() {
        let err = Error::Timeout;
        assert_eq!(format!("{}", err), "Operation timed out");

        let err = Error::Session(SessionError::NotFound("sess-123".into()));
        assert!(format!("{}", err).contains("sess-123"));

        let err = Error::Protocol(ProtocolError::ChecksumMismatch);
        assert!(format!("{}", err).contains("Checksum"));
    }

    #[test]
    fn test_session_error_variants() {
        let err = SessionError::NotFound("sess-1".into());
        assert!(format!("{}", err).contains("sess-1"));

        let err = SessionError::AlreadyExists("sess-2".into());
        assert!(format!("{}", err).contains("sess-2"));

        let err = SessionError::InitializationFailed("handshake failed".into());
        assert!(format!("{}", err).contains("handshake"));
    }

    #[test]
    fn test_protocol_error_variants() {
        let err = ProtocolError::InvalidMessage("bad header".into());
        assert!(format!("{}", err).contains("bad header"));

        let err = ProtocolError::UnknownMessageType("xyz".into());
        assert!(format!("{}", err).contains("xyz"));

        let err = ProtocolError::InvalidSequence {
            expected: 5,
            actual: 3,
        };
        assert!(format!("{}", err).contains("5"));
        assert!(format!("{}", err).contains("3"));

        let err = ProtocolError::Framing("truncated".into());
        assert!(format!("{}", err).contains("truncated"));

        let err = ProtocolError::UnsupportedVersion("2.0".into());
        assert!(format!("{}", err).contains("2.0"));
    }

    #[test]
    fn test_transport_error_variants() {
        let err = TransportError::WebSocket("connection reset".into());
        assert!(format!("{}", err).contains("connection reset"));

        let err = TransportError::ConnectionClosed {
            reason: "EOF".into(),
        };
        assert!(format!("{}", err).contains("EOF"));

        let err = TransportError::Channel("send failed".into());
        assert!(format!("{}", err).contains("send failed"));

        let err = TransportError::HeartbeatTimeout;
        assert!(format!("{}", err).contains("Heartbeat"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
