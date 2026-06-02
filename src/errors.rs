//! Error types for aws-ssm-bridge

use std::fmt;

/// Result type alias for aws-ssm-bridge operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the library
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// AWS SDK errors
    #[error("AWS SDK error: {0}")]
    AwsSdk(String),

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
    /// Check if error is retriable
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::Timeout => true,
            Error::Transport(TransportError::HeartbeatTimeout) => true,
            Error::Transport(TransportError::ConnectionFailed(_)) => true,
            Error::Transport(TransportError::WebSocket(_)) => true,
            // Only retry *transient* AWS errors — permanent failures (AccessDenied,
            // InvalidInstanceId, TargetNotConnected) must propagate immediately.
            Error::AwsSdk(msg) => {
                msg.contains("ThrottlingException")
                    || msg.contains("ServiceUnavailableException")
                    || msg.contains("InternalServerError")
                    || msg.contains("InternalFailure")
                    || msg.contains("ServiceUnavailable")
                    || msg.contains("RequestTimeout")
                    || msg.contains("Throttling")
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
            Error::Transport(TransportError::Channel(_)) => true,
            Error::Transport(TransportError::WebSocket(msg)) => {
                msg.contains("closed") || msg.contains("closing")
            }
            _ => false,
        }
    }
}

// Implement conversion from AWS SDK errors
impl<E, R> From<aws_smithy_runtime_api::client::result::SdkError<E, R>> for Error
where
    E: fmt::Debug,
    R: fmt::Debug,
{
    fn from(err: aws_smithy_runtime_api::client::result::SdkError<E, R>) -> Self {
        Error::AwsSdk(format!("{:?}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_retriable() {
        // Retriable errors
        assert!(Error::Timeout.is_retriable());
        assert!(Error::Transport(TransportError::HeartbeatTimeout).is_retriable());
        assert!(Error::Transport(TransportError::ConnectionFailed("test".into())).is_retriable());
        assert!(Error::Transport(TransportError::WebSocket("test".into())).is_retriable());
        assert!(Error::AwsSdk("ThrottlingException: request rate exceeded".into()).is_retriable());

        // Permanent AWS errors — must NOT be retried
        assert!(!Error::AwsSdk("AccessDeniedException: ...".into()).is_retriable());
        assert!(!Error::AwsSdk("InvalidInstanceId: ...".into()).is_retriable());
        assert!(!Error::AwsSdk("TargetNotConnected: ...".into()).is_retriable());

        // Transient AWS errors — must be retried
        assert!(Error::AwsSdk("ThrottlingException: ...".into()).is_retriable());
        assert!(Error::AwsSdk("ServiceUnavailableException: ...".into()).is_retriable());
        assert!(Error::AwsSdk("InternalServerError: ...".into()).is_retriable());

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
        assert!(!Error::AwsSdk("error".into()).is_fatal());
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
