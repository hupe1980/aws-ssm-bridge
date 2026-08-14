//! Error types for `aws-ssm-bridge`.
//!
//! Every fallible operation returns [`Result<T>`], aliasing
//! `std::result::Result<T, Error>`.  [`Error`] is a flat enum: there is one
//! variant per failure domain and no nested error hierarchies to match through.

use std::fmt;

/// Result type alias for `aws-ssm-bridge` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type returned by every fallible operation in this crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An AWS API call failed.
    ///
    /// `code` is the typed service error code (e.g. `"ThrottlingException"`,
    /// `"TargetNotConnected"`) when the SDK provided one.  It is `None` for
    /// transport-level failures (dispatch, timeout) and for errors this crate
    /// constructs from a bare message.
    #[error("AWS API error{}: {message}", .code.as_deref().map(|c| format!(" ({c})")).unwrap_or_default())]
    Aws {
        /// Human-readable description of the failure.
        message: String,
        /// Typed AWS service error code, when available.
        code: Option<String>,
    },

    /// The SSM binary protocol was violated by the remote peer.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The WebSocket transport failed or was closed.
    #[error("transport error: {0}")]
    Transport(String),

    /// The session is not in a state that permits the requested operation.
    #[error("session closed: {0}")]
    SessionClosed(String),

    /// Invalid configuration or arguments supplied by the caller.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// The SSM agent required a feature this client does not implement.
    #[error("unsupported by this client: {0}")]
    Unsupported(String),

    /// Session encryption (KMS / AES-GCM) failed.
    #[error("session encryption error: {0}")]
    Crypto(String),

    /// An underlying I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON (de)serialization failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// The operation did not complete within its deadline.
    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),
}

impl Error {
    /// Construct an [`Error::Aws`] from a message with no typed service code.
    pub(crate) fn aws(message: impl Into<String>) -> Self {
        Error::Aws {
            message: message.into(),
            code: None,
        }
    }

    /// Construct an [`Error::Protocol`].
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Error::Protocol(message.into())
    }

    /// Construct an [`Error::Transport`].
    pub(crate) fn transport(message: impl Into<String>) -> Self {
        Error::Transport(message.into())
    }

    /// Whether retrying the same operation could plausibly succeed.
    ///
    /// Transport failures and *transient* AWS service errors are retriable.
    /// Permanent AWS errors (`AccessDeniedException`, `TargetNotConnected`,
    /// `InvalidInstanceId`, …) are not, and neither are configuration or
    /// protocol errors — retrying those just wastes the caller's time.
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::Timeout(_) | Error::Transport(_) => true,
            Error::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::Interrupted
            ),
            Error::Aws { code, .. } => code.as_deref().is_some_and(is_transient_aws_code),
            _ => false,
        }
    }
}

/// AWS service error codes that indicate a transient condition.
///
/// Sourced from the AWS SDK's standard retry classifier.  Anything not listed
/// is treated as permanent: a client that retries `AccessDeniedException` is
/// broken, not resilient.
fn is_transient_aws_code(code: &str) -> bool {
    matches!(
        code,
        "ThrottlingException"
            | "Throttling"
            | "ThrottledException"
            | "TooManyRequestsException"
            | "RequestThrottled"
            | "RequestThrottledException"
            | "ProvisionedThroughputExceededException"
            | "TransactionInProgressException"
            | "ServiceUnavailable"
            | "ServiceUnavailableException"
            | "InternalServerError"
            | "InternalFailure"
            | "RequestTimeout"
            | "RequestTimeoutException"
    )
}

impl<E, R> From<aws_smithy_runtime_api::client::result::SdkError<E, R>> for Error
where
    E: fmt::Debug + aws_smithy_types::error::metadata::ProvideErrorMetadata,
    R: fmt::Debug,
{
    fn from(err: aws_smithy_runtime_api::client::result::SdkError<E, R>) -> Self {
        use aws_smithy_types::error::metadata::ProvideErrorMetadata;
        let code = err.code().map(str::to_owned);
        // Prefer the service's own message; fall back to the Debug form, which
        // carries dispatch/timeout detail that has no `message` field.
        let message = err
            .message()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{err:?}"));
        Error::Aws { message, code }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aws_code(code: &str) -> Error {
        Error::Aws {
            message: "request failed".into(),
            code: Some(code.into()),
        }
    }

    #[test]
    fn transient_aws_errors_are_retriable() {
        assert!(aws_code("ThrottlingException").is_retriable());
        assert!(aws_code("ServiceUnavailable").is_retriable());
        assert!(aws_code("InternalFailure").is_retriable());
    }

    #[test]
    fn permanent_aws_errors_are_not_retriable() {
        assert!(!aws_code("AccessDeniedException").is_retriable());
        assert!(!aws_code("TargetNotConnected").is_retriable());
        assert!(!aws_code("InvalidInstanceId").is_retriable());
        // No typed code at all: cannot prove it is transient, so do not retry.
        assert!(!Error::aws("no code here").is_retriable());
    }

    #[test]
    fn transport_and_timeout_are_retriable() {
        assert!(Error::transport("reset by peer").is_retriable());
        assert!(Error::Timeout(std::time::Duration::from_secs(1)).is_retriable());
        assert!(!Error::Config("bad target".into()).is_retriable());
        assert!(!Error::protocol("bad digest").is_retriable());
    }

    #[test]
    fn aws_error_display_includes_code() {
        let msg = aws_code("ThrottlingException").to_string();
        assert!(msg.contains("ThrottlingException"), "{msg}");
        assert!(msg.contains("request failed"), "{msg}");
    }
}
