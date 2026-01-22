//! Structured tracing extensions for distributed observability.
//!
//! This module provides tracing spans and instrumentation for:
//! - Session lifecycle events
//! - Message send/receive operations
//! - Connection state changes
//! - Error tracking with context
//!
//! Integrates with OpenTelemetry, Jaeger, Datadog, and other tracing backends.
//!
//! # Example
//!
//! ```rust,no_run
//! use aws_ssm_bridge::tracing_ext::{SessionSpan, span_session};
//! use tracing::info_span;
//!
//! // Create a session span with automatic field population
//! let span = span_session("i-1234567890abcdef0", "sess-abc123");
//! let _guard = span.enter();
//!
//! // All logs within this scope include session context
//! tracing::info!("Processing command");
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{span, Level, Span};

/// Global request ID counter for correlation
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID for tracing correlation.
pub fn generate_request_id() -> String {
    let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("req-{:016x}", id)
}

/// Standard span field names used throughout the library.
pub mod fields {
    /// Session ID field name
    pub const SESSION_ID: &str = "session.id";
    /// Target instance/document field name
    pub const TARGET: &str = "session.target";
    /// Message sequence number
    pub const SEQUENCE_NUM: &str = "msg.seq";
    /// Message type
    pub const MESSAGE_TYPE: &str = "msg.type";
    /// Payload size in bytes
    pub const PAYLOAD_SIZE: &str = "msg.payload_size";
    /// Operation name
    pub const OPERATION: &str = "operation";
    /// Error type
    pub const ERROR_TYPE: &str = "error.type";
    /// Error message
    pub const ERROR_MESSAGE: &str = "error.message";
    /// Request ID for correlation
    pub const REQUEST_ID: &str = "request.id";
    /// RTT measurement
    pub const RTT_MS: &str = "rtt.ms";
    /// Connection state
    pub const CONNECTION_STATE: &str = "connection.state";
}

/// Create a span for session operations.
///
/// This is the primary span that should wrap all session-level operations.
///
/// # Example
///
/// ```rust
/// use aws_ssm_bridge::tracing_ext::span_session;
///
/// let span = span_session("i-1234567890abcdef0", "sess-abc123");
/// let _guard = span.enter();
/// // All operations here are traced under this session
/// ```
#[must_use]
pub fn span_session(target: &str, session_id: &str) -> Span {
    span!(
        Level::INFO,
        "ssm.session",
        { fields::TARGET } = target,
        { fields::SESSION_ID } = session_id,
    )
}

/// Create a span for message send operations.
#[must_use]
pub fn span_send(seq: i64, msg_type: &str, payload_size: usize) -> Span {
    span!(
        Level::DEBUG,
        "ssm.send",
        { fields::SEQUENCE_NUM } = seq,
        { fields::MESSAGE_TYPE } = msg_type,
        { fields::PAYLOAD_SIZE } = payload_size,
    )
}

/// Create a span for message receive operations.
#[must_use]
pub fn span_receive(seq: i64, msg_type: &str, payload_size: usize) -> Span {
    span!(
        Level::DEBUG,
        "ssm.receive",
        { fields::SEQUENCE_NUM } = seq,
        { fields::MESSAGE_TYPE } = msg_type,
        { fields::PAYLOAD_SIZE } = payload_size,
    )
}

/// Create a span for connection operations.
#[must_use]
pub fn span_connection(operation: &str, target: &str) -> Span {
    span!(
        Level::INFO,
        "ssm.connection",
        { fields::OPERATION } = operation,
        { fields::TARGET } = target,
    )
}

/// Create a span for handshake operations.
#[must_use]
pub fn span_handshake(session_id: &str, phase: &str) -> Span {
    span!(
        Level::DEBUG,
        "ssm.handshake",
        { fields::SESSION_ID } = session_id,
        phase = phase,
    )
}

/// Create a span for retransmission operations.
#[must_use]
pub fn span_retransmit(seq: i64, attempt: u32) -> Span {
    span!(
        Level::DEBUG,
        "ssm.retransmit",
        { fields::SEQUENCE_NUM } = seq,
        attempt = attempt,
    )
}

/// Create a span for encryption/decryption operations.
#[must_use]
pub fn span_crypto(operation: &str, algorithm: &str, size: usize) -> Span {
    span!(
        Level::TRACE,
        "ssm.crypto",
        { fields::OPERATION } = operation,
        algorithm = algorithm,
        size = size,
    )
}

/// Create a span for compression operations.
#[must_use]
pub fn span_compression(operation: &str, algorithm: &str, input_size: usize) -> Span {
    span!(
        Level::TRACE,
        "ssm.compression",
        { fields::OPERATION } = operation,
        algorithm = algorithm,
        input_size = input_size,
    )
}

/// Create a span for reconnection attempts.
#[must_use]
pub fn span_reconnect(target: &str, attempt: u32) -> Span {
    span!(
        Level::INFO,
        "ssm.reconnect",
        { fields::TARGET } = target,
        attempt = attempt,
    )
}

/// Create a span for pool operations.
#[must_use]
pub fn span_pool(operation: &str, pool_size: usize) -> Span {
    span!(
        Level::DEBUG,
        "ssm.pool",
        { fields::OPERATION } = operation,
        pool_size = pool_size,
    )
}

/// Trait extension for adding error context to spans.
pub trait SpanExt {
    /// Record an error on the span.
    fn record_error(&self, error: &dyn std::error::Error);

    /// Record success status on the span.
    fn record_ok(&self);

    /// Record RTT measurement.
    fn record_rtt(&self, rtt_ms: f64);
}

impl SpanExt for Span {
    fn record_error(&self, error: &dyn std::error::Error) {
        self.record(fields::ERROR_TYPE, std::any::type_name_of_val(error));
        self.record(
            fields::ERROR_MESSAGE,
            &error.to_string() as &dyn tracing::Value,
        );
    }

    fn record_ok(&self) {
        self.record("status", "ok");
    }

    fn record_rtt(&self, rtt_ms: f64) {
        self.record(fields::RTT_MS, rtt_ms);
    }
}

/// Session span wrapper for automatic lifecycle tracking.
///
/// This struct creates a span on construction and properly closes it on drop.
pub struct SessionSpan {
    span: Span,
    _guard: tracing::span::EnteredSpan,
}

impl SessionSpan {
    /// Create a new session span.
    pub fn new(target: &str, session_id: &str) -> Self {
        let span = span_session(target, session_id);
        let guard = span.clone().entered();
        Self {
            span,
            _guard: guard,
        }
    }

    /// Get a reference to the underlying span.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Record an event on this span.
    pub fn event(&self, message: &str) {
        tracing::info!(parent: &self.span, "{}", message);
    }

    /// Record an error event.
    pub fn error(&self, message: &str) {
        tracing::error!(parent: &self.span, "{}", message);
    }
}

/// Trace context for propagation across service boundaries.
///
/// This can be serialized and passed to remote services for
/// distributed tracing correlation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TraceContext {
    /// Trace ID (128-bit hex)
    pub trace_id: String,
    /// Span ID (64-bit hex)
    pub span_id: String,
    /// Parent span ID (optional)
    pub parent_span_id: Option<String>,
    /// Sampling decision
    pub sampled: bool,
}

impl TraceContext {
    /// Generate a new trace context.
    pub fn new() -> Self {
        Self {
            trace_id: format!("{:032x}", rand::random::<u128>()),
            span_id: format!("{:016x}", rand::random::<u64>()),
            parent_span_id: None,
            sampled: true,
        }
    }

    /// Create a child context from this one.
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: format!("{:016x}", rand::random::<u64>()),
            parent_span_id: Some(self.span_id.clone()),
            sampled: self.sampled,
        }
    }

    /// Convert to W3C Trace Context header format.
    pub fn to_traceparent(&self) -> String {
        let sampled_flag = if self.sampled { "01" } else { "00" };
        format!("00-{}-{}-{}", self.trace_id, self.span_id, sampled_flag)
    }

    /// Parse from W3C Trace Context header.
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }

        Some(Self {
            trace_id: parts[1].to_string(),
            span_id: parts[2].to_string(),
            parent_span_id: None,
            sampled: parts[3] == "01",
        })
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_request_id() {
        let id1 = generate_request_id();
        let id2 = generate_request_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("req-"));
    }

    #[test]
    fn test_span_session_creates() {
        // Just verify spans can be created (they may be disabled without a subscriber)
        let _span = span_session("i-test", "sess-123");
    }

    #[test]
    fn test_span_send_creates() {
        let _span = span_send(42, "output_stream_data", 1024);
    }

    #[test]
    fn test_trace_context_new() {
        let ctx = TraceContext::new();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
        assert!(ctx.sampled);
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.child();

        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.span_id, child.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let ctx = TraceContext::new();
        let header = ctx.to_traceparent();
        let parsed = TraceContext::from_traceparent(&header).unwrap();

        assert_eq!(ctx.trace_id, parsed.trace_id);
        assert_eq!(ctx.span_id, parsed.span_id);
        assert_eq!(ctx.sampled, parsed.sampled);
    }

    #[test]
    fn test_traceparent_format() {
        let ctx = TraceContext {
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            span_id: "0123456789abcdef".to_string(),
            parent_span_id: None,
            sampled: true,
        };

        let header = ctx.to_traceparent();
        assert_eq!(
            header,
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
        );
    }

    #[test]
    fn test_invalid_traceparent() {
        assert!(TraceContext::from_traceparent("invalid").is_none());
        assert!(TraceContext::from_traceparent("01-xxx-yyy-00").is_none()); // Wrong version
    }

    #[test]
    fn test_session_span_wrapper() {
        // Can create session span wrapper without subscriber
        // (won't actually trace but shouldn't panic)
        let span = SessionSpan::new("i-test", "sess-123");
        span.event("test event");
        span.error("test error");
    }

    #[test]
    fn test_all_span_factories() {
        // Ensure all span factories work without panicking
        let _ = span_session("target", "session");
        let _ = span_send(1, "type", 100);
        let _ = span_receive(1, "type", 100);
        let _ = span_connection("connect", "target");
        let _ = span_handshake("session", "request");
        let _ = span_retransmit(1, 1);
        let _ = span_crypto("encrypt", "aes-gcm", 1024);
        let _ = span_compression("compress", "zstd", 1024);
        let _ = span_reconnect("target", 1);
        let _ = span_pool("add", 5);
    }
}
