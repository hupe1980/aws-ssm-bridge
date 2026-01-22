//! Metrics and observability hooks for production monitoring.
//!
//! This module provides a trait-based metrics system that allows users to
//! plug in their preferred metrics backend (Prometheus, OpenTelemetry, etc.)
//! without adding heavy dependencies to the core library.
//!
//! # Design Philosophy
//!
//! - **Zero-cost when unused**: No metrics overhead if no recorder is registered
//! - **Backend-agnostic**: Works with Prometheus, OpenTelemetry, StatsD, etc.
//! - **Production-ready**: Counters, gauges, histograms for real workloads
//!
//! # Usage
//!
//! ```rust
//! use aws_ssm_bridge::metrics::{MetricsRecorder, register_metrics};
//!
//! // Implement your own recorder
//! struct PrometheusRecorder;
//!
//! impl MetricsRecorder for PrometheusRecorder {
//!     fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
//!         // Push to Prometheus
//!     }
//!     // ... other methods
//! }
//!
//! // Register globally
//! register_metrics(Box::new(PrometheusRecorder));
//! ```

use std::sync::OnceLock;
use std::time::Duration;

/// Global metrics recorder (set once at startup)
static METRICS: OnceLock<Box<dyn MetricsRecorder>> = OnceLock::new();

/// Trait for metrics recording backends.
///
/// Implement this trait to integrate with your metrics system.
/// All methods have no-op default implementations for convenience.
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Increment a counter by the given value.
    ///
    /// Counters are monotonically increasing values (e.g., total requests).
    fn increment_counter(&self, _name: &str, _value: u64, _labels: &[(&str, &str)]) {}

    /// Set a gauge to a specific value.
    ///
    /// Gauges represent point-in-time values (e.g., active connections).
    fn set_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}

    /// Record a value in a histogram.
    ///
    /// Histograms track distributions (e.g., request latencies).
    fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}

    /// Record a timing duration.
    ///
    /// Convenience method that converts Duration to seconds for histograms.
    fn record_timing(&self, name: &str, duration: Duration, labels: &[(&str, &str)]) {
        self.record_histogram(name, duration.as_secs_f64(), labels);
    }
}

/// Register a global metrics recorder.
///
/// Should be called once at application startup. Subsequent calls are ignored.
///
/// # Example
///
/// ```rust
/// use aws_ssm_bridge::metrics::{MetricsRecorder, register_metrics};
///
/// struct MyMetrics;
/// impl MetricsRecorder for MyMetrics {}
///
/// register_metrics(Box::new(MyMetrics));
/// ```
pub fn register_metrics(recorder: Box<dyn MetricsRecorder>) {
    let _ = METRICS.set(recorder);
}

/// Get the registered metrics recorder, if any.
pub fn get_metrics() -> Option<&'static dyn MetricsRecorder> {
    METRICS.get().map(|b| b.as_ref())
}

// =============================================================================
// Metric Names (constants for consistency)
// =============================================================================

/// Metric name constants for consistent naming across the library.
pub mod names {
    /// Sessions started (counter)
    pub const SESSIONS_STARTED: &str = "ssm_sessions_started_total";
    /// Sessions terminated (counter)
    pub const SESSIONS_TERMINATED: &str = "ssm_sessions_terminated_total";
    /// Session errors (counter)
    pub const SESSION_ERRORS: &str = "ssm_session_errors_total";
    /// Active sessions (gauge)
    pub const ACTIVE_SESSIONS: &str = "ssm_active_sessions";
    /// Messages sent (counter)
    pub const MESSAGES_SENT: &str = "ssm_messages_sent_total";
    /// Messages received (counter)
    pub const MESSAGES_RECEIVED: &str = "ssm_messages_received_total";
    /// Bytes sent (counter)
    pub const BYTES_SENT: &str = "ssm_bytes_sent_total";
    /// Bytes received (counter)
    pub const BYTES_RECEIVED: &str = "ssm_bytes_received_total";
    /// Message retransmissions (counter)
    pub const RETRANSMISSIONS: &str = "ssm_message_retransmissions_total";
    /// ACKs received (counter)
    pub const ACKS_RECEIVED: &str = "ssm_acks_received_total";
    /// RTT in seconds (histogram)
    pub const RTT_SECONDS: &str = "ssm_rtt_seconds";
    /// Connection duration (histogram)
    pub const CONNECTION_DURATION: &str = "ssm_connection_duration_seconds";
    /// Handshake duration (histogram)
    pub const HANDSHAKE_DURATION: &str = "ssm_handshake_duration_seconds";
    /// WebSocket reconnections (counter)
    pub const RECONNECTIONS: &str = "ssm_websocket_reconnections_total";
    /// Connection health status (gauge: 0=unknown, 1=unhealthy, 2=degraded, 3=healthy)
    pub const CONNECTION_HEALTH: &str = "ssm_connection_health";
    /// Packet loss percentage (gauge)
    pub const PACKET_LOSS_PERCENT: &str = "ssm_packet_loss_percent";
    /// RTT jitter in seconds (gauge)
    pub const RTT_JITTER_SECONDS: &str = "ssm_rtt_jitter_seconds";
}

// =============================================================================
// Internal helpers (used by other modules when wired up)
// =============================================================================

/// Increment a counter if metrics are registered.
#[inline]
#[allow(dead_code)]
pub(crate) fn counter(name: &str, value: u64, labels: &[(&str, &str)]) {
    if let Some(m) = get_metrics() {
        m.increment_counter(name, value, labels);
    }
}

/// Set a gauge if metrics are registered.
#[inline]
#[allow(dead_code)]
pub(crate) fn gauge(name: &str, value: f64, labels: &[(&str, &str)]) {
    if let Some(m) = get_metrics() {
        m.set_gauge(name, value, labels);
    }
}

/// Record a histogram value if metrics are registered.
#[inline]
#[allow(dead_code)]
pub(crate) fn histogram(name: &str, value: f64, labels: &[(&str, &str)]) {
    if let Some(m) = get_metrics() {
        m.record_histogram(name, value, labels);
    }
}

/// Record a timing if metrics are registered.
#[inline]
#[allow(dead_code)]
pub(crate) fn timing(name: &str, duration: Duration, labels: &[(&str, &str)]) {
    if let Some(m) = get_metrics() {
        m.record_timing(name, duration, labels);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    struct TestRecorder {
        counter_calls: Arc<AtomicU64>,
    }

    impl MetricsRecorder for TestRecorder {
        fn increment_counter(&self, _name: &str, value: u64, _labels: &[(&str, &str)]) {
            self.counter_calls.fetch_add(value, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_metrics_no_recorder() {
        // Should not panic when no recorder registered
        counter("test", 1, &[]);
        gauge("test", 1.0, &[]);
        histogram("test", 1.0, &[]);
    }

    #[test]
    fn test_metrics_with_recorder() {
        let counter_calls = Arc::new(AtomicU64::new(0));
        let recorder = TestRecorder {
            counter_calls: counter_calls.clone(),
        };

        // Register the recorder
        register_metrics(Box::new(recorder));

        // Call counter
        counter("test_metric", 5, &[("label", "value")]);

        // Verify it was called (note: test isolation not guaranteed with global state)
        // This test mainly verifies the recorder receives calls
        let calls = counter_calls.load(Ordering::Relaxed);
        assert!(
            calls >= 5,
            "Expected at least 5 counter increments, got {}",
            calls
        );
    }

    #[test]
    fn test_metric_names() {
        // Ensure all metric names follow conventions
        assert!(names::SESSIONS_STARTED.starts_with("ssm_"));
        assert!(names::SESSIONS_STARTED.ends_with("_total"));
        assert!(names::RTT_SECONDS.ends_with("_seconds"));
    }
}
