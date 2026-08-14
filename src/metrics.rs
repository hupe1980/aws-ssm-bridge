//! Optional metrics hooks.
//!
//! The crate emits counters and histograms through a global recorder you
//! install once at startup. With no recorder installed every call is a single
//! relaxed atomic load and a branch, so leaving metrics off costs nothing.
//!
//! ```
//! use aws_ssm_bridge::metrics::{register, MetricsRecorder};
//!
//! struct Prometheus;
//!
//! impl MetricsRecorder for Prometheus {
//!     fn counter(&self, name: &str, value: u64) {
//!         // metrics::counter!(name).increment(value)
//!         let _ = (name, value);
//!     }
//!     fn histogram(&self, name: &str, value: f64) {
//!         let _ = (name, value);
//!     }
//! }
//!
//! let _ = register(Box::new(Prometheus));
//! ```
//!
//! For tracing rather than aggregate metrics, install a `tracing` subscriber:
//! every module here emits structured spans and events.

use std::sync::OnceLock;

static RECORDER: OnceLock<Box<dyn MetricsRecorder>> = OnceLock::new();

/// Receives the crate's metrics.
///
/// Both methods default to no-ops, so implementers only override what they use.
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Add `value` to a monotonically increasing counter.
    fn counter(&self, _name: &str, _value: u64) {}

    /// Record one observation in a distribution.
    fn histogram(&self, _name: &str, _value: f64) {}
}

/// Install the process-wide recorder.
///
/// # Errors
///
/// Returns the rejected recorder if one was already installed. Registration is
/// deliberately one-shot: swapping recorders mid-flight would silently split a
/// counter series across two backends.
pub fn register(recorder: Box<dyn MetricsRecorder>) -> Result<(), Box<dyn MetricsRecorder>> {
    RECORDER.set(recorder)
}

/// Metric names emitted by this crate.
///
/// Names follow Prometheus conventions: `_total` for counters, a base unit
/// suffix for histograms.
pub mod names {
    /// Sessions successfully started (counter).
    pub const SESSIONS_STARTED: &str = "ssm_sessions_started_total";
    /// Sessions that ended, for any reason (counter).
    pub const SESSIONS_ENDED: &str = "ssm_sessions_ended_total";
    /// Protocol messages sent (counter).
    pub const MESSAGES_SENT: &str = "ssm_messages_sent_total";
    /// Protocol messages received (counter).
    pub const MESSAGES_RECEIVED: &str = "ssm_messages_received_total";
    /// Bytes sent, including protocol headers (counter).
    pub const BYTES_SENT: &str = "ssm_bytes_sent_total";
    /// Bytes received, including protocol headers (counter).
    pub const BYTES_RECEIVED: &str = "ssm_bytes_received_total";
    /// Messages retransmitted after an acknowledgement timeout (counter).
    pub const RETRANSMISSIONS: &str = "ssm_retransmissions_total";
    /// Acknowledgements received from the agent (counter).
    pub const ACKS_RECEIVED: &str = "ssm_acks_received_total";
    /// Smoothed round-trip time, in seconds (histogram).
    pub const RTT_SECONDS: &str = "ssm_rtt_seconds";
    /// Agent handshake duration, in seconds (histogram).
    pub const HANDSHAKE_SECONDS: &str = "ssm_handshake_seconds";
}

#[inline]
pub(crate) fn counter(name: &str, value: u64) {
    if let Some(recorder) = RECORDER.get() {
        recorder.counter(name, value);
    }
}

#[inline]
pub(crate) fn histogram(name: &str, value: f64) {
    if let Some(recorder) = RECORDER.get() {
        recorder.histogram(name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_without_a_registered_recorder_is_a_no_op() {
        counter(names::MESSAGES_SENT, 1);
        histogram(names::RTT_SECONDS, 0.01);
    }

    #[test]
    fn metric_names_follow_prometheus_conventions() {
        for name in [
            names::SESSIONS_STARTED,
            names::SESSIONS_ENDED,
            names::MESSAGES_SENT,
            names::MESSAGES_RECEIVED,
            names::BYTES_SENT,
            names::BYTES_RECEIVED,
            names::RETRANSMISSIONS,
            names::ACKS_RECEIVED,
        ] {
            assert!(name.starts_with("ssm_"), "{name}");
            assert!(name.ends_with("_total"), "{name}");
        }
        for name in [names::RTT_SECONDS, names::HANDSHAKE_SECONDS] {
            assert!(name.ends_with("_seconds"), "{name}");
        }
    }

    #[test]
    fn registration_is_one_shot() {
        struct Noop;
        impl MetricsRecorder for Noop {}

        // Whether this process already has a recorder depends on test ordering,
        // so assert the invariant rather than a specific outcome: after any
        // successful registration, a second attempt must be refused.
        let _ = register(Box::new(Noop));
        assert!(
            register(Box::new(Noop)).is_err(),
            "a second recorder must be refused"
        );
    }
}
