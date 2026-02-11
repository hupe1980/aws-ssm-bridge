//! Acknowledgment and retransmission system with RTT tracking.
//!
//! Implements reliable message delivery over the SSM protocol with:
//! - RTT (Round-Trip Time) measurement
//! - Adaptive timeout calculation
//! - Automatic retransmission of unacknowledged messages
//! - Sequence number tracking
//!
//! # How It Works
//!
//! ```text
//! Client                              Agent
//!   │                                   │
//!   │──── Message (seq=1) ─────────────►│
//!   │                                   │
//!   │◄──── Acknowledge (seq=1) ─────────│  ← RTT measured
//!   │                                   │
//!   │──── Message (seq=2) ─────────────►│
//!   │                                   │
//!   │     [timeout - no ack]            │
//!   │                                   │
//!   │──── Retransmit (seq=2) ──────────►│  ← Automatic retry
//!   │                                   │
//!   │◄──── Acknowledge (seq=2) ─────────│
//! ```

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, instrument, trace, warn};

use crate::binary_protocol::{ClientMessage, PayloadType};
use crate::errors::{Error, ProtocolError, Result};
use crate::protocol::MessageType;

/// Default RTT if no measurements available (100ms)
pub const DEFAULT_RTT_MS: u64 = 100;

/// RTT variation factor for timeout calculation
pub const RTT_VARIATION_FACTOR: f64 = 4.0;

/// Minimum timeout (50ms)
pub const MIN_TIMEOUT_MS: u64 = 50;

/// Maximum timeout (30 seconds)
pub const MAX_TIMEOUT_MS: u64 = 30_000;

/// Maximum retransmission attempts
pub const MAX_RETRANSMIT_ATTEMPTS: u32 = 5;

/// Acknowledgment content matching AWS format
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AcknowledgeContent {
    /// Type of acknowledgment
    #[serde(rename = "AcknowledgedMessageType")]
    pub acknowledged_message_type: String,
    /// ID of acknowledged message
    #[serde(rename = "AcknowledgedMessageId")]
    pub acknowledged_message_id: String,
    /// Sequence number being acknowledged
    #[serde(rename = "AcknowledgedMessageSequenceNumber")]
    pub acknowledged_message_sequence_number: i64,
    /// Whether message was delivered out of order
    #[serde(rename = "IsSequentialMessage")]
    pub is_sequential_message: bool,
}

/// A message pending acknowledgment
#[derive(Debug, Clone)]
pub struct PendingMessage {
    /// Original message
    pub message: ClientMessage,
    /// Serialized message bytes (for retransmission)
    pub serialized: Bytes,
    /// Time the message was sent
    pub sent_at: Instant,
    /// Number of transmission attempts
    pub attempts: u32,
    /// Time of last transmission
    pub last_sent: Instant,
}

/// RTT statistics
#[derive(Debug, Clone)]
pub struct RttStats {
    /// Smoothed RTT (exponential moving average)
    pub srtt: Duration,
    /// RTT variation (mean deviation)
    pub rttvar: Duration,
    /// Current RTO (Retransmission Timeout)
    pub rto: Duration,
    /// Number of RTT samples
    pub sample_count: u64,
    /// Minimum observed RTT
    pub min_rtt: Duration,
    /// Maximum observed RTT
    pub max_rtt: Duration,
}

impl Default for RttStats {
    fn default() -> Self {
        let default_rtt = Duration::from_millis(DEFAULT_RTT_MS);
        Self {
            srtt: default_rtt,
            rttvar: default_rtt / 2,
            rto: Duration::from_millis(DEFAULT_RTT_MS * 3),
            sample_count: 0,
            min_rtt: Duration::from_secs(u64::MAX),
            max_rtt: Duration::ZERO,
        }
    }
}

impl RttStats {
    /// Update RTT statistics with a new sample
    ///
    /// Uses Jacobson/Karels algorithm (RFC 6298)
    pub fn update(&mut self, sample: Duration) {
        self.sample_count += 1;

        if sample < self.min_rtt {
            self.min_rtt = sample;
        }
        if sample > self.max_rtt {
            self.max_rtt = sample;
        }

        if self.sample_count == 1 {
            // First sample
            self.srtt = sample;
            self.rttvar = sample / 2;
        } else {
            // Jacobson/Karels algorithm
            // RTTVAR = (1 - β) * RTTVAR + β * |SRTT - R|
            // SRTT = (1 - α) * SRTT + α * R
            // where α = 1/8 and β = 1/4

            let diff = self.srtt.abs_diff(sample);

            // RTTVAR = 3/4 * RTTVAR + 1/4 * diff
            self.rttvar = (self.rttvar * 3 + diff) / 4;

            // SRTT = 7/8 * SRTT + 1/8 * sample
            self.srtt = (self.srtt * 7 + sample) / 8;
        }

        // RTO = SRTT + max(G, K*RTTVAR)
        // where G is clock granularity (1ms) and K = 4
        let rto_raw = self.srtt + self.rttvar.mul_f64(RTT_VARIATION_FACTOR);

        // Clamp to min/max
        self.rto = rto_raw
            .max(Duration::from_millis(MIN_TIMEOUT_MS))
            .min(Duration::from_millis(MAX_TIMEOUT_MS));
    }

    /// Get current retransmission timeout
    pub fn timeout(&self) -> Duration {
        self.rto
    }
}

/// Acknowledgment tracker
pub struct AckTracker {
    /// Messages pending acknowledgment (keyed by sequence number)
    pending: RwLock<BTreeMap<i64, PendingMessage>>,
    /// RTT statistics
    rtt_stats: RwLock<RttStats>,
    /// Next sequence number to use
    next_sequence: AtomicI64,
    /// Expected incoming sequence number
    expected_sequence: AtomicI64,
    /// Maximum number of pending messages (flow control)
    max_pending: usize,
}

impl AckTracker {
    /// Create a new acknowledgment tracker
    pub fn new(max_pending: usize) -> Self {
        Self {
            pending: RwLock::new(BTreeMap::new()),
            rtt_stats: RwLock::new(RttStats::default()),
            next_sequence: AtomicI64::new(0),
            expected_sequence: AtomicI64::new(0),
            max_pending,
        }
    }

    /// Get the next sequence number
    pub fn next_sequence(&self) -> i64 {
        self.next_sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Get expected incoming sequence number
    pub fn expected_sequence(&self) -> i64 {
        self.expected_sequence.load(Ordering::SeqCst)
    }

    /// Track a sent message
    #[instrument(skip(self, message, serialized))]
    pub async fn track_sent(&self, message: ClientMessage, serialized: Bytes) -> Result<()> {
        let mut pending = self.pending.write().await;

        // Check flow control
        if pending.len() >= self.max_pending {
            warn!(
                pending = pending.len(),
                max = self.max_pending,
                "Flow control: too many pending messages"
            );
            return Err(Error::Protocol(ProtocolError::InvalidMessage(
                "Too many pending messages".to_string(),
            )));
        }

        let seq = message.sequence_number;
        let now = Instant::now();

        pending.insert(
            seq,
            PendingMessage {
                message,
                serialized,
                sent_at: now,
                attempts: 1,
                last_sent: now,
            },
        );

        trace!(sequence = seq, pending = pending.len(), "Tracking message");
        Ok(())
    }

    /// Process an acknowledgment
    #[instrument(skip(self))]
    pub async fn process_ack(&self, ack: &AcknowledgeContent) -> Result<Duration> {
        let seq = ack.acknowledged_message_sequence_number;
        let mut pending = self.pending.write().await;

        if let Some(msg) = pending.remove(&seq) {
            let rtt = msg.sent_at.elapsed();

            // Update RTT stats (only for first transmission)
            if msg.attempts == 1 {
                let mut stats = self.rtt_stats.write().await;
                stats.update(rtt);
                debug!(
                    sequence = seq,
                    rtt_ms = rtt.as_millis(),
                    srtt_ms = stats.srtt.as_millis(),
                    rto_ms = stats.rto.as_millis(),
                    "Acknowledgment received"
                );
            }

            Ok(rtt)
        } else {
            trace!(sequence = seq, "Ack for unknown or already-acked message");
            Ok(Duration::ZERO)
        }
    }

    /// Update expected sequence number for incoming messages
    pub fn update_expected_sequence(&self, received_seq: i64) {
        let expected = self.expected_sequence.load(Ordering::SeqCst);
        if received_seq == expected {
            self.expected_sequence.store(expected + 1, Ordering::SeqCst);
        }
    }

    /// Get messages that need retransmission
    #[instrument(skip(self))]
    pub async fn get_retransmit_candidates(&self) -> Vec<(i64, Bytes)> {
        let stats = self.rtt_stats.read().await;
        let timeout = stats.timeout();
        drop(stats);

        let mut pending = self.pending.write().await;
        let mut candidates = Vec::new();
        let now = Instant::now();

        for (seq, msg) in pending.iter_mut() {
            if now.duration_since(msg.last_sent) >= timeout {
                if msg.attempts >= MAX_RETRANSMIT_ATTEMPTS {
                    warn!(
                        sequence = seq,
                        attempts = msg.attempts,
                        "Max retransmit attempts reached"
                    );
                    continue;
                }

                msg.attempts += 1;
                msg.last_sent = now;
                candidates.push((*seq, msg.serialized.clone()));

                debug!(
                    sequence = seq,
                    attempt = msg.attempts,
                    timeout_ms = timeout.as_millis(),
                    "Retransmitting message"
                );
            }
        }

        candidates
    }

    /// Remove messages that exceeded max retries
    pub async fn prune_failed(&self) -> Vec<i64> {
        let mut pending = self.pending.write().await;
        let mut failed = Vec::new();

        pending.retain(|seq, msg| {
            if msg.attempts >= MAX_RETRANSMIT_ATTEMPTS {
                failed.push(*seq);
                false
            } else {
                true
            }
        });

        failed
    }

    /// Get current RTT statistics
    pub async fn rtt_stats(&self) -> RttStats {
        self.rtt_stats.read().await.clone()
    }

    /// Get number of pending messages
    pub async fn pending_count(&self) -> usize {
        self.pending.read().await.len()
    }

    /// Create an acknowledgment message for a received message
    ///
    /// Per AWS reference implementation (messageparser.go:486-515):
    /// - ACK SequenceNumber is always 0
    /// - ACK Flags is always 3 (SYN | FIN)
    pub fn create_ack(message: &ClientMessage, is_sequential: bool) -> Result<ClientMessage> {
        use crate::binary_protocol::flags;

        let ack_content = AcknowledgeContent {
            acknowledged_message_type: message.message_type.as_str().to_string(),
            acknowledged_message_id: message.message_id.to_string(),
            acknowledged_message_sequence_number: message.sequence_number,
            is_sequential_message: is_sequential,
        };

        let payload = serde_json::to_vec(&ack_content).map_err(|e| {
            Error::Protocol(ProtocolError::Framing(format!(
                "Failed to serialize ack: {}",
                e
            )))
        })?;

        // AWS reference: ACK uses SequenceNumber=0 and Flags=3 (SYN|FIN)
        let mut ack_msg = ClientMessage::new(
            MessageType::Acknowledge,
            0,                      // ACK sequence is always 0 per AWS reference
            PayloadType::Undefined, // ACK doesn't have a meaningful payload type
            Bytes::from(payload),
        );
        ack_msg.flags = flags::SYN | flags::FIN; // Flags = 3 per AWS reference

        Ok(ack_msg)
    }

    /// Parse acknowledgment from message payload
    pub fn parse_ack(message: &ClientMessage) -> Result<AcknowledgeContent> {
        serde_json::from_slice(&message.payload).map_err(|e| {
            Error::Protocol(ProtocolError::Framing(format!(
                "Failed to parse ack: {}",
                e
            )))
        })
    }
}

/// Type alias for async send function used by ReliableSender
type AsyncSendFn = Box<
    dyn Fn(Bytes) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Reliable message sender that handles acks and retransmits
pub struct ReliableSender {
    ack_tracker: Arc<AckTracker>,
    /// Channel to send messages through
    send_fn: AsyncSendFn,
}

impl ReliableSender {
    /// Create a new reliable sender
    pub fn new<F, Fut>(ack_tracker: Arc<AckTracker>, send_fn: F) -> Self
    where
        F: Fn(Bytes) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            ack_tracker,
            send_fn: Box::new(move |data| Box::pin(send_fn(data))),
        }
    }

    /// Send a message reliably (with ack tracking)
    pub async fn send(&self, message: ClientMessage) -> Result<()> {
        let serialized = message.serialize()?;
        self.ack_tracker
            .track_sent(message, serialized.clone())
            .await?;
        (self.send_fn)(serialized).await
    }

    /// Run retransmission loop (should be spawned as a task)
    pub async fn retransmit_loop(&self, interval: Duration) {
        let mut timer = tokio::time::interval(interval);

        loop {
            timer.tick().await;

            let candidates = self.ack_tracker.get_retransmit_candidates().await;
            for (seq, data) in candidates {
                if let Err(e) = (self.send_fn)(data).await {
                    warn!(sequence = seq, error = ?e, "Retransmit failed");
                }
            }

            // Prune failed messages
            let failed = self.ack_tracker.prune_failed().await;
            if !failed.is_empty() {
                warn!(failed = ?failed, "Messages failed after max retries");
            }
        }
    }
}

/// Buffer for out-of-order incoming messages
///
/// Matches AWS IncomingMessageBuffer (capacity-limited).
/// Uses `BTreeMap` for consistent ordered iteration and defense against HashDoS.
/// Messages are stored when seq > expected and processed when gaps are filled.
pub struct IncomingMessageBuffer {
    /// Messages stored by sequence number (ordered)
    messages: RwLock<std::collections::BTreeMap<i64, BufferedMessage>>,
    /// Maximum buffer capacity
    capacity: usize,
}

/// A buffered incoming message
#[derive(Debug, Clone)]
pub struct BufferedMessage {
    /// Raw message bytes
    pub raw: Bytes,
    /// Parsed message
    pub message: ClientMessage,
    /// Time received
    pub received_at: Instant,
}

impl IncomingMessageBuffer {
    /// Create a new incoming message buffer
    pub fn new(capacity: usize) -> Self {
        Self {
            messages: RwLock::new(std::collections::BTreeMap::new()),
            capacity,
        }
    }

    /// Check if buffer has capacity for a new message
    pub async fn has_capacity(&self) -> bool {
        self.messages.read().await.len() < self.capacity
    }

    /// Add a message to the buffer
    ///
    /// Returns true if added, false if no capacity
    pub async fn add(&self, message: ClientMessage, raw: Bytes) -> bool {
        let mut messages = self.messages.write().await;

        if messages.len() >= self.capacity {
            debug!(
                capacity = self.capacity,
                size = messages.len(),
                seq = message.sequence_number,
                "IncomingMessageBuffer full, dropping out-of-order message"
            );
            return false;
        }

        let seq = message.sequence_number;
        messages.insert(
            seq,
            BufferedMessage {
                raw,
                message,
                received_at: Instant::now(),
            },
        );

        debug!(
            seq,
            buffer_size = messages.len(),
            "Added to IncomingMessageBuffer"
        );
        true
    }

    /// Try to get a message at the expected sequence number
    pub async fn get(&self, seq: i64) -> Option<BufferedMessage> {
        self.messages.read().await.get(&seq).cloned()
    }

    /// Remove a message from the buffer
    pub async fn remove(&self, seq: i64) -> Option<BufferedMessage> {
        self.messages.write().await.remove(&seq)
    }

    /// Check if buffer contains a specific sequence
    pub async fn contains(&self, seq: i64) -> bool {
        self.messages.read().await.contains_key(&seq)
    }

    /// Get current buffer size
    pub async fn len(&self) -> usize {
        self.messages.read().await.len()
    }

    /// Check if buffer is empty
    pub async fn is_empty(&self) -> bool {
        self.messages.read().await.is_empty()
    }

    /// Get all buffered sequence numbers (for debugging)
    pub async fn buffered_sequences(&self) -> Vec<i64> {
        self.messages.read().await.keys().copied().collect()
    }
}

/// Outgoing message buffer for reliable delivery
///
/// Matches AWS OutgoingMessageBuffer (LinkedList-based, capacity-limited)
/// Messages are stored after sending and removed when ACKed.
/// Used for retransmission of unACKed messages.
pub struct OutgoingMessageBuffer {
    /// Messages stored in send order (front = oldest)
    messages: RwLock<std::collections::VecDeque<StreamingMessage>>,
    /// Maximum buffer capacity (AWS default: 10,000)
    capacity: usize,
    /// RTT statistics for adaptive timeout
    rtt_stats: RwLock<RttStats>,
    /// Retransmission timeout (adaptive based on RTT)
    retransmission_timeout: RwLock<Duration>,
}

/// A message in the outgoing buffer awaiting ACK
#[derive(Debug, Clone)]
pub struct StreamingMessage {
    /// Raw serialized message bytes
    pub content: Bytes,
    /// Sequence number
    pub sequence_number: i64,
    /// Time last sent
    pub last_sent_time: Instant,
    /// Number of resend attempts
    pub resend_attempt: u32,
}

impl OutgoingMessageBuffer {
    /// Create a new outgoing message buffer
    pub fn new(capacity: usize) -> Self {
        Self {
            messages: RwLock::new(std::collections::VecDeque::with_capacity(
                capacity.min(1000),
            )),
            capacity,
            rtt_stats: RwLock::new(RttStats::default()),
            retransmission_timeout: RwLock::new(Duration::from_millis(DEFAULT_RTT_MS * 3)),
        }
    }

    /// Add a message to the buffer after sending
    ///
    /// If buffer is at capacity, oldest message is dropped (AWS behavior)
    pub async fn add(&self, content: Bytes, sequence_number: i64) {
        let mut messages = self.messages.write().await;

        // If at capacity, remove oldest message (front)
        if messages.len() >= self.capacity {
            if let Some(dropped) = messages.pop_front() {
                warn!(
                    seq = dropped.sequence_number,
                    "OutgoingMessageBuffer full, dropping oldest unACKed message"
                );
            }
        }

        messages.push_back(StreamingMessage {
            content,
            sequence_number,
            last_sent_time: Instant::now(),
            resend_attempt: 0,
        });

        trace!(
            seq = sequence_number,
            buffer_size = messages.len(),
            "Added to OutgoingMessageBuffer"
        );
    }

    /// Process an ACK by removing the message and updating RTT
    ///
    /// Returns true if message was found and removed
    pub async fn process_ack(&self, sequence_number: i64) -> bool {
        let mut messages = self.messages.write().await;

        // Find and remove the message
        if let Some(pos) = messages
            .iter()
            .position(|m| m.sequence_number == sequence_number)
        {
            let msg = messages
                .remove(pos)
                .expect("index was just found via position()");

            // Calculate RTT and update timeout (only if this was first transmission)
            if msg.resend_attempt == 0 {
                let rtt = msg.last_sent_time.elapsed();
                drop(messages); // Release lock before updating RTT

                self.update_rtt(rtt).await;

                debug!(
                    seq = sequence_number,
                    rtt_ms = rtt.as_millis(),
                    "ACK processed, RTT measured"
                );
            } else {
                debug!(
                    seq = sequence_number,
                    resend_attempts = msg.resend_attempt,
                    "ACK processed for retransmitted message"
                );
            }

            true
        } else {
            trace!(seq = sequence_number, "ACK for unknown sequence number");
            false
        }
    }

    /// Update RTT statistics and recalculate retransmission timeout
    async fn update_rtt(&self, rtt: Duration) {
        let mut stats = self.rtt_stats.write().await;
        stats.update(rtt);

        let new_timeout = stats.timeout();
        drop(stats);

        let mut timeout = self.retransmission_timeout.write().await;
        *timeout = new_timeout;
    }

    /// Get messages that need retransmission
    ///
    /// Returns messages where time since last_sent > retransmission_timeout
    /// Updates last_sent_time and resend_attempt for returned messages
    pub async fn get_retransmit_candidates(&self, max_attempts: u32) -> Vec<(i64, Bytes, bool)> {
        let timeout = *self.retransmission_timeout.read().await;
        let mut messages = self.messages.write().await;
        let now = Instant::now();

        let mut candidates = Vec::new();
        let mut timed_out = false;

        // Only check front of buffer (oldest message) per AWS behavior
        if let Some(msg) = messages.front_mut() {
            if now.duration_since(msg.last_sent_time) > timeout {
                if msg.resend_attempt >= max_attempts {
                    warn!(
                        seq = msg.sequence_number,
                        attempts = msg.resend_attempt,
                        "Max retransmit attempts reached"
                    );
                    timed_out = true;
                } else {
                    msg.resend_attempt += 1;
                    msg.last_sent_time = now;

                    debug!(
                        seq = msg.sequence_number,
                        attempt = msg.resend_attempt,
                        timeout_ms = timeout.as_millis(),
                        "Retransmitting message"
                    );

                    candidates.push((msg.sequence_number, msg.content.clone(), false));
                }
            }
        }

        if timed_out {
            // Signal timeout by returning empty with special marker
            candidates.push((-1, Bytes::new(), true));
        }

        candidates
    }

    /// Get current buffer size
    pub async fn len(&self) -> usize {
        self.messages.read().await.len()
    }

    /// Check if buffer is empty
    pub async fn is_empty(&self) -> bool {
        self.messages.read().await.is_empty()
    }

    /// Get current retransmission timeout
    pub async fn retransmission_timeout(&self) -> Duration {
        *self.retransmission_timeout.read().await
    }

    /// Get RTT statistics
    pub async fn rtt_stats(&self) -> RttStats {
        self.rtt_stats.read().await.clone()
    }

    /// Get last measured RTT (smoothed)
    pub async fn last_rtt(&self) -> Option<Duration> {
        let stats = self.rtt_stats.read().await;
        if stats.sample_count > 0 {
            Some(stats.srtt)
        } else {
            None
        }
    }

    /// Clear all messages (e.g., on reconnect)
    pub async fn clear(&self) {
        self.messages.write().await.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtt_stats_initial() {
        let stats = RttStats::default();
        assert_eq!(stats.sample_count, 0);
        assert!(stats.rto >= Duration::from_millis(MIN_TIMEOUT_MS));
    }

    #[test]
    fn test_rtt_stats_first_sample() {
        let mut stats = RttStats::default();
        let sample = Duration::from_millis(50);

        stats.update(sample);

        assert_eq!(stats.sample_count, 1);
        assert_eq!(stats.srtt, sample);
        assert_eq!(stats.min_rtt, sample);
        assert_eq!(stats.max_rtt, sample);
    }

    #[test]
    fn test_rtt_stats_multiple_samples() {
        let mut stats = RttStats::default();

        // Simulate varying RTT
        let samples = [50, 60, 55, 70, 45, 65, 50, 55, 60, 52];

        for sample_ms in samples {
            stats.update(Duration::from_millis(sample_ms));
        }

        assert_eq!(stats.sample_count, 10);
        assert!(stats.min_rtt == Duration::from_millis(45));
        assert!(stats.max_rtt == Duration::from_millis(70));

        // SRTT should be close to mean (~56ms)
        let srtt_ms = stats.srtt.as_millis();
        assert!((50..=65).contains(&srtt_ms));
    }

    #[test]
    fn test_rtt_stats_timeout_bounds() {
        let mut stats = RttStats::default();

        // Very small RTT should still have minimum timeout
        stats.update(Duration::from_micros(100));
        assert!(stats.rto >= Duration::from_millis(MIN_TIMEOUT_MS));

        // Reset and try very large RTT
        let mut stats2 = RttStats::default();
        stats2.update(Duration::from_secs(60));
        assert!(stats2.rto <= Duration::from_millis(MAX_TIMEOUT_MS));
    }

    #[tokio::test]
    async fn test_ack_tracker_basic() {
        let tracker = AckTracker::new(100);

        // Get sequence numbers
        assert_eq!(tracker.next_sequence(), 0);
        assert_eq!(tracker.next_sequence(), 1);
        assert_eq!(tracker.next_sequence(), 2);

        // Track a message
        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            0,
            PayloadType::Output,
            Bytes::from("test"),
        );
        let serialized = msg.serialize().unwrap();

        tracker.track_sent(msg, serialized).await.unwrap();
        assert_eq!(tracker.pending_count().await, 1);
    }

    #[tokio::test]
    async fn test_ack_tracker_process_ack() {
        let tracker = AckTracker::new(100);

        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            42,
            PayloadType::Output,
            Bytes::from("test"),
        );
        let serialized = msg.serialize().unwrap();

        tracker.track_sent(msg, serialized).await.unwrap();

        // Process ack
        let ack = AcknowledgeContent {
            acknowledged_message_type: "input_stream_data".to_string(),
            acknowledged_message_id: "test-id".to_string(),
            acknowledged_message_sequence_number: 42,
            is_sequential_message: true,
        };

        let rtt = tracker.process_ack(&ack).await.unwrap();
        assert!(rtt >= Duration::ZERO); // Time passed

        assert_eq!(tracker.pending_count().await, 0);
    }

    #[test]
    fn test_ack_content_serialization() {
        let ack = AcknowledgeContent {
            acknowledged_message_type: "input_stream_data".to_string(),
            acknowledged_message_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            acknowledged_message_sequence_number: 42,
            is_sequential_message: true,
        };

        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("AcknowledgedMessageType"));
        assert!(json.contains("AcknowledgedMessageSequenceNumber"));

        let parsed: AcknowledgeContent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.acknowledged_message_sequence_number, 42);
    }
}
