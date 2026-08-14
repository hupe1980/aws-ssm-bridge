//! Reliable delivery: acknowledgements, retransmission and RTT estimation.
//!
//! The MGS data channel runs over TLS/TCP, so bytes are not lost in transit —
//! but the *agent* may drop a message when its own buffers are full, and it
//! signals successful processing with an `acknowledge` message rather than at
//! the transport layer.  Both directions therefore carry sequence numbers and
//! both sides retransmit.
//!
//! ```text
//! client                                agent
//!   │── input_stream_data seq=0 ────────►│
//!   │◄─ acknowledge       seq=0 ─────────│   RTT sample
//!   │── input_stream_data seq=1 ────────►│
//!   │                (no ack)            │
//!   │   … RTO elapses …                  │
//!   │── input_stream_data seq=1 ────────►│   retransmit
//!   │◄─ acknowledge       seq=1 ─────────│   no RTT sample (Karn's algorithm)
//! ```
//!
//! Inbound messages that arrive out of order are held in
//! [`IncomingBuffer`] until the gap is filled, matching the reference plugin's
//! `IncomingMessageBuffer`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

use crate::binary_protocol::{flags, ClientMessage, MessageType, PayloadType};
use crate::errors::{Error, Result};

/// Initial RTT estimate before any sample is available.
///
/// Matches `config.DefaultRoundTripTime` in the reference plugin.
const INITIAL_RTT: Duration = Duration::from_millis(100);
/// Initial retransmission timeout, used until the first RTT sample lands.
///
/// Matches `config.DefaultTransmissionTimeout`.
const INITIAL_RTO: Duration = Duration::from_millis(200);
/// Lower bound on the retransmission timeout.
const MIN_RTO: Duration = Duration::from_millis(50);
/// Upper bound on the retransmission timeout.
const MAX_RTO: Duration = Duration::from_secs(30);
/// RFC 6298 `K`: how many mean deviations of headroom the RTO carries.
const RTO_VARIANCE_FACTOR: u32 = 4;

// ---------------------------------------------------------------------------
// Acknowledgement payload
// ---------------------------------------------------------------------------

/// JSON body of an `acknowledge` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcknowledgeContent {
    /// Type of the message being acknowledged.
    #[serde(rename = "AcknowledgedMessageType")]
    pub message_type: String,
    /// `MessageId` of the message being acknowledged.
    #[serde(rename = "AcknowledgedMessageId")]
    pub message_id: String,
    /// Sequence number of the message being acknowledged.
    #[serde(rename = "AcknowledgedMessageSequenceNumber")]
    pub sequence_number: i64,
    /// `false` when the message arrived out of order and was buffered.
    #[serde(rename = "IsSequentialMessage")]
    pub is_sequential: bool,
}

/// Build an `acknowledge` message for a received message.
///
/// The reference plugin always sends acknowledgements with `SequenceNumber = 0`
/// and `Flags = SYN | FIN`; the acknowledged sequence number lives in the JSON
/// payload, not the header.
pub fn build_ack(received: &ClientMessage, is_sequential: bool) -> Result<ClientMessage> {
    let content = AcknowledgeContent {
        message_type: received.message_type.as_str().to_owned(),
        message_id: received.message_id.to_string(),
        sequence_number: received.sequence_number,
        is_sequential,
    };
    let payload = serde_json::to_vec(&content)?;

    let mut ack = ClientMessage::new(
        MessageType::Acknowledge,
        0,
        PayloadType::Undefined,
        Bytes::from(payload),
    );
    ack.flags = flags::SYN | flags::FIN;
    Ok(ack)
}

/// Parse the JSON body of an `acknowledge` message.
pub fn parse_ack(message: &ClientMessage) -> Result<AcknowledgeContent> {
    serde_json::from_slice(&message.payload)
        .map_err(|e| Error::protocol(format!("malformed acknowledge payload: {e}")))
}

// ---------------------------------------------------------------------------
// RTT estimation
// ---------------------------------------------------------------------------

/// Smoothed round-trip time estimate and the retransmission timeout derived
/// from it, per the Jacobson/Karels algorithm (RFC 6298).
#[derive(Debug, Clone, Copy)]
pub struct RttEstimate {
    /// Smoothed RTT (`SRTT`).
    pub smoothed: Duration,
    /// Mean deviation of the RTT (`RTTVAR`).
    pub variance: Duration,
    /// Current retransmission timeout (`RTO`).
    pub rto: Duration,
    /// Number of samples folded into the estimate.
    pub samples: u64,
}

impl Default for RttEstimate {
    fn default() -> Self {
        Self {
            smoothed: INITIAL_RTT,
            variance: INITIAL_RTT / 2,
            rto: INITIAL_RTO,
            samples: 0,
        }
    }
}

impl RttEstimate {
    /// Fold in a new RTT sample.
    ///
    /// Per Karn's algorithm the caller must only supply samples from messages
    /// that were transmitted exactly once — a sample taken across a
    /// retransmission cannot be attributed to either send.
    pub fn record(&mut self, sample: Duration) {
        if self.samples == 0 {
            self.smoothed = sample;
            self.variance = sample / 2;
        } else {
            let deviation = self.smoothed.abs_diff(sample);
            // RTTVAR = 3/4·RTTVAR + 1/4·|SRTT − R|
            self.variance = (self.variance * 3 + deviation) / 4;
            // SRTT = 7/8·SRTT + 1/8·R
            self.smoothed = (self.smoothed * 7 + sample) / 8;
        }
        self.samples += 1;
        self.rto = (self.smoothed + self.variance * RTO_VARIANCE_FACTOR).clamp(MIN_RTO, MAX_RTO);
    }
}

// ---------------------------------------------------------------------------
// Outgoing buffer
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Pending {
    wire: Bytes,
    sequence: i64,
    last_sent: Instant,
    attempts: u32,
}

/// What the retransmit scheduler should do on this tick.
#[derive(Debug)]
pub enum RetransmitAction {
    /// Nothing is overdue.
    Idle,
    /// Resend these bytes; the entry's timer has already been reset.
    Resend {
        /// Sequence number of the message being resent, for logging.
        sequence: i64,
        /// The serialized message to write to the socket again.
        wire: Bytes,
    },
    /// The oldest unacknowledged message exhausted its retry budget.  The peer
    /// is not processing our data and the session should be torn down.
    GaveUp {
        /// Sequence number that was never acknowledged.
        sequence: i64,
        /// How many times it was transmitted.
        attempts: u32,
    },
}

/// Messages sent but not yet acknowledged, ordered oldest-first.
///
/// Mirrors the reference plugin's `OutgoingMessageBuffer`: only the head of the
/// queue is ever retransmitted, because the agent processes the stream strictly
/// in order — resending later messages while the head is still missing cannot
/// make progress.
#[derive(Debug)]
pub struct OutgoingBuffer {
    state: Mutex<OutgoingState>,
    capacity: usize,
    max_attempts: u32,
}

#[derive(Debug)]
struct OutgoingState {
    pending: VecDeque<Pending>,
    rtt: RttEstimate,
}

impl OutgoingBuffer {
    /// Create a buffer holding at most `capacity` unacknowledged messages and
    /// retransmitting each at most `max_attempts` times.
    pub fn new(capacity: usize, max_attempts: u32) -> Self {
        Self {
            state: Mutex::new(OutgoingState {
                pending: VecDeque::new(),
                rtt: RttEstimate::default(),
            }),
            capacity,
            max_attempts,
        }
    }

    /// Record a message that has just been handed to the writer.
    ///
    /// Returns `false` when the buffer is full.  Unlike the reference plugin,
    /// which silently discards the oldest entry, we refuse the new message so
    /// the caller learns that delivery can no longer be guaranteed — dropping
    /// the head would abandon a message the agent is still waiting for and
    /// stall the stream forever.
    #[must_use]
    pub fn track(&self, wire: Bytes, sequence: i64) -> bool {
        let mut state = self.lock();
        if state.pending.len() >= self.capacity {
            warn!(
                sequence,
                capacity = self.capacity,
                "outgoing buffer is full; refusing to track another unacknowledged message"
            );
            return false;
        }
        state.pending.push_back(Pending {
            wire,
            sequence,
            last_sent: Instant::now(),
            attempts: 1,
        });
        true
    }

    /// Retire an acknowledged message and update the RTT estimate.
    ///
    /// Returns `true` if the sequence number was pending.  Duplicate or unknown
    /// acknowledgements are ignored.
    pub fn acknowledge(&self, sequence: i64) -> bool {
        let mut state = self.lock();
        let Some(index) = state.pending.iter().position(|p| p.sequence == sequence) else {
            trace!(
                sequence,
                "acknowledge for an unknown or already-retired message"
            );
            return false;
        };

        let entry = state.pending.remove(index).expect("index from position()");
        if entry.attempts == 1 {
            let sample = entry.last_sent.elapsed();
            state.rtt.record(sample);
            trace!(
                sequence,
                rtt_ms = sample.as_millis(),
                rto_ms = state.rtt.rto.as_millis(),
                "acknowledged, RTT updated"
            );
        }
        true
    }

    /// Decide what the retransmit scheduler should do right now.
    ///
    /// On [`RetransmitAction::Resend`] the entry's `last_sent` and `attempts`
    /// have already been advanced, so the caller is obliged to actually send
    /// the returned bytes.
    pub fn poll_retransmit(&self) -> RetransmitAction {
        let mut state = self.lock();
        let rto = state.rtt.rto;
        let Some(head) = state.pending.front_mut() else {
            return RetransmitAction::Idle;
        };
        if head.last_sent.elapsed() <= rto {
            return RetransmitAction::Idle;
        }
        if head.attempts >= self.max_attempts {
            return RetransmitAction::GaveUp {
                sequence: head.sequence,
                attempts: head.attempts,
            };
        }
        head.attempts += 1;
        head.last_sent = Instant::now();
        debug!(
            sequence = head.sequence,
            attempt = head.attempts,
            rto_ms = rto.as_millis(),
            "retransmitting unacknowledged message"
        );
        RetransmitAction::Resend {
            sequence: head.sequence,
            wire: head.wire.clone(),
        }
    }

    /// Number of messages awaiting acknowledgement.
    pub fn len(&self) -> usize {
        self.lock().pending.len()
    }

    /// Whether every sent message has been acknowledged.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current RTT estimate.
    pub fn rtt(&self) -> RttEstimate {
        self.lock().rtt
    }

    /// Drop all pending messages, e.g. when the channel is being re-established.
    pub fn clear(&self) {
        self.lock().pending.clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, OutgoingState> {
        // The critical sections here contain no panicking code and no awaits,
        // so poisoning can only follow an unrelated panic elsewhere; recovering
        // the guard keeps a dying task from cascading into every other task.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ---------------------------------------------------------------------------
// Incoming buffer
// ---------------------------------------------------------------------------

/// Out-of-order inbound messages, keyed by sequence number.
///
/// `BTreeMap` rather than `HashMap`: sequence numbers are attacker-influenced
/// only in the sense that a hostile agent could pick them, and ordered
/// iteration makes the "what are we waiting for" question answerable in logs.
#[derive(Debug)]
pub struct IncomingBuffer {
    messages: Mutex<BTreeMap<i64, ClientMessage>>,
    capacity: usize,
}

impl IncomingBuffer {
    /// Create a buffer holding at most `capacity` out-of-order messages.
    pub fn new(capacity: usize) -> Self {
        Self {
            messages: Mutex::new(BTreeMap::new()),
            capacity,
        }
    }

    /// Store an out-of-order message.
    ///
    /// Returns `false` when the buffer is full, in which case the caller must
    /// **not** acknowledge the message — leaving it unacknowledged is what makes
    /// the agent retransmit it once we have caught up.
    #[must_use]
    pub fn insert(&self, message: ClientMessage) -> bool {
        let mut messages = self.lock();
        if messages.len() >= self.capacity && !messages.contains_key(&message.sequence_number) {
            return false;
        }
        messages.insert(message.sequence_number, message);
        true
    }

    /// Take the message with this sequence number, if it is buffered.
    pub fn take(&self, sequence: i64) -> Option<ClientMessage> {
        self.lock().remove(&sequence)
    }

    /// Number of buffered messages.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the buffer holds no messages.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<i64, ClientMessage>> {
        self.messages.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(sequence: i64) -> ClientMessage {
        ClientMessage::new(
            MessageType::InputStreamData,
            sequence,
            PayloadType::Output,
            Bytes::from_static(b"payload"),
        )
    }

    #[test]
    fn ack_uses_sequence_zero_and_syn_fin() {
        let ack = build_ack(&message(42), true).unwrap();
        assert_eq!(ack.sequence_number, 0);
        assert_eq!(ack.flags, flags::SYN | flags::FIN);
        assert_eq!(ack.message_type, MessageType::Acknowledge);

        let content = parse_ack(&ack).unwrap();
        assert_eq!(content.sequence_number, 42);
        assert!(content.is_sequential);
        assert_eq!(content.message_type, "input_stream_data");
    }

    #[test]
    fn ack_json_uses_the_pascal_case_names_the_agent_expects() {
        let ack = build_ack(&message(1), false).unwrap();
        let json = std::str::from_utf8(&ack.payload).unwrap();
        for key in [
            "AcknowledgedMessageType",
            "AcknowledgedMessageId",
            "AcknowledgedMessageSequenceNumber",
            "IsSequentialMessage",
        ] {
            assert!(json.contains(key), "missing {key} in {json}");
        }
    }

    #[test]
    fn rtt_first_sample_seeds_the_estimate() {
        let mut rtt = RttEstimate::default();
        rtt.record(Duration::from_millis(50));
        assert_eq!(rtt.smoothed, Duration::from_millis(50));
        assert_eq!(rtt.samples, 1);
        assert!(rtt.rto >= MIN_RTO && rtt.rto <= MAX_RTO);
    }

    #[test]
    fn rtt_converges_towards_the_mean() {
        let mut rtt = RttEstimate::default();
        for _ in 0..40 {
            rtt.record(Duration::from_millis(60));
        }
        assert!(
            (55..=65).contains(&rtt.smoothed.as_millis()),
            "srtt drifted to {:?}",
            rtt.smoothed
        );
        // With zero variance the RTO collapses towards SRTT but stays above the floor.
        assert!(rtt.rto >= MIN_RTO);
    }

    #[test]
    fn rto_is_clamped_at_both_ends() {
        let mut fast = RttEstimate::default();
        fast.record(Duration::from_micros(10));
        assert!(fast.rto >= MIN_RTO);

        let mut slow = RttEstimate::default();
        slow.record(Duration::from_secs(120));
        assert!(slow.rto <= MAX_RTO);
    }

    #[test]
    fn outgoing_buffer_retires_acknowledged_messages() {
        let buffer = OutgoingBuffer::new(8, 3);
        assert!(buffer.track(Bytes::from_static(b"a"), 0));
        assert!(buffer.track(Bytes::from_static(b"b"), 1));
        assert_eq!(buffer.len(), 2);

        assert!(buffer.acknowledge(0));
        assert_eq!(buffer.len(), 1);
        // A duplicate acknowledgement is harmless.
        assert!(!buffer.acknowledge(0));
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn outgoing_buffer_refuses_to_overflow() {
        let buffer = OutgoingBuffer::new(2, 3);
        assert!(buffer.track(Bytes::from_static(b"a"), 0));
        assert!(buffer.track(Bytes::from_static(b"b"), 1));
        // Refuse rather than evict: dropping seq 0 would stall the stream.
        assert!(!buffer.track(Bytes::from_static(b"c"), 2));
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn poll_retransmit_is_idle_until_the_rto_elapses() {
        let buffer = OutgoingBuffer::new(8, 3);
        assert!(matches!(buffer.poll_retransmit(), RetransmitAction::Idle));
        assert!(buffer.track(Bytes::from_static(b"a"), 0));
        assert!(matches!(buffer.poll_retransmit(), RetransmitAction::Idle));
    }

    #[test]
    fn poll_retransmit_gives_up_after_the_attempt_budget() {
        let buffer = OutgoingBuffer::new(8, 2);
        assert!(buffer.track(Bytes::from_static(b"a"), 7));

        // Force the head to look overdue without sleeping for a real RTO.
        {
            let mut state = buffer.lock();
            state.pending[0].last_sent = Instant::now() - Duration::from_secs(60);
        }
        match buffer.poll_retransmit() {
            RetransmitAction::Resend { sequence, .. } => assert_eq!(sequence, 7),
            other => panic!("expected a resend, got {other:?}"),
        }

        {
            let mut state = buffer.lock();
            state.pending[0].last_sent = Instant::now() - Duration::from_secs(60);
        }
        match buffer.poll_retransmit() {
            RetransmitAction::GaveUp { sequence, attempts } => {
                assert_eq!(sequence, 7);
                assert_eq!(attempts, 2);
            }
            other => panic!("expected to give up, got {other:?}"),
        }
    }

    /// Karn's algorithm: a retransmitted message must not feed the RTT estimate,
    /// because the acknowledgement cannot be attributed to a specific send.
    #[test]
    fn retransmitted_messages_do_not_pollute_the_rtt_estimate() {
        let buffer = OutgoingBuffer::new(8, 5);
        assert!(buffer.track(Bytes::from_static(b"a"), 0));
        {
            let mut state = buffer.lock();
            state.pending[0].last_sent = Instant::now() - Duration::from_secs(60);
        }
        let _ = buffer.poll_retransmit();

        let before = buffer.rtt();
        assert!(buffer.acknowledge(0));
        let after = buffer.rtt();
        assert_eq!(
            before.samples, after.samples,
            "sample count must not change"
        );
        assert_eq!(before.smoothed, after.smoothed);
    }

    #[test]
    fn incoming_buffer_holds_and_releases_by_sequence() {
        let buffer = IncomingBuffer::new(4);
        assert!(buffer.insert(message(5)));
        assert!(buffer.insert(message(6)));
        assert_eq!(buffer.len(), 2);

        assert!(buffer.take(7).is_none());
        assert_eq!(buffer.take(5).unwrap().sequence_number, 5);
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn incoming_buffer_rejects_new_sequences_when_full() {
        let buffer = IncomingBuffer::new(2);
        assert!(buffer.insert(message(1)));
        assert!(buffer.insert(message(2)));
        assert!(!buffer.insert(message(3)), "must refuse when full");
        // Replacing an already-buffered sequence stays within capacity.
        assert!(buffer.insert(message(2)));
    }
}
