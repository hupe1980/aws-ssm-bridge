//! AWS SSM Session Manager Binary Protocol Implementation
//!
//! This module implements the official AWS SSM binary protocol as specified in
//! github.com/aws/session-manager-plugin (Go implementation).
//!
//! ## Binary Message Format
//!
//! Messages use a 116-byte fixed header followed by variable-length payload:
//!
//! ```text
//! | HL(4) | MessageType(32) | Ver(4) | CD(8) | Seq(8) | Flags(8) |
//! | MessageId(16) | PayloadDigest(32) | PayType(4) | PayLen(4) |
//! | Payload(variable) |
//! ```
//!
//! Total header: 120 bytes (4 + 32 + 4 + 8 + 8 + 8 + 16 + 32 + 4 + 4)
//!
//! ## Field Specifications
//!
//! - **HeaderLength** (4 bytes, u32): Always 116 (size excluding HL field itself)
//! - **MessageType** (32 bytes): UTF-8 string, space-padded
//! - **SchemaVersion** (4 bytes, u32): Protocol version (1)
//! - **CreatedDate** (8 bytes, u64): Unix timestamp in milliseconds
//! - **SequenceNumber** (8 bytes, i64): Message sequence number
//! - **Flags** (8 bytes, u64): Bit flags (SYN=1, FIN=2)
//! - **MessageId** (16 bytes): UUID in binary format (no hyphens)
//! - **PayloadDigest** (32 bytes): SHA-256 hash of payload
//! - **PayloadType** (4 bytes, u32): Payload type enum (1-12)
//! - **PayloadLength** (4 bytes, u32): Length of payload in bytes
//! - **Payload** (variable): Raw payload bytes

use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::errors::{ProtocolError, Result};
use crate::protocol::MessageType;

// Binary protocol constants
const HEADER_LENGTH: u32 = 116; // Excludes the 4-byte HL field
const MESSAGE_TYPE_LENGTH: usize = 32;
const MESSAGE_ID_LENGTH: usize = 16;
const PAYLOAD_DIGEST_LENGTH: usize = 32;
const TOTAL_HEADER_SIZE: usize = 120; // 4 + 116

/// Maximum payload size (10MB) - prevents memory exhaustion attacks
pub const MAX_PAYLOAD_SIZE: u32 = 10 * 1024 * 1024;

// Field offsets within the 120-byte binary header (for reference):
//
//   Offset  Size  Field
//   ------  ----  ------------------
//     0       4   HL (header length = 116)
//     4      32   MessageType
//    36       4   SchemaVersion
//    40       8   CreatedDate (ms since epoch)
//    48       8   SequenceNumber
//    56       8   Flags
//    64      16   MessageId (UUID bytes)
//    80      32   PayloadDigest (SHA-256)
//   112       4   PayloadType
//   116       4   PayloadLength
//   120       …   Payload

/// Payload type enumeration (AWS official specification)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PayloadType {
    /// Undefined/unset payload type (initial messages)
    Undefined = 0,
    /// Standard output (stdout)
    Output = 1,
    /// Error output
    Error = 2,
    /// Terminal size change
    Size = 3,
    /// Session parameters
    Parameter = 4,
    /// Handshake request from agent
    HandshakeRequest = 5,
    /// Handshake response from client
    HandshakeResponse = 6,
    /// Handshake complete notification
    HandshakeComplete = 7,
    /// Encryption challenge request
    EncChallengeRequest = 8,
    /// Encryption challenge response
    EncChallengeResponse = 9,
    /// Control flag
    Flag = 10,
    /// Explicit stderr stream
    StdErr = 11,
    /// Process exit code
    ExitCode = 12,
}

impl PayloadType {
    /// Convert from u32 wire format
    pub fn from_u32(value: u32) -> Result<Self> {
        match value {
            0 => Ok(PayloadType::Undefined),
            1 => Ok(PayloadType::Output),
            2 => Ok(PayloadType::Error),
            3 => Ok(PayloadType::Size),
            4 => Ok(PayloadType::Parameter),
            5 => Ok(PayloadType::HandshakeRequest),
            6 => Ok(PayloadType::HandshakeResponse),
            7 => Ok(PayloadType::HandshakeComplete),
            8 => Ok(PayloadType::EncChallengeRequest),
            9 => Ok(PayloadType::EncChallengeResponse),
            10 => Ok(PayloadType::Flag),
            11 => Ok(PayloadType::StdErr),
            12 => Ok(PayloadType::ExitCode),
            _ => {
                Err(ProtocolError::InvalidMessage(format!("Invalid PayloadType: {}", value)).into())
            }
        }
    }

    /// Convert to u32 wire format
    pub fn to_u32(self) -> u32 {
        self as u32
    }
}

/// Control flags for payload type Flag (type 10)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PayloadTypeFlag {
    /// Disconnect from port
    DisconnectToPort = 1,
    /// Terminate session
    TerminateSession = 2,
    /// Connection to port failed
    ConnectToPortError = 3,
}

/// Message flags (bit flags)
pub mod flags {
    /// First message in stream (SYN)
    pub const SYN: u64 = 1 << 0;
    /// Final message in sequence (FIN)
    pub const FIN: u64 = 1 << 1;
}

/// Binary ClientMessage structure (AWS official format)
///
/// This represents the complete message structure used by AWS SSM Session Manager.
/// All fields are in network byte order (big-endian).
#[derive(Debug, Clone)]
pub struct ClientMessage {
    /// Header length (always 116)
    pub header_length: u32,
    /// Message type (32-byte padded string)
    pub message_type: MessageType,
    /// Schema version (always 1)
    pub schema_version: u32,
    /// Created timestamp (Unix milliseconds)
    pub created_date: u64,
    /// Sequence number for ordering
    pub sequence_number: i64,
    /// Bit flags (SYN, FIN)
    pub flags: u64,
    /// Unique message identifier
    pub message_id: Uuid,
    /// SHA-256 hash of payload
    pub payload_digest: [u8; 32],
    /// Payload type enum
    pub payload_type: PayloadType,
    /// Payload length in bytes
    pub payload_length: u32,
    /// Raw payload bytes
    pub payload: Bytes,
}

impl ClientMessage {
    /// Create a new message with automatic field population
    pub fn new(
        message_type: MessageType,
        sequence_number: i64,
        payload_type: PayloadType,
        payload: Bytes,
    ) -> Self {
        let payload_length = payload.len() as u32;
        let payload_digest = compute_digest(&payload);
        // Use saturating conversion to handle edge case of system time before UNIX epoch
        let created_date = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            header_length: HEADER_LENGTH,
            message_type,
            schema_version: 1,
            created_date,
            sequence_number,
            flags: 0,
            message_id: Uuid::new_v4(),
            payload_digest,
            payload_type,
            payload_length,
            payload,
        }
    }

    /// Serialize to binary format (AWS wire protocol)
    pub fn serialize(&self) -> Result<Bytes> {
        let total_len = TOTAL_HEADER_SIZE + self.payload.len();
        let mut buf = BytesMut::with_capacity(total_len);

        // Header length (4 bytes)
        buf.put_u32(self.header_length);

        // Message type (32 bytes, space-padded)
        buf.put_slice(&message_type_to_padded(&self.message_type));

        // Schema version (4 bytes)
        buf.put_u32(self.schema_version);

        // Created date (8 bytes)
        buf.put_u64(self.created_date);

        // Sequence number (8 bytes)
        buf.put_i64(self.sequence_number);

        // Flags (8 bytes)
        buf.put_u64(self.flags);

        // Message ID (16 bytes, binary UUID)
        // AWS/Java interop: The SSM agent is written in Java, which stores UUIDs as
        // two longs (mostSigBits, leastSigBits). When Java serializes via DataOutputStream,
        // it writes MSB first, then LSB. But the Go plugin's twinj/uuid library expects
        // RFC 4122 byte order. To match the Java agent's wire format, we must swap the
        // two 8-byte halves: write bytes[8..16] first, then bytes[0..8].
        let uuid_bytes = self.message_id.as_bytes();
        buf.put_slice(&uuid_bytes[8..16]); // LSB half first (Java's MSB position)
        buf.put_slice(&uuid_bytes[0..8]); // MSB half second (Java's LSB position)

        // Payload digest (32 bytes)
        buf.put_slice(&self.payload_digest);

        // Payload type (4 bytes)
        buf.put_u32(self.payload_type.to_u32());

        // Payload length (4 bytes)
        buf.put_u32(self.payload_length);

        // Payload (variable)
        buf.put_slice(&self.payload);

        Ok(buf.freeze())
    }

    /// Deserialize from binary format
    pub fn deserialize(mut data: Bytes) -> Result<Self> {
        if data.len() < TOTAL_HEADER_SIZE {
            return Err(ProtocolError::InvalidMessage(format!(
                "Message too short: {} bytes (need at least {})",
                data.len(),
                TOTAL_HEADER_SIZE
            ))
            .into());
        }

        // Parse header length
        let header_length = data.get_u32();
        if header_length != HEADER_LENGTH {
            return Err(ProtocolError::InvalidMessage(format!(
                "Invalid header length: {} (expected {})",
                header_length, HEADER_LENGTH
            ))
            .into());
        }

        // Parse message type (32 bytes)
        let mut msg_type_bytes = [0u8; MESSAGE_TYPE_LENGTH];
        data.copy_to_slice(&mut msg_type_bytes);
        let message_type = message_type_from_padded(&msg_type_bytes)?;

        // Parse schema version
        let schema_version = data.get_u32();

        // Parse created date
        let created_date = data.get_u64();

        // Parse sequence number
        let sequence_number = data.get_i64();

        // Parse flags
        let flags = data.get_u64();

        // Parse message ID (16 bytes)
        // AWS/Java interop: The SSM agent (Java) writes UUIDs with swapped halves.
        // Wire format has LSB at offset+0 and MSB at offset+8, but Rust's Uuid expects
        // RFC 4122 order (MSB first). We read both halves and swap them back.
        let mut wire_uuid = [0u8; MESSAGE_ID_LENGTH];
        data.copy_to_slice(&mut wire_uuid);
        let mut message_id_bytes = [0u8; MESSAGE_ID_LENGTH];
        message_id_bytes[0..8].copy_from_slice(&wire_uuid[8..16]); // MSB from wire's second half
        message_id_bytes[8..16].copy_from_slice(&wire_uuid[0..8]); // LSB from wire's first half
        let message_id = Uuid::from_bytes(message_id_bytes);

        // Parse payload digest (32 bytes)
        let mut payload_digest = [0u8; PAYLOAD_DIGEST_LENGTH];
        data.copy_to_slice(&mut payload_digest);

        // Parse payload type
        let payload_type_u32 = data.get_u32();
        let payload_type = PayloadType::from_u32(payload_type_u32)?;

        // Parse payload length
        let payload_length = data.get_u32();

        // Security check: prevent memory exhaustion attacks
        if payload_length > MAX_PAYLOAD_SIZE {
            return Err(ProtocolError::InvalidMessage(format!(
                "Payload too large: {} bytes (max {})",
                payload_length, MAX_PAYLOAD_SIZE
            ))
            .into());
        }

        // Parse payload
        if data.remaining() < payload_length as usize {
            return Err(ProtocolError::InvalidMessage(format!(
                "Payload length mismatch: declared {}, have {}",
                payload_length,
                data.remaining()
            ))
            .into());
        }

        let payload = data.copy_to_bytes(payload_length as usize);

        let msg = Self {
            header_length,
            message_type,
            schema_version,
            created_date,
            sequence_number,
            flags,
            message_id,
            payload_digest,
            payload_type,
            payload_length,
            payload,
        };

        // Validate message
        msg.validate()?;

        Ok(msg)
    }

    /// Validate message integrity
    pub fn validate(&self) -> Result<()> {
        // Check header length
        if self.header_length != HEADER_LENGTH {
            return Err(
                ProtocolError::InvalidMessage("HeaderLength must be 116".to_string()).into(),
            );
        }

        // Check sequence number is within safe bounds (prevents overflow attacks)
        // Leave headroom of 1000 from i64 boundaries
        const MAX_SAFE_SEQUENCE: i64 = i64::MAX - 1000;
        const MIN_SAFE_SEQUENCE: i64 = i64::MIN + 1000;
        if self.sequence_number > MAX_SAFE_SEQUENCE || self.sequence_number < MIN_SAFE_SEQUENCE {
            return Err(ProtocolError::InvalidMessage(format!(
                "Sequence number {} outside safe bounds [{}, {}]",
                self.sequence_number, MIN_SAFE_SEQUENCE, MAX_SAFE_SEQUENCE
            ))
            .into());
        }

        // Check payload length matches actual payload
        if self.payload_length != self.payload.len() as u32 {
            return Err(ProtocolError::InvalidMessage(format!(
                "PayloadLength mismatch: declared {}, actual {}",
                self.payload_length,
                self.payload.len()
            ))
            .into());
        }

        // Validate payload digest (SHA-256)
        // Note: Simple comparison is fine here - the digest is not secret.
        // An attacker who can observe the message already has the payload
        // and can compute the correct digest themselves.
        //
        // AWS sends some control messages with empty payloads where the digest
        // is all zeros rather than SHA-256(""). We accept zero-filled digests
        // for empty payloads since they indicate "not applicable".
        let is_zero_digest = self.payload_digest == [0u8; 32];
        if !(self.payload.is_empty() && is_zero_digest) {
            let computed_digest = compute_digest(&self.payload);
            if computed_digest != self.payload_digest {
                return Err(ProtocolError::InvalidMessage(
                    "Payload digest validation failed (SHA-256 mismatch)".to_string(),
                )
                .into());
            }
        }

        Ok(())
    }
}

/// Compute SHA-256 digest of payload
fn compute_digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.finalize().into()
}

/// Convert MessageType to 32-byte space-padded array
fn message_type_to_padded(msg_type: &MessageType) -> [u8; MESSAGE_TYPE_LENGTH] {
    let s = match msg_type {
        MessageType::InputStreamData => "input_stream_data",
        MessageType::OutputStreamData => "output_stream_data",
        MessageType::Acknowledge => "acknowledge",
        MessageType::ChannelClosed => "channel_closed",
        MessageType::StartPublication => "start_publication",
        MessageType::PausePublication => "pause_publication",
    };

    let mut bytes = [b' '; MESSAGE_TYPE_LENGTH];
    let len = s.len().min(MESSAGE_TYPE_LENGTH);
    bytes[..len].copy_from_slice(&s.as_bytes()[..len]);
    bytes
}

/// Parse MessageType from 32-byte padded array
fn message_type_from_padded(bytes: &[u8; MESSAGE_TYPE_LENGTH]) -> Result<MessageType> {
    let s = std::str::from_utf8(bytes)
        .map_err(|e| ProtocolError::InvalidMessage(format!("Invalid UTF-8 in MessageType: {}", e)))?
        .trim()
        .trim_end_matches('\0'); // AWS uses null-padding, not space-padding

    match s {
        "input_stream_data" => Ok(MessageType::InputStreamData),
        "output_stream_data" => Ok(MessageType::OutputStreamData),
        "acknowledge" => Ok(MessageType::Acknowledge),
        "channel_closed" => Ok(MessageType::ChannelClosed),
        "start_publication" => Ok(MessageType::StartPublication),
        "pause_publication" => Ok(MessageType::PausePublication),
        _ => Err(ProtocolError::InvalidMessage(format!("Unknown MessageType: {}", s)).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_type_conversion() {
        assert_eq!(PayloadType::from_u32(1).unwrap(), PayloadType::Output);
        assert_eq!(PayloadType::from_u32(12).unwrap(), PayloadType::ExitCode);
        assert!(PayloadType::from_u32(99).is_err());

        assert_eq!(PayloadType::Output.to_u32(), 1);
        assert_eq!(PayloadType::ExitCode.to_u32(), 12);
    }

    #[test]
    fn test_message_type_padding() {
        let msg_type = MessageType::InputStreamData;
        let padded = message_type_to_padded(&msg_type);

        assert_eq!(padded.len(), MESSAGE_TYPE_LENGTH);
        assert_eq!(&padded[..17], b"input_stream_data");
        assert_eq!(&padded[17..], &[b' '; 15]);

        let parsed = message_type_from_padded(&padded).unwrap();
        assert_eq!(parsed, msg_type);
    }

    #[test]
    fn test_digest_computation() {
        let payload = b"test payload";
        let digest = compute_digest(payload);

        // SHA-256 is deterministic
        let digest2 = compute_digest(payload);
        assert_eq!(digest, digest2);

        // Different payload = different digest
        let digest3 = compute_digest(b"different");
        assert_ne!(digest, digest3);
    }

    #[test]
    fn test_message_serialization_roundtrip() {
        let payload = Bytes::from_static(b"Hello, SSM!");
        let msg = ClientMessage::new(
            MessageType::OutputStreamData,
            42,
            PayloadType::Output,
            payload.clone(),
        );

        let serialized = msg.serialize().unwrap();
        assert_eq!(serialized.len(), TOTAL_HEADER_SIZE + payload.len());

        let deserialized = ClientMessage::deserialize(serialized).unwrap();

        assert_eq!(deserialized.message_type, msg.message_type);
        assert_eq!(deserialized.sequence_number, msg.sequence_number);
        assert_eq!(deserialized.payload_type, msg.payload_type);
        assert_eq!(deserialized.payload, msg.payload);
        assert_eq!(deserialized.payload_digest, msg.payload_digest);
    }

    #[test]
    fn test_message_validation() {
        let payload = Bytes::from_static(b"test");
        let mut msg = ClientMessage::new(
            MessageType::InputStreamData,
            1,
            PayloadType::Output,
            payload,
        );

        // Valid message
        assert!(msg.validate().is_ok());

        // Invalid header length
        msg.header_length = 100;
        assert!(msg.validate().is_err());
        msg.header_length = HEADER_LENGTH;

        // Invalid payload digest
        msg.payload_digest = [0u8; 32];
        assert!(msg.validate().is_err());
    }

    #[test]
    fn test_empty_payload_digest_validated() {
        // Empty payload with correct SHA-256("") digest must pass
        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            0,
            PayloadType::Output,
            Bytes::new(), // empty
        );
        assert!(msg.validate().is_ok());

        // AWS sends some control messages with empty payload and zero-filled digest
        // (meaning "not applicable") — this must also pass
        let mut zeros = msg.clone();
        zeros.payload_digest = [0u8; 32];
        assert!(zeros.validate().is_ok());

        // Tampered digest (non-zero, non-matching) on empty payload must fail
        let mut tampered = msg;
        tampered.payload_digest = [0xFFu8; 32];
        assert!(tampered.validate().is_err());
    }

    #[test]
    fn test_flags() {
        assert_eq!(flags::SYN, 1);
        assert_eq!(flags::FIN, 2);
        assert_eq!(flags::SYN | flags::FIN, 3);
    }
}

// =============================================================================
// Property-Based Tests
// =============================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    // Strategy for generating valid PayloadType values
    fn payload_type_strategy() -> impl Strategy<Value = PayloadType> {
        prop_oneof![
            Just(PayloadType::Undefined),
            Just(PayloadType::Output),
            Just(PayloadType::Error),
            Just(PayloadType::Size),
            Just(PayloadType::Parameter),
            Just(PayloadType::HandshakeRequest),
            Just(PayloadType::HandshakeResponse),
            Just(PayloadType::HandshakeComplete),
            Just(PayloadType::EncChallengeRequest),
            Just(PayloadType::EncChallengeResponse),
            Just(PayloadType::Flag),
            Just(PayloadType::StdErr),
            Just(PayloadType::ExitCode),
        ]
    }

    // Strategy for generating valid MessageType values
    fn message_type_strategy() -> impl Strategy<Value = MessageType> {
        prop_oneof![
            Just(MessageType::InputStreamData),
            Just(MessageType::OutputStreamData),
            Just(MessageType::Acknowledge),
            Just(MessageType::ChannelClosed),
            Just(MessageType::StartPublication),
            Just(MessageType::PausePublication),
        ]
    }

    proptest! {
        /// Property: Serialization followed by deserialization is identity
        #[test]
        fn roundtrip_preserves_data(
            seq_num in any::<i64>(),
            payload in prop::collection::vec(any::<u8>(), 0..4096),
            payload_type in payload_type_strategy(),
            message_type in message_type_strategy(),
        ) {
            let msg = ClientMessage::new(
                message_type,
                seq_num,
                payload_type,
                Bytes::from(payload.clone()),
            );

            let serialized = msg.serialize().expect("serialization should succeed");
            let deserialized = ClientMessage::deserialize(serialized)
                .expect("deserialization should succeed");

            prop_assert_eq!(deserialized.message_type, msg.message_type);
            prop_assert_eq!(deserialized.sequence_number, msg.sequence_number);
            prop_assert_eq!(deserialized.payload_type, msg.payload_type);
            prop_assert_eq!(deserialized.payload.as_ref(), payload.as_slice());
        }

        /// Property: Deserialization of random bytes never panics
        #[test]
        fn deserialize_never_panics(data in prop::collection::vec(any::<u8>(), 0..8192)) {
            // This should return Ok or Err, but never panic
            let _ = ClientMessage::deserialize(Bytes::from(data));
        }

        /// Property: Serialized message has correct length
        #[test]
        fn serialized_length_correct(
            payload in prop::collection::vec(any::<u8>(), 0..4096),
        ) {
            let msg = ClientMessage::new(
                MessageType::OutputStreamData,
                0,
                PayloadType::Output,
                Bytes::from(payload.clone()),
            );

            let serialized = msg.serialize().expect("serialization should succeed");
            prop_assert_eq!(serialized.len(), TOTAL_HEADER_SIZE + payload.len());
        }

        /// Property: Payload digest matches payload
        #[test]
        fn digest_matches_payload(
            payload in prop::collection::vec(any::<u8>(), 0..4096),
        ) {
            let msg = ClientMessage::new(
                MessageType::OutputStreamData,
                0,
                PayloadType::Output,
                Bytes::from(payload.clone()),
            );

            let expected_digest = compute_digest(&payload);
            prop_assert_eq!(msg.payload_digest, expected_digest);
        }

        /// Property: Validation passes for correctly constructed messages
        #[test]
        fn validation_passes_for_valid_messages(
            seq_num in any::<i64>(),
            payload in prop::collection::vec(any::<u8>(), 0..1024),
            payload_type in payload_type_strategy(),
            message_type in message_type_strategy(),
        ) {
            let msg = ClientMessage::new(
                message_type,
                seq_num,
                payload_type,
                Bytes::from(payload),
            );

            prop_assert!(msg.validate().is_ok(), "Valid message should pass validation");
        }
    }
}
