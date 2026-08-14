//! The AWS SSM Session Manager binary wire format.
//!
//! Messages on the MGS data channel are a fixed 120-byte header followed by a
//! variable-length payload.  The layout below is byte-for-byte compatible with
//! the reference implementation in
//! [`aws/session-manager-plugin`](https://github.com/aws/session-manager-plugin)
//! (`src/message/messageparser.go`).
//!
//! ```text
//! offset  size  field
//! ------  ----  ---------------------------------------------
//!      0     4  HeaderLength   u32  — always 116 (excludes itself)
//!      4    32  MessageType         — ASCII, space-padded
//!     36     4  SchemaVersion  u32  — always 1
//!     40     8  CreatedDate    u64  — Unix milliseconds
//!     48     8  SequenceNumber i64  — per-direction, starts at 0
//!     56     8  Flags          u64  — SYN = 1, FIN = 2
//!     64    16  MessageId           — UUID, Java long-pair order
//!     80    32  PayloadDigest       — SHA-256 of the payload
//!    112     4  PayloadType    u32  — see [`PayloadType`]
//!    116     4  PayloadLength  u32
//!    120     …  Payload
//! ```
//!
//! All integers are big-endian.
//!
//! # MessageId byte order
//!
//! The SSM agent stores UUIDs the way Java does — as two `long`s
//! (`mostSigBits`, `leastSigBits`) — and the reference plugin writes the
//! **least**-significant half first (`putUuid` in `messageparser.go`).  That is
//! *not* RFC 4122 order, so the two 8-byte halves are swapped on the way in and
//! out.  Getting this wrong produces messages the agent silently ignores.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::errors::{Error, Result};

/// Value of the `HeaderLength` field: the header size excluding the field itself.
const HEADER_LENGTH: u32 = 116;
/// Total on-wire header size, including the 4-byte `HeaderLength` field.
pub(crate) const HEADER_SIZE: usize = 120;

const MESSAGE_TYPE_LEN: usize = 32;
const MESSAGE_ID_LEN: usize = 16;
const DIGEST_LEN: usize = 32;

/// Largest payload this client will emit or accept in a single message (10 MiB).
///
/// Guards against a hostile or malfunctioning peer declaring a huge
/// `PayloadLength` and forcing a large allocation.
pub const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// Payload chunk size used when splitting caller data into stream messages.
///
/// Matches `config.StreamDataPayloadSize` in the reference plugin.  Every
/// chunk costs a 120-byte header and one round-trip ACK, so this is the
/// throughput/latency knob — see [`SessionConfig::payload_chunk_size`].
///
/// [`SessionConfig::payload_chunk_size`]: crate::SessionConfig::payload_chunk_size
pub const DEFAULT_PAYLOAD_CHUNK_SIZE: usize = 1024;

// ---------------------------------------------------------------------------
// MessageType
// ---------------------------------------------------------------------------

/// The `MessageType` header field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// Client → agent stream data.
    InputStreamData,
    /// Agent → client stream data.
    OutputStreamData,
    /// Acknowledgement of a received stream-data message.
    Acknowledge,
    /// The agent closed the channel.
    ChannelClosed,
    /// The service is ready to accept client data.
    StartPublication,
    /// The service asks the client to stop sending.
    PausePublication,
}

impl MessageType {
    /// The on-wire ASCII name.
    pub const fn as_str(self) -> &'static str {
        match self {
            MessageType::InputStreamData => "input_stream_data",
            MessageType::OutputStreamData => "output_stream_data",
            MessageType::Acknowledge => "acknowledge",
            MessageType::ChannelClosed => "channel_closed",
            MessageType::StartPublication => "start_publication",
            MessageType::PausePublication => "pause_publication",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "input_stream_data" => MessageType::InputStreamData,
            "output_stream_data" => MessageType::OutputStreamData,
            "acknowledge" => MessageType::Acknowledge,
            "channel_closed" => MessageType::ChannelClosed,
            "start_publication" => MessageType::StartPublication,
            "pause_publication" => MessageType::PausePublication,
            _ => return None,
        })
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// PayloadType
// ---------------------------------------------------------------------------

/// The `PayloadType` header field: what the payload bytes mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum PayloadType {
    /// No specific type. Some agents use this for early shell output.
    Undefined = 0,
    /// Standard output, and the type used for client keystrokes.
    Output = 1,
    /// Error text.
    Error = 2,
    /// Terminal dimensions, as `{"cols":N,"rows":M}`.
    Size = 3,
    /// Session parameters.
    Parameter = 4,
    /// Agent → client handshake request.
    HandshakeRequest = 5,
    /// Client → agent handshake response.
    HandshakeResponse = 6,
    /// Agent → client handshake completion.
    HandshakeComplete = 7,
    /// Agent → client encryption challenge.
    EncChallengeRequest = 8,
    /// Client → agent encryption challenge response.
    EncChallengeResponse = 9,
    /// Control flag; see [`ControlFlag`].
    Flag = 10,
    /// Standard error stream.
    StdErr = 11,
    /// Remote process exit code.
    ExitCode = 12,
}

impl PayloadType {
    /// Parse from the wire representation.
    pub fn from_u32(value: u32) -> Result<Self> {
        Ok(match value {
            0 => PayloadType::Undefined,
            1 => PayloadType::Output,
            2 => PayloadType::Error,
            3 => PayloadType::Size,
            4 => PayloadType::Parameter,
            5 => PayloadType::HandshakeRequest,
            6 => PayloadType::HandshakeResponse,
            7 => PayloadType::HandshakeComplete,
            8 => PayloadType::EncChallengeRequest,
            9 => PayloadType::EncChallengeResponse,
            10 => PayloadType::Flag,
            11 => PayloadType::StdErr,
            12 => PayloadType::ExitCode,
            other => return Err(Error::protocol(format!("unknown PayloadType {other}"))),
        })
    }

    /// Whether this payload is subject to KMS session encryption.
    ///
    /// The agent encrypts `Output`, `StdErr` and `ExitCode` payloads; the
    /// client encrypts only `Output`.  Handshake and control payloads always
    /// travel in the clear because they carry the key agreement itself.
    pub(crate) const fn is_encrypted_inbound(self) -> bool {
        matches!(
            self,
            PayloadType::Output | PayloadType::StdErr | PayloadType::ExitCode
        )
    }
}

/// Payload of a [`PayloadType::Flag`] message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ControlFlag {
    /// The remote end disconnected from the forwarded port.
    DisconnectToPort = 1,
    /// The session is being terminated.
    TerminateSession = 2,
    /// The agent could not connect to the forwarded port.
    ConnectToPortError = 3,
}

impl ControlFlag {
    /// Parse a big-endian `u32` flag payload.
    pub fn from_payload(payload: &[u8]) -> Option<Self> {
        let raw = u32::from_be_bytes(payload.get(..4)?.try_into().ok()?);
        Some(match raw {
            1 => ControlFlag::DisconnectToPort,
            2 => ControlFlag::TerminateSession,
            3 => ControlFlag::ConnectToPortError,
            _ => return None,
        })
    }
}

/// Bit values for the `Flags` header field.
pub mod flags {
    /// First message of a stream.
    pub const SYN: u64 = 1 << 0;
    /// Last message of a stream.
    pub const FIN: u64 = 1 << 1;
}

// ---------------------------------------------------------------------------
// ClientMessage
// ---------------------------------------------------------------------------

/// A single message on the SSM data channel.
#[derive(Debug, Clone)]
pub struct ClientMessage {
    /// Message type.
    pub message_type: MessageType,
    /// Protocol schema version; always 1.
    pub schema_version: u32,
    /// Creation time, Unix milliseconds.
    pub created_date: u64,
    /// Sequence number within this direction's stream.
    pub sequence_number: i64,
    /// SYN/FIN bits; see [`flags`].
    pub flags: u64,
    /// Unique message identifier, echoed back in acknowledgements.
    pub message_id: Uuid,
    /// SHA-256 of [`payload`](Self::payload) as it appears on the wire.
    pub payload_digest: [u8; DIGEST_LEN],
    /// Payload discriminator.
    pub payload_type: PayloadType,
    /// Payload bytes.
    pub payload: Bytes,
}

impl ClientMessage {
    /// Build a message, computing the digest and timestamp automatically.
    pub fn new(
        message_type: MessageType,
        sequence_number: i64,
        payload_type: PayloadType,
        payload: Bytes,
    ) -> Self {
        Self {
            message_type,
            schema_version: 1,
            created_date: now_millis(),
            sequence_number,
            flags: 0,
            message_id: Uuid::new_v4(),
            payload_digest: sha256(&payload),
            payload_type,
            payload,
        }
    }

    /// Encode to the on-wire byte representation.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + self.payload.len());

        buf.put_u32(HEADER_LENGTH);
        buf.put_slice(&padded_message_type(self.message_type));
        buf.put_u32(self.schema_version);
        buf.put_u64(self.created_date);
        buf.put_i64(self.sequence_number);
        buf.put_u64(self.flags);

        // Java long-pair order: least-significant half first. See module docs.
        let uuid = self.message_id.as_bytes();
        buf.put_slice(&uuid[8..16]);
        buf.put_slice(&uuid[0..8]);

        buf.put_slice(&self.payload_digest);
        buf.put_u32(self.payload_type as u32);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        buf.freeze()
    }

    /// Decode from the on-wire byte representation.
    ///
    /// Structural checks (header length, payload length, sequence number) and
    /// the SHA-256 payload digest are all verified here, matching
    /// `ClientMessage.Validate()` in the reference plugin.  A message that
    /// fails any of them is a protocol violation, not a recoverable condition.
    ///
    /// `start_publication` and `pause_publication` skip validation entirely —
    /// the service sends them with an empty payload and a zeroed digest, and
    /// the reference implementation exempts them by name.
    pub fn deserialize(mut data: Bytes) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(Error::protocol(format!(
                "message truncated: {} bytes, need at least {HEADER_SIZE}",
                data.len()
            )));
        }

        let header_length = data.get_u32();
        if header_length != HEADER_LENGTH {
            return Err(Error::protocol(format!(
                "bad HeaderLength {header_length}, expected {HEADER_LENGTH}"
            )));
        }

        let mut type_bytes = [0u8; MESSAGE_TYPE_LEN];
        data.copy_to_slice(&mut type_bytes);
        let message_type = parse_message_type(&type_bytes)?;

        let schema_version = data.get_u32();
        let created_date = data.get_u64();
        let sequence_number = data.get_i64();
        let flags = data.get_u64();

        let mut wire_uuid = [0u8; MESSAGE_ID_LEN];
        data.copy_to_slice(&mut wire_uuid);
        let mut uuid_bytes = [0u8; MESSAGE_ID_LEN];
        uuid_bytes[0..8].copy_from_slice(&wire_uuid[8..16]);
        uuid_bytes[8..16].copy_from_slice(&wire_uuid[0..8]);
        let message_id = Uuid::from_bytes(uuid_bytes);

        let mut payload_digest = [0u8; DIGEST_LEN];
        data.copy_to_slice(&mut payload_digest);

        let payload_type = PayloadType::from_u32(data.get_u32())?;
        let payload_length = data.get_u32() as usize;

        if payload_length > MAX_PAYLOAD_SIZE {
            return Err(Error::protocol(format!(
                "declared payload of {payload_length} bytes exceeds the {MAX_PAYLOAD_SIZE}-byte limit"
            )));
        }
        if data.remaining() < payload_length {
            return Err(Error::protocol(format!(
                "declared payload of {payload_length} bytes but only {} remain",
                data.remaining()
            )));
        }
        let payload = data.copy_to_bytes(payload_length);

        let msg = Self {
            message_type,
            schema_version,
            created_date,
            sequence_number,
            flags,
            message_id,
            payload_digest,
            payload_type,
            payload,
        };
        msg.validate()?;
        Ok(msg)
    }

    fn validate(&self) -> Result<()> {
        // Publication control messages carry no payload and no digest.
        if matches!(
            self.message_type,
            MessageType::StartPublication | MessageType::PausePublication
        ) {
            return Ok(());
        }

        if self.sequence_number < 0 {
            return Err(Error::protocol(format!(
                "negative sequence number {}",
                self.sequence_number
            )));
        }

        // A zero-length payload has no digest to check; the reference
        // implementation skips the comparison in exactly this case.
        if !self.payload.is_empty() && sha256(&self.payload) != self.payload_digest {
            return Err(Error::protocol(format!(
                "payload digest mismatch on {} seq {}",
                self.message_type, self.sequence_number
            )));
        }

        Ok(())
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn sha256(data: &[u8]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn padded_message_type(message_type: MessageType) -> [u8; MESSAGE_TYPE_LEN] {
    let s = message_type.as_str().as_bytes();
    debug_assert!(s.len() <= MESSAGE_TYPE_LEN);
    let mut out = [b' '; MESSAGE_TYPE_LEN];
    out[..s.len()].copy_from_slice(s);
    out
}

fn parse_message_type(bytes: &[u8; MESSAGE_TYPE_LEN]) -> Result<MessageType> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error::protocol(format!("MessageType is not valid UTF-8: {e}")))?;
    // Agents pad with spaces; some builds pad with NULs. Trim both.
    let name = text.trim_matches(|c: char| c == '\0' || c.is_whitespace());
    MessageType::parse(name).ok_or_else(|| Error::protocol(format!("unknown MessageType {name:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(payload: &'static [u8]) -> ClientMessage {
        ClientMessage::new(
            MessageType::OutputStreamData,
            7,
            PayloadType::Output,
            Bytes::from_static(payload),
        )
    }

    #[test]
    fn roundtrip_preserves_every_field() {
        let msg = sample(b"hello, session manager");
        let wire = msg.serialize();
        assert_eq!(wire.len(), HEADER_SIZE + msg.payload.len());

        let back = ClientMessage::deserialize(wire).expect("valid message");
        assert_eq!(back.message_type, msg.message_type);
        assert_eq!(back.sequence_number, msg.sequence_number);
        assert_eq!(back.payload_type, msg.payload_type);
        assert_eq!(back.payload, msg.payload);
        assert_eq!(back.message_id, msg.message_id);
        assert_eq!(back.payload_digest, msg.payload_digest);
        assert_eq!(back.created_date, msg.created_date);
    }

    /// The agent writes the least-significant UUID half first (Java long-pair
    /// order).  Pin the exact byte layout so a "cleanup" cannot silently break
    /// wire compatibility.
    #[test]
    fn message_id_uses_java_long_pair_order() {
        let mut msg = sample(b"x");
        msg.message_id = Uuid::from_bytes([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ]);
        let wire = msg.serialize();
        assert_eq!(
            &wire[64..72],
            &[0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f]
        );
        assert_eq!(
            &wire[72..80],
            &[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]
        );

        let back = ClientMessage::deserialize(wire).unwrap();
        assert_eq!(back.message_id, msg.message_id);
    }

    #[test]
    fn header_field_offsets_match_the_spec() {
        let msg = sample(b"abc");
        let wire = msg.serialize();
        assert_eq!(u32::from_be_bytes(wire[0..4].try_into().unwrap()), 116);
        assert_eq!(&wire[4..21], b"output_stream_data"[..17].as_ref());
        assert_eq!(u32::from_be_bytes(wire[36..40].try_into().unwrap()), 1);
        assert_eq!(i64::from_be_bytes(wire[48..56].try_into().unwrap()), 7);
        assert_eq!(u32::from_be_bytes(wire[112..116].try_into().unwrap()), 1);
        assert_eq!(u32::from_be_bytes(wire[116..120].try_into().unwrap()), 3);
    }

    #[test]
    fn digest_mismatch_is_rejected() {
        let msg = sample(b"payload bytes");
        let mut wire = msg.serialize().to_vec();
        wire[80] ^= 0xff; // corrupt the first digest byte
        let err = ClientMessage::deserialize(Bytes::from(wire)).unwrap_err();
        assert!(err.to_string().contains("digest mismatch"), "{err}");
    }

    #[test]
    fn corrupt_payload_is_rejected() {
        let msg = sample(b"payload bytes");
        let mut wire = msg.serialize().to_vec();
        wire[HEADER_SIZE] ^= 0xff; // flip a payload byte, digest now stale
        let err = ClientMessage::deserialize(Bytes::from(wire)).unwrap_err();
        assert!(err.to_string().contains("digest mismatch"), "{err}");
    }

    /// Publication control messages arrive with an all-zero digest and must be
    /// accepted, otherwise the session never learns it may start sending.
    #[test]
    fn publication_messages_skip_validation() {
        let mut msg = ClientMessage::new(
            MessageType::StartPublication,
            0,
            PayloadType::Undefined,
            Bytes::new(),
        );
        msg.payload_digest = [0u8; 32];
        ClientMessage::deserialize(msg.serialize()).expect("start_publication must be accepted");
    }

    #[test]
    fn empty_payload_needs_no_digest() {
        let mut msg = ClientMessage::new(
            MessageType::OutputStreamData,
            0,
            PayloadType::Undefined,
            Bytes::new(),
        );
        msg.payload_digest = [0u8; 32];
        ClientMessage::deserialize(msg.serialize()).expect("empty payload is always valid");
    }

    #[test]
    fn negative_sequence_number_is_rejected() {
        let mut msg = sample(b"data");
        msg.sequence_number = -1;
        let err = ClientMessage::deserialize(msg.serialize()).unwrap_err();
        assert!(err.to_string().contains("negative sequence"), "{err}");
    }

    #[test]
    fn oversized_declared_payload_is_rejected_without_allocating() {
        let msg = sample(b"tiny");
        let mut wire = msg.serialize().to_vec();
        wire[116..120].copy_from_slice(&u32::MAX.to_be_bytes());
        let err = ClientMessage::deserialize(Bytes::from(wire)).unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    /// Document the integrity envelope precisely: the digest covers the payload
    /// and nothing else, so a corrupted timestamp, sequence number, flag or
    /// message ID is *not* detectable at this layer. That is by design — the
    /// wire format has no header checksum — and callers relying on those fields
    /// for anything security-sensitive would be relying on nothing.
    #[test]
    fn header_metadata_is_outside_the_integrity_envelope() {
        let msg = sample(b"payload");
        // CreatedDate, SequenceNumber, Flags and MessageId: bytes 40..80.
        for offset in [40usize, 56, 64, 79] {
            let mut wire = msg.serialize().to_vec();
            wire[offset] ^= 0x01;
            assert!(
                ClientMessage::deserialize(Bytes::from(wire)).is_ok(),
                "byte {offset} is not digest-covered, so it must still parse"
            );
        }
    }

    #[test]
    fn truncated_message_is_rejected() {
        let wire = sample(b"data").serialize();
        let err = ClientMessage::deserialize(wire.slice(..50)).unwrap_err();
        assert!(err.to_string().contains("truncated"), "{err}");
    }

    #[test]
    fn message_type_padding_roundtrips() {
        for mt in [
            MessageType::InputStreamData,
            MessageType::OutputStreamData,
            MessageType::Acknowledge,
            MessageType::ChannelClosed,
            MessageType::StartPublication,
            MessageType::PausePublication,
        ] {
            assert_eq!(parse_message_type(&padded_message_type(mt)).unwrap(), mt);
        }
    }

    #[test]
    fn nul_padded_message_type_is_accepted() {
        let mut bytes = [0u8; MESSAGE_TYPE_LEN];
        bytes[..11].copy_from_slice(b"acknowledge");
        assert_eq!(
            parse_message_type(&bytes).unwrap(),
            MessageType::Acknowledge
        );
    }

    #[test]
    fn payload_type_rejects_unknown_discriminants() {
        assert_eq!(PayloadType::from_u32(1).unwrap(), PayloadType::Output);
        assert_eq!(PayloadType::from_u32(12).unwrap(), PayloadType::ExitCode);
        assert!(PayloadType::from_u32(13).is_err());
    }

    #[test]
    fn control_flag_parses_big_endian() {
        assert_eq!(
            ControlFlag::from_payload(&2u32.to_be_bytes()),
            Some(ControlFlag::TerminateSession)
        );
        assert_eq!(ControlFlag::from_payload(&[0, 0]), None);
        assert_eq!(ControlFlag::from_payload(&99u32.to_be_bytes()), None);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn any_message_type() -> impl Strategy<Value = MessageType> {
        prop_oneof![
            Just(MessageType::InputStreamData),
            Just(MessageType::OutputStreamData),
            Just(MessageType::Acknowledge),
            Just(MessageType::ChannelClosed),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_is_lossless(
            seq in 0i64..i64::MAX,
            payload in prop::collection::vec(any::<u8>(), 0..4096),
            message_type in any_message_type(),
        ) {
            let msg = ClientMessage::new(
                message_type,
                seq,
                PayloadType::Output,
                Bytes::from(payload.clone()),
            );
            let back = ClientMessage::deserialize(msg.serialize()).expect("valid");
            prop_assert_eq!(back.sequence_number, seq);
            prop_assert_eq!(back.message_type, message_type);
            prop_assert_eq!(back.payload.as_ref(), payload.as_slice());
        }

        /// Arbitrary bytes must produce an error, never a panic.
        #[test]
        fn deserialize_never_panics(data in prop::collection::vec(any::<u8>(), 0..8192)) {
            let _ = ClientMessage::deserialize(Bytes::from(data));
        }

        /// The digest covers the payload, so flipping any payload bit must be
        /// caught. This is the guarantee callers actually depend on: header
        /// fields are metadata, but payload bytes become terminal output or
        /// forwarded TCP data, where silent corruption is indistinguishable
        /// from the remote having sent something else.
        #[test]
        fn payload_corruption_is_always_detected(
            payload in prop::collection::vec(any::<u8>(), 1..512),
            offset in 0usize..512,
            bit in 0u32..8,
        ) {
            let len = payload.len();
            let msg = ClientMessage::new(
                MessageType::OutputStreamData,
                1,
                PayloadType::Output,
                Bytes::from(payload),
            );
            let mut wire = msg.serialize().to_vec();
            wire[HEADER_SIZE + offset % len] ^= 1 << bit;

            prop_assert!(
                ClientMessage::deserialize(Bytes::from(wire)).is_err(),
                "payload corruption at offset {} went undetected",
                offset % len,
            );
        }

        /// Corrupting the digest itself must also be caught — otherwise an
        /// attacker who can rewrite the digest could rewrite the payload too.
        #[test]
        fn digest_corruption_is_always_detected(
            payload in prop::collection::vec(any::<u8>(), 1..512),
            offset in 0usize..32,
            bit in 0u32..8,
        ) {
            let msg = ClientMessage::new(
                MessageType::OutputStreamData,
                1,
                PayloadType::Output,
                Bytes::from(payload),
            );
            let mut wire = msg.serialize().to_vec();
            wire[80 + offset] ^= 1 << bit;

            prop_assert!(ClientMessage::deserialize(Bytes::from(wire)).is_err());
        }
    }
}
