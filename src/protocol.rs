//! SSM Protocol message types and framing
//!
//! This module implements the AWS SSM Session Manager WebSocket protocol,
//! including message framing, sequencing, and channel multiplexing.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::errors::{ProtocolError, Result};

/// Protocol version
pub const PROTOCOL_VERSION: &str = "1.0";

/// Message type identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    /// Input data (stdin)
    InputStreamData,
    /// Output data (stdout)
    OutputStreamData,
    /// Acknowledge message
    Acknowledge,
    /// Channel closed
    ChannelClosed,
    /// Start publication
    StartPublication,
    /// Pause publication
    PausePublication,
}

impl MessageType {
    /// Returns the string representation of the message type
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::InputStreamData => "input_stream_data",
            MessageType::OutputStreamData => "output_stream_data",
            MessageType::Acknowledge => "acknowledge",
            MessageType::ChannelClosed => "channel_closed",
            MessageType::StartPublication => "start_publication",
            MessageType::PausePublication => "pause_publication",
        }
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Session type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[derive(Default)]
pub enum SessionType {
    /// Standard shell session
    #[serde(rename = "Standard_Stream")]
    #[default]
    StandardStream,
    /// Port forwarding session
    #[serde(rename = "Port")]
    Port,
    /// Interactive commands (AWS-StartInteractiveCommand)
    #[serde(rename = "InteractiveCommands")]
    InteractiveCommands,
}

/// Channel type for multiplexed streams
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelType {
    /// Standard input
    Stdin = 0,
    /// Standard output
    Stdout = 1,
    /// Standard error
    Stderr = 2,
    /// Control channel
    Control = 3,
}

impl TryFrom<u32> for ChannelType {
    type Error = crate::errors::Error;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            0 => Ok(ChannelType::Stdin),
            1 => Ok(ChannelType::Stdout),
            2 => Ok(ChannelType::Stderr),
            3 => Ok(ChannelType::Control),
            _ => Err(
                ProtocolError::InvalidMessage(format!("Invalid channel type: {}", value)).into(),
            ),
        }
    }
}

/// Agent message sent over WebSocket
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AgentMessage {
    /// Message type
    pub message_type: MessageType,
    /// Schema version
    pub schema_version: u32,
    /// Message creation timestamp (Unix milliseconds)
    pub created_date: u64,
    /// Sequence number
    pub sequence_number: i64,
    /// Flags
    pub flags: u64,
    /// Message ID
    pub message_id: Uuid,
    /// Payload digest (SHA-256)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_digest: Option<String>,
    /// Payload type
    pub payload_type: u32,
    /// Payload length
    pub payload_length: u32,
    /// Payload data
    #[serde(skip)]
    pub payload: Bytes,
}

impl AgentMessage {
    /// Create a new agent message
    pub fn new(message_type: MessageType, sequence_number: i64, payload: Bytes) -> Self {
        Self {
            message_type,
            schema_version: 1,
            // Use unwrap_or_default to handle edge case of system time before UNIX epoch
            created_date: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            sequence_number,
            flags: 0,
            message_id: Uuid::new_v4(),
            payload_digest: None,
            payload_type: 1,
            payload_length: payload.len() as u32,
            payload,
        }
    }

    /// Serialize message to bytes (header + payload)
    pub fn to_bytes(&self) -> Result<Bytes> {
        // Serialize header to JSON
        let header_json = serde_json::to_vec(self)?;
        let header_len = header_json.len() as u32;

        // Build frame: [header_len(4)][header][payload]
        let total_len = 4 + header_len as usize + self.payload.len();
        let mut buf = BytesMut::with_capacity(total_len);

        buf.put_u32(header_len);
        buf.put_slice(&header_json);
        buf.put_slice(&self.payload);

        Ok(buf.freeze())
    }

    /// Deserialize message from bytes
    pub fn from_bytes(mut data: Bytes) -> Result<Self> {
        if data.len() < 4 {
            return Err(ProtocolError::Framing("Message too short".to_string()).into());
        }

        // Read header length with validation
        let header_len = data.get_u32() as usize;

        // Security: Validate header length is reasonable (max 1MB for JSON header)
        const MAX_HEADER_SIZE: usize = 1024 * 1024;
        if header_len > MAX_HEADER_SIZE {
            return Err(ProtocolError::Framing(format!(
                "Header length {} exceeds maximum {}",
                header_len, MAX_HEADER_SIZE
            ))
            .into());
        }

        if data.len() < header_len {
            return Err(ProtocolError::Framing(format!(
                "Incomplete header: expected {}, got {}",
                header_len,
                data.len()
            ))
            .into());
        }

        // Parse header
        let header_bytes = data.split_to(header_len);
        let mut msg: AgentMessage = serde_json::from_slice(&header_bytes)?;

        // Validate payload length
        if data.len() != msg.payload_length as usize {
            return Err(ProtocolError::Framing(format!(
                "Payload length mismatch: expected {}, got {}",
                msg.payload_length,
                data.len()
            ))
            .into());
        }

        // Attach payload
        msg.payload = data;

        Ok(msg)
    }
}

/// Payload for input/output stream data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StreamDataPayload {
    /// Data content (base64 encoded)
    pub data: String,
}

impl StreamDataPayload {
    /// Create new stream data payload
    pub fn new(data: &[u8]) -> Self {
        Self {
            data: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data),
        }
    }

    /// Decode data from base64
    pub fn decode(&self) -> Result<Vec<u8>> {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &self.data).map_err(
            |e| ProtocolError::InvalidMessage(format!("Base64 decode error: {}", e)).into(),
        )
    }
}

/// Acknowledge payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AcknowledgePayload {
    /// Message ID being acknowledged
    pub acknowledged_message_id: Uuid,
    /// Sequence number
    pub sequence_number: i64,
    /// Is sequence number valid
    pub is_sequential_message: bool,
}

/// Channel closed payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChannelClosedPayload {
    /// Output from the session (base64 encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Session ID
    pub session_id: String,
    /// Message ID
    pub message_id: Uuid,
    /// Exit code
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_roundtrip() {
        let payload = Bytes::from("test payload");
        let msg = AgentMessage::new(MessageType::InputStreamData, 1, payload.clone());

        let bytes = msg.to_bytes().unwrap();
        let decoded = AgentMessage::from_bytes(bytes).unwrap();

        assert_eq!(msg.message_type, decoded.message_type);
        assert_eq!(msg.sequence_number, decoded.sequence_number);
        assert_eq!(msg.payload, decoded.payload);
    }

    #[test]
    fn test_stream_data_payload() {
        let data = b"hello world";
        let payload = StreamDataPayload::new(data);
        let decoded = payload.decode().unwrap();

        assert_eq!(data, decoded.as_slice());
    }

    #[test]
    fn test_channel_type_conversion() {
        assert_eq!(ChannelType::try_from(0).unwrap(), ChannelType::Stdin);
        assert_eq!(ChannelType::try_from(1).unwrap(), ChannelType::Stdout);
        assert!(ChannelType::try_from(99).is_err());
    }
}
