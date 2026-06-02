//! SSM Protocol message types and enums.
//!
//! This module defines the message type enumerations and session type used
//! throughout the library.  The actual binary wire format is implemented in
//! [`crate::binary_protocol`]; this module intentionally contains no
//! serialization/framing logic to avoid confusion.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::errors::{ProtocolError, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_type_conversion() {
        assert_eq!(ChannelType::try_from(0).unwrap(), ChannelType::Stdin);
        assert_eq!(ChannelType::try_from(1).unwrap(), ChannelType::Stdout);
        assert!(ChannelType::try_from(99).is_err());
    }

    #[test]
    fn test_message_type_as_str() {
        assert_eq!(MessageType::InputStreamData.as_str(), "input_stream_data");
        assert_eq!(MessageType::Acknowledge.as_str(), "acknowledge");
    }
}
