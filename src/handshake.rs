//! AWS SSM Handshake Protocol Implementation
//!
//! Implements the 3-phase handshake protocol used by AWS SSM for session initialization:
//!
//! 1. **HandshakeRequest** (Agent → Client): Agent sends capabilities and requirements
//! 2. **HandshakeResponse** (Client → Agent): Client responds with processed actions
//! 3. **HandshakeComplete** (Agent → Client): Agent confirms handshake completion
//!
//! # Protocol Flow
//!
//! ```text
//! Client                              Agent (EC2)
//!   │                                    │
//!   │◄──── HandshakeRequest ─────────────│  (PayloadType 5)
//!   │      - AgentVersion                │
//!   │      - RequestedClientActions      │
//!   │        - KMSEncryption            │
//!   │        - SessionType              │
//!   │                                    │
//!   │───── HandshakeResponse ───────────►│  (PayloadType 6)
//!   │      - ClientVersion               │
//!   │      - ProcessedClientActions      │
//!   │        - ActionStatus (Success/Fail)│
//!   │                                    │
//!   │◄──── HandshakeComplete ────────────│  (PayloadType 7)
//!   │      - HandshakeTimeToComplete     │
//!   │      - CustomerMessage             │
//!   │                                    │
//!   │══════ Session Ready ═══════════════│
//! ```

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, instrument, warn};

use crate::binary_protocol::{ClientMessage, PayloadType};
use crate::errors::{Error, ProtocolError, Result};
use crate::protocol::MessageType;

/// Action types that can be requested during handshake
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionType {
    /// KMS encryption setup
    #[serde(rename = "KMSEncryption")]
    KmsEncryption,
    /// Session type negotiation
    #[serde(rename = "SessionType")]
    SessionType,
}

/// Status of a processed action
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ActionStatus {
    /// Action completed successfully
    Success = 1,
    /// Action failed
    Failed = 2,
    /// Action not supported by client
    Unsupported = 3,
}

impl serde::Serialize for ActionStatus {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u32(*self as u32)
    }
}

impl<'de> serde::Deserialize<'de> for ActionStatus {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        match value {
            1 => Ok(ActionStatus::Success),
            2 => Ok(ActionStatus::Failed),
            3 => Ok(ActionStatus::Unsupported),
            _ => Err(serde::de::Error::custom(format!(
                "invalid ActionStatus: {}",
                value
            ))),
        }
    }
}

/// Session types supported by SSM
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SessionTypeValue {
    /// Standard shell session
    #[serde(rename = "Standard_Stream")]
    #[default]
    StandardStream,
    /// Interactive commands (AWS-StartInteractiveCommand)
    #[serde(rename = "InteractiveCommands")]
    InteractiveCommands,
    /// Port forwarding
    #[serde(rename = "Port")]
    Port,
}

/// KMS encryption request from agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsEncryptionRequest {
    /// KMS Key ID for encryption
    #[serde(rename = "KMSKeyId")]
    pub kms_key_id: String,
}

/// KMS encryption response to agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsEncryptionResponse {
    /// Encrypted data key (ciphertext blob)
    #[serde(rename = "KMSCipherTextKey")]
    pub kms_cipher_text_key: Vec<u8>,
    /// Optional hash of the cipher text
    #[serde(rename = "KMSCipherTextHash", skip_serializing_if = "Option::is_none")]
    pub kms_cipher_text_hash: Option<Vec<u8>>,
}

/// Session type request from agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTypeRequest {
    /// Type of session
    #[serde(rename = "SessionType")]
    pub session_type: String,
    /// Additional properties for the session
    #[serde(rename = "Properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,
}

/// Action requested by agent during handshake
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestedClientAction {
    /// Type of action
    #[serde(rename = "ActionType")]
    pub action_type: ActionType,
    /// Action parameters (JSON)
    #[serde(rename = "ActionParameters")]
    pub action_parameters: serde_json::Value,
}

/// Handshake request sent by agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    /// Agent version string
    #[serde(rename = "AgentVersion")]
    pub agent_version: String,
    /// Actions requested by agent
    #[serde(rename = "RequestedClientActions")]
    pub requested_client_actions: Vec<RequestedClientAction>,
}

/// Result of processing a client action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedClientAction {
    /// Type of action that was processed
    #[serde(rename = "ActionType")]
    pub action_type: ActionType,
    /// Status of the action
    #[serde(rename = "ActionStatus")]
    pub action_status: ActionStatus,
    /// Result of the action (depends on action type)
    #[serde(rename = "ActionResult", skip_serializing_if = "Option::is_none")]
    pub action_result: Option<serde_json::Value>,
    /// Error message if action failed
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handshake response sent by client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    /// Client version string
    #[serde(rename = "ClientVersion")]
    pub client_version: String,
    /// Processed actions
    #[serde(rename = "ProcessedClientActions")]
    pub processed_client_actions: Vec<ProcessedClientAction>,
    /// Errors encountered
    #[serde(rename = "Errors", skip_serializing_if = "Vec::is_empty", default)]
    pub errors: Vec<String>,
}

/// Handshake completion message from agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeComplete {
    /// Time taken to complete handshake (nanoseconds)
    #[serde(rename = "HandshakeTimeToComplete")]
    pub handshake_time_to_complete: i64,
    /// Optional customer message to display
    #[serde(rename = "CustomerMessage", skip_serializing_if = "Option::is_none")]
    pub customer_message: Option<String>,
}

/// Handler for encryption challenges (if KMS is negotiated)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionChallengeRequest {
    /// Encrypted challenge data
    #[serde(rename = "Challenge")]
    pub challenge: Vec<u8>,
}

/// Response to encryption challenge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionChallengeResponse {
    /// Re-encrypted challenge data
    #[serde(rename = "Challenge")]
    pub challenge: Vec<u8>,
}

/// Handshake state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeState {
    /// Waiting for HandshakeRequest from agent
    AwaitingRequest,
    /// Processing request, preparing response
    Processing,
    /// Response sent, waiting for HandshakeComplete
    AwaitingComplete,
    /// Handshake completed successfully
    Completed,
    /// Handshake failed
    Failed,
}

/// Configuration for handshake behavior
#[derive(Debug, Clone)]
pub struct HandshakeConfig {
    /// Client version to report
    pub client_version: String,
    /// Whether to support KMS encryption
    pub support_kms: bool,
    /// Supported session types
    pub supported_session_types: Vec<SessionTypeValue>,
    /// Timeout for handshake completion
    pub timeout: Duration,
}

impl Default for HandshakeConfig {
    fn default() -> Self {
        Self {
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            support_kms: false, // We support our own encryption, not KMS by default
            supported_session_types: vec![
                SessionTypeValue::StandardStream,
                SessionTypeValue::InteractiveCommands,
                SessionTypeValue::Port,
            ],
            timeout: Duration::from_secs(30),
        }
    }
}

/// Handshake protocol handler
pub struct HandshakeHandler {
    config: HandshakeConfig,
    state: HandshakeState,
    negotiated_session_type: Option<SessionTypeValue>,
    agent_version: Option<String>,
    kms_enabled: bool,
}

impl HandshakeHandler {
    /// Create a new handshake handler
    pub fn new(config: HandshakeConfig) -> Self {
        Self {
            config,
            state: HandshakeState::AwaitingRequest,
            negotiated_session_type: None,
            agent_version: None,
            kms_enabled: false,
        }
    }

    /// Get current handshake state
    pub fn state(&self) -> HandshakeState {
        self.state
    }

    /// Get negotiated session type (after handshake completes)
    pub fn session_type(&self) -> Option<&SessionTypeValue> {
        self.negotiated_session_type.as_ref()
    }

    /// Get agent version (after handshake request received)
    pub fn agent_version(&self) -> Option<&str> {
        self.agent_version.as_deref()
    }

    /// Check if KMS encryption was negotiated
    pub fn is_kms_enabled(&self) -> bool {
        self.kms_enabled
    }

    /// Process a handshake request and generate response
    ///
    /// If we receive a duplicate HandshakeRequest after already sending a response
    /// (state is AwaitingComplete), we return None to indicate this should be ignored.
    /// The caller should handle the duplicate silently.
    #[instrument(skip(self, request))]
    pub fn process_request(
        &mut self,
        request: HandshakeRequest,
    ) -> Result<Option<HandshakeResponse>> {
        // If we're already waiting for HandshakeComplete, ignore duplicate requests
        // (the agent may be retransmitting before it received our response)
        if self.state == HandshakeState::AwaitingComplete || self.state == HandshakeState::Completed
        {
            debug!(state = ?self.state, "Ignoring duplicate HandshakeRequest");
            return Ok(None);
        }

        if self.state != HandshakeState::AwaitingRequest {
            return Err(Error::Protocol(ProtocolError::InvalidMessage(format!(
                "Invalid state for handshake request: {:?}",
                self.state
            ))));
        }

        self.state = HandshakeState::Processing;
        self.agent_version = Some(request.agent_version.clone());

        info!(
            agent_version = %request.agent_version,
            actions = request.requested_client_actions.len(),
            "Processing handshake request"
        );

        let mut processed_actions = Vec::new();
        let mut errors = Vec::new();

        for action in &request.requested_client_actions {
            let processed = self.process_action(action);
            if processed.action_status == ActionStatus::Failed {
                if let Some(ref err) = processed.error {
                    errors.push(err.clone());
                }
            }
            processed_actions.push(processed);
        }

        let response = HandshakeResponse {
            client_version: self.config.client_version.clone(),
            processed_client_actions: processed_actions,
            errors,
        };

        self.state = HandshakeState::AwaitingComplete;

        debug!("Handshake response prepared");
        Ok(Some(response))
    }

    /// Process a single action from the handshake request
    fn process_action(&mut self, action: &RequestedClientAction) -> ProcessedClientAction {
        match action.action_type {
            ActionType::KmsEncryption => self.process_kms_action(action),
            ActionType::SessionType => self.process_session_type_action(action),
        }
    }

    /// Process KMS encryption action
    fn process_kms_action(&mut self, action: &RequestedClientAction) -> ProcessedClientAction {
        if !self.config.support_kms {
            debug!("KMS encryption not supported, marking as unsupported");
            return ProcessedClientAction {
                action_type: ActionType::KmsEncryption,
                action_status: ActionStatus::Unsupported,
                action_result: None,
                error: Some("KMS encryption not supported by this client".to_string()),
            };
        }

        // Parse KMS request
        let kms_request: std::result::Result<KmsEncryptionRequest, _> =
            serde_json::from_value(action.action_parameters.clone());

        match kms_request {
            Ok(req) => {
                info!(kms_key_id = %req.kms_key_id, "KMS encryption requested");

                // In a real implementation, we would:
                // 1. Call AWS KMS to generate a data key
                // 2. Return the encrypted key to the agent
                // For now, we mark as unsupported
                warn!("KMS key generation not implemented yet");

                ProcessedClientAction {
                    action_type: ActionType::KmsEncryption,
                    action_status: ActionStatus::Unsupported,
                    action_result: None,
                    error: Some("KMS key generation not implemented".to_string()),
                }
            }
            Err(e) => ProcessedClientAction {
                action_type: ActionType::KmsEncryption,
                action_status: ActionStatus::Failed,
                action_result: None,
                error: Some(format!("Failed to parse KMS request: {}", e)),
            },
        }
    }

    /// Process session type action
    fn process_session_type_action(
        &mut self,
        action: &RequestedClientAction,
    ) -> ProcessedClientAction {
        let session_req: std::result::Result<SessionTypeRequest, _> =
            serde_json::from_value(action.action_parameters.clone());

        match session_req {
            Ok(req) => {
                info!(session_type = %req.session_type, "Session type requested");

                // Map session type string to enum
                let session_type = match req.session_type.as_str() {
                    "Standard_Stream" => SessionTypeValue::StandardStream,
                    "InteractiveCommands" => SessionTypeValue::InteractiveCommands,
                    "Port" => SessionTypeValue::Port,
                    other => {
                        warn!(
                            session_type = other,
                            "Unknown session type, defaulting to StandardStream"
                        );
                        SessionTypeValue::StandardStream
                    }
                };

                self.negotiated_session_type = Some(session_type);

                ProcessedClientAction {
                    action_type: ActionType::SessionType,
                    action_status: ActionStatus::Success,
                    action_result: None,
                    error: None,
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse session type request");
                ProcessedClientAction {
                    action_type: ActionType::SessionType,
                    action_status: ActionStatus::Failed,
                    action_result: None,
                    error: Some(format!("Failed to parse session type: {}", e)),
                }
            }
        }
    }

    /// Process handshake complete message
    #[instrument(skip(self, complete))]
    pub fn process_complete(&mut self, complete: HandshakeComplete) -> Result<()> {
        if self.state != HandshakeState::AwaitingComplete {
            return Err(Error::Protocol(ProtocolError::InvalidMessage(format!(
                "Invalid state for handshake complete: {:?}",
                self.state
            ))));
        }

        let duration = Duration::from_nanos(complete.handshake_time_to_complete as u64);

        info!(
            duration_ms = duration.as_millis(),
            customer_message = ?complete.customer_message,
            "Handshake completed"
        );

        if let Some(msg) = &complete.customer_message {
            info!(message = %msg, "Customer message received");
        }

        self.state = HandshakeState::Completed;
        Ok(())
    }

    /// Serialize handshake response to binary protocol message
    pub fn response_to_message(
        &self,
        response: &HandshakeResponse,
        sequence_number: i64,
    ) -> Result<ClientMessage> {
        let payload = serde_json::to_vec(response).map_err(|e| {
            Error::Protocol(ProtocolError::Framing(format!(
                "Failed to serialize response: {}",
                e
            )))
        })?;

        Ok(ClientMessage::new(
            MessageType::InputStreamData,
            sequence_number,
            PayloadType::HandshakeResponse,
            Bytes::from(payload),
        ))
    }

    /// Parse handshake request from binary protocol message
    pub fn parse_request(message: &ClientMessage) -> Result<HandshakeRequest> {
        if message.payload_type != PayloadType::HandshakeRequest {
            return Err(Error::Protocol(ProtocolError::InvalidMessage(format!(
                "Expected HandshakeRequest, got {:?}",
                message.payload_type
            ))));
        }

        serde_json::from_slice(&message.payload).map_err(|e| {
            Error::Protocol(ProtocolError::Framing(format!(
                "Failed to parse HandshakeRequest: {}",
                e
            )))
        })
    }

    /// Parse handshake complete from binary protocol message
    pub fn parse_complete(message: &ClientMessage) -> Result<HandshakeComplete> {
        if message.payload_type != PayloadType::HandshakeComplete {
            return Err(Error::Protocol(ProtocolError::InvalidMessage(format!(
                "Expected HandshakeComplete, got {:?}",
                message.payload_type
            ))));
        }

        serde_json::from_slice(&message.payload).map_err(|e| {
            Error::Protocol(ProtocolError::Framing(format!(
                "Failed to parse HandshakeComplete: {}",
                e
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_request_parsing() {
        let json = r#"{
            "AgentVersion": "3.0.0",
            "RequestedClientActions": [
                {
                    "ActionType": "SessionType",
                    "ActionParameters": {
                        "SessionType": "Standard_Stream"
                    }
                }
            ]
        }"#;

        let request: HandshakeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.agent_version, "3.0.0");
        assert_eq!(request.requested_client_actions.len(), 1);
        assert_eq!(
            request.requested_client_actions[0].action_type,
            ActionType::SessionType
        );
    }

    #[test]
    fn test_handshake_response_serialization() {
        let response = HandshakeResponse {
            client_version: "0.1.0".to_string(),
            processed_client_actions: vec![ProcessedClientAction {
                action_type: ActionType::SessionType,
                action_status: ActionStatus::Success,
                action_result: None,
                error: None,
            }],
            errors: vec![],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("ClientVersion"));
        assert!(json.contains("ProcessedClientActions"));
    }

    #[test]
    fn test_handshake_complete_parsing() {
        let json = r#"{
            "HandshakeTimeToComplete": 1000000000,
            "CustomerMessage": "Welcome to SSM"
        }"#;

        let complete: HandshakeComplete = serde_json::from_str(json).unwrap();
        assert_eq!(complete.handshake_time_to_complete, 1_000_000_000);
        assert_eq!(
            complete.customer_message,
            Some("Welcome to SSM".to_string())
        );
    }

    #[test]
    fn test_handshake_state_machine() {
        let config = HandshakeConfig::default();
        let mut handler = HandshakeHandler::new(config);

        assert_eq!(handler.state(), HandshakeState::AwaitingRequest);

        // Simulate handshake request
        let request = HandshakeRequest {
            agent_version: "3.0.0".to_string(),
            requested_client_actions: vec![RequestedClientAction {
                action_type: ActionType::SessionType,
                action_parameters: serde_json::json!({
                    "SessionType": "Standard_Stream"
                }),
            }],
        };

        let response = handler
            .process_request(request)
            .unwrap()
            .expect("Should return response");
        assert_eq!(handler.state(), HandshakeState::AwaitingComplete);
        assert_eq!(response.processed_client_actions.len(), 1);
        assert_eq!(
            response.processed_client_actions[0].action_status,
            ActionStatus::Success
        );

        // Simulate handshake complete
        let complete = HandshakeComplete {
            handshake_time_to_complete: 500_000_000,
            customer_message: Some("Session ready".to_string()),
        };

        handler.process_complete(complete).unwrap();
        assert_eq!(handler.state(), HandshakeState::Completed);
        assert!(handler.session_type().is_some());
    }

    #[test]
    fn test_kms_unsupported() {
        let config = HandshakeConfig {
            support_kms: false,
            ..Default::default()
        };
        let mut handler = HandshakeHandler::new(config);

        let request = HandshakeRequest {
            agent_version: "3.0.0".to_string(),
            requested_client_actions: vec![RequestedClientAction {
                action_type: ActionType::KmsEncryption,
                action_parameters: serde_json::json!({
                    "KMSKeyId": "arn:aws:kms:us-east-1:123456789:key/abc"
                }),
            }],
        };

        let response = handler
            .process_request(request)
            .unwrap()
            .expect("Should return response");
        assert_eq!(
            response.processed_client_actions[0].action_status,
            ActionStatus::Unsupported
        );
    }

    #[test]
    fn test_duplicate_request_ignored() {
        let config = HandshakeConfig::default();
        let mut handler = HandshakeHandler::new(config);

        let request = HandshakeRequest {
            agent_version: "3.0.0".to_string(),
            requested_client_actions: vec![RequestedClientAction {
                action_type: ActionType::SessionType,
                action_parameters: serde_json::json!({
                    "SessionType": "Standard_Stream"
                }),
            }],
        };

        // First request should return a response
        let response = handler.process_request(request.clone()).unwrap();
        assert!(response.is_some());
        assert_eq!(handler.state(), HandshakeState::AwaitingComplete);

        // Second (duplicate) request should return None
        let duplicate_response = handler.process_request(request).unwrap();
        assert!(duplicate_response.is_none());
        // State should still be AwaitingComplete
        assert_eq!(handler.state(), HandshakeState::AwaitingComplete);
    }
}
