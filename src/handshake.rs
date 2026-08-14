//! The SSM agent handshake.
//!
//! Every modern SSM agent opens a session with a three-message exchange before
//! any stream data flows:
//!
//! ```text
//! agent                                                client
//!   │── HandshakeRequest (payload type 5) ───────────────►│
//!   │     AgentVersion, RequestedClientActions            │
//!   │                                                      │
//!   │◄─ HandshakeResponse (payload type 6) ───────────────│
//!   │     ClientVersion, ProcessedClientActions           │
//!   │                                                      │
//!   │── HandshakeComplete (payload type 7) ──────────────►│
//!   │     HandshakeTimeToComplete, CustomerMessage        │
//!   ╞══════════════ session is ready ═════════════════════╡
//! ```
//!
//! Two actions can be requested:
//!
//! * **`SessionType`** — tells the client whether this is a shell, a port
//!   forward, or an interactive command.  Always supported.
//! * **`KMSEncryption`** — the account's session preferences require end-to-end
//!   encryption.  Handled by [`crate::crypto`] when the `kms` feature is
//!   enabled; otherwise the action is failed with an actionable message rather
//!   than silently downgrading to an unencrypted session.
//!
//! Legacy agents (pre-2.3) send output immediately without a handshake; the
//! connection layer detects that separately and does not depend on this module.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::errors::{Error, Result};

/// Version this client reports to the agent in its handshake response.
///
/// The agent gates protocol features on the *plugin* version string, not on the
/// crate version:
///
/// | Threshold   | Behaviour unlocked                                |
/// |-------------|---------------------------------------------------|
/// | `≥ 1.1.70`  | smux-multiplexed port forwarding                  |
/// | `> 1.2.331` | agent stops sending smux keep-alive NOP frames    |
///
/// It is therefore deliberately decoupled from `CARGO_PKG_VERSION`: bumping the
/// crate must not change how the agent behaves.
pub const CLIENT_PROTOCOL_VERSION: &str = "1.2.707.0";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// An action the agent asks the client to perform during the handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionType {
    /// Negotiate a KMS data key for end-to-end session encryption.
    #[serde(rename = "KMSEncryption")]
    KmsEncryption,
    /// Declare the kind of session being established.
    #[serde(rename = "SessionType")]
    SessionType,
}

/// Outcome of a requested action, as reported back to the agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ActionStatus {
    /// The client performed the action.
    Success = 1,
    /// The client tried and failed; the agent will end the session.
    Failed = 2,
    /// The client does not implement the action.
    Unsupported = 3,
}

impl Serialize for ActionStatus {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_u32(*self as u32)
    }
}

impl<'de> Deserialize<'de> for ActionStatus {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        match u32::deserialize(d)? {
            1 => Ok(ActionStatus::Success),
            2 => Ok(ActionStatus::Failed),
            3 => Ok(ActionStatus::Unsupported),
            other => Err(serde::de::Error::custom(format!(
                "invalid ActionStatus {other}"
            ))),
        }
    }
}

/// The kind of session the agent negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NegotiatedSessionType {
    /// Interactive shell.
    #[default]
    StandardStream,
    /// A specific command run interactively.
    InteractiveCommands,
    /// Port forwarding.
    Port,
}

impl NegotiatedSessionType {
    fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "Standard_Stream" => NegotiatedSessionType::StandardStream,
            "InteractiveCommands" => NegotiatedSessionType::InteractiveCommands,
            "Port" => NegotiatedSessionType::Port,
            _ => return None,
        })
    }
}

/// Base64 codec for the handshake's byte-array fields.
///
/// The agent is Go, and Go's `encoding/json` renders a `[]byte` as a **base64
/// string**.  `serde` renders a `Vec<u8>` as an *array of numbers*.  Every field
/// the reference implementation declares as `[]byte` — `KMSCipherTextKey` and
/// both `Challenge`s — therefore needs an explicit codec on this side; without
/// it the agent rejects our handshake response and encrypted sessions never
/// start.
mod base64_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        bytes: &[u8],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Vec<u8>, D::Error> {
        // Go writes `null` for a nil slice, which decodes to an empty payload
        // rather than an error.
        let Some(encoded) = Option::<String>::deserialize(deserializer)? else {
            return Ok(Vec::new());
        };
        STANDARD
            .decode(encoded)
            .map_err(|e| serde::de::Error::custom(format!("field is not valid base64: {e}")))
    }
}

/// Parameters of a `KMSEncryption` action.
#[cfg(feature = "kms")]
#[derive(Debug, Clone, Deserialize)]
struct KmsEncryptionRequest {
    #[serde(rename = "KMSKeyId")]
    kms_key_id: String,
}

/// Result payload returned for a successful `KMSEncryption` action.
#[cfg(feature = "kms")]
#[derive(Debug, Clone, Serialize)]
struct KmsEncryptionResponse {
    /// KMS ciphertext blob the agent decrypts to derive the same key pair.
    #[serde(rename = "KMSCipherTextKey", with = "base64_bytes")]
    kms_cipher_text_key: Vec<u8>,
}

/// Parameters of a `SessionType` action.
#[derive(Debug, Clone, Deserialize)]
struct SessionTypeRequest {
    #[serde(rename = "SessionType")]
    session_type: String,
}

/// One entry of `RequestedClientActions`.
#[derive(Debug, Clone, Deserialize)]
pub struct RequestedClientAction {
    /// What the agent wants done.
    #[serde(rename = "ActionType")]
    pub action_type: ActionType,
    /// Action-specific parameters.
    #[serde(rename = "ActionParameters")]
    pub action_parameters: serde_json::Value,
}

/// The agent's handshake request.
#[derive(Debug, Clone, Deserialize)]
pub struct HandshakeRequest {
    /// Version of the SSM agent on the target.
    #[serde(rename = "AgentVersion")]
    pub agent_version: String,
    /// Actions the client must perform before the session starts.
    #[serde(rename = "RequestedClientActions")]
    pub requested_client_actions: Vec<RequestedClientAction>,
}

/// One entry of `ProcessedClientActions`.
#[derive(Debug, Clone, Serialize)]
pub struct ProcessedClientAction {
    /// The action this result refers to.
    #[serde(rename = "ActionType")]
    pub action_type: ActionType,
    /// Whether the client performed it.
    #[serde(rename = "ActionStatus")]
    pub action_status: ActionStatus,
    /// Action-specific result data.
    #[serde(rename = "ActionResult", skip_serializing_if = "Option::is_none")]
    pub action_result: Option<serde_json::Value>,
    /// Why the action failed, when it did.
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The client's handshake response.
#[derive(Debug, Clone, Serialize)]
pub struct HandshakeResponse {
    /// Protocol version this client speaks; see [`CLIENT_PROTOCOL_VERSION`].
    #[serde(rename = "ClientVersion")]
    pub client_version: String,
    /// One result per requested action, in request order.
    #[serde(rename = "ProcessedClientActions")]
    pub processed_client_actions: Vec<ProcessedClientAction>,
    /// Aggregated error strings; the agent surfaces these to the operator.
    #[serde(rename = "Errors")]
    pub errors: Vec<String>,
}

/// The agent's confirmation that the handshake finished.
#[derive(Debug, Clone, Deserialize)]
pub struct HandshakeComplete {
    /// How long the agent took, in nanoseconds.
    #[serde(rename = "HandshakeTimeToComplete")]
    pub handshake_time_to_complete: i64,
    /// Optional message for the operator (e.g. a login banner).
    ///
    /// The agent always emits the field and leaves it as `""` when there is no
    /// banner, so an empty string is normalised to `None` by
    /// [`HandshakeHandler::on_complete`] rather than surfacing as a blank line.
    #[serde(rename = "CustomerMessage")]
    #[serde(default)]
    pub customer_message: Option<String>,
}

/// The agent's encryption challenge: proof that both sides derived the same key.
#[derive(Debug, Clone, Deserialize)]
pub struct EncryptionChallengeRequest {
    /// Challenge bytes, encrypted with the agent's encryption key.
    ///
    /// Base64-encoded on the wire, matching Go's rendering of a `[]byte`.
    #[serde(rename = "Challenge", with = "base64_bytes")]
    pub challenge: Vec<u8>,
}

/// The client's answer to an [`EncryptionChallengeRequest`].
#[derive(Debug, Clone, Serialize)]
pub struct EncryptionChallengeResponse {
    /// The same challenge, re-encrypted with the client's encryption key.
    ///
    /// Base64-encoded on the wire, matching Go's rendering of a `[]byte`.
    #[serde(rename = "Challenge", with = "base64_bytes")]
    pub challenge: Vec<u8>,
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Where the handshake has got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeState {
    /// No `HandshakeRequest` seen yet.
    AwaitingRequest,
    /// Response sent; waiting for `HandshakeComplete`.
    AwaitingComplete,
    /// The agent confirmed completion.
    Completed,
    /// An action failed; the agent will terminate the session.
    Failed,
}

/// Everything needed to negotiate a KMS data key, when the session may require one.
#[cfg(feature = "kms")]
#[derive(Debug)]
pub(crate) struct KmsContext {
    /// KMS client built from the same credentials as the SSM client.
    pub client: aws_sdk_kms::Client,
    /// This session's ID; part of the KMS encryption context.
    pub session_id: String,
    /// The `StartSession` target; part of the KMS encryption context.
    pub target_id: String,
}

/// Drives the handshake and, when the agent asks for it, session encryption.
#[derive(Debug)]
pub struct HandshakeHandler {
    state: HandshakeState,
    agent_version: Option<String>,
    session_type: Option<NegotiatedSessionType>,
    #[cfg(feature = "kms")]
    kms: Option<KmsContext>,
    crypto: Option<Arc<crate::crypto::SessionCrypto>>,
}

impl HandshakeHandler {
    /// Create a handler for a session that cannot use KMS encryption.
    ///
    /// If the agent requests `KMSEncryption`, the action is failed with a
    /// message explaining what to change.
    pub fn new() -> Self {
        Self {
            state: HandshakeState::AwaitingRequest,
            agent_version: None,
            session_type: None,
            #[cfg(feature = "kms")]
            kms: None,
            crypto: None,
        }
    }

    /// Create a handler that can negotiate a KMS data key when asked.
    #[cfg(feature = "kms")]
    pub(crate) fn with_kms(kms: KmsContext) -> Self {
        Self {
            kms: Some(kms),
            ..Self::new()
        }
    }

    /// Current state.
    pub fn state(&self) -> HandshakeState {
        self.state
    }

    /// Version of the SSM agent, once the request has been seen.
    pub fn agent_version(&self) -> Option<&str> {
        self.agent_version.as_deref()
    }

    /// Session type the agent negotiated, once the request has been seen.
    pub fn session_type(&self) -> Option<NegotiatedSessionType> {
        self.session_type
    }

    /// The negotiated session cipher, if encryption was enabled.
    pub fn crypto(&self) -> Option<Arc<crate::crypto::SessionCrypto>> {
        self.crypto.clone()
    }

    /// Process a `HandshakeRequest` and produce the response to send back.
    ///
    /// Returns `Ok(None)` for a duplicate request received after the response
    /// was already sent — the agent retransmits until it sees our reply, and
    /// answering twice would push the state machine out of sync.
    pub async fn on_request(
        &mut self,
        request: HandshakeRequest,
    ) -> Result<Option<HandshakeResponse>> {
        match self.state {
            HandshakeState::AwaitingComplete | HandshakeState::Completed => {
                debug!(state = ?self.state, "ignoring duplicate HandshakeRequest");
                return Ok(None);
            }
            HandshakeState::Failed => {
                return Err(Error::protocol(
                    "HandshakeRequest received after the handshake already failed",
                ));
            }
            HandshakeState::AwaitingRequest => {}
        }

        info!(
            agent_version = %request.agent_version,
            actions = request.requested_client_actions.len(),
            "processing agent handshake request"
        );
        self.agent_version = Some(request.agent_version);

        let mut processed = Vec::with_capacity(request.requested_client_actions.len());
        let mut errors = Vec::new();

        for action in &request.requested_client_actions {
            let result = match action.action_type {
                ActionType::SessionType => self.handle_session_type(action),
                ActionType::KmsEncryption => self.handle_kms_encryption(action).await,
            };
            if let Some(message) = &result.error {
                errors.push(message.clone());
            }
            processed.push(result);
        }

        let any_failed = processed
            .iter()
            .any(|a| a.action_status != ActionStatus::Success);
        self.state = if any_failed {
            HandshakeState::Failed
        } else {
            HandshakeState::AwaitingComplete
        };

        Ok(Some(HandshakeResponse {
            client_version: CLIENT_PROTOCOL_VERSION.to_owned(),
            processed_client_actions: processed,
            errors,
        }))
    }

    /// Process a `HandshakeComplete`, returning the agent's operator message.
    pub fn on_complete(&mut self, complete: HandshakeComplete) -> Result<Option<String>> {
        if self.state != HandshakeState::AwaitingComplete {
            return Err(Error::protocol(format!(
                "HandshakeComplete received in state {:?}",
                self.state
            )));
        }
        self.state = HandshakeState::Completed;

        let elapsed = Duration::from_nanos(complete.handshake_time_to_complete.max(0) as u64);
        info!(
            elapsed_ms = elapsed.as_millis(),
            session_type = ?self.session_type,
            encrypted = self.crypto.is_some(),
            "agent handshake complete"
        );
        // Go serialises an absent banner as `""`, not as a missing field.
        Ok(complete.customer_message.filter(|m| !m.is_empty()))
    }

    /// Answer an encryption challenge: decrypt with our inbound key, re-encrypt
    /// with our outbound key.  A mismatch here proves the two sides derived
    /// different keys, which is fatal.
    pub fn answer_challenge(
        &self,
        request: &EncryptionChallengeRequest,
    ) -> Result<EncryptionChallengeResponse> {
        let crypto = self.crypto.as_ref().ok_or_else(|| {
            Error::protocol("agent sent an encryption challenge but no key was negotiated")
        })?;
        let plaintext = crypto.decrypt(&request.challenge)?;
        Ok(EncryptionChallengeResponse {
            challenge: crypto.encrypt(&plaintext)?.to_vec(),
        })
    }

    fn handle_session_type(&mut self, action: &RequestedClientAction) -> ProcessedClientAction {
        match serde_json::from_value::<SessionTypeRequest>(action.action_parameters.clone()) {
            Ok(request) => {
                let negotiated = NegotiatedSessionType::parse(&request.session_type)
                    .unwrap_or_else(|| {
                        warn!(
                            session_type = %request.session_type,
                            "agent negotiated an unrecognised session type; assuming a shell"
                        );
                        NegotiatedSessionType::StandardStream
                    });
                debug!(?negotiated, "session type negotiated");
                self.session_type = Some(negotiated);
                success(ActionType::SessionType, None)
            }
            Err(e) => failure(
                ActionType::SessionType,
                format!("could not parse SessionType parameters: {e}"),
            ),
        }
    }

    #[cfg(feature = "kms")]
    async fn handle_kms_encryption(
        &mut self,
        action: &RequestedClientAction,
    ) -> ProcessedClientAction {
        let Some(kms) = self.kms.as_ref() else {
            return failure(
                ActionType::KmsEncryption,
                "this session was created without a KMS client, so session encryption \
                 cannot be negotiated"
                    .to_owned(),
            );
        };

        let request: KmsEncryptionRequest =
            match serde_json::from_value(action.action_parameters.clone()) {
                Ok(r) => r,
                Err(e) => {
                    return failure(
                        ActionType::KmsEncryption,
                        format!("could not parse KMSEncryption parameters: {e}"),
                    )
                }
            };

        info!(kms_key_id = %request.kms_key_id, "negotiating session encryption key");
        match crate::crypto::SessionCrypto::negotiate(
            &kms.client,
            &request.kms_key_id,
            &kms.session_id,
            &kms.target_id,
        )
        .await
        {
            Ok(crypto) => {
                let blob = crypto.cipher_text_blob().to_vec();
                self.crypto = Some(Arc::new(crypto));
                let result = serde_json::to_value(KmsEncryptionResponse {
                    kms_cipher_text_key: blob,
                })
                .expect("KmsEncryptionResponse always serializes");
                success(ActionType::KmsEncryption, Some(result))
            }
            Err(e) => failure(
                ActionType::KmsEncryption,
                format!(
                    "kms:GenerateDataKey failed for key {}: {e}. The caller needs \
                     kms:GenerateDataKey on this key, and the target's instance profile \
                     needs kms:Decrypt.",
                    request.kms_key_id
                ),
            ),
        }
    }

    #[cfg(not(feature = "kms"))]
    async fn handle_kms_encryption(
        &mut self,
        _action: &RequestedClientAction,
    ) -> ProcessedClientAction {
        ProcessedClientAction {
            action_type: ActionType::KmsEncryption,
            action_status: ActionStatus::Unsupported,
            action_result: None,
            error: Some(
                "this build of aws-ssm-bridge was compiled without the `kms` feature, but the \
                 session preferences require encrypted sessions. Rebuild with `--features kms`, \
                 or disable \"Encrypt session data\" in the Session Manager preferences."
                    .to_owned(),
            ),
        }
    }
}

impl Default for HandshakeHandler {
    fn default() -> Self {
        Self::new()
    }
}

fn success(action_type: ActionType, result: Option<serde_json::Value>) -> ProcessedClientAction {
    ProcessedClientAction {
        action_type,
        action_status: ActionStatus::Success,
        action_result: result,
        error: None,
    }
}

fn failure(action_type: ActionType, message: String) -> ProcessedClientAction {
    warn!(?action_type, %message, "handshake action failed");
    ProcessedClientAction {
        action_type,
        action_status: ActionStatus::Failed,
        action_result: None,
        error: Some(message),
    }
}

/// Serialize a handshake response into the payload of a stream-data message.
pub(crate) fn response_payload(response: &HandshakeResponse) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(response)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_type_request(kind: &str) -> HandshakeRequest {
        HandshakeRequest {
            agent_version: "3.3.1345.0".to_owned(),
            requested_client_actions: vec![RequestedClientAction {
                action_type: ActionType::SessionType,
                action_parameters: serde_json::json!({ "SessionType": kind }),
            }],
        }
    }

    #[tokio::test]
    async fn happy_path_reaches_completed() {
        let mut handler = HandshakeHandler::new();
        assert_eq!(handler.state(), HandshakeState::AwaitingRequest);

        let response = handler
            .on_request(session_type_request("Port"))
            .await
            .unwrap()
            .expect("a response is required");
        assert_eq!(handler.state(), HandshakeState::AwaitingComplete);
        assert_eq!(response.client_version, CLIENT_PROTOCOL_VERSION);
        assert_eq!(
            response.processed_client_actions[0].action_status,
            ActionStatus::Success
        );
        assert_eq!(handler.session_type(), Some(NegotiatedSessionType::Port));
        assert_eq!(handler.agent_version(), Some("3.3.1345.0"));

        let banner = handler
            .on_complete(HandshakeComplete {
                handshake_time_to_complete: 1_500_000,
                customer_message: Some("Welcome".into()),
            })
            .unwrap();
        assert_eq!(handler.state(), HandshakeState::Completed);
        assert_eq!(banner.as_deref(), Some("Welcome"));
    }

    /// The agent retransmits its request until it sees a response; answering a
    /// duplicate would send a second response and desynchronise the exchange.
    #[tokio::test]
    async fn duplicate_request_is_ignored() {
        let mut handler = HandshakeHandler::new();
        assert!(handler
            .on_request(session_type_request("Standard_Stream"))
            .await
            .unwrap()
            .is_some());
        assert!(handler
            .on_request(session_type_request("Standard_Stream"))
            .await
            .unwrap()
            .is_none());
        assert_eq!(handler.state(), HandshakeState::AwaitingComplete);
    }

    #[tokio::test]
    async fn unknown_session_type_falls_back_to_shell() {
        let mut handler = HandshakeHandler::new();
        handler
            .on_request(session_type_request("Telepathy"))
            .await
            .unwrap();
        assert_eq!(
            handler.session_type(),
            Some(NegotiatedSessionType::StandardStream)
        );
    }

    #[tokio::test]
    async fn malformed_action_parameters_fail_the_handshake() {
        let mut handler = HandshakeHandler::new();
        let response = handler
            .on_request(HandshakeRequest {
                agent_version: "3.0.0.0".into(),
                requested_client_actions: vec![RequestedClientAction {
                    action_type: ActionType::SessionType,
                    action_parameters: serde_json::json!({ "Nope": 1 }),
                }],
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            response.processed_client_actions[0].action_status,
            ActionStatus::Failed
        );
        assert!(!response.errors.is_empty());
        assert_eq!(handler.state(), HandshakeState::Failed);
    }

    /// Without a KMS client the action must fail loudly. Silently continuing
    /// would produce an unencrypted session in an account that mandated
    /// encryption — the one outcome an operator must never get.
    #[tokio::test]
    async fn kms_without_a_client_fails_rather_than_downgrading() {
        let mut handler = HandshakeHandler::new();
        let response = handler
            .on_request(HandshakeRequest {
                agent_version: "3.0.0.0".into(),
                requested_client_actions: vec![RequestedClientAction {
                    action_type: ActionType::KmsEncryption,
                    action_parameters: serde_json::json!({
                        "KMSKeyId": "arn:aws:kms:us-east-1:111122223333:key/abc"
                    }),
                }],
            })
            .await
            .unwrap()
            .unwrap();

        assert_ne!(
            response.processed_client_actions[0].action_status,
            ActionStatus::Success
        );
        assert!(handler.crypto().is_none());
        assert_eq!(handler.state(), HandshakeState::Failed);
    }

    #[tokio::test]
    async fn complete_out_of_order_is_rejected() {
        let mut handler = HandshakeHandler::new();
        assert!(handler
            .on_complete(HandshakeComplete {
                handshake_time_to_complete: 0,
                customer_message: None,
            })
            .is_err());
    }

    #[test]
    fn request_parses_the_agent_wire_format() {
        let json = r#"{
            "AgentVersion": "3.3.1345.0",
            "RequestedClientActions": [
                {"ActionType": "SessionType",
                 "ActionParameters": {"SessionType": "Port", "Properties": {"portNumber": "22"}}}
            ]
        }"#;
        let request: HandshakeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.agent_version, "3.3.1345.0");
        assert_eq!(
            request.requested_client_actions[0].action_type,
            ActionType::SessionType
        );
    }

    #[test]
    fn response_serializes_to_the_shape_the_agent_expects() {
        let response = HandshakeResponse {
            client_version: CLIENT_PROTOCOL_VERSION.to_owned(),
            processed_client_actions: vec![success(ActionType::SessionType, None)],
            errors: vec![],
        };
        let json: serde_json::Value =
            serde_json::from_slice(&response_payload(&response).unwrap()).unwrap();
        assert_eq!(json["ClientVersion"], CLIENT_PROTOCOL_VERSION);
        assert_eq!(
            json["ProcessedClientActions"][0]["ActionType"],
            "SessionType"
        );
        // ActionStatus is a number on the wire, not a string.
        assert_eq!(json["ProcessedClientActions"][0]["ActionStatus"], 1);
        assert!(json["Errors"].is_array());
    }

    #[test]
    fn complete_tolerates_a_missing_customer_message() {
        let complete: HandshakeComplete =
            serde_json::from_str(r#"{"HandshakeTimeToComplete": 42}"#).unwrap();
        assert_eq!(complete.customer_message, None);
    }

    /// Go renders an absent banner as `""`, so a blank line would otherwise be
    /// printed above every shell prompt.
    #[tokio::test]
    async fn an_empty_customer_message_is_not_a_banner() {
        let mut handler = HandshakeHandler::new();
        handler
            .on_request(session_type_request("Standard_Stream"))
            .await
            .unwrap();
        let banner = handler
            .on_complete(HandshakeComplete {
                handshake_time_to_complete: 1,
                customer_message: Some(String::new()),
            })
            .unwrap();
        assert_eq!(banner, None);
    }

    /// Go's `encoding/json` renders `[]byte` as base64. serde would render
    /// `Vec<u8>` as `[1,2,3]`, which the agent rejects outright — so this is the
    /// difference between working and broken session encryption, in both
    /// directions.
    #[test]
    fn byte_array_fields_use_base64_not_a_number_array() {
        let response = EncryptionChallengeResponse {
            challenge: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert_eq!(json, r#"{"Challenge":"3q2+7w=="}"#);

        let request: EncryptionChallengeRequest =
            serde_json::from_str(r#"{"Challenge":"3q2+7w=="}"#).unwrap();
        assert_eq!(request.challenge, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn a_null_byte_array_decodes_to_an_empty_payload() {
        let request: EncryptionChallengeRequest =
            serde_json::from_str(r#"{"Challenge":null}"#).unwrap();
        assert!(request.challenge.is_empty());
    }

    #[test]
    fn a_non_base64_challenge_is_rejected_rather_than_silently_empty() {
        assert!(
            serde_json::from_str::<EncryptionChallengeRequest>(r#"{"Challenge":"!!!!"}"#).is_err()
        );
        // An int array is exactly what the old, broken encoding produced.
        assert!(
            serde_json::from_str::<EncryptionChallengeRequest>(r#"{"Challenge":[1,2,3]}"#).is_err()
        );
    }

    #[cfg(feature = "kms")]
    #[test]
    fn the_kms_ciphertext_blob_is_base64_in_the_handshake_response() {
        let json = serde_json::to_string(&KmsEncryptionResponse {
            kms_cipher_text_key: b"blob".to_vec(),
        })
        .unwrap();
        assert_eq!(json, r#"{"KMSCipherTextKey":"YmxvYg=="}"#);
    }

    /// The advertised version gates smux port forwarding on the agent side, so
    /// it must stay above the thresholds documented on the constant.
    #[test]
    fn client_version_unlocks_smux_multiplexing() {
        let parts: Vec<u32> = CLIENT_PROTOCOL_VERSION
            .split('.')
            .map(|p| p.parse().unwrap())
            .collect();
        assert_eq!(parts.len(), 4, "expected a four-part version");
        assert!(
            (parts[0], parts[1], parts[2]) > (1, 2, 331),
            "{CLIENT_PROTOCOL_VERSION} must exceed 1.2.331 to disable agent keep-alives"
        );
    }
}
