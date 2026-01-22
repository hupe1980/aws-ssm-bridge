//! Common test infrastructure and utilities
#![allow(dead_code)]

use base64::Engine;
use serde_json::json;

pub mod mock_ssm;

/// Test utilities and helpers
pub struct TestContext {
    pub mock_server_url: String,
    pub instance_id: String,
}

impl TestContext {
    pub fn new(port: u16) -> Self {
        Self {
            mock_server_url: format!("ws://127.0.0.1:{}", port),
            instance_id: "i-test1234567890abc".to_string(),
        }
    }
}

/// Create a test session configuration
pub fn test_session_config() -> aws_ssm_bridge::SessionConfig {
    aws_ssm_bridge::SessionConfig {
        target: "i-test1234567890abc".to_string(),
        region: Some("us-east-1".to_string()),
        session_type: aws_ssm_bridge::SessionType::InteractiveCommands,
        document_name: Some("AWS-StartInteractiveCommand".to_string()),
        reason: Some("Integration test".to_string()),
        parameters: Default::default(),
        ..Default::default()
    }
}

/// Helper to create test protocol messages
pub fn create_output_message(stream_id: &str, seq: i64, data: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(data);
    json!({
        "headerLength": 0,
        "messageType": "output_stream_data",
        "schemaVersion": 1,
        "createdDate": 1234567890,
        "sequenceNumber": seq,
        "flags": 0,
        "messageId": format!("msg-{}", seq),
        "payloadDigest": "",
        "payloadType": 1,
        "payloadLength": data.len(),
        "payload": encoded,
        "streamId": stream_id
    })
    .to_string()
}

/// Helper to create input message (for sending to server)
pub fn create_input_message(stream_id: &str, seq: i64, data: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(data);
    json!({
        "headerLength": 0,
        "messageType": "input_stream_data",
        "schemaVersion": 1,
        "createdDate": 1234567890,
        "sequenceNumber": seq,
        "flags": 0,
        "messageId": format!("msg-{}", seq),
        "payloadDigest": "",
        "payloadType": 1,
        "payloadLength": data.len(),
        "payload": encoded,
        "streamId": stream_id
    })
    .to_string()
}

/// Helper to create acknowledge message
pub fn create_ack_message(msg_id: &str, seq: i64) -> String {
    json!({
        "headerLength": 0,
        "messageType": "acknowledge",
        "schemaVersion": 1,
        "createdDate": 1234567890,
        "sequenceNumber": seq,
        "flags": 0,
        "messageId": msg_id,
        "acknowledgedMessageType": "input_stream_data",
        "acknowledgedMessageId": msg_id,
        "acknowledgedMessageSequenceNumber": seq,
        "isSequentialMessage": true
    })
    .to_string()
}

/// Helper to create channel closed message
pub fn create_channel_closed_message(stream_id: &str, output: &str) -> String {
    json!({
        "headerLength": 0,
        "messageType": "channel_closed",
        "schemaVersion": 1,
        "createdDate": 1234567890,
        "sequenceNumber": 0,
        "flags": 0,
        "messageId": "close-msg",
        "streamId": stream_id,
        "output": output
    })
    .to_string()
}
