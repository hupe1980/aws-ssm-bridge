//! Fuzz target for handshake protocol parsing
//!
//! Tests parsing of HandshakeRequest, HandshakeResponse, and HandshakeComplete
//! JSON messages that come from the SSM agent.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Try parsing as UTF-8 first (JSON requires valid UTF-8)
    if let Ok(json_str) = std::str::from_utf8(data) {
        // Try parsing as HandshakeRequest
        let _: Result<aws_ssm_bridge::handshake::HandshakeRequest, _> = 
            serde_json::from_str(json_str);
        
        // Try parsing as HandshakeResponse
        let _: Result<aws_ssm_bridge::handshake::HandshakeResponse, _> = 
            serde_json::from_str(json_str);
        
        // Try parsing as HandshakeComplete
        let _: Result<aws_ssm_bridge::handshake::HandshakeComplete, _> = 
            serde_json::from_str(json_str);
    }
});
