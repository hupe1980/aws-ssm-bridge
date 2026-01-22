//! Fuzz target for JSON message parsing
//!
//! Tests parsing of ACK content, control messages, and other JSON payloads
//! that are embedded in binary protocol messages.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Try parsing as UTF-8 first
    if let Ok(json_str) = std::str::from_utf8(data) {
        // Try parsing as AcknowledgeContent
        let _: Result<aws_ssm_bridge::ack::AcknowledgeContent, _> = 
            serde_json::from_str(json_str);
        
        // Try parsing as generic JSON value (for control messages)
        let _: Result<serde_json::Value, _> = serde_json::from_str(json_str);
    }
});
