//! Fuzz target for binary protocol parsing
//!
//! This is the most critical fuzz target as it handles untrusted data
//! directly from the network (WebSocket messages from AWS).

#![no_main]

use libfuzzer_sys::fuzz_target;
use aws_ssm_bridge::binary_protocol::ClientMessage;
use bytes::Bytes;

fuzz_target!(|data: &[u8]| {
    // Try to deserialize arbitrary bytes as a binary protocol message
    // This should never panic, only return errors
    let _ = ClientMessage::deserialize(Bytes::copy_from_slice(data));
    
    // If we successfully parsed, try to re-serialize
    if let Ok(msg) = ClientMessage::deserialize(Bytes::copy_from_slice(data)) {
        // Serialization should also never panic
        let _ = msg.serialize();
        
        // Validate round-trip (if parse succeeded, serialize should too)
        if let Ok(reserialized) = msg.serialize() {
            // Deserialize again - should produce equivalent message
            if let Ok(msg2) = ClientMessage::deserialize(reserialized) {
                // Key fields should match
                assert_eq!(msg.message_type, msg2.message_type);
                assert_eq!(msg.sequence_number, msg2.sequence_number);
                assert_eq!(msg.payload_type, msg2.payload_type);
                assert_eq!(msg.payload, msg2.payload);
            }
        }
    }
});
