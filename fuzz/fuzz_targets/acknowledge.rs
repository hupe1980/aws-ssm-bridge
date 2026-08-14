//! Acknowledgement payloads drive the retransmission buffer, so a malformed one
//! must be rejected rather than corrupting delivery state.
#![no_main]

use aws_ssm_bridge::ack::AcknowledgeContent;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<AcknowledgeContent>(data);
});
