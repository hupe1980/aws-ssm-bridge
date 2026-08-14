//! The agent's handshake JSON is attacker-adjacent input: it arrives before any
//! session state exists and is parsed by serde. Malformed input must produce an
//! error, never a panic.
#![no_main]

use aws_ssm_bridge::handshake::{HandshakeComplete, HandshakeRequest};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<HandshakeRequest>(data);
    let _ = serde_json::from_slice::<HandshakeComplete>(data);
});
