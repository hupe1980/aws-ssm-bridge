//! Arbitrary bytes must never panic the message parser.
//!
//! The parser is the first thing that touches data from the network, so a panic
//! here is a remote denial of service.
#![no_main]

use aws_ssm_bridge::binary_protocol::ClientMessage;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(message) = ClientMessage::deserialize(Bytes::copy_from_slice(data)) else {
        return;
    };

    // Anything that parsed must survive a re-encode. Byte equality with the
    // input would be too strong: agents pad the MessageType field with NULs and
    // this encoder pads with spaces, both of which parse to the same type.
    let again = ClientMessage::deserialize(message.serialize())
        .expect("a message this parser produced must parse again");

    assert_eq!(again.message_type, message.message_type);
    assert_eq!(again.sequence_number, message.sequence_number);
    assert_eq!(again.payload_type, message.payload_type);
    assert_eq!(again.message_id, message.message_id);
    assert_eq!(again.flags, message.flags);
    assert_eq!(again.created_date, message.created_date);
    assert_eq!(again.payload, message.payload);
});
