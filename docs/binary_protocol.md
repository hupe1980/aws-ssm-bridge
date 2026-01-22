---
layout: default
title: Binary Protocol
nav_order: 6
---

# Binary Protocol
{: .no_toc }

AWS SSM binary message format (116-byte header).
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Message Format

Messages use a 120-byte total header (4-byte HL field + 116-byte fixed header):

```text
| HL(4) | MessageType(32) | Ver(4) | CD(8) | Seq(8) | Flags(8) |
| MessageId(16) | PayloadDigest(32) | PayType(4) | PayLen(4) |
| Payload(variable) |
```

### Field Specifications

| Field | Size | Type | Description |
|-------|------|------|-------------|
| **HeaderLength** | 4 bytes | u32 | Always 116 (excludes HL field) |
| **MessageType** | 32 bytes | UTF-8 | Space-padded string |
| **SchemaVersion** | 4 bytes | u32 | Protocol version (1) |
| **CreatedDate** | 8 bytes | u64 | Unix timestamp (milliseconds) |
| **SequenceNumber** | 8 bytes | i64 | Message sequence number |
| **Flags** | 8 bytes | u64 | Bit flags (SYN=1, FIN=2) |
| **MessageId** | 16 bytes | UUID | Binary UUID (no hyphens) |
| **PayloadDigest** | 32 bytes | [u8; 32] | SHA-256 hash |
| **PayloadType** | 4 bytes | u32 | Enum 1-12 |
| **PayloadLength** | 4 bytes | u32 | Payload byte count |
| **Payload** | variable | bytes | Raw payload data |

**Byte Order:** Big-endian (network byte order)

---

## Usage

### Creating Messages

```rust
use aws_ssm_bridge::binary_protocol::{ClientMessage, PayloadType};
use aws_ssm_bridge::protocol::MessageType;
use bytes::Bytes;

let payload = Bytes::from("ls -la\n");
let message = ClientMessage::new(
    MessageType::InputStreamData,
    1,  // sequence_number
    PayloadType::Output,
    payload,
);
// SHA-256 digest is automatically computed
// UUID and timestamp are automatically generated
```

### Serialization

```rust
let binary_data = message.serialize()?;
websocket.send(binary_data).await?;
```

### Deserialization

```rust
use bytes::Bytes;

let received = websocket.recv().await?;
let message = ClientMessage::deserialize(Bytes::from(received))?;

// Validation is automatic:
// - Header length check
// - Payload length vs actual payload
// - SHA-256 digest verification
```

---

## Payload Types

All 12 AWS payload types are supported:

```rust
pub enum PayloadType {
    Output = 1,              // stdout
    Error = 2,               // stderr
    Size = 3,                // terminal resize
    Parameter = 4,           // session parameters
    HandshakeRequest = 5,    // handshake from agent
    HandshakeResponse = 6,   // handshake from client
    HandshakeComplete = 7,   // handshake done
    EncChallengeRequest = 8, // encryption challenge
    EncChallengeResponse = 9,// encryption response
    Flag = 10,               // control flags
    StdErr = 11,             // explicit stderr
    ExitCode = 12,           // process exit code
}
```

### Control Flags (PayloadType::Flag)

```rust
pub enum PayloadTypeFlag {
    DisconnectToPort = 1,    // TCP connection closed
    TerminateSession = 2,    // End session
    ConnectToPortError = 3,  // Port unavailable
}
```

---

## Message Flags

Bit flags for stream boundaries:

```rust
pub mod flags {
    pub const SYN: u64 = 1 << 0;  // First message
    pub const FIN: u64 = 1 << 1;  // Last message
}

message.flags = flags::SYN;              // First message
message.flags = flags::FIN;              // Last message
message.flags = flags::SYN | flags::FIN; // Single-message stream
```

---

## SHA-256 Digest

The payload digest is **automatically computed** during message creation and 
**automatically validated** during deserialization.

```rust
// Automatic on creation
let message = ClientMessage::new(...);
// message.payload_digest = SHA256(payload)

// Automatic on deserialization
let parsed = ClientMessage::deserialize(data)?;
// Returns error if digest doesn't match
```

---

## Performance

SHA-256 computation (using `aws-lc-rs`):

| Payload | Time | Throughput |
|---------|------|------------|
| 1 KB | ~5 µs | 200+ MiB/s |
| 16 KB | ~50 µs | 300+ MiB/s |

The library uses `bytes::Bytes` for zero-copy buffer management.

---

## Reference

- **AWS Implementation:** [session-manager-plugin/messageparser.go](https://github.com/aws/session-manager-plugin/blob/mainline/src/message/messageparser.go)
- **Module Source:** [src/binary_protocol.rs](../src/binary_protocol.rs)
