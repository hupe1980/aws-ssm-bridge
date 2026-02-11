---
layout: default
title: Protocol Flow
nav_order: 7
---

# Protocol Flow
{: .no_toc }

Sequence diagrams for AWS SSM Session Manager.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

This document describes the protocol flow for AWS SSM Session Manager WebSocket communication, based on analysis of the [AWS session-manager-plugin](https://github.com/aws/session-manager-plugin).

## Message Buffers

Reliable message delivery uses two buffers:

| Buffer | Type | Capacity | Purpose |
|:-------|:-----|:---------|:--------|
| **OutgoingMessageBuffer** | LinkedList | 10,000 | Messages waiting for ACK |
| **IncomingMessageBuffer** | BTreeMap | 10,000 | Out-of-order messages |

**RTT Tracking** (Jacobson/Karels algorithm, RFC 6298):
- `RoundTripTime` - Smoothed RTT estimate
- `RoundTripTimeVariation` - RTT variance
- `RetransmissionTimeout` - Adaptive timeout

---

## Reliable Delivery

### Outgoing Flow

```mermaid
flowchart LR
    A[Send Data] --> B[Add to Buffer]
    B --> C[Send via WebSocket]
    C --> D{ACK Received?}
    D -->|Yes| E[Remove from Buffer]
    D -->|No| F[Timeout 200ms]
    F --> C
```

### Incoming Flow

```mermaid
flowchart TD
    A[Receive Message] --> B{seq vs expected?}
    B -->|seq == expected| C[Process + ACK]
    B -->|seq > expected| D[Buffer + ACK]
    B -->|seq < expected| E[Duplicate: Drop silently]
    C --> F[Increment expected]
    F --> G{Buffer has next?}
    G -->|Yes| C
    G -->|No| H[Done]
```

{: .warning }
> **Critical**: Do NOT ACK duplicates. The sender tracks unacked messages and will stop retrying when duplicates aren't acknowledged.

---

## Session Lifecycle

```mermaid
flowchart TD
    A[Connect WebSocket] --> B[Send OpenDataChannelInput]
    B --> C{Agent Type?}
    C -->|Modern| D[Receive HandshakeRequest]
    C -->|Legacy| E[Receive Output directly]
    D --> F[Send HandshakeResponse]
    F --> G[Receive HandshakeComplete]
    G --> H[Session Ready]
    E --> H
    H --> I[Data Exchange]
    I --> J[channel_closed]
```

---

## Handshake Sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant A as Agent

    Note over C,A: Modern Agents v2.3.722+
    A->>C: HandshakeRequest
    C->>A: ACK
    C->>A: HandshakeResponse
    A->>C: ACK
    A->>C: HandshakeComplete
    C->>A: ACK
    Note over C,A: Session Ready
```

---

## Data Exchange

```mermaid
sequenceDiagram
    participant C as Client
    participant A as Agent

    loop Interactive Shell
        C->>A: input_stream_data
        A->>C: ACK
        A->>C: output_stream_data
        C->>A: ACK
    end
```

---

## Binary Header Format

| Offset | Size | Field | Description |
|:-------|:-----|:------|:------------|
| 0 | 4 | HeaderLength | Always 116 |
| 4 | 32 | MessageType | e.g. "input_stream_data" |
| 36 | 4 | SchemaVersion | Always 1 |
| 40 | 8 | CreatedDate | Unix millis |
| 48 | 8 | SequenceNumber | Message sequence |
| 56 | 8 | Flags | SYN=1, FIN=2 |
| 64 | 16 | MessageId | UUID (byte-swapped) |
| 80 | 32 | PayloadDigest | SHA-256 of payload |
| 112 | 4 | PayloadType | 1=Output, 5=Handshake, etc. |
| 116 | 4 | PayloadLength | Payload size |

---

## Payload Types

| Type | Name | Direction | Description |
|:-----|:-----|:----------|:------------|
| 1 | Output | Agent→Client | Shell stdout |
| 2 | Error | Agent→Client | Error message |
| 3 | Size | Client→Agent | Terminal resize |
| 5 | HandshakeRequest | Agent→Client | Handshake initiation |
| 6 | HandshakeResponse | Client→Agent | Handshake reply |
| 7 | HandshakeComplete | Agent→Client | Handshake done |
| 10 | Flag | Both | Control flags |
| 11 | StdErr | Agent→Client | Shell stderr |
| 12 | ExitCode | Agent→Client | Process exit |

---

## ACK Format

ACK messages have specific fixed values:

| Field | Value |
|:------|:------|
| SequenceNumber | 0 (always) |
| Flags | 3 (SYN \| FIN) |
| MessageType | "acknowledge" |

**Payload** (JSON):
```json
{
  "AcknowledgedMessageType": "output_stream_data",
  "AcknowledgedMessageId": "uuid-string",
  "AcknowledgedMessageSequenceNumber": 42,
  "IsSequentialMessage": true
}
```

---

## UUID Wire Format

AWS uses non-standard byte order:

```
Standard:  [MSB 0-7] [LSB 8-15]
AWS Wire:  [LSB 8-15] [MSB 0-7]
```

---

## Port Forwarding

```mermaid
sequenceDiagram
    participant L as Local App
    participant C as Client
    participant A as Agent
    participant T as Target

    L->>C: TCP Connect
    C->>A: Flag SYN
    A->>T: TCP Connect
    A->>C: Flag SYN
    
    loop Data Transfer
        L->>C: Data
        C->>A: input_stream_data
        A->>T: Data
        T->>A: Data
        A->>C: output_stream_data
        C->>L: Data
    end
    
    L--xC: TCP Close
    C->>A: Flag FIN
```

---

## Protocol Constants

| Constant | Value |
|:---------|:------|
| Header Length | 116 bytes |
| Schema Version | 1 |
| Max Payload | 64 KB |
| ACK Timeout | ~200ms |
| Max Retries | 3000 (5 min) |
| Buffer Capacity | 10,000 messages |

---

## Implementation Notes

1. **ACK Only Non-Duplicates**: `seq < expected` → drop silently
2. **ACK Immediately**: Don't wait for processing
3. **UUID Byte Swap**: LSB first (8-15), MSB second (0-7)
4. **PayloadDigest**: SHA-256 of payload only
5. **OpenDataChannelInput**: Send as TEXT, not binary
6. **Respect pause_publication**: Stop sending immediately
