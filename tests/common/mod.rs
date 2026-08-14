//! A mock Amazon Message Gateway that speaks the real binary protocol.
//!
//! Tests point a [`Session`](aws_ssm_bridge::Session) at this instead of AWS, so
//! the full stack — WebSocket framing, the 120-byte header, sequencing,
//! acknowledgement, retransmission and the agent handshake — is exercised
//! against something that can misbehave on demand.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aws_ssm_bridge::binary_protocol::{ClientMessage, MessageType, PayloadType};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

/// How the mock gateway should behave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Behaviour {
    /// Complete the handshake, then echo every input payload back as output.
    Echo,
    /// Complete the handshake, then close the WebSocket.
    CloseAfterHandshake,
    /// Complete the handshake, then stop responding entirely — no data, no
    /// acknowledgements, not even a pong. Simulates a black-holed connection.
    GoSilent,
    /// Accept the connection and then drop the TCP socket without a close frame.
    DropConnection,
    /// Send output messages out of order (2, 1, 0) to exercise reordering.
    OutOfOrder,
    /// Never acknowledge client input, forcing retransmission.
    NeverAcknowledge,
    /// Skip the handshake entirely, like a pre-2.3 agent.
    LegacyNoHandshake,
    /// Complete the handshake, then send a `channel_closed` message carrying the
    /// agent's JSON explanation of why.
    ChannelClosedWithReason,
    /// Complete the handshake, then send an encryption challenge with a
    /// base64-encoded `Challenge`, exactly as the Go agent encodes a `[]byte`.
    Base64Challenge,
}

/// What the gateway observed, for assertions after the fact.
#[derive(Debug, Default)]
pub struct Observed {
    /// The JSON body of the data-channel open message.
    pub open_message: Option<String>,
    /// Payloads of every `input_stream_data` message received, in order.
    pub inputs: Vec<Vec<u8>>,
    /// Sequence numbers of every client message, including duplicates.
    pub sequences: Vec<i64>,
    /// The client's `HandshakeResponse` JSON, if it sent one.
    pub handshake_response: Option<String>,
    /// Payloads received with `PayloadType::Size`.
    pub terminal_sizes: Vec<String>,
    /// Number of WebSocket pings received.
    pub pings: usize,
}

/// A running mock gateway.
pub struct MockGateway {
    addr: SocketAddr,
    observed: Arc<Mutex<Observed>>,
}

impl MockGateway {
    /// Bind on an ephemeral loopback port and serve exactly one connection.
    pub async fn start(behaviour: Behaviour) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let observed = Arc::new(Mutex::new(Observed::default()));

        tokio::spawn({
            let observed = Arc::clone(&observed);
            async move {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                if behaviour == Behaviour::DropConnection {
                    drop(stream);
                    return;
                }
                let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                serve(ws, behaviour, observed).await;
            }
        });

        Self { addr, observed }
    }

    /// The `ws://` URL to hand to `Session::attach`.
    pub fn url(&self) -> String {
        format!("ws://{}/v1/data-channel/mock", self.addr)
    }

    /// A snapshot of what the gateway has seen so far.
    pub async fn observed(&self) -> Observed {
        let guard = self.observed.lock().await;
        Observed {
            open_message: guard.open_message.clone(),
            inputs: guard.inputs.clone(),
            sequences: guard.sequences.clone(),
            handshake_response: guard.handshake_response.clone(),
            terminal_sizes: guard.terminal_sizes.clone(),
            pings: guard.pings,
        }
    }

    /// Wait until `predicate` holds, or fail after `timeout`.
    pub async fn wait_for(
        &self,
        timeout: Duration,
        label: &str,
        predicate: impl Fn(&Observed) -> bool,
    ) -> Observed {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let observed = self.observed().await;
            if predicate(&observed) {
                return observed;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {label}; observed {observed:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

type Sink<S> = futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>;

/// Send one `output_stream_data` message.
async fn send<S>(
    sink: &mut Sink<S>,
    sequence: i64,
    payload_type: PayloadType,
    payload: Bytes,
) -> bool
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = ClientMessage::new(
        MessageType::OutputStreamData,
        sequence,
        payload_type,
        payload,
    );
    sink.send(Message::Binary(message.serialize()))
        .await
        .is_ok()
}

async fn serve<S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
    behaviour: Behaviour,
    observed: Arc<Mutex<Observed>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut sink, mut source) = ws.split();
    let mut outbound_sequence: i64 = 0;

    // The client opens with a JSON text frame carrying the session token.
    match source.next().await {
        Some(Ok(Message::Text(text))) => {
            observed.lock().await.open_message = Some(text.to_string());
        }
        _ => return,
    }

    if behaviour != Behaviour::LegacyNoHandshake {
        let request = serde_json::json!({
            "AgentVersion": "3.3.1345.0",
            "RequestedClientActions": [{
                "ActionType": "SessionType",
                "ActionParameters": { "SessionType": "Standard_Stream" }
            }]
        })
        .to_string();
        if !send(
            &mut sink,
            outbound_sequence,
            PayloadType::HandshakeRequest,
            Bytes::from(request),
        )
        .await
        {
            return;
        }
        outbound_sequence += 1;
    } else {
        // A legacy agent just starts streaming.
        send(
            &mut sink,
            outbound_sequence,
            PayloadType::Output,
            Bytes::from_static(b"legacy-shell$ "),
        )
        .await;
        outbound_sequence += 1;
    }

    while let Some(frame) = source.next().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return,
        };

        match frame {
            Message::Ping(_) => {
                observed.lock().await.pings += 1;
                if sink.send(Message::Pong(Bytes::new())).await.is_err() {
                    return;
                }
            }
            Message::Close(_) => return,
            Message::Binary(data) => {
                let Ok(message) = ClientMessage::deserialize(data) else {
                    continue;
                };
                // The client acknowledges our output too. Those are not client
                // input and always carry sequence 0, so counting them would
                // corrupt every assertion about what the client sent.
                if message.message_type == MessageType::Acknowledge {
                    continue;
                }
                observed
                    .lock()
                    .await
                    .sequences
                    .push(message.sequence_number);

                match message.payload_type {
                    PayloadType::HandshakeResponse => {
                        observed.lock().await.handshake_response =
                            Some(String::from_utf8_lossy(&message.payload).into_owned());
                        acknowledge(&mut sink, &message).await;

                        let complete = serde_json::json!({
                            "HandshakeTimeToComplete": 1_000_000i64,
                            "CustomerMessage": "mock gateway ready"
                        })
                        .to_string();
                        if !send(
                            &mut sink,
                            outbound_sequence,
                            PayloadType::HandshakeComplete,
                            Bytes::from(complete),
                        )
                        .await
                        {
                            return;
                        }
                        outbound_sequence += 1;

                        match behaviour {
                            Behaviour::GoSilent => {
                                // Hold the socket open but stop servicing it.
                                // tungstenite answers pings automatically while
                                // a connection is being read, so the only true
                                // black hole is one nobody reads.
                                std::future::pending::<()>().await;
                            }
                            Behaviour::CloseAfterHandshake => {
                                let _ = sink.send(Message::Close(None)).await;
                                return;
                            }
                            Behaviour::ChannelClosedWithReason => {
                                let body = serde_json::json!({
                                    "MessageId": "11111111-2222-3333-4444-555555555555",
                                    "SessionId": "mock-session",
                                    "Output": "Connection refused by 10.0.0.7:5432",
                                })
                                .to_string();
                                let closed = ClientMessage::new(
                                    MessageType::ChannelClosed,
                                    outbound_sequence,
                                    PayloadType::Undefined,
                                    Bytes::from(body),
                                );
                                let _ = sink.send(Message::Binary(closed.serialize())).await;
                                return;
                            }
                            Behaviour::Base64Challenge => {
                                // Go marshals `Challenge []byte` as base64, so
                                // this is byte-for-byte what a real agent sends.
                                let body = serde_json::json!({
                                    "Challenge": "3q2+7w==",
                                })
                                .to_string();
                                if !send(
                                    &mut sink,
                                    outbound_sequence,
                                    PayloadType::EncChallengeRequest,
                                    Bytes::from(body),
                                )
                                .await
                                {
                                    return;
                                }
                                outbound_sequence += 1;
                            }
                            Behaviour::OutOfOrder => {
                                // Deliver 2, 1, 0 so the client must buffer and
                                // reassemble before anything reaches the caller.
                                let base = outbound_sequence;
                                for (offset, text) in
                                    [(2i64, "third "), (1, "second "), (0, "first ")]
                                {
                                    send(
                                        &mut sink,
                                        base + offset,
                                        PayloadType::Output,
                                        Bytes::from_static(text.as_bytes()),
                                    )
                                    .await;
                                }
                                outbound_sequence = base + 3;
                            }
                            _ => {}
                        }
                    }
                    PayloadType::Size => {
                        observed
                            .lock()
                            .await
                            .terminal_sizes
                            .push(String::from_utf8_lossy(&message.payload).into_owned());
                        acknowledge(&mut sink, &message).await;
                    }
                    _ => {
                        observed.lock().await.inputs.push(message.payload.to_vec());

                        if behaviour != Behaviour::NeverAcknowledge {
                            acknowledge(&mut sink, &message).await;
                        }
                        if matches!(behaviour, Behaviour::Echo | Behaviour::LegacyNoHandshake) {
                            send(
                                &mut sink,
                                outbound_sequence,
                                PayloadType::Output,
                                message.payload.clone(),
                            )
                            .await;
                            outbound_sequence += 1;
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

async fn acknowledge<S>(sink: &mut Sink<S>, message: &ClientMessage)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let content = serde_json::json!({
        "AcknowledgedMessageType": message.message_type.as_str(),
        "AcknowledgedMessageId": message.message_id.to_string(),
        "AcknowledgedMessageSequenceNumber": message.sequence_number,
        "IsSequentialMessage": true,
    })
    .to_string();

    let ack = ClientMessage::new(
        MessageType::Acknowledge,
        0,
        PayloadType::Undefined,
        Bytes::from(content),
    );
    let _ = sink.send(Message::Binary(ack.serialize())).await;
}
