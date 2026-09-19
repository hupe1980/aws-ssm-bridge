//! A mock Amazon Message Gateway that speaks the real binary protocol.
//!
//! Tests point a [`Session`](aws_ssm_bridge::Session) at this instead of AWS, so
//! the full stack — WebSocket framing, the 120-byte header, sequencing,
//! acknowledgement, retransmission and the agent handshake — is exercised
//! against something that can misbehave on demand.

#![allow(dead_code)]

pub mod ssm_api;

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
    /// Complete the handshake, echo, then close the WebSocket without a close
    /// frame once the client has sent something.
    EchoThenDrop,
    /// A **resumed** data channel, as the agent presents one.
    ///
    /// No handshake — the agent initialises its handshake state in `Initialize`
    /// and a resume does not go through it — and the sequence counters carry on
    /// from the previous connection rather than restarting, because both sides
    /// of the reference reset them only in `Initialize`
    /// (`agent/datachannel.go:251-252`). It opens with `start_publication`,
    /// which is how the gateway lets a client send on a fresh channel.
    ResumeEcho,
    /// Complete the handshake, then close the WebSocket.
    CloseAfterHandshake,
    /// Complete the handshake, stay open briefly, then close the WebSocket.
    ///
    /// The pause is the whole point: a caller of `ReconnectingSession` can only
    /// subscribe to events *after* `connect_with` returns, so a close that lands
    /// immediately is unobservable and makes any test of it flaky. This gives
    /// the test somewhere to stand.
    CloseAfterPause,
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
    /// Negotiate a `Port` session and speak smux: echo every `PSH` frame back on
    /// the stream it arrived on, and answer `FIN` with `FIN`.
    SmuxEcho,
    /// Negotiate a `Port` session, then flood the **first** stream the client
    /// opens with more frames than its inbound queue holds, while still echoing
    /// on every other stream. Drives the slow-consumer eviction path.
    SmuxFlood,
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
    /// Payload type of every client stream-data message, in arrival order.
    ///
    /// `inputs` and `terminal_sizes` are separate vectors, so the *relative*
    /// order of data and control payloads is only observable here.
    pub payload_order: Vec<PayloadType>,
    /// Number of WebSocket pings received.
    pub pings: usize,
    /// How many WebSocket connections the gateway has accepted.
    ///
    /// A reconnect is a *new* data channel, so this is how "it reconnected" is
    /// observed from the gateway's side.
    pub connections: usize,
    /// Headers of the WebSocket upgrade request, lowercased names.
    pub upgrade_headers: Vec<(String, String)>,
}

/// A running mock gateway.
pub struct MockGateway {
    addr: SocketAddr,
    observed: Arc<Mutex<Observed>>,
    /// Captured separately from [`Observed`] because the upgrade callback is
    /// synchronous and cannot await the async mutex the rest of the gateway
    /// uses. Merged back in by [`MockGateway::observed`].
    upgrade_headers: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    /// Outbound sequence, shared across connections so a resumed channel can
    /// continue it the way a real agent does.
    outbound_sequence: Arc<std::sync::atomic::AtomicI64>,
}

impl MockGateway {
    /// Bind on an ephemeral loopback port and serve exactly one connection.
    pub async fn start(behaviour: Behaviour) -> Self {
        Self::scripted(vec![behaviour]).await
    }

    /// Serve a **sequence** of connections, one behaviour each.
    ///
    /// The last behaviour repeats for any further connection, so a script of
    /// `[DropConnection, Echo]` means "fail the first data channel, then behave"
    /// however many times the client comes back.
    pub async fn scripted(script: Vec<Behaviour>) -> Self {
        assert!(!script.is_empty(), "a script needs at least one behaviour");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let observed = Arc::new(Mutex::new(Observed::default()));
        let upgrade_headers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let outbound_sequence = Arc::new(std::sync::atomic::AtomicI64::new(0));

        tokio::spawn({
            let observed = Arc::clone(&observed);
            let captured = Arc::clone(&upgrade_headers);
            let sequence = Arc::clone(&outbound_sequence);
            async move {
                for connection in 0.. {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let behaviour = script[connection.min(script.len() - 1)];
                    observed.lock().await.connections += 1;

                    let observed = Arc::clone(&observed);
                    let captured = Arc::clone(&captured);
                    let sequence = Arc::clone(&sequence);
                    // One task per connection so a lingering previous session
                    // cannot hold up the next dial.
                    tokio::spawn(async move {
                        serve_connection(stream, behaviour, observed, captured, sequence).await;
                    });
                }
            }
        });

        Self {
            addr,
            observed,
            upgrade_headers,
            outbound_sequence,
        }
    }

    /// The `ws://` URL to hand to `Session::attach`.
    pub fn url(&self) -> String {
        format!("ws://{}/v1/data-channel/mock", self.addr)
    }
}

/// Accept one WebSocket and serve it with `behaviour`.
async fn serve_connection(
    stream: tokio::net::TcpStream,
    behaviour: Behaviour,
    observed: Arc<Mutex<Observed>>,
    captured: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    sequence: Arc<std::sync::atomic::AtomicI64>,
) {
    if behaviour == Behaviour::DropConnection {
        drop(stream);
        return;
    }

    // `accept_hdr_async` rather than `accept_async`: the upgrade request is the
    // only place the SigV4 headers appear, so a test that wants to assert on
    // them needs the gateway to have seen them.
    //
    // The `Err` variant is tungstenite's `ErrorResponse`, whose size is fixed by
    // the callback signature this has to match.
    #[allow(clippy::result_large_err)]
    let inspect =
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            let mut seen = captured.lock().unwrap_or_else(|e| e.into_inner());
            *seen = request
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_ascii_lowercase(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
                .collect();
            Ok(response)
        };

    let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, inspect).await else {
        return;
    };
    serve(ws, behaviour, observed, sequence).await;
}

impl MockGateway {
    /// A snapshot of what the gateway has seen so far.
    pub async fn observed(&self) -> Observed {
        let guard = self.observed.lock().await;
        Observed {
            open_message: guard.open_message.clone(),
            inputs: guard.inputs.clone(),
            sequences: guard.sequences.clone(),
            handshake_response: guard.handshake_response.clone(),
            terminal_sizes: guard.terminal_sizes.clone(),
            payload_order: guard.payload_order.clone(),
            pings: guard.pings,
            connections: guard.connections,
            upgrade_headers: self
                .upgrade_headers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
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

/// How long [`Behaviour::CloseAfterPause`] stays open before closing.
///
/// Long enough that a test reliably subscribes first, short enough not to be
/// felt in the suite.
pub const CLOSE_PAUSE: Duration = Duration::from_millis(300);

/// Frames the gateway floods the first stream with under [`Behaviour::SmuxFlood`].
///
/// The client holds 256 frames per stream, so this must exceed that for the
/// queue to overflow and the stream to be evicted.
pub const FLOOD_FRAMES: usize = 300;

/// smux v1 framing, for the gateway side.
///
/// Transcribed from `xtaci/smux` `frame.go` (`rawHeader` at `:77-91`) rather
/// than from this crate's own encoder: a mock that reuses the code under test
/// agrees with it by construction and proves nothing.
///
/// ```text
/// [0] version  [1] cmd  [2..4] length (u16 LE)  [4..8] stream id (u32 LE)
/// ```
pub mod smux {
    /// Protocol version 1 — the version the agent's `smux.DefaultConfig()` uses.
    pub const VERSION: u8 = 1;
    /// Stream open.
    pub const SYN: u8 = 0;
    /// Stream close, a.k.a. EOF.
    pub const FIN: u8 = 1;
    /// Data push.
    pub const PSH: u8 = 2;
    /// The fixed header size.
    pub const HEADER: usize = 8;

    /// Encode one frame.
    pub fn encode(cmd: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER + payload.len());
        out.push(VERSION);
        out.push(cmd);
        out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        out.extend_from_slice(&stream_id.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Decode one frame from the front of `buf`, consuming it on success.
    pub fn decode(buf: &mut Vec<u8>) -> Option<(u8, u32, Vec<u8>)> {
        if buf.len() < HEADER {
            return None;
        }
        let length = u16::from_le_bytes([buf[2], buf[3]]) as usize;
        if buf.len() < HEADER + length {
            return None;
        }
        let cmd = buf[1];
        let stream_id = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let payload = buf[HEADER..HEADER + length].to_vec();
        buf.drain(..HEADER + length);
        Some((cmd, stream_id, payload))
    }
}

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
    sequence: Arc<std::sync::atomic::AtomicI64>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut sink, mut source) = ws.split();
    // A resumed channel continues the agent's numbering; any other connection is
    // a new data channel, whose counters `Initialize` sets to zero.
    if behaviour != Behaviour::ResumeEcho {
        sequence.store(0, std::sync::atomic::Ordering::Release);
    }
    macro_rules! next_sequence {
        () => {
            sequence.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
        };
    }
    // smux reassembly: the client's frames arrive split across stream-data
    // payloads and do not align to frame boundaries.
    let mut smux_buf: Vec<u8> = Vec::new();
    let mut flooded: Option<u32> = None;

    // The client opens with a JSON text frame carrying the session token.
    match source.next().await {
        Some(Ok(Message::Text(text))) => {
            observed.lock().await.open_message = Some(text.to_string());
        }
        _ => return,
    }

    if behaviour == Behaviour::ResumeEcho {
        // No handshake on a resume, and the sequence counter is *not* reset —
        // both are what the agent does, and both are what the client has to
        // cope with. `start_publication` carries sequence 0 and a zeroed digest
        // by protocol, so it does not consume a stream sequence number.
        let open = ClientMessage::new(
            MessageType::StartPublication,
            0,
            PayloadType::Undefined,
            Bytes::new(),
        );
        if sink.send(Message::Binary(open.serialize())).await.is_err() {
            return;
        }
    } else if behaviour != Behaviour::LegacyNoHandshake {
        let session_type = match behaviour {
            Behaviour::SmuxEcho | Behaviour::SmuxFlood => "Port",
            _ => "Standard_Stream",
        };
        let request = serde_json::json!({
            "AgentVersion": "3.3.1345.0",
            "RequestedClientActions": [{
                "ActionType": "SessionType",
                "ActionParameters": { "SessionType": session_type }
            }]
        })
        .to_string();
        if !send(
            &mut sink,
            next_sequence!(),
            PayloadType::HandshakeRequest,
            Bytes::from(request),
        )
        .await
        {
            return;
        }
    } else {
        // A legacy agent just starts streaming.
        send(
            &mut sink,
            next_sequence!(),
            PayloadType::Output,
            Bytes::from_static(b"legacy-shell$ "),
        )
        .await;
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

                observed
                    .lock()
                    .await
                    .payload_order
                    .push(message.payload_type);

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
                            next_sequence!(),
                            PayloadType::HandshakeComplete,
                            Bytes::from(complete),
                        )
                        .await
                        {
                            return;
                        }

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
                            Behaviour::CloseAfterPause => {
                                tokio::time::sleep(CLOSE_PAUSE).await;
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
                                    next_sequence!(),
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
                                    next_sequence!(),
                                    PayloadType::EncChallengeRequest,
                                    Bytes::from(body),
                                )
                                .await
                                {
                                    return;
                                }
                            }
                            Behaviour::OutOfOrder => {
                                // Deliver 2, 1, 0 so the client must buffer and
                                // reassemble before anything reaches the caller.
                                let base =
                                    sequence.fetch_add(3, std::sync::atomic::Ordering::AcqRel);
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
                    _ if matches!(behaviour, Behaviour::SmuxEcho | Behaviour::SmuxFlood) => {
                        acknowledge(&mut sink, &message).await;
                        smux_buf.extend_from_slice(&message.payload);

                        while let Some((cmd, stream_id, payload)) = smux::decode(&mut smux_buf) {
                            let replies: Vec<Vec<u8>> = match cmd {
                                smux::SYN
                                    if behaviour == Behaviour::SmuxFlood && flooded.is_none() =>
                                {
                                    flooded = Some(stream_id);
                                    // One more than the client's per-stream
                                    // queue holds, so the last frame is the one
                                    // that cannot be delivered.
                                    (0..FLOOD_FRAMES)
                                        .map(|n| {
                                            smux::encode(
                                                smux::PSH,
                                                stream_id,
                                                format!("{n:04}").as_bytes(),
                                            )
                                        })
                                        .collect()
                                }
                                smux::SYN => Vec::new(),
                                smux::PSH => vec![smux::encode(smux::PSH, stream_id, &payload)],
                                smux::FIN => vec![smux::encode(smux::FIN, stream_id, &[])],
                                _ => Vec::new(),
                            };

                            for reply in replies {
                                if !send(
                                    &mut sink,
                                    next_sequence!(),
                                    PayloadType::Output,
                                    Bytes::from(reply),
                                )
                                .await
                                {
                                    return;
                                }
                            }
                        }
                    }
                    _ => {
                        observed.lock().await.inputs.push(message.payload.to_vec());

                        if behaviour != Behaviour::NeverAcknowledge {
                            acknowledge(&mut sink, &message).await;
                        }
                        if matches!(
                            behaviour,
                            Behaviour::Echo
                                | Behaviour::LegacyNoHandshake
                                | Behaviour::ResumeEcho
                                | Behaviour::EchoThenDrop
                        ) {
                            send(
                                &mut sink,
                                next_sequence!(),
                                PayloadType::Output,
                                message.payload.clone(),
                            )
                            .await;

                            // Die once the client has seen one echo, so the
                            // session has real sequence state to carry over.
                            if behaviour == Behaviour::EchoThenDrop {
                                return;
                            }
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
