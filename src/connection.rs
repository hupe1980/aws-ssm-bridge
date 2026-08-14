//! The MGS data channel: WebSocket transport, framing and message routing.
//!
//! # Task topology
//!
//! ```text
//!                    ┌──────────────┐
//!   Session::send ──►│ command task │──┐
//!                    └──────────────┘  │
//!                    ┌──────────────┐  ├──► writer task ──► WebSocket sink
//!   retransmit ─────►│  scheduler   │──┤
//!                    └──────────────┘  │
//!                    ┌──────────────┐  │
//!   heartbeat ──────►│    pinger    │──┘
//!                    └──────────────┘
//!
//!   WebSocket stream ──► reader task ──┬──► output fan-out (consumers)
//!                                      ├──► acknowledgements (writer task)
//!                                      └──► handshake / control handling
//! ```
//!
//! The WebSocket is split so the reader never waits on the writer. Exactly one
//! task owns the sink; everything else enqueues on a bounded channel, which
//! gives backpressure without a mutex on the hot path.
//!
//! Every task exits when [`SessionCore::close`] fires, and every exit path calls
//! it — so a failure anywhere (dead peer, protocol violation, socket error)
//! tears the whole session down and wakes anyone waiting on it. There is no
//! state in which the session looks alive but nothing is running.

use bytes::Bytes;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, trace, warn};
use zeroize::Zeroizing;

use crate::ack::{self, IncomingBuffer, OutgoingBuffer, RetransmitAction};
use crate::binary_protocol::{
    ClientMessage, ControlFlag, MessageType, PayloadType, MAX_PAYLOAD_SIZE,
};
use crate::errors::{Error, Result};
use crate::handshake::{
    self, EncryptionChallengeRequest, HandshakeComplete, HandshakeHandler, HandshakeRequest,
};
use crate::metrics::{self, names};
use crate::session::{CloseReason, SessionCore};

/// Schema version sent in the data-channel open message.
const MESSAGE_SCHEMA_VERSION: &str = "1.0";

/// How often the retransmit scheduler examines the outgoing buffer.
///
/// Matches `config.ResendSleepInterval` in the reference plugin.
const RETRANSMIT_TICK: Duration = Duration::from_millis(100);

/// Retransmission attempts before the session is declared unrecoverable.
///
/// Matches `config.ResendMaxAttempt`.
const MAX_RETRANSMIT_ATTEMPTS: u32 = 3000;

/// Unacknowledged messages held in each direction.
///
/// Matches `config.{In,Out}goingMessageBufferCapacity`.
const MESSAGE_BUFFER_CAPACITY: usize = 10_000;

/// Depth of the queue feeding the writer task.
const WRITER_QUEUE_DEPTH: usize = 1024;

/// Depth of the queue feeding the command task from [`Session::send`].
///
/// [`Session::send`]: crate::Session::send
pub(crate) const COMMAND_QUEUE_DEPTH: usize = 256;

/// WebSocket dial attempts before giving up.
///
/// Only network-level failures are retried; an HTTP error from the service
/// (expired token, wrong signature) will not fix itself.
const CONNECT_ATTEMPTS: u32 = 3;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsSource = SplitStream<WsStream>;

// ---------------------------------------------------------------------------
// Endpoint policy
// ---------------------------------------------------------------------------

/// Which WebSocket endpoints the client is willing to connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EndpointPolicy {
    /// Require `wss://` and an AWS-operated SSM messages endpoint.
    ///
    /// Accepts the regional, FIPS and VPC-endpoint hostname forms:
    ///
    /// ```text
    /// wss://ssmmessages.us-east-1.amazonaws.com/…
    /// wss://ssmmessages-fips.us-gov-west-1.amazonaws.com/…
    /// wss://ssmmessages.cn-north-1.amazonaws.com.cn/…
    /// wss://vpce-0abc-1def.ssmmessages.eu-central-1.vpce.amazonaws.com/…
    /// ```
    #[default]
    AwsOnly,
    /// Accept any `wss://` or `ws://` URL.
    ///
    /// Intended for tests against a local mock gateway. Using this against
    /// untrusted input removes the guard that keeps a tampered `StartSession`
    /// response from redirecting the session token to a host of the attacker's
    /// choosing.
    AllowAny,
}

impl EndpointPolicy {
    /// Check a stream URL against this policy.
    pub(crate) fn validate(self, raw: &str) -> Result<()> {
        let url = url::Url::parse(raw)
            .map_err(|e| Error::Config(format!("stream URL is not a valid URL: {e}")))?;

        if self == EndpointPolicy::AllowAny {
            return match url.scheme() {
                "ws" | "wss" => Ok(()),
                other => Err(Error::Config(format!(
                    "stream URL scheme must be ws or wss, got {other}"
                ))),
            };
        }

        if url.scheme() != "wss" {
            return Err(Error::Config(format!(
                "stream URL must use wss://, got {}://",
                url.scheme()
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| Error::Config("stream URL has no host".into()))?;

        let in_aws = host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn");
        // Require a whole label to be the service name, so `evil-ssmmessages…`
        // and `…ssmmessages.attacker.com` are both rejected while the VPC
        // endpoint form `vpce-….ssmmessages.<region>.vpce.amazonaws.com` passes.
        let is_ssm = host
            .split('.')
            .any(|label| label == "ssmmessages" || label.starts_with("ssmmessages-"));

        if in_aws && is_ssm {
            Ok(())
        } else {
            Err(Error::Config(format!(
                "refusing to connect to {host}: not an AWS SSM messages endpoint. \
                 Set SessionConfig::endpoint_policy to EndpointPolicy::AllowAny to override."
            )))
        }
    }
}

/// Strip credentials from a URL before it reaches a log line.
fn sanitize_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut url) => {
            let kept: Vec<(String, String)> = url
                .query_pairs()
                .filter(|(k, _)| !k.eq_ignore_ascii_case("tokenValue"))
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            url.set_query(None);
            if !kept.is_empty() {
                url.query_pairs_mut().extend_pairs(kept);
            }
            url.into()
        }
        Err(_) => "<unparseable URL>".to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Data channel open message
// ---------------------------------------------------------------------------

/// First message on the channel: authenticates the WebSocket to the gateway.
///
/// The token travels in this JSON body, never in the URL — matching
/// `FinalizeDataChannelHandshake` in the reference plugin and keeping the
/// credential out of proxy logs and connection traces.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct OpenDataChannelInput<'a> {
    message_schema_version: &'a str,
    request_id: String,
    token_value: &'a str,
    client_id: &'a str,
    client_version: &'a str,
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Work submitted to the data channel by the owning [`Session`].
///
/// [`Session`]: crate::Session
#[derive(Debug)]
pub(crate) enum Command {
    /// Stream bytes to the remote process (chunked and, if negotiated, encrypted).
    Data(Bytes),
    /// Send a single control payload verbatim.
    Control {
        payload_type: PayloadType,
        data: Bytes,
    },
}

/// Everything the data channel needs to establish itself.
pub(crate) struct ConnectionParams {
    pub session_id: String,
    pub target: String,
    pub stream_url: String,
    pub token: Zeroizing<String>,
    pub chunk_size: usize,
    pub endpoint_policy: EndpointPolicy,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    #[cfg(feature = "kms")]
    pub kms: Option<aws_sdk_kms::Client>,
}

// ---------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------

/// Open the data channel and spawn the tasks that drive it.
///
/// Returns the command sender for [`Session`] and the spawned task handles, so
/// the session can abort them if it is dropped without a clean terminate.
///
/// [`Session`]: crate::Session
pub(crate) async fn connect(
    core: Arc<SessionCore>,
    params: ConnectionParams,
) -> Result<(mpsc::Sender<Command>, Vec<JoinHandle<()>>)> {
    params.endpoint_policy.validate(&params.stream_url)?;

    info!(
        url = %sanitize_url(&params.stream_url),
        "opening SSM data channel"
    );

    let ws = dial(&params.stream_url).await?;
    let (mut sink, source) = ws.split();

    // Authenticate the channel before anything else is written to it.
    let open = OpenDataChannelInput {
        message_schema_version: MESSAGE_SCHEMA_VERSION,
        request_id: uuid::Uuid::new_v4().to_string(),
        token_value: &params.token,
        client_id: &params.session_id,
        client_version: handshake::CLIENT_PROTOCOL_VERSION,
    };
    // Held in a Zeroizing buffer: this string contains the session token.
    let open_json = Zeroizing::new(serde_json::to_string(&open)?);
    sink.send(Message::Text(open_json.as_str().into()))
        .await
        .map_err(|e| Error::transport(format!("failed to open the data channel: {e}")))?;
    debug!("data channel open message sent");

    let (writer_tx, writer_rx) = mpsc::channel::<Message>(WRITER_QUEUE_DEPTH);
    let (command_tx, command_rx) = mpsc::channel::<Command>(COMMAND_QUEUE_DEPTH);

    let shared = Arc::new(ChannelState {
        core: Arc::clone(&core),
        writer_tx: writer_tx.clone(),
        outgoing: OutgoingBuffer::new(MESSAGE_BUFFER_CAPACITY, MAX_RETRANSMIT_ATTEMPTS),
        sequence: tokio::sync::Mutex::new(0),
        last_inbound: Mutexed::new(Instant::now()),
        chunk_size: params.chunk_size.clamp(1, MAX_PAYLOAD_SIZE),
    });

    #[cfg(feature = "kms")]
    let handshake_handler = match params.kms {
        Some(client) => HandshakeHandler::with_kms(handshake::KmsContext {
            client,
            session_id: params.session_id.clone(),
            target_id: params.target.clone(),
        }),
        None => HandshakeHandler::new(),
    };
    #[cfg(not(feature = "kms"))]
    let handshake_handler = {
        let _ = &params.target;
        HandshakeHandler::new()
    };

    let tasks = vec![
        tokio::spawn(writer_task(sink, writer_rx, Arc::clone(&core))),
        tokio::spawn(reader_task(source, Arc::clone(&shared), handshake_handler)),
        tokio::spawn(command_task(command_rx, Arc::clone(&shared))),
        tokio::spawn(retransmit_task(Arc::clone(&shared))),
        tokio::spawn(heartbeat_task(
            Arc::clone(&shared),
            params.heartbeat_interval,
            params.idle_timeout,
        )),
    ];

    Ok((command_tx, tasks))
}

/// Dial the WebSocket, retrying only failures that a retry can fix.
async fn dial(url: &str) -> Result<WsStream> {
    let mut backoff = Duration::from_millis(200);
    for attempt in 1..=CONNECT_ATTEMPTS {
        match connect_async(url).await {
            Ok((ws, response)) => {
                debug!(status = ?response.status(), attempt, "WebSocket connected");
                return Ok(ws);
            }
            Err(e) => {
                // An HTTP rejection means the token or signature is bad; a TLS
                // failure means the trust chain is wrong. Neither improves on
                // a second attempt, and retrying just delays a clear error.
                let retriable = matches!(e, tungstenite::Error::Io(_));
                if !retriable || attempt == CONNECT_ATTEMPTS {
                    return Err(Error::transport(format!(
                        "could not open the SSM WebSocket: {e}"
                    )));
                }
                warn!(attempt, error = %e, "WebSocket dial failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
    }
    unreachable!("the loop returns on the final attempt")
}

// ---------------------------------------------------------------------------
// Shared channel state
// ---------------------------------------------------------------------------

/// A `std::sync::Mutex` wrapper that never propagates poisoning.
struct Mutexed<T>(std::sync::Mutex<T>);

impl<T> Mutexed<T> {
    fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(value))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// State shared by every task driving one data channel.
struct ChannelState {
    core: Arc<SessionCore>,
    writer_tx: mpsc::Sender<Message>,
    outgoing: OutgoingBuffer,
    /// Sequence counter for everything this client sends — one stream, one
    /// counter — behind the mutex that serialises the whole outbound path.
    ///
    /// Two tasks reach [`send_payload`] concurrently: the reader answering a
    /// handshake or an encryption challenge, and the command task streaming
    /// caller data. Allocating the number, recording the message for
    /// retransmission and handing it to the writer must therefore happen as one
    /// critical section. Interleaved, two messages would be stamped with the
    /// same sequence number and the counter would skip one, leaving a gap the
    /// agent waits on forever — the session hangs with no error anywhere.
    sequence: tokio::sync::Mutex<i64>,
    /// When the last frame of any kind arrived; drives dead-peer detection.
    last_inbound: Mutexed<Instant>,
    chunk_size: usize,
}

impl ChannelState {
    fn mark_inbound(&self) {
        *self.last_inbound.lock() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_inbound.lock().elapsed()
    }

    /// Enqueue a frame, waiting for room. Fails once the writer has stopped.
    async fn write(&self, message: Message) -> Result<()> {
        self.writer_tx
            .send(message)
            .await
            .map_err(|_| Error::transport("the data channel writer has stopped"))
    }

    /// Enqueue a frame without waiting; used where blocking would be worse than
    /// dropping (acknowledgements, keep-alive pings).
    fn try_write(&self, message: Message) -> bool {
        self.writer_tx.try_send(message).is_ok()
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Owns the WebSocket sink and writes whatever the other tasks queue.
async fn writer_task(mut sink: WsSink, mut rx: mpsc::Receiver<Message>, core: Arc<SessionCore>) {
    loop {
        tokio::select! {
            biased;
            () = core.closed() => break,
            message = rx.recv() => {
                let Some(message) = message else { break };
                if let Err(e) = sink.send(message).await {
                    // Distinguish a socket dying under us from an orderly close
                    // that the reader has already reported.
                    if core.is_closed() {
                        debug!(error = %e, "write failed during shutdown");
                    } else {
                        core.close(CloseReason::Transport(format!("WebSocket write failed: {e}")));
                    }
                    break;
                }
            }
        }
    }

    // Best-effort: tell the gateway we are going away so it can release the
    // session rather than waiting for its own idle timeout.
    let _ = sink.close().await;
    debug!("writer task finished");
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

/// Sends keep-alive pings and declares the peer dead when nothing comes back.
///
/// The reference plugin pings every five minutes and never checks for a reply,
/// so a silently dead connection can hang a session indefinitely. Here liveness
/// is judged on *any* inbound frame — data, pong, or control — which means a
/// busy session is never mistaken for a dead one, and a truly dead one is
/// detected within `idle_timeout`.
async fn heartbeat_task(state: Arc<ChannelState>, interval: Duration, idle_timeout: Duration) {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            () = state.core.closed() => break,
            _ = ticker.tick() => {
                let idle = state.idle_for();
                if idle > idle_timeout {
                    error!(
                        idle_secs = idle.as_secs(),
                        timeout_secs = idle_timeout.as_secs(),
                        "no traffic from the SSM agent within the idle timeout; \
                         treating the connection as dead"
                    );
                    state.core.close(CloseReason::PeerUnresponsive { idle });
                    break;
                }
                trace!(idle_ms = idle.as_millis(), "sending keep-alive ping");
                if !state.try_write(Message::Ping(Bytes::new())) {
                    // The queue being full means the writer is saturated with
                    // real work, which is itself proof of life. Skip this ping.
                    debug!("writer queue full; skipping keep-alive ping");
                }
            }
        }
    }
    debug!("heartbeat task finished");
}

// ---------------------------------------------------------------------------
// Retransmission
// ---------------------------------------------------------------------------

/// Resends the oldest unacknowledged message once its RTO expires.
async fn retransmit_task(state: Arc<ChannelState>) {
    let mut ticker = tokio::time::interval(RETRANSMIT_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            () = state.core.closed() => break,
            _ = ticker.tick() => match state.outgoing.poll_retransmit() {
                RetransmitAction::Idle => {}
                RetransmitAction::Resend { sequence, wire } => {
                    metrics::counter(names::RETRANSMISSIONS, 1);
                    // poll_retransmit already reset the timer, so this send must
                    // actually happen; race it against shutdown so a stalled
                    // socket cannot pin the task open.
                    tokio::select! {
                        biased;
                        () = state.core.closed() => break,
                        result = state.write(Message::Binary(wire)) => {
                            if result.is_err() {
                                debug!(sequence, "writer gone; stopping retransmission");
                                break;
                            }
                        }
                    }
                }
                RetransmitAction::GaveUp { sequence, attempts } => {
                    error!(
                        sequence,
                        attempts,
                        "the SSM agent never acknowledged a message; giving up on the session"
                    );
                    state.core.close(CloseReason::DeliveryFailed { sequence, attempts });
                    break;
                }
            },
        }
    }
    debug!("retransmit task finished");
}

// ---------------------------------------------------------------------------
// Command handling (outbound)
// ---------------------------------------------------------------------------

/// Turns caller-level commands into wire messages.
async fn command_task(mut rx: mpsc::Receiver<Command>, state: Arc<ChannelState>) {
    loop {
        let command = tokio::select! {
            biased;
            () = state.core.closed() => break,
            command = rx.recv() => match command {
                Some(c) => c,
                None => break,
            },
        };

        // Hold outbound data until the agent says it is listening. Anything sent
        // before `start_publication` (or the handshake completing) is discarded
        // by the gateway, which looks to the caller like lost keystrokes.
        let permitted = tokio::select! {
            biased;
            () = state.core.closed() => false,
            () = state.core.wait_sendable() => true,
        };
        if !permitted {
            break;
        }

        let result = match command {
            Command::Data(data) => send_stream_data(&state, data).await,
            Command::Control { payload_type, data } => {
                send_payload(&state, payload_type, data).await
            }
        };

        if let Err(e) = result {
            if state.core.is_closed() {
                debug!(error = %e, "send failed during shutdown");
            } else {
                error!(error = %e, "failed to send on the data channel");
                state.core.close(CloseReason::Transport(e.to_string()));
            }
            break;
        }
    }
    debug!("command task finished");
}

/// Split caller data into protocol-sized chunks and send each one.
async fn send_stream_data(state: &ChannelState, data: Bytes) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + state.chunk_size).min(data.len());
        send_payload(state, PayloadType::Output, data.slice(offset..end)).await?;
        offset = end;
    }
    Ok(())
}

/// Encrypt if required, serialize, track for retransmission, and enqueue.
async fn send_payload(
    state: &ChannelState,
    payload_type: PayloadType,
    payload: Bytes,
) -> Result<()> {
    // Only stream output is encrypted; handshake and control payloads carry the
    // key agreement itself and must stay readable to the gateway.
    let payload = match (payload_type, state.core.crypto()) {
        (PayloadType::Output, Some(crypto)) => crypto.encrypt(&payload)?,
        _ => payload,
    };

    // One sender at a time, for the whole allocate/track/write sequence. See
    // `ChannelState::sequence` for why splitting this up corrupts the stream.
    let mut sequence = state.sequence.lock().await;

    // Built once: nothing about the message changes while we wait for room, and
    // re-serializing would re-hash the payload on every retry — precisely when
    // the channel is already struggling.
    let wire = ClientMessage::new(
        MessageType::InputStreamData,
        *sequence,
        payload_type,
        payload,
    )
    .serialize();

    // Wait for buffer room rather than dropping: an unacknowledged backlog means
    // the agent is behind, and discarding a message would stall its in-order
    // stream permanently.
    loop {
        // Enqueue for the acknowledgement wakeup *before* testing for room, so
        // the acknowledgement that makes room cannot land in the gap and leave
        // this task parked until some later one happens to arrive.
        let acked = state.core.ack_notified();

        if state.outgoing.track(wire.clone(), *sequence) {
            *sequence += 1;
            break;
        }

        debug!("outgoing buffer full; waiting for acknowledgements");
        tokio::select! {
            biased;
            () = state.core.closed() => {
                return Err(Error::SessionClosed(
                    "session closed while waiting for acknowledgements".into(),
                ))
            }
            () = acked => {}
        }
    }

    metrics::counter(names::MESSAGES_SENT, 1);
    metrics::counter(names::BYTES_SENT, wire.len() as u64);

    // Still holding the lock: the writer queue is FIFO, so releasing it before
    // the enqueue would let a later message reach the socket ahead of an
    // earlier one and force the agent to reorder what it need not.
    state.write(Message::Binary(wire)).await
}

// ---------------------------------------------------------------------------
// Reader (inbound)
// ---------------------------------------------------------------------------

/// Per-reader state that evolves as the session progresses.
struct ReaderState {
    handshake: HandshakeHandler,
    /// Next `output_stream_data` sequence number we expect from the agent.
    expected_sequence: i64,
    incoming: IncomingBuffer,
    handshake_started: Option<Instant>,
}

/// Owns the WebSocket stream, decodes messages and routes them.
async fn reader_task(mut source: WsSource, state: Arc<ChannelState>, handshake: HandshakeHandler) {
    let mut reader = ReaderState {
        handshake,
        expected_sequence: 0,
        incoming: IncomingBuffer::new(MESSAGE_BUFFER_CAPACITY),
        handshake_started: None,
    };

    let reason = loop {
        let frame = tokio::select! {
            biased;
            () = state.core.closed() => return,
            frame = source.next() => frame,
        };

        // Any frame at all proves the peer is alive.
        state.mark_inbound();

        match frame {
            Some(Ok(Message::Binary(data))) => {
                metrics::counter(names::MESSAGES_RECEIVED, 1);
                metrics::counter(names::BYTES_RECEIVED, data.len() as u64);

                let message = match ClientMessage::deserialize(data) {
                    Ok(m) => m,
                    Err(e) => {
                        // A message we cannot parse is a message we cannot
                        // acknowledge, which stalls the agent's in-order stream.
                        // Failing loudly beats hanging.
                        break CloseReason::Protocol(e.to_string());
                    }
                };
                if let Err(e) = route(&state, &mut reader, message).await {
                    break CloseReason::Protocol(e.to_string());
                }
            }
            Some(Ok(Message::Text(text))) => {
                if let Some(reason) = handle_text(&state, text.as_str()) {
                    break reason;
                }
            }
            Some(Ok(Message::Close(frame))) => {
                info!(?frame, "gateway closed the WebSocket");
                break CloseReason::AgentClosed {
                    exit_code: state.core.exit_code(),
                    detail: frame.and_then(|f| {
                        let reason = f.reason.trim().to_owned();
                        (!reason.is_empty()).then_some(reason)
                    }),
                };
            }
            // tungstenite answers pings itself; nothing to do beyond the
            // liveness update already recorded above.
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
            Some(Err(e)) => break CloseReason::Transport(format!("WebSocket read failed: {e}")),
            None => break CloseReason::Transport("WebSocket stream ended".into()),
        }
    };

    state.core.close(reason);
    debug!("reader task finished");
}

/// Handle the plain-text control messages the gateway sends outside the binary
/// framing.
fn handle_text(state: &ChannelState, text: &str) -> Option<CloseReason> {
    match text.trim() {
        "start_publication" => {
            debug!("gateway is ready to receive (text start_publication)");
            state.core.set_sendable(true);
            None
        }
        "pause_publication" => {
            debug!("gateway asked us to pause sending (text pause_publication)");
            state.core.set_sendable(false);
            None
        }
        "channel_closed" => Some(CloseReason::AgentClosed {
            exit_code: state.core.exit_code(),
            detail: None,
        }),
        other => {
            debug!(preview = %truncate(other, 200), "ignoring unrecognised text frame");
            None
        }
    }
}

async fn route(
    state: &ChannelState,
    reader: &mut ReaderState,
    message: ClientMessage,
) -> Result<()> {
    match message.message_type {
        MessageType::OutputStreamData => route_stream_data(state, reader, message).await,
        MessageType::Acknowledge => {
            let content = ack::parse_ack(&message)?;
            metrics::counter(names::ACKS_RECEIVED, 1);
            if state.outgoing.acknowledge(content.sequence_number) {
                state.core.notify_ack();
                let rtt = state.outgoing.rtt();
                metrics::histogram(names::RTT_SECONDS, rtt.smoothed.as_secs_f64());
            }
            Ok(())
        }
        MessageType::StartPublication => {
            debug!("gateway is ready to receive");
            state.core.set_sendable(true);
            Ok(())
        }
        MessageType::PausePublication => {
            debug!("gateway asked us to pause sending");
            state.core.set_sendable(false);
            Ok(())
        }
        MessageType::ChannelClosed => {
            let detail = channel_closed_detail(&message.payload);
            info!(detail = ?detail, "agent closed the channel");
            state.core.close(CloseReason::AgentClosed {
                exit_code: state.core.exit_code(),
                detail,
            });
            Ok(())
        }
        MessageType::InputStreamData => {
            // We are the client; the agent never sends us input.
            warn!("ignoring unexpected input_stream_data from the agent");
            Ok(())
        }
    }
}

/// Apply the agent's in-order delivery contract.
///
/// * `seq == expected` — process it, acknowledge, then drain anything the gap
///   was blocking.
/// * `seq > expected` — buffer and acknowledge, so the agent stops resending it.
/// * `seq < expected` — already processed. Do **not** acknowledge: a second
///   acknowledgement for a retired sequence number confuses the agent's own
///   buffer accounting.
async fn route_stream_data(
    state: &ChannelState,
    reader: &mut ReaderState,
    message: ClientMessage,
) -> Result<()> {
    use std::cmp::Ordering as Ord;

    match message.sequence_number.cmp(&reader.expected_sequence) {
        Ord::Less => {
            trace!(
                sequence = message.sequence_number,
                expected = reader.expected_sequence,
                "dropping an already-processed message without acknowledging"
            );
            Ok(())
        }
        Ord::Greater => {
            let sequence = message.sequence_number;
            if reader.incoming.insert(message.clone()) {
                acknowledge(state, &message, false);
                trace!(
                    sequence,
                    expected = reader.expected_sequence,
                    buffered = reader.incoming.len(),
                    "buffered an out-of-order message"
                );
            } else {
                // No room: staying silent makes the agent retransmit once we
                // have caught up, which is exactly what we want.
                warn!(
                    sequence,
                    "reorder buffer full; dropping without acknowledging"
                );
            }
            Ok(())
        }
        Ord::Equal => {
            acknowledge(state, &message, true);
            deliver(state, reader, message).await?;
            reader.expected_sequence += 1;

            // Drain whatever the gap was holding back.
            while let Some(buffered) = reader.incoming.take(reader.expected_sequence) {
                deliver(state, reader, buffered).await?;
                reader.expected_sequence += 1;
            }
            Ok(())
        }
    }
}

fn acknowledge(state: &ChannelState, message: &ClientMessage, sequential: bool) {
    match ack::build_ack(message, sequential) {
        Ok(reply) => {
            if !state.try_write(Message::Binary(reply.serialize())) {
                // The agent retransmits unacknowledged messages, so a dropped
                // acknowledgement costs latency rather than correctness.
                warn!(
                    sequence = message.sequence_number,
                    "writer queue full; dropped an acknowledgement"
                );
            }
        }
        Err(e) => error!(error = %e, "could not build an acknowledgement"),
    }
}

/// Act on one in-order stream-data message.
async fn deliver(
    state: &ChannelState,
    reader: &mut ReaderState,
    message: ClientMessage,
) -> Result<()> {
    match message.payload_type {
        PayloadType::Output | PayloadType::StdErr | PayloadType::Undefined => {
            // Agents older than the handshake protocol start streaming
            // immediately. Seeing output before any handshake means this is one
            // of them, and we may start sending.
            if !state.core.is_sendable() {
                debug!("agent sent output without a handshake; enabling sending");
                state.core.set_sendable(true);
            }
            if message.payload.is_empty() {
                return Ok(());
            }
            let payload = decrypt_if_needed(state, &message)?;
            state.core.emit_output(payload);
            Ok(())
        }

        PayloadType::HandshakeRequest => {
            let request: HandshakeRequest = serde_json::from_slice(&message.payload)
                .map_err(|e| Error::protocol(format!("malformed HandshakeRequest: {e}")))?;
            reader.handshake_started.get_or_insert_with(Instant::now);

            let Some(response) = reader.handshake.on_request(request).await? else {
                return Ok(()); // duplicate; response already sent
            };
            let failed = reader.handshake.state() == handshake::HandshakeState::Failed;
            let errors = response.errors.join("; ");

            // Send the response even when actions failed: the agent needs it to
            // report a useful reason to the operator before ending the session.
            let payload = handshake::response_payload(&response)?;
            send_payload(state, PayloadType::HandshakeResponse, payload).await?;
            debug!("handshake response sent");

            if failed {
                return Err(Error::Unsupported(errors));
            }
            state
                .core
                .set_agent_version(reader.handshake.agent_version());
            if let Some(crypto) = reader.handshake.crypto() {
                info!("session encryption negotiated (AES-256-GCM)");
                state.core.set_crypto(crypto);
            }
            Ok(())
        }

        PayloadType::HandshakeComplete => {
            let complete: HandshakeComplete = serde_json::from_slice(&message.payload)
                .map_err(|e| Error::protocol(format!("malformed HandshakeComplete: {e}")))?;
            let banner = reader.handshake.on_complete(complete)?;
            if let Some(started) = reader.handshake_started.take() {
                metrics::histogram(names::HANDSHAKE_SECONDS, started.elapsed().as_secs_f64());
            }
            state.core.set_session_banner(banner);
            state.core.set_sendable(true);
            Ok(())
        }

        PayloadType::EncChallengeRequest => {
            let request: EncryptionChallengeRequest = serde_json::from_slice(&message.payload)
                .map_err(|e| Error::protocol(format!("malformed encryption challenge: {e}")))?;
            let response = reader.handshake.answer_challenge(&request)?;
            let payload = Bytes::from(serde_json::to_vec(&response)?);
            send_payload(state, PayloadType::EncChallengeResponse, payload).await?;
            debug!("answered the agent's encryption challenge");
            Ok(())
        }

        PayloadType::ExitCode => {
            let payload = decrypt_if_needed(state, &message)?;
            let exit_code = std::str::from_utf8(&payload)
                .ok()
                .and_then(|s| s.trim().parse::<i32>().ok());
            info!(?exit_code, "remote process exited");
            state.core.set_exit_code(exit_code);
            Ok(())
        }

        PayloadType::Flag => {
            match ControlFlag::from_payload(&message.payload) {
                Some(ControlFlag::TerminateSession) => {
                    info!("agent signalled session termination");
                    state.core.close(CloseReason::AgentClosed {
                        exit_code: state.core.exit_code(),
                        detail: None,
                    });
                }
                Some(flag) => debug!(?flag, "received a control flag"),
                None => debug!("received an unrecognised control flag"),
            }
            Ok(())
        }

        PayloadType::Error => {
            let text = String::from_utf8_lossy(&message.payload);
            warn!(error = %truncate(&text, 500), "agent reported an error");
            Ok(())
        }

        other => {
            debug!(payload_type = ?other, "ignoring an unhandled payload type");
            Ok(())
        }
    }
}

/// Extract the human-readable reason from a `channel_closed` payload.
///
/// The agent sends a JSON body whose `Output` field carries the explanation
/// ("Connection refused", a plugin error, an operator termination). Older and
/// non-conforming agents send bare text instead, so fall back to that rather
/// than discarding the only diagnostic the operator will get.
fn channel_closed_detail(payload: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ChannelClosed {
        #[serde(rename = "Output")]
        output: Option<String>,
    }

    let text = match serde_json::from_slice::<ChannelClosed>(payload) {
        Ok(parsed) => parsed.output.unwrap_or_default(),
        Err(_) => String::from_utf8_lossy(payload).into_owned(),
    };
    let trimmed = truncate(text.trim(), 500);
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn decrypt_if_needed(state: &ChannelState, message: &ClientMessage) -> Result<Bytes> {
    match state.core.crypto() {
        Some(crypto) if message.payload_type.is_encrypted_inbound() => {
            crypto.decrypt(&message.payload)
        }
        _ => Ok(message.payload.clone()),
    }
}

fn truncate(text: &str, max: usize) -> &str {
    match text.char_indices().nth(max) {
        Some((idx, _)) => &text[..idx],
        None => text,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_endpoints_are_accepted() {
        for url in [
            "wss://ssmmessages.us-east-1.amazonaws.com/v1/data-channel/abc",
            "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/abc?role=publish",
            "wss://ssmmessages-fips.us-gov-west-1.amazonaws.com/v1/data-channel/abc",
            "wss://ssmmessages.cn-north-1.amazonaws.com.cn/v1/data-channel/abc",
        ] {
            EndpointPolicy::AwsOnly
                .validate(url)
                .unwrap_or_else(|e| panic!("{url} should be accepted: {e}"));
        }
    }

    /// PrivateLink hostnames put the service name in a middle label. Rejecting
    /// them breaks every VPC-endpoint deployment without private DNS.
    #[test]
    fn vpc_endpoint_hostnames_are_accepted() {
        EndpointPolicy::AwsOnly
            .validate("wss://vpce-0abc1def.ssmmessages.eu-central-1.vpce.amazonaws.com/v1/x")
            .expect("VPC endpoint hostnames must work");
    }

    #[test]
    fn non_ssm_and_lookalike_hosts_are_rejected() {
        for url in [
            "wss://evil.com/v1/data-channel/abc",
            "wss://ssmmessages.attacker.com/steal",
            "wss://s3.us-east-1.amazonaws.com/bucket",
            "wss://amazonaws.com.evil.com/fake",
            // A prefix match would let this through; a whole-label match does not.
            "wss://evil-ssmmessages.us-east-1.amazonaws.com/v1/x",
        ] {
            assert!(
                EndpointPolicy::AwsOnly.validate(url).is_err(),
                "{url} must be rejected"
            );
        }
    }

    #[test]
    fn plaintext_websocket_is_rejected_under_the_default_policy() {
        let err = EndpointPolicy::AwsOnly
            .validate("ws://ssmmessages.us-east-1.amazonaws.com/v1/x")
            .unwrap_err();
        assert!(err.to_string().contains("wss://"), "{err}");
    }

    #[test]
    fn allow_any_accepts_local_mocks_but_still_requires_websocket_scheme() {
        assert!(EndpointPolicy::AllowAny
            .validate("ws://127.0.0.1:9001/x")
            .is_ok());
        assert!(EndpointPolicy::AllowAny
            .validate("wss://localhost/x")
            .is_ok());
        assert!(EndpointPolicy::AllowAny
            .validate("http://127.0.0.1/x")
            .is_err());
    }

    #[test]
    fn sanitize_url_strips_the_token_and_keeps_everything_else() {
        let raw = "wss://ssmmessages.us-east-1.amazonaws.com/v1/data-channel/abc\
                   ?role=publish&tokenValue=SECRET&cell-number=7";
        let clean = sanitize_url(raw);
        assert!(!clean.contains("SECRET"), "{clean}");
        assert!(!clean.contains("tokenValue"), "{clean}");
        assert!(clean.contains("role=publish"), "{clean}");
        assert!(clean.contains("cell-number=7"), "{clean}");
    }

    #[test]
    fn sanitize_url_is_case_insensitive_about_the_token_parameter() {
        let clean = sanitize_url("wss://h.amazonaws.com/?TokenValue=a&TOKENVALUE=b&tokenvalue=c");
        for secret in ["a", "b", "c"] {
            assert!(!clean.contains(&format!("={secret}")), "{clean}");
        }
    }

    #[test]
    fn sanitize_url_handles_garbage() {
        assert_eq!(sanitize_url("not a url"), "<unparseable URL>");
    }

    /// The token must never reach the URL: it would land in proxy logs and in
    /// any tracing that records the connection target.
    #[test]
    fn open_message_carries_the_token_not_the_url() {
        let open = OpenDataChannelInput {
            message_schema_version: MESSAGE_SCHEMA_VERSION,
            request_id: "req-1".into(),
            token_value: "SECRET-TOKEN",
            client_id: "session-1",
            client_version: handshake::CLIENT_PROTOCOL_VERSION,
        };
        let json = serde_json::to_string(&open).unwrap();
        assert!(json.contains("\"TokenValue\":\"SECRET-TOKEN\""), "{json}");
        assert!(json.contains("\"MessageSchemaVersion\":\"1.0\""), "{json}");
        assert!(json.contains("\"ClientId\":\"session-1\""), "{json}");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 3), "hel");
        assert_eq!(truncate("hello", 99), "hello");
        // Multi-byte input must not panic or split a code point.
        assert_eq!(truncate("äöü", 2), "äö");
    }
}
