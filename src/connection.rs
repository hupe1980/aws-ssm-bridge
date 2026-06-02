//! Connection manager for handling session lifecycle and message processing
//!
//! Production-grade implementation with:
//! - **Split read/write streams** - Eliminates deadlocks by separating concerns
//! - Automatic retry with exponential backoff
//! - Structured tracing with spans
//! - Heartbeat monitoring with timeout detection
//! - Graceful shutdown handling
//! - AWS SSM binary protocol support
//! - **DoS protection** - Message rate limiting, size limits, buffer bounds

use bytes::Bytes;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::ack::{AckTracker, IncomingMessageBuffer, OutgoingMessageBuffer};
use crate::binary_protocol::{ClientMessage, PayloadType};
use crate::channels::ChannelMultiplexer;
use crate::errors::{Error, Result, TransportError};
use crate::handshake::{
    HandshakeConfig, HandshakeHandler, HandshakeRequest, HandshakeResponse, HandshakeState,
};
use crate::metrics::{self, names as MetricNames};
use crate::protocol::MessageType;
use crate::rate_limit::{RateLimitConfig, RateLimiter};
use crate::retry::{retry_with_backoff, RetryConfig};
use zeroize::Zeroize;

/// Maximum message size (10MB) - prevent DoS
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Heartbeat interval (30 seconds)
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Maximum incoming messages per second - DoS protection
/// This is generous for normal usage but prevents flood attacks
const MAX_MESSAGES_PER_SECOND: f64 = 5000.0;

/// Message schema version
const MESSAGE_SCHEMA_VERSION: &str = "1.0";

/// Client version — identifies this library to the SSM service.
/// Treated as an opaque string by the SSM agent; the format is not required
/// to match the official plugin's numeric version string.
const CLIENT_VERSION: &str = concat!("aws-ssm-bridge/", env!("CARGO_PKG_VERSION"));

/// Buffer capacity for out-of-order messages (matches AWS default)
const INCOMING_BUFFER_CAPACITY: usize = 10000;

/// Buffer capacity for unACKed outgoing messages (matches AWS default)
const OUTGOING_BUFFER_CAPACITY: usize = 10000;

/// Retransmission check interval (200ms - matches AWS ResendSleepInterval)
const RETRANSMIT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Maximum retransmission attempts (3000 per AWS = 5 minutes / 200ms interval)
const MAX_RETRANSMIT_ATTEMPTS: u32 = 3000;

/// Buffer capacity for the lock-free WebSocket writer channel.
///
/// Bounded at 2× the retransmit buffer so heavy retransmit storms do not
/// cause unbounded memory growth when the remote end stalls.  Senders that
/// exceed this capacity will block (`.await`) until capacity is available;
/// an error is only returned when the receiver has been dropped.
const WRITER_CHANNEL_CAPACITY: usize = OUTGOING_BUFFER_CAPACITY * 2;

/// Maximum consecutive missed pong responses before declaring connection dead
const MAX_MISSED_PONGS: u32 = 3;
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

/// Open data channel input - sent as JSON after WebSocket connects.
///
/// **Security**: Has a manual `Debug` implementation that redacts `token_value`
/// to prevent accidental token leakage in logs.
/// Implements `Zeroize` to scrub `token_value` from memory on drop.
/// `Clone` is intentionally absent — the struct itself cannot be cloned,
/// preventing accidental duplication of this security-sensitive value.
#[derive(Serialize, Deserialize, Zeroize)]
#[zeroize(drop)]
#[serde(rename_all = "PascalCase")]
struct OpenDataChannelInput {
    message_schema_version: String,
    request_id: String,
    token_value: String,
    client_id: String,
    client_version: String,
}

// Manual Debug impl that redacts the token
impl std::fmt::Debug for OpenDataChannelInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenDataChannelInput")
            .field("message_schema_version", &self.message_schema_version)
            .field("request_id", &self.request_id)
            .field("token_value", &"[REDACTED]")
            .field("client_id", &self.client_id)
            .field("client_version", &self.client_version)
            .finish()
    }
}

/// Connection manager handles WebSocket lifecycle and message routing
///
/// Uses split read/write streams to allow concurrent send/receive without deadlock.
/// The WebSocket is split into:
/// - Writer (SplitSink) - owned by a dedicated writer task fed via mpsc channel (lock-free)
/// - Reader (SplitStream) - consumed by dedicated receiver task
pub struct ConnectionManager {
    /// Session ID
    session_id: String,

    /// Write channel — all tasks send WebSocket frames here (lock-free, bounded)
    writer_tx: mpsc::Sender<Message>,

    /// Channel multiplexer
    channels: Arc<ChannelMultiplexer>,

    /// Command receiver
    command_rx: mpsc::UnboundedReceiver<ManagerCommand>,

    /// Shutdown signal
    shutdown_tx: tokio::sync::broadcast::Sender<()>,

    /// Task handles
    tasks: Vec<JoinHandle<()>>,

    /// Global sequence number counter for ALL outgoing messages
    sequence: Arc<std::sync::atomic::AtomicI64>,

    /// Publication state - whether we've received start_publication
    can_send: Arc<std::sync::atomic::AtomicBool>,

    /// Notified when can_send transitions to true
    #[allow(dead_code)] // Kept alive via Arc; actual notification happens in receiver task
    ready_notify: Arc<tokio::sync::Notify>,

    /// Outgoing message buffer for reliable delivery with retransmission
    outgoing_buffer: Arc<OutgoingMessageBuffer>,
}

/// Commands sent to the connection manager
#[derive(Debug)]
pub enum ManagerCommand {
    /// Send data to stdin (uses PayloadType::Output)
    SendData(Bytes),
    /// Send a message with specific payload type
    SendMessage {
        /// Data payload
        data: Bytes,
        /// Payload type discriminator
        payload_type: crate::binary_protocol::PayloadType,
    },
    /// Terminate connection
    Terminate,
}

/// Shared context passed through the receiver task's message-processing pipeline.
///
/// Groups the five references that every internal routing function needs,
/// reducing `route_message` from 11 parameters to 6 and `process_output_message`
/// from 7 to 3.
struct ReceiverContext {
    channels: Arc<ChannelMultiplexer>,
    writer_tx: mpsc::Sender<Message>,
    can_send: Arc<std::sync::atomic::AtomicBool>,
    ready_notify: Arc<tokio::sync::Notify>,
    sequence: Arc<std::sync::atomic::AtomicI64>,
}

/// Mutable per-receiver state that travels with the message-processing pipeline.
///
/// Groups the evolving state that `route_message` and friends need,
/// keeping function signatures lean.
struct ReceiverState {
    handshake_handler: HandshakeHandler,
    expected_sequence: i64,
    handshake_started: Option<Instant>,
}

impl ConnectionManager {
    /// Create a new connection manager and establish WebSocket connection
    /// Uses retry logic with exponential backoff for resilience
    #[instrument(skip(token_value, command_rx, ready_notify), fields(session_id = %session_id))]
    pub async fn connect(
        session_id: String,
        stream_url: String,
        token_value: String,
        command_rx: mpsc::UnboundedReceiver<ManagerCommand>,
        ready_notify: Arc<tokio::sync::Notify>,
    ) -> Result<Self> {
        // Validate stream URL to prevent SSRF attacks
        Self::validate_stream_url(&stream_url)?;

        // Build WebSocket URL with authentication token
        // SECURITY: Token is in query params but this URL is NEVER logged
        // Use url::Url for proper encoding of special characters in token/session ID
        let mut parsed_url = url::Url::parse(&stream_url)
            .map_err(|e| Error::Config(format!("Invalid stream URL: {}", e)))?;
        parsed_url
            .query_pairs_mut()
            .append_pair("sessionId", &session_id)
            .append_pair("tokenValue", &token_value);
        let ws_url = parsed_url.to_string();

        // Log sanitized URL (without token) for debugging
        info!(url = %Self::sanitize_url(&stream_url), "Attempting WebSocket connection");

        // Connect with retry logic for resilience
        let retry_config = RetryConfig::default();

        let (ws_stream, response) = retry_with_backoff(
            retry_config,
            || async {
                connect_async(&ws_url).await.map_err(|e| {
                    warn!(error = ?e, "WebSocket connection attempt failed");
                    TransportError::ConnectionFailed(e.to_string()).into()
                })
            },
            "websocket_connect",
        )
        .await?;

        info!(
            status = ?response.status(),
            headers = ?response.headers().len(),
            "WebSocket connection established"
        );

        // Split into read/write halves BEFORE sending handshake
        // This is crucial to avoid deadlocks - each half can be used independently
        let (mut writer, reader) = ws_stream.split();

        // Send data channel handshake (OpenDataChannelInput) as JSON text message
        // This is required by AWS SSM protocol before any binary messages
        let handshake = OpenDataChannelInput {
            message_schema_version: MESSAGE_SCHEMA_VERSION.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            token_value: token_value.clone(),
            client_id: session_id.clone(),
            client_version: CLIENT_VERSION.to_string(),
        };

        let handshake_json = serde_json::to_string(&handshake).map_err(Error::Serialization)?;

        // Note: We do NOT log handshake_json as it contains token_value (credential)
        debug!("Sending data channel handshake");

        writer
            .send(Message::Text(handshake_json.into()))
            .await
            .map_err(|e| TransportError::WebSocket(e.to_string()))?;

        info!("Data channel handshake sent");

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(16);
        let channels = Arc::new(ChannelMultiplexer::new());
        let can_send = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let pong_received = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let sequence = Arc::new(std::sync::atomic::AtomicI64::new(0));

        // Create outgoing message buffer for reliable delivery (before receiver task)
        let outgoing_buffer = Arc::new(OutgoingMessageBuffer::new(OUTGOING_BUFFER_CAPACITY));

        // Create bounded write channel — provides backpressure when the
        // WebSocket writer stalls (prevents OOM under slow remote endpoints).
        let (writer_tx, writer_rx) = mpsc::channel::<Message>(WRITER_CHANNEL_CAPACITY);

        // Spawn dedicated writer task (owns the WsWriter sink; also runs the
        // heartbeat so pings are sent directly to the socket without going through
        // the writer channel, eliminating the enqueue-vs-write pending_pong race).
        let writer_task = Self::spawn_writer_task(
            writer,
            writer_rx,
            shutdown_tx.subscribe(),
            Arc::clone(&pong_received),
            shutdown_tx.clone(),
        );

        // Spawn receiver task immediately with the reader half
        // This task owns the reader and runs independently of send operations
        let receiver_task = Self::spawn_receiver_task(
            reader,
            ReceiverContext {
                channels: Arc::clone(&channels),
                writer_tx: writer_tx.clone(),
                can_send: Arc::clone(&can_send),
                ready_notify: Arc::clone(&ready_notify),
                sequence: Arc::clone(&sequence),
            },
            Arc::clone(&outgoing_buffer),
            Arc::clone(&pong_received),
            shutdown_tx.subscribe(),
        );

        Ok(Self {
            session_id,
            writer_tx,
            channels,
            command_rx,
            shutdown_tx,
            tasks: vec![writer_task, receiver_task],
            sequence,
            can_send,
            ready_notify,
            outgoing_buffer,
        })
    }

    /// Get the channel multiplexer
    pub fn channels(&self) -> Arc<ChannelMultiplexer> {
        Arc::clone(&self.channels)
    }

    /// Get the publication state (whether sending is allowed)
    pub fn can_send(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.can_send)
    }

    /// Get shutdown receiver
    pub fn shutdown_rx(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Run the connection manager (spawns background tasks)
    pub async fn run(mut self) -> Result<()> {
        info!(session_id = %self.session_id, "Starting connection manager");

        // Spawn retransmission scheduler task (matches AWS ResendStreamDataMessageScheduler)
        let retransmit_task = self.spawn_retransmit_task();
        self.tasks.push(retransmit_task);

        // Subscribe BEFORE entering the loop so that any shutdown_tx.send(())
        // fired by heartbeat/retransmit tasks (dead-connection detection, retransmit
        // timeout) wakes this select! promptly.  Without this arm the loop can
        // be stuck waiting for the next command while all background tasks have
        // already exited, leaving the session in a zombie state.
        let mut shutdown_rx = self.shutdown_rx();

        // Main command processing loop
        debug!("Entering command processing loop");
        loop {
            tokio::select! {
                biased;

                _ = shutdown_rx.recv() => {
                    info!("Internal shutdown triggered — exiting command loop");
                    break;
                }

                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        ManagerCommand::SendData(data) => {
                            debug!(len = data.len(), "Processing SendData command");
                            if let Err(e) = self.send_data(data).await {
                                if e.is_shutdown_related() {
                                    debug!(error = ?e, "Send failed (connection closing)");
                                } else {
                                    error!(error = ?e, "Failed to send data");
                                }
                            }
                        }
                        ManagerCommand::SendMessage { data, payload_type } => {
                            debug!(len = data.len(), ?payload_type, "Processing SendMessage command");
                            if let Err(e) = self.send_message(data, payload_type).await {
                                if e.is_shutdown_related() {
                                    debug!(error = ?e, "Send failed (connection closing)");
                                } else {
                                    error!(error = ?e, "Failed to send message");
                                }
                            }
                        }
                        ManagerCommand::Terminate => {
                            info!("Terminating connection");
                            break;
                        }
                    }
                }

                else => {
                    warn!("Command channel closed");
                    break;
                }
            }
        }

        // Shutdown gracefully
        self.shutdown().await?;

        Ok(())
    }

    /// Spawn task to receive and route messages
    ///
    /// This task owns the reader half of the WebSocket stream and runs independently.
    /// It cannot deadlock with send operations because they use separate stream halves.
    fn spawn_receiver_task(
        mut reader: WsReader,
        ctx: ReceiverContext,
        outgoing_buffer: Arc<OutgoingMessageBuffer>,
        pong_received: Arc<std::sync::atomic::AtomicBool>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            debug!("Receiver task started");

            let mut state = ReceiverState {
                handshake_handler: HandshakeHandler::new(HandshakeConfig::default()),
                expected_sequence: 0,
                handshake_started: None,
            };
            // Buffer for out-of-order messages (like AWS IncomingMessageBuffer)
            let incoming_buffer = IncomingMessageBuffer::new(INCOMING_BUFFER_CAPACITY);

            // Rate limiter for DoS protection - prevents message flood attacks
            let rate_limiter = RateLimiter::new(RateLimitConfig {
                tokens_per_second: MAX_MESSAGES_PER_SECOND,
                bucket_size: 1000, // Allow burst of 1000 messages
                initial_tokens: Some(1000),
            });
            let mut rate_limit_warnings: u32 = 0;

            loop {
                tokio::select! {
                    biased;  // Check shutdown first for faster termination

                    _ = shutdown_rx.recv() => {
                        debug!("Receiver task shutting down");
                        break;
                    }

                    msg_result = reader.next() => {
                        match msg_result {
                            Some(Ok(Message::Binary(data))) => {
                                // Rate limit check - DoS protection
                                if !rate_limiter.try_acquire() {
                                    rate_limit_warnings += 1;
                                    if rate_limit_warnings % 100 == 1 {
                                        warn!(
                                            warnings = rate_limit_warnings,
                                            "Message rate limit exceeded, dropping messages"
                                        );
                                    }
                                    continue;
                                }

                                if data.len() > MAX_MESSAGE_SIZE {
                                    error!(size = data.len(), "Message too large, dropping");
                                    continue;
                                }

                                trace!(size = data.len(), "Received binary message");

                                // Record metrics
                                metrics::counter(MetricNames::MESSAGES_RECEIVED, 1, &[]);
                                metrics::counter(MetricNames::BYTES_RECEIVED, data.len() as u64, &[]);

                                // data is already Bytes (reference counted) — clone is cheap
                                match ClientMessage::deserialize(data.clone()) {
                                    Ok(msg) => {
                                        // Advisory digest check: warn but still process.
                                        // Some AWS SSM agent versions send messages where
                                        // payload_digest was computed over a different byte
                                        // sequence.  Dropping these messages can break the
                                        // session (e.g. HandshakeComplete or start_publication
                                        // gets silently lost).  Authentication is the TLS/SigV4
                                        // layer, not this field.
                                        if !msg.verify_digest() {
                                            warn!(
                                                message_type = %msg.message_type,
                                                sequence = msg.sequence_number,
                                                payload_type = ?msg.payload_type,
                                                "Payload digest mismatch (known AWS agent quirk); \
                                                 processing message anyway"
                                            );
                                        }
                                        debug!(
                                            message_type = %msg.message_type,
                                            sequence = msg.sequence_number,
                                            payload_type = ?msg.payload_type,
                                            payload_type_raw = msg.payload_type as u32,
                                            payload_len = msg.payload.len(),
                                            "Parsed binary message"
                                        );

                                        // Extra debug: log first bytes of payload if it looks like JSON
                                        // Guarded to avoid allocation when debug logging is disabled
                                        if tracing::enabled!(tracing::Level::DEBUG) && !msg.payload.is_empty() {
                                            if let Ok(s) = std::str::from_utf8(&msg.payload) {
                                                if s.starts_with('{') {
                                                    debug!(payload_preview = %s[..s.len().min(300)], "Payload content");
                                                }
                                            }
                                        }
                                        if let Err(e) = Self::route_message(
                                            &ctx,
                                            msg,
                                            &mut state,
                                            &outgoing_buffer,
                                            &incoming_buffer,
                                            data,
                                        ).await {
                                            error!(error = ?e, "Failed to route message");
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = ?e, "Failed to deserialize message");
                                    }
                                }
                            }
                            Some(Ok(Message::Text(text))) => {
                                debug!(len = text.len(), "Received text message: {}", &text[..text.len().min(200)]);

                                // Handle control messages sent as TEXT (per AWS protocol)
                                let text_trimmed = text.trim();
                                if text_trimmed == "start_publication" {
                                    info!("Received TEXT start_publication - ready to send data");
                                    ctx.can_send.store(true, std::sync::atomic::Ordering::SeqCst);
                                    ctx.ready_notify.notify_waiters();
                                } else if text_trimmed == "pause_publication" {
                                    debug!("Received TEXT pause_publication - pausing data send");
                                    ctx.can_send.store(false, std::sync::atomic::Ordering::SeqCst);
                                } else if text_trimmed == "channel_closed" {
                                    info!("Received TEXT channel_closed");
                                    ctx.channels.close();
                                    break;
                                } else if text_trimmed.starts_with('{') {
                                    // Might be JSON - try to parse
                                    debug!("Received JSON text message");
                                } else {
                                    debug!("Received unknown text message type");
                                }
                            }
                            Some(Ok(Message::Close(frame))) => {
                                info!(?frame, "WebSocket close frame received");
                                ctx.channels.close();
                                break;
                            }
                            Some(Ok(Message::Ping(_))) => {
                                trace!("Received ping");
                            }
                            Some(Ok(Message::Pong(_))) => {
                                trace!("Received pong");
                                pong_received.store(true, std::sync::atomic::Ordering::SeqCst);
                            }
                            Some(Ok(Message::Frame(_))) => {
                                trace!("Received raw frame");
                            }
                            Some(Err(e)) => {
                                error!(error = ?e, "WebSocket error");
                                ctx.channels.close();
                                break;
                            }
                            None => {
                                info!("WebSocket stream ended");
                                ctx.channels.close();
                                break;
                            }
                        }
                    }
                }
            }
            debug!("Receiver task exited");
        })
    }

    /// Spawn dedicated writer task that owns the WebSocket sink.
    ///
    /// All other tasks send `Message` values through a bounded mpsc channel.
    /// This eliminates mutex contention between retransmit, receiver ACKs,
    /// and the main command loop.
    ///
    /// **Heartbeat co-location**: the ping/pong dead-connection detector runs
    /// inside this task rather than in a separate task so that `pending_pong`
    /// is set only *after* `writer.send(Ping)` returns — i.e. the Ping has
    /// reached the OS send buffer.  A separate heartbeat task that enqueues
    /// Pings via `try_send` would set `pending_pong = true` at enqueue time,
    /// which can trigger false "missed pong" counts when the writer is backlogged
    /// but the queue is not yet full.
    ///
    /// **Heartbeat during backpressure**: the message-send path pins its future
    /// and drives it in a 3-way inner `select!` (shutdown | heartbeat | send).
    /// When a heartbeat tick fires while the socket is stalled:
    ///
    /// * **Pong detection** runs once (on the first stall tick) using the
    ///   `pending_pong` / `pong_received` flags.  Subsequent ticks skip this
    ///   check because `pending_pong` is cleared after the first detection;
    ///   re-checking on later ticks would count false negatives for a Ping that
    ///   hasn't been sent yet.
    /// * **Send-stall trip-wire**: an independent `send_stall_ticks` counter
    ///   increments on every stall tick.  After `MAX_MISSED_PONGS` intervals the
    ///   connection is declared dead and shutdown is triggered.  This covers
    ///   the case where the OS TCP buffer is permanently full (dead connection)
    ///   yet the ping/pong path would only ever accumulate a single missed-pong
    ///   count (because the deferred Ping is never sent while the socket is
    ///   stuck).
    fn spawn_writer_task(
        mut writer: WsWriter,
        mut rx: mpsc::Receiver<Message>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        pong_received: Arc<std::sync::atomic::AtomicBool>,
        shutdown_tx: tokio::sync::broadcast::Sender<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            debug!("Writer task started");

            let mut heartbeat = tokio::time::interval_at(
                tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
                HEARTBEAT_INTERVAL,
            );
            // After a writer.send stall the default Burst policy would fire
            // multiple ticks back-to-back, falsely incrementing missed_pongs
            // without a real 30 s gap.  Delay always waits a full interval
            // from the last processed tick, guaranteeing genuine spacing.
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let mut missed_pongs: u32 = 0;
            // Set only after writer.send(Ping) returns — guarantees the Ping
            // reached the OS send buffer before we start waiting for a Pong.
            let mut pending_pong = false;
            // Set when a heartbeat tick fires during a message send (socket busy).
            // The deferred Ping is sent at the top of the next outer-loop iteration
            // once the socket is free, preserving the pending_pong correctness
            // invariant while still running detection on schedule.
            let mut deferred_ping = false;

            'task: loop {
                // --- Deferred Ping flush ---
                // Send any Ping that was deferred because a heartbeat tick fired
                // while a regular message send was occupying the socket.
                if deferred_ping {
                    deferred_ping = false;
                    trace!("Sending deferred heartbeat ping");
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.recv() => break 'task,
                        result = writer.send(Message::Ping(Bytes::new())) => {
                            match result {
                                Ok(()) => { pending_pong = true; }
                                Err(e) => {
                                    error!(error = ?e, "Deferred heartbeat ping send failed");
                                    let _ = shutdown_tx.send(());
                                    break 'task;
                                }
                            }
                        }
                    }
                    continue 'task;
                }

                tokio::select! {
                    biased;

                    _ = shutdown_rx.recv() => {
                        debug!("Writer task shutting down");
                        break 'task;
                    }

                    _ = heartbeat.tick() => {
                        // Socket is idle — run detection then send Ping immediately.
                        if pending_pong {
                            if !pong_received.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                missed_pongs += 1;
                                warn!(missed = missed_pongs, "Missed pong response");
                                if missed_pongs >= MAX_MISSED_PONGS {
                                    error!(
                                        missed = missed_pongs,
                                        threshold = MAX_MISSED_PONGS,
                                        "Connection appears dead -- triggering session shutdown"
                                    );
                                    metrics::counter(MetricNames::MESSAGES_RECEIVED, 0, &[("dead_connection", "true")]);
                                    let _ = shutdown_tx.send(());
                                    break 'task;
                                }
                            } else {
                                missed_pongs = 0;
                            }
                        }
                        // pending_pong = false not needed: success sets it to true,
                        // all failure paths break out of 'task.
                        trace!("Sending heartbeat ping");
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.recv() => break 'task,
                            result = writer.send(Message::Ping(Bytes::new())) => {
                                match result {
                                    Ok(()) => { pending_pong = true; }
                                    Err(e) => {
                                        error!(error = ?e, "Heartbeat ping send failed");
                                        let _ = shutdown_tx.send(());
                                        break 'task;
                                    }
                                }
                            }
                        }
                    }

                    msg = rx.recv() => {
                        let msg = match msg {
                            Some(m) => m,
                            None => {
                                debug!("Writer channel closed");
                                break 'task;
                            }
                        };

                        // Pin the send future so it can be polled incrementally
                        // inside the inner loop while heartbeat ticks are
                        // interleaved.  Without pinning, a stalled socket send
                        // would block heartbeat.tick() polling, delaying
                        // missed-pong detection indefinitely under backpressure.
                        let send_fut = writer.send(msg);
                        tokio::pin!(send_fut);

                        // Counts heartbeat ticks that fired while the current
                        // send was in progress.  Used as an independent dead-
                        // connection trip-wire: if the OS send buffer stays
                        // full for MAX_MISSED_PONGS consecutive heartbeat
                        // intervals the connection is almost certainly dead
                        // even if the TCP stack hasn't timed out yet, so we
                        // declare it dead ourselves rather than waiting for the
                        // OS retransmit timeout (which can be minutes).
                        let mut send_stall_ticks: u32 = 0;

                        loop {
                            tokio::select! {
                                biased;

                                _ = shutdown_rx.recv() => {
                                    debug!("Writer task shutting down during send");
                                    break 'task;
                                }

                                _ = heartbeat.tick() => {
                                    // --- Pong detection (runs once per stall, on the
                                    // first tick only) ---
                                    // After the first tick `pending_pong` is cleared,
                                    // so subsequent ticks skip this block.  This is
                                    // intentional: `pong_received` was consumed on the
                                    // first tick; re-checking it on later ticks would
                                    // count false negatives for a ping we haven't
                                    // sent yet (the deferred one).
                                    if pending_pong {
                                        if !pong_received.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                            missed_pongs += 1;
                                            warn!(missed = missed_pongs, "Missed pong response");
                                            if missed_pongs >= MAX_MISSED_PONGS {
                                                error!(
                                                    missed = missed_pongs,
                                                    threshold = MAX_MISSED_PONGS,
                                                    "Connection appears dead -- triggering session shutdown"
                                                );
                                                metrics::counter(MetricNames::MESSAGES_RECEIVED, 0, &[("dead_connection", "true")]);
                                                let _ = shutdown_tx.send(());
                                                break 'task;
                                            }
                                        } else {
                                            missed_pongs = 0;
                                        }
                                        pending_pong = false;
                                        deferred_ping = true;
                                    }

                                    // --- Send-stall trip-wire ---
                                    // Independent of pong state: if the socket has
                                    // been unable to accept a single write for
                                    // MAX_MISSED_PONGS heartbeat intervals the
                                    // OS TCP buffer is persistently full, which
                                    // indicates a dead or completely saturated
                                    // connection.
                                    send_stall_ticks += 1;
                                    if send_stall_ticks >= MAX_MISSED_PONGS {
                                        error!(
                                            stall_ticks = send_stall_ticks,
                                            threshold = MAX_MISSED_PONGS,
                                            "Send stalled for {} heartbeat intervals -- \
                                             connection appears dead, triggering shutdown",
                                            send_stall_ticks,
                                        );
                                        metrics::counter(MetricNames::MESSAGES_RECEIVED, 0, &[("dead_connection", "true")]);
                                        let _ = shutdown_tx.send(());
                                        break 'task;
                                    }
                                    // Keep polling the in-flight send.
                                }

                                result = &mut send_fut => {
                                    if let Err(e) = result {
                                        error!(error = ?e, "Writer task: WebSocket send failed");
                                        break 'task;
                                    }
                                    break; // send complete, return to outer loop
                                }
                            }
                        }
                    }
                }
            }
            let _ = writer.close().await;
            debug!("Writer task exited");
        })
    }

    /// Spawn retransmission scheduler task (matches AWS ResendStreamDataMessageScheduler)
    ///
    /// Checks OutgoingMessageBuffer at fixed intervals and retransmits messages
    /// that haven't been ACKed within the retransmission timeout (adaptive based on RTT).
    fn spawn_retransmit_task(&self) -> JoinHandle<()> {
        let writer_tx = self.writer_tx.clone();
        let outgoing_buffer = Arc::clone(&self.outgoing_buffer);
        let can_send = Arc::clone(&self.can_send);
        let shutdown_tx = self.shutdown_tx.clone();
        let mut shutdown_rx = self.shutdown_rx();
        let session_id = self.session_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RETRANSMIT_INTERVAL);
            let mut tick_count: u64 = 0;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.recv() => {
                        debug!("Retransmit task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        tick_count += 1;

                        // Emit health metrics every ~5 seconds (25 ticks at 200ms)
                        if tick_count % 25 == 0 {
                            let buf_len = outgoing_buffer.len().await;
                            let is_connected = can_send.load(std::sync::atomic::Ordering::SeqCst);
                            let health: f64 = if !is_connected {
                                1.0 // unhealthy
                            } else if buf_len > 100 {
                                2.0 // degraded
                            } else {
                                3.0 // healthy
                            };
                            metrics::gauge(MetricNames::CONNECTION_HEALTH, health, &[]);

                            // Estimate packet loss: unACKed messages / buffer capacity
                            let loss_pct = (buf_len as f64 / OUTGOING_BUFFER_CAPACITY as f64) * 100.0;
                            metrics::gauge(MetricNames::PACKET_LOSS_PERCENT, loss_pct, &[]);

                            // RTT jitter
                            if let Some(rtt) = outgoing_buffer.last_rtt().await {
                                let stats = outgoing_buffer.rtt_stats().await;
                                let jitter = stats.rttvar.as_secs_f64();
                                metrics::gauge(MetricNames::RTT_JITTER_SECONDS, jitter, &[]);
                                metrics::histogram(MetricNames::RTT_SECONDS, rtt.as_secs_f64(), &[]);
                            }
                        }

                        // Get candidates for retransmission
                        let candidates = outgoing_buffer.get_retransmit_candidates(MAX_RETRANSMIT_ATTEMPTS).await;

                        let mut fatal = false;
                        for (seq, data, timed_out) in candidates {
                            if timed_out {
                                // C-2: Propagate to all tasks so callers see an error
                                // instead of hanging forever on an unresponsive endpoint.
                                error!(
                                    session_id = %session_id,
                                    seq,
                                    "Stream data retransmission timed out -- triggering session shutdown"
                                );
                                let _ = shutdown_tx.send(());
                                fatal = true;
                                break;
                            }

                            // Record retransmission metric
                            metrics::counter(MetricNames::RETRANSMISSIONS, 1, &[]);

                            // `get_retransmit_candidates` already advanced last_sent_time and
                            // resend_attempt for this entry, so we must deliver it.  Race the
                            // send with shutdown so a stalled socket cannot block teardown.
                            let send_ok = tokio::select! {
                                biased;
                                _ = shutdown_rx.recv() => false,
                                result = writer_tx.send(Message::Binary(data)) => result.is_ok(),
                            };
                            if !send_ok {
                                warn!(
                                    seq,
                                    "Writer channel closed during retransmit — stopping"
                                );
                                fatal = true;
                                break;
                            } else {
                                trace!(seq, "Retransmitted message");
                            }
                        }
                        if fatal { break; }
                    }
                }
            }
        })
    }

    /// Send data to the session using AWS binary protocol
    ///
    /// Messages are tracked in OutgoingMessageBuffer for reliable delivery.
    /// If the server doesn't ACK, the message will be retransmitted automatically
    /// by the retransmit task.
    async fn send_data(&self, data: Bytes) -> Result<()> {
        let sequence = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let data_len = data.len();

        // Create binary protocol message
        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            sequence,
            PayloadType::Output, // Input data uses Output payload type
            data,
        );

        // Serialize to binary
        let msg_bytes = msg.serialize()?;

        // Validate message size
        if msg_bytes.len() > MAX_MESSAGE_SIZE {
            return Err(Error::InvalidState(format!(
                "Message too large: {} bytes (max: {})",
                msg_bytes.len(),
                MAX_MESSAGE_SIZE
            )));
        }

        // H-4: Buffer BEFORE sending — ensures the retransmit scheduler always
        // has the message in its buffer even if the send fails or is reordered.
        self.outgoing_buffer.add(msg_bytes.clone(), sequence).await;

        // Send via bounded writer channel (backpressure: await when full).
        // A send failure means the writer task has exited — this is an internal
        // channel shutdown, not a WebSocket I/O error.
        self.writer_tx
            .send(Message::Binary(msg_bytes.clone()))
            .await
            .map_err(|_| TransportError::Channel("writer channel closed".to_string()))?;

        // Record send metrics
        metrics::counter(MetricNames::MESSAGES_SENT, 1, &[]);
        metrics::counter(MetricNames::BYTES_SENT, msg_bytes.len() as u64, &[]);

        debug!(
            sequence,
            len = data_len,
            "Sent input data (tracked for ACK)"
        );

        Ok(())
    }

    /// Send a message with a specific payload type
    ///
    /// Used for control messages like terminal size that need specific PayloadType.
    async fn send_message(&self, data: Bytes, payload_type: PayloadType) -> Result<()> {
        let sequence = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let data_len = data.len();

        // Create binary protocol message with specified payload type
        let msg = ClientMessage::new(MessageType::InputStreamData, sequence, payload_type, data);

        // Serialize to binary
        let msg_bytes = msg.serialize()?;

        // Validate message size
        if msg_bytes.len() > MAX_MESSAGE_SIZE {
            return Err(Error::InvalidState(format!(
                "Message too large: {} bytes (max: {})",
                msg_bytes.len(),
                MAX_MESSAGE_SIZE
            )));
        }

        // H-4: Buffer BEFORE sending — ensures the retransmit scheduler always
        // has the message in its buffer even if the send fails or is reordered.
        self.outgoing_buffer.add(msg_bytes.clone(), sequence).await;

        // Send via bounded writer channel (backpressure: await when full).
        // A send failure means the writer task has exited — channel closed, not
        // a WebSocket I/O error.
        self.writer_tx
            .send(Message::Binary(msg_bytes.clone()))
            .await
            .map_err(|_| TransportError::Channel("writer channel closed".to_string()))?;

        // Record send metrics
        metrics::counter(MetricNames::MESSAGES_SENT, 1, &[]);
        metrics::counter(MetricNames::BYTES_SENT, msg_bytes.len() as u64, &[]);

        debug!(
            sequence,
            ?payload_type,
            len = data_len,
            "Sent message (tracked for ACK)"
        );

        Ok(())
    }

    /// Route incoming message to appropriate channel
    ///
    /// Handles modern agents with handshake protocol.
    /// Uses expected_sequence_number to detect and suppress duplicate messages
    /// while NOT ACKing duplicates (per AWS protocol behavior).
    ///
    /// AWS Protocol ACK Rules (from session-manager-plugin streaming.go):
    /// - seq == expected: Process message, increment expected, send ACK
    /// - seq > expected:  Out-of-order, buffer if possible, send ACK immediately
    /// - seq < expected:  Duplicate, do NOT ACK, silently drop
    async fn route_message(
        ctx: &ReceiverContext,
        msg: ClientMessage,
        state: &mut ReceiverState,
        outgoing_buffer: &OutgoingMessageBuffer,
        incoming_buffer: &IncomingMessageBuffer,
        raw_bytes: Bytes,
    ) -> Result<()> {
        match msg.message_type.as_str() {
            "output_stream_data" => {
                // AWS Protocol: Check sequence number BEFORE sending ACK
                if msg.sequence_number < state.expected_sequence {
                    // Duplicate message - do NOT ACK, silently drop
                    // AWS behavior: sender will eventually stop retrying
                    debug!(
                        sequence = msg.sequence_number,
                        expected = state.expected_sequence,
                        "Duplicate message detected, NOT ACKing (AWS protocol)"
                    );
                    return Ok(());
                }

                if msg.sequence_number == state.expected_sequence {
                    // In-order message: process, ACK, then check buffer for consecutive messages
                    if let Err(e) = Self::send_acknowledge(&ctx.writer_tx, &msg) {
                        error!(error = ?e, "Failed to send acknowledge");
                    } else {
                        debug!(
                            sequence = msg.sequence_number,
                            "Sent acknowledge for in-order message"
                        );
                    }

                    // Process the message
                    Self::process_output_message(
                        ctx,
                        &msg,
                        &mut state.handshake_handler,
                        &mut state.handshake_started,
                    )
                    .await?;

                    // Increment expected sequence
                    state.expected_sequence = msg.sequence_number + 1;

                    // Process any buffered messages that are now in-order
                    Self::process_buffered_messages(
                        ctx,
                        incoming_buffer,
                        &mut state.handshake_handler,
                        &mut state.expected_sequence,
                        &mut state.handshake_started,
                    )
                    .await?;
                } else {
                    // Out-of-order: seq > expected
                    // Buffer if we have capacity, send ACK immediately
                    debug!(
                        sequence = msg.sequence_number,
                        expected = state.expected_sequence,
                        "Out-of-order message received"
                    );

                    if incoming_buffer.add(msg.clone(), raw_bytes).await {
                        // Successfully buffered - send ACK with IsSequentialMessage=false
                        if let Err(e) =
                            Self::send_acknowledge_non_sequential(&ctx.writer_tx, &msg)
                        {
                            error!(error = ?e, "Failed to send acknowledge for out-of-order message");
                        } else {
                            let buffer_size = incoming_buffer.len().await;
                            debug!(
                                sequence = msg.sequence_number,
                                buffer_size, "Buffered out-of-order message, sent ACK"
                            );
                        }
                    } else {
                        // No capacity - drop without ACK (per AWS protocol)
                        debug!(
                            sequence = msg.sequence_number,
                            "IncomingMessageBuffer full, dropping without ACK"
                        );
                    }
                }
            }
            "acknowledge" => {
                // Parse the ACK content from payload
                if let Ok(ack_content) = crate::ack::AckTracker::parse_ack(&msg) {
                    let ack_seq = ack_content.acknowledged_message_sequence_number;
                    debug!(
                        sequence = ack_seq,
                        is_sequential = ack_content.is_sequential_message,
                        "Received acknowledgment"
                    );

                    // Record ACK metric
                    metrics::counter(MetricNames::ACKS_RECEIVED, 1, &[]);

                    // Remove from OutgoingMessageBuffer and update RTT
                    if outgoing_buffer.process_ack(ack_seq).await {
                        // Record RTT if available
                        if let Some(rtt) = outgoing_buffer.last_rtt().await {
                            metrics::histogram(MetricNames::RTT_SECONDS, rtt.as_secs_f64(), &[]);
                        }
                        trace!(
                            sequence = ack_seq,
                            "ACK processed, message removed from buffer"
                        );
                    } else {
                        trace!(sequence = ack_seq, "ACK for unknown/already-acked message");
                    }
                } else {
                    debug!(
                        sequence = msg.sequence_number,
                        "Received acknowledgment (couldn't parse content)"
                    );
                }
            }
            "channel_closed" => {
                info!("Channel closed by server");
                // Parse exit code from payload if present
                if !msg.payload.is_empty() {
                    if let Ok(exit_info) = std::str::from_utf8(&msg.payload) {
                        info!(exit_info = %exit_info, "Channel close info");
                    }
                }
                // Close the output channel to signal consumers
                ctx.channels.close();
            }
            "start_publication" => {
                info!("Received start_publication - ready to send data");
                ctx.can_send
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                ctx.ready_notify.notify_waiters();
            }
            "pause_publication" => {
                debug!("Received pause_publication - pausing data send");
                ctx.can_send
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                if tracing::enabled!(tracing::Level::DEBUG) && !msg.payload.is_empty() {
                    if let Ok(payload_str) = std::str::from_utf8(&msg.payload) {
                        debug!(payload = %payload_str, "pause_publication payload");
                    }
                }
            }
            msg_type => {
                debug!(message_type = %msg_type, "Received message type: {}", msg_type);
                if tracing::enabled!(tracing::Level::DEBUG) && !msg.payload.is_empty() {
                    if let Ok(payload_str) = std::str::from_utf8(&msg.payload) {
                        debug!(payload = %payload_str, "Message payload");
                    }
                }
            }
        }

        Ok(())
    }

    /// Process an output_stream_data message payload
    /// Extracted to be reusable for both direct and buffered messages
    async fn process_output_message(
        ctx: &ReceiverContext,
        msg: &ClientMessage,
        handshake_handler: &mut HandshakeHandler,
        handshake_started: &mut Option<Instant>,
    ) -> Result<()> {
        match msg.payload_type {
            PayloadType::Output | PayloadType::StdErr | PayloadType::Undefined => {
                // Check for legacy agent: if we receive output before handshake completes,
                // this is a legacy shell session that doesn't do handshake
                if handshake_handler.state() == HandshakeState::AwaitingRequest
                    && !ctx.can_send.load(std::sync::atomic::Ordering::SeqCst)
                {
                    info!(
                        "Legacy agent detected: receiving output without handshake, enabling send"
                    );
                    ctx.can_send
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    ctx.ready_notify.notify_waiters();
                }

                // Normal output data - send to output channel
                if !msg.payload.is_empty() {
                    trace!(len = msg.payload.len(), payload_type = ?msg.payload_type, "Routing output data");
                    ctx.channels.send_output(msg.payload.clone())?;
                }
            }
            PayloadType::HandshakeRequest => {
                // Agent handshake request - parse and respond
                if let Ok(handshake_json) = std::str::from_utf8(&msg.payload) {
                    debug!(handshake = %handshake_json, "HandshakeRequest payload");

                    // Parse handshake request
                    if let Ok(request) = serde_json::from_str::<HandshakeRequest>(handshake_json) {
                        // Process and generate response
                        match handshake_handler.process_request(request) {
                            Ok(Some(response)) => {
                                // First handshake request - log at INFO and send response
                                info!("Received HandshakeRequest from agent");
                                *handshake_started = Some(Instant::now());
                                // Send handshake response
                                if let Err(e) = Self::send_handshake_response(
                                    &ctx.writer_tx,
                                    &response,
                                    &ctx.sequence,
                                )
                                .await
                                {
                                    error!(error = ?e, "Failed to send handshake response");
                                } else {
                                    info!("Handshake response sent");
                                }
                            }
                            Ok(None) => {
                                // Duplicate request, already sent response - ignore at debug level
                                trace!("Duplicate HandshakeRequest ignored");
                            }
                            Err(e) => {
                                error!(error = ?e, "Failed to process handshake request");
                            }
                        }
                    } else {
                        error!("Failed to parse handshake request JSON");
                    }
                }
            }
            PayloadType::HandshakeComplete => {
                // Handshake complete - session is ready
                info!("Agent handshake complete, session ready");
                if let Some(started) = handshake_started.take() {
                    let duration = started.elapsed();
                    info!(duration_ms = duration.as_millis(), "Handshake duration");
                    metrics::timing(MetricNames::HANDSHAKE_DURATION, duration, &[]);
                }
                if tracing::enabled!(tracing::Level::DEBUG) {
                    if let Ok(complete_json) = std::str::from_utf8(&msg.payload) {
                        debug!(complete = %complete_json, "HandshakeComplete payload");
                    }
                }
                // Mark that we can start sending data
                ctx.can_send
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                ctx.ready_notify.notify_waiters();
            }
            PayloadType::Size => {
                debug!("Received size update request");
            }
            PayloadType::ExitCode => {
                if !msg.payload.is_empty() {
                    if let Ok(exit_info) = std::str::from_utf8(&msg.payload) {
                        info!(exit_code = %exit_info, "Process exit code");
                    }
                }
            }
            PayloadType::Flag => {
                debug!("Received control flag");
            }
            _ => {
                debug!(payload_type = ?msg.payload_type, "Received payload type");
            }
        }
        Ok(())
    }

    /// Process buffered messages that are now in-order
    /// (Per AWS ProcessIncomingMessageBufferItems)
    async fn process_buffered_messages(
        ctx: &ReceiverContext,
        incoming_buffer: &IncomingMessageBuffer,
        handshake_handler: &mut HandshakeHandler,
        expected_sequence_number: &mut i64,
        handshake_started: &mut Option<Instant>,
    ) -> Result<()> {
        while let Some(buffered) = incoming_buffer.remove(*expected_sequence_number).await {
            debug!(
                sequence = buffered.message.sequence_number,
                "Processing buffered message"
            );

            // Process the buffered message
            Self::process_output_message(
                ctx,
                &buffered.message,
                handshake_handler,
                handshake_started,
            )
            .await?;

            // Increment expected sequence
            *expected_sequence_number += 1;
        }
        Ok(())
    }

    /// Send handshake response via writer channel
    async fn send_handshake_response(
        writer_tx: &mpsc::Sender<Message>,
        response: &HandshakeResponse,
        sequence: &Arc<std::sync::atomic::AtomicI64>,
    ) -> Result<()> {
        let response_json = serde_json::to_vec(response).map_err(Error::Serialization)?;

        let seq = sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        debug!(
            response_json = %String::from_utf8_lossy(&response_json),
            sequence = seq,
            "Sending HandshakeResponse"
        );

        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            seq,
            PayloadType::HandshakeResponse,
            Bytes::from(response_json),
        );

        let msg_bytes = msg.serialize()?;
        debug!(
            msg_len = msg_bytes.len(),
            "Serialized HandshakeResponse message"
        );

        // Send failure means the writer task has exited (channel closed).
        writer_tx
            .send(Message::Binary(msg_bytes))
            .await
            .map_err(|_| TransportError::Channel("writer channel closed".to_string()))?;

        debug!("HandshakeResponse sent to WebSocket");
        Ok(())
    }

    /// Send acknowledge message for a received message (sequential).
    ///
    /// Uses `try_send` so this never blocks the receiver loop.  If the writer
    /// channel is momentarily full the ACK is dropped with a warning; the
    /// remote end will retransmit and the next ACK attempt will succeed once
    /// the writer drains.  Pong frames and other control messages therefore
    /// remain unaffected.
    fn send_acknowledge(
        writer_tx: &mpsc::Sender<Message>,
        received_msg: &ClientMessage,
    ) -> Result<()> {
        // Create acknowledgment message using AckTracker helper
        let ack_msg = AckTracker::create_ack(received_msg, true)?;
        let ack_payload = String::from_utf8_lossy(&ack_msg.payload);
        trace!(
            ack_seq = ack_msg.sequence_number,
            ack_flags = ack_msg.flags,
            ack_payload = %ack_payload,
            original_seq = received_msg.sequence_number,
            original_msg_id = %received_msg.message_id,
            "Sending ACK (sequential)"
        );
        let msg_bytes = ack_msg.serialize()?;

        match writer_tx.try_send(Message::Binary(msg_bytes)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    original_seq = received_msg.sequence_number,
                    "ACK dropped: writer channel full (remote will retransmit)"
                );
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(TransportError::Channel("writer channel closed".to_string()).into())
            }
        }
    }

    /// Send acknowledge message for an out-of-order message (non-sequential).
    ///
    /// Uses `try_send` — see [`Self::send_acknowledge`] for rationale.
    fn send_acknowledge_non_sequential(
        writer_tx: &mpsc::Sender<Message>,
        received_msg: &ClientMessage,
    ) -> Result<()> {
        // Create acknowledgment with IsSequentialMessage=false
        let ack_msg = AckTracker::create_ack(received_msg, false)?;
        let ack_payload = String::from_utf8_lossy(&ack_msg.payload);
        trace!(
            ack_seq = ack_msg.sequence_number,
            ack_flags = ack_msg.flags,
            ack_payload = %ack_payload,
            original_seq = received_msg.sequence_number,
            original_msg_id = %received_msg.message_id,
            "Sending ACK (non-sequential/out-of-order)"
        );
        let msg_bytes = ack_msg.serialize()?;

        match writer_tx.try_send(Message::Binary(msg_bytes)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    original_seq = received_msg.sequence_number,
                    "ACK (non-sequential) dropped: writer channel full (remote will retransmit)"
                );
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(TransportError::Channel("writer channel closed".to_string()).into())
            }
        }
    }

    /// Shutdown the connection gracefully
    async fn shutdown(self) -> Result<()> {
        info!("Shutting down connection manager");

        // Close the channel multiplexer so all Session::output() consumers
        // immediately see EOF rather than blocking forever.  The receiver task
        // also exits on the shutdown broadcast below, but races mean it may not
        // have closed the channels yet when consumers check.
        self.channels.close();

        // Signal all tasks to shutdown
        let _ = self.shutdown_tx.send(());

        // Drop the write channel — this causes the writer task to close the WebSocket
        drop(self.writer_tx);

        // Wait for all tasks to complete with timeout
        for task in self.tasks {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
        }

        info!("Connection manager shutdown complete");

        Ok(())
    }

    /// Validate stream URL to prevent SSRF attacks
    ///
    /// **Security**: Ensures the URL is a legitimate AWS SSM endpoint.
    fn validate_stream_url(url: &str) -> Result<()> {
        // Parse URL
        let parsed = url::Url::parse(url)
            .map_err(|e| Error::Config(format!("Invalid stream URL: {}", e)))?;

        // Ensure secure WebSocket (WSS)
        if parsed.scheme() != "wss" {
            return Err(Error::Config(
                "Stream URL must use wss:// scheme for secure connection".into(),
            ));
        }

        // Validate against AWS domain pattern
        let host = parsed
            .host_str()
            .ok_or_else(|| Error::Config("Stream URL missing host".into()))?;

        // AWS SSM endpoints follow these patterns:
        // - ssmmessages.<region>.amazonaws.com
        // - ssmmessages-fips.<region>.amazonaws.com
        // - ssmmessages.<region>.amazonaws.com.cn (China regions)
        //
        // SSRF hardening: require the host to start with `ssmmessages` so that
        // attacker-controlled hostnames like `evil.ssmmessages.com.amazonaws.com`
        // or `s3.amazonaws.com` are rejected even though they end with the right
        // suffix.
        let is_ssm_prefix =
            host.starts_with("ssmmessages.") || host.starts_with("ssmmessages-");
        let is_aws_domain =
            host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn");

        if !is_ssm_prefix || !is_aws_domain {
            return Err(Error::Config(format!(
                "Stream URL host '{}' is not a valid AWS SSM messages endpoint \
                 (expected ssmmessages[‑fips].<region>.amazonaws.com[.cn])",
                host
            )));
        }

        Ok(())
    }

    /// Sanitize URL for logging (remove sensitive query params)
    fn sanitize_url(url: &str) -> String {
        if let Ok(mut parsed) = url::Url::parse(url) {
            // Remove token-related query params
            let pairs: Vec<(String, String)> = parsed
                .query_pairs()
                .filter(|(k, _)| !k.eq_ignore_ascii_case("tokenValue"))
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();

            parsed.query_pairs_mut().clear();
            for (k, v) in pairs {
                parsed.query_pairs_mut().append_pair(&k, &v);
            }
            parsed.to_string()
        } else {
            "[invalid URL]".to_string()
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_stream_url_valid_aws() {
        // Valid AWS SSM endpoints
        let valid_urls = [
            "wss://ssmmessages.us-east-1.amazonaws.com/v1/data-channel/session-id",
            "wss://ssmmessages.eu-west-1.amazonaws.com/v1/data-channel/session-id?role=publish",
            "wss://ssmmessages-fips.us-gov-west-1.amazonaws.com/v1/data-channel/session-id",
            "wss://ssmmessages.cn-north-1.amazonaws.com.cn/v1/data-channel/session-id",
        ];

        for url in &valid_urls {
            assert!(
                ConnectionManager::validate_stream_url(url).is_ok(),
                "Expected valid URL: {}",
                url
            );
        }
    }

    #[test]
    fn test_validate_stream_url_rejects_non_wss() {
        // Non-secure WebSocket should be rejected
        let result = ConnectionManager::validate_stream_url(
            "ws://ssmmessages.us-east-1.amazonaws.com/v1/data-channel/session-id",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("wss://"));
    }

    #[test]
    fn test_validate_stream_url_rejects_non_aws() {
        // Non-AWS domains should be rejected (SSRF protection)
        let malicious_urls = [
            "wss://evil.com/v1/data-channel/session-id",
            "wss://ssmmessages.attacker.com/steal-token",
            "wss://amazonaws.com.evil.com/fake",
            "wss://internal-service.local/ssmmessages",
        ];

        for url in &malicious_urls {
            assert!(
                ConnectionManager::validate_stream_url(url).is_err(),
                "Expected rejection of malicious URL: {}",
                url
            );
        }
    }

    #[test]
    fn test_validate_stream_url_rejects_non_ssm() {
        // Non-SSM AWS services should be rejected
        let result = ConnectionManager::validate_stream_url(
            "wss://s3.us-east-1.amazonaws.com/bucket/object",
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not a valid AWS SSM messages endpoint"));
    }

    #[test]
    fn test_sanitize_url_removes_token() {
        let url_with_token = "wss://ssmmessages.us-east-1.amazonaws.com/v1/data-channel/session-id?role=publish&tokenValue=secret123&cell-number=1";
        let sanitized = ConnectionManager::sanitize_url(url_with_token);

        assert!(
            !sanitized.contains("secret123"),
            "Token value should be removed"
        );
        assert!(
            !sanitized.contains("tokenValue"),
            "tokenValue param should be removed"
        );
        assert!(
            sanitized.contains("role=publish"),
            "Other params should be preserved"
        );
        assert!(
            sanitized.contains("cell-number=1"),
            "Other params should be preserved"
        );
    }

    #[test]
    fn test_sanitize_url_handles_invalid() {
        let result = ConnectionManager::sanitize_url("not a valid url");
        assert_eq!(result, "[invalid URL]");
    }

    #[test]
    fn test_sanitize_url_case_insensitive_token() {
        // Token removal should be case-insensitive
        let url = "wss://example.com?TokenValue=secret&TOKENVALUE=secret2&tokenvalue=secret3";
        let sanitized = ConnectionManager::sanitize_url(url);

        assert!(
            !sanitized.contains("secret"),
            "All token variants should be removed"
        );
    }

    #[test]
    fn test_open_data_channel_input_debug_redacts_token() {
        let input = OpenDataChannelInput {
            message_schema_version: "1.0".to_string(),
            request_id: "req-123".to_string(),
            token_value: "super-secret-token".to_string(),
            client_id: "client-456".to_string(),
            client_version: "1.0.0".to_string(),
        };

        let debug_output = format!("{:?}", input);

        assert!(
            debug_output.contains("[REDACTED]"),
            "Token should be redacted in debug output"
        );
        assert!(
            !debug_output.contains("super-secret-token"),
            "Actual token should not appear"
        );
        assert!(
            debug_output.contains("req-123"),
            "Non-sensitive fields should appear"
        );
    }

    #[test]
    fn test_max_message_size_constant() {
        // Ensure size limit is reasonable (10MB)
        assert_eq!(MAX_MESSAGE_SIZE, 10 * 1024 * 1024);
    }

    #[test]
    fn test_buffer_capacity_constants() {
        // Ensure buffer sizes match AWS defaults
        assert_eq!(INCOMING_BUFFER_CAPACITY, 10000);
        assert_eq!(OUTGOING_BUFFER_CAPACITY, 10000);
    }

    #[test]
    fn test_retransmit_constants() {
        // Ensure retransmit settings match AWS
        assert_eq!(RETRANSMIT_INTERVAL.as_millis(), 200);
        assert_eq!(MAX_RETRANSMIT_ATTEMPTS, 3000); // 5 minutes at 200ms intervals
    }

    #[test]
    fn test_missed_pongs_constant() {
        // 3 missed pongs = 90 seconds without response at 30s heartbeat interval
        assert_eq!(MAX_MISSED_PONGS, 3);
        let dead_threshold = HEARTBEAT_INTERVAL * MAX_MISSED_PONGS;
        assert_eq!(dead_threshold.as_secs(), 90);
    }
}
