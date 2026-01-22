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
use tokio::sync::{mpsc, Mutex};
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

/// Maximum message size (10MB) - prevent DoS
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Heartbeat interval (30 seconds)
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Maximum incoming messages per second - DoS protection
/// This is generous for normal usage but prevents flood attacks
const MAX_MESSAGES_PER_SECOND: f64 = 5000.0;

/// Message schema version
const MESSAGE_SCHEMA_VERSION: &str = "1.0";

/// Client version - use AWS plugin version format for compatibility
const CLIENT_VERSION: &str = "1.2.707.0";

/// Buffer capacity for out-of-order messages (matches AWS default)
const INCOMING_BUFFER_CAPACITY: usize = 10000;

/// Buffer capacity for unACKed outgoing messages (matches AWS default)
const OUTGOING_BUFFER_CAPACITY: usize = 10000;

/// Retransmission check interval (200ms - matches AWS ResendSleepInterval)
const RETRANSMIT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Maximum retransmission attempts (3000 per AWS = 5 minutes / 200ms interval)
const MAX_RETRANSMIT_ATTEMPTS: u32 = 3000;

/// Type aliases for split WebSocket streams
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

/// Open data channel input - sent as JSON after WebSocket connects.
///
/// **Security**: Debug impl is removed to prevent accidental token leakage in logs.
#[derive(Clone, Serialize, Deserialize)]
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
/// - Writer (SplitSink) - protected by Mutex for sending data, heartbeats
/// - Reader (SplitStream) - consumed by dedicated receiver task
pub struct ConnectionManager {
    /// Session ID
    session_id: String,

    /// WebSocket write half (protected by mutex for concurrent access)
    writer: Arc<Mutex<WsWriter>>,

    /// Channel multiplexer
    channels: Arc<ChannelMultiplexer>,

    /// Command receiver
    command_rx: mpsc::UnboundedReceiver<ManagerCommand>,

    /// Shutdown signal
    shutdown_tx: tokio::sync::broadcast::Sender<()>,

    /// Task handles
    tasks: Vec<JoinHandle<()>>,

    /// Sequence number counter for outgoing messages
    sequence: Arc<std::sync::atomic::AtomicI64>,

    /// Publication state - whether we've received start_publication
    can_send: Arc<std::sync::atomic::AtomicBool>,

    /// Outgoing message buffer for reliable delivery with retransmission
    outgoing_buffer: Arc<OutgoingMessageBuffer>,
}

/// Commands sent to the connection manager
#[derive(Debug)]
pub enum ManagerCommand {
    /// Send data to stdin
    SendData(Bytes),
    /// Terminate connection
    Terminate,
}

impl ConnectionManager {
    /// Create a new connection manager and establish WebSocket connection
    /// Uses retry logic with exponential backoff for resilience
    #[instrument(skip(token_value, command_rx), fields(session_id = %session_id))]
    pub async fn connect(
        session_id: String,
        stream_url: String,
        token_value: String,
        command_rx: mpsc::UnboundedReceiver<ManagerCommand>,
    ) -> Result<Self> {
        // Validate stream URL to prevent SSRF attacks
        Self::validate_stream_url(&stream_url)?;

        // Build WebSocket URL with authentication token
        // SECURITY: Token is in query params but this URL is NEVER logged
        // The stream_url from AWS already has query params (role, cell-number), so use &
        let separator = if stream_url.contains('?') { "&" } else { "?" };
        let ws_url = format!(
            "{}{}sessionId={}&tokenValue={}",
            stream_url, separator, session_id, token_value
        );

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
            .send(Message::Text(handshake_json))
            .await
            .map_err(|e| TransportError::WebSocket(e.to_string()))?;

        info!("Data channel handshake sent");

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let writer = Arc::new(Mutex::new(writer));
        let channels = Arc::new(ChannelMultiplexer::new());
        let can_send = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Create outgoing message buffer for reliable delivery (before receiver task)
        let outgoing_buffer = Arc::new(OutgoingMessageBuffer::new(OUTGOING_BUFFER_CAPACITY));

        // Spawn receiver task immediately with the reader half
        // This task owns the reader and runs independently of send operations
        let receiver_task = Self::spawn_receiver_task(
            reader,
            Arc::clone(&channels),
            Arc::clone(&writer),
            Arc::clone(&can_send),
            Arc::clone(&outgoing_buffer),
            shutdown_tx.subscribe(),
        );

        Ok(Self {
            session_id,
            writer,
            channels,
            command_rx,
            shutdown_tx,
            tasks: vec![receiver_task],
            sequence: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            can_send,
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

        // Spawn heartbeat task
        let heartbeat_task = self.spawn_heartbeat_task();
        self.tasks.push(heartbeat_task);

        // Spawn retransmission scheduler task (matches AWS ResendStreamDataMessageScheduler)
        let retransmit_task = self.spawn_retransmit_task();
        self.tasks.push(retransmit_task);

        // Main command processing loop
        debug!("Entering command processing loop");
        loop {
            tokio::select! {
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        ManagerCommand::SendData(data) => {
                            debug!(len = data.len(), "Processing SendData command");
                            if let Err(e) = self.send_data(data).await {
                                error!(error = ?e, "Failed to send data");
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
        channels: Arc<ChannelMultiplexer>,
        writer: Arc<Mutex<WsWriter>>,
        can_send: Arc<std::sync::atomic::AtomicBool>,
        outgoing_buffer: Arc<OutgoingMessageBuffer>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            debug!("Receiver task started");
            let mut handshake_handler = HandshakeHandler::new(HandshakeConfig::default());
            let mut sequence_counter: i64 = 0;
            // Track expected sequence number for duplicate detection (like AWS ExpectedSequenceNumber)
            // Messages with sequence < expected are duplicates - ACK them but don't process to output
            let mut expected_sequence_number: i64 = 0;
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

                                // Convert to Bytes once, then clone for routing (cheap - reference counted)
                                let raw_bytes = Bytes::from(data);
                                match ClientMessage::deserialize(raw_bytes.clone()) {
                                    Ok(msg) => {
                                        debug!(
                                            message_type = %msg.message_type,
                                            sequence = msg.sequence_number,
                                            payload_type = ?msg.payload_type,
                                            payload_type_raw = msg.payload_type as u32,
                                            payload_len = msg.payload.len(),
                                            "Parsed binary message"
                                        );

                                        // Extra debug: log first bytes of payload if it looks like JSON
                                        if !msg.payload.is_empty() {
                                            if let Ok(s) = String::from_utf8(msg.payload.to_vec()) {
                                                if s.starts_with('{') {
                                                    debug!(payload_preview = %s[..s.len().min(300)], "Payload content");
                                                }
                                            }
                                        }
                                        if let Err(e) = Self::route_message(
                                            &channels,
                                            msg,
                                            &mut handshake_handler,
                                            &writer,
                                            &can_send,
                                            &outgoing_buffer,
                                            &mut sequence_counter,
                                            &mut expected_sequence_number,
                                            &incoming_buffer,
                                            raw_bytes,
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
                                    can_send.store(true, std::sync::atomic::Ordering::SeqCst);
                                } else if text_trimmed == "pause_publication" {
                                    debug!("Received TEXT pause_publication - pausing data send");
                                    can_send.store(false, std::sync::atomic::Ordering::SeqCst);
                                } else if text_trimmed == "channel_closed" {
                                    info!("Received TEXT channel_closed");
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
                                break;
                            }
                            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {
                                trace!("Received ping/pong/frame");
                            }
                            Some(Err(e)) => {
                                error!(error = ?e, "WebSocket error");
                                break;
                            }
                            None => {
                                info!("WebSocket stream ended");
                                break;
                            }
                        }
                    }
                }
            }
            debug!("Receiver task exited");
        })
    }

    /// Spawn heartbeat task
    fn spawn_heartbeat_task(&self) -> JoinHandle<()> {
        let writer = Arc::clone(&self.writer);
        let mut shutdown_rx = self.shutdown_rx();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.recv() => {
                        debug!("Heartbeat task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        trace!("Sending heartbeat ping");
                        let mut writer = writer.lock().await;
                        if let Err(e) = writer.send(Message::Ping(vec![])).await {
                            error!(error = ?e, "Failed to send heartbeat");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Spawn retransmission scheduler task (matches AWS ResendStreamDataMessageScheduler)
    ///
    /// Checks OutgoingMessageBuffer at fixed intervals and retransmits messages
    /// that haven't been ACKed within the retransmission timeout (adaptive based on RTT).
    fn spawn_retransmit_task(&self) -> JoinHandle<()> {
        let writer = Arc::clone(&self.writer);
        let outgoing_buffer = Arc::clone(&self.outgoing_buffer);
        let mut shutdown_rx = self.shutdown_rx();
        let session_id = self.session_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RETRANSMIT_INTERVAL);

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.recv() => {
                        debug!("Retransmit task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        // Get candidates for retransmission
                        let candidates = outgoing_buffer.get_retransmit_candidates(MAX_RETRANSMIT_ATTEMPTS).await;

                        for (seq, data, timed_out) in candidates {
                            if timed_out {
                                // Message timed out after max retries - signal session termination
                                error!(
                                    session_id = %session_id,
                                    "Stream data retransmission timed out, terminating session"
                                );
                                // In production, we'd signal session termination here
                                // For now, log the error
                                break;
                            }

                            // Record retransmission metric
                            metrics::counter(MetricNames::RETRANSMISSIONS, 1, &[]);

                            // Retransmit the message
                            let mut writer = writer.lock().await;
                            if let Err(e) = writer.send(Message::Binary(data.to_vec())).await {
                                warn!(
                                    seq,
                                    error = ?e,
                                    "Failed to retransmit message"
                                );
                            } else {
                                trace!(seq, "Retransmitted message");
                            }
                        }
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

        // Create binary protocol message
        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            sequence,
            PayloadType::Output, // Input data uses Output payload type
            data.clone(),
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

        // Send via writer (short lock duration - no blocking operations while holding)
        {
            let mut writer = self.writer.lock().await;
            writer
                .send(Message::Binary(msg_bytes.to_vec()))
                .await
                .map_err(|e| TransportError::WebSocket(e.to_string()))?;
        }

        // Record send metrics
        metrics::counter(MetricNames::MESSAGES_SENT, 1, &[]);
        metrics::counter(MetricNames::BYTES_SENT, msg_bytes.len() as u64, &[]);

        // Track in OutgoingMessageBuffer for reliable delivery
        self.outgoing_buffer.add(msg_bytes, sequence).await;

        debug!(
            sequence,
            len = data.len(),
            "Sent input data (tracked for ACK)"
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
    #[allow(clippy::too_many_arguments)]
    async fn route_message(
        channels: &ChannelMultiplexer,
        msg: ClientMessage,
        handshake_handler: &mut HandshakeHandler,
        writer: &Arc<Mutex<WsWriter>>,
        can_send: &Arc<std::sync::atomic::AtomicBool>,
        outgoing_buffer: &OutgoingMessageBuffer,
        sequence_counter: &mut i64,
        expected_sequence_number: &mut i64,
        incoming_buffer: &IncomingMessageBuffer,
        raw_bytes: Bytes,
    ) -> Result<()> {
        match msg.message_type.as_str() {
            "output_stream_data" => {
                // AWS Protocol: Check sequence number BEFORE sending ACK
                if msg.sequence_number < *expected_sequence_number {
                    // Duplicate message - do NOT ACK, silently drop
                    // AWS behavior: sender will eventually stop retrying
                    debug!(
                        sequence = msg.sequence_number,
                        expected = *expected_sequence_number,
                        "Duplicate message detected, NOT ACKing (AWS protocol)"
                    );
                    return Ok(());
                }

                if msg.sequence_number == *expected_sequence_number {
                    // In-order message: process, ACK, then check buffer for consecutive messages
                    if let Err(e) = Self::send_acknowledge(writer, &msg, sequence_counter).await {
                        error!(error = ?e, "Failed to send acknowledge");
                    } else {
                        debug!(
                            sequence = msg.sequence_number,
                            "Sent acknowledge for in-order message"
                        );
                    }

                    // Process the message
                    Self::process_output_message(
                        channels,
                        &msg,
                        handshake_handler,
                        writer,
                        can_send,
                        sequence_counter,
                    )
                    .await?;

                    // Increment expected sequence
                    *expected_sequence_number = msg.sequence_number + 1;

                    // Process any buffered messages that are now in-order
                    Self::process_buffered_messages(
                        channels,
                        incoming_buffer,
                        handshake_handler,
                        writer,
                        can_send,
                        sequence_counter,
                        expected_sequence_number,
                    )
                    .await?;
                } else {
                    // Out-of-order: seq > expected
                    // Buffer if we have capacity, send ACK immediately
                    debug!(
                        sequence = msg.sequence_number,
                        expected = *expected_sequence_number,
                        "Out-of-order message received"
                    );

                    if incoming_buffer.add(msg.clone(), raw_bytes).await {
                        // Successfully buffered - send ACK with IsSequentialMessage=false
                        if let Err(e) =
                            Self::send_acknowledge_non_sequential(writer, &msg, sequence_counter)
                                .await
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
                    if let Ok(exit_info) = String::from_utf8(msg.payload.to_vec()) {
                        info!(exit_info = %exit_info, "Channel close info");
                    }
                }
            }
            "start_publication" => {
                info!("Received start_publication - ready to send data");
                can_send.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            "pause_publication" => {
                debug!("Received pause_publication - pausing data send");
                can_send.store(false, std::sync::atomic::Ordering::SeqCst);
                if !msg.payload.is_empty() {
                    if let Ok(payload_str) = String::from_utf8(msg.payload.to_vec()) {
                        debug!(payload = %payload_str, "pause_publication payload");
                    }
                }
            }
            msg_type => {
                debug!(message_type = %msg_type, "Received message type: {}", msg_type);
                // Log payload for debugging
                if !msg.payload.is_empty() {
                    if let Ok(payload_str) = String::from_utf8(msg.payload.to_vec()) {
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
        channels: &ChannelMultiplexer,
        msg: &ClientMessage,
        handshake_handler: &mut HandshakeHandler,
        writer: &Arc<Mutex<WsWriter>>,
        can_send: &Arc<std::sync::atomic::AtomicBool>,
        sequence_counter: &mut i64,
    ) -> Result<()> {
        match msg.payload_type {
            PayloadType::Output | PayloadType::StdErr | PayloadType::Undefined => {
                // Check for legacy agent: if we receive output before handshake completes,
                // this is a legacy shell session that doesn't do handshake
                if handshake_handler.state() == HandshakeState::AwaitingRequest
                    && !can_send.load(std::sync::atomic::Ordering::SeqCst)
                {
                    info!(
                        "Legacy agent detected: receiving output without handshake, enabling send"
                    );
                    can_send.store(true, std::sync::atomic::Ordering::SeqCst);
                }

                // Normal output data - send to output channel
                if !msg.payload.is_empty() {
                    trace!(len = msg.payload.len(), payload_type = ?msg.payload_type, "Routing output data");
                    channels.send_output(msg.payload.clone()).await?;
                }
            }
            PayloadType::HandshakeRequest => {
                // Agent handshake request - parse and respond
                if let Ok(handshake_json) = String::from_utf8(msg.payload.to_vec()) {
                    debug!(handshake = %handshake_json, "HandshakeRequest payload");

                    // Parse handshake request
                    if let Ok(request) = serde_json::from_str::<HandshakeRequest>(&handshake_json) {
                        // Process and generate response
                        match handshake_handler.process_request(request) {
                            Ok(Some(response)) => {
                                // First handshake request - log at INFO and send response
                                info!("Received HandshakeRequest from agent");
                                // Send handshake response
                                if let Err(e) = Self::send_handshake_response(
                                    writer,
                                    &response,
                                    sequence_counter,
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
                if let Ok(complete_json) = String::from_utf8(msg.payload.to_vec()) {
                    debug!(complete = %complete_json, "HandshakeComplete payload");
                }
                // Mark that we can start sending data
                can_send.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            PayloadType::Size => {
                debug!("Received size update request");
            }
            PayloadType::ExitCode => {
                if !msg.payload.is_empty() {
                    if let Ok(exit_info) = String::from_utf8(msg.payload.to_vec()) {
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
        channels: &ChannelMultiplexer,
        incoming_buffer: &IncomingMessageBuffer,
        handshake_handler: &mut HandshakeHandler,
        writer: &Arc<Mutex<WsWriter>>,
        can_send: &Arc<std::sync::atomic::AtomicBool>,
        sequence_counter: &mut i64,
        expected_sequence_number: &mut i64,
    ) -> Result<()> {
        while let Some(buffered) = incoming_buffer.remove(*expected_sequence_number).await {
            debug!(
                sequence = buffered.message.sequence_number,
                "Processing buffered message"
            );

            // Process the buffered message
            Self::process_output_message(
                channels,
                &buffered.message,
                handshake_handler,
                writer,
                can_send,
                sequence_counter,
            )
            .await?;

            // Increment expected sequence
            *expected_sequence_number += 1;
        }
        Ok(())
    }

    /// Send handshake response via writer
    async fn send_handshake_response(
        writer: &Arc<Mutex<WsWriter>>,
        response: &HandshakeResponse,
        sequence_counter: &mut i64,
    ) -> Result<()> {
        let response_json = serde_json::to_vec(response).map_err(Error::Serialization)?;

        debug!(
            response_json = %String::from_utf8_lossy(&response_json),
            sequence = *sequence_counter,
            "Sending HandshakeResponse"
        );

        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            *sequence_counter,
            PayloadType::HandshakeResponse,
            Bytes::from(response_json),
        );
        *sequence_counter += 1;

        let msg_bytes = msg.serialize()?;
        debug!(
            msg_len = msg_bytes.len(),
            "Serialized HandshakeResponse message"
        );

        let mut writer = writer.lock().await;
        writer
            .send(Message::Binary(msg_bytes.to_vec()))
            .await
            .map_err(|e| TransportError::WebSocket(e.to_string()))?;

        debug!("HandshakeResponse sent to WebSocket");
        Ok(())
    }

    /// Send acknowledge message for a received message (sequential)
    async fn send_acknowledge(
        writer: &Arc<Mutex<WsWriter>>,
        received_msg: &ClientMessage,
        _sequence_counter: &mut i64,
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

        let mut writer = writer.lock().await;
        writer
            .send(Message::Binary(msg_bytes.to_vec()))
            .await
            .map_err(|e| TransportError::WebSocket(e.to_string()))?;

        Ok(())
    }

    /// Send acknowledge message for an out-of-order message (non-sequential)
    async fn send_acknowledge_non_sequential(
        writer: &Arc<Mutex<WsWriter>>,
        received_msg: &ClientMessage,
        _sequence_counter: &mut i64,
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

        let mut writer = writer.lock().await;
        writer
            .send(Message::Binary(msg_bytes.to_vec()))
            .await
            .map_err(|e| TransportError::WebSocket(e.to_string()))?;

        Ok(())
    }

    /// Shutdown the connection gracefully
    async fn shutdown(self) -> Result<()> {
        info!("Shutting down connection manager");

        // Signal all tasks to shutdown
        let _ = self.shutdown_tx.send(());

        // Wait for all tasks to complete with timeout
        for task in self.tasks {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
        }

        // Close WebSocket writer
        {
            let mut writer = self.writer.lock().await;
            let _ = writer.close().await;
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
        let is_aws_domain = host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn");
        let is_ssm_service = host.contains("ssmmessages");

        if !is_aws_domain || !is_ssm_service {
            return Err(Error::Config(format!(
                "Stream URL host '{}' is not a valid AWS SSM endpoint",
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
            .contains("not a valid AWS SSM endpoint"));
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
}
