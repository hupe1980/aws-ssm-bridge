//! smux v1 stream multiplexer over an SSM data channel.
//!
//! Implements the [xtaci/smux v1 protocol][smux] as a framing layer on top of
//! an SSM [`Session`], enabling multiple independent TCP connections to share a
//! single SSM data channel without corrupting each other.
//!
//! This is the same multiplexing scheme used by the official
//! `session-manager-plugin` binary for port-forwarding sessions.
//!
//! [smux]: https://github.com/xtaci/smux
//!
//! # Frame wire format (8-byte header, little-endian)
//!
//! ```text
//! ┌────────┬────────┬─────────────┬───────────────────────┐
//! │ ver(1) │ cmd(1) │  length(2)  │     stream_id(4)      │
//! ├────────┴────────┴─────────────┴───────────────────────┤
//! │               payload  (length bytes)                  │
//! └────────────────────────────────────────────────────────┘
//! ```
//!
//! Commands: `SYN=0`, `FIN=1`, `PSH=2`, `NOP=3`

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering},
    Arc, Mutex,
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Notify};
use tokio::time::interval;
use tokio_util::sync::PollSender;
use tracing::{debug, trace, warn};

use crate::errors::{Error, Result};
use crate::session::Session;

// ─────────────────────────────────────────────────────────────────────────────
// Protocol constants – must match xtaci/smux v1
// ─────────────────────────────────────────────────────────────────────────────

const VERSION: u8 = 1;
const CMD_SYN: u8 = 0;
const CMD_FIN: u8 = 1;
const CMD_PSH: u8 = 2;
const CMD_NOP: u8 = 3;

/// Fixed header size in bytes.
const HEADER_SIZE: usize = 8;

/// Maximum PSH payload per frame (32 KiB – matches xtaci/smux default).
const MAX_PAYLOAD: usize = 32 * 1024;

/// Outbound frame channel capacity (frames queued for the send task).
const FRAME_CHANNEL_CAP: usize = 256;

/// Per-stream inbound data channel capacity.
const STREAM_CHANNEL_CAP: usize = 64;

/// Keep-alive NOP interval (10 s – matches xtaci/smux default).
const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

// ─────────────────────────────────────────────────────────────────────────────
// Frame encode / decode
// ─────────────────────────────────────────────────────────────────────────────

fn encode_frame(cmd: u8, stream_id: u32, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(VERSION);
    buf.put_u8(cmd);
    buf.put_u16_le(payload.len() as u16);
    buf.put_u32_le(stream_id);
    buf.put_slice(payload);
    buf.freeze()
}

#[inline]
fn encode_ctrl(cmd: u8, stream_id: u32) -> Bytes {
    encode_frame(cmd, stream_id, &[])
}

/// Try to decode one complete frame from `buf`.
///
/// Consumes the frame bytes on success; returns `None` when there are not
/// enough bytes for a complete frame yet.
///
/// Frames with an unexpected version byte are discarded with a warning so
/// the stream stays synchronised even if a future agent adds a v2 frame.
fn decode_frame(buf: &mut BytesMut) -> Option<(u8, u32, Bytes)> {
    loop {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        // Peek at version and length fields without consuming.
        let version = buf[0];
        let length = u16::from_le_bytes([buf[2], buf[3]]) as usize;
        if buf.len() < HEADER_SIZE + length {
            return None;
        }
        // M-8: Validate version byte — discard unknown-version frames to avoid
        // mis-routing a future smux v2 command byte as a v1 command.
        // Loop (not return None) so frames buffered after the bad one are
        // still processed without waiting for more bytes to arrive.
        if version != VERSION {
            warn!(version, "Unexpected smux version byte — discarding frame");
            buf.advance(HEADER_SIZE + length);
            continue;
        }
        buf.advance(1); // version (validated above)
        let cmd = buf.get_u8();
        buf.advance(2); // length (already peeked)
        let stream_id = buf.get_u32_le();
        let payload = buf.split_to(length).freeze();
        return Some((cmd, stream_id, payload));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream close reason
// ─────────────────────────────────────────────────────────────────────────────

/// Why a [`SmuxStream`]'s read half reached EOF.
///
/// Available via [`SmuxStream::close_reason`] once `AsyncRead` returns
/// `Poll::Ready(Ok(()))` with an empty buffer (i.e. EOF).
///
/// Distinguishing a clean close from a slow-consumer eviction lets callers
/// log diagnostics or trigger metrics without having to instrument the entire
/// mux pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCloseReason {
    /// Normal close: remote sent a FIN frame or the session ended cleanly.
    Clean = 0,
    /// Slow consumer: the per-stream receive buffer was full when the mux
    /// tried to deliver a frame.  The mux sent a FIN to the remote and evicted
    /// this stream to prevent head-of-line blocking for other streams.
    SlowConsumer = 1,
}

// Sentinel stored in `SmuxStream::close_reason_tag` while the stream is still open.
const REASON_OPEN: u8 = 255;

/// Per-stream entry stored in [`Inner::streams`]:
/// the inbound-data sender and a close-reason tag shared with the stream reader.
type StreamEntry = (mpsc::Sender<Bytes>, Arc<AtomicU8>);

// ─────────────────────────────────────────────────────────────────────────────
// Shared inner state
// ─────────────────────────────────────────────────────────────────────────────

struct Inner {
    /// Active streams: stream_id → (sender for inbound data, close-reason tag).
    streams: Mutex<HashMap<u32, StreamEntry>>,
    /// Outbound frame queue consumed by the send task.
    frame_tx: mpsc::Sender<Bytes>,
    /// Next stream ID for client-initiated streams (odd: 1, 3, 5 …).
    next_id: AtomicU32,
    /// Set once the mux session is torn down.
    closed: AtomicBool,
    /// Notified on session close so background tasks wake up promptly.
    die: Notify,
}

impl Inner {
    /// Close the session, wake all background tasks, and clear stream channels.
    fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.die.notify_waiters();
        }
        // Mark all remaining open streams as Clean before dropping the senders.
        // This ensures SmuxStream::close_reason() returns Clean for streams that
        // are closed due to session teardown rather than slow-consumer eviction.
        let mut streams = self.streams.lock().unwrap();
        for (_, reason_tag) in streams.values() {
            reason_tag
                .compare_exchange(
                    REASON_OPEN,
                    StreamCloseReason::Clean as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .ok();
        }
        streams.clear();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Dispatch an inbound PSH frame to its stream, evicting a dead entry if needed.
    ///
    /// Uses `try_send` rather than `send().await` to avoid head-of-line
    /// blocking: a single slow consumer cannot stall frame delivery for all
    /// other streams.  When the per-stream buffer is full the stream is closed
    /// (the consumer sees EOF) rather than blocking the entire mux.
    fn route_psh(&self, stream_id: u32, data: Bytes) {
        let entry = self
            .streams
            .lock()
            .unwrap()
            .get(&stream_id)
            .map(|(tx, reason)| (tx.clone(), Arc::clone(reason)));
        if entry.is_none() {
            debug!(
                stream_id,
                "smux route_psh: no stream registered for id — dropping frame"
            );
        }
        if let Some((tx, reason_tag)) = entry {
            trace!(
                stream_id,
                bytes = data.len(),
                "smux route_psh: routing to stream"
            );
            match tx.try_send(data) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Consumer too slow — tag and evict this stream so other streams
                    // are not blocked.  The SmuxStream will observe SlowConsumer
                    // via `close_reason()` after reading EOF.
                    warn!(
                        stream_id,
                        "Per-stream receive buffer full — evicting slow consumer"
                    );
                    reason_tag.store(StreamCloseReason::SlowConsumer as u8, Ordering::SeqCst);
                    self.streams.lock().unwrap().remove(&stream_id);
                    // Best-effort FIN to the remote agent so it can release resources.
                    let _ = self.frame_tx.try_send(encode_ctrl(CMD_FIN, stream_id));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Stream already gone — evict the dead entry.
                    self.streams.lock().unwrap().remove(&stream_id);
                }
            }
        }
    }

    /// Drop the stream sender so the stream's `data_rx` sees EOF (clean close).
    fn route_fin(&self, stream_id: u32) {
        if let Some((_, reason_tag)) = self.streams.lock().unwrap().remove(&stream_id) {
            // Only mark Clean if the reason hasn't already been set to SlowConsumer.
            reason_tag
                .compare_exchange(
                    REASON_OPEN,
                    StreamCloseReason::Clean as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .ok();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Background tasks
// ─────────────────────────────────────────────────────────────────────────────

/// Reads raw bytes from the SSM session, reassembles smux frames, and routes
/// them to the appropriate per-stream channels.
async fn recv_task(mut output_rx: mpsc::Receiver<Bytes>, inner: Arc<Inner>) {
    let mut buf = BytesMut::new();

    loop {
        tokio::select! {
            biased;
            _ = inner.die.notified() => break,
            chunk = output_rx.recv() => {
                match chunk {
                    Some(bytes) => {
                        trace!(len = bytes.len(), "smux recv_task: chunk received");
                        buf.extend_from_slice(&bytes);
                        if !dispatch_frames(&mut buf, &inner) {
                            break; // protocol violation — mux torn down
                        }
                    }
                    None => {
                        debug!("smux recv_task: output_rx closed (no more data)");
                        break; // SSM session closed
                    }
                }
            }
        }
    }

    // Flush any remaining complete frames before terminating.
    dispatch_frames(&mut buf, &inner);
    inner.close();
}

/// Dispatches all complete frames from `buf` into their per-stream channels.
///
/// Returns `true` when the buffer is exhausted (more data needed) and `false`
/// when a protocol violation is detected (e.g. oversized length field).  On
/// `false` the mux is already closed and `recv_task` should exit immediately.
fn dispatch_frames(buf: &mut BytesMut, inner: &Inner) -> bool {
    loop {
        // Guard against garbled headers: a declared payload length that
        // exceeds MAX_PAYLOAD means the framing is corrupted.  Continuing to
        // buffer would allow unbounded memory growth while waiting for a frame
        // that may never arrive — treat it as a fatal protocol violation.
        if buf.len() >= HEADER_SIZE {
            let version = buf[0];
            let length = u16::from_le_bytes([buf[2], buf[3]]) as usize;
            // Only enforce MAX_PAYLOAD for version-1 frames.  Unknown-version
            // frames are discarded by decode_frame (buf.advance past the whole
            // frame), so applying this guard to them would misclassify a
            // garbled-length unknown-version frame as a v1 protocol violation.
            if version == VERSION && length > MAX_PAYLOAD {
                warn!(
                    length,
                    MAX_PAYLOAD,
                    "smux frame length exceeds MAX_PAYLOAD — protocol violation, tearing down mux"
                );
                buf.clear();
                inner.close();
                return false;
            }
        }
        match decode_frame(buf) {
            None => return true, // need more data
            Some((cmd, stream_id, data)) => {
                trace!(
                    cmd,
                    stream_id,
                    payload_len = data.len(),
                    "smux dispatch_frames: decoded frame"
                );
                match cmd {
                    CMD_PSH => inner.route_psh(stream_id, data),
                    CMD_FIN => {
                        debug!(stream_id, "smux dispatch_frames: FIN received");
                        inner.route_fin(stream_id);
                    }
                    CMD_NOP | CMD_SYN => {}
                    _ => warn!(cmd, stream_id, "Unknown smux command – ignoring"),
                }
            }
        }
    }
}

/// Drains the outbound frame queue and writes each frame to the SSM session.
///
/// Exits immediately when `inner.die` is notified so that a stalled WebSocket
/// writer does not keep the task alive after session teardown.
async fn send_task(session: Arc<Session>, mut frame_rx: mpsc::Receiver<Bytes>, inner: Arc<Inner>) {
    loop {
        // IMPORTANT: register the `die` subscription BEFORE the `is_closed()`
        // latch check.  `Notify` is edge-triggered: `notify_waiters()` only wakes
        // futures that are *currently* registered.  The wrong order is:
        //   1. check is_closed() → false
        //   2. close() fires notify_waiters()   ← notification lost
        //   3. register notified()              ← never woken
        // By pinning first we guarantee: if close() fires after pin but before
        // select!, the wakeup is captured; if it fired before pin, is_closed()
        // catches it on the latch check immediately below.
        let die = inner.die.notified();
        tokio::pin!(die);
        if inner.is_closed() {
            break;
        }
        tokio::select! {
            biased;
            _ = &mut die => break,
            frame = frame_rx.recv() => {
                match frame {
                    Some(f) => {
                        // Subscribe to `die` *before* starting the send so that
                        // a concurrent `notify_waiters()` during the send is not
                        // missed (Tokio's Notify only wakes current subscribers).
                        let send_die = inner.die.notified();
                        tokio::pin!(send_die);
                        tokio::select! {
                            biased;
                            _ = &mut send_die => break,
                            result = session.send(f) => {
                                if let Err(e) = result {
                                    warn!(error = ?e, "smux send task: session error");
                                    break;
                                }
                            }
                        }
                    }
                    None => break,
                }
            }
        }
    }
    inner.close();
}

/// Sends a NOP keep-alive frame at `KEEPALIVE_INTERVAL` to prevent the SSM
/// agent from timing out an idle multiplexed session.
///
/// **Note**: The official `session-manager-plugin` disables smux keepalive
/// (`KeepAliveDisabled = true`) to let SSM's own idle-timeout mechanism work
/// correctly.  Only enable this when you have confirmed the target agent
/// does not interpret NOP frames as activity.
async fn keepalive_task(inner: Arc<Inner>) {
    let mut ticker = interval(KEEPALIVE_INTERVAL);
    ticker.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            biased;
            _ = inner.die.notified() => break,
            _ = ticker.tick() => {
                if inner.is_closed() {
                    break;
                }
                let nop = encode_ctrl(CMD_NOP, 0);
                match inner.frame_tx.try_send(nop) {
                    Ok(()) => {}
                    // Channel temporarily full — skip this NOP tick; the
                    // session is busy but still alive, so don't stop keepalives.
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    // Channel closed — send task has exited; stop keepalives.
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SmuxSession
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the smux multiplexer.
#[derive(Debug, Clone, Default)]
pub struct SmuxConfig {
    /// Enable NOP keepalive frames.
    ///
    /// Defaults to `false` to match the official `session-manager-plugin`
    /// behaviour (`KeepAliveDisabled = true`).  Enabling keepalive can
    /// interfere with SSM's idle-timeout enforcement.
    pub keepalive: bool,
}

/// smux v1 client session multiplexing multiple TCP connections over a single
/// SSM data channel.
///
/// Create once when the SSM session is ready, then call
/// [`open_stream`](SmuxSession::open_stream) for each accepted TCP connection.
/// The returned [`SmuxStream`] implements [`AsyncRead`] and [`AsyncWrite`] and
/// can be passed directly to [`tokio::io::copy_bidirectional`].
///
/// # Background tasks
///
/// `SmuxSession::new` spawns two background tasks (three when
/// [`SmuxConfig::keepalive`] is enabled):
///
/// * **recv** – reads raw bytes from the SSM session, reassembles smux frames,
///   and routes each PSH frame to the correct per-stream channel.
/// * **send** – drains the shared outbound frame queue and writes to the SSM
///   session, preserving frame ordering.
/// * **keepalive** *(optional, requires `SmuxConfig { keepalive: true }`)* –
///   sends a NOP frame every 10 seconds so the remote SSM agent does not time
///   out an idle session.  Disabled by default to match the official
///   `session-manager-plugin` behaviour.
pub struct SmuxSession {
    inner: Arc<Inner>,
}

impl SmuxSession {
    /// Wrap an existing, ready SSM session in a smux multiplexer.
    ///
    /// The `session` should already be in the `Connected` state – i.e.
    /// [`Session::wait_for_ready`] must have returned `true` before calling
    /// this.
    ///
    /// Pass [`SmuxConfig::default()`] unless you have a specific reason to
    /// deviate from the official plugin's defaults (keepalive disabled).
    pub fn new(session: Arc<Session>, config: SmuxConfig) -> Self {
        let (frame_tx, frame_rx) = mpsc::channel::<Bytes>(FRAME_CHANNEL_CAP);

        let inner = Arc::new(Inner {
            streams: Mutex::new(HashMap::new()),
            frame_tx,
            next_id: AtomicU32::new(1), // client uses odd IDs (1, 3, 5 …)
            closed: AtomicBool::new(false),
            die: Notify::new(),
        });

        let output_rx = session.subscribe_output();
        debug!("Subscribed to session output (direct_subs)");

        tokio::spawn(recv_task(output_rx, Arc::clone(&inner)));
        tokio::spawn(send_task(
            Arc::clone(&session),
            frame_rx,
            Arc::clone(&inner),
        ));
        if config.keepalive {
            tokio::spawn(keepalive_task(Arc::clone(&inner)));
        }

        Self { inner }
    }

    /// Open a new logical stream for an accepted TCP connection.
    ///
    /// Allocates the next odd stream ID, registers a per-stream receive
    /// channel, and sends a SYN frame to the remote SSM agent.
    pub fn open_stream(&self) -> Result<SmuxStream> {
        if self.inner.is_closed() {
            return Err(Error::InvalidState("smux session is closed".to_string()));
        }

        let stream_id = self.inner.next_id.fetch_add(2, Ordering::SeqCst);
        let (data_tx, data_rx) = mpsc::channel::<Bytes>(STREAM_CHANNEL_CAP);
        let close_reason_tag = Arc::new(AtomicU8::new(REASON_OPEN));

        self.inner
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, (data_tx, Arc::clone(&close_reason_tag)));

        // Inform the remote agent of the new stream.
        // Use try_send: open_stream is sync and the channel has ample capacity.
        match self
            .inner
            .frame_tx
            .try_send(encode_ctrl(CMD_SYN, stream_id))
        {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // send task has already exited — the session is gone.
                self.inner.streams.lock().unwrap().remove(&stream_id);
                return Err(Error::InvalidState("smux send task has exited".to_string()));
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Transient back-pressure — caller should retry shortly.
                self.inner.streams.lock().unwrap().remove(&stream_id);
                return Err(Error::InvalidState(
                    "smux frame queue is full; retry open_stream".to_string(),
                ));
            }
        }

        debug!(stream_id, "Opened smux stream");

        let frame_sink = PollSender::new(self.inner.frame_tx.clone());

        Ok(SmuxStream {
            stream_id,
            inner: Arc::clone(&self.inner),
            data_rx,
            current_chunk: None,
            read_closed: false,
            write_closed: false,
            close_reason_tag,
            frame_sink,
        })
    }

    /// Returns `true` if the underlying SSM session has been torn down.
    #[allow(dead_code)]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Drop for SmuxSession {
    fn drop(&mut self) {
        self.inner.close();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SmuxStream – AsyncRead + AsyncWrite
// ─────────────────────────────────────────────────────────────────────────────

/// A single logical stream within a [`SmuxSession`].
///
/// Implements [`AsyncRead`] and [`AsyncWrite`]; pass it (mutably borrowed)
/// directly to [`tokio::io::copy_bidirectional`] alongside the local
/// [`TcpStream`].
///
/// Dropping the stream sends a FIN frame and removes it from the routing table
/// even if [`AsyncWrite::poll_shutdown`] was never called.
///
/// [`TcpStream`]: tokio::net::TcpStream
pub struct SmuxStream {
    stream_id: u32,
    inner: Arc<Inner>,
    /// Incoming data dispatched by the recv task.
    data_rx: mpsc::Receiver<Bytes>,
    /// Leftover bytes from a previous partial read.
    current_chunk: Option<Bytes>,
    read_closed: bool,
    write_closed: bool,
    /// Shared atomic set by the mux when it evicts this stream.
    /// `REASON_OPEN` while open; `StreamCloseReason` discriminant when closed.
    close_reason_tag: Arc<AtomicU8>,
    /// Backpressured sender for outbound PSH frames.
    frame_sink: PollSender<Bytes>,
}

impl SmuxStream {
    /// Returns the reason the read half reached EOF, or `None` if the stream
    /// is still open.
    ///
    /// This is only meaningful after `AsyncRead` has returned an empty read
    /// (i.e. EOF).  Calling it while the stream is still live always returns
    /// `None`.
    pub fn close_reason(&self) -> Option<StreamCloseReason> {
        if !self.read_closed {
            return None;
        }
        match self.close_reason_tag.load(Ordering::SeqCst) {
            x if x == StreamCloseReason::SlowConsumer as u8 => {
                Some(StreamCloseReason::SlowConsumer)
            }
            _ => Some(StreamCloseReason::Clean),
        }
    }
}

impl AsyncRead for SmuxStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this.read_closed {
            return Poll::Ready(Ok(())); // EOF
        }

        // Drain leftover bytes from a previous partial read BEFORE checking whether
        // the mux has closed.  A closed mux does not invalidate already-buffered data;
        // returning BrokenPipe here would silently drop buffered bytes and cause
        // copy_bidirectional to fail with a noisy error on normal session teardown.
        if let Some(ref mut chunk) = this.current_chunk {
            let n = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..n]);
            chunk.advance(n);
            if chunk.is_empty() {
                this.current_chunk = None;
            }
            return Poll::Ready(Ok(()));
        }

        // Poll the channel before consulting is_closed().  There may still be
        // frames queued for this stream even after the mux has been closed
        // (route_psh enqueues before the close flag is set).  Checking
        // is_closed() first would silently drop those bytes; instead we only
        // use is_closed() to convert a Pending result into EOF once the channel
        // is confirmed empty.
        match this.data_rx.poll_recv(cx) {
            Poll::Ready(Some(mut chunk)) => {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..n]);
                chunk.advance(n);
                if !chunk.is_empty() {
                    this.current_chunk = Some(chunk);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => {
                // Sender dropped: FIN received or session closed → EOF.
                this.read_closed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                // No data queued.  If the mux is closed no more frames will
                // ever arrive for this stream — convert Pending to clean EOF.
                if this.inner.is_closed() {
                    this.read_closed = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for SmuxStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        if this.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream is write-closed",
            )));
        }
        if this.inner.is_closed() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "smux session closed",
            )));
        }

        // Respect MAX_PAYLOAD; the caller retries for the remainder.
        let n = buf.len().min(MAX_PAYLOAD);

        // Reserve a slot in the bounded outbound channel before encoding the
        // frame; if the channel is full we park until space is available.
        match this.frame_sink.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "smux session closed",
            ))),
            Poll::Ready(Ok(())) => {
                let frame = encode_frame(CMD_PSH, this.stream_id, &buf[..n]);
                this.frame_sink.send_item(frame).map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "smux session closed")
                })?;
                Poll::Ready(Ok(n))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(())) // frames are queued immediately on write
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_closed {
            return Poll::Ready(Ok(()));
        }
        // Reserve a slot for the FIN frame, applying backpressure if the
        // outbound channel is full (same mechanism as poll_write).
        match this.frame_sink.poll_reserve(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => {
                // Mux is already closed; treat as a clean shutdown since the
                // remote side observes channel close through other signals.
                this.write_closed = true;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Ok(())) => {}
        }
        this.write_closed = true;
        // Ignore error: if the mux closed between poll_reserve and send_item
        // the stream is already torn down on both sides.
        this.frame_sink
            .send_item(encode_ctrl(CMD_FIN, this.stream_id))
            .ok();
        debug!(stream_id = this.stream_id, "Sent FIN");
        Poll::Ready(Ok(()))
    }
}

impl Drop for SmuxStream {
    fn drop(&mut self) {
        // Best-effort FIN on early drop (e.g. handler error path).
        if !self.write_closed {
            self.write_closed = true;
            self.inner
                .frame_tx
                .try_send(encode_ctrl(CMD_FIN, self.stream_id))
                .ok();
        }
        self.inner.streams.lock().unwrap().remove(&self.stream_id);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the 8-byte frame header is encoded correctly (little-endian).
    #[test]
    fn test_encode_decode_frame() {
        let payload = b"hello";
        let frame = encode_frame(CMD_PSH, 0x0000_0003, payload);

        assert_eq!(frame.len(), HEADER_SIZE + payload.len());
        assert_eq!(frame[0], VERSION);
        assert_eq!(frame[1], CMD_PSH);
        // length = 5, little-endian
        assert_eq!(&frame[2..4], &[5u8, 0]);
        // stream_id = 3, little-endian
        assert_eq!(&frame[4..8], &[3u8, 0, 0, 0]);
        assert_eq!(&frame[8..], payload);
    }

    /// Verify decode_frame is the left-inverse of encode_frame.
    #[test]
    fn test_roundtrip() {
        let original = b"smux test payload";
        let stream_id = 7u32;
        let encoded = encode_frame(CMD_PSH, stream_id, original);

        let mut buf = BytesMut::from(&encoded[..]);
        let (cmd, sid, data) = decode_frame(&mut buf).expect("should decode");

        assert_eq!(cmd, CMD_PSH);
        assert_eq!(sid, stream_id);
        assert_eq!(&data[..], original);
        assert!(buf.is_empty());
    }

    /// Partial frame should return None (more bytes needed).
    #[test]
    fn test_partial_frame_returns_none() {
        let frame = encode_frame(CMD_PSH, 1, b"data");
        // Feed only the header, not the payload.
        let mut buf = BytesMut::from(&frame[..HEADER_SIZE]);
        assert!(decode_frame(&mut buf).is_none());
    }

    /// A control frame (no payload) round-trips cleanly.
    #[test]
    fn test_ctrl_frame() {
        let frame = encode_ctrl(CMD_SYN, 5);
        assert_eq!(frame.len(), HEADER_SIZE);

        let mut buf = BytesMut::from(&frame[..]);
        let (cmd, sid, data) = decode_frame(&mut buf).expect("should decode");

        assert_eq!(cmd, CMD_SYN);
        assert_eq!(sid, 5);
        assert!(data.is_empty());
    }

    /// M-8: An unknown version byte must cause the frame to be discarded
    /// without corrupting the stream position.
    #[test]
    fn test_unknown_version_discarded() {
        // Build a well-formed frame then corrupt the version byte.
        let mut frame = BytesMut::from(&encode_frame(CMD_PSH, 3, b"payload")[..]);
        frame[0] = 0x02; // version 2 — not supported

        let mut buf = frame;
        // decode_frame must discard the frame and return None.
        assert!(
            decode_frame(&mut buf).is_none(),
            "Unknown version must be discarded"
        );
        // Buffer must be fully consumed (no desync).
        assert!(buf.is_empty(), "Buffer must be drained after discard");
    }
}
