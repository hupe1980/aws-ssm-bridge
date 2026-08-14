//! smux v1 stream multiplexing over one SSM data channel.
//!
//! A port-forwarding session carries many TCP connections over a single
//! WebSocket. The SSM agent frames them with [xtaci/smux] v1, the same scheme
//! the official plugin uses, so every byte is tagged with the logical stream it
//! belongs to.
//!
//! ```text
//!  ┌────────┬────────┬─────────────┬──────────────────┐
//!  │ ver(1) │ cmd(1) │ length(2,LE)│ stream_id(4, LE) │
//!  ├────────┴────────┴─────────────┴──────────────────┤
//!  │                payload (length bytes)             │
//!  └───────────────────────────────────────────────────┘
//!
//!  cmd: SYN=0  FIN=1  PSH=2  NOP=3
//! ```
//!
//! Client-initiated streams use odd IDs. smux v1 has no per-stream flow
//! control — that arrived in v2 — so a consumer that stops reading is handled
//! here by eviction rather than backpressure; see [`SmuxSession`].
//!
//! [xtaci/smux]: https://github.com/xtaci/smux

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::PollSender;
use tracing::{debug, trace, warn};

use crate::errors::{Error, Result};
use crate::session::Session;

const VERSION: u8 = 1;
const CMD_SYN: u8 = 0;
const CMD_FIN: u8 = 1;
const CMD_PSH: u8 = 2;
const CMD_NOP: u8 = 3;

/// Fixed smux header size.
const HEADER_SIZE: usize = 8;

/// Largest payload one smux frame may carry (32 KiB, the xtaci/smux default).
const MAX_FRAME_PAYLOAD: usize = 32 * 1024;

/// Outbound frame queue depth, shared by all streams.
const FRAME_QUEUE_DEPTH: usize = 256;

/// Per-stream inbound queue depth, in frames.
const STREAM_QUEUE_DEPTH: usize = 256;

/// Keep-alive interval when [`SmuxConfig::keepalive`] is on.
const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

fn encode(cmd: u8, stream_id: u32, payload: &[u8]) -> Bytes {
    debug_assert!(payload.len() <= MAX_FRAME_PAYLOAD);
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(VERSION);
    buf.put_u8(cmd);
    buf.put_u16_le(payload.len() as u16);
    buf.put_u32_le(stream_id);
    buf.put_slice(payload);
    buf.freeze()
}

#[inline]
fn control(cmd: u8, stream_id: u32) -> Bytes {
    encode(cmd, stream_id, &[])
}

/// One decoded frame.
struct Frame {
    cmd: u8,
    stream_id: u32,
    payload: Bytes,
}

/// Outcome of trying to decode from the reassembly buffer.
enum Decoded {
    /// A complete frame was consumed.
    Frame(Frame),
    /// More bytes are needed.
    Incomplete,
    /// The stream is not smux and cannot be resynchronised.
    Corrupt(&'static str),
}

// ---------------------------------------------------------------------------
// Shared session state
// ---------------------------------------------------------------------------

/// Sentinel stored in a stream's close tag while it is still open.
const REASON_OPEN: u8 = u8::MAX;

/// Why a [`SmuxStream`] reached end of file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCloseReason {
    /// The remote sent FIN, or the session ended normally.
    Clean = 0,
    /// This stream's consumer stopped reading and was evicted to keep the other
    /// streams flowing. Bytes were lost.
    SlowConsumer = 1,
    /// The multiplexer tore down because the byte stream stopped being valid
    /// smux. Every stream on the session is affected.
    Desynchronised = 2,
}

impl std::fmt::Display for StreamCloseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StreamCloseReason::Clean => "closed cleanly",
            StreamCloseReason::SlowConsumer => "evicted: the consumer stopped reading",
            StreamCloseReason::Desynchronised => "aborted: smux framing was lost",
        })
    }
}

#[derive(Debug)]
struct StreamEntry {
    inbound: mpsc::Sender<Bytes>,
    close_reason: Arc<AtomicU8>,
}

#[derive(Debug)]
struct Inner {
    streams: Mutex<HashMap<u32, StreamEntry>>,
    frames: mpsc::Sender<Bytes>,
    next_id: AtomicU32,
    closed: AtomicBool,
    die: Notify,
}

impl Inner {
    fn close(&self, reason: StreamCloseReason) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.die.notify_waiters();

        let mut streams = self.lock();
        for entry in streams.values() {
            // Do not overwrite a per-stream reason that was already recorded.
            let _ = entry.close_reason.compare_exchange(
                REASON_OPEN,
                reason as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        streams.clear();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u32, StreamEntry>> {
        self.streams.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Deliver a PSH frame to its stream.
    ///
    /// Uses `try_send`: blocking here would let one stalled TCP connection stop
    /// delivery for every other stream sharing the channel. A stream whose queue
    /// is full is evicted instead, and its reader learns why from
    /// [`SmuxStream::close_reason`].
    fn route_push(&self, stream_id: u32, payload: Bytes) {
        let entry = self
            .lock()
            .get(&stream_id)
            .map(|e| (e.inbound.clone(), Arc::clone(&e.close_reason)));

        let Some((inbound, close_reason)) = entry else {
            trace!(stream_id, "data for an unknown stream; replying with FIN");
            let _ = self.frames.try_send(control(CMD_FIN, stream_id));
            return;
        };

        match inbound.try_send(payload) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(stream_id, "stream consumer is not keeping up; evicting it");
                close_reason.store(StreamCloseReason::SlowConsumer as u8, Ordering::Release);
                self.lock().remove(&stream_id);
                let _ = self.frames.try_send(control(CMD_FIN, stream_id));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.lock().remove(&stream_id);
            }
        }
    }

    /// Handle a FIN: dropping the sender gives the reader a clean EOF.
    fn route_fin(&self, stream_id: u32) {
        if let Some(entry) = self.lock().remove(&stream_id) {
            let _ = entry.close_reason.compare_exchange(
                REASON_OPEN,
                StreamCloseReason::Clean as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

/// Reassembles smux frames from the session's byte stream and routes them.
async fn recv_task(session: Arc<Session>, inner: Arc<Inner>) {
    let mut output = session.output();
    let mut buf = BytesMut::new();
    let mut reason = StreamCloseReason::Clean;

    loop {
        let chunk = tokio::select! {
            biased;
            () = inner.die.notified() => break,
            chunk = output.next() => chunk,
        };

        let Some(chunk) = chunk else {
            if output.lagged() {
                // Bytes were dropped between the session and this parser, so
                // every subsequent frame boundary is guesswork.
                warn!("smux reader fell behind the session; frame sync is lost");
                reason = StreamCloseReason::Desynchronised;
            } else {
                debug!("session output ended; closing the multiplexer");
            }
            break;
        };

        buf.extend_from_slice(&chunk);
        if let Err(problem) = dispatch(&mut buf, &inner) {
            warn!(
                problem,
                "smux framing is corrupt; tearing down the multiplexer"
            );
            reason = StreamCloseReason::Desynchronised;
            break;
        }
    }

    inner.close(reason);
    debug!("smux receive task finished");
}

/// Drain complete frames from `buf`.
fn dispatch(buf: &mut BytesMut, inner: &Inner) -> std::result::Result<(), &'static str> {
    loop {
        match decode_frame(buf) {
            Decoded::Incomplete => return Ok(()),
            Decoded::Corrupt(problem) => {
                buf.clear();
                return Err(problem);
            }
            Decoded::Frame(frame) => match frame.cmd {
                CMD_PSH => inner.route_push(frame.stream_id, frame.payload),
                CMD_FIN => {
                    trace!(stream_id = frame.stream_id, "stream closed by the remote");
                    inner.route_fin(frame.stream_id);
                }
                // The agent never opens streams towards the client, and NOP is
                // pure keep-alive. Both are safely ignored.
                CMD_SYN | CMD_NOP => {}
                other => trace!(cmd = other, "ignoring an unknown smux command"),
            },
        }
    }
}

/// Decode one frame from the front of `buf`, consuming its bytes on success.
///
/// A wrong version byte or an over-long length field means the byte stream is
/// not smux at this offset. smux has no framing marker to resynchronise on, so
/// the only honest response is to fail the multiplexer rather than guess where
/// the next frame starts and hand corrupt bytes to a real TCP connection.
fn decode_frame(buf: &mut BytesMut) -> Decoded {
    if buf.len() < HEADER_SIZE {
        return Decoded::Incomplete;
    }
    if buf[0] != VERSION {
        return Decoded::Corrupt("unexpected smux version byte");
    }
    let length = u16::from_le_bytes([buf[2], buf[3]]) as usize;
    if length > MAX_FRAME_PAYLOAD {
        return Decoded::Corrupt("smux frame length exceeds the protocol maximum");
    }
    if buf.len() < HEADER_SIZE + length {
        return Decoded::Incomplete;
    }

    buf.advance(1); // version, already validated
    let cmd = buf.get_u8();
    buf.advance(2); // length, already read
    let stream_id = buf.get_u32_le();
    let payload = buf.split_to(length).freeze();

    Decoded::Frame(Frame {
        cmd,
        stream_id,
        payload,
    })
}

/// Writes queued frames to the SSM session, preserving order.
async fn send_task(session: Arc<Session>, mut frames: mpsc::Receiver<Bytes>, inner: Arc<Inner>) {
    loop {
        // Enqueue the death notification before the latch check: `Notify` only
        // wakes futures that are already enqueued, and constructing a `Notified`
        // does not enqueue it — `enable()` does. Without that, a close landing
        // between the check and the select is dropped and this task parks.
        let die = inner.die.notified();
        tokio::pin!(die);
        die.as_mut().enable();
        if inner.is_closed() {
            break;
        }

        let frame = tokio::select! {
            biased;
            () = &mut die => break,
            () = session.closed() => break,
            frame = frames.recv() => match frame {
                Some(f) => f,
                None => break,
            },
        };

        let die = inner.die.notified();
        tokio::pin!(die);
        tokio::select! {
            biased;
            () = &mut die => break,
            result = session.send(frame) => {
                if let Err(e) = result {
                    debug!(error = %e, "session rejected an smux frame");
                    break;
                }
            }
        }
    }

    inner.close(StreamCloseReason::Clean);
    debug!("smux send task finished");
}

/// Emits NOP keep-alives so an idle multiplexed session is not reaped.
async fn keepalive_task(inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + KEEPALIVE_INTERVAL,
        KEEPALIVE_INTERVAL,
    );
    loop {
        tokio::select! {
            biased;
            () = inner.die.notified() => break,
            _ = ticker.tick() => {
                if inner.frames.try_send(control(CMD_NOP, 0)).is_err() && inner.is_closed() {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SmuxSession
// ---------------------------------------------------------------------------

/// How the multiplexer behaves.
#[derive(Debug, Clone, Copy, Default)]
pub struct SmuxConfig {
    /// Send NOP keep-alive frames every ten seconds.
    ///
    /// Off by default, matching the official plugin. SSM enforces its own idle
    /// timeout on the session; synthetic keep-alives defeat it and leave
    /// forgotten tunnels open indefinitely.
    pub keepalive: bool,
}

/// Multiplexes many TCP connections over one SSM session.
///
/// Create one per port-forwarding session, then call
/// [`open_stream`](Self::open_stream) for each accepted connection. Dropping the
/// session closes every stream.
#[derive(Debug)]
pub struct SmuxSession {
    inner: Arc<Inner>,
}

impl SmuxSession {
    /// Wrap a ready port-forwarding session.
    ///
    /// The session must have completed its handshake — call
    /// [`Session::wait_ready`] first — and must have been started with a
    /// port-forwarding document. Spawns a receive task, a send task, and a
    /// keep-alive task when [`SmuxConfig::keepalive`] is set.
    pub fn new(session: Arc<Session>, config: SmuxConfig) -> Self {
        let (frames, frame_rx) = mpsc::channel(FRAME_QUEUE_DEPTH);
        let inner = Arc::new(Inner {
            streams: Mutex::new(HashMap::new()),
            frames,
            // Clients use odd stream IDs; the agent would use even ones.
            next_id: AtomicU32::new(1),
            closed: AtomicBool::new(false),
            die: Notify::new(),
        });

        tokio::spawn(recv_task(Arc::clone(&session), Arc::clone(&inner)));
        tokio::spawn(send_task(session, frame_rx, Arc::clone(&inner)));
        if config.keepalive {
            tokio::spawn(keepalive_task(Arc::clone(&inner)));
        }

        Self { inner }
    }

    /// Open a logical stream and announce it to the agent.
    pub fn open_stream(&self) -> Result<SmuxStream> {
        if self.inner.is_closed() {
            return Err(Error::SessionClosed("the smux session is closed".into()));
        }

        let stream_id = self.inner.next_id.fetch_add(2, Ordering::Relaxed);
        let (inbound, data_rx) = mpsc::channel(STREAM_QUEUE_DEPTH);
        let close_reason = Arc::new(AtomicU8::new(REASON_OPEN));

        self.inner.lock().insert(
            stream_id,
            StreamEntry {
                inbound,
                close_reason: Arc::clone(&close_reason),
            },
        );

        if let Err(e) = self.inner.frames.try_send(control(CMD_SYN, stream_id)) {
            self.inner.lock().remove(&stream_id);
            return Err(match e {
                mpsc::error::TrySendError::Closed(_) => {
                    Error::SessionClosed("the smux session is closed".into())
                }
                mpsc::error::TrySendError::Full(_) => Error::transport(
                    "the smux frame queue is saturated; retry once the session drains",
                ),
            });
        }

        debug!(stream_id, "opened an smux stream");
        Ok(SmuxStream {
            stream_id,
            inner: Arc::clone(&self.inner),
            data_rx,
            partial: None,
            read_closed: false,
            write_closed: false,
            close_reason,
            frames: PollSender::new(self.inner.frames.clone()),
        })
    }

    /// Whether the multiplexer has shut down.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Drop for SmuxSession {
    fn drop(&mut self) {
        self.inner.close(StreamCloseReason::Clean);
    }
}

// ---------------------------------------------------------------------------
// SmuxStream
// ---------------------------------------------------------------------------

/// One logical connection inside a [`SmuxSession`].
///
/// Implements [`AsyncRead`] and [`AsyncWrite`], so it plugs straight into
/// [`tokio::io::copy`] alongside a [`TcpStream`]. Dropping it sends FIN.
///
/// [`TcpStream`]: tokio::net::TcpStream
#[derive(Debug)]
pub struct SmuxStream {
    stream_id: u32,
    inner: Arc<Inner>,
    data_rx: mpsc::Receiver<Bytes>,
    /// Remainder of a frame that did not fit in the caller's last buffer.
    partial: Option<Bytes>,
    read_closed: bool,
    write_closed: bool,
    close_reason: Arc<AtomicU8>,
    frames: PollSender<Bytes>,
}

impl SmuxStream {
    /// This stream's smux ID.
    pub fn id(&self) -> u32 {
        self.stream_id
    }

    /// Why the read half ended, or `None` while it is still open.
    ///
    /// Only [`StreamCloseReason::Clean`] means the byte stream was complete.
    pub fn close_reason(&self) -> Option<StreamCloseReason> {
        if !self.read_closed {
            return None;
        }
        Some(match self.close_reason.load(Ordering::Acquire) {
            x if x == StreamCloseReason::SlowConsumer as u8 => StreamCloseReason::SlowConsumer,
            x if x == StreamCloseReason::Desynchronised as u8 => StreamCloseReason::Desynchronised,
            _ => StreamCloseReason::Clean,
        })
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
            return Poll::Ready(Ok(()));
        }

        // Finish the previous frame before looking at anything else: already
        // received bytes stay valid even if the session has since closed.
        if let Some(chunk) = this.partial.as_mut() {
            let n = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..n]);
            chunk.advance(n);
            if chunk.is_empty() {
                this.partial = None;
            }
            return Poll::Ready(Ok(()));
        }

        match this.data_rx.poll_recv(cx) {
            Poll::Ready(Some(mut chunk)) => {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..n]);
                chunk.advance(n);
                if !chunk.is_empty() {
                    this.partial = Some(chunk);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => {
                this.read_closed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                // Poll the queue before consulting the close flag: frames routed
                // just before teardown are still worth delivering, and only an
                // empty queue plus a closed session means genuine EOF.
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
                "smux stream is write-closed",
            )));
        }
        if this.inner.is_closed() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "smux session is closed",
            )));
        }

        // Reserve queue capacity before encoding, so a full queue parks the
        // caller instead of allocating a frame that cannot be sent.
        match this.frames.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "smux session is closed",
            ))),
            Poll::Ready(Ok(())) => {
                let n = buf.len().min(MAX_FRAME_PAYLOAD);
                this.frames
                    .send_item(encode(CMD_PSH, this.stream_id, &buf[..n]))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "smux session is closed")
                    })?;
                Poll::Ready(Ok(n))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Frames are queued on write; the session layer owns delivery.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_closed {
            return Poll::Ready(Ok(()));
        }
        match this.frames.poll_reserve(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => {
                // The session is gone; the remote already sees the stream as closed.
                this.write_closed = true;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Ok(())) => {}
        }
        this.write_closed = true;
        let _ = this.frames.send_item(control(CMD_FIN, this.stream_id));
        trace!(stream_id = this.stream_id, "sent FIN");
        Poll::Ready(Ok(()))
    }
}

impl Drop for SmuxStream {
    fn drop(&mut self) {
        if !self.write_closed {
            // Best effort: tell the agent to release its end even if the caller
            // dropped us without shutting down.
            let _ = self.inner.frames.try_send(control(CMD_FIN, self.stream_id));
        }
        self.inner.lock().remove(&self.stream_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_layout_matches_smux_v1() {
        let frame = encode(CMD_PSH, 3, b"hello");
        assert_eq!(frame.len(), HEADER_SIZE + 5);
        assert_eq!(frame[0], VERSION);
        assert_eq!(frame[1], CMD_PSH);
        assert_eq!(&frame[2..4], &[5, 0], "length is little-endian");
        assert_eq!(&frame[4..8], &[3, 0, 0, 0], "stream id is little-endian");
        assert_eq!(&frame[8..], b"hello");
    }

    #[test]
    fn frames_round_trip() {
        let mut buf = BytesMut::from(&encode(CMD_PSH, 7, b"smux payload")[..]);
        match decode_frame(&mut buf) {
            Decoded::Frame(frame) => {
                assert_eq!(frame.cmd, CMD_PSH);
                assert_eq!(frame.stream_id, 7);
                assert_eq!(&frame.payload[..], b"smux payload");
            }
            _ => panic!("expected a complete frame"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn control_frames_have_no_payload() {
        let mut buf = BytesMut::from(&control(CMD_SYN, 5)[..]);
        match decode_frame(&mut buf) {
            Decoded::Frame(frame) => {
                assert_eq!(frame.cmd, CMD_SYN);
                assert_eq!(frame.stream_id, 5);
                assert!(frame.payload.is_empty());
            }
            _ => panic!("expected a complete frame"),
        }
    }

    #[test]
    fn a_partial_frame_waits_for_more_bytes() {
        let frame = encode(CMD_PSH, 1, b"data");
        for split in 0..frame.len() {
            let mut buf = BytesMut::from(&frame[..split]);
            assert!(
                matches!(decode_frame(&mut buf), Decoded::Incomplete),
                "{split} bytes should be incomplete"
            );
            assert_eq!(buf.len(), split, "an incomplete decode must not consume");
        }
    }

    /// Frames can be split across any number of session messages, so the parser
    /// must reassemble byte-by-byte without losing or duplicating anything.
    #[test]
    fn frames_reassemble_across_arbitrary_chunk_boundaries() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&encode(CMD_PSH, 1, b"first"));
        wire.extend_from_slice(&control(CMD_FIN, 1));
        wire.extend_from_slice(&encode(CMD_PSH, 3, b"second"));

        let mut buf = BytesMut::new();
        let mut decoded = Vec::new();
        for byte in wire.iter() {
            buf.extend_from_slice(&[*byte]);
            while let Decoded::Frame(frame) = decode_frame(&mut buf) {
                decoded.push((frame.cmd, frame.stream_id, frame.payload));
            }
        }

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].0, CMD_PSH);
        assert_eq!(&decoded[0].2[..], b"first");
        assert_eq!(decoded[1].0, CMD_FIN);
        assert_eq!(decoded[2].1, 3);
        assert_eq!(&decoded[2].2[..], b"second");
    }

    /// There is no resynchronisation marker in smux, so a bad version byte must
    /// fail the whole multiplexer rather than be skipped: guessing where the
    /// next frame starts would deliver corrupt bytes to a real TCP connection.
    #[test]
    fn a_bad_version_byte_is_fatal() {
        let mut frame = BytesMut::from(&encode(CMD_PSH, 3, b"payload")[..]);
        frame[0] = 2;
        assert!(matches!(decode_frame(&mut frame), Decoded::Corrupt(_)));
    }

    #[test]
    fn an_oversized_length_field_is_fatal() {
        let mut buf = BytesMut::new();
        buf.put_u8(VERSION);
        buf.put_u8(CMD_PSH);
        buf.put_u16_le(u16::MAX); // 65535 > MAX_FRAME_PAYLOAD
        buf.put_u32_le(1);
        assert!(matches!(decode_frame(&mut buf), Decoded::Corrupt(_)));
    }

    #[test]
    fn a_maximum_size_frame_is_accepted() {
        let payload = vec![0xAB; MAX_FRAME_PAYLOAD];
        let mut buf = BytesMut::from(&encode(CMD_PSH, 9, &payload)[..]);
        match decode_frame(&mut buf) {
            Decoded::Frame(frame) => assert_eq!(frame.payload.len(), MAX_FRAME_PAYLOAD),
            _ => panic!("a 32 KiB frame is legal"),
        }
    }
}
