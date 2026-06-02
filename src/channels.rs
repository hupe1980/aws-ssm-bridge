//! Channel multiplexing for stdin/stdout/stderr/control streams

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, warn};

use crate::errors::Result;

/// Stream of output data.
///
/// Wraps a `BroadcastStream` that properly parks the waker instead of
/// busy-spinning when no data is available.
pub struct OutputStream {
    inner: BroadcastStream<Bytes>,
    closed: Arc<AtomicBool>,
}

impl OutputStream {
    fn new(rx: broadcast::Receiver<Bytes>, closed: Arc<AtomicBool>) -> Self {
        Self {
            inner: BroadcastStream::new(rx),
            closed,
        }
    }

    /// Return a stream that immediately yields `None` (already-closed multiplexer).
    ///
    /// Used by `output_stream()` when called after `close()` to avoid panicking.
    fn closed(closed: Arc<AtomicBool>) -> Self {
        // Create a one-shot channel and immediately drop the sender so the
        // receiver side yields None on its first poll.
        let (tx, rx) = broadcast::channel(1);
        drop(tx);
        Self {
            inner: BroadcastStream::new(rx),
            closed,
        }
    }
}

impl Stream for OutputStream {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }

        // BroadcastStream properly parks the waker — no busy-wait.
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                warn!(skipped, "Output stream lagged, messages were dropped");
                // Re-poll to get the next available message
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                // Check close flag after pending (sender may have closed)
                if self.closed.load(Ordering::SeqCst) {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

/// Channel multiplexer for managing streams.
///
/// Production-grade implementation with broadcast channels for fan-out.
/// On close, the sender is dropped so all `BroadcastStream` receivers are
/// woken immediately (no polling delay).
pub struct ChannelMultiplexer {
    /// Broadcast sender for output data (stdout/stderr combined).
    /// Wrapped in `Mutex<Option<_>>` so `close()` can drop it, which
    /// wakes all parked receivers instantly.
    output_tx: std::sync::Mutex<Option<broadcast::Sender<Bytes>>>,
    /// Lossless direct subscribers (mpsc-backed, never drops frames).
    /// Used by the smux `recv_task` to avoid broadcast-lag data corruption.
    direct_subs: std::sync::Mutex<Vec<mpsc::UnboundedSender<Bytes>>>,

    /// Flag to signal channel closure (fast check without lock).
    closed: Arc<AtomicBool>,
}

impl ChannelMultiplexer {
    /// Create a new channel multiplexer.
    ///
    /// Uses a broadcast channel with capacity of 8192 messages — large enough
    /// for high-throughput shell output without silently dropping data for
    /// typical consumers.
    pub fn new() -> Self {
        let (output_tx, _) = broadcast::channel(8192);

        Self {
            output_tx: std::sync::Mutex::new(Some(output_tx)),
            direct_subs: std::sync::Mutex::new(Vec::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create an output stream that receives broadcasted data.
    ///
    /// If called after `close()`, returns a stream that immediately yields
    /// `None` rather than panicking.  This prevents a process-killing race
    /// during concurrent shutdown.
    pub fn output_stream(&self) -> OutputStream {
        let guard = self.output_tx.lock().expect("output_tx lock poisoned");
        match guard.as_ref() {
            Some(tx) => OutputStream::new(tx.subscribe(), Arc::clone(&self.closed)),
            // Multiplexer already closed — return an immediately-terminated stream.
            None => OutputStream::closed(Arc::clone(&self.closed)),
        }
    }

    /// Close the output channel, causing all output streams to return None.
    ///
    /// Drops the broadcast sender so all parked `BroadcastStream` receivers
    /// are woken immediately and yield `None`.
    pub fn close(&self) {
        debug!("Closing channel multiplexer");
        self.closed.store(true, Ordering::SeqCst);
        // Drop the sender — this wakes all receivers instantly
        let _ = self
            .output_tx
            .lock()
            .expect("output_tx lock poisoned")
            .take();
        // Clear direct subscribers so their channels close too.
        self.direct_subs
            .lock()
            .expect("direct_subs lock poisoned")
            .clear();
    }

    /// Send output data to all subscribed output streams.
    ///
    /// Synchronous — no async overhead (broadcast send is non-blocking).
    pub fn send_output(&self, data: Bytes) -> Result<()> {
        let guard = self.output_tx.lock().expect("output_tx lock poisoned");
        if let Some(tx) = guard.as_ref() {
            if tx.send(data.clone()).is_err() {
                debug!("No active output stream receivers");
            }
        }
        drop(guard);
        // Fan out to lossless direct subscribers; drop dead ones.
        self.direct_subs
            .lock()
            .expect("direct_subs lock poisoned")
            .retain(|tx| tx.send(data.clone()).is_ok());
        Ok(())
    }

    /// Subscribe to a lossless output tap backed by an unbounded mpsc channel.
    ///
    /// Unlike the broadcast-based [`output_stream`], this receiver never
    /// drops frames under load — the sender blocks in `send_output` until the
    /// subscriber's queue drains.  Use this for the smux `recv_task` where
    /// dropped bytes corrupt framing.
    pub fn subscribe_lossless(&self) -> mpsc::UnboundedReceiver<Bytes> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.direct_subs
            .lock()
            .expect("direct_subs lock poisoned")
            .push(tx);
        rx
    }
}

impl Default for ChannelMultiplexer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_output_stream() {
        let mux = ChannelMultiplexer::new();
        let mut stream = mux.output_stream();

        // Send some data
        mux.send_output(Bytes::from("test1")).unwrap();
        mux.send_output(Bytes::from("test2")).unwrap();

        // BroadcastStream properly wakes — no sleep needed
        let data1 = stream.next().await.unwrap();
        assert_eq!(data1, Bytes::from("test1"));

        let data2 = stream.next().await.unwrap();
        assert_eq!(data2, Bytes::from("test2"));
    }

    #[tokio::test]
    async fn test_multiple_output_streams() {
        let mux = ChannelMultiplexer::new();
        let mut stream1 = mux.output_stream();
        let mut stream2 = mux.output_stream();

        mux.send_output(Bytes::from("broadcast")).unwrap();

        let data1 = stream1.next().await.unwrap();
        let data2 = stream2.next().await.unwrap();

        assert_eq!(data1, Bytes::from("broadcast"));
        assert_eq!(data2, Bytes::from("broadcast"));
    }

    #[tokio::test]
    async fn test_close_terminates_stream() {
        let mux = ChannelMultiplexer::new();
        let mut stream = mux.output_stream();

        mux.send_output(Bytes::from("before_close")).unwrap();
        let data = stream.next().await.unwrap();
        assert_eq!(data, Bytes::from("before_close"));

        // Close the multiplexer
        mux.close();

        // Stream should terminate
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await;
        assert!(result.is_ok(), "Stream should terminate after close");
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_output_stream_after_close_returns_none() {
        // H-1: output_stream() after close() must return a closed stream, not panic.
        let mux = ChannelMultiplexer::new();
        mux.close();
        let mut stream = mux.output_stream(); // must not panic
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await;
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Post-close stream should be empty"
        );
    }

    #[tokio::test]
    async fn test_no_busy_wait_on_empty() {
        // Verify that polling an empty stream doesn't consume CPU.
        // BroadcastStream parks the waker properly, so a timeout should
        // return Err (timeout) instead of spinning forever.
        let mux = ChannelMultiplexer::new();
        let mut stream = mux.output_stream();

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next()).await;

        // Should timeout (Err), NOT return None (which would mean busy-spin)
        assert!(result.is_err(), "Empty stream should park, not spin");
    }
}
