//! Channel multiplexing for stdin/stdout/stderr/control streams

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::broadcast;
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

    /// Flag to signal channel closure (fast check without lock).
    closed: Arc<AtomicBool>,
}

impl ChannelMultiplexer {
    /// Create a new channel multiplexer.
    /// Uses broadcast channel with capacity of 1024 messages.
    pub fn new() -> Self {
        let (output_tx, _) = broadcast::channel(1024);

        Self {
            output_tx: std::sync::Mutex::new(Some(output_tx)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create an output stream that receives broadcasted data.
    /// Each call creates a new subscriber to the broadcast channel.
    pub fn output_stream(&self) -> OutputStream {
        let guard = self.output_tx.lock().expect("output_tx lock poisoned");
        let rx = guard
            .as_ref()
            .expect("output_stream() called after close()")
            .subscribe();
        OutputStream::new(rx, Arc::clone(&self.closed))
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
    }

    /// Send output data to all subscribed output streams.
    pub async fn send_output(&self, data: Bytes) -> Result<()> {
        let guard = self.output_tx.lock().expect("output_tx lock poisoned");
        if let Some(tx) = guard.as_ref() {
            if tx.send(data).is_err() {
                debug!("No active output stream receivers");
            }
        }
        Ok(())
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
        mux.send_output(Bytes::from("test1")).await.unwrap();
        mux.send_output(Bytes::from("test2")).await.unwrap();

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

        mux.send_output(Bytes::from("broadcast")).await.unwrap();

        let data1 = stream1.next().await.unwrap();
        let data2 = stream2.next().await.unwrap();

        assert_eq!(data1, Bytes::from("broadcast"));
        assert_eq!(data2, Bytes::from("broadcast"));
    }

    #[tokio::test]
    async fn test_close_terminates_stream() {
        let mux = ChannelMultiplexer::new();
        let mut stream = mux.output_stream();

        mux.send_output(Bytes::from("before_close")).await.unwrap();
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
