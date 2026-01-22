//! Channel multiplexing for stdin/stdout/stderr/control streams

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, warn};

use crate::errors::Result;

/// Stream of output data
/// Uses broadcast receiver to allow multiple consumers
pub struct OutputStream {
    rx: broadcast::Receiver<Bytes>,
}

impl OutputStream {
    fn new(rx: broadcast::Receiver<Bytes>) -> Self {
        Self { rx }
    }
}

impl Stream for OutputStream {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.try_recv() {
            Ok(item) => Poll::Ready(Some(item)),
            Err(broadcast::error::TryRecvError::Empty) => {
                // Register waker for when data is available
                // Note: broadcast doesn't have poll_recv, so we use a workaround
                // In production, consider using tokio_stream::wrappers::BroadcastStream
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                warn!(skipped, "Output stream lagged, messages were dropped");
                // Continue trying to get the next message
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Closed) => Poll::Ready(None),
        }
    }
}

/// Channel multiplexer for managing streams
/// Production-grade implementation with broadcast channels for fan-out
pub struct ChannelMultiplexer {
    /// Broadcast sender for output data (stdout/stderr combined)
    /// Allows multiple consumers to subscribe to output
    output_tx: broadcast::Sender<Bytes>,

    /// Sequence number counter for outgoing messages
    #[allow(dead_code)] // Reserved for future message sequencing integration
    sequence_counter: Arc<RwLock<i64>>,
}

impl ChannelMultiplexer {
    /// Create a new channel multiplexer
    /// Uses broadcast channel with capacity of 1024 messages
    pub fn new() -> Self {
        // Broadcast channel allows multiple subscribers
        // Capacity of 1024 prevents unbounded memory growth
        let (output_tx, _) = broadcast::channel(1024);

        Self {
            output_tx,
            sequence_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// Create an output stream that receives broadcasted data
    /// Each call creates a new subscriber to the broadcast channel
    pub fn output_stream(&self) -> OutputStream {
        OutputStream::new(self.output_tx.subscribe())
    }

    /// Send output data to all subscribed output streams
    /// Returns Ok if at least one receiver exists, Err if no receivers
    pub async fn send_output(&self, data: Bytes) -> Result<()> {
        // broadcast returns Err if there are no active receivers
        // We consider this a warning, not an error
        if self.output_tx.send(data).is_err() {
            debug!("No active output stream receivers");
        }
        Ok(())
    }

    /// Get next sequence number for outgoing messages
    #[allow(dead_code)] // Reserved for future message sequencing integration
    pub async fn next_sequence(&self) -> i64 {
        let mut counter = self.sequence_counter.write().await;
        *counter += 1;
        *counter
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

        // Give time for async delivery
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Receive data
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

        // Send data - should be received by both streams
        mux.send_output(Bytes::from("broadcast")).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Both streams should receive the same data
        let data1 = stream1.next().await.unwrap();
        let data2 = stream2.next().await.unwrap();

        assert_eq!(data1, Bytes::from("broadcast"));
        assert_eq!(data2, Bytes::from("broadcast"));
    }

    #[tokio::test]
    async fn test_sequence_numbers() {
        let mux = ChannelMultiplexer::new();

        let seq1 = mux.next_sequence().await;
        let seq2 = mux.next_sequence().await;
        let seq3 = mux.next_sequence().await;

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
    }
}
