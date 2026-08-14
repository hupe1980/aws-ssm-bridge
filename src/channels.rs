//! Fan-out of agent output to session consumers.
//!
//! A session may have several readers — an interactive shell painting the
//! terminal, a log tap, the smux demultiplexer behind a port forward.  They all
//! see the same byte stream.
//!
//! # Why every subscriber is lossless
//!
//! An earlier design fanned out over a `broadcast` channel, which silently drops
//! the oldest messages when a receiver falls behind.  For terminal output that
//! is merely ugly; for the smux framing layer it is fatal, because a dropped
//! chunk desynchronises the frame parser and every subsequent byte is garbage
//! attributed to the wrong stream.
//!
//! Each subscriber therefore gets its own bounded queue.  When a queue fills,
//! that one subscriber is evicted and its [`OutputStream`] reports
//! [`lagged`](OutputStream::lagged) — an explicit, observable failure instead of
//! silent corruption.  Other subscribers and the session itself are unaffected.

use bytes::Bytes;
use futures_util::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Default per-subscriber queue depth, in messages.
///
/// At the 1 KiB default chunk size this is ~8 MiB of headroom, which is more
/// than any consumer that is actually making progress will ever need.
pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 8192;

/// A borrowed view of the session's output stream.
///
/// Implements [`Stream<Item = Bytes>`](futures_util::Stream); the chunk
/// boundaries are those of the underlying protocol messages and carry no
/// meaning, so consumers should treat this as a byte stream.
///
/// The stream ends when the session closes, or when this subscriber is evicted
/// for falling too far behind — [`lagged`](Self::lagged) distinguishes the two.
pub struct OutputStream {
    rx: mpsc::Receiver<Bytes>,
    lagged: Arc<AtomicBool>,
}

impl OutputStream {
    /// Receive the next chunk, or `None` once the stream has ended.
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.rx.recv().await
    }

    /// Whether this subscriber was evicted for falling behind.
    ///
    /// Only meaningful once the stream has ended.  `true` means bytes were lost
    /// and any framing built on top of this stream must be considered corrupt;
    /// `false` means the session closed normally.
    pub fn lagged(&self) -> bool {
        self.lagged.load(Ordering::Acquire)
    }

    /// An already-ended stream, for subscribing to a closed session.
    fn ended() -> Self {
        let (_tx, rx) = mpsc::channel(1);
        Self {
            rx,
            lagged: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Stream for OutputStream {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Bytes>> {
        self.rx.poll_recv(cx)
    }
}

impl std::fmt::Debug for OutputStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputStream")
            .field("lagged", &self.lagged())
            .finish()
    }
}

#[derive(Debug)]
struct Subscriber {
    tx: mpsc::Sender<Bytes>,
    lagged: Arc<AtomicBool>,
}

/// Distributes agent output to every [`OutputStream`] subscriber.
#[derive(Debug, Default)]
pub(crate) struct OutputFanout {
    subscribers: Mutex<Vec<Subscriber>>,
    closed: AtomicBool,
}

impl OutputFanout {
    /// Create an empty fan-out.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Add a subscriber with the given queue depth.
    ///
    /// Subscribing to an already-closed fan-out yields an immediately-ended
    /// stream rather than one that blocks forever.
    pub(crate) fn subscribe(&self, capacity: usize) -> OutputStream {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let lagged = Arc::new(AtomicBool::new(false));

        let mut subscribers = self.lock();
        // Check `closed` while holding the lock: `close()` sets the flag and
        // then takes the same lock, so this ordering cannot miss a concurrent
        // close and leave a subscriber attached to a dead session.
        if self.closed.load(Ordering::Acquire) {
            return OutputStream::ended();
        }
        subscribers.push(Subscriber {
            tx,
            lagged: Arc::clone(&lagged),
        });

        OutputStream { rx, lagged }
    }

    /// Deliver a chunk to every subscriber, evicting any that cannot keep up.
    pub(crate) fn send(&self, data: Bytes) {
        let mut subscribers = self.lock();
        subscribers.retain(|sub| match sub.tx.try_send(data.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    bytes = data.len(),
                    "output subscriber is not keeping up; evicting it to protect the session"
                );
                // Publish the flag before dropping the sender so a consumer that
                // wakes on the channel close is guaranteed to observe it.
                sub.lagged.store(true, Ordering::Release);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
    }

    /// End every subscriber's stream and reject future subscriptions.
    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        debug!("closing session output fan-out");
        self.lock().clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Subscriber>> {
        self.subscribers.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn every_subscriber_sees_every_chunk() {
        let fanout = OutputFanout::new();
        let mut a = fanout.subscribe(8);
        let mut b = fanout.subscribe(8);

        fanout.send(Bytes::from_static(b"one"));
        fanout.send(Bytes::from_static(b"two"));

        assert_eq!(a.next().await.unwrap(), Bytes::from_static(b"one"));
        assert_eq!(a.next().await.unwrap(), Bytes::from_static(b"two"));
        assert_eq!(b.next().await.unwrap(), Bytes::from_static(b"one"));
        assert_eq!(b.next().await.unwrap(), Bytes::from_static(b"two"));
    }

    #[tokio::test]
    async fn close_ends_open_streams() {
        let fanout = OutputFanout::new();
        let mut stream = fanout.subscribe(8);
        fanout.send(Bytes::from_static(b"before"));
        fanout.close();

        // Buffered data is still delivered, then the stream ends.
        assert_eq!(stream.next().await.unwrap(), Bytes::from_static(b"before"));
        assert!(stream.next().await.is_none());
        assert!(!stream.lagged(), "a clean close is not a lag");
    }

    #[tokio::test]
    async fn subscribing_after_close_yields_an_ended_stream() {
        let fanout = OutputFanout::new();
        fanout.close();
        let mut stream = fanout.subscribe(8);
        assert!(stream.next().await.is_none());
    }

    /// A slow consumer must be evicted rather than back-pressuring the session,
    /// and must be able to tell that it lost bytes.
    #[tokio::test]
    async fn slow_subscriber_is_evicted_and_reports_lagging() {
        let fanout = OutputFanout::new();
        let mut slow = fanout.subscribe(2);
        let mut healthy = fanout.subscribe(64);

        for i in 0..10u8 {
            fanout.send(Bytes::from(vec![i]));
        }

        // The slow subscriber gets what fitted, then ends with the lag flag set.
        let mut received = 0;
        while slow.next().await.is_some() {
            received += 1;
        }
        assert!(
            received <= 2,
            "received {received} chunks from a depth-2 queue"
        );
        assert!(slow.lagged(), "eviction must be observable");

        // The healthy subscriber is untouched.
        for i in 0..10u8 {
            assert_eq!(healthy.next().await.unwrap(), Bytes::from(vec![i]));
        }
    }

    #[tokio::test]
    async fn dropped_subscribers_are_reaped() {
        let fanout = OutputFanout::new();
        let keep = fanout.subscribe(8);
        drop(fanout.subscribe(8));

        fanout.send(Bytes::from_static(b"x"));
        assert_eq!(
            fanout.lock().len(),
            1,
            "the dropped subscriber must be reaped"
        );
        drop(keep);
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let fanout = OutputFanout::new();
        fanout.close();
        fanout.close();
        let mut stream = fanout.subscribe(4);
        assert!(stream.next().await.is_none());
    }

    /// Polling an idle stream must park the task rather than spin.
    #[tokio::test]
    async fn idle_stream_parks_instead_of_spinning() {
        let fanout = OutputFanout::new();
        let mut stream = fanout.subscribe(8);
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next()).await;
        assert!(result.is_err(), "an idle stream must not resolve");
    }
}
