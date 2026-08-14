//! Cooperative cancellation for long-running session work.
//!
//! [`PortForwarder::forward`] and friends run until told to stop. A
//! [`ShutdownSignal`] is the "stop now" broadcast: clone it into every task,
//! select on [`cancelled`](ShutdownSignal::cancelled), and trigger it from a
//! signal handler or your own supervisor.
//!
//! ```no_run
//! use aws_ssm_bridge::shutdown::{install_signal_handlers, ShutdownSignal};
//!
//! # async fn example() {
//! let shutdown = ShutdownSignal::new();
//! install_signal_handlers(shutdown.clone());
//!
//! tokio::select! {
//!     () = shutdown.cancelled() => println!("stopping"),
//!     _ = tokio::time::sleep(std::time::Duration::from_secs(3600)) => {}
//! }
//! # }
//! ```
//!
//! [`PortForwarder::forward`]: crate::PortForwarder::forward

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::info;

/// A latching, clonable "stop" broadcast.
///
/// Latching matters: a task that starts *after* shutdown was triggered still
/// sees it immediately, so there is no race between spawning and cancelling.
#[derive(Clone, Default)]
pub struct ShutdownSignal {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    triggered: AtomicBool,
    notify: Notify,
}

impl ShutdownSignal {
    /// Create a signal that has not been triggered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Trigger shutdown, waking every waiter. Idempotent.
    pub fn shutdown(&self) {
        if !self.inner.triggered.swap(true, Ordering::AcqRel) {
            info!("shutdown requested");
            self.inner.notify.notify_waiters();
        }
    }

    /// Whether shutdown has been triggered.
    pub fn is_shutdown(&self) -> bool {
        self.inner.triggered.load(Ordering::Acquire)
    }

    /// Resolve once shutdown is triggered, immediately if it already was.
    ///
    /// Cancel-safe.
    pub async fn cancelled(&self) {
        // Register before checking the flag. `notify_waiters` only wakes futures
        // that are already *enqueued*, and constructing a `Notified` does not
        // enqueue it — the first poll does, or `enable()` up front. Without the
        // `enable()` a shutdown landing in the gap between the check and the
        // await would be dropped and this task would wait forever.
        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_shutdown() {
            return;
        }
        notified.await;
    }

    /// Wait for shutdown, giving up after `timeout`.
    ///
    /// Returns `true` if shutdown was triggered, `false` on timeout.
    pub async fn wait_timeout(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, self.cancelled())
            .await
            .is_ok()
    }
}

impl std::fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownSignal")
            .field("triggered", &self.is_shutdown())
            .finish()
    }
}

/// Trigger `signal` on the first termination signal from the OS.
///
/// Handles `SIGINT`, `SIGTERM` and `SIGQUIT` on Unix, and Ctrl-C on Windows.
/// Spawns one background task and returns immediately.
///
/// If a handler cannot be installed — typically because the process is not the
/// signal group leader, or another library already claimed it — the failure is
/// logged and the remaining handlers are still installed. Silently degrading is
/// better than aborting the process at startup, but check the logs if Ctrl-C
/// appears not to work.
pub fn install_signal_handlers(signal: ShutdownSignal) {
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal as unix_signal, SignalKind};

        let mut streams: Vec<_> = [
            ("SIGINT", SignalKind::interrupt()),
            ("SIGTERM", SignalKind::terminate()),
            ("SIGQUIT", SignalKind::quit()),
        ]
        .into_iter()
        .filter_map(|(name, kind)| match unix_signal(kind) {
            Ok(stream) => Some((name, stream)),
            Err(e) => {
                tracing::warn!(signal = name, error = %e, "could not install a signal handler");
                None
            }
        })
        .collect();

        if streams.is_empty() {
            return;
        }

        let received =
            futures_util::future::select_all(streams.iter_mut().map(|(name, stream)| {
                Box::pin(async move { stream.recv().await.map(|()| *name) })
            }))
            .await
            .0;

        if let Some(name) = received {
            info!(signal = name, "received termination signal");
        }
        signal.shutdown();
    });

    #[cfg(windows)]
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => info!("received Ctrl-C"),
            Err(e) => {
                tracing::warn!(error = %e, "could not install the Ctrl-C handler");
                return;
            }
        }
        signal.shutdown();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_is_observable_and_idempotent() {
        let signal = ShutdownSignal::new();
        assert!(!signal.is_shutdown());
        signal.shutdown();
        signal.shutdown();
        assert!(signal.is_shutdown());
    }

    #[tokio::test]
    async fn a_waiter_registered_first_is_woken() {
        let signal = ShutdownSignal::new();
        let waiter = tokio::spawn({
            let signal = signal.clone();
            async move { signal.cancelled().await }
        });
        tokio::task::yield_now().await;
        signal.shutdown();

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must be woken")
            .unwrap();
    }

    /// The signal latches, so a task that starts after the trigger still stops.
    #[tokio::test]
    async fn cancellation_latches_for_late_waiters() {
        let signal = ShutdownSignal::new();
        signal.shutdown();
        tokio::time::timeout(Duration::from_millis(50), signal.cancelled())
            .await
            .expect("a late waiter must return immediately");
    }

    #[tokio::test]
    async fn clones_share_one_state() {
        let signal = ShutdownSignal::new();
        let a = signal.clone();
        let b = signal.clone();
        a.shutdown();
        assert!(b.is_shutdown() && signal.is_shutdown());
    }

    #[tokio::test]
    async fn wait_timeout_reports_which_happened() {
        let signal = ShutdownSignal::new();
        assert!(!signal.wait_timeout(Duration::from_millis(20)).await);
        signal.shutdown();
        assert!(signal.wait_timeout(Duration::from_millis(20)).await);
    }
}
