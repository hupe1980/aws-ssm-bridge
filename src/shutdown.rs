//! Graceful shutdown utilities.
//!
//! Provides a cancellation token pattern for coordinating graceful shutdown
//! across async tasks. This is critical for production deployments where
//! you need to ensure all resources are properly cleaned up.
//!
//! # Example
//!
//! ```rust
//! use aws_ssm_bridge::shutdown::ShutdownSignal;
//! use std::time::Duration;
//!
//! # async fn example() {
//! // Create a shutdown signal
//! let signal = ShutdownSignal::new();
//!
//! // Clone for each task that needs to respond to shutdown
//! let worker_signal = signal.clone();
//!
//! tokio::spawn(async move {
//!     loop {
//!         tokio::select! {
//!             _ = worker_signal.cancelled() => {
//!                 println!("Worker shutting down");
//!                 break;
//!             }
//!             _ = tokio::time::sleep(Duration::from_secs(1)) => {
//!                 println!("Working...");
//!             }
//!         }
//!     }
//! });
//!
//! // Trigger shutdown after some time
//! tokio::time::sleep(Duration::from_secs(5)).await;
//! signal.shutdown();
//! # }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info};

/// A clonable signal for coordinating graceful shutdown.
///
/// The signal can be cloned and shared across multiple tasks. When `shutdown()`
/// is called, all tasks waiting on `cancelled()` will be notified.
#[derive(Clone)]
pub struct ShutdownSignal {
    inner: Arc<ShutdownInner>,
}

struct ShutdownInner {
    /// Whether shutdown has been triggered
    triggered: AtomicBool,
    /// Notify for waking waiters
    notify: Notify,
}

impl ShutdownSignal {
    /// Create a new shutdown signal.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownInner {
                triggered: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// Trigger shutdown.
    ///
    /// This will notify all tasks waiting on `cancelled()`.
    /// Calling this multiple times is safe and has no additional effect.
    pub fn shutdown(&self) {
        if !self.inner.triggered.swap(true, Ordering::SeqCst) {
            info!("Shutdown signal triggered");
            self.inner.notify.notify_waiters();
        }
    }

    /// Check if shutdown has been triggered.
    pub fn is_shutdown(&self) -> bool {
        self.inner.triggered.load(Ordering::SeqCst)
    }

    /// Wait for shutdown to be triggered.
    ///
    /// This returns a future that completes when `shutdown()` is called.
    /// If shutdown was already triggered, this returns immediately.
    pub async fn cancelled(&self) {
        // Fast path: already triggered
        if self.is_shutdown() {
            return;
        }

        // Wait for notification
        self.inner.notify.notified().await;
    }

    /// Wait for shutdown or timeout.
    ///
    /// Returns `true` if shutdown was triggered, `false` if timeout elapsed.
    pub async fn wait_timeout(&self, timeout: Duration) -> bool {
        tokio::select! {
            _ = self.cancelled() => true,
            _ = tokio::time::sleep(timeout) => false,
        }
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownSignal")
            .field("triggered", &self.is_shutdown())
            .finish()
    }
}

/// Guard that triggers shutdown on drop.
///
/// Useful for ensuring shutdown is triggered even if code panics or
/// returns early.
pub struct ShutdownGuard {
    signal: ShutdownSignal,
    armed: bool,
}

impl ShutdownGuard {
    /// Create a new shutdown guard.
    pub fn new(signal: ShutdownSignal) -> Self {
        Self {
            signal,
            armed: true,
        }
    }

    /// Disarm the guard without triggering shutdown.
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if self.armed {
            debug!("ShutdownGuard dropped, triggering shutdown");
            self.signal.shutdown();
        }
    }
}

/// Install OS signal handlers for graceful shutdown.
///
/// This sets up handlers for SIGINT (Ctrl+C) and SIGTERM that will
/// trigger the provided shutdown signal.
///
/// # Platform Support
///
/// - **Unix**: Handles SIGINT, SIGTERM, SIGQUIT
/// - **Windows**: Handles Ctrl+C, Ctrl+Break
///
/// # Example
///
/// ```rust,no_run
/// use aws_ssm_bridge::shutdown::{ShutdownSignal, install_signal_handlers};
///
/// # async fn example() {
/// let signal = ShutdownSignal::new();
/// install_signal_handlers(signal.clone());
///
/// // Your application code here
/// signal.cancelled().await;
/// println!("Shutting down gracefully");
/// # }
/// ```
pub fn install_signal_handlers(signal: ShutdownSignal) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal as unix_signal, SignalKind};

        let signal_clone = signal.clone();
        tokio::spawn(async move {
            let mut sigint =
                unix_signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
            let mut sigterm =
                unix_signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
            let mut sigquit =
                unix_signal(SignalKind::quit()).expect("Failed to install SIGQUIT handler");

            tokio::select! {
                _ = sigint.recv() => info!("Received SIGINT"),
                _ = sigterm.recv() => info!("Received SIGTERM"),
                _ = sigquit.recv() => info!("Received SIGQUIT"),
            }

            signal_clone.shutdown();
        });
    }

    #[cfg(windows)]
    {
        let signal_clone = signal.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
            info!("Received Ctrl+C");
            signal_clone.shutdown();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal_basic() {
        let signal = ShutdownSignal::new();

        assert!(!signal.is_shutdown());
        signal.shutdown();
        assert!(signal.is_shutdown());
    }

    #[tokio::test]
    async fn test_shutdown_signal_cancelled() {
        let signal = ShutdownSignal::new();

        let signal_clone = signal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            signal_clone.shutdown();
        });

        signal.cancelled().await;
        assert!(signal.is_shutdown());
    }

    #[tokio::test]
    async fn test_shutdown_already_triggered() {
        let signal = ShutdownSignal::new();
        signal.shutdown();

        // Should return immediately
        tokio::time::timeout(Duration::from_millis(10), signal.cancelled())
            .await
            .expect("Should complete immediately");
    }

    #[tokio::test]
    async fn test_shutdown_wait_timeout() {
        let signal = ShutdownSignal::new();

        // Timeout should elapse
        let result = signal.wait_timeout(Duration::from_millis(10)).await;
        assert!(!result);

        // Now trigger shutdown
        signal.shutdown();
        let result = signal.wait_timeout(Duration::from_millis(10)).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_shutdown_guard() {
        let signal = ShutdownSignal::new();

        {
            let _guard = ShutdownGuard::new(signal.clone());
            // Guard will be dropped here
        }

        assert!(signal.is_shutdown());
    }

    #[tokio::test]
    async fn test_shutdown_guard_disarm() {
        let signal = ShutdownSignal::new();

        {
            let mut guard = ShutdownGuard::new(signal.clone());
            guard.disarm();
            // Guard is disarmed, won't trigger shutdown on drop
        }

        assert!(!signal.is_shutdown());
    }

    #[tokio::test]
    async fn test_multiple_clones() {
        let signal = ShutdownSignal::new();
        let clone1 = signal.clone();
        let clone2 = signal.clone();

        // Trigger from one clone
        clone1.shutdown();

        // All clones should see shutdown
        assert!(signal.is_shutdown());
        assert!(clone1.is_shutdown());
        assert!(clone2.is_shutdown());
    }
}
