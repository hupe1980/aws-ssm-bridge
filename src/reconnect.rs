//! Session reconnection utilities for handling network interruptions.
//!
//! Provides automatic reconnection with exponential backoff and configurable
//! retry strategies for production resilience.
//!
//! # Example
//!
//! ```rust,no_run
//! use aws_ssm_bridge::reconnect::{ReconnectingSession, ReconnectConfig};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create reconnecting wrapper with retry config
//! let session = ReconnectingSession::new(
//!     "i-instance123",
//!     ReconnectConfig {
//!         max_retries: 5,
//!         initial_delay: Duration::from_secs(1),
//!         max_delay: Duration::from_secs(30),
//!         ..Default::default()
//!     }
//! ).await?;
//!
//! // Use like a normal session - auto-reconnects on failure
//! session.send(bytes::Bytes::from("ls\n")).await?;
//!
//! // Monitor reconnection events
//! let mut events = session.events();
//! tokio::spawn(async move {
//!     while let Ok(event) = events.recv().await {
//!         println!("Event: {:?}", event);
//!     }
//! });
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::errors::{Error, Result, TransportError};
use crate::session::{Session, SessionConfig, SessionManager, SessionState};
use crate::shutdown::ShutdownSignal;

/// Configuration for session reconnection behavior.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts (0 = unlimited)
    pub max_retries: u32,
    /// Initial delay between retry attempts
    pub initial_delay: Duration,
    /// Maximum delay between retry attempts
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Whether to add jitter to delays (recommended for thundering herd prevention)
    pub jitter: bool,
    /// Session configuration template
    pub session_config: SessionConfig,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            jitter: true,
            session_config: SessionConfig::default(),
        }
    }
}

/// Events emitted during reconnection.
#[derive(Debug, Clone)]
pub enum ReconnectEvent {
    /// Connection was lost
    Disconnected {
        /// Error that caused disconnection
        error: String,
    },
    /// Attempting to reconnect
    Reconnecting {
        /// Current attempt number (1-indexed)
        attempt: u32,
        /// Delay before this attempt
        delay: Duration,
    },
    /// Successfully reconnected
    Reconnected {
        /// New session ID
        session_id: String,
        /// Total attempts taken
        attempts: u32,
    },
    /// All reconnection attempts exhausted
    Failed {
        /// Total attempts made
        total_attempts: u32,
        /// Last error
        last_error: String,
    },
}

/// Statistics about reconnection attempts.
#[derive(Debug, Clone, Default)]
pub struct ReconnectStats {
    /// Total reconnection attempts
    pub total_attempts: u64,
    /// Successful reconnections
    pub successful_reconnects: u64,
    /// Failed reconnection cycles (all retries exhausted)
    pub failed_reconnects: u64,
    /// Current consecutive failures
    pub consecutive_failures: u32,
}

/// A session wrapper that automatically reconnects on failure.
///
/// This wraps a regular Session and monitors its health. When the session
/// becomes disconnected, it automatically attempts to reconnect with
/// exponential backoff.
pub struct ReconnectingSession {
    /// Target instance/document
    target: String,
    /// Reconnection configuration
    config: ReconnectConfig,
    /// Current session (wrapped in mutex for replacement)
    session: Arc<RwLock<Option<Session>>>,
    /// Session manager for creating new sessions
    manager: Arc<SessionManager>,
    /// Event broadcast channel
    events: broadcast::Sender<ReconnectEvent>,
    /// Statistics
    stats: Arc<ReconnectStatsInner>,
    /// Shutdown signal
    shutdown: ShutdownSignal,
    /// Flag indicating if currently reconnecting
    reconnecting: Arc<Mutex<bool>>,
}

struct ReconnectStatsInner {
    total_attempts: AtomicU64,
    successful_reconnects: AtomicU64,
    failed_reconnects: AtomicU64,
    consecutive_failures: AtomicU64,
}

impl Default for ReconnectStatsInner {
    fn default() -> Self {
        Self {
            total_attempts: AtomicU64::new(0),
            successful_reconnects: AtomicU64::new(0),
            failed_reconnects: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
        }
    }
}

impl ReconnectingSession {
    /// Create a new reconnecting session for the given target.
    pub async fn new(target: &str, config: ReconnectConfig) -> Result<Self> {
        let manager = Arc::new(SessionManager::new().await?);
        Self::with_manager(target, config, manager).await
    }

    /// Create a reconnecting session with a custom session manager.
    pub async fn with_manager(
        target: &str,
        config: ReconnectConfig,
        manager: Arc<SessionManager>,
    ) -> Result<Self> {
        let (events_tx, _) = broadcast::channel(100);

        let instance = Self {
            target: target.to_string(),
            config,
            session: Arc::new(RwLock::new(None)),
            manager,
            events: events_tx,
            stats: Arc::new(ReconnectStatsInner::default()),
            shutdown: ShutdownSignal::new(),
            reconnecting: Arc::new(Mutex::new(false)),
        };

        // Initial connection
        instance.connect().await?;

        Ok(instance)
    }

    /// Connect (or reconnect) to the target.
    async fn connect(&self) -> Result<()> {
        let mut session_config = self.config.session_config.clone();
        session_config.target = self.target.clone();

        let session = self.manager.start_session(session_config).await?;

        {
            let mut guard = self.session.write().await;
            *guard = Some(session);
        }

        info!(target = %self.target, "Connected to target");
        Ok(())
    }

    /// Attempt reconnection with exponential backoff.
    async fn reconnect(&self, initial_error: &str) -> Result<()> {
        // Prevent concurrent reconnection attempts
        {
            let mut reconnecting = self.reconnecting.lock().await;
            if *reconnecting {
                debug!("Already reconnecting, skipping");
                return Err(Error::InvalidState("Already reconnecting".to_string()));
            }
            *reconnecting = true;
        }

        // Emit disconnected event
        let _ = self.events.send(ReconnectEvent::Disconnected {
            error: initial_error.to_string(),
        });

        let mut delay = self.config.initial_delay;
        let mut attempts = 0u32;
        let mut last_error = initial_error.to_string();

        loop {
            // Check if max retries exceeded
            if self.config.max_retries > 0 && attempts >= self.config.max_retries {
                self.stats.failed_reconnects.fetch_add(1, Ordering::Relaxed);
                let _ = self.events.send(ReconnectEvent::Failed {
                    total_attempts: attempts,
                    last_error: last_error.clone(),
                });

                *self.reconnecting.lock().await = false;
                return Err(Error::Transport(TransportError::ConnectionFailed(format!(
                    "Max reconnection attempts ({}) exceeded: {}",
                    self.config.max_retries, last_error
                ))));
            }

            attempts += 1;
            self.stats.total_attempts.fetch_add(1, Ordering::Relaxed);

            // Apply jitter if enabled
            let actual_delay = if self.config.jitter {
                let jitter_factor = 0.5 + rand_jitter() * 0.5; // 50-100% of delay
                Duration::from_secs_f64(delay.as_secs_f64() * jitter_factor)
            } else {
                delay
            };

            let _ = self.events.send(ReconnectEvent::Reconnecting {
                attempt: attempts,
                delay: actual_delay,
            });

            info!(
                target = %self.target,
                attempt = attempts,
                delay_ms = actual_delay.as_millis(),
                "Attempting reconnection"
            );

            // Wait before retry
            tokio::select! {
                _ = sleep(actual_delay) => {}
                _ = self.shutdown.cancelled() => {
                    *self.reconnecting.lock().await = false;
                    return Err(Error::InvalidState("Shutdown requested".to_string()));
                }
            }

            // Attempt connection
            match self.connect().await {
                Ok(()) => {
                    self.stats
                        .successful_reconnects
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats.consecutive_failures.store(0, Ordering::Relaxed);

                    let session_id = {
                        let guard = self.session.read().await;
                        guard
                            .as_ref()
                            .map(|s| s.id().to_string())
                            .unwrap_or_default()
                    };

                    let _ = self.events.send(ReconnectEvent::Reconnected {
                        session_id,
                        attempts,
                    });

                    *self.reconnecting.lock().await = false;
                    return Ok(());
                }
                Err(e) => {
                    last_error = e.to_string();
                    self.stats
                        .consecutive_failures
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target = %self.target,
                        attempt = attempts,
                        error = %e,
                        "Reconnection attempt failed"
                    );
                }
            }

            // Increase delay with exponential backoff
            delay = Duration::from_secs_f64(
                (delay.as_secs_f64() * self.config.backoff_multiplier)
                    .min(self.config.max_delay.as_secs_f64()),
            );
        }
    }

    /// Send data to the session, reconnecting if necessary.
    pub async fn send(&self, data: bytes::Bytes) -> Result<()> {
        loop {
            let result = {
                let guard = self.session.read().await;
                match guard.as_ref() {
                    Some(session) => session.send(data.clone()).await,
                    None => Err(Error::InvalidState("No active session".to_string())),
                }
            };

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Check if this is a recoverable error
                    if is_connection_error(&e) {
                        warn!(error = %e, "Connection error, attempting reconnection");
                        self.reconnect(&e.to_string()).await?;
                        // Retry send after reconnection
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Get the output stream.
    pub async fn output(&self) -> Option<crate::channels::OutputStream> {
        let guard = self.session.read().await;
        guard.as_ref().map(|s| s.output())
    }

    /// Subscribe to reconnection events.
    pub fn events(&self) -> broadcast::Receiver<ReconnectEvent> {
        self.events.subscribe()
    }

    /// Get reconnection statistics.
    pub fn stats(&self) -> ReconnectStats {
        ReconnectStats {
            total_attempts: self.stats.total_attempts.load(Ordering::Relaxed),
            successful_reconnects: self.stats.successful_reconnects.load(Ordering::Relaxed),
            failed_reconnects: self.stats.failed_reconnects.load(Ordering::Relaxed),
            consecutive_failures: self.stats.consecutive_failures.load(Ordering::Relaxed) as u32,
        }
    }

    /// Get the current session state.
    pub async fn state(&self) -> SessionState {
        let guard = self.session.read().await;
        match guard.as_ref() {
            Some(session) => session.state().await,
            None => SessionState::Terminated,
        }
    }

    /// Check if the session is ready.
    pub async fn is_ready(&self) -> bool {
        let guard = self.session.read().await;
        guard.as_ref().map(|s| s.is_ready()).unwrap_or(false)
    }

    /// Check if currently in a reconnection cycle.
    pub async fn is_reconnecting(&self) -> bool {
        *self.reconnecting.lock().await
    }

    /// Get the target identifier.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Get the shutdown signal.
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.shutdown.clone()
    }

    /// Gracefully terminate the session.
    pub async fn terminate(&self) -> Result<()> {
        self.shutdown.shutdown();

        let mut guard = self.session.write().await;
        if let Some(mut session) = guard.take() {
            session.terminate().await?;
        }

        info!(target = %self.target, "Reconnecting session terminated");
        Ok(())
    }

    /// Force immediate reconnection (useful after network recovery).
    pub async fn force_reconnect(&self) -> Result<()> {
        info!(target = %self.target, "Forcing reconnection");

        // Terminate current session
        {
            let mut guard = self.session.write().await;
            if let Some(mut session) = guard.take() {
                let _ = session.terminate().await;
            }
        }

        // Connect fresh
        self.connect().await
    }
}

/// Check if an error indicates a connection problem that's worth retrying.
fn is_connection_error(error: &Error) -> bool {
    match error {
        Error::Transport(TransportError::WebSocket(_)) => true,
        Error::Transport(TransportError::ConnectionFailed(_)) => true,
        Error::Transport(TransportError::ConnectionClosed { .. }) => true,
        Error::Transport(TransportError::HeartbeatTimeout) => true,
        Error::Timeout => true,
        Error::Io(e) => {
            use std::io::ErrorKind;
            matches!(
                e.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::TimedOut
                    | ErrorKind::NotConnected
            )
        }
        _ => false,
    }
}

/// Generate a random jitter value between 0.0 and 1.0.
fn rand_jitter() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos as f64) / (u32::MAX as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconnect_config_default() {
        let config = ReconnectConfig::default();
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert!(config.jitter);
    }

    #[test]
    fn test_reconnect_stats_default() {
        let stats = ReconnectStats::default();
        assert_eq!(stats.total_attempts, 0);
        assert_eq!(stats.successful_reconnects, 0);
        assert_eq!(stats.failed_reconnects, 0);
        assert_eq!(stats.consecutive_failures, 0);
    }

    #[test]
    fn test_is_connection_error() {
        // WebSocket errors are connection errors
        assert!(is_connection_error(&Error::Transport(
            TransportError::WebSocket("connection closed".to_string())
        )));

        // Connection failed is retriable
        assert!(is_connection_error(&Error::Transport(
            TransportError::ConnectionFailed("network unreachable".to_string())
        )));

        // Timeout is retriable
        assert!(is_connection_error(&Error::Timeout));

        // Config errors are not retriable
        assert!(!is_connection_error(&Error::Config(
            "bad config".to_string()
        )));

        // Protocol errors are not retriable
        use crate::errors::ProtocolError;
        assert!(!is_connection_error(&Error::Protocol(
            ProtocolError::InvalidMessage("invalid message".to_string())
        )));
    }

    #[test]
    fn test_rand_jitter() {
        // Just verify it produces values
        let j1 = rand_jitter();
        assert!((0.0..=1.0).contains(&j1));
    }

    #[test]
    fn test_reconnect_event_debug() {
        let event = ReconnectEvent::Disconnected {
            error: "test".to_string(),
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Disconnected"));

        let event = ReconnectEvent::Reconnecting {
            attempt: 1,
            delay: Duration::from_secs(1),
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Reconnecting"));

        let event = ReconnectEvent::Reconnected {
            session_id: "abc".to_string(),
            attempts: 2,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Reconnected"));

        let event = ReconnectEvent::Failed {
            total_attempts: 5,
            last_error: "failed".to_string(),
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Failed"));
    }
}
