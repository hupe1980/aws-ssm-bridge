//! A session that rebuilds itself when the connection drops.
//!
//! SSM sessions die for boring reasons: a laptop lid closes, a NAT table
//! expires, an agent restarts during patching. [`ReconnectingSession`] watches
//! for that, starts a fresh session, and keeps your output stream and your
//! handle valid across the gap.
//!
//! ```no_run
//! use aws_ssm_bridge::{ReconnectConfig, ReconnectingSession};
//! use futures_util::StreamExt;
//!
//! # async fn example() -> aws_ssm_bridge::Result<()> {
//! let session = ReconnectingSession::connect("i-0123456789abcdef0", ReconnectConfig::default())
//!     .await?;
//!
//! // This stream survives reconnects; the underlying session does not.
//! let mut output = session.output();
//! tokio::spawn(async move {
//!     while let Some(chunk) = output.next().await {
//!         print!("{}", String::from_utf8_lossy(&chunk));
//!     }
//! });
//!
//! session.send(&b"tail -f /var/log/syslog\r"[..]).await?;
//! # Ok(()) }
//! ```
//!
//! # What reconnection cannot restore
//!
//! A new session is a new process on the target. Shell state — the working
//! directory, environment, running foreground job, scrollback — is gone, and
//! output produced while disconnected was never sent. This type restores
//! *connectivity*, not continuity; treat every [`ReconnectEvent::Reconnected`]
//! as a fresh shell.
//!
//! Only failures that a retry could plausibly fix trigger a reconnect:
//! [`CloseReason::is_recoverable`] decides. A session the agent closed, or one
//! you terminated, stays closed.

use bytes::Bytes;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::channels::{OutputFanout, OutputStream};
use crate::errors::{Error, Result};
use crate::session::{CloseReason, Session, SessionConfig, SessionManager};
use crate::shutdown::ShutdownSignal;

/// How hard to try, and how long to wait between attempts.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Consecutive failed attempts before giving up; `0` means keep trying.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Ceiling on the backoff delay.
    pub max_delay: Duration,
    /// Template for each new session.
    pub session: SessionConfig,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            session: SessionConfig::default(),
        }
    }
}

/// Lifecycle notifications from a [`ReconnectingSession`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReconnectEvent {
    /// The underlying session ended.
    Disconnected {
        /// Why it ended.
        reason: CloseReason,
    },
    /// About to try again.
    Reconnecting {
        /// Attempt number, starting at 1.
        attempt: u32,
        /// How long the supervisor will wait first.
        delay: Duration,
    },
    /// A new session is open.
    Reconnected {
        /// ID of the new session — different from the old one.
        session_id: String,
        /// How many attempts it took.
        attempts: u32,
    },
    /// No further attempts will be made.
    GaveUp {
        /// Attempts made before stopping.
        attempts: u32,
        /// Why the supervisor stopped.
        reason: String,
    },
}

struct Inner {
    manager: SessionManager,
    config: ReconnectConfig,
    current: RwLock<Option<Arc<Session>>>,
    output: Arc<OutputFanout>,
    events: broadcast::Sender<ReconnectEvent>,
    shutdown: ShutdownSignal,
    generation: Mutex<u64>,
}

/// A session handle that outlives the sessions underneath it.
///
/// Cheap to clone; every clone drives the same supervisor.
#[derive(Clone)]
pub struct ReconnectingSession {
    inner: Arc<Inner>,
    supervisor: Arc<JoinHandle<()>>,
}

impl ReconnectingSession {
    /// Open the first session and start supervising it.
    ///
    /// Fails if the *initial* connection fails: a target that cannot be reached
    /// at all is a configuration problem, and retrying it silently would hide a
    /// typo'd instance ID behind a minute of backoff.
    pub async fn connect(target: impl Into<String>, config: ReconnectConfig) -> Result<Self> {
        let manager = SessionManager::new().await?;
        Self::connect_with(target, config, manager).await
    }

    /// Open the first session using an existing manager.
    pub async fn connect_with(
        target: impl Into<String>,
        mut config: ReconnectConfig,
        manager: SessionManager,
    ) -> Result<Self> {
        config.session.target = target.into();

        let (events, _) = broadcast::channel(64);
        let inner = Arc::new(Inner {
            manager,
            config,
            current: RwLock::new(None),
            output: Arc::new(OutputFanout::new()),
            events,
            shutdown: ShutdownSignal::new(),
            generation: Mutex::new(0),
        });

        let session = Arc::new(
            inner
                .manager
                .start_session(inner.config.session.clone())
                .await?,
        );
        inner.adopt(session).await;

        let supervisor = tokio::spawn(supervise(Arc::clone(&inner)));

        Ok(Self {
            inner,
            supervisor: Arc::new(supervisor),
        })
    }

    /// The target this session connects to.
    pub fn target(&self) -> &str {
        &self.inner.config.session.target
    }

    /// The current underlying session, if one is connected right now.
    ///
    /// Do not hold this across an await if you care about reconnects — it is a
    /// snapshot, and it may be closed by the time you use it.
    pub async fn current(&self) -> Option<Arc<Session>> {
        self.inner.current.read().await.clone()
    }

    /// Subscribe to output.
    ///
    /// The stream spans reconnects: it does not end when a session dies, only
    /// when the supervisor gives up or you [`terminate`](Self::terminate).
    pub fn output(&self) -> OutputStream {
        self.inner
            .output
            .subscribe(self.inner.config.session.output_buffer)
    }

    /// Subscribe to reconnection events.
    pub fn events(&self) -> broadcast::Receiver<ReconnectEvent> {
        self.inner.events.subscribe()
    }

    /// How many times the underlying session has been replaced.
    pub fn generation(&self) -> u64 {
        *self
            .inner
            .generation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Whether a session is connected and ready right now.
    pub async fn is_ready(&self) -> bool {
        matches!(self.current().await, Some(s) if s.is_ready() && !s.is_closed())
    }

    /// Send data on the current session, waiting through a reconnect if needed.
    ///
    /// Blocks for at most [`SessionConfig::ready_timeout`] while the supervisor
    /// rebuilds a dropped session. Fails immediately once the supervisor has
    /// given up or [`terminate`](Self::terminate) was called.
    pub async fn send(&self, data: impl Into<Bytes>) -> Result<()> {
        let data = data.into();
        let deadline = tokio::time::Instant::now() + self.inner.config.session.ready_timeout;

        loop {
            if let Some(session) = self.current().await {
                if !session.is_closed() {
                    match session.send(data.clone()).await {
                        Ok(()) => return Ok(()),
                        // The session died between the snapshot and the send.
                        // Fall through and wait for its replacement.
                        Err(Error::SessionClosed(_)) => {}
                        Err(e) => return Err(e),
                    }
                }
            }

            if self.inner.shutdown.is_shutdown() {
                return Err(Error::SessionClosed(
                    "the reconnecting session has stopped".into(),
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout(self.inner.config.session.ready_timeout));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop supervising and terminate the current session.
    ///
    /// After this the handle is inert: `send` fails and `output` ends.
    pub async fn terminate(&self) -> Result<()> {
        self.inner.shutdown.shutdown();
        let session = self.inner.current.write().await.take();
        let result = match session {
            Some(session) => session.terminate().await,
            None => Ok(()),
        };
        self.inner.output.close();
        result
    }
}

impl Drop for ReconnectingSession {
    fn drop(&mut self) {
        // Only the final handle tears things down; clones share the supervisor.
        if Arc::strong_count(&self.supervisor) == 1 {
            self.inner.shutdown.shutdown();
            self.supervisor.abort();
        }
    }
}

impl std::fmt::Debug for ReconnectingSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectingSession")
            .field("target", &self.target())
            .field("generation", &self.generation())
            .finish()
    }
}

impl Inner {
    /// Install a session and start pumping its output into the durable fan-out.
    async fn adopt(&self, session: Arc<Session>) {
        *self.current.write().await = Some(Arc::clone(&session));
        *self.generation.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        // The pump outlives one reconnect cycle, so it needs its own handle on
        // the fan-out rather than borrowing from `Inner`.
        tokio::spawn(pump_output(session, Arc::clone(&self.output)));
    }

    fn emit(&self, event: ReconnectEvent) {
        // A closed receiver set is normal: nobody is required to watch events.
        let _ = self.events.send(event);
    }
}

/// Forward one session's output into the durable fan-out.
async fn pump_output(session: Arc<Session>, output: Arc<OutputFanout>) {
    use futures_util::StreamExt;

    let mut stream = session.output();
    while let Some(chunk) = stream.next().await {
        output.send(chunk);
    }
    if stream.lagged() {
        warn!("output consumer fell behind during a reconnecting session");
    }
}

/// Watch the current session and rebuild it when it drops.
async fn supervise(inner: Arc<Inner>) {
    loop {
        let session = {
            let guard = inner.current.read().await;
            match guard.as_ref() {
                Some(session) => Arc::clone(session),
                None => return,
            }
        };

        tokio::select! {
            biased;
            () = inner.shutdown.cancelled() => return,
            () = session.closed() => {}
        }

        let reason = session
            .close_reason()
            .unwrap_or(CloseReason::Transport("connection lost".into()));
        inner.emit(ReconnectEvent::Disconnected {
            reason: reason.clone(),
        });

        if !reason.is_recoverable() {
            info!(%reason, "session ended for a reason a reconnect cannot fix");
            inner.emit(ReconnectEvent::GaveUp {
                attempts: 0,
                reason: reason.to_string(),
            });
            break;
        }

        match reconnect(&inner).await {
            Ok(()) => continue,
            Err(e) => {
                inner.emit(ReconnectEvent::GaveUp {
                    attempts: inner.config.max_attempts,
                    reason: e.to_string(),
                });
                break;
            }
        }
    }

    inner.output.close();
    *inner.current.write().await = None;
    debug!("reconnect supervisor finished");
}

/// Retry with exponential backoff and full jitter until a session opens.
async fn reconnect(inner: &Arc<Inner>) -> Result<()> {
    let mut attempt = 0u32;
    let mut ceiling = inner.config.initial_delay;

    loop {
        attempt += 1;
        if inner.config.max_attempts > 0 && attempt > inner.config.max_attempts {
            return Err(Error::transport(format!(
                "gave up reconnecting to {} after {} attempts",
                inner.config.session.target, inner.config.max_attempts
            )));
        }

        // Full jitter (AWS's own recommendation): sleeping a uniform sample from
        // [0, ceiling] rather than the ceiling itself keeps a fleet of clients
        // from retrying in lockstep after a shared outage.
        let delay = jitter(ceiling);
        inner.emit(ReconnectEvent::Reconnecting { attempt, delay });
        info!(attempt, ?delay, target = %inner.config.session.target, "reconnecting");

        tokio::select! {
            biased;
            () = inner.shutdown.cancelled() => {
                return Err(Error::SessionClosed("shutdown requested while reconnecting".into()));
            }
            _ = tokio::time::sleep(delay) => {}
        }

        match inner
            .manager
            .start_session(inner.config.session.clone())
            .await
        {
            Ok(session) => {
                let session = Arc::new(session);
                let session_id = session.id().to_owned();
                inner.adopt(session).await;
                info!(%session_id, attempt, "reconnected");
                inner.emit(ReconnectEvent::Reconnected {
                    session_id,
                    attempts: attempt,
                });
                return Ok(());
            }
            Err(e) if !e.is_retriable() => {
                warn!(error = %e, "reconnect failed permanently");
                return Err(e);
            }
            Err(e) => {
                warn!(attempt, error = %e, "reconnect attempt failed");
                ceiling = (ceiling * 2).min(inner.config.max_delay);
            }
        }
    }
}

/// A uniform sample from `[0, ceiling]`.
fn jitter(ceiling: Duration) -> Duration {
    let millis = ceiling.as_millis().min(u128::from(u64::MAX)) as u64;
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rand::thread_rng().gen_range(0..=millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_bound_both_attempts_and_delay() {
        let config = ReconnectConfig::default();
        assert_eq!(config.max_attempts, 10);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(60));
    }

    /// Full jitter must be able to produce both ends of the range, and never
    /// exceed the ceiling — a delay above `max_delay` would defeat the cap.
    #[test]
    fn jitter_stays_within_the_ceiling() {
        let ceiling = Duration::from_millis(1000);
        let samples: Vec<Duration> = (0..500).map(|_| jitter(ceiling)).collect();

        assert!(
            samples.iter().all(|d| *d <= ceiling),
            "jitter exceeded the ceiling"
        );
        assert!(
            samples.iter().any(|d| *d < ceiling / 2),
            "jitter should reach the low half of the range"
        );
        assert!(
            samples.iter().any(|d| *d > ceiling / 2),
            "jitter should reach the high half of the range"
        );
    }

    #[test]
    fn jitter_of_zero_is_zero() {
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
    }

    /// Only failures a retry could fix should restart the loop. Reconnecting
    /// after a clean agent close would resurrect a session the operator ended.
    #[test]
    fn only_recoverable_closures_trigger_a_reconnect() {
        assert!(CloseReason::Transport("reset".into()).is_recoverable());
        assert!(CloseReason::PeerUnresponsive {
            idle: Duration::from_secs(120)
        }
        .is_recoverable());
        assert!(CloseReason::DeliveryFailed {
            sequence: 1,
            attempts: 3000
        }
        .is_recoverable());

        assert!(!CloseReason::Terminated.is_recoverable());
        assert!(!CloseReason::AgentClosed {
            exit_code: Some(0),
            detail: None
        }
        .is_recoverable());
        assert!(!CloseReason::Protocol("bad digest".into()).is_recoverable());
    }

    #[test]
    fn events_are_cloneable_for_broadcast() {
        let event = ReconnectEvent::Reconnected {
            session_id: "s-1".into(),
            attempts: 3,
        };
        let debug = format!("{:?}", event.clone());
        assert!(debug.contains("s-1"), "{debug}");
    }
}
