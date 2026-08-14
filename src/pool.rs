//! Managing many concurrent sessions.
//!
//! A pool is worth having when you fan out across a fleet: it caps how many
//! sessions you hold open, indexes them by ID and target, and terminates the lot
//! on shutdown so nothing is left running on the AWS side.
//!
//! Sessions that end on their own — a dead network, an agent restart, an idle
//! timeout — are dropped from the pool the next time it is inspected. There is
//! no background reaper: [`Session::is_closed`] is authoritative, so filtering on
//! access is both cheaper and impossible to get out of sync.
//!
//! ```no_run
//! use aws_ssm_bridge::{PoolConfig, SessionPool};
//!
//! # async fn example() -> aws_ssm_bridge::Result<()> {
//! let pool = SessionPool::new(PoolConfig { max_sessions: 25, ..Default::default() }).await?;
//!
//! let session = pool.start("i-0123456789abcdef0").await?;
//! session.wait_ready().await?;
//! session.send(&b"uptime\r"[..]).await?;
//!
//! println!("{} sessions live", pool.stats().live);
//! pool.shutdown().await;
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::errors::{Error, Result};
use crate::session::{Session, SessionConfig, SessionManager};

/// Limits and defaults for a [`SessionPool`].
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum live sessions; `0` means unlimited.
    pub max_sessions: usize,
    /// Allow more than one session to the same target.
    pub allow_duplicate_targets: bool,
    /// Template applied to sessions started through [`SessionPool::start`].
    pub session_defaults: SessionConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_sessions: 100,
            allow_duplicate_targets: true,
            session_defaults: SessionConfig::default(),
        }
    }
}

/// A point-in-time view of a pool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Sessions currently open.
    pub live: usize,
    /// Sessions started over the pool's lifetime.
    pub started: u64,
    /// Sessions that have ended, however they ended.
    pub ended: u64,
}

/// A bounded collection of concurrent sessions.
pub struct SessionPool {
    manager: SessionManager,
    config: PoolConfig,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    started: Mutex<u64>,
}

impl SessionPool {
    /// Build a pool with its own [`SessionManager`].
    pub async fn new(config: PoolConfig) -> Result<Self> {
        Ok(Self::with_manager(config, SessionManager::new().await?))
    }

    /// Build a pool around an existing manager.
    pub fn with_manager(config: PoolConfig, manager: SessionManager) -> Self {
        Self {
            manager,
            config,
            sessions: Mutex::new(HashMap::new()),
            started: Mutex::new(0),
        }
    }

    /// Start a shell session against `target` using the pool's defaults.
    pub async fn start(&self, target: impl Into<String>) -> Result<Arc<Session>> {
        let config = SessionConfig {
            target: target.into(),
            ..self.config.session_defaults.clone()
        };
        self.start_with(config).await
    }

    /// Start a session with an explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if the pool is full, or if a session to this
    /// target already exists and duplicates are disallowed.
    pub async fn start_with(&self, config: SessionConfig) -> Result<Arc<Session>> {
        let target = config.target.clone();
        self.check_admission(&target)?;

        let session = Arc::new(self.manager.start_session(config).await?);

        // Re-check under the lock: another task may have filled the last slot
        // while this one was waiting on StartSession. Terminating the session we
        // just opened is the only way to honour the limit without leaking it.
        let rejection = {
            let mut sessions = self.lock();
            reap(&mut sessions);
            match admit(&self.config, &sessions, &target) {
                Ok(()) => {
                    sessions.insert(session.id().to_owned(), Arc::clone(&session));
                    None
                }
                Err(e) => Some(e),
            }
        };

        if let Some(e) = rejection {
            let _ = session.terminate().await;
            return Err(e);
        }
        *lock(&self.started) += 1;

        Ok(session)
    }

    /// Add an externally created session to the pool.
    ///
    /// Useful when a session was built with [`SessionBuilder`] but should share
    /// the pool's lifecycle management.
    ///
    /// [`SessionBuilder`]: crate::SessionBuilder
    pub fn insert(&self, session: Arc<Session>) -> Result<Arc<Session>> {
        let target = session.config().target.clone();
        let mut sessions = self.lock();
        reap(&mut sessions);
        admit(&self.config, &sessions, &target)?;
        sessions.insert(session.id().to_owned(), Arc::clone(&session));
        drop(sessions);
        *lock(&self.started) += 1;
        Ok(session)
    }

    /// Look up a live session by ID.
    pub fn get(&self, session_id: &str) -> Option<Arc<Session>> {
        let mut sessions = self.lock();
        reap(&mut sessions);
        sessions.get(session_id).cloned()
    }

    /// Every live session against `target`.
    pub fn for_target(&self, target: &str) -> Vec<Arc<Session>> {
        let mut sessions = self.lock();
        reap(&mut sessions);
        sessions
            .values()
            .filter(|s| s.config().target == target)
            .cloned()
            .collect()
    }

    /// Every live session.
    pub fn sessions(&self) -> Vec<Arc<Session>> {
        let mut sessions = self.lock();
        reap(&mut sessions);
        sessions.values().cloned().collect()
    }

    /// IDs of every live session.
    pub fn session_ids(&self) -> Vec<String> {
        let mut sessions = self.lock();
        reap(&mut sessions);
        sessions.keys().cloned().collect()
    }

    /// Current counts.
    pub fn stats(&self) -> PoolStats {
        let mut sessions = self.lock();
        reap(&mut sessions);
        let live = sessions.len();
        let started = *lock(&self.started);
        PoolStats {
            live,
            started,
            ended: started.saturating_sub(live as u64),
        }
    }

    /// Terminate one session and drop it from the pool.
    ///
    /// Unknown IDs are not an error — the session may already have ended and
    /// been reaped.
    pub async fn terminate(&self, session_id: &str) -> Result<()> {
        let session = self.lock().remove(session_id);
        match session {
            Some(session) => session.terminate().await,
            None => Ok(()),
        }
    }

    /// Terminate every session against `target`.
    pub async fn terminate_target(&self, target: &str) -> Result<()> {
        let doomed: Vec<Arc<Session>> = {
            let mut sessions = self.lock();
            let ids: Vec<String> = sessions
                .iter()
                .filter(|(_, s)| s.config().target == target)
                .map(|(id, _)| id.clone())
                .collect();
            ids.iter().filter_map(|id| sessions.remove(id)).collect()
        };
        terminate_all(doomed).await;
        Ok(())
    }

    /// Terminate everything.
    ///
    /// Sessions are terminated concurrently, so a fleet-wide shutdown costs one
    /// round trip rather than one per session. Failures are logged, not
    /// returned: there is nothing useful a caller can do about a session that
    /// refuses to die, and AWS reclaims it on its own timeout.
    pub async fn shutdown(&self) {
        let doomed: Vec<Arc<Session>> = self.lock().drain().map(|(_, s)| s).collect();
        tracing::info!(count = doomed.len(), "shutting down the session pool");
        terminate_all(doomed).await;
    }

    fn check_admission(&self, target: &str) -> Result<()> {
        let mut sessions = self.lock();
        reap(&mut sessions);
        admit(&self.config, &sessions, target)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Session>>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl std::fmt::Debug for SessionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionPool")
            .field("stats", &self.stats())
            .field("max_sessions", &self.config.max_sessions)
            .finish()
    }
}

/// Drop sessions that have already ended.
fn reap(sessions: &mut HashMap<String, Arc<Session>>) {
    sessions.retain(|_, session| !session.is_closed());
}

fn admit(
    config: &PoolConfig,
    sessions: &HashMap<String, Arc<Session>>,
    target: &str,
) -> Result<()> {
    if config.max_sessions > 0 && sessions.len() >= config.max_sessions {
        return Err(Error::Config(format!(
            "session pool is full ({} of {} in use)",
            sessions.len(),
            config.max_sessions
        )));
    }
    if !config.allow_duplicate_targets && sessions.values().any(|s| s.config().target == target) {
        return Err(Error::Config(format!(
            "a session to {target} is already open and PoolConfig::allow_duplicate_targets is false"
        )));
    }
    Ok(())
}

async fn terminate_all(sessions: Vec<Arc<Session>>) {
    let outcomes = futures_util::future::join_all(
        sessions
            .iter()
            .map(|session| async move { (session.id(), session.terminate().await) }),
    )
    .await;

    for (id, result) in outcomes {
        if let Err(e) = result {
            tracing::warn!(session_id = %id, error = %e, "failed to terminate a pooled session");
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_sessions: usize, allow_duplicates: bool) -> PoolConfig {
        PoolConfig {
            max_sessions,
            allow_duplicate_targets: allow_duplicates,
            session_defaults: SessionConfig::default(),
        }
    }

    #[test]
    fn defaults_are_permissive_but_bounded() {
        let config = PoolConfig::default();
        assert_eq!(config.max_sessions, 100);
        assert!(config.allow_duplicate_targets);
    }

    #[test]
    fn an_empty_pool_admits_under_every_policy() {
        let sessions = HashMap::new();
        assert!(
            admit(&config(0, true), &sessions, "i-a").is_ok(),
            "0 means unlimited"
        );
        assert!(admit(&config(1, true), &sessions, "i-a").is_ok());
        assert!(admit(&config(1, false), &sessions, "i-a").is_ok());
    }

    #[test]
    fn stats_start_at_zero() {
        assert_eq!(
            PoolStats::default(),
            PoolStats {
                live: 0,
                started: 0,
                ended: 0
            }
        );
    }

    #[test]
    fn reaping_an_empty_map_is_a_no_op() {
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        reap(&mut sessions);
        assert!(sessions.is_empty());
    }
}
