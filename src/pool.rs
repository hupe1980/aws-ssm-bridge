//! Session pool for managing multiple concurrent SSM sessions.
//!
//! Provides efficient management of multiple sessions with:
//! - Automatic cleanup of terminated sessions
//! - Configurable pool size limits
//! - Session lookup by ID or target
//! - Graceful shutdown of all sessions
//!
//! # Example
//!
//! ```rust,no_run
//! use aws_ssm_bridge::pool::{SessionPool, PoolConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a pool with max 10 concurrent sessions
//! let pool = SessionPool::new(PoolConfig {
//!     max_sessions: 10,
//!     ..Default::default()
//! }).await?;
//!
//! // Start sessions (automatically tracked)
//! let session1 = pool.start_session("i-instance1").await?;
//! let session2 = pool.start_session("i-instance2").await?;
//!
//! // Get session by ID
//! if let Some(session) = pool.get(&session1.id()).await {
//!     session.send(bytes::Bytes::from("ls\n")).await?;
//! }
//!
//! // List all active sessions
//! for id in pool.list_sessions().await {
//!     println!("Active session: {}", id);
//! }
//!
//! // Graceful shutdown of all sessions
//! pool.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::errors::{Error, Result};
use crate::session::{Session, SessionConfig, SessionId, SessionManager, SessionState};
use crate::shutdown::ShutdownSignal;

/// Configuration for the session pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of concurrent sessions (0 = unlimited)
    pub max_sessions: usize,
    /// Whether to allow multiple sessions to the same target
    pub allow_duplicate_targets: bool,
    /// Default session configuration template
    pub default_session_config: SessionConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_sessions: 100,
            allow_duplicate_targets: true,
            default_session_config: SessionConfig::default(),
        }
    }
}

/// Statistics about the session pool.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Number of currently active sessions
    pub active_sessions: usize,
    /// Total sessions started since pool creation
    pub total_started: u64,
    /// Total sessions terminated since pool creation
    pub total_terminated: u64,
    /// Number of sessions by target
    pub sessions_per_target: HashMap<String, usize>,
}

/// A pool for managing multiple concurrent SSM sessions.
///
/// The pool automatically tracks sessions and provides utilities for
/// managing their lifecycle.
pub struct SessionPool {
    /// Pool configuration
    config: PoolConfig,
    /// Session manager for creating new sessions
    manager: SessionManager,
    /// Active sessions indexed by session ID
    sessions: Arc<RwLock<HashMap<SessionId, SessionEntry>>>,
    /// Sessions indexed by target for quick lookup
    by_target: Arc<RwLock<HashMap<String, Vec<SessionId>>>>,
    /// Statistics
    stats: Arc<RwLock<PoolStats>>,
    /// Shutdown signal
    shutdown: ShutdownSignal,
}

/// Internal entry tracking a session.
struct SessionEntry {
    session: Session,
    target: String,
}

impl SessionPool {
    /// Create a new session pool with the given configuration.
    pub async fn new(config: PoolConfig) -> Result<Self> {
        let manager = SessionManager::new().await?;

        Ok(Self {
            config,
            manager,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            by_target: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(PoolStats::default())),
            shutdown: ShutdownSignal::new(),
        })
    }

    /// Create a session pool with a custom session manager.
    pub fn with_manager(config: PoolConfig, manager: SessionManager) -> Self {
        Self {
            config,
            manager,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            by_target: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(PoolStats::default())),
            shutdown: ShutdownSignal::new(),
        }
    }

    /// Start a new session to the given target.
    ///
    /// Uses the pool's default session configuration.
    pub async fn start_session(&self, target: &str) -> Result<SessionHandle<'_>> {
        let mut config = self.config.default_session_config.clone();
        config.target = target.to_string();
        self.start_session_with_config(config).await
    }

    /// Start a new session with custom configuration.
    pub async fn start_session_with_config(
        &self,
        config: SessionConfig,
    ) -> Result<SessionHandle<'_>> {
        let target = config.target.clone();

        // Pre-check with read lock (fast path rejection)
        // Note: This is an optimization; the authoritative check happens below with write lock
        {
            let sessions = self.sessions.read().await;
            if self.config.max_sessions > 0 && sessions.len() >= self.config.max_sessions {
                return Err(Error::Config(format!(
                    "Pool limit reached: {} sessions (max: {})",
                    sessions.len(),
                    self.config.max_sessions
                )));
            }
        }

        // Check duplicate target policy (fast path)
        if !self.config.allow_duplicate_targets {
            let by_target = self.by_target.read().await;
            if let Some(existing) = by_target.get(&target) {
                if !existing.is_empty() {
                    return Err(Error::Config(format!(
                        "Session already exists for target: {}",
                        target
                    )));
                }
            }
        }

        // Start the session (this is the slow network call)
        let session = self.manager.start_session(config).await?;
        let session_id = session.id().to_string();

        // Track in pool with write lock - re-check limits to prevent TOCTOU race
        {
            let mut sessions = self.sessions.write().await;

            // Re-check pool limits under write lock to prevent race condition
            if self.config.max_sessions > 0 && sessions.len() >= self.config.max_sessions {
                // Pool filled while we were starting session - terminate and return error
                // We already started the session so we need to clean it up
                drop(sessions); // Release lock before async termination
                let session = session;
                let _ = session.terminate().await;
                return Err(Error::Config(
                    "Pool limit reached while starting session (race condition)".to_string(),
                ));
            }

            let mut by_target = self.by_target.write().await;

            // Re-check duplicate target under write lock
            if !self.config.allow_duplicate_targets {
                if let Some(existing) = by_target.get(&target) {
                    if !existing.is_empty() {
                        drop(sessions);
                        drop(by_target);
                        let session = session;
                        let _ = session.terminate().await;
                        return Err(Error::Config(format!(
                            "Session already exists for target: {} (race condition)",
                            target
                        )));
                    }
                }
            }

            let mut stats = self.stats.write().await;

            sessions.insert(
                session_id.clone(),
                SessionEntry {
                    session,
                    target: target.clone(),
                },
            );

            by_target
                .entry(target.clone())
                .or_default()
                .push(session_id.clone());

            stats.active_sessions = sessions.len();
            stats.total_started += 1;
            *stats.sessions_per_target.entry(target).or_insert(0) += 1;
        }

        info!(session_id = %session_id, "Session added to pool");

        Ok(SessionHandle {
            session_id,
            pool: self,
        })
    }

    /// Get a session by ID.
    pub async fn get(&self, session_id: &str) -> Option<SessionRef<'_>> {
        let sessions = self.sessions.read().await;
        if sessions.contains_key(session_id) {
            Some(SessionRef {
                session_id: session_id.to_string(),
                pool: self,
            })
        } else {
            None
        }
    }

    /// Get all sessions for a target.
    pub async fn get_by_target(&self, target: &str) -> Vec<String> {
        let by_target = self.by_target.read().await;
        by_target.get(target).cloned().unwrap_or_default()
    }

    /// List all active session IDs.
    pub async fn list_sessions(&self) -> Vec<String> {
        let sessions = self.sessions.read().await;
        sessions.keys().cloned().collect()
    }

    /// Get pool statistics.
    pub async fn stats(&self) -> PoolStats {
        self.stats.read().await.clone()
    }

    /// Terminate a session by ID.
    pub async fn terminate(&self, session_id: &str) -> Result<()> {
        let entry = {
            let mut sessions = self.sessions.write().await;
            sessions.remove(session_id)
        };

        if let Some(entry) = entry {
            // Remove from target index
            {
                let mut by_target = self.by_target.write().await;
                if let Some(ids) = by_target.get_mut(&entry.target) {
                    ids.retain(|id| id != session_id);
                }
            }

            // Update stats
            {
                let mut stats = self.stats.write().await;
                stats.active_sessions = stats.active_sessions.saturating_sub(1);
                stats.total_terminated += 1;
                if let Some(count) = stats.sessions_per_target.get_mut(&entry.target) {
                    *count = count.saturating_sub(1);
                }
            }

            // Terminate the session
            entry.session.terminate().await?;

            info!(session_id = %session_id, "Session removed from pool");
        } else {
            warn!(session_id = %session_id, "Session not found in pool");
        }

        Ok(())
    }

    /// Terminate all sessions for a target.
    pub async fn terminate_target(&self, target: &str) -> Result<()> {
        let session_ids = self.get_by_target(target).await;
        for session_id in session_ids {
            self.terminate(&session_id).await?;
        }
        Ok(())
    }

    /// Shutdown the pool, terminating all sessions.
    pub async fn shutdown(&self) {
        info!("Shutting down session pool");
        self.shutdown.shutdown();

        // Get all session IDs
        let session_ids: Vec<String> = {
            let sessions = self.sessions.read().await;
            sessions.keys().cloned().collect()
        };

        // Terminate all sessions
        for session_id in session_ids {
            if let Err(e) = self.terminate(&session_id).await {
                warn!(session_id = %session_id, error = ?e, "Failed to terminate session during shutdown");
            }
        }

        info!("Session pool shutdown complete");
    }

    /// Get the shutdown signal for this pool.
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.shutdown.clone()
    }

    /// Clean up terminated sessions (garbage collection).
    ///
    /// This removes sessions that have been terminated but not yet removed
    /// from the pool's tracking structures.
    pub async fn cleanup(&self) {
        let mut to_remove = Vec::new();

        {
            let sessions = self.sessions.read().await;
            for (id, entry) in sessions.iter() {
                if entry.session.state().await == SessionState::Terminated {
                    to_remove.push(id.clone());
                }
            }
        }

        for session_id in to_remove {
            debug!(session_id = %session_id, "Cleaning up terminated session");
            let _ = self.terminate(&session_id).await;
        }
    }
}

/// A handle to a session in the pool.
///
/// This provides access to the session while keeping it tracked in the pool.
pub struct SessionHandle<'a> {
    session_id: String,
    pool: &'a SessionPool,
}

impl<'a> SessionHandle<'a> {
    /// Get the session ID.
    pub fn id(&self) -> &str {
        &self.session_id
    }

    /// Send data to the session.
    pub async fn send(&self, data: bytes::Bytes) -> Result<()> {
        let sessions = self.pool.sessions.read().await;
        if let Some(entry) = sessions.get(&self.session_id) {
            entry.session.send(data).await
        } else {
            Err(Error::InvalidState("Session no longer in pool".to_string()))
        }
    }

    /// Get the output stream for the session.
    pub async fn output(&self) -> Option<crate::channels::OutputStream> {
        let sessions = self.pool.sessions.read().await;
        sessions.get(&self.session_id).map(|e| e.session.output())
    }

    /// Terminate this session.
    pub async fn terminate(self) -> Result<()> {
        self.pool.terminate(&self.session_id).await
    }
}

/// A reference to a session in the pool (for get operations).
pub struct SessionRef<'a> {
    session_id: String,
    pool: &'a SessionPool,
}

impl<'a> SessionRef<'a> {
    /// Get the session ID.
    pub fn id(&self) -> &str {
        &self.session_id
    }

    /// Send data to the session.
    pub async fn send(&self, data: bytes::Bytes) -> Result<()> {
        let sessions = self.pool.sessions.read().await;
        if let Some(entry) = sessions.get(&self.session_id) {
            entry.session.send(data).await
        } else {
            Err(Error::InvalidState("Session no longer in pool".to_string()))
        }
    }

    /// Get the output stream for the session.
    pub async fn output(&self) -> Option<crate::channels::OutputStream> {
        let sessions = self.pool.sessions.read().await;
        sessions.get(&self.session_id).map(|e| e.session.output())
    }

    /// Check if the session is ready to send.
    pub async fn is_ready(&self) -> bool {
        let sessions = self.pool.sessions.read().await;
        sessions
            .get(&self.session_id)
            .map(|e| e.session.is_ready())
            .unwrap_or(false)
    }

    /// Get the session state.
    pub async fn state(&self) -> Option<SessionState> {
        let sessions = self.pool.sessions.read().await;
        if let Some(entry) = sessions.get(&self.session_id) {
            Some(entry.session.state().await)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_sessions, 100);
        assert!(config.allow_duplicate_targets);
    }

    #[test]
    fn test_pool_stats_default() {
        let stats = PoolStats::default();
        assert_eq!(stats.active_sessions, 0);
        assert_eq!(stats.total_started, 0);
        assert_eq!(stats.total_terminated, 0);
    }
}
