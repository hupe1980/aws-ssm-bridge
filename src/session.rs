//! Session management and lifecycle

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::errors::{Error, Result, SessionError};
use crate::protocol::SessionType;
use crate::{
    channels::ChannelMultiplexer,
    connection::{ConnectionManager, ManagerCommand},
    OutputStream,
};

/// Session identifier
pub type SessionId = String;

/// Session state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Session is being created
    Initializing,
    /// Session is connected and active
    Connected,
    /// Session is disconnecting
    Disconnecting,
    /// Session is terminated
    Terminated,
}

impl SessionState {
    /// Check if session can accept commands
    pub fn can_send(&self) -> bool {
        matches!(self, SessionState::Connected)
    }

    /// Check if session is active
    pub fn is_active(&self) -> bool {
        matches!(self, SessionState::Initializing | SessionState::Connected)
    }
}

/// Session configuration
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Target instance ID or other target identifier
    pub target: String,

    /// AWS region (if None, uses default from AWS config)
    pub region: Option<String>,

    /// Session type
    pub session_type: SessionType,

    /// Document name for SSM (if None, uses default)
    pub document_name: Option<String>,

    /// Parameters for the session document
    pub parameters: std::collections::HashMap<String, Vec<String>>,

    /// Reason for starting the session (for auditing)
    pub reason: Option<String>,

    /// Connection timeout (default: 30 seconds)
    pub connect_timeout: std::time::Duration,

    /// Idle timeout - disconnect if no activity (default: 20 minutes)
    pub idle_timeout: std::time::Duration,

    /// Maximum session duration (default: none)
    pub max_duration: Option<std::time::Duration>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            target: String::new(),
            region: None,
            session_type: SessionType::StandardStream,
            document_name: None,
            parameters: std::collections::HashMap::new(),
            reason: None,
            connect_timeout: std::time::Duration::from_secs(30),
            idle_timeout: std::time::Duration::from_secs(20 * 60), // 20 min
            max_duration: None,
        }
    }
}

/// Represents an active SSM session with proper lifecycle management
pub struct Session {
    /// Session ID from AWS
    session_id: SessionId,

    /// Session configuration
    config: SessionConfig,

    /// Current session state
    state: Arc<RwLock<SessionState>>,

    /// Command sender to connection manager
    command_tx: mpsc::UnboundedSender<ManagerCommand>,

    /// Channel multiplexer (shared with connection manager)
    channels: Arc<ChannelMultiplexer>,

    /// Connection manager task handle
    manager_task: Option<JoinHandle<Result<()>>>,

    /// Publication state from connection manager (protocol-level can_send)
    protocol_can_send: Arc<std::sync::atomic::AtomicBool>,
}

impl Session {
    /// Create a new session and establish connection
    pub(crate) async fn new(
        session_id: SessionId,
        config: SessionConfig,
        stream_url: String,
        token_value: String,
    ) -> Result<Self> {
        info!(session_id = %session_id, "Creating new session");

        // Create command channel
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        // Create connection manager
        let manager =
            ConnectionManager::connect(session_id.clone(), stream_url, token_value, command_rx)
                .await?;

        // Get channels and can_send state before moving manager
        let channels = manager.channels();
        let protocol_can_send = manager.can_send();

        // Spawn connection manager task
        let manager_task = tokio::spawn(async move { manager.run().await });

        let session = Self {
            session_id,
            config,
            state: Arc::new(RwLock::new(SessionState::Initializing)),
            command_tx,
            channels,
            manager_task: Some(manager_task),
            protocol_can_send,
        };

        // Transition to connected state
        session.set_state(SessionState::Connected).await;

        Ok(session)
    }

    /// Get session ID
    pub fn id(&self) -> &str {
        &self.session_id
    }

    /// Get session configuration
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Get current session state
    pub async fn state(&self) -> SessionState {
        *self.state.read().await
    }

    /// Check if the protocol is ready to send (start_publication received)
    pub fn is_ready(&self) -> bool {
        self.protocol_can_send
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wait for the session to be ready (start_publication or handshake complete)
    ///
    /// This should be called before sending data to ensure the agent is ready.
    /// Returns `true` if ready, `false` if timeout expired.
    pub async fn wait_for_ready(&self, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let check_interval = std::time::Duration::from_millis(100);

        while tokio::time::Instant::now() < deadline {
            if self
                .protocol_can_send
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return true;
            }
            tokio::time::sleep(check_interval).await;
        }

        false
    }

    /// Update session state
    async fn set_state(&self, new_state: SessionState) {
        let mut state = self.state.write().await;
        debug!(
            session_id = %self.session_id,
            old_state = ?*state,
            new_state = ?new_state,
            "Session state transition"
        );
        *state = new_state;
    }

    /// Get output stream for reading stdout/stderr
    pub fn output(&self) -> OutputStream {
        self.channels.output_stream()
    }

    /// Send data to the session (stdin)
    pub async fn send(&self, data: Bytes) -> Result<()> {
        let state = self.state().await;
        if !state.can_send() {
            return Err(SessionError::InvalidState {
                expected: "Connected".to_string(),
                actual: format!("{:?}", state),
            }
            .into());
        }

        self.command_tx
            .send(ManagerCommand::SendData(data))
            .map_err(|_| Error::InvalidState("Session command channel closed".to_string()))?;

        Ok(())
    }

    /// Terminate the session
    pub async fn terminate(&mut self) -> Result<()> {
        info!(session_id = %self.session_id, "Terminating session");

        self.set_state(SessionState::Disconnecting).await;

        self.command_tx
            .send(ManagerCommand::Terminate)
            .map_err(|_| Error::InvalidState("Session command channel closed".to_string()))?;

        // Wait for manager task to complete
        if let Some(task) = self.manager_task.take() {
            match task.await {
                Ok(Ok(())) => debug!("Manager task completed successfully"),
                Ok(Err(e)) => warn!(error = ?e, "Manager task completed with error"),
                Err(e) => warn!(error = ?e, "Manager task panicked"),
            }
        }

        self.set_state(SessionState::Terminated).await;

        Ok(())
    }

    /// Wait for session to terminate
    pub async fn wait_terminated(&self) {
        loop {
            let state = self.state().await;
            if state == SessionState::Terminated {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}

// Implement Drop to ensure cleanup
impl Drop for Session {
    fn drop(&mut self) {
        // If manager task still exists, abort it
        if let Some(task) = self.manager_task.take() {
            task.abort();
        }
    }
}

/// Session manager for creating and managing SSM sessions
pub struct SessionManager {
    /// AWS SSM client
    ssm_client: Arc<aws_sdk_ssm::Client>,
}

impl SessionManager {
    /// Create a new session manager
    ///
    /// This will use the default AWS credential provider chain and region.
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_from_env().await;
        let ssm_client = aws_sdk_ssm::Client::new(&config);

        Ok(Self {
            ssm_client: Arc::new(ssm_client),
        })
    }

    /// Create a new session manager with custom AWS config
    pub fn with_config(config: &aws_config::SdkConfig) -> Self {
        let ssm_client = aws_sdk_ssm::Client::new(config);

        Self {
            ssm_client: Arc::new(ssm_client),
        }
    }

    /// Start a new SSM session
    pub async fn start_session(&self, config: SessionConfig) -> Result<Session> {
        info!(
            target = %config.target,
            session_type = ?config.session_type,
            "Starting SSM session"
        );

        // Validate configuration
        if config.target.is_empty() {
            return Err(Error::Config("Target cannot be empty".to_string()));
        }

        // Determine document name based on session type
        // AWS SSM Session Manager supports these documents:
        // - (none)                                      → Standard shell session
        // - AWS-StartPortForwardingSession              → Port forwarding to instance
        // - AWS-StartPortForwardingSessionToRemoteHost  → Port forwarding through instance
        // - AWS-StartInteractiveCommand                 → Execute specific commands interactively
        // - AWS-StartSSHSession                         → SSH over Session Manager
        let document_name = config.document_name.clone().or_else(|| {
            match config.session_type {
                SessionType::StandardStream => None, // Use default shell session (no document)
                SessionType::Port => Some("AWS-StartPortForwardingSession".to_string()),
                SessionType::InteractiveCommands => Some("AWS-StartInteractiveCommand".to_string()),
            }
        });

        // Build StartSession request
        let mut request = self.ssm_client.start_session().target(&config.target);

        // Only set document_name if we have one
        if let Some(ref doc) = document_name {
            request = request.document_name(doc);
        }

        // Add parameters
        if !config.parameters.is_empty() {
            request = request.set_parameters(Some(config.parameters.clone()));
        }

        // Add reason if provided
        if let Some(ref reason) = config.reason {
            request = request.reason(reason);
        }

        // Execute request
        let response = request.send().await.map_err(|e| {
            warn!(error = ?e, "Failed to start SSM session");
            Error::from(e)
        })?;

        // Extract session details
        let session_id = response
            .session_id()
            .ok_or_else(|| Error::AwsSdk("No session ID in response".to_string()))?
            .to_string();

        let stream_url = response
            .stream_url()
            .ok_or_else(|| Error::AwsSdk("No stream URL in response".to_string()))?
            .to_string();

        let token_value = response
            .token_value()
            .ok_or_else(|| Error::AwsSdk("No token value in response".to_string()))?
            .to_string();

        info!(session_id = %session_id, "SSM session started");

        // Create session with actual connection
        let session = Session::new(session_id, config, stream_url, token_value).await?;

        Ok(session)
    }

    /// Terminate a session by ID
    pub async fn terminate_session(&self, session_id: &str) -> Result<()> {
        info!(session_id = %session_id, "Terminating session via AWS API");

        self.ssm_client
            .terminate_session()
            .session_id(session_id)
            .send()
            .await
            .map_err(|e| {
                warn!(error = ?e, "Failed to terminate session");
                Error::from(e)
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_state_transitions() {
        assert!(SessionState::Connected.can_send());
        assert!(!SessionState::Terminated.can_send());
        assert!(SessionState::Connected.is_active());
        assert!(!SessionState::Terminated.is_active());
    }

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();
        assert_eq!(config.session_type, SessionType::StandardStream);
        assert!(config.document_name.is_none());
    }
}
