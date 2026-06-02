//! Session management and lifecycle

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::binary_protocol::PayloadType;
use crate::errors::{Error, Result, SessionError};
use crate::protocol::SessionType;
use crate::terminal::TerminalSize;
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

    /// Notified when protocol becomes ready (start_publication / handshake complete)
    ready_notify: Arc<Notify>,

    /// Notified when session transitions to Terminated
    terminated_notify: Arc<Notify>,

    /// Latching flag set when session transitions to Terminated.
    /// Allows `wait_terminated` to return immediately if called after termination.
    terminated_flag: Arc<std::sync::atomic::AtomicBool>,

    /// SSM client for API-side termination (optional — None for bare connections)
    ssm_client: Option<Arc<aws_sdk_ssm::Client>>,
}

impl Session {
    /// Create a new session and establish connection
    pub(crate) async fn new(
        session_id: SessionId,
        config: SessionConfig,
        stream_url: String,
        token_value: String,
        ssm_client: Option<Arc<aws_sdk_ssm::Client>>,
    ) -> Result<Self> {
        info!(session_id = %session_id, "Creating new session");

        // Create command channel
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        // Create Notify for ready signaling
        let ready_notify = Arc::new(Notify::new());

        // Create connection manager
        let manager = ConnectionManager::connect(
            session_id.clone(),
            stream_url,
            token_value,
            command_rx,
            Arc::clone(&ready_notify),
        )
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
            ready_notify,
            terminated_notify: Arc::new(Notify::new()),
            terminated_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ssm_client,
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

    /// Return a clone of the protocol-ready signal for lock-free polling.
    ///
    /// Callers (e.g. Python bindings) can cache this and call
    /// `load(Ordering::SeqCst)` directly without acquiring the session lock.
    #[cfg(feature = "python")]
    pub(crate) fn can_send_signal(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.protocol_can_send)
    }

    /// Return a clone of the ready [`Notify`] for lock-free waiting.
    ///
    /// Callers can `tokio::time::timeout(t, notify.notified()).await` without
    /// ever holding the session lock.
    #[cfg(feature = "python")]
    pub(crate) fn ready_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.ready_notify)
    }

    /// Return a clone of the terminated [`Notify`].
    ///
    /// Callers can await `notify.notified()` without holding the session lock.
    #[cfg(feature = "python")]
    pub(crate) fn terminated_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.terminated_notify)
    }

    /// Return a clone of the terminated latch flag.
    ///
    /// Callers can `load(Ordering::SeqCst)` to check termination without a lock.
    #[cfg(feature = "python")]
    pub(crate) fn terminated_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.terminated_flag)
    }

    /// Wait for the session to be ready (start_publication or handshake complete)
    ///
    /// This should be called before sending data to ensure the agent is ready.
    /// Returns `true` if ready, `false` if timeout expired.
    pub async fn wait_for_ready(&self, timeout: std::time::Duration) -> bool {
        // Subscribe to notification BEFORE checking the flag so that a
        // notification fired between the check and the subscribe is not lost
        // (Notify only wakes registered subscribers).
        let notified = self.ready_notify.notified();

        // Fast path: already ready
        if self
            .protocol_can_send
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return true;
        }

        // Wait for notification or timeout
        match tokio::time::timeout(timeout, notified).await {
            Ok(_) => true,
            Err(_) => {
                // Timeout — check once more (notification may have raced)
                self.protocol_can_send
                    .load(std::sync::atomic::Ordering::SeqCst)
            }
        }
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
        if new_state == SessionState::Terminated {
            // Store the flag BEFORE notify_waiters so any racer that wakes and
            // checks the flag will see it set.
            self.terminated_flag
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.terminated_notify.notify_waiters();
        }
    }

    /// Get output stream for reading stdout/stderr
    pub fn output(&self) -> OutputStream {
        self.channels.output_stream()
    }

    /// Subscribe to a lossless output stream backed by a bounded mpsc channel.
    ///
    /// Unlike [`output`], this receiver never drops frames under load and is
    /// suitable for consumers that must not lose any bytes (e.g. the smux
    /// `recv_task`).  Sustained consumer backpressure is treated as fatal and
    /// will close the multiplexer.
    pub fn subscribe_output(&self) -> mpsc::Receiver<bytes::Bytes> {
        self.channels.subscribe_lossless()
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

    /// Send terminal size to the session
    ///
    /// This sends a control message with PayloadType::Size to inform the
    /// remote agent of the terminal dimensions.
    pub async fn send_size(&self, size: TerminalSize) -> Result<()> {
        let state = self.state().await;
        if !state.can_send() {
            return Err(SessionError::InvalidState {
                expected: "Connected".to_string(),
                actual: format!("{:?}", state),
            }
            .into());
        }

        let data = size.to_json()?;
        debug!(cols = size.cols, rows = size.rows, "Sending terminal size");

        self.command_tx
            .send(ManagerCommand::SendMessage {
                data,
                payload_type: PayloadType::Size,
            })
            .map_err(|_| Error::InvalidState("Session command channel closed".to_string()))?;

        Ok(())
    }

    /// Terminate the session
    ///
    /// Terminates both the WebSocket connection and the AWS-side session.
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

        // Terminate on AWS side (best-effort)
        if let Some(ref client) = self.ssm_client {
            if let Err(e) = client
                .terminate_session()
                .session_id(&self.session_id)
                .send()
                .await
            {
                warn!(error = ?e, "Failed to terminate session via AWS API (best-effort)");
            } else {
                debug!("Session terminated via AWS API");
            }
        }

        self.set_state(SessionState::Terminated).await;

        Ok(())
    }

    /// Wait for session to terminate
    pub async fn wait_terminated(&self) {
        // Create the notified() future BEFORE the flag check to avoid the race
        // where termination happens between the load and the notified().await.
        let notified = self.terminated_notify.notified();
        if self
            .terminated_flag
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        notified.await;
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

        // Validate target format (instance-id, mi-id, or ECS target)
        if !Self::is_valid_target(&config.target) {
            return Err(Error::Config(format!(
                "Invalid target format: '{}'. Expected i-xxx, mi-xxx, or ARN",
                config.target
            )));
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
        let session = Session::new(
            session_id,
            config,
            stream_url,
            token_value,
            Some(Arc::clone(&self.ssm_client)),
        )
        .await?;

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

    /// Validate target format.
    ///
    /// Accepted formats:
    /// - EC2 instance ID: `i-<hex17>`
    /// - Managed instance: `mi-<hex17>`
    /// - ARN (ECS tasks, etc.): `arn:aws:...`
    fn is_valid_target(target: &str) -> bool {
        // Instance IDs: i-0123456789abcdef0 or mi-0123456789abcdef0
        let is_instance_id = (target.starts_with("i-") || target.starts_with("mi-"))
            && target.len() >= 10
            && target
                .split('-')
                .next_back()
                .is_some_and(|hex| hex.chars().all(|c| c.is_ascii_hexdigit()));

        // ARN format: arn:aws:...
        let is_arn = target.starts_with("arn:aws:");

        is_instance_id || is_arn
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

    #[test]
    fn test_valid_targets() {
        assert!(SessionManager::is_valid_target("i-1234567890abcdef0"));
        assert!(SessionManager::is_valid_target("mi-1234567890abcdef0"));
        assert!(SessionManager::is_valid_target(
            "arn:aws:ecs:us-east-1:123456789012:task/my-cluster/abc123"
        ));
    }

    #[test]
    fn test_invalid_targets() {
        assert!(!SessionManager::is_valid_target(""));
        assert!(!SessionManager::is_valid_target("foobar"));
        assert!(!SessionManager::is_valid_target("i-"));
        assert!(!SessionManager::is_valid_target("x-1234567890abcdef0"));
        assert!(!SessionManager::is_valid_target("../../etc/passwd"));
        assert!(!SessionManager::is_valid_target("i-ZZZZ"));
    }
}
