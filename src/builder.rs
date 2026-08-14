//! Fluent construction of sessions.

use std::time::Duration;

use crate::connection::EndpointPolicy;
use crate::documents::{PortForwardingSession, SsmDocument};
use crate::errors::Result;
use crate::session::{DocumentSpec, Session, SessionConfig, SessionManager};

/// Builds and starts a [`Session`].
///
/// ```no_run
/// use aws_ssm_bridge::{SessionBuilder, documents::PortForwardingToRemoteHost};
///
/// # async fn example() -> aws_ssm_bridge::Result<()> {
/// let session = SessionBuilder::new("i-0123456789abcdef0")
///     .region("eu-central-1")
///     .document(PortForwardingToRemoteHost::new("db.internal", 5432))
///     .reason("incident 4711: inspect replica lag")
///     .start()
///     .await?;
/// # Ok(()) }
/// ```
///
/// Starting several sessions? Build one [`SessionManager`] and use
/// [`start_with`](Self::start_with) so they share a connection pool and
/// credential cache.
#[derive(Debug, Clone)]
pub struct SessionBuilder {
    region: Option<String>,
    config: SessionConfig,
}

impl SessionBuilder {
    /// Start building a session against `target`.
    ///
    /// See [`SessionManager::start_session`] for the accepted target forms.
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            region: None,
            config: SessionConfig::new(target),
        }
    }

    /// Pin the session to an AWS region.
    ///
    /// Only affects [`start`](Self::start), which builds its own manager.
    /// [`start_with`](Self::start_with) uses the region its manager was built
    /// with, since the manager already owns a configured SSM client.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Pin the region only if `region` is `Some`.
    ///
    /// Convenient for plumbing an optional CLI flag through without a branch.
    pub fn maybe_region(mut self, region: Option<impl Into<String>>) -> Self {
        if let Some(region) = region {
            self.region = Some(region.into());
        }
        self
    }

    /// Run a specific SSM document; see [`crate::documents`].
    pub fn document(mut self, document: impl SsmDocument) -> Self {
        self.config.document = Some(DocumentSpec::new(&document));
        self
    }

    /// Forward `remote_port` on the target instance.
    ///
    /// Shorthand for `.document(PortForwardingSession::new(remote_port))`.
    pub fn port_forward(self, remote_port: u16) -> Self {
        self.document(PortForwardingSession::new(remote_port))
    }

    /// Record why this session was opened. Shows up in CloudTrail.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.config.reason = Some(reason.into());
        self
    }

    /// How long to wait for the agent handshake.
    pub fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.config.ready_timeout = timeout;
        self
    }

    /// Keep-alive ping interval and the idle window that declares the peer dead.
    ///
    /// `idle_timeout` must be longer than `interval`; starting the session fails
    /// otherwise.
    pub fn keepalive(mut self, interval: Duration, idle_timeout: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self.config.idle_timeout = idle_timeout;
        self
    }

    /// Bytes per protocol message when splitting outbound data.
    ///
    /// The default matches the reference plugin. Raising it improves throughput
    /// for bulk transfers at the cost of coarser acknowledgements; leave it
    /// alone for interactive sessions.
    pub fn payload_chunk_size(mut self, bytes: usize) -> Self {
        self.config.payload_chunk_size = bytes;
        self
    }

    /// Queue depth for each [`Session::output`] subscriber.
    ///
    /// [`Session::output`]: crate::Session::output
    pub fn output_buffer(mut self, messages: usize) -> Self {
        self.config.output_buffer = messages;
        self
    }

    /// Relax or restore endpoint validation for the data channel.
    ///
    /// Only useful for pointing tests at a local mock gateway.
    pub fn endpoint_policy(mut self, policy: EndpointPolicy) -> Self {
        self.config.endpoint_policy = policy;
        self
    }

    /// The configuration, without starting anything.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Consume the builder and return its configuration.
    pub fn into_config(self) -> SessionConfig {
        self.config
    }

    /// Build a manager and start the session.
    pub async fn start(self) -> Result<Session> {
        let manager = match &self.region {
            Some(region) => SessionManager::for_region(region.clone()).await?,
            None => SessionManager::new().await?,
        };
        manager.start_session(self.config).await
    }

    /// Start the session using an existing manager.
    pub async fn start_with(self, manager: &SessionManager) -> Result<Session> {
        manager.start_session(self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::documents::SessionType;

    #[test]
    fn port_forward_shorthand_matches_the_document() {
        let config = SessionBuilder::new("i-abc")
            .port_forward(3306)
            .into_config();
        let document = config.document.expect("a document must be set");
        assert_eq!(document.name, "AWS-StartPortForwardingSession");
        assert_eq!(document.parameters["portNumber"], vec!["3306".to_string()]);
        assert_eq!(document.session_type, SessionType::Port);
    }

    #[test]
    fn a_plain_builder_starts_a_shell() {
        let config = SessionBuilder::new("i-abc").into_config();
        assert!(config.document.is_none());
        assert_eq!(config.session_type(), SessionType::StandardStream);
    }

    /// `region()` used to be silently ignored. Pin it to the builder state so a
    /// refactor cannot quietly drop it again.
    #[test]
    fn region_is_recorded() {
        assert_eq!(
            SessionBuilder::new("i-abc").region("eu-west-1").region,
            Some("eu-west-1".to_owned())
        );
        assert_eq!(SessionBuilder::new("i-abc").region, None);
    }

    #[test]
    fn maybe_region_leaves_the_region_alone_when_none() {
        let builder = SessionBuilder::new("i-abc")
            .region("eu-west-1")
            .maybe_region(Option::<String>::None);
        assert_eq!(builder.region, Some("eu-west-1".to_owned()));

        let builder = builder.maybe_region(Some("us-east-1"));
        assert_eq!(builder.region, Some("us-east-1".to_owned()));
    }

    #[test]
    fn tuning_knobs_reach_the_config() {
        let config = SessionBuilder::new("i-abc")
            .reason("audit")
            .ready_timeout(Duration::from_secs(5))
            .keepalive(Duration::from_secs(10), Duration::from_secs(45))
            .payload_chunk_size(8192)
            .output_buffer(64)
            .endpoint_policy(EndpointPolicy::AllowAny)
            .into_config();

        assert_eq!(config.reason.as_deref(), Some("audit"));
        assert_eq!(config.ready_timeout, Duration::from_secs(5));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.idle_timeout, Duration::from_secs(45));
        assert_eq!(config.payload_chunk_size, 8192);
        assert_eq!(config.output_buffer, 64);
        assert_eq!(config.endpoint_policy, EndpointPolicy::AllowAny);
    }

    #[test]
    fn the_last_document_wins() {
        let config = SessionBuilder::new("i-abc")
            .port_forward(1234)
            .document(crate::documents::SshSession::new())
            .into_config();
        let document = config.document.unwrap();
        assert_eq!(document.name, "AWS-StartSSHSession");
        assert_eq!(document.session_type, SessionType::StandardStream);
    }
}
