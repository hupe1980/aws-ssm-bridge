//! Session builder for ergonomic session creation
//!
//! Provides a fluent API for configuring and creating sessions.

use crate::documents::SsmDocument;
use crate::errors::Result;
use crate::protocol::SessionType;
use crate::session::{Session, SessionConfig, SessionManager};
use std::collections::HashMap;

/// Builder for creating SSM sessions with a fluent API
///
/// # Example
///
/// ```rust,no_run
/// use aws_ssm_bridge::{SessionBuilder, SessionType};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let session = SessionBuilder::new("i-1234567890abcdef0")
///     .region("us-east-1")
///     .session_type(SessionType::StandardStream)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct SessionBuilder {
    target: String,
    region: Option<String>,
    session_type: SessionType,
    document_name: Option<String>,
    parameters: HashMap<String, Vec<String>>,
    reason: Option<String>,
}

impl SessionBuilder {
    /// Create a new session builder for the given target
    ///
    /// # Arguments
    ///
    /// * `target` - EC2 instance ID (e.g., "i-1234567890abcdef0")
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            region: None,
            session_type: SessionType::StandardStream,
            document_name: None,
            parameters: HashMap::new(),
            reason: None,
        }
    }

    /// Set the AWS region
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set the AWS region if Some, otherwise no-op
    pub fn maybe_region(mut self, region: Option<impl Into<String>>) -> Self {
        self.region = region.map(Into::into);
        self
    }

    /// Set the session type
    pub fn session_type(mut self, session_type: SessionType) -> Self {
        self.session_type = session_type;
        self
    }

    /// Set the SSM document name
    pub fn document_name(mut self, document_name: impl Into<String>) -> Self {
        self.document_name = Some(document_name.into());
        self
    }

    /// Add a parameter for the session document
    pub fn parameter(mut self, key: impl Into<String>, values: Vec<String>) -> Self {
        self.parameters.insert(key.into(), values);
        self
    }

    /// Set the reason for starting the session (for auditing)
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Configure session using a type-safe SSM document
    ///
    /// This is the preferred way to configure sessions - it eliminates
    /// magic strings and provides compile-time validation.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use aws_ssm_bridge::{SessionBuilder, documents::PortForwardingSession};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let session = SessionBuilder::new("i-1234567890abcdef0")
    ///     .document(PortForwardingSession::new(3306))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn document(mut self, doc: impl SsmDocument) -> Self {
        let doc_name = doc.document_name();
        if !doc_name.is_empty() {
            self.document_name = Some(doc_name.to_string());
        }
        self.session_type = doc.session_type();
        self.parameters = doc.parameters();
        self
    }

    /// Build a port forwarding session (convenience method)
    ///
    /// Equivalent to `.document(PortForwardingSession::new(port))`
    ///
    /// # Arguments
    ///
    /// * `port` - Remote port to forward to
    pub fn port_forward(mut self, port: u16) -> Self {
        self.session_type = SessionType::Port;
        self.document_name = Some("AWS-StartPortForwardingSession".to_string());
        self.parameters
            .insert("portNumber".to_string(), vec![port.to_string()]);
        self
    }

    /// Build and start the session
    ///
    /// This creates a SessionManager, configures it, and starts the session.
    pub async fn build(self) -> Result<Session> {
        let manager = SessionManager::new().await?;
        self.build_with(&manager).await
    }

    /// Build and start the session using an existing SessionManager
    ///
    /// Use this when you want to reuse a manager for multiple sessions.
    pub async fn build_with(self, manager: &SessionManager) -> Result<Session> {
        let config = SessionConfig {
            target: self.target,
            region: self.region,
            session_type: self.session_type,
            document_name: self.document_name,
            parameters: self.parameters,
            reason: self.reason,
            ..Default::default()
        };

        manager.start_session(config).await
    }

    /// Build the configuration without starting a session
    ///
    /// Useful if you want to reuse the same manager for multiple sessions.
    pub fn build_config(self) -> SessionConfig {
        SessionConfig {
            target: self.target,
            region: self.region,
            session_type: self.session_type,
            document_name: self.document_name,
            parameters: self.parameters,
            reason: self.reason,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let config = SessionBuilder::new("i-test")
            .region("us-west-2")
            .build_config();

        assert_eq!(config.target, "i-test");
        assert_eq!(config.region, Some("us-west-2".to_string()));
    }

    #[test]
    fn test_builder_port_forward() {
        let config = SessionBuilder::new("i-test")
            .port_forward(3389)
            .build_config();

        assert_eq!(config.session_type, SessionType::Port);
        assert_eq!(
            config.parameters.get("portNumber"),
            Some(&vec!["3389".to_string()])
        );
    }

    #[test]
    fn test_builder_with_reason() {
        let config = SessionBuilder::new("i-test")
            .reason("Security audit")
            .build_config();

        assert_eq!(config.reason, Some("Security audit".to_string()));
    }
}
