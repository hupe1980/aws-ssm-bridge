//! Type-safe AWS SSM document definitions
//!
//! This module provides strongly-typed representations of AWS SSM Session Manager
//! documents and their parameters, eliminating magic strings and enabling
//! compile-time validation.
//!
//! # AWS SSM Session Manager Documents
//!
//! AWS SSM Session Manager supports exactly **4 built-in documents**:
//!
//! | Document | Rust Type | Purpose |
//! |----------|-----------|---------|
//! | (none) | [`ShellSession`] | Standard interactive shell |
//! | `AWS-StartPortForwardingSession` | [`PortForwardingSession`] | Port forward to instance |
//! | `AWS-StartPortForwardingSessionToRemoteHost` | [`PortForwardingToRemoteHost`] | Port forward through instance |
//! | `AWS-StartInteractiveCommand` | [`InteractiveCommand`] | Execute specific commands |
//! | `AWS-StartSSHSession` | [`SshSession`] | SSH over Session Manager |
//!
//! **Note:** Non-interactive commands are handled by AWS Run Command (a different
//! service), not Session Manager. Documents like `AWS-RunShellScript` are for
//! Run Command, not sessions.
//!
//! # Design Philosophy
//!
//! Instead of using `HashMap<String, Vec<String>>` with magic keys like
//! `"portNumber"`, we define proper Rust types that encode the valid
//! parameters for each document.
//!
//! # Example
//!
//! ```rust,no_run
//! use aws_ssm_bridge::documents::{PortForwardingSession, SsmDocument};
//!
//! // Type-safe port forwarding configuration
//! let doc = PortForwardingSession::new(3306);
//!
//! // With optional local port
//! let doc = PortForwardingSession::builder()
//!     .remote_port(3306)
//!     .local_port(13306)
//!     .build()?;
//! # Ok::<(), aws_ssm_bridge::errors::Error>(())
//! ```

use crate::protocol::SessionType;
use std::collections::HashMap;

// =============================================================================
// Document Trait
// =============================================================================

/// Trait for SSM document types
///
/// Each document type implements this trait to provide:
/// - The document name
/// - The session type
/// - Parameters as a HashMap (for AWS API compatibility)
pub trait SsmDocument: Send + Sync {
    /// The AWS SSM document name (e.g., "AWS-StartPortForwardingSession")
    fn document_name(&self) -> &'static str;

    /// The session type for this document
    fn session_type(&self) -> SessionType;

    /// Convert parameters to HashMap for AWS API
    fn parameters(&self) -> HashMap<String, Vec<String>>;
}

// =============================================================================
// Port Forwarding Document
// =============================================================================

/// AWS-StartPortForwardingSession document
///
/// Forwards a remote port on an EC2 instance to a local port.
///
/// # Example
///
/// ```rust
/// use aws_ssm_bridge::documents::PortForwardingSession;
///
/// // Simple: just remote port
/// let doc = PortForwardingSession::new(3306);
///
/// // With local port specified
/// let doc = PortForwardingSession::builder()
///     .remote_port(3306)
///     .local_port(13306)
///     .build()?;
/// # Ok::<(), aws_ssm_bridge::errors::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct PortForwardingSession {
    /// Remote port on the EC2 instance
    pub remote_port: u16,
    /// Optional local port (if not specified, a random port is used)
    pub local_port: Option<u16>,
}

impl PortForwardingSession {
    /// Document name constant
    pub const DOCUMENT_NAME: &'static str = "AWS-StartPortForwardingSession";

    /// Create a new port forwarding session for the given remote port
    pub fn new(remote_port: u16) -> Self {
        Self {
            remote_port,
            local_port: None,
        }
    }

    /// Create a builder for more complex configurations
    pub fn builder() -> PortForwardingSessionBuilder {
        PortForwardingSessionBuilder::default()
    }
}

impl SsmDocument for PortForwardingSession {
    fn document_name(&self) -> &'static str {
        Self::DOCUMENT_NAME
    }

    fn session_type(&self) -> SessionType {
        SessionType::Port
    }

    fn parameters(&self) -> HashMap<String, Vec<String>> {
        let mut params = HashMap::new();
        params.insert("portNumber".to_string(), vec![self.remote_port.to_string()]);
        if let Some(local) = self.local_port {
            params.insert("localPortNumber".to_string(), vec![local.to_string()]);
        }
        params
    }
}

/// Builder for PortForwardingSession
#[derive(Debug, Default)]
pub struct PortForwardingSessionBuilder {
    remote_port: Option<u16>,
    local_port: Option<u16>,
}

impl PortForwardingSessionBuilder {
    /// Set the remote port (required)
    pub fn remote_port(mut self, port: u16) -> Self {
        self.remote_port = Some(port);
        self
    }

    /// Set the local port (optional)
    pub fn local_port(mut self, port: u16) -> Self {
        self.local_port = Some(port);
        self
    }

    /// Build the document configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if `remote_port` was not set.
    pub fn build(self) -> crate::errors::Result<PortForwardingSession> {
        let remote_port = self.remote_port.ok_or_else(|| {
            crate::errors::Error::Config("remote_port is required for PortForwardingSession".into())
        })?;
        Ok(PortForwardingSession {
            remote_port,
            local_port: self.local_port,
        })
    }
}

// =============================================================================
// Port Forwarding to Remote Host Document
// =============================================================================

/// AWS-StartPortForwardingSessionToRemoteHost document
///
/// Forwards a port through an EC2 instance to a remote host (e.g., RDS).
///
/// # Example
///
/// ```rust
/// use aws_ssm_bridge::documents::PortForwardingToRemoteHost;
///
/// // Forward to an RDS instance through a bastion
/// let doc = PortForwardingToRemoteHost::new("mydb.cluster-xxx.us-east-1.rds.amazonaws.com", 3306);
/// ```
#[derive(Debug, Clone)]
pub struct PortForwardingToRemoteHost {
    /// Remote host to connect to (hostname or IP)
    pub host: String,
    /// Port on the remote host
    pub remote_port: u16,
    /// Optional local port
    pub local_port: Option<u16>,
}

impl PortForwardingToRemoteHost {
    /// Document name constant
    pub const DOCUMENT_NAME: &'static str = "AWS-StartPortForwardingSessionToRemoteHost";

    /// Create a new port forwarding session to a remote host
    pub fn new(host: impl Into<String>, remote_port: u16) -> Self {
        Self {
            host: host.into(),
            remote_port,
            local_port: None,
        }
    }

    /// Set the local port
    pub fn with_local_port(mut self, port: u16) -> Self {
        self.local_port = Some(port);
        self
    }
}

impl SsmDocument for PortForwardingToRemoteHost {
    fn document_name(&self) -> &'static str {
        Self::DOCUMENT_NAME
    }

    fn session_type(&self) -> SessionType {
        SessionType::Port
    }

    fn parameters(&self) -> HashMap<String, Vec<String>> {
        let mut params = HashMap::new();
        params.insert("host".to_string(), vec![self.host.clone()]);
        params.insert("portNumber".to_string(), vec![self.remote_port.to_string()]);
        if let Some(local) = self.local_port {
            params.insert("localPortNumber".to_string(), vec![local.to_string()]);
        }
        params
    }
}

// =============================================================================
// Interactive Shell Document
// =============================================================================

/// Standard interactive shell session (no custom document)
///
/// This is the default session type - a simple shell session.
#[derive(Debug, Clone, Default)]
pub struct ShellSession;

impl ShellSession {
    /// Create a new shell session
    pub fn new() -> Self {
        Self
    }
}

impl SsmDocument for ShellSession {
    fn document_name(&self) -> &'static str {
        // Shell sessions don't require a document name
        ""
    }

    fn session_type(&self) -> SessionType {
        SessionType::StandardStream
    }

    fn parameters(&self) -> HashMap<String, Vec<String>> {
        HashMap::new()
    }
}

// =============================================================================
// Interactive Command Document
// =============================================================================

/// AWS-StartInteractiveCommand document
///
/// Execute commands in an interactive shell session with specified commands.
/// Unlike a plain shell session, this runs specific commands.
///
/// # Example
///
/// ```rust
/// use aws_ssm_bridge::documents::InteractiveCommand;
///
/// // Run a single command
/// let doc = InteractiveCommand::new("top");
///
/// // Run multiple commands
/// let doc = InteractiveCommand::with_commands(vec!["cd /var/log".into(), "tail -f syslog".into()]);
/// ```
#[derive(Debug, Clone)]
pub struct InteractiveCommand {
    /// Commands to execute
    pub commands: Vec<String>,
}

impl InteractiveCommand {
    /// Document name constant
    pub const DOCUMENT_NAME: &'static str = "AWS-StartInteractiveCommand";

    /// Create with a single command
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            commands: vec![command.into()],
        }
    }

    /// Create with multiple commands
    pub fn with_commands(commands: Vec<String>) -> Self {
        Self { commands }
    }
}

impl SsmDocument for InteractiveCommand {
    fn document_name(&self) -> &'static str {
        Self::DOCUMENT_NAME
    }

    fn session_type(&self) -> SessionType {
        SessionType::InteractiveCommands
    }

    fn parameters(&self) -> HashMap<String, Vec<String>> {
        let mut params = HashMap::new();
        params.insert("command".to_string(), self.commands.clone());
        params
    }
}

// =============================================================================
// SSH Session Document
// =============================================================================

/// AWS-StartSSHSession document
///
/// Start an SSH session through SSM (SSH over Session Manager).
/// This is used by `aws ssm start-session --document-name AWS-StartSSHSession`.
///
/// # Example
///
/// ```rust
/// use aws_ssm_bridge::documents::SshSession;
///
/// // Default SSH port (22)
/// let doc = SshSession::new();
///
/// // Custom SSH port
/// let doc = SshSession::with_port(2222);
/// ```
#[derive(Debug, Clone)]
pub struct SshSession {
    /// SSH port on the remote instance (default: 22)
    pub port: u16,
}

impl SshSession {
    /// Document name constant
    pub const DOCUMENT_NAME: &'static str = "AWS-StartSSHSession";

    /// Create a new SSH session with default port (22)
    pub fn new() -> Self {
        Self { port: 22 }
    }

    /// Create a new SSH session with custom port
    pub fn with_port(port: u16) -> Self {
        Self { port }
    }
}

impl Default for SshSession {
    fn default() -> Self {
        Self::new()
    }
}

impl SsmDocument for SshSession {
    fn document_name(&self) -> &'static str {
        Self::DOCUMENT_NAME
    }

    fn session_type(&self) -> SessionType {
        SessionType::Port
    }

    fn parameters(&self) -> HashMap<String, Vec<String>> {
        let mut params = HashMap::new();
        params.insert("portNumber".to_string(), vec![self.port.to_string()]);
        params
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_forwarding_simple() {
        let doc = PortForwardingSession::new(3306);
        assert_eq!(doc.document_name(), "AWS-StartPortForwardingSession");
        assert_eq!(doc.session_type(), SessionType::Port);

        let params = doc.parameters();
        assert_eq!(params.get("portNumber"), Some(&vec!["3306".to_string()]));
        assert!(!params.contains_key("localPortNumber"));
    }

    #[test]
    fn test_port_forwarding_builder() {
        let doc = PortForwardingSession::builder()
            .remote_port(3306)
            .local_port(13306)
            .build()
            .unwrap();

        let params = doc.parameters();
        assert_eq!(params.get("portNumber"), Some(&vec!["3306".to_string()]));
        assert_eq!(
            params.get("localPortNumber"),
            Some(&vec!["13306".to_string()])
        );
    }

    #[test]
    fn test_port_forwarding_to_remote_host() {
        let doc =
            PortForwardingToRemoteHost::new("mydb.rds.amazonaws.com", 5432).with_local_port(15432);

        assert_eq!(
            doc.document_name(),
            "AWS-StartPortForwardingSessionToRemoteHost"
        );

        let params = doc.parameters();
        assert_eq!(
            params.get("host"),
            Some(&vec!["mydb.rds.amazonaws.com".to_string()])
        );
        assert_eq!(params.get("portNumber"), Some(&vec!["5432".to_string()]));
        assert_eq!(
            params.get("localPortNumber"),
            Some(&vec!["15432".to_string()])
        );
    }

    #[test]
    fn test_shell_session() {
        let doc = ShellSession::new();
        assert_eq!(doc.session_type(), SessionType::StandardStream);
        assert!(doc.parameters().is_empty());
    }

    #[test]
    fn test_interactive_command() {
        let doc = InteractiveCommand::new("top");
        assert_eq!(doc.document_name(), "AWS-StartInteractiveCommand");
        assert_eq!(doc.session_type(), SessionType::InteractiveCommands);

        let params = doc.parameters();
        assert_eq!(params.get("command"), Some(&vec!["top".to_string()]));
    }

    #[test]
    fn test_ssh_session() {
        let doc = SshSession::new();
        assert_eq!(doc.document_name(), "AWS-StartSSHSession");
        assert_eq!(doc.session_type(), SessionType::Port);

        let params = doc.parameters();
        assert_eq!(params.get("portNumber"), Some(&vec!["22".to_string()]));

        // Custom port
        let doc = SshSession::with_port(2222);
        let params = doc.parameters();
        assert_eq!(params.get("portNumber"), Some(&vec!["2222".to_string()]));
    }
}
