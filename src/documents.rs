//! Typed wrappers for the AWS-managed Session Manager documents.
//!
//! `StartSession` takes a document name and a `HashMap<String, Vec<String>>` of
//! parameters. Getting a key wrong there fails at runtime, in the API response,
//! after the round trip. These types encode each document's real parameters so
//! the compiler catches the mistake instead.
//!
//! | Document | Type | What it does |
//! |----------|------|--------------|
//! | *(none)* | [`ShellSession`] | Interactive shell — the default |
//! | `AWS-StartPortForwardingSession` | [`PortForwardingSession`] | Forward a port on the instance itself |
//! | `AWS-StartPortForwardingSessionToRemoteHost` | [`PortForwardingToRemoteHost`] | Forward through the instance to another host |
//! | `AWS-StartSSHSession` | [`SshSession`] | SSH transport for `ProxyCommand` |
//! | `AWS-StartInteractiveCommand` | [`InteractiveCommand`] | Run a command with a pty attached |
//! | `AWS-StartNonInteractiveCommand` | [`NonInteractiveCommand`] | Run a command without a pty |
//!
//! Non-interactive *fleet* automation (`AWS-RunShellScript` and friends) belongs
//! to Run Command, a different service; those documents will not start a session.
//!
//! # Session types and port forwarding
//!
//! Only the two port-forwarding documents produce a [`SessionType::Port`]
//! session, and only those multiplex TCP connections over smux — which is why
//! [`PortForwarder`] accepts them and nothing else. `AWS-StartSSHSession` looks
//! like port forwarding but is a plain bidirectional byte stream: SSH speaks its
//! own protocol over the session's stdin and stdout. Treating it as a
//! multiplexed forward would frame SSH's handshake as smux frames and hang.
//!
//! [`PortForwarder`]: crate::PortForwarder

use std::collections::HashMap;

/// What a session's byte stream means, and therefore how to drive it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SessionType {
    /// A bidirectional byte stream: shell, SSH transport, or a command's stdio.
    #[default]
    StandardStream,
    /// TCP connections multiplexed over smux; drive with [`PortForwarder`].
    ///
    /// [`PortForwarder`]: crate::PortForwarder
    Port,
}

/// An SSM document that can start a session.
pub trait SsmDocument {
    /// The document name passed to `StartSession`.
    ///
    /// Empty means "no document", which starts a default shell session.
    fn document_name(&self) -> &'static str;

    /// The kind of session this document produces.
    fn session_type(&self) -> SessionType;

    /// Document parameters in `StartSession` form.
    fn parameters(&self) -> HashMap<String, Vec<String>>;
}

fn params<const N: usize>(pairs: [(&str, String); N]) -> HashMap<String, Vec<String>> {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_owned(), vec![v]))
        .collect()
}

// ---------------------------------------------------------------------------
// Shell
// ---------------------------------------------------------------------------

/// A plain interactive shell — what `aws ssm start-session` gives you with no
/// `--document-name`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShellSession;

impl ShellSession {
    /// Create a shell session document.
    pub const fn new() -> Self {
        Self
    }
}

impl SsmDocument for ShellSession {
    fn document_name(&self) -> &'static str {
        ""
    }
    fn session_type(&self) -> SessionType {
        SessionType::StandardStream
    }
    fn parameters(&self) -> HashMap<String, Vec<String>> {
        HashMap::new()
    }
}

// ---------------------------------------------------------------------------
// Port forwarding
// ---------------------------------------------------------------------------

/// `AWS-StartPortForwardingSession` — reach a port on the instance itself.
///
/// ```
/// use aws_ssm_bridge::documents::{PortForwardingSession, SsmDocument, SessionType};
///
/// let doc = PortForwardingSession::new(3306);
/// assert_eq!(doc.session_type(), SessionType::Port);
/// assert_eq!(doc.parameters()["portNumber"], vec!["3306".to_string()]);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortForwardingSession {
    /// Port to connect to on the instance.
    pub remote_port: u16,
}

impl PortForwardingSession {
    /// The AWS document name.
    pub const DOCUMENT_NAME: &'static str = "AWS-StartPortForwardingSession";

    /// Forward to `remote_port` on the target instance.
    pub const fn new(remote_port: u16) -> Self {
        Self { remote_port }
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
        params([("portNumber", self.remote_port.to_string())])
    }
}

/// `AWS-StartPortForwardingSessionToRemoteHost` — reach a third host through the
/// instance, for example an RDS endpoint from a bastion.
///
/// ```
/// use aws_ssm_bridge::documents::{PortForwardingToRemoteHost, SsmDocument};
///
/// let doc = PortForwardingToRemoteHost::new("db.cluster-abc.eu-central-1.rds.amazonaws.com", 5432);
/// assert_eq!(doc.parameters()["portNumber"], vec!["5432".to_string()]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForwardingToRemoteHost {
    /// Hostname or IP the instance should connect to.
    pub host: String,
    /// Port on that host.
    pub remote_port: u16,
}

impl PortForwardingToRemoteHost {
    /// The AWS document name.
    pub const DOCUMENT_NAME: &'static str = "AWS-StartPortForwardingSessionToRemoteHost";

    /// Forward through the target instance to `host:remote_port`.
    pub fn new(host: impl Into<String>, remote_port: u16) -> Self {
        Self {
            host: host.into(),
            remote_port,
        }
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
        params([
            ("host", self.host.clone()),
            ("portNumber", self.remote_port.to_string()),
        ])
    }
}

// ---------------------------------------------------------------------------
// SSH
// ---------------------------------------------------------------------------

/// `AWS-StartSSHSession` — the transport `ssh -o ProxyCommand` uses.
///
/// This is a **byte stream**, not a multiplexed port forward: pipe
/// [`Session::output`] to your SSH client's stdin and its stdout to
/// [`Session::send`]. See the module documentation for why the distinction
/// matters.
///
/// [`Session::output`]: crate::Session::output
/// [`Session::send`]: crate::Session::send
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SshSession {
    /// SSH port on the instance.
    pub port: u16,
}

impl SshSession {
    /// The AWS document name.
    pub const DOCUMENT_NAME: &'static str = "AWS-StartSSHSession";

    /// SSH on the standard port 22.
    pub const fn new() -> Self {
        Self { port: 22 }
    }

    /// SSH on a non-standard port.
    pub const fn on_port(port: u16) -> Self {
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
        // Deliberately not `Port`: the agent streams SSH's own protocol here
        // rather than smux frames.
        SessionType::StandardStream
    }
    fn parameters(&self) -> HashMap<String, Vec<String>> {
        params([("portNumber", self.port.to_string())])
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// `AWS-StartInteractiveCommand` — run a command with a pty attached.
///
/// Use this for anything that draws to the terminal or reads input (`top`,
/// `less`, a REPL). For a command whose output you only want to collect, prefer
/// [`NonInteractiveCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractiveCommand {
    /// Command line to run.
    pub command: String,
}

impl InteractiveCommand {
    /// The AWS document name.
    pub const DOCUMENT_NAME: &'static str = "AWS-StartInteractiveCommand";

    /// Run `command` interactively.
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }
}

impl SsmDocument for InteractiveCommand {
    fn document_name(&self) -> &'static str {
        Self::DOCUMENT_NAME
    }
    fn session_type(&self) -> SessionType {
        SessionType::StandardStream
    }
    fn parameters(&self) -> HashMap<String, Vec<String>> {
        params([("command", self.command.clone())])
    }
}

/// `AWS-StartNonInteractiveCommand` — run a command with no pty.
///
/// The command's output streams back and the session ends when it exits; read
/// the status from [`Session::exit_code`].
///
/// [`Session::exit_code`]: crate::Session::exit_code
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonInteractiveCommand {
    /// Command line to run.
    pub command: String,
}

impl NonInteractiveCommand {
    /// The AWS document name.
    pub const DOCUMENT_NAME: &'static str = "AWS-StartNonInteractiveCommand";

    /// Run `command` and stream its output.
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }
}

impl SsmDocument for NonInteractiveCommand {
    fn document_name(&self) -> &'static str {
        Self::DOCUMENT_NAME
    }
    fn session_type(&self) -> SessionType {
        SessionType::StandardStream
    }
    fn parameters(&self) -> HashMap<String, Vec<String>> {
        params([("command", self.command.clone())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_sessions_carry_no_document() {
        let doc = ShellSession::new();
        assert!(doc.document_name().is_empty());
        assert!(doc.parameters().is_empty());
        assert_eq!(doc.session_type(), SessionType::StandardStream);
    }

    #[test]
    fn port_forwarding_uses_the_documented_parameter_names() {
        let doc = PortForwardingSession::new(3389);
        assert_eq!(doc.document_name(), "AWS-StartPortForwardingSession");
        assert_eq!(doc.parameters()["portNumber"], vec!["3389".to_string()]);
        assert_eq!(doc.session_type(), SessionType::Port);
    }

    #[test]
    fn remote_host_forwarding_sends_host_and_port() {
        let doc = PortForwardingToRemoteHost::new("db.internal", 5432);
        let p = doc.parameters();
        assert_eq!(
            doc.document_name(),
            "AWS-StartPortForwardingSessionToRemoteHost"
        );
        assert_eq!(p["host"], vec!["db.internal".to_string()]);
        assert_eq!(p["portNumber"], vec!["5432".to_string()]);
        assert_eq!(doc.session_type(), SessionType::Port);
    }

    /// The agent only speaks smux for documents whose session properties set
    /// `type: LocalPortForwarding`. `AWS-StartSSHSession` does not, so
    /// classifying it as `Port` would send SSH's handshake into the smux frame
    /// parser and hang the session.
    #[test]
    fn ssh_is_a_byte_stream_not_a_multiplexed_forward() {
        assert_eq!(
            SshSession::new().session_type(),
            SessionType::StandardStream
        );
        assert_eq!(
            SshSession::new().parameters()["portNumber"],
            vec!["22".to_string()]
        );
        assert_eq!(
            SshSession::on_port(2222).parameters()["portNumber"],
            vec!["2222".to_string()]
        );
    }

    #[test]
    fn command_documents_pass_the_command_through() {
        assert_eq!(
            InteractiveCommand::new("top -b").parameters()["command"],
            vec!["top -b".to_string()]
        );
        assert_eq!(
            NonInteractiveCommand::new("uname -a").parameters()["command"],
            vec!["uname -a".to_string()]
        );
        assert_eq!(
            NonInteractiveCommand::DOCUMENT_NAME,
            "AWS-StartNonInteractiveCommand"
        );
    }

    /// Only the two forwarding documents may be handed to `PortForwarder`.
    #[test]
    fn exactly_two_documents_are_port_sessions() {
        let port_types = [
            PortForwardingSession::new(1).session_type(),
            PortForwardingToRemoteHost::new("h", 1).session_type(),
        ];
        assert!(port_types.iter().all(|t| *t == SessionType::Port));

        let stream_types = [
            ShellSession::new().session_type(),
            SshSession::new().session_type(),
            InteractiveCommand::new("x").session_type(),
            NonInteractiveCommand::new("x").session_type(),
        ];
        assert!(stream_types
            .iter()
            .all(|t| *t == SessionType::StandardStream));
    }
}
