//! Interactive shell session combining terminal handling with SSM protocol.
//!
//! This module provides a complete interactive shell experience that goes
//! BEYOND what AWS session-manager-plugin offers:
//!
//! ## Improvements Over AWS
//!
//! | Feature | AWS Plugin | aws-ssm-bridge |
//! |---------|-----------|----------------|
//! | Terminal handling | OS-specific exec | crossterm (unified) |
//! | Resize detection | 500ms polling | Instant via events |
//! | Input batching | None | Smart batching |
//! | Error recovery | Basic | Automatic retry |
//! | State restoration | Manual cleanup | RAII guaranteed |
//!
//! ## Usage
//!
//! ```rust,no_run
//! use aws_ssm_bridge::interactive::{InteractiveShell, InteractiveConfig};
//! use aws_ssm_bridge::SessionConfig;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = InteractiveConfig::default();
//! let mut shell = InteractiveShell::new(config)?;
//!
//! // Connect to instance
//! shell.connect("i-1234567890abcdef0").await?;
//!
//! // Run interactive session (blocks until exit)
//! shell.run().await?;
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use futures::StreamExt;
use tracing::{debug, info, instrument, trace};

use crate::binary_protocol::{ClientMessage, PayloadType};
use crate::errors::{Error, Result};
use crate::protocol::MessageType;
use crate::session::{Session, SessionConfig, SessionState};
use crate::terminal::{ControlSignal, Terminal, TerminalConfig, TerminalInput};
use crate::SessionManager;

/// Configuration for interactive shell sessions
#[derive(Debug, Clone)]
pub struct InteractiveConfig {
    /// Terminal configuration
    pub terminal: TerminalConfig,
    /// Send initial terminal size on connect
    pub send_initial_size: bool,
    /// Display banner on connect
    pub show_banner: bool,
    /// Forward Ctrl+C as signal (true) or bytes (false)
    pub forward_signals: bool,
}

impl Default for InteractiveConfig {
    fn default() -> Self {
        Self {
            terminal: TerminalConfig::default(),
            send_initial_size: true,
            show_banner: true,
            forward_signals: true,
        }
    }
}

/// Interactive shell session with full terminal support
pub struct InteractiveShell {
    config: InteractiveConfig,
    terminal: Terminal,
    session: Option<Session>,
    session_manager: Option<SessionManager>,
}

impl InteractiveShell {
    /// Create a new interactive shell
    pub fn new(config: InteractiveConfig) -> Result<Self> {
        let terminal = Terminal::new(config.terminal.clone())?;
        Ok(Self {
            config,
            terminal,
            session: None,
            session_manager: None,
        })
    }

    /// Connect to an EC2 instance
    #[instrument(skip(self))]
    pub async fn connect(&mut self, target: &str) -> Result<()> {
        info!(target = %target, "Connecting to instance");

        // Create session manager
        let manager = SessionManager::new().await?;

        // Create session config
        let session_config = SessionConfig {
            target: target.to_string(),
            ..Default::default()
        };

        // Start session
        let session = manager.start_session(session_config).await?;

        if self.config.show_banner {
            println!(
                "\n\x1b[32mStarting session with SessionId: {}\x1b[0m\n",
                session.id()
            );
        }

        self.session = Some(session);
        self.session_manager = Some(manager);

        // Send initial terminal size
        if self.config.send_initial_size {
            self.send_terminal_size().await?;
        }

        Ok(())
    }

    /// Send terminal size to session
    async fn send_size_to_session(session: &Session, terminal: &Terminal) -> Result<()> {
        let size = terminal.size();
        let payload = size.to_json()?;

        debug!(cols = size.cols, rows = size.rows, "Sending terminal size");

        // Create size message with PayloadType::Size
        let message = ClientMessage::new(
            MessageType::InputStreamData,
            0, // Sequence number managed by session
            PayloadType::Size,
            payload,
        );

        session.send(message.serialize()?).await
    }

    /// Send current terminal size to remote
    async fn send_terminal_size(&self) -> Result<()> {
        if let Some(ref session) = self.session {
            Self::send_size_to_session(session, &self.terminal).await?;
        }
        Ok(())
    }

    /// Run the interactive session (blocks until exit)
    #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        // Take session out for the duration of run()
        let mut session = self
            .session
            .take()
            .ok_or_else(|| Error::Config("Not connected".to_string()))?;

        // Enable raw mode with RAII guard
        let _raw_guard = self.terminal.enable_raw_mode()?;

        // Start terminal input reader
        let mut input_rx = self.terminal.start_input_reader();

        // Get output stream from session
        let mut output = session.output();

        // Main event loop
        let result = loop {
            tokio::select! {
                // Handle terminal input
                input = input_rx.recv() => {
                    match input {
                        Some(TerminalInput::Data(data)) => {
                            trace!(len = data.len(), "Sending input data");
                            if let Err(e) = session.send(data).await {
                                break Err(e);
                            }
                        }
                        Some(TerminalInput::Signal(signal)) => {
                            if self.config.forward_signals {
                                let byte = signal.as_byte();
                                debug!(?signal, byte, "Forwarding control signal");
                                if let Err(e) = session.send(Bytes::from(vec![byte])).await {
                                    break Err(e);
                                }
                            }

                            // Handle Ctrl+D as EOF
                            if matches!(signal, ControlSignal::EndOfFile) {
                                info!("EOF received, terminating session");
                                break Ok(());
                            }
                        }
                        Some(TerminalInput::Resize(size)) => {
                            debug!(cols = size.cols, rows = size.rows, "Terminal resized");
                            if let Err(e) = Self::send_size_to_session(&session, &self.terminal).await {
                                break Err(e);
                            }
                        }
                        Some(TerminalInput::Eof) | None => {
                            info!("Terminal input closed");
                            break Ok(());
                        }
                    }
                }

                // Handle session output
                output_data = output.next() => {
                    match output_data {
                        Some(data) => {
                            trace!(len = data.len(), "Received output data");
                            if let Err(e) = Terminal::write_output(&data) {
                                break Err(Error::Io(e));
                            }
                        }
                        None => {
                            info!("Session output closed");
                            break Ok(());
                        }
                    }
                }
            }
        };

        // Graceful shutdown
        self.terminal.stop();
        session.terminate().await?;

        if self.config.show_banner {
            println!("\n\x1b[33mSession terminated.\x1b[0m\n");
        }

        result
    }

    /// Get the underlying session (if connected)
    pub fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    /// Check if connected (async to check state)
    pub async fn is_connected(&self) -> bool {
        if let Some(ref session) = self.session {
            session.state().await == SessionState::Connected
        } else {
            false
        }
    }
}

impl Drop for InteractiveShell {
    fn drop(&mut self) {
        self.terminal.stop();
    }
}

/// Convenience function for quick interactive session
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// aws_ssm_bridge::interactive::run_shell("i-1234567890abcdef0").await?;
/// # Ok(())
/// # }
/// ```
pub async fn run_shell(target: &str) -> Result<()> {
    let config = InteractiveConfig::default();
    let mut shell = InteractiveShell::new(config)?;
    shell.connect(target).await?;
    shell.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interactive_config_default() {
        let config = InteractiveConfig::default();
        assert!(config.send_initial_size);
        assert!(config.show_banner);
        assert!(config.forward_signals);
    }

    #[test]
    fn test_interactive_shell_creation() {
        let config = InteractiveConfig::default();
        let shell = InteractiveShell::new(config);
        assert!(shell.is_ok());
    }

    #[tokio::test]
    async fn test_not_connected_initially() {
        let config = InteractiveConfig::default();
        let shell = InteractiveShell::new(config).unwrap();
        assert!(!shell.is_connected().await);
    }
}
