//! A complete interactive shell, terminal and all.
//!
//! ```no_run
//! use aws_ssm_bridge::InteractiveShell;
//!
//! # async fn example() -> aws_ssm_bridge::Result<()> {
//! let exit_code = InteractiveShell::new(Default::default())
//!     .run("i-0123456789abcdef0")
//!     .await?;
//! std::process::exit(exit_code.unwrap_or(0));
//! # }
//! ```
//!
//! Everything the local terminal produces is forwarded verbatim and everything
//! the agent sends is written straight to stdout — see [`crate::terminal`] for
//! why that is the only correct approach. Resizes propagate through SIGWINCH,
//! and the terminal is restored on every exit path, panics included.

use futures_util::StreamExt;
use std::sync::Arc;
use tracing::{debug, info};

use crate::builder::SessionBuilder;
use crate::errors::Result;
use crate::session::{CloseReason, Session, SessionManager};
use crate::terminal::{self, RawModeGuard, TerminalEvent, TerminalReader, TerminalSize};

/// How the interactive shell presents itself.
#[derive(Debug, Clone)]
pub struct InteractiveConfig {
    /// Print "Starting session …" and "Session ended" around the session.
    pub show_banner: bool,
    /// Send the terminal size once the session is ready.
    ///
    /// Without this the remote pty stays at its default 80×24 and full-screen
    /// programs draw at the wrong size.
    pub send_initial_size: bool,
    /// AWS region; `None` uses the ambient configuration.
    pub region: Option<String>,
    /// Reason recorded in CloudTrail.
    pub reason: Option<String>,
}

impl Default for InteractiveConfig {
    fn default() -> Self {
        Self {
            show_banner: true,
            send_initial_size: true,
            region: None,
            reason: None,
        }
    }
}

/// Runs a session against the local terminal.
#[derive(Debug, Default)]
pub struct InteractiveShell {
    config: InteractiveConfig,
}

impl InteractiveShell {
    /// Build a shell with the given presentation options.
    pub fn new(config: InteractiveConfig) -> Self {
        Self { config }
    }

    /// Start a session against `target` and run it until it ends.
    ///
    /// Returns the remote process's exit code when the agent reported one.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Config`] if stdin and stdout are not a terminal;
    /// raw mode is meaningless on a pipe. Use [`Session::send`] and
    /// [`Session::output`] for headless use.
    pub async fn run(&self, target: &str) -> Result<Option<i32>> {
        terminal::require_terminal()?;

        let mut builder = SessionBuilder::new(target).maybe_region(self.config.region.clone());
        if let Some(reason) = &self.config.reason {
            builder = builder.reason(reason.clone());
        }
        let session = Arc::new(builder.start().await?);
        self.drive(session).await
    }

    /// Start a session using an existing manager, then run it.
    pub async fn run_with(&self, target: &str, manager: &SessionManager) -> Result<Option<i32>> {
        terminal::require_terminal()?;
        let session = Arc::new(SessionBuilder::new(target).start_with(manager).await?);
        self.drive(session).await
    }

    /// Attach the local terminal to an already-open session.
    ///
    /// The session is terminated when the shell exits.
    pub async fn attach(&self, session: Arc<Session>) -> Result<Option<i32>> {
        terminal::require_terminal()?;
        self.drive(session).await
    }

    async fn drive(&self, session: Arc<Session>) -> Result<Option<i32>> {
        if self.config.show_banner {
            println!("Starting session with SessionId: {}", session.id());
        }

        session.wait_ready().await?;
        if let Some(banner) = session.banner() {
            println!("{banner}");
        }

        // Raw mode goes on last, and comes off first: every early return above
        // this point leaves the terminal untouched, and the guard restores it on
        // every path below, panics included.
        let _raw = RawModeGuard::enter()?;

        if self.config.send_initial_size {
            let size = TerminalSize::current();
            session.send_terminal_size(size.cols, size.rows).await?;
        }

        let result = self.pump(&session).await;

        // Leave the cursor at column zero. In raw mode `\n` is a line feed only,
        // so without the carriage return the shell prompt resumes mid-line.
        let _ = terminal::write_output(b"\r\n");
        drop(_raw);

        let reason = session.close_reason();
        let exit_code = session.exit_code();
        session.terminate().await?;

        if self.config.show_banner {
            match &reason {
                Some(CloseReason::Terminated) | None => println!("Session ended."),
                Some(reason) => println!("Session ended: {reason}"),
            }
        }

        result.map(|()| exit_code)
    }

    /// Shuttle bytes between the terminal and the session until either stops.
    async fn pump(&self, session: &Session) -> Result<()> {
        let mut terminal_input = TerminalReader::start();
        let mut output = session.output();

        loop {
            tokio::select! {
                // Session output first: draining what the remote already sent
                // keeps the display current even while the user types fast.
                biased;

                chunk = output.next() => match chunk {
                    Some(chunk) => terminal::write_output(&chunk)?,
                    None => {
                        debug!("session output ended");
                        return Ok(());
                    }
                },

                event = terminal_input.next() => match event {
                    Some(TerminalEvent::Input(bytes)) => session.send(bytes).await?,
                    Some(TerminalEvent::Resize(size)) => {
                        session.send_terminal_size(size.cols, size.rows).await?;
                    }
                    // Ctrl-D at the shell prompt arrives as a 0x04 byte and is
                    // forwarded like any other input; the remote shell decides
                    // what it means. Genuine stdin EOF ends the session.
                    Some(TerminalEvent::Eof) | None => {
                        info!("terminal input ended");
                        return Ok(());
                    }
                },

                () = session.closed() => {
                    debug!(reason = ?session.close_reason(), "session ended");
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_show_a_banner_and_send_the_size() {
        let config = InteractiveConfig::default();
        assert!(config.show_banner);
        assert!(config.send_initial_size);
        assert!(config.region.is_none());
    }

    /// Raw mode on a pipe is meaningless and `enable_raw_mode` would fail deep
    /// inside the run. Fail early with an explanation instead.
    #[tokio::test]
    async fn running_without_a_terminal_is_rejected_before_any_aws_call() {
        if terminal::is_terminal() {
            return; // running attached to a real terminal; nothing to assert
        }
        let err = InteractiveShell::default()
            .run("i-0123456789abcdef0")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("terminal"), "{err}");
    }
}
