//! A Rust implementation of the AWS Systems Manager Session Manager protocol.
//!
//! The official [`session-manager-plugin`] is a CLI binary. This is a library:
//! open sessions, stream bytes, and forward ports from inside your own async
//! application, with no subprocess and no plugin to install.
//!
//! ```no_run
//! use aws_ssm_bridge::SessionBuilder;
//! use futures_util::StreamExt;
//!
//! # async fn example() -> aws_ssm_bridge::Result<()> {
//! let session = SessionBuilder::new("i-0123456789abcdef0").start().await?;
//! session.wait_ready().await?;
//!
//! let mut output = session.output();
//! session.send(&b"uname -a\r"[..]).await?;
//!
//! while let Some(chunk) = output.next().await {
//!     print!("{}", String::from_utf8_lossy(&chunk));
//! }
//! session.terminate().await?;
//! # Ok(()) }
//! ```
//!
//! # What you get
//!
//! | Capability | Where |
//! |---|---|
//! | Shell and command sessions | [`Session`], [`documents`] |
//! | TCP port forwarding (smux-multiplexed) | [`PortForwarder`] |
//! | End-to-end KMS session encryption | [`crypto`] (feature `kms`) |
//! | Interactive terminal | [`InteractiveShell`] (feature `interactive`) |
//! | Automatic reconnection | [`ReconnectingSession`] |
//! | Many concurrent sessions | [`SessionPool`] |
//! | Metrics hooks | [`metrics`] |
//!
//! # Session lifetime
//!
//! A [`Session`] is either running or closed, and every way it can end —
//! [`terminate`](Session::terminate), the agent hanging up, a dead network, a
//! protocol violation — resolves [`Session::closed`] and records a
//! [`CloseReason`]. Build on that signal rather than polling:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(session: Arc<aws_ssm_bridge::Session>) {
//! tokio::select! {
//!     () = session.closed() => {
//!         eprintln!("session ended: {}", session.close_reason().unwrap());
//!     }
//!     _ = do_work(&session) => {}
//! }
//! # }
//! # async fn do_work(_: &aws_ssm_bridge::Session) {}
//! ```
//!
//! # Ordering and delivery
//!
//! The protocol layer handles sequencing, acknowledgement, retransmission and
//! reordering, so [`Session::send`] and [`Session::output`] behave like an
//! ordered, lossless byte stream. Message boundaries are an artefact of chunking
//! and carry no meaning.
//!
//! # Feature flags
//!
//! | Feature | Default | Effect |
//! |---|---|---|
//! | `interactive` | yes | [`terminal`] and [`InteractiveShell`]; pulls in `crossterm` |
//! | `kms` | yes | KMS session encryption; pulls in `aws-sdk-kms` and `aes-gcm` |
//! | `python` | no | PyO3 bindings |
//! | `extension-module` | no | Link the bindings as a Python extension module; set by `maturin` |
//!
//! Without `kms`, a session whose account requires encrypted sessions fails the
//! handshake with an explicit error rather than downgrading to plaintext.
//!
//! `extension-module` is separate from `python` because it leaves the CPython
//! symbols for the interpreter to resolve at load time: correct for a wheel,
//! fatal for a test binary. `--all-features` therefore does not link; name the
//! features you want.
//!
//! # Not affiliated with AWS
//!
//! This is an independent implementation, not endorsed by or sponsored by
//! Amazon Web Services, Inc.
//!
//! [`session-manager-plugin`]: https://github.com/aws/session-manager-plugin

// ---------------------------------------------------------------------------
// Modules
// ---------------------------------------------------------------------------

pub mod ack;
pub mod binary_protocol;
pub mod builder;
pub mod crypto;
pub mod documents;
pub mod errors;
pub mod handshake;
pub mod metrics;
pub mod mux;
pub mod pool;
pub mod port_forward;
pub mod reconnect;
pub mod session;
pub mod shutdown;

#[cfg(feature = "interactive")]
pub mod interactive;
#[cfg(feature = "interactive")]
pub mod terminal;

mod channels;
mod connection;

/// PyO3 bindings.
#[cfg(feature = "python")]
pub mod python;

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

pub use builder::SessionBuilder;
pub use channels::OutputStream;
pub use connection::EndpointPolicy;
pub use documents::{SessionType, SsmDocument};
pub use errors::{Error, Result};
pub use metrics::MetricsRecorder;
pub use mux::{SmuxConfig, SmuxSession, SmuxStream};
pub use pool::{PoolConfig, PoolStats, SessionPool};
pub use port_forward::{PortForwardConfig, PortForwarder};
pub use reconnect::{ReconnectConfig, ReconnectEvent, ReconnectingSession};
pub use session::{CloseReason, DocumentSpec, Session, SessionConfig, SessionManager};
pub use shutdown::{install_signal_handlers, ShutdownSignal};

#[cfg(feature = "interactive")]
pub use interactive::{InteractiveConfig, InteractiveShell};
#[cfg(feature = "interactive")]
pub use terminal::TerminalSize;

/// The version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
