//! # AWS SSM Bridge
//!
//! A high-performance Rust library implementing the AWS Systems Manager (SSM)
//! Session Manager protocol with first-class Python bindings.
//!
//! ## Architecture Philosophy
//!
//! **Flat module structure** - Following Rust best practices (tokio, serde, reqwest):
//! - Direct module access without deep nesting
//! - Clear naming makes deep hierarchies unnecessary
//! - The crate boundary provides encapsulation
//!
//! ## Module Organization
//!
//! ### Public API (re-exported at crate root)
//! - [`SessionManager`] - High-level session management
//! - [`Session`] - Active session handle with streaming API
//! - [`SessionConfig`] - Session configuration
//! - [`Error`], [`Result`] - Error handling
//!
//! ### Public Modules (for advanced usage)
//! - [`errors`] - Domain-specific error types with retry classification
//! - [`protocol`] - SSM protocol message types and framing
//! - [`session`] - Session management internals
//! - [`retry`] - Exponential backoff and circuit breaker patterns
//!
//! ### Internal Modules (implementation details)
//! - `aws_client` - AWS SDK integration wrapper
//! - `connection` - WebSocket connection lifecycle
//! - `channels` - Channel multiplexing for stdin/stdout/stderr
//!
//! ## Usage
//!
//! ```rust,no_run
//! use aws_ssm_bridge::{SessionConfig, SessionManager};
//! use futures::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create session manager
//! let manager = SessionManager::new().await?;
//!
//! // Start a session
//! let mut session = manager.start_session(SessionConfig {
//!     target: "i-1234567890abcdef0".to_string(),
//!     ..Default::default()
//! }).await?;
//!
//! // Stream output
//! let mut output = session.output();
//! tokio::spawn(async move {
//!     while let Some(data) = output.next().await {
//!         print!("{}", String::from_utf8_lossy(&data));
//!     }
//! });
//!
//! // Send commands
//! session.send(bytes::Bytes::from("ls -la\n")).await?;
//!
//! // Clean shutdown
//! session.terminate().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Disclaimer
//!
//! This project is not affiliated with, endorsed by, or sponsored by
//! Amazon Web Services, Inc. or any of its affiliates.

#![deny(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum message payload size (10MB)
pub const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

// ============================================================================
// Public Modules
// ============================================================================

/// Error types and result aliases
pub mod errors;

/// SSM protocol message types and framing
pub mod protocol;

/// Binary protocol implementation (AWS-compatible)
pub mod binary_protocol;

/// Session management and lifecycle
pub mod session;

/// Retry logic with exponential backoff and circuit breaker
pub mod retry;

/// Token bucket rate limiting for DoS protection
pub mod rate_limit;

/// Session builder for fluent API
pub mod builder;

/// Type-safe SSM document definitions
pub mod documents;

/// Port forwarding implementation
pub mod port_forward;

/// AWS SSM handshake protocol (3-phase)
pub mod handshake;

/// Acknowledgment and retransmission with RTT tracking
pub mod ack;

/// Cross-platform terminal handling for interactive shells
pub mod terminal;

/// Interactive shell sessions with full terminal support
pub mod interactive;

/// Metrics and observability hooks
pub mod metrics;

/// Graceful shutdown utilities
pub mod shutdown;

/// Session pool for managing multiple concurrent sessions
pub mod pool;

/// Session reconnection with exponential backoff
pub mod reconnect;

/// Structured tracing for distributed observability
pub mod tracing_ext;

// ============================================================================
// Internal Modules (not re-exported)
// ============================================================================

mod aws_client;
mod channels;
mod connection;

// ============================================================================
// Feature-Gated Modules
// ============================================================================

/// Python bindings via PyO3
#[cfg(feature = "python")]
pub mod python;

// ============================================================================
// Crate-Root Re-exports (convenience)
// ============================================================================

// Primary API types
pub use session::{Session, SessionConfig, SessionManager, SessionState};

// Builder pattern for ergonomic session creation
pub use builder::SessionBuilder;

// Error handling
pub use errors::{Error, Result};

// Protocol types
pub use protocol::{MessageType, SessionType};

// Retry utilities
pub use retry::{CircuitBreaker, CircuitState, RetryConfig, RetryStrategy};

// Rate limiting
pub use rate_limit::{RateLimitConfig, RateLimitResult, RateLimiter};

// Port forwarding
pub use port_forward::{PortForwardConfig, PortForwarder};

// Streaming API
pub use channels::OutputStream;

// Terminal handling
pub use terminal::{Terminal, TerminalConfig, TerminalInput, TerminalSize};

// Interactive shell
pub use interactive::{InteractiveConfig, InteractiveShell};

// Metrics (observability hooks)
pub use metrics::{register_metrics, MetricsRecorder};

// Graceful shutdown
pub use shutdown::{install_signal_handlers, ShutdownGuard, ShutdownSignal};

// Session pool
pub use pool::{PoolConfig, PoolStats, SessionPool};

// Reconnection
pub use reconnect::{ReconnectConfig, ReconnectEvent, ReconnectStats, ReconnectingSession};

// Tracing
pub use tracing_ext::{
    span_connection, span_handshake, span_receive, span_send, span_session, SessionSpan,
    TraceContext,
};
