//! Port forwarding implementation for SSM sessions
//!
//! Enables local port forwarding through SSM sessions, useful for:
//! - Accessing services on private instances (RDP, SSH, databases)
//! - Secure tunneling without exposing ports
//! - Bypassing firewall restrictions
//!
//! # Architecture
//!
//! ```text
//! Local App → TCP Listener → smux stream → SSM Session → Remote Port
//!     ↓           ↓               ↓              ↓            ↓
//!  :8080    127.0.0.1:8080   stream N/M      WebSocket  instance:3389
//! ```
//!
//! Multiple concurrent TCP connections are multiplexed over a single SSM data
//! channel via smux framing, matching the behaviour of the official
//! `session-manager-plugin` binary.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument, warn};

use crate::errors::{Error, Result};
use crate::mux::{SmuxConfig, SmuxSession};
use crate::session::Session;

/// Port forwarding configuration
#[derive(Debug, Clone)]
pub struct PortForwardConfig {
    /// Local address to bind to (e.g., "127.0.0.1:8080")
    pub local_addr: SocketAddr,

    /// Remote port on the instance
    pub remote_port: u16,

    /// Maximum concurrent connections
    pub max_connections: usize,
}

impl Default for PortForwardConfig {
    fn default() -> Self {
        Self {
            local_addr: "127.0.0.1:0"
                .parse()
                .expect("127.0.0.1:0 is a valid socket address"), // Random port
            remote_port: 0,
            max_connections: 10,
        }
    }
}

/// Port forwarding session manager
pub struct PortForwarder {
    config: PortForwardConfig,
    listener: Option<TcpListener>,
}

impl PortForwarder {
    /// Create a new port forwarder
    pub fn new(config: PortForwardConfig) -> Self {
        Self {
            config,
            listener: None,
        }
    }

    /// Start listening for local connections
    #[instrument(skip(self), fields(local_addr = %self.config.local_addr))]
    pub async fn listen(&mut self) -> Result<SocketAddr> {
        // I-5: Catch the most common misconfiguration early with a clear error
        // message rather than silently connecting to port 0 (which the remote
        // SSM agent would reject with an opaque protocol error).
        if self.config.remote_port == 0 {
            return Err(Error::Config(
                "remote_port must be non-zero; set PortForwardConfig::remote_port before calling listen()"
                    .to_string(),
            ));
        }

        info!("Starting port forwarding listener");

        let listener = TcpListener::bind(self.config.local_addr)
            .await
            .map_err(Error::Io)?;

        let local_addr = listener.local_addr().map_err(Error::Io)?;

        info!(
            local_addr = %local_addr,
            remote_port = self.config.remote_port,
            "Port forwarding listener started"
        );

        self.listener = Some(listener);
        Ok(local_addr)
    }

    /// Accept connections and forward to the SSM session via smux multiplexing.
    ///
    /// Each accepted TCP connection gets its own smux logical stream within the
    /// single underlying SSM data channel.  Concurrent connections do not
    /// interfere with each other because every byte is tagged with a unique
    /// stream ID by the smux framing layer.
    ///
    /// Requires the SSM session to have been started with a mux-capable
    /// document such as `AWS-StartPortForwardingSessionToRemoteHost`.
    /// L-3: Accepts a `ShutdownSignal` so the forwarding loop can be
    /// cancelled cleanly (e.g. from `Ctrl-C` or session termination);
    /// on shutdown the listener is explicitly dropped to release the bound port.
    #[instrument(skip(self, session, shutdown))]
    pub async fn forward(
        &mut self,
        session: Arc<Session>,
        shutdown: crate::shutdown::ShutdownSignal,
    ) -> Result<()> {
        let listener = self
            .listener
            .as_ref()
            .ok_or_else(|| Error::InvalidState("Listener not started".to_string()))?;

        // Block until the SSM protocol handshake is complete.
        let connect_timeout = session.config().connect_timeout;
        if !session.wait_for_ready(connect_timeout).await {
            return Err(Error::InvalidState(
                "SSM session not ready: handshake timed out".to_string(),
            ));
        }

        // Wrap the session in a smux multiplexer (one per PortForwarder lifetime).
        let mux = Arc::new(SmuxSession::new(
            Arc::clone(&session),
            SmuxConfig::default(),
        ));

        let semaphore = Arc::new(Semaphore::new(self.config.max_connections));
        let max_connections = self.config.max_connections;

        info!(max_connections, "Accepting port forwarding connections");

        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!("Port forwarder shutdown requested");
                    self.listener = None; // release the bound port immediately
                    return Ok(());
                }

                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer_addr = %peer_addr, "Accepted connection");

                            let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!(
                                        max_connections,
                                        "Connection limit reached, rejecting connection"
                                    );
                                    drop(stream);
                                    continue;
                                }
                            };

                            let mux = Arc::clone(&mux);
                            tokio::spawn(async move {
                                let _permit = permit; // released when the task finishes
                                if let Err(e) = Self::handle_connection(stream, mux).await {
                                    error!(error = ?e, "Connection handler error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = ?e, "Failed to accept connection");
                            return Err(Error::Io(e));
                        }
                    }
                }
            }
        }
    }

    /// Bidirectionally copy between a local TCP connection and a smux stream.
    #[instrument(skip(stream, mux))]
    async fn handle_connection(mut stream: TcpStream, mux: Arc<SmuxSession>) -> Result<()> {
        debug!("Starting connection handler");

        let mut smux_stream = mux.open_stream()?;

        tokio::io::copy_bidirectional(&mut stream, &mut smux_stream)
            .await
            .map_err(Error::Io)?;

        info!("Connection handler completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_forward_config_default() {
        let config = PortForwardConfig::default();
        assert_eq!(config.max_connections, 10);
    }

    #[test]
    fn test_port_forward_config_custom() {
        let config = PortForwardConfig {
            local_addr: "127.0.0.1:8080".parse().unwrap(),
            remote_port: 3389,
            max_connections: 5,
        };

        assert_eq!(config.remote_port, 3389);
        assert_eq!(config.max_connections, 5);
    }

    /// I-5: remote_port=0 must be rejected before binding the listener.
    #[tokio::test]
    async fn test_listen_rejects_zero_remote_port() {
        let mut forwarder = PortForwarder::new(PortForwardConfig::default());
        // default remote_port is 0
        let result = forwarder.listen().await;
        assert!(
            result.is_err(),
            "listen() must return an error when remote_port == 0"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("remote_port"),
            "Error message should mention remote_port, got: {msg}"
        );
    }
}
