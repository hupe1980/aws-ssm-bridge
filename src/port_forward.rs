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
    /// the listener is moved into the function and dropped when it returns,
    /// releasing the bound port on every exit path.
    #[instrument(skip(self, session, shutdown))]
    pub async fn forward(
        &mut self,
        session: Arc<Session>,
        shutdown: crate::shutdown::ShutdownSignal,
    ) -> Result<()> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| Error::InvalidState("Listener not started".to_string()))?;

        // Block until the SSM protocol handshake is complete, racing against
        // shutdown and premature session termination.
        let connect_timeout = session.config().connect_timeout;
        let ready = tokio::select! {
            ready = session.wait_for_ready(connect_timeout) => ready,
            _ = shutdown.cancelled() => {
                info!("Shutting down before session handshake completed");
                return Ok(());
            }
            _ = session.wait_terminated() => {
                info!("SSM session terminated before becoming ready");
                return Ok(());
            }
        };
        if !ready {
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
                    return Ok(());
                }

                // Bridge session termination: if the SSM session terminates
                // independently of the caller's ShutdownSignal (e.g. the
                // ConnectionManager detects a dead connection), trigger
                // shutdown here so the accept loop exits and the bound port
                // is released promptly.  Also cancels active connection
                // handlers that share the same ShutdownSignal.
                _ = session.wait_terminated() => {
                    info!("SSM session terminated, stopping port forwarder");
                    shutdown.shutdown();
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
                            let handler_shutdown = shutdown.clone();
                            tokio::spawn(async move {
                                let _permit = permit; // released when the task finishes
                                if let Err(e) = Self::handle_connection(stream, mux, handler_shutdown).await {
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

    /// Copy between a local TCP connection and a smux stream in both directions.
    ///
    /// Two independent one-way `copy` futures run concurrently; the select exits
    /// as soon as **either** direction completes or errors.  This avoids the
    /// zombie-connection problem with `copy_bidirectional`: after the remote sends
    /// `CMD_FIN` the smux read half returns EOF, but `copy_bidirectional` would
    /// keep draining the local TCP socket and writing PSH frames into a stream
    /// the remote has already closed.
    #[instrument(skip(stream, mux, shutdown))]
    async fn handle_connection(
        stream: TcpStream,
        mux: Arc<SmuxSession>,
        shutdown: crate::shutdown::ShutdownSignal,
    ) -> Result<()> {
        debug!("Starting connection handler");

        let smux_stream = mux.open_stream()?;

        let (mut tcp_rx, mut tcp_tx) = stream.into_split();
        let (mut smux_rx, mut smux_tx) = tokio::io::split(smux_stream);

        let local_to_remote = tokio::io::copy(&mut tcp_rx, &mut smux_tx);
        let remote_to_local = tokio::io::copy(&mut smux_rx, &mut tcp_tx);
        tokio::pin!(local_to_remote, remote_to_local);

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                debug!("Connection handler cancelled due to shutdown");
            }
            res = &mut local_to_remote => {
                match res {
                    Ok(n) => info!(bytes = n, "Local\u{2192}remote copy completed"),
                    Err(e) => return Err(Error::Io(e)),
                }
            }
            res = &mut remote_to_local => {
                match res {
                    Ok(n) => info!(bytes = n, "Remote\u{2192}local copy completed"),
                    Err(e) => return Err(Error::Io(e)),
                }
            }
        }

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
