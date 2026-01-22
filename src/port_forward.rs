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
//! Local App → TCP Listener → SSM Session → Remote Port
//!     ↓           ↓              ↓            ↓
//!   :8080    127.0.0.1:8080  WebSocket   instance:3389
//! ```

use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};

use crate::errors::{Error, Result};
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
            local_addr: "127.0.0.1:0".parse().unwrap(), // Random port
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

    /// Accept connections and forward to SSM session
    ///
    /// This runs in a loop, accepting connections and spawning handlers.
    #[instrument(skip(self, session))]
    pub async fn forward(&mut self, session: Arc<Session>) -> Result<()> {
        let listener = self
            .listener
            .as_ref()
            .ok_or_else(|| Error::InvalidState("Listener not started".to_string()))?;

        let (connection_tx, mut connection_rx): (mpsc::Sender<()>, _) =
            mpsc::channel(self.config.max_connections);
        let max_connections = self.config.max_connections;

        info!("Accepting port forwarding connections");

        loop {
            tokio::select! {
                // Accept new connection
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer_addr = %peer_addr, "Accepted connection");

                            // Check connection limit
                            if connection_tx.capacity() == 0 {
                                warn!(
                                    max_connections,
                                    "Connection limit reached, rejecting connection"
                                );
                                drop(stream);
                                continue;
                            }

                            // Spawn connection handler
                            let session = Arc::clone(&session);
                            let tx = connection_tx.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(stream, session).await {
                                    error!(error = ?e, "Connection handler error");
                                }
                                drop(tx); // Release connection slot
                            });
                        }
                        Err(e) => {
                            error!(error = ?e, "Failed to accept connection");
                            return Err(Error::Io(e));
                        }
                    }
                }

                // Handle connection cleanup
                _ = connection_rx.recv() => {
                    // Connection ended, slot freed
                }
            }
        }
    }

    /// Handle a single port forwarding connection
    #[instrument(skip(stream, session))]
    async fn handle_connection(mut stream: TcpStream, session: Arc<Session>) -> Result<()> {
        use futures::StreamExt;

        debug!("Starting connection handler");

        let mut buffer = vec![0u8; 8192];

        // Get output stream from session
        let mut output = session.output();

        loop {
            tokio::select! {
                // Read from local socket
                result = stream.read(&mut buffer) => {
                    match result {
                        Ok(0) => {
                            debug!("Local connection closed");
                            break;
                        }
                        Ok(n) => {
                            debug!(bytes = n, "Read from local socket");

                            // Send to SSM session
                            let data = Bytes::copy_from_slice(&buffer[..n]);
                            session.send(data).await?;
                        }
                        Err(e) => {
                            error!(error = ?e, "Error reading from local socket");
                            return Err(Error::Io(e));
                        }
                    }
                }

                // Read from SSM session
                data = output.next() => {
                    match data {
                        Some(bytes) => {
                            debug!(bytes = bytes.len(), "Received from SSM session");

                            // Write to local socket
                            stream.write_all(&bytes).await.map_err(Error::Io)?;
                        }
                        None => {
                            debug!("SSM session closed");
                            break;
                        }
                    }
                }
            }
        }

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
}
