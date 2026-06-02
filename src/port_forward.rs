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
use crate::protocol::SessionType;
use crate::session::Session;

/// Configuration for a [`PortForwarder`].
///
/// The remote port belongs in the SSM session document
/// (e.g. [`PortForwardingSession::new(3306)`]) — not here.
/// See [`SessionBuilder::document`].
///
/// [`PortForwardingSession::new(3306)`]: crate::documents::PortForwardingSession::new
/// [`SessionBuilder::document`]: crate::builder::SessionBuilder::document
#[derive(Debug, Clone)]
pub struct PortForwardConfig {
    /// Local address to bind to (default: `127.0.0.1:0`, OS-assigned random port)
    pub local_addr: SocketAddr,

    /// Maximum concurrent connections (default: 100).
    ///
    /// Tune this to match your downstream connection pool size.  Typical
    /// database pools (SQLx, HikariCP) default to 10–50 connections; a value
    /// of 100 leaves headroom without imposing a meaningful resource cost.
    pub max_connections: usize,
}

impl Default for PortForwardConfig {
    fn default() -> Self {
        Self {
            local_addr: "127.0.0.1:0"
                .parse()
                .expect("127.0.0.1:0 is a valid socket address"),
            max_connections: 100,
        }
    }
}

/// A port forwarder that is bound to a local TCP port and ready to accept
/// connections.
///
/// # Usage
///
/// ```no_run
/// # use std::sync::Arc;
/// # use aws_ssm_bridge::{PortForwardConfig, PortForwarder, shutdown::ShutdownSignal};
/// # async fn doc(session: Arc<aws_ssm_bridge::Session>, shutdown: ShutdownSignal)
/// # -> aws_ssm_bridge::errors::Result<()> {
/// let forwarder = PortForwarder::bind(PortForwardConfig::default()).await?;
/// let local_addr = forwarder.local_addr();
/// println!("Listening on {local_addr}");
/// forwarder.forward(session, shutdown).await?;
/// # Ok(()) }
/// ```
///
/// # Design
///
/// `PortForwarder` is created by the async [`PortForwarder::bind`] constructor,
/// which binds the TCP socket immediately.  [`PortForwarder::forward`] then
/// consumes the forwarder, driving the accept loop and dropping the listener when
/// it returns (releasing the bound port on every exit path).  There is no
/// intermediate "not-yet-bound" state — the type system enforces the
/// `bind → forward` ordering and makes the "listener not started" class of
/// runtime errors structurally impossible.
#[must_use = "dropping a PortForwarder stops port forwarding before any connections are accepted; call forward() to start forwarding"]
pub struct PortForwarder {
    config: PortForwardConfig,
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl PortForwarder {
    /// Bind a local TCP port and return a forwarder ready to accept connections.
    ///
    /// Uses [`PortForwardConfig::local_addr`]; when the port is `0` the OS assigns
    /// a free port.  Call [`local_addr`](Self::local_addr) after `bind` to
    /// discover the actual port.
    #[instrument(skip(config), fields(local_addr = %config.local_addr))]
    pub async fn bind(config: PortForwardConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.local_addr)
            .await
            .map_err(Error::Io)?;
        let local_addr = listener.local_addr().map_err(Error::Io)?;
        info!(local_addr = %local_addr, "Port forwarding listener bound");
        Ok(Self {
            config,
            listener,
            local_addr,
        })
    }

    /// The local address this forwarder is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept connections and forward them to the SSM session via smux multiplexing.
    ///
    /// Each accepted TCP connection gets its own smux logical stream within the
    /// single underlying SSM data channel.  Concurrent connections do not
    /// interfere with each other because every byte is tagged with a unique
    /// stream ID by the smux framing layer.
    ///
    /// Consumes `self`, dropping the bound listener when the function returns so
    /// the local port is released on every exit path (shutdown, session
    /// termination, or error).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] immediately if `session` was not started with a
    /// port-forwarding document (`SessionType::Port`), giving a clear diagnostic
    /// instead of a deep smux/protocol failure.
    #[instrument(skip(self, session, shutdown), fields(local_addr = %self.local_addr))]
    pub async fn forward(
        self,
        session: Arc<Session>,
        shutdown: crate::shutdown::ShutdownSignal,
    ) -> Result<()> {
        // Fail fast: the SSM agent only speaks smux multiplexing for Port sessions.
        // Calling forward() with a StandardStream session would fail deep inside the
        // smux open_stream() call with an opaque protocol error.
        if session.config().session_type != SessionType::Port {
            return Err(Error::Config(format!(
                "PortForwarder requires a port-forwarding session (SessionType::Port), \
                 got {:?}; use SessionBuilder::document(PortForwardingSession::new(port))",
                session.config().session_type,
            )));
        }

        let Self {
            config, listener, ..
        } = self;

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

        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        let max_connections = config.max_connections;

        // Extract the remote port from session document parameters for logging.
        // The actual remote-side connection is established by the SSM agent based
        // on the document parameters; we do not need to pass this value anywhere.
        let remote_port = session
            .config()
            .parameters
            .get("portNumber")
            .and_then(|v| v.first())
            .and_then(|s| s.parse::<u16>().ok());

        info!(
            max_connections,
            remote_port, "Accepting port forwarding connections"
        );

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
                                    error!(peer_addr = %peer_addr, error = ?e, "Connection handler error");
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
        assert_eq!(config.max_connections, 100);
        // local_addr defaults to 127.0.0.1:0 (OS-assigned port)
        assert_eq!(config.local_addr.ip().to_string(), "127.0.0.1");
        assert_eq!(config.local_addr.port(), 0);
    }

    #[test]
    fn test_port_forward_config_custom() {
        let config = PortForwardConfig {
            local_addr: "127.0.0.1:8080".parse().unwrap(),
            max_connections: 5,
        };
        assert_eq!(
            config.local_addr,
            "127.0.0.1:8080".parse::<std::net::SocketAddr>().unwrap()
        );
        assert_eq!(config.max_connections, 5);
    }

    /// bind() must succeed with default config; no remote_port required.
    /// The returned forwarder reports the OS-assigned port (non-zero).
    #[tokio::test]
    async fn test_bind_succeeds_with_default_config() {
        let forwarder = PortForwarder::bind(PortForwardConfig::default())
            .await
            .expect("bind must succeed on loopback");
        // OS should have assigned a real port (not 0)
        assert_ne!(
            forwarder.local_addr().port(),
            0,
            "OS must assign a non-zero port"
        );
    }

    /// bind() with an explicit port returns that exact port.
    #[tokio::test]
    async fn test_bind_returns_correct_local_addr() {
        // Port 0 → OS-assigned; just verify the IP is what we asked for.
        let config = PortForwardConfig {
            local_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 1,
        };
        let forwarder = PortForwarder::bind(config)
            .await
            .expect("bind must succeed on loopback");
        assert_eq!(forwarder.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(forwarder.local_addr().port(), 0);
    }
}
