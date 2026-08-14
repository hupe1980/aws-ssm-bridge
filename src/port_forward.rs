//! Local TCP port forwarding over an SSM session.
//!
//! ```text
//!   psql :15432 ──► TcpListener ──► smux stream 1 ─┐
//!   psql :15432 ──► TcpListener ──► smux stream 3 ─┼─► one SSM session ──► agent ──► db:5432
//!   psql :15432 ──► TcpListener ──► smux stream 5 ─┘
//! ```
//!
//! Every accepted connection becomes its own smux stream inside a single
//! WebSocket, so concurrent connections neither block nor corrupt each other.
//!
//! ```no_run
//! use std::sync::Arc;
//! use aws_ssm_bridge::{
//!     documents::PortForwardingToRemoteHost, PortForwardConfig, PortForwarder,
//!     SessionBuilder, ShutdownSignal, install_signal_handlers,
//! };
//!
//! # async fn example() -> aws_ssm_bridge::Result<()> {
//! let shutdown = ShutdownSignal::new();
//! install_signal_handlers(shutdown.clone());
//!
//! let session = Arc::new(
//!     SessionBuilder::new("i-0123456789abcdef0")
//!         .document(PortForwardingToRemoteHost::new("db.internal", 5432))
//!         .start()
//!         .await?,
//! );
//!
//! let forwarder = PortForwarder::bind(PortForwardConfig {
//!     local_addr: "127.0.0.1:15432".parse().unwrap(),
//!     ..Default::default()
//! })
//! .await?;
//!
//! println!("psql -h 127.0.0.1 -p {}", forwarder.local_addr().port());
//! forwarder.forward(session, shutdown).await?;
//! # Ok(()) }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument, warn};

use crate::documents::SessionType;
use crate::errors::{Error, Result};
use crate::mux::{SmuxConfig, SmuxSession};
use crate::session::Session;
use crate::shutdown::ShutdownSignal;

/// How a [`PortForwarder`] listens and how much it will accept.
#[derive(Debug, Clone)]
pub struct PortForwardConfig {
    /// Address to listen on.
    ///
    /// Defaults to `127.0.0.1:0`, which asks the OS for a free port — read it
    /// back from [`PortForwarder::local_addr`]. Binding to `0.0.0.0` exposes
    /// the tunnel to your whole network; do that deliberately or not at all.
    pub local_addr: SocketAddr,

    /// Maximum simultaneous forwarded connections.
    ///
    /// Excess connections are accepted and immediately closed, which surfaces
    /// as a connection reset in the client rather than an unbounded queue.
    pub max_connections: usize,

    /// Send smux keep-alive frames.
    ///
    /// Off by default, matching the official plugin: SSM applies its own idle
    /// timeout, and synthetic keep-alives keep forgotten tunnels alive forever.
    pub keepalive: bool,
}

impl Default for PortForwardConfig {
    fn default() -> Self {
        Self {
            local_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_connections: 100,
            keepalive: false,
        }
    }
}

/// A bound local port, ready to forward connections.
///
/// [`bind`](Self::bind) claims the port and [`forward`](Self::forward) consumes
/// the forwarder to run the accept loop. There is no unbound state, so
/// "listener was never started" cannot happen, and the port is released on every
/// exit path.
#[must_use = "a PortForwarder does nothing until forward() is called"]
#[derive(Debug)]
pub struct PortForwarder {
    config: PortForwardConfig,
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl PortForwarder {
    /// Bind the local port.
    #[instrument(skip(config), fields(local_addr = %config.local_addr))]
    pub async fn bind(config: PortForwardConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.local_addr).await?;
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "port forwarder listening");
        Ok(Self {
            config,
            listener,
            local_addr,
        })
    }

    /// The address actually bound, with the OS-assigned port filled in.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and forward connections until the session ends or shutdown fires.
    ///
    /// Returns `Ok(())` for both of those; an error means the listener itself
    /// failed or the session was not a port-forwarding session.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] straight away if `session` was not started with
    /// a port-forwarding document. The agent only speaks smux for those, so
    /// forwarding over a shell session would frame terminal output as smux
    /// frames and hang — worth catching here rather than diagnosing later.
    #[instrument(skip(self, session, shutdown), fields(local_addr = %self.local_addr))]
    pub async fn forward(self, session: Arc<Session>, shutdown: ShutdownSignal) -> Result<()> {
        if session.config().session_type() != SessionType::Port {
            return Err(Error::Config(
                "PortForwarder needs a port-forwarding session. Start it with \
                 SessionBuilder::port_forward(port) or \
                 .document(PortForwardingToRemoteHost::new(host, port))."
                    .into(),
            ));
        }

        let Self {
            config, listener, ..
        } = self;

        // The agent will not accept smux frames until the handshake finishes.
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                debug!("shutdown requested before the session was ready");
                return Ok(());
            }
            result = session.wait_ready() => result?,
        }

        let mux = Arc::new(SmuxSession::new(
            Arc::clone(&session),
            SmuxConfig {
                keepalive: config.keepalive,
            },
        ));
        let permits = Arc::new(Semaphore::new(config.max_connections));
        info!(
            max_connections = config.max_connections,
            "forwarding connections"
        );

        loop {
            tokio::select! {
                biased;

                () = shutdown.cancelled() => {
                    info!("shutdown requested; releasing the local port");
                    return Ok(());
                }

                // A session can die on its own — a dead network, an agent
                // restart, an idle timeout. Without this arm the listener would
                // keep accepting connections into a tunnel that no longer exists.
                () = session.closed() => {
                    let reason = session.close_reason();
                    info!(?reason, "session ended; stopping the port forwarder");
                    shutdown.shutdown();
                    return Ok(());
                }

                accepted = listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(pair) => pair,
                        Err(e) => {
                            error!(error = %e, "accept failed");
                            return Err(Error::Io(e));
                        }
                    };

                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        warn!(
                            %peer,
                            max_connections = config.max_connections,
                            "connection limit reached; refusing"
                        );
                        drop(stream);
                        continue;
                    };

                    // Nagle would batch the small writes that interactive
                    // protocols depend on; a tunnel should not add latency the
                    // direct connection would not have.
                    if let Err(e) = stream.set_nodelay(true) {
                        debug!(error = %e, "could not disable Nagle on the accepted socket");
                    }

                    debug!(%peer, "accepted");
                    let mux = Arc::clone(&mux);
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Err(e) = forward_connection(stream, mux, shutdown).await {
                            warn!(%peer, error = %e, "forwarded connection failed");
                        }
                    });
                }
            }
        }
    }
}

/// Pump one accepted connection through its own smux stream.
async fn forward_connection(
    stream: TcpStream,
    mux: Arc<SmuxSession>,
    shutdown: ShutdownSignal,
) -> Result<()> {
    let smux_stream = mux.open_stream()?;
    let stream_id = smux_stream.id();

    let (tcp_read, tcp_write) = stream.into_split();
    let (mux_read, mux_write) = tokio::io::split(smux_stream);

    // Half-close matters: many protocols (HTTP without keep-alive, `nc`,
    // anything shell-piped) signal "request finished" by closing one direction
    // and then read the reply. Tearing the whole connection down as soon as
    // either direction ends would truncate that reply.
    let outbound = pump(tcp_read, mux_write);
    let inbound = pump(mux_read, tcp_write);

    tokio::select! {
        biased;
        () = shutdown.cancelled() => {
            debug!(stream_id, "connection cancelled by shutdown");
        }
        (sent, received) = async { tokio::join!(outbound, inbound) } => {
            debug!(
                stream_id,
                sent = ?sent.as_ref().ok(),
                received = ?received.as_ref().ok(),
                "connection finished",
            );
            sent?;
            received?;
        }
    }
    Ok(())
}

/// Copy one direction to EOF, then half-close the writer.
async fn pump<R, W>(mut reader: R, mut writer: W) -> std::io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let copied = tokio::io::copy(&mut reader, &mut writer).await?;
    // Propagate EOF so the peer learns this direction is done while the other
    // direction keeps flowing.
    writer.shutdown().await?;
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_bind_loopback_on_an_ephemeral_port() {
        let config = PortForwardConfig::default();
        assert!(
            config.local_addr.ip().is_loopback(),
            "must not be public by default"
        );
        assert_eq!(config.local_addr.port(), 0, "0 asks the OS for a free port");
        assert_eq!(config.max_connections, 100);
        assert!(!config.keepalive, "match the official plugin's default");
    }

    #[tokio::test]
    async fn bind_reports_the_os_assigned_port() {
        let forwarder = PortForwarder::bind(PortForwardConfig::default())
            .await
            .expect("binding loopback must succeed");
        assert_ne!(forwarder.local_addr().port(), 0);
        assert!(forwarder.local_addr().ip().is_loopback());
    }

    #[tokio::test]
    async fn bind_honours_an_explicit_port() {
        // Claim a port, release it, then ask for it explicitly.
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let wanted = probe.local_addr().unwrap();
        drop(probe);

        let forwarder = PortForwarder::bind(PortForwardConfig {
            local_addr: wanted,
            ..Default::default()
        })
        .await
        .expect("re-binding a just-released port must succeed");
        assert_eq!(forwarder.local_addr(), wanted);
    }

    #[tokio::test]
    async fn binding_a_taken_port_fails() {
        let holder = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let taken = holder.local_addr().unwrap();

        let result = PortForwarder::bind(PortForwardConfig {
            local_addr: taken,
            ..Default::default()
        })
        .await;
        assert!(result.is_err(), "a port in use must not bind twice");
    }

    /// `pump` must forward every byte and then half-close, so the peer sees EOF.
    #[tokio::test]
    async fn pump_copies_then_half_closes() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            // Returns only once the writer half-closed, proving EOF propagated.
            socket.read_to_end(&mut received).await.unwrap();
            received
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let source = std::io::Cursor::new(b"forwarded payload".to_vec());
        let copied = pump(source, client).await.unwrap();
        assert_eq!(copied, 17);

        assert_eq!(server.await.unwrap(), b"forwarded payload");
    }
}
