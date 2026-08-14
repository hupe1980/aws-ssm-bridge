//! Forward a local TCP port to a port on the instance, or through it to
//! another host.
//!
//! ```sh
//! # instance-local port
//! cargo run --example port_forward -- i-0123456789abcdef0 3306 127.0.0.1:13306
//!
//! # through the instance to an RDS endpoint
//! cargo run --example port_forward -- i-0123456789abcdef0 5432 127.0.0.1:15432 db.internal
//! ```

use std::sync::Arc;

use aws_ssm_bridge::{
    documents::{PortForwardingSession, PortForwardingToRemoteHost},
    install_signal_handlers, PortForwardConfig, PortForwarder, SessionBuilder, ShutdownSignal,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aws_ssm_bridge=info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let target = args
        .next()
        .ok_or("usage: port_forward <instance-id> <remote-port> [local-addr] [remote-host]")?;
    let remote_port: u16 = args.next().ok_or("missing remote port")?.parse()?;
    let local_addr = args.next().unwrap_or_else(|| "127.0.0.1:0".into());
    let remote_host = args.next();

    let shutdown = ShutdownSignal::new();
    install_signal_handlers(shutdown.clone());

    let builder = SessionBuilder::new(&target).reason("aws-ssm-bridge port forwarding example");
    let session = Arc::new(match &remote_host {
        Some(host) => {
            builder
                .document(PortForwardingToRemoteHost::new(host, remote_port))
                .start()
                .await?
        }
        None => {
            builder
                .document(PortForwardingSession::new(remote_port))
                .start()
                .await?
        }
    });

    // Bind before the tunnel is live so the address can be printed — and so a
    // port conflict fails immediately rather than after the AWS round trip.
    let forwarder = PortForwarder::bind(PortForwardConfig {
        local_addr: local_addr.parse()?,
        max_connections: 100,
        ..Default::default()
    })
    .await?;

    let destination = remote_host.as_deref().unwrap_or("the instance");
    println!(
        "forwarding {} -> {destination}:{remote_port} (Ctrl-C to stop)",
        forwarder.local_addr()
    );

    forwarder.forward(Arc::clone(&session), shutdown).await?;

    println!("stopped: {:?}", session.close_reason());
    session.terminate().await?;
    Ok(())
}
