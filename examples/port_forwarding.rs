//! Example: Port forwarding session
//!
//! Forwards a local TCP port to a port on an EC2 instance via SSM.
//!
//! ## Usage
//! ```bash
//! cargo run --example port_forwarding -- <instance-id> <remote-port> <local-port> [region]
//! cargo run --example port_forwarding -- i-0123456789abcdef0 80 8080
//! ```
//!
//! Then in another terminal: `curl http://localhost:8080`

use std::net::SocketAddr;
use std::sync::Arc;

use aws_ssm_bridge::{
    documents::PortForwardingSession,
    shutdown::{install_signal_handlers, ShutdownSignal},
    PortForwardConfig, PortForwarder, SessionBuilder, SessionManager,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <instance-id> <remote-port> <local-port> [region]", args[0]);
        eprintln!("Example: {} i-1234567890abcdef0 80 8080", args[0]);
        std::process::exit(1);
    }

    let instance_id = &args[1];
    let remote_port: u16 = args[2].parse()?;
    let local_port: u16 = args[3].parse()?;
    let region = args.get(4).cloned();

    // Install OS signal handlers (Ctrl+C / SIGTERM) into a shared shutdown token.
    let shutdown = ShutdownSignal::new();
    install_signal_handlers(shutdown.clone());

    // Build the SSM session.  The remote port is part of the session document —
    // the SSM agent uses it to decide which port on the instance to connect to.
    let manager = SessionManager::new().await?;
    let mut builder = SessionBuilder::new(instance_id)
        .document(PortForwardingSession::new(remote_port))
        .reason("Example port forwarding");
    if let Some(r) = region {
        builder = builder.region(r);
    }
    let session_arc = Arc::new(builder.build_with(&manager).await?);

    // Bind the local TCP port *before* printing the ready message so the port
    // is available as soon as the user sees the prompt.
    let local_addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse()?;
    let forwarder = PortForwarder::bind(PortForwardConfig {
        local_addr,
        ..Default::default()
    })
    .await?;

    println!("\n✓ Port forwarding session started: {}", session_arc.id());
    println!("  Forwarding {}  →  remote:{}", forwarder.local_addr(), remote_port);
    println!("  Press Ctrl+C to stop\n");

    // forward() drives the accept loop until shutdown is signalled, the session
    // terminates, or an unrecoverable error occurs.
    forwarder.forward(Arc::clone(&session_arc), shutdown).await?;

    println!("\nTerminating session...");
    session_arc.terminate().await?;
    println!("✓ Session terminated");

    Ok(())
}
