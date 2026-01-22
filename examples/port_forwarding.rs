//! Example: Port forwarding session
//!
//! This example demonstrates how to set up port forwarding to an EC2 instance.

use aws_ssm_bridge::{documents::PortForwardingSession, SessionBuilder, SessionManager};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "Usage: {} <instance-id> <remote-port> <local-port> [region]",
            args[0]
        );
        eprintln!(
            "Example: {} i-1234567890abcdef0 3306 13306 us-east-1",
            args[0]
        );
        std::process::exit(1);
    }

    let instance_id = &args[1];
    let remote_port: u16 = args[2].parse()?;
    let local_port = &args[3];
    let region = args.get(4).cloned();

    println!("Starting port forwarding session");
    println!("  Instance: {}", instance_id);
    println!("  Remote port: {}", remote_port);
    println!("  Local port: {}", local_port);
    if let Some(ref r) = region {
        println!("  Region: {}", r);
    }

    // Create session manager
    let manager = SessionManager::new().await?;

    // Use type-safe document (no magic strings!)
    let mut builder = SessionBuilder::new(instance_id)
        .document(PortForwardingSession::new(remote_port))
        .reason("Example port forwarding");

    // Conditionally add region
    if let Some(r) = region {
        builder = builder.region(r);
    }

    // Start session
    let mut session = builder.build_with(&manager).await?;

    println!("\n✓ Port forwarding session started: {}", session.id());
    println!("  You can now connect to localhost:{}", local_port);

    // Check state
    let state = session.state().await;
    println!("  State: {:?}", state);

    // Keep session alive
    println!("\nPress Ctrl+C to terminate the session...");
    tokio::signal::ctrl_c().await?;

    // Terminate session
    println!("\nTerminating session...");
    session.terminate().await?;
    session.wait_terminated().await;

    println!("✓ Session terminated");

    Ok(())
}
