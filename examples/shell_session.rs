//! Example: Basic shell session
//!
//! This example demonstrates how to start a basic shell session with an EC2 instance.

use aws_ssm_bridge::{SessionConfig, SessionManager, SessionType};
use futures::StreamExt;

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
    if args.len() < 2 {
        eprintln!("Usage: {} <instance-id> [region]", args[0]);
        std::process::exit(1);
    }

    let instance_id = &args[1];
    let region = args.get(2).cloned();

    println!("Starting SSM session to {}", instance_id);
    if let Some(ref r) = region {
        println!("Region: {}", r);
    }

    // Create session manager
    let manager = SessionManager::new().await?;

    // Configure session
    let config = SessionConfig {
        target: instance_id.clone(),
        region,
        session_type: SessionType::StandardStream,
        document_name: None,
        parameters: std::collections::HashMap::new(),
        reason: Some("Example shell session".to_string()),
        ..Default::default()
    };

    // Start session
    let mut session = manager.start_session(config).await?;

    println!("✓ Session started: {}", session.id());

    // Check state
    let state = session.state().await;
    println!("  State: {:?}", state);

    // Get output stream
    let mut output = session.output();

    // Spawn task to print output
    let output_task = tokio::spawn(async move {
        while let Some(data) = output.next().await {
            print!("{}", String::from_utf8_lossy(&data));
        }
    });

    // Wait for protocol-level ready state (start_publication or handshake complete)
    println!("\nWaiting for session to be protocol-ready...");
    let ready = session
        .wait_for_ready(tokio::time::Duration::from_secs(30))
        .await;

    if !ready {
        println!("⚠ Session did not become ready within timeout");
        println!("  This may indicate the agent isn't responding with start_publication");
        println!("  Protocol state: is_ready={}", session.is_ready());
    } else {
        println!("✓ Session protocol ready (can_send=true)");
    }

    // Send a simple command
    println!("\nSending command: echo 'Hello from Rust!'");
    session
        .send(bytes::Bytes::from("echo 'Hello from Rust!'\n"))
        .await?;

    // Wait longer for output
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Send exit command
    session.send(bytes::Bytes::from("exit\n")).await?;

    // Wait a bit more
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Terminate session
    println!("\nTerminating session...");
    session.terminate().await?;

    // Cancel output task
    output_task.abort();

    println!("✓ Session terminated successfully");

    Ok(())
}
