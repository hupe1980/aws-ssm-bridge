//! Example: Auto-Reconnecting Session
//!
//! This example demonstrates automatic reconnection with exponential backoff.
//! The session will automatically reconnect if the connection drops.
//!
//! # Features Demonstrated
//! - Automatic reconnection on connection failure
//! - Exponential backoff with jitter
//! - Reconnection event monitoring
//! - Graceful shutdown
//!
//! # Running
//! ```bash
//! cargo run --example reconnecting -- i-1234567890abcdef0
//! ```

use aws_ssm_bridge::{
    reconnect::{ReconnectConfig, ReconnectEvent, ReconnectingSession},
    shutdown::{install_signal_handlers, ShutdownSignal},
};
use bytes::Bytes;
use std::env;
use std::time::Duration;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse instance ID from command line
    let instance_id = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: reconnecting <instance-id>");
        std::process::exit(1);
    });

    info!(target = %instance_id, "Starting reconnecting session example");

    // Set up graceful shutdown
    let shutdown = ShutdownSignal::new();
    install_signal_handlers(shutdown.clone());

    // Create reconnecting session with custom configuration
    let session = ReconnectingSession::new(
        &instance_id,
        ReconnectConfig {
            max_retries: 10,                       // Maximum reconnection attempts
            initial_delay: Duration::from_secs(1), // Start with 1 second delay
            max_delay: Duration::from_secs(60),    // Cap at 60 seconds
            jitter: true,                          // Add randomness to prevent thundering herd
            ..Default::default()
        },
    )
    .await?;

    info!("Reconnecting session established");

    // Monitor reconnection events
    let mut events = session.events();
    let event_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = event_shutdown.cancelled() => break,
                event = events.recv() => {
                    match event {
                        Ok(ReconnectEvent::Reconnecting { attempt, delay }) => {
                            warn!(
                                attempt = attempt,
                                delay_ms = delay.as_millis(),
                                "Reconnecting..."
                            );
                        }
                        Ok(ReconnectEvent::Reconnected { session_id, attempts }) => {
                            info!(
                                session_id = %session_id,
                                attempts = attempts,
                                "Successfully reconnected!"
                            );
                        }
                        Ok(ReconnectEvent::Failed { total_attempts, last_error }) => {
                            error!(
                                attempts = total_attempts,
                                error = %last_error,
                                "Reconnection failed permanently"
                            );
                        }
                        Ok(ReconnectEvent::Disconnected { error }) => {
                            warn!(error = %error, "Connection lost");
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    // Send a test command
    info!("Sending test command...");
    session
        .send(Bytes::from("echo 'Hello from reconnecting session'\n"))
        .await?;

    // Display session info
    let stats = session.stats();
    info!(
        total_attempts = stats.total_attempts,
        successful = stats.successful_reconnects,
        "Initial connection statistics"
    );

    // Wait for shutdown signal with periodic stats
    info!("Session ready. Press Ctrl+C to exit.");
    info!("The session will automatically reconnect if the connection drops.");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                let stats = session.stats();
                let is_ready = session.is_ready().await;
                info!(
                    ready = is_ready,
                    reconnects = stats.successful_reconnects,
                    "Session status"
                );
            }
        }
    }

    info!("Shutting down...");
    session.terminate().await?;

    info!("Session terminated");
    Ok(())
}
