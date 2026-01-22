//! Example: Session Pool for Multiple Concurrent Sessions
//!
//! This example demonstrates how to use SessionPool for managing
//! multiple concurrent SSM sessions efficiently.
//!
//! # Features Demonstrated
//! - Creating a session pool with configurable limits
//! - Starting multiple sessions concurrently
//! - Monitoring pool statistics
//! - Graceful shutdown of all sessions
//!
//! # Running
//! ```bash
//! # Requires valid AWS credentials and target instances
//! cargo run --example session_pool -- i-instance1 i-instance2
//! ```

use aws_ssm_bridge::{
    pool::{PoolConfig, SessionPool},
    shutdown::{install_signal_handlers, ShutdownSignal},
};
use std::env;
use std::time::Duration;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse instance IDs from command line
    let instance_ids: Vec<String> = env::args().skip(1).collect();
    if instance_ids.is_empty() {
        eprintln!("Usage: session_pool <instance-id> [instance-id...]");
        eprintln!("Example: session_pool i-0123456789abcdef0 i-0987654321fedcba0");
        std::process::exit(1);
    }

    info!(count = instance_ids.len(), "Starting session pool example");

    // Set up graceful shutdown
    let shutdown = ShutdownSignal::new();
    install_signal_handlers(shutdown.clone());

    // Create session pool with custom configuration
    let pool = SessionPool::new(PoolConfig {
        max_sessions: 10,               // Maximum concurrent sessions
        allow_duplicate_targets: false, // Prevent duplicate sessions to same target
        ..Default::default()
    })
    .await?;

    info!("Session pool created");

    // Start sessions for each instance
    for instance_id in &instance_ids {
        match pool.start_session(instance_id).await {
            Ok(_session) => {
                info!(target = %instance_id, "Session started");
            }
            Err(e) => {
                warn!(target = %instance_id, error = ?e, "Failed to start session");
            }
        }
    }

    // List active sessions
    let active = pool.list_sessions().await;
    info!(sessions = ?active, "Active sessions");

    // Monitor pool statistics
    let stats = pool.stats().await;
    info!(
        active = stats.active_sessions,
        total_started = stats.total_started,
        "Pool statistics"
    );

    // Wait for shutdown signal
    info!("Press Ctrl+C to shutdown all sessions");

    // Poll stats periodically until shutdown
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                let stats = pool.stats().await;
                info!(
                    active = stats.active_sessions,
                    "Pool heartbeat"
                );
            }
        }
    }

    // Graceful shutdown
    info!("Initiating graceful shutdown...");
    pool.shutdown().await;

    info!("All sessions terminated");
    Ok(())
}
