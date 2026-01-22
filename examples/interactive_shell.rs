//! Example: Interactive shell with full terminal support
//!
//! This example demonstrates a true interactive shell session with an EC2 instance,
//! with proper terminal handling (raw mode, resize, signals).
//!
//! ## Features
//! - Raw terminal mode (no echo, immediate input)
//! - Terminal resize detection (SIGWINCH on Unix)
//! - Signal forwarding (Ctrl+C, Ctrl+D, Ctrl+Z)
//! - Automatic terminal restoration on exit/crash
//!
//! ## Usage
//! ```bash
//! cargo run --example interactive_shell -- i-0123456789abcdef0 [region]
//! ```
//!
//! Press Ctrl+D to exit the session cleanly.

use aws_ssm_bridge::interactive::{InteractiveConfig, InteractiveShell};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging (set RUST_LOG=debug for verbose output)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::WARN.into()),
        )
        .init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Interactive SSM Shell");
        eprintln!();
        eprintln!("Usage: {} <instance-id> [region]", args[0]);
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} i-0123456789abcdef0", args[0]);
        eprintln!("  {} i-0123456789abcdef0 us-west-2", args[0]);
        eprintln!();
        eprintln!("Controls:");
        eprintln!("  Ctrl+D  - Exit session");
        eprintln!("  Ctrl+C  - Send interrupt signal");
        eprintln!("  Ctrl+Z  - Suspend current process");
        std::process::exit(1);
    }

    let instance_id = &args[1];
    let _region = args.get(2).cloned();

    // Option 1: Quick one-liner (uses default config)
    // run_shell(instance_id).await?;

    // Option 2: Full control with custom configuration
    let config = InteractiveConfig {
        show_banner: true,       // Show session ID on connect
        send_initial_size: true, // Send terminal dimensions on connect
        forward_signals: true,   // Forward Ctrl+C/Z to remote
        ..Default::default()
    };

    let mut shell = InteractiveShell::new(config)?;

    // Connect to instance
    shell.connect(instance_id).await?;

    // Run interactive session (blocks until Ctrl+D or session closes)
    // Terminal is automatically restored on exit, even on panic
    shell.run().await?;

    Ok(())
}
