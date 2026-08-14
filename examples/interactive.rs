//! A full interactive shell, equivalent to `aws ssm start-session`.
//!
//! ```sh
//! cargo run --example interactive -- i-0123456789abcdef0
//! ```

use aws_ssm_bridge::{InteractiveConfig, InteractiveShell};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Log to stderr: stdout belongs to the remote terminal, and interleaving log
    // lines with its output would corrupt the display.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aws_ssm_bridge=warn".into()),
        )
        .init();

    let target = std::env::args()
        .nth(1)
        .ok_or("usage: interactive <instance-id>")?;

    let exit_code = InteractiveShell::new(InteractiveConfig {
        reason: Some("aws-ssm-bridge interactive example".into()),
        ..Default::default()
    })
    .run(&target)
    .await?;

    std::process::exit(exit_code.unwrap_or(0));
}
