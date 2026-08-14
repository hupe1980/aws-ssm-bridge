//! A session that rebuilds itself when the connection drops.
//!
//! ```sh
//! cargo run --example reconnecting -- i-0123456789abcdef0
//! ```
//!
//! Kill the instance's network (or suspend the laptop) to watch it recover.
//! Note that a reconnect starts a *new* shell: working directory, environment
//! and running jobs are not preserved.

use std::time::Duration;

use aws_ssm_bridge::{ReconnectConfig, ReconnectEvent, ReconnectingSession};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aws_ssm_bridge=info".into()),
        )
        .init();

    let target = std::env::args()
        .nth(1)
        .ok_or("usage: reconnecting <instance-id>")?;

    let session = ReconnectingSession::connect(
        &target,
        ReconnectConfig {
            max_attempts: 0, // keep trying indefinitely
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await?;

    // This stream spans reconnects; the sessions underneath it do not.
    let mut output = session.output();
    tokio::spawn(async move {
        while let Some(chunk) = output.next().await {
            print!("{}", String::from_utf8_lossy(&chunk));
        }
    });

    let mut events = session.events();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                ReconnectEvent::Disconnected { reason } => eprintln!("!! disconnected: {reason}"),
                ReconnectEvent::Reconnecting { attempt, delay } => {
                    eprintln!("   attempt {attempt} in {delay:?}");
                }
                ReconnectEvent::Reconnected {
                    session_id,
                    attempts,
                } => eprintln!("++ reconnected as {session_id} after {attempts} attempts"),
                ReconnectEvent::GaveUp { reason, .. } => eprintln!("xx gave up: {reason}"),
                // ReconnectEvent is #[non_exhaustive]; new variants are
                // informational and should not break this handler.
                other => eprintln!("   {other:?}"),
            }
        }
    });

    // Print the date every five seconds so a reconnect is visible in the output.
    for _ in 0..24 {
        if let Err(e) = session.send(&b"date\r"[..]).await {
            eprintln!("send failed: {e}");
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    session.terminate().await?;
    Ok(())
}
