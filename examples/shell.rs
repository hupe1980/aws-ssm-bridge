//! Run one command in a shell session and print its output.
//!
//! ```sh
//! cargo run --example shell -- i-0123456789abcdef0 "uname -a"
//! ```

use std::time::Duration;

use aws_ssm_bridge::SessionBuilder;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aws_ssm_bridge=info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let target = args.next().ok_or("usage: shell <instance-id> [command]")?;
    let command = args.next().unwrap_or_else(|| "uname -a".into());

    let session = SessionBuilder::new(&target)
        .reason("aws-ssm-bridge shell example")
        .start()
        .await?;

    // Subscribe before sending: output produced before the subscription is not
    // replayed, so subscribing afterwards can miss the start of the response.
    let mut output = session.output();
    session.wait_ready().await?;

    println!("session {} ready", session.id());
    if let Some(version) = session.agent_version() {
        println!("agent version {version}");
    }

    // The remote pty maps CR to NL; a bare LF is not portable.
    session.send(format!("{command}\r").into_bytes()).await?;

    // Read until the remote goes quiet, or the session ends.
    loop {
        tokio::select! {
            chunk = output.next() => match chunk {
                Some(chunk) => print!("{}", String::from_utf8_lossy(&chunk)),
                None => break,
            },
            () = session.closed() => break,
            _ = tokio::time::sleep(Duration::from_secs(2)) => break,
        }
    }

    session.terminate().await?;
    println!("\nsession ended: {:?}", session.close_reason());
    Ok(())
}
