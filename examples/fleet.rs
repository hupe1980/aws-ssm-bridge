//! Run one command across several instances concurrently, via a session pool.
//!
//! ```sh
//! cargo run --example fleet -- "uptime" i-0123456789abcdef0 i-0fedcba9876543210
//! ```

use std::time::Duration;

use aws_ssm_bridge::{PoolConfig, SessionPool};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aws_ssm_bridge=warn".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or("usage: fleet <command> <instance-id>...")?;
    let targets: Vec<String> = args.collect();
    if targets.is_empty() {
        return Err("at least one instance ID is required".into());
    }

    let pool = SessionPool::new(PoolConfig {
        max_sessions: targets.len(),
        allow_duplicate_targets: false,
        ..Default::default()
    })
    .await?;

    // Start every session concurrently: the AWS round trip dominates, so doing
    // this serially would make a ten-instance fleet ten times slower.
    let results = futures_util::future::join_all(targets.iter().map(|target| {
        let pool = &pool;
        let command = command.clone();
        async move {
            let outcome = run_one(pool, target, &command).await;
            (target.clone(), outcome)
        }
    }))
    .await;

    for (target, outcome) in results {
        println!("=== {target} ===");
        match outcome {
            Ok(output) => println!("{}", output.trim_end()),
            Err(e) => println!("failed: {e}"),
        }
    }

    pool.shutdown().await;
    Ok(())
}

async fn run_one(
    pool: &SessionPool,
    target: &str,
    command: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let session = pool.start(target).await?;
    let mut output = session.output();
    session.wait_ready().await?;
    session.send(format!("{command}\r").into_bytes()).await?;

    let mut collected = String::new();
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            chunk = output.next() => match chunk {
                Some(chunk) => collected.push_str(&String::from_utf8_lossy(&chunk)),
                None => break,
            },
            () = session.closed() => break,
            _ = &mut deadline => break,
        }
    }

    pool.terminate(session.id()).await?;
    Ok(collected)
}
