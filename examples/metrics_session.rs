//! Example: Session with metrics and observability
//!
//! This example demonstrates how to use the metrics hooks for production observability.
//!
//! Run with: cargo run --example metrics_session -- <instance-id> [region]

use aws_ssm_bridge::{
    metrics::{register_metrics, MetricsRecorder},
    shutdown::{install_signal_handlers, ShutdownSignal},
    tracing_ext::span_session,
    SessionConfig, SessionManager, SessionType,
};
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Simple metrics recorder that prints to stdout
/// In production, you'd push to Prometheus, StatsD, or OpenTelemetry
struct PrintMetricsRecorder {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

impl PrintMetricsRecorder {
    fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }
}

impl MetricsRecorder for PrintMetricsRecorder {
    fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        match name {
            "ssm.messages.sent" => {
                self.messages_sent.fetch_add(value, Ordering::Relaxed);
            }
            "ssm.messages.received" => {
                self.messages_received.fetch_add(value, Ordering::Relaxed);
            }
            "ssm.bytes.sent" => {
                self.bytes_sent.fetch_add(value, Ordering::Relaxed);
            }
            "ssm.bytes.received" => {
                self.bytes_received.fetch_add(value, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn set_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}

    fn record_histogram(&self, name: &str, value: f64, _labels: &[(&str, &str)]) {
        if name == "ssm.rtt.seconds" {
            println!("[METRIC] RTT: {:.3}ms", value * 1000.0);
        }
    }
}

/// Newtype so we can implement `MetricsRecorder` for a shared `Arc`.
struct SharedRecorder(Arc<PrintMetricsRecorder>);

impl MetricsRecorder for SharedRecorder {
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        self.0.increment_counter(name, value, labels);
    }

    fn set_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.0.set_gauge(name, value, labels);
    }

    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.0.record_histogram(name, value, labels);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging with tracing
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

    // Create and register metrics recorder.  We share the same Arc so the
    // summary printed at the end reflects the actual counters incremented by
    // the library — not a separate dead instance.
    let recorder = Arc::new(PrintMetricsRecorder::new());
    register_metrics(Box::new(SharedRecorder(Arc::clone(&recorder))))
        .unwrap_or_else(|_| panic!("a metrics recorder was already registered"));

    // Setup graceful shutdown
    let shutdown = ShutdownSignal::new();
    install_signal_handlers(shutdown.clone());

    println!(
        "\nStarting SSM session to {} with observability",
        instance_id
    );

    // Create session manager
    let manager = SessionManager::new().await?;

    // Configure session
    let config = SessionConfig {
        target: instance_id.clone(),
        region,
        session_type: SessionType::StandardStream,
        reason: Some("Metrics example session".to_string()),
        ..Default::default()
    };

    // Start session
    let session = manager.start_session(config).await?;
    let session_id = session.id().to_string();
    println!("✓ Session started: {}", session_id);

    // Create tracing span for session operations
    let _span = span_session(instance_id, &session_id);

    // Get output stream
    let mut output = session.output();
    let shutdown_for_output = shutdown.clone();

    // Spawn task to print output
    let output_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_for_output.cancelled() => break,
                data = output.next() => {
                    match data {
                        Some(data) => print!("{}", String::from_utf8_lossy(&data)),
                        None => break,
                    }
                }
            }
        }
    });

    // Wait for session to be ready
    println!("\nWaiting for session to be protocol-ready...");
    let ready = session
        .wait_for_ready(tokio::time::Duration::from_secs(30))
        .await;

    if !ready {
        println!("⚠ Session did not become ready within timeout");
    } else {
        println!("✓ Session protocol ready");
    }

    // Send test commands
    println!("\nSending test commands...");
    for i in 1..=3 {
        session
            .send(bytes::Bytes::from(format!("echo 'Test command {}'\n", i)))
            .await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Print metrics summary
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("\n--- Metrics Summary ---");
    println!(
        "Messages sent: {}",
        recorder.messages_sent.load(Ordering::Relaxed)
    );
    println!(
        "Messages received: {}",
        recorder.messages_received.load(Ordering::Relaxed)
    );
    println!(
        "Bytes sent: {}",
        recorder.bytes_sent.load(Ordering::Relaxed)
    );
    println!(
        "Bytes received: {}",
        recorder.bytes_received.load(Ordering::Relaxed)
    );
    println!("-----------------------\n");

    // Wait for Ctrl+C or session end
    println!("Press Ctrl+C to exit...\n");
    shutdown.cancelled().await;

    // Cleanup
    println!("\nShutting down...");
    session.terminate().await?;
    output_task.abort();

    println!("Session terminated");
    Ok(())
}
