//! Wire the crate's metrics hooks to a backend.
//!
//! ```sh
//! cargo run --example metrics -- i-0123456789abcdef0
//! ```

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use aws_ssm_bridge::{
    metrics::{names, register, MetricsRecorder},
    SessionBuilder,
};
use futures_util::StreamExt;

/// A recorder that just accumulates in memory. Swap the bodies for
/// `metrics::counter!` / `prometheus::Counter::inc_by` in a real application.
#[derive(Default)]
struct InMemory {
    counters: Mutex<BTreeMap<String, u64>>,
    histograms: Mutex<BTreeMap<String, Vec<f64>>>,
}

impl MetricsRecorder for InMemory {
    fn counter(&self, name: &str, value: u64) {
        *self
            .counters
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_default() += value;
    }

    fn histogram(&self, name: &str, value: f64) {
        self.histograms
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_default()
            .push(value);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let recorder = Box::leak(Box::new(InMemory::default()));
    // Registration is one-shot; a second call would be refused.
    register(Box::new(Handle(recorder))).ok();

    let target = std::env::args()
        .nth(1)
        .ok_or("usage: metrics <instance-id>")?;

    let session = SessionBuilder::new(&target).start().await?;
    let mut output = session.output();
    session.wait_ready().await?;
    session.send(&b"ls -la /\r"[..]).await?;

    let deadline = tokio::time::sleep(Duration::from_secs(3));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            chunk = output.next() => if chunk.is_none() { break },
            _ = &mut deadline => break,
        }
    }
    session.terminate().await?;

    println!("\n--- counters ---");
    for (name, value) in recorder.counters.lock().unwrap().iter() {
        println!("{name:40} {value}");
    }
    println!("--- histograms ---");
    for (name, values) in recorder.histograms.lock().unwrap().iter() {
        let mean = values.iter().sum::<f64>() / values.len().max(1) as f64;
        println!("{name:40} n={} mean={mean:.4}", values.len());
    }
    println!("(see {} and friends)", names::RTT_SECONDS);
    Ok(())
}

/// Forwards to the leaked recorder so the example can read the totals back.
struct Handle(&'static InMemory);

impl MetricsRecorder for Handle {
    fn counter(&self, name: &str, value: u64) {
        self.0.counter(name, value);
    }
    fn histogram(&self, name: &str, value: f64) {
        self.0.histogram(name, value);
    }
}
