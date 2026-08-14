//! Micro-benchmarks for the hot path: framing, digests and reordering.
//!
//! ```sh
//! cargo bench
//! ```
//!
//! These cover the per-message work the data channel does for every chunk.
//! Anything above them is dominated by network latency and is not meaningful
//! to benchmark locally.

// criterion's macros generate undocumented items.
#![allow(missing_docs)]

use aws_ssm_bridge::ack::{build_ack, IncomingBuffer, OutgoingBuffer, RttEstimate};
use aws_ssm_bridge::binary_protocol::{ClientMessage, MessageType, PayloadType};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;

/// Payload sizes spanning a keystroke, the default chunk, and a bulk transfer.
const SIZES: [usize; 5] = [1, 64, 1024, 8192, 32768];

fn message(size: usize) -> ClientMessage {
    ClientMessage::new(
        MessageType::InputStreamData,
        1,
        PayloadType::Output,
        Bytes::from(vec![0x5A; size]),
    )
}

fn serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");
    for size in SIZES {
        let msg = message(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| black_box(msg.serialize()));
        });
    }
    group.finish();
}

fn deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize");
    for size in SIZES {
        let wire = message(size).serialize();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            // Includes SHA-256 verification, which dominates at larger sizes.
            b.iter(|| black_box(ClientMessage::deserialize(wire.clone()).unwrap()));
        });
    }
    group.finish();
}

fn acknowledge(c: &mut Criterion) {
    let received = message(1024);
    c.bench_function("build_ack", |b| {
        b.iter(|| black_box(build_ack(&received, true).unwrap()));
    });
}

fn reliability(c: &mut Criterion) {
    let wire = message(1024).serialize();

    c.bench_function("outgoing_track_then_ack", |b| {
        let buffer = OutgoingBuffer::new(10_000, 3000);
        let mut sequence = 0i64;
        b.iter(|| {
            assert!(buffer.track(wire.clone(), sequence));
            assert!(buffer.acknowledge(sequence));
            sequence += 1;
        });
    });

    c.bench_function("incoming_reorder_roundtrip", |b| {
        let buffer = IncomingBuffer::new(10_000);
        let mut sequence = 0i64;
        b.iter(|| {
            let mut msg = message(64);
            msg.sequence_number = sequence;
            assert!(buffer.insert(msg));
            black_box(buffer.take(sequence));
            sequence += 1;
        });
    });

    c.bench_function("rtt_estimate_update", |b| {
        let mut rtt = RttEstimate::default();
        b.iter(|| {
            rtt.record(black_box(Duration::from_micros(1234)));
        });
    });
}

criterion_group!(benches, serialize, deserialize, acknowledge, reliability);
criterion_main!(benches);
