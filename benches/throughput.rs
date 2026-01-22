use aws_ssm_bridge::binary_protocol::{ClientMessage, PayloadType};
use aws_ssm_bridge::errors::*;
use aws_ssm_bridge::protocol::MessageType;
use bytes::Bytes;
/// Performance benchmarks for aws-ssm-bridge
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;

/// Benchmark binary message serialization
fn bench_message_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_serialization");

    // Benchmark different payload sizes
    for size in [64, 256, 1024, 4096, 16384].iter() {
        let payload = vec![0u8; *size];
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let msg = ClientMessage::new(
                    MessageType::InputStreamData,
                    black_box(1),
                    PayloadType::Output,
                    black_box(Bytes::from(payload.clone())),
                );
                black_box(msg.serialize().unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark binary message deserialization
fn bench_message_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_deserialization");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let payload = vec![0u8; *size];
        let msg = ClientMessage::new(
            MessageType::InputStreamData,
            1,
            PayloadType::Output,
            Bytes::from(payload),
        );
        let serialized = msg.serialize().unwrap();

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let parsed = ClientMessage::deserialize(black_box(serialized.clone())).unwrap();
                black_box(parsed)
            });
        });
    }

    group.finish();
}

/// Benchmark Base64 encoding/decoding (still used in some JSON payloads)
fn bench_base64_operations(c: &mut Criterion) {
    use base64::Engine;
    let mut group = c.benchmark_group("base64_operations");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let data = vec![0u8; *size];
        group.throughput(Throughput::Bytes(*size as u64));

        // Encoding
        group.bench_with_input(BenchmarkId::new("encode", size), size, |b, &_size| {
            b.iter(|| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(black_box(&data));
                black_box(encoded)
            });
        });

        // Decoding
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
        group.bench_with_input(BenchmarkId::new("decode", size), size, |b, &_size| {
            b.iter(|| {
                let decoded = base64::engine::general_purpose::STANDARD.decode(black_box(&encoded));
                black_box(decoded)
            });
        });
    }

    group.finish();
}

/// Benchmark error classification
fn bench_error_classification(c: &mut Criterion) {
    let mut group = c.benchmark_group("error_classification");

    group.bench_function("is_retriable_timeout", |b| {
        let error = Error::Timeout;
        b.iter(|| black_box(error.is_retriable()));
    });

    group.bench_function("is_fatal_transport", |b| {
        let error = Error::Transport(TransportError::HeartbeatTimeout);
        b.iter(|| black_box(error.is_fatal()));
    });

    group.finish();
}

/// Benchmark SHA-256 digest computation (used for message integrity)
fn bench_sha256_digest(c: &mut Criterion) {
    use sha2::{Digest, Sha256};
    let mut group = c.benchmark_group("sha256_digest");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let data = vec![0u8; *size];
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                let mut hasher = Sha256::new();
                hasher.update(black_box(&data));
                black_box(hasher.finalize())
            });
        });
    }

    group.finish();
}

/// Benchmark message validation (digest verification)
fn bench_message_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_validation");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let payload = vec![0u8; *size];
        let msg = ClientMessage::new(
            MessageType::OutputStreamData,
            1,
            PayloadType::Output,
            Bytes::from(payload),
        );

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
            b.iter(|| {
                msg.validate().unwrap();
                black_box(())
            });
        });
    }

    group.finish();
}

/// Configuration for benchmarks
fn configure_benchmarks() -> Criterion {
    Criterion::default()
        .sample_size(100)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(2))
}

criterion_group! {
    name = benches;
    config = configure_benchmarks();
    targets =
        bench_message_serialization,
        bench_message_deserialization,
        bench_base64_operations,
        bench_error_classification,
        bench_sha256_digest,
        bench_message_validation
}

criterion_main!(benches);
