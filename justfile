# aws-ssm-bridge justfile
# Run `just --list` to see all available commands

set shell := ["bash", "-uc"]

# Default recipe - show help
default:
    @just --list

# ============================================================================
# Development
# ============================================================================

# Run all tests
test:
    cargo test

# Run library tests only (faster)
test-lib:
    cargo test --lib

# Run tests with output
test-verbose:
    cargo test -- --nocapture

# Run a specific test
test-one NAME:
    cargo test {{NAME}} -- --nocapture

# Run clippy lints
lint:
    cargo clippy --all-targets --all-features

# Format code
fmt:
    cargo fmt

# Check formatting without changes
fmt-check:
    cargo fmt -- --check

# Full CI check (format, lint, test)
ci: fmt-check lint test
    @echo "✅ All CI checks passed!"

# ============================================================================
# Build
# ============================================================================

# Debug build
build:
    cargo build

# Release build with optimizations
build-release:
    cargo build --release

# Build Python wheel
build-python:
    maturin build --release

# Build Python wheel for development (faster)
build-python-dev:
    maturin develop

# ============================================================================
# Documentation
# ============================================================================

# Generate Rust docs
docs:
    cargo doc --no-deps --open

# Generate docs without opening
docs-build:
    cargo doc --no-deps

# Serve docs locally (uses _config_dev.yml with no baseurl)
docs-serve:
    cd docs && jekyll serve --config _config_dev.yml --livereload

# Serve docs locally with Python (no Jekyll required)
docs-serve-simple:
    cd docs && python3 -m http.server 4000

# Build Jekyll docs for production
docs-jekyll:
    cd docs && jekyll build

# Build Jekyll docs for local testing
docs-jekyll-dev:
    cd docs && jekyll build --config _config_dev.yml

# ============================================================================
# Benchmarks
# ============================================================================

# Run all benchmarks
bench:
    cargo bench

# Run specific benchmark
bench-one NAME:
    cargo bench -- {{NAME}}

# ============================================================================
# Fuzzing (requires nightly)
# ============================================================================

# Run binary protocol fuzzer
fuzz-binary:
    cd fuzz && cargo +nightly fuzz run fuzz_binary_protocol -- -max_total_time=60

# Run handshake fuzzer
fuzz-handshake:
    cd fuzz && cargo +nightly fuzz run fuzz_handshake -- -max_total_time=60

# Run JSON messages fuzzer
fuzz-json:
    cd fuzz && cargo +nightly fuzz run fuzz_json_messages -- -max_total_time=60

# Run all fuzzers for 1 minute each
fuzz-all: fuzz-binary fuzz-handshake fuzz-json

# ============================================================================
# Security
# ============================================================================

# Run security audit
audit:
    cargo audit

# Check for outdated dependencies
outdated:
    cargo outdated

# Update dependencies
update:
    cargo update

# ============================================================================
# Examples
# ============================================================================

# Run shell session example
example-shell INSTANCE:
    cargo run --example shell_session -- {{INSTANCE}}

# Run port forwarding example
example-port-forward INSTANCE LOCAL_PORT REMOTE_PORT:
    cargo run --example port_forwarding -- {{INSTANCE}} {{LOCAL_PORT}} {{REMOTE_PORT}}

# Run session pool example
example-pool *INSTANCES:
    cargo run --example session_pool -- {{INSTANCES}}

# Run reconnecting session example
example-reconnect INSTANCE:
    cargo run --example reconnecting -- {{INSTANCE}}

# Run metrics example
example-metrics INSTANCE:
    cargo run --example metrics_session -- {{INSTANCE}}

# ============================================================================
# Release
# ============================================================================

# Create a release build and show binary size
release-size:
    cargo build --release
    @echo "Binary sizes:"
    @ls -lh target/release/*.rlib 2>/dev/null || true
    @ls -lh target/release/libaws_ssm_bridge.* 2>/dev/null || true

# Build and strip release binary
release-strip:
    cargo build --release
    strip target/release/libaws_ssm_bridge.so 2>/dev/null || true
    @echo "Stripped binary sizes:"
    @ls -lh target/release/libaws_ssm_bridge.* 2>/dev/null || true

# ============================================================================
# Cleanup
# ============================================================================

# Clean build artifacts
clean:
    cargo clean

# Clean everything including fuzz corpus
clean-all: clean
    rm -rf fuzz/target
    rm -rf fuzz/corpus

# ============================================================================
# Info
# ============================================================================

# Show project info
info:
    @echo "aws-ssm-bridge"
    @echo "=============="
    @cargo --version
    @rustc --version
    @echo ""
    @echo "Test count:"
    @cargo test --lib 2>&1 | grep -E "^test result" || true
    @echo ""
    @echo "Lines of code:"
    @find src -name '*.rs' | xargs wc -l | tail -1

# Show dependency tree
deps:
    cargo tree

# Show features
features:
    cargo tree --features compression,encryption,interactive,python -e features
