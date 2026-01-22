# Fuzz Testing for AWS SSM Bridge

This directory contains fuzz testing targets using `cargo-fuzz` (libFuzzer).

## Setup

```bash
# Install cargo-fuzz (requires nightly)
cargo install cargo-fuzz

# Switch to nightly for fuzzing
rustup override set nightly
```

## Running Fuzz Tests

```bash
# Fuzz the binary protocol parser (most critical)
cargo +nightly fuzz run fuzz_binary_protocol

# Fuzz the handshake parser
cargo +nightly fuzz run fuzz_handshake

# Fuzz JSON message parsing
cargo +nightly fuzz run fuzz_json_messages

# Run with specific options
cargo +nightly fuzz run fuzz_binary_protocol -- -max_len=65536 -jobs=4
```

## Targets

| Target | Description | Priority |
|--------|-------------|----------|
| `fuzz_binary_protocol` | 116-byte header + payload parsing | **Critical** |
| `fuzz_handshake` | Handshake request/response JSON | High |
| `fuzz_json_messages` | ACK, control messages | High |

## Coverage

```bash
# Generate coverage report
cargo +nightly fuzz coverage fuzz_binary_protocol
```

## Findings

Any crashes or hangs found by fuzzing should be:
1. Documented in this file
2. Added to the corpus as regression tests
3. Fixed with a unit test

### Known Issues

None yet - fuzzing in progress.
