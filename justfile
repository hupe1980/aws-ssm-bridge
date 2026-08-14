# aws-ssm-bridge development tasks
#
#   just --list      show everything
#   just check       what CI runs, locally

set shell := ["bash", "-uc"]

# `--all-features` would enable `extension-module`, which leaves the CPython
# symbols unresolved for the interpreter to supply at load time — right for a
# wheel, fatal for a test binary. Name the features instead.
FEATURES := "interactive,kms"

# An instance to run the live checks against. Override per invocation:
#   just live-shell TARGET=i-0123456789abcdef0
TARGET := env_var_or_default("SSM_TARGET", "")

_default:
    @just --list

# ---------------------------------------------------------------------------
# The main loop
# ---------------------------------------------------------------------------

# Everything CI checks, in the order it fails fastest.
check: fmt-check lint test doc

# Run every test: unit, integration and doc.
test:
    cargo test --all-targets --features {{FEATURES}}
    cargo test --doc --features {{FEATURES}}

# Unit tests only — the fast inner loop.
test-lib:
    cargo test --lib --features {{FEATURES}}

# Run one test by name, with output.
test-one NAME:
    cargo test --features {{FEATURES}} {{NAME}} -- --nocapture

# Clippy with warnings denied, as CI does.
lint:
    cargo clippy --all-targets --features {{FEATURES}} -- -D warnings

# Format everything in place.
fmt:
    cargo fmt --all

# Fail if anything is unformatted, as CI does.
fmt-check:
    cargo fmt --all -- --check

# Build the docs and fail on any rustdoc warning.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features {{FEATURES}}

# Build the docs and open them.
doc-open:
    cargo doc --no-deps --features {{FEATURES}} --open

# ---------------------------------------------------------------------------
# Feature matrix
# ---------------------------------------------------------------------------

# Regressions here are usually a missing `#[cfg]` on something only one feature
# needs.

# Build and unit-test every feature combination.
matrix:
    cargo test --lib --no-default-features
    cargo test --lib --no-default-features --features kms
    cargo test --lib --no-default-features --features interactive
    cargo test --lib --no-default-features --features interactive,kms
    # `python` without `extension-module` links against libpython, so the
    # bindings are type-checked here rather than only at wheel-build time.
    cargo clippy --lib --features python -- -D warnings
    cargo clippy --lib --features python,extension-module -- -D warnings

# ---------------------------------------------------------------------------
# Benchmarks and fuzzing
# ---------------------------------------------------------------------------

# Framing and reliability micro-benchmarks.
bench:
    cargo bench --features {{FEATURES}}

# Fuzz one parser until you stop it (needs nightly and cargo-fuzz).
fuzz TARGET="fuzz_binary_protocol":
    cargo +nightly fuzz run {{TARGET}}

# A short run of every fuzz target, as CI does.
fuzz-smoke:
    for t in fuzz_binary_protocol fuzz_handshake fuzz_acknowledge; do \
        cargo +nightly fuzz run "$t" -- -max_total_time=60; \
    done

# ---------------------------------------------------------------------------
# Python
# ---------------------------------------------------------------------------

# Build the extension and install it into the active virtualenv.
python:
    maturin develop --release

# Build a release wheel.
python-wheel:
    maturin build --release

# ---------------------------------------------------------------------------
# Live checks
# ---------------------------------------------------------------------------
#
# These talk to real AWS and cost real sessions. Set SSM_TARGET first:
#   export SSM_TARGET=i-0123456789abcdef0

_require-target:
    @[ -n "{{TARGET}}" ] || { echo "set SSM_TARGET, or pass TARGET=i-…"; exit 1; }

# Run a command on the target and print the output.
live-shell CMD="uname -a": _require-target
    RUST_LOG=aws_ssm_bridge=info cargo run --example shell --features {{FEATURES}} -- {{TARGET}} "{{CMD}}"

# Forward a port from the target. Ctrl-C to stop.
live-forward PORT="22" LOCAL="127.0.0.1:0": _require-target
    RUST_LOG=aws_ssm_bridge=info cargo run --example port_forward --features {{FEATURES}} -- {{TARGET}} {{PORT}} {{LOCAL}}

# Open an interactive shell on the target.
live-interactive: _require-target
    cargo run --example interactive --features {{FEATURES}} -- {{TARGET}}

# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------

# Keep VERSION in step with `rust-version` in Cargo.toml. CI pins the same one,
# and a drift between the two makes the check meaningless.
#
# `--locked` is the whole point: without it cargo re-resolves to the newest
# semver-compatible dependencies, which routinely need a newer toolchain than
# the lockfile users actually get — so the check fails for a reason that has
# nothing to do with the declared MSRV.

# Build against the oldest supported toolchain.
msrv VERSION="1.94.1":
    @rustup run {{VERSION}} cargo --version >/dev/null 2>&1 \
        || { echo "install it first: rustup toolchain install {{VERSION}}"; exit 1; }
    rustup run {{VERSION}} cargo check --locked --features {{FEATURES}}

# The optional tools are skipped rather than fatal, so this runs usefully on a
# fresh checkout: cargo install cargo-semver-checks cargo-machete

# Everything that must hold before cutting a release.
release-check: check matrix audit msrv
    # The lockfile must already satisfy the manifest — a release that silently
    # resolves new versions is not the thing that was tested.
    cargo check --locked --features {{FEATURES}}
    cargo build --release --features {{FEATURES}}
    cargo publish --dry-run --features {{FEATURES}}
    @command -v cargo-semver-checks >/dev/null \
        && cargo semver-checks check-release \
        || echo "skipped: cargo-semver-checks not installed"
    @command -v cargo-machete >/dev/null \
        && cargo machete \
        || echo "skipped: cargo-machete not installed"
    @command -v maturin >/dev/null \
        && maturin build --release \
        || echo "skipped: maturin not installed"

# ---------------------------------------------------------------------------
# Documentation site
# ---------------------------------------------------------------------------
#
# The site under `site/` is built with Zola and published to GitHub Pages by
# .github/workflows/pages.yml. Install it with `brew install zola` or from
# https://www.getzola.org.

# Serve the site locally with live reload.
site PORT="1111":
    cd site && zola serve --port {{PORT}}

# Build the site into site/public.
site-build:
    cd site && zola build

# Fail on any internal link to a page that does not exist.
site-check:
    cd site && zola check --skip-external-links

# Re-render the Open Graph card from its SVG source (needs librsvg).
social-card:
    rsvg-convert -w 1200 -h 630 site/design/social-card.svg -o site/static/social-card.png

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

# Check dependencies for known advisories.
audit:
    cargo audit

# Everything before opening a pull request.
pre-commit: fmt check matrix audit site-check

# Remove build artefacts, including the fuzz and docs output.
clean:
    cargo clean
    rm -rf fuzz/target site/public
