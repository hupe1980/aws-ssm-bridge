#!/usr/bin/env bash
# aws-ssm-bridge pre-commit/pre-push checks
# Run this locally before pushing to catch CI failures early
#
# Usage:
#   ./scripts/check.sh              # Run all checks
#   ./scripts/check.sh --quick      # Run only fast checks (fmt, clippy, check)
#   ./scripts/check.sh --test       # Run checks + unit tests
#   ./scripts/check.sh --integration # Run checks + all tests including integration
#   ./scripts/check.sh --release    # Run full pre-release validation
#   ./scripts/check.sh --clean      # Clean build artifacts to free disk space
#   ./scripts/check.sh --help       # Show this help message
#
# To install as pre-commit hook:
#   ln -sf ../../scripts/check.sh .git/hooks/pre-commit

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Timing
START_TIME=$(date +%s)
CHECKS_PASSED=0
CHECKS_FAILED=0
CHECKS_SKIPPED=0
WARNINGS=()

show_help() {
    cat << EOF
${BOLD}aws-ssm-bridge CI Check Script${NC}

${CYAN}Usage:${NC}
  ./scripts/check.sh [OPTIONS]

${CYAN}Options:${NC}
  --quick, -q        Run only fast checks (fmt, clippy, check)
  --test, -t         Run checks + unit tests
  --integration, -i  Run checks + all tests including integration
  --release, -r      Run full pre-release validation
  --clean, -c        Clean build artifacts to free disk space
  --verbose, -v      Show verbose output
  --no-color         Disable colored output
  --help, -h         Show this help message

${CYAN}Examples:${NC}
  ./scripts/check.sh              # Standard checks
  ./scripts/check.sh -q           # Quick pre-commit check
  ./scripts/check.sh -t           # Run with unit tests
  ./scripts/check.sh -r           # Full release validation
  ./scripts/check.sh -c           # Clean target folder

${CYAN}Pre-commit Hook:${NC}
  ln -sf ../../scripts/check.sh .git/hooks/pre-commit

EOF
    exit 0
}

# Parse arguments
QUICK=false
TEST=false
INTEGRATION=false
RELEASE=false
CLEAN=false
VERBOSE=false
NO_COLOR=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick|-q) QUICK=true; shift ;;
        --test|-t) TEST=true; shift ;;
        --integration|-i) INTEGRATION=true; shift ;;
        --release|-r) RELEASE=true; TEST=true; INTEGRATION=true; shift ;;
        --clean|-c) CLEAN=true; shift ;;
        --verbose|-v) VERBOSE=true; shift ;;
        --no-color) NO_COLOR=true; shift ;;
        --help|-h) show_help ;;
        *) echo "Unknown option: $1. Use --help for usage."; exit 1 ;;
    esac
done

# Disable colors if requested or not a TTY
if $NO_COLOR || [[ ! -t 1 ]]; then
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' BOLD='' NC=''
fi

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_ok() {
    echo -e "${GREEN}✓ $1${NC}"
    ((CHECKS_PASSED++)) || true
}

print_err() {
    echo -e "${RED}✗ $1${NC}"
    ((CHECKS_FAILED++)) || true
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_warn() {
    echo -e "${YELLOW}⚠ $1${NC}"
    WARNINGS+=("$1")
}

print_skip() {
    echo -e "${CYAN}⊘ $1 (skipped)${NC}"
    ((CHECKS_SKIPPED++)) || true
}

run_check() {
    local name="$1"
    shift
    print_step "$name"
    local start=$(date +%s)
    if "$@"; then
        local elapsed=$(($(date +%s) - start))
        if $VERBOSE; then
            print_ok "$name passed (${elapsed}s)"
        else
            print_ok "$name passed"
        fi
    else
        print_err "$name failed"
        exit 1
    fi
}

run_check_optional() {
    local name="$1"
    shift
    print_step "$name"
    if "$@"; then
        print_ok "$name passed"
    else
        print_warn "$name failed (non-blocking)"
    fi
}

print_section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  ${BOLD}$1${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# Clean build artifacts
do_clean() {
    print_section "Cleaning Build Artifacts"

    local target_dir="$ROOT_DIR/target"

    if [[ -d "$target_dir" ]]; then
        local size_before
        size_before=$(du -sh "$target_dir" 2>/dev/null | cut -f1)
        print_info "Current target folder size: $size_before"

        print_step "Running cargo clean"
        if cargo clean; then
            print_ok "Build artifacts cleaned successfully"
            print_info "Freed approximately $size_before of disk space"
        else
            print_err "Failed to clean build artifacts"
            exit 1
        fi
    else
        print_info "Target directory does not exist — nothing to clean"
    fi

    # Also clean fuzz target if present
    if [[ -d "$ROOT_DIR/fuzz/target" ]]; then
        print_step "Cleaning fuzz target"
        rm -rf "$ROOT_DIR/fuzz/target"
        print_ok "Fuzz build artifacts cleaned"
    fi

    echo ""
    print_ok "Cleanup complete!"
    exit 0
}

# Handle --clean flag early
if $CLEAN; then
    do_clean
fi

# Check for uncommitted changes
check_git_status() {
    print_step "Git Status"
    if [[ -d ".git" ]]; then
        local uncommitted
        uncommitted=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
        if [[ "$uncommitted" -gt 0 ]]; then
            print_warn "You have $uncommitted uncommitted changes"
            if $VERBOSE; then
                git status --short
            fi
        else
            print_ok "Working directory clean"
        fi
    else
        print_skip "Git Status (not a git repository)"
    fi
}

# Check if Cargo.lock is up to date
check_cargo_lock() {
    print_step "Cargo.lock Freshness"
    if cargo check --locked --quiet 2>/dev/null; then
        print_ok "Cargo.lock is up to date"
    else
        print_warn "Cargo.lock may need updating (run 'cargo update')"
    fi
}

print_section "aws-ssm-bridge CI Checks"

# Show environment info
print_info "Rust: $(rustc --version 2>/dev/null || echo 'not found')"
print_info "Cargo: $(cargo --version 2>/dev/null || echo 'not found')"

# Git status check (informational)
check_git_status

# Essential checks (always run)
run_check "Formatting" cargo fmt --all -- --check
run_check "Clippy" cargo clippy --all-targets --all-features -- -D warnings
run_check "Compilation" cargo check --all-features

# Cargo.lock freshness
check_cargo_lock

if $QUICK; then
    echo ""
    ELAPSED=$(($(date +%s) - START_TIME))
    print_ok "Quick checks passed! (${ELAPSED}s)"
    exit 0
fi

# Documentation check
print_step "Documentation"
DOC_WARNINGS=$(cargo doc --no-deps --all-features 2>&1 | grep -cE "^warning" || true)
if [[ "$DOC_WARNINGS" -gt 0 ]]; then
    print_warn "Documentation has $DOC_WARNINGS warnings"
else
    print_ok "Documentation passed"
fi

# MSRV check (requires the toolchain to be installed)
MSRV="1.81"
if rustup run "$MSRV" cargo --version &>/dev/null; then
    run_check "MSRV ($MSRV)" rustup run "$MSRV" cargo check --all-features
else
    print_skip "MSRV ($MSRV) — toolchain not installed"
    print_info "Install with: rustup toolchain install $MSRV"
fi

if $TEST; then
    print_section "Running Unit Tests"

    run_check "Unit Tests" cargo test --all-features --lib --bins
    run_check "Doc Tests" cargo test --all-features --doc
fi

if $INTEGRATION; then
    print_section "Running All Tests"

    run_check "All Tests" cargo test --all-features
fi

if $RELEASE; then
    print_section "Pre-Release Validation"

    # Check for uncommitted changes (blocking for release)
    if [[ -d ".git" ]]; then
        local_changes=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
        if [[ "$local_changes" -gt 0 ]]; then
            print_warn "Uncommitted changes detected — consider committing before release"
        fi
    fi

    # Check current branch
    print_step "Branch Check"
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    if [[ "$CURRENT_BRANCH" == "main" || "$CURRENT_BRANCH" == "master" ]]; then
        print_ok "On release branch: $CURRENT_BRANCH"
    else
        print_warn "Not on main branch (currently on: $CURRENT_BRANCH)"
    fi

    # Security audit
    if command -v cargo-audit &>/dev/null; then
        run_check "Security Audit" cargo audit
    else
        print_skip "Security Audit — cargo-audit not installed"
        print_info "Install with: cargo install cargo-audit"
    fi

    # Check for outdated dependencies
    if command -v cargo-outdated &>/dev/null; then
        print_step "Dependency Freshness"
        OUTDATED=$(cargo outdated --depth 1 2>/dev/null | grep -c "^[a-z]" || echo "0")
        if [[ "$OUTDATED" -gt 10 ]]; then
            print_warn "$OUTDATED direct dependencies have updates available"
        else
            print_ok "Dependencies reasonably up to date ($OUTDATED with updates)"
        fi
    else
        print_skip "Dependency Freshness — cargo-outdated not installed"
    fi

    # Check for duplicate dependencies
    print_step "Duplicate Dependencies"
    DUPES=$(cargo tree --duplicates 2>/dev/null | grep -cE "^[a-z]" || true)
    DUPES=${DUPES:-0}
    if [[ "$DUPES" -gt 20 ]]; then
        print_warn "Found $DUPES duplicate dependency versions (consider deduplication)"
    else
        print_ok "Duplicate dependencies within limits ($DUPES)"
    fi

    # Check for unused dependencies
    if command -v cargo-machete &>/dev/null; then
        run_check_optional "Unused Dependencies" cargo machete
    else
        print_skip "Unused Dependencies — cargo-machete not installed"
    fi

    # Semver check
    if command -v cargo-semver-checks &>/dev/null; then
        run_check_optional "Semver Compatibility" cargo semver-checks check-release
    else
        print_skip "Semver Compatibility — cargo-semver-checks not installed"
    fi

    # Verify Cargo.toml metadata for publishing
    print_step "Crate Metadata Validation"
    MISSING_METADATA=false
    METADATA_ISSUES=()
    if ! grep -q "description" Cargo.toml; then
        METADATA_ISSUES+=("missing description")
        MISSING_METADATA=true
    fi
    if ! grep -q "license" Cargo.toml; then
        METADATA_ISSUES+=("missing license")
        MISSING_METADATA=true
    fi
    if ! grep -q "repository" Cargo.toml; then
        METADATA_ISSUES+=("missing repository")
        MISSING_METADATA=true
    fi
    if ! grep -q "readme" Cargo.toml && ! [[ -f "README.md" ]]; then
        METADATA_ISSUES+=("missing README")
        MISSING_METADATA=true
    fi
    if $MISSING_METADATA; then
        for issue in "${METADATA_ISSUES[@]}"; do
            echo -e "${YELLOW}  ⚠ $issue${NC}"
        done
        print_warn "Crate has incomplete metadata"
    else
        print_ok "Crate Metadata Validation passed"
    fi

    # Version consistency check
    print_step "Version Consistency"
    CRATE_VERSION=$(grep -E "^version = " Cargo.toml | head -1 | cut -d'"' -f2)
    print_info "Crate version: $CRATE_VERSION"

    # Validate semver format
    if [[ ! "$CRATE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
        print_warn "Version '$CRATE_VERSION' may not be valid semver"
    else
        print_ok "Version format valid"
    fi

    # Check pyproject.toml version matches (may be dynamic)
    if [[ -f "pyproject.toml" ]]; then
        PY_VERSION=$(grep -E "^version = " pyproject.toml 2>/dev/null | head -1 | cut -d'"' -f2 || true)
        if [[ -z "$PY_VERSION" ]]; then
            print_info "pyproject.toml uses dynamic versioning"
        elif [[ "$PY_VERSION" != "$CRATE_VERSION" ]]; then
            print_warn "pyproject.toml version ($PY_VERSION) differs from Cargo.toml ($CRATE_VERSION)"
        else
            print_ok "Cargo.toml ↔ pyproject.toml versions match"
        fi
    fi

    # Check CHANGELOG.md
    print_step "Changelog Validation"
    if [[ -f "CHANGELOG.md" ]]; then
        if grep -q "$CRATE_VERSION" CHANGELOG.md; then
            print_ok "Changelog contains version $CRATE_VERSION"
        else
            print_warn "CHANGELOG.md does not mention version $CRATE_VERSION"
        fi
    else
        print_warn "No CHANGELOG.md found"
    fi

    # Check git tag doesn't already exist
    print_step "Git Tag Check"
    TAG_NAME="v$CRATE_VERSION"
    if git rev-parse "$TAG_NAME" &>/dev/null; then
        print_warn "Git tag $TAG_NAME already exists"
    else
        print_ok "Git tag $TAG_NAME is available"
    fi

    # Release build
    print_section "Release Build"
    run_check "Release Build" cargo build --release

    # Library size report
    print_step "Library Size Report"
    echo "  Library sizes (release build):"
    for lib in target/release/libaws_ssm_bridge.*; do
        if [[ -f "$lib" ]]; then
            SIZE_HUMAN=$(du -h "$lib" | cut -f1)
            echo "    $(basename "$lib"): $SIZE_HUMAN"
        fi
    done
    print_ok "Library Size Report complete"

    # Dry-run publish check
    print_step "Publish Dry Run"
    if cargo publish --dry-run --allow-dirty 2>/dev/null; then
        print_ok "Publish dry run passed"
    else
        print_warn "Publish dry run failed (may need fixes before publish)"
    fi

    # Python bindings check
    print_section "Python Bindings"
    if command -v maturin &>/dev/null; then
        print_step "Python Wheel Build"
        if maturin build --release --out /tmp/aws-ssm-bridge-wheels 2>&1 | tail -5; then
            print_ok "Python bindings build successfully"
            rm -rf /tmp/aws-ssm-bridge-wheels
        else
            print_warn "Python bindings build failed"
        fi
    else
        print_skip "Python Bindings — maturin not installed"
        print_info "Install with: pip install maturin"
    fi

    # Test that examples compile
    print_section "Examples & Features"
    run_check "Examples Compilation" cargo build --examples --release

    # Feature matrix check
    print_step "Feature Matrix"
    run_check "Build (no default features)" cargo check --no-default-features
    run_check "Build (interactive)" cargo check --features interactive
    run_check "Build (python)" cargo check --features python
    run_check "Build (all features)" cargo check --all-features
    print_ok "Feature Matrix passed"

    # Code quality analysis
    print_section "Code Quality Analysis"

    # Check for TODO/FIXME in code
    print_step "Code Quality Notes"
    TODO_COUNT=$(grep -rE "TODO|FIXME|XXX|HACK" --include="*.rs" src/ 2>/dev/null | wc -l | tr -d ' ' || true)
    TODO_COUNT=${TODO_COUNT:-0}
    if [[ "$TODO_COUNT" -gt 0 ]]; then
        print_info "Found $TODO_COUNT TODO/FIXME/XXX/HACK comments in src/"
        if $VERBOSE; then
            echo "  Top files with TODOs:"
            grep -rEl "TODO|FIXME|XXX|HACK" --include="*.rs" src/ 2>/dev/null | head -5 | while read -r f; do
                count=$(grep -cE "TODO|FIXME|XXX|HACK" "$f" 2>/dev/null || echo "0")
                echo "    $f: $count"
            done
        fi
    else
        print_ok "No TODO/FIXME comments found"
    fi

    # Check for unwrap() calls in non-test code
    UNWRAP_COUNT=$(grep -r "\.unwrap()" --include="*.rs" src/ 2>/dev/null | grep -v "_test\|#\[test\]\|#\[cfg(test)\]" | wc -l | tr -d ' ' || true)
    UNWRAP_COUNT=${UNWRAP_COUNT:-0}
    EXPECT_COUNT=$(grep -r "\.expect(" --include="*.rs" src/ 2>/dev/null | grep -v "_test\|#\[test\]\|#\[cfg(test)\]" | wc -l | tr -d ' ' || true)
    EXPECT_COUNT=${EXPECT_COUNT:-0}
    print_info "Found $UNWRAP_COUNT .unwrap() and $EXPECT_COUNT .expect() calls in src/ (non-test)"
    if [[ "$UNWRAP_COUNT" -gt 20 ]]; then
        print_warn "High unwrap() count ($UNWRAP_COUNT) — consider using proper error handling"
    fi

    # Check for unsafe code
    UNSAFE_COUNT=$(grep -r "unsafe " --include="*.rs" src/ 2>/dev/null | wc -l | tr -d ' ' || true)
    UNSAFE_COUNT=${UNSAFE_COUNT:-0}
    if [[ "$UNSAFE_COUNT" -gt 0 ]]; then
        print_info "Found $UNSAFE_COUNT unsafe blocks in src/"
    else
        print_ok "No unsafe code blocks found"
    fi

    # Check for panics
    PANIC_COUNT=$(grep -rE "panic!|unreachable!|unimplemented!" --include="*.rs" src/ 2>/dev/null | grep -v "_test\|#\[test\]\|#\[cfg(test)\]" | wc -l | tr -d ' ' || true)
    PANIC_COUNT=${PANIC_COUNT:-0}
    if [[ "$PANIC_COUNT" -gt 5 ]]; then
        print_warn "Found $PANIC_COUNT panic!/unreachable!/unimplemented! in non-test code"
    else
        print_info "Found $PANIC_COUNT panic macros in non-test code"
    fi
fi

# Summary
print_section "Summary"

ELAPSED=$(($(date +%s) - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo -e "  ${GREEN}Passed:${NC}  $CHECKS_PASSED"
echo -e "  ${YELLOW}Skipped:${NC} $CHECKS_SKIPPED"
echo -e "  ${RED}Failed:${NC}  $CHECKS_FAILED"
echo -e "  ${BLUE}Time:${NC}    ${MINUTES}m ${SECONDS}s"
echo ""

if [[ ${#WARNINGS[@]} -gt 0 ]]; then
    echo -e "${YELLOW}Warnings:${NC}"
    for warn in "${WARNINGS[@]}"; do
        echo -e "  • $warn"
    done
    echo ""
fi

if [[ "$CHECKS_FAILED" -gt 0 ]]; then
    print_err "Some checks failed!"
    exit 1
fi

print_ok "All checks passed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
