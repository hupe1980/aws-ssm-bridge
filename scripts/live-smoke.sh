#!/usr/bin/env bash
#
# Exercise this crate against a real SSM agent.
#
# Nothing else in this repository talks to AWS: the test suite runs against a
# mock gateway that is this crate's own reading of an undocumented protocol, so
# a misreading passes every test. This is the only check that would notice.
#
#   SSM_TARGET=i-0123456789abcdef0 scripts/live-smoke.sh
#
# It starts real sessions on a real instance and costs real money, and it is
# deliberately **not** wired into CI: this project does not put AWS credentials
# into GitHub. Run it by hand before tagging a release.
#
# Environment:
#   SSM_TARGET      instance to connect to            (required)
#   SSM_SMOKE_PORT  port to forward for the tunnel check  (default 22)
#   FEATURES        cargo features                    (default interactive,kms)

set -euo pipefail

TARGET="${SSM_TARGET:-}"
PORT="${SSM_SMOKE_PORT:-22}"
FEATURES="${FEATURES:-interactive,kms}"
SENTINEL="live-smoke-$$-$(date +%s)"
LOCAL_ADDR="127.0.0.1:18022"
WORK="$(mktemp -d)"
FORWARDER=""

if [[ -z "$TARGET" ]]; then
  echo "SSM_TARGET is not set" >&2
  exit 2
fi

cleanup() {
  [[ -n "$FORWARDER" ]] && kill -INT "$FORWARDER" 2>/dev/null || true
  # Give the forwarder a moment to release its session rather than orphaning it.
  sleep 3
  kill -9 "$FORWARDER" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$1" >&2; exit 1; }
pass() { printf '\033[32mok\033[0m   %s\n' "$1"; }

# The signing line is DEBUG; without it a successful run cannot be told apart
# from one that silently fell back to an unsigned upgrade.
export RUST_LOG="${RUST_LOG:-aws_ssm_bridge=debug}"

# Without this the tracing output carries ANSI escapes *inside* field names, so
# `status=101` never appears literally and every assertion below fails. A
# failure is the safe direction, but a check that cannot pass is not a check.
export NO_COLOR=1

step "Building"
cargo build --quiet --examples --features "$FEATURES"

# ---------------------------------------------------------------------------
# 1. Shell session
# ---------------------------------------------------------------------------
step "Shell session against $TARGET"
./target/debug/examples/shell "$TARGET" "echo $SENTINEL" >"$WORK/shell.log" 2>&1 \
  || fail "the shell example exited non-zero"

grep -q "data channel upgrade signed" "$WORK/shell.log" \
  || fail "the upgrade was NOT signed — credentials or region did not resolve, so this run proves nothing about signing"
pass "upgrade signed"

grep -qE "WebSocket connected .*status=101" "$WORK/shell.log" \
  || fail "no 101 upgrade — AWS did not accept the signed request"
pass "AWS accepted the signed upgrade"

grep -qE "WebSocket connected .*attempt=1" "$WORK/shell.log" \
  || fail "the upgrade needed more than one attempt — the first was rejected"
pass "accepted on the first attempt"

grep -q "$SENTINEL" "$WORK/shell.log" \
  || fail "the remote did not echo the sentinel back"
pass "shell round-trip"

# ---------------------------------------------------------------------------
# 2. Port forwarding, on two concurrent streams
# ---------------------------------------------------------------------------
step "Port forwarding $LOCAL_ADDR -> $TARGET:$PORT"
./target/debug/examples/port_forward "$TARGET" "$PORT" "$LOCAL_ADDR" >"$WORK/fwd.log" 2>&1 &
FORWARDER=$!

for _ in $(seq 1 60); do
  grep -q "forwarding connections" "$WORK/fwd.log" 2>/dev/null && break
  kill -0 "$FORWARDER" 2>/dev/null || fail "the forwarder exited early: $(tail -3 "$WORK/fwd.log")"
  sleep 1
done
grep -q "forwarding connections" "$WORK/fwd.log" || fail "the forwarder never became ready"

grep -qE "session_type=Some\(Port\)" "$WORK/fwd.log" \
  || fail "the agent did not negotiate a Port session"
pass "Port session negotiated"

# A shell session gets no handshake at all (the agent's `standardstream` plugin
# returns RequireHandshake() == false), so this is the only place the agent
# version is observable.
AGENT_VERSION="$(sed -nE 's/.*agent_version=([0-9.]+).*/\1/p' "$WORK/fwd.log" | head -1)"
[[ -n "$AGENT_VERSION" ]] || fail "no agent version in the handshake"
pass "agent version $AGENT_VERSION"

# Two at once: a demultiplexer that ignored the stream id would still pass one.
python3 - "$LOCAL_ADDR" <<'PY' || fail "the forwarded streams did not both carry bytes"
import socket, sys, threading
host, port = sys.argv[1].split(":")
got = {}
def probe(n):
    try:
        s = socket.create_connection((host, int(port)), timeout=20)
        s.settimeout(20)
        got[n] = s.recv(128)
        s.close()
    except Exception as e:
        got[n] = f"ERROR: {e}".encode()
ts = [threading.Thread(target=probe, args=(i,)) for i in (1, 2)]
for t in ts: t.start()
for t in ts: t.join()
for n, data in sorted(got.items()):
    print(f"  stream {n}: {data[:60]!r}")
sys.exit(0 if all(d and not d.startswith(b"ERROR") for d in got.values()) else 1)
PY
pass "two concurrent forwarded streams carried bytes"

step "Shutting the forwarder down"
kill -INT "$FORWARDER"
for _ in $(seq 1 20); do kill -0 "$FORWARDER" 2>/dev/null || break; sleep 1; done
kill -0 "$FORWARDER" 2>/dev/null && fail "the forwarder ignored SIGINT"
FORWARDER=""
grep -q "releasing the local port" "$WORK/fwd.log" || fail "the port was not released on shutdown"
pass "clean shutdown"

# ---------------------------------------------------------------------------
# 3. Nothing left running on the AWS side
# ---------------------------------------------------------------------------
if command -v aws >/dev/null; then
  step "Checking for orphaned sessions"
  sleep 2
  ORPHANS="$(aws ssm describe-sessions --state Active \
      --query "length(Sessions[?Target=='$TARGET'])" --output text 2>/dev/null || echo unknown)"
  case "$ORPHANS" in
    0)        pass "no sessions left active on $TARGET" ;;
    unknown)  printf '     (skipped: describe-sessions not permitted)\n' ;;
    *)        fail "$ORPHANS session(s) still active on $TARGET" ;;
  esac
fi

printf '\n\033[32mlive smoke passed\033[0m — agent %s, features %s\n' "$AGENT_VERSION" "$FEATURES"
