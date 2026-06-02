//! Integration tests for port forwarding against a real AWS SSM endpoint.
//!
//! All tests in this file are `#[ignore]`-gated.  They require real AWS
//! credentials and a reachable EC2 instance with SSM agent running.
//!
//! # Required environment variables
//!
//! | Variable | Example | Description |
//! |---|---|---|
//! | `SSM_TEST_INSTANCE_ID` | `i-0123456789abcdef0` | EC2 instance ID |
//! | `SSM_TEST_REMOTE_PORT` | `80` | Port accessible on the instance |
//! | `AWS_DEFAULT_REGION` | `eu-central-1` | AWS region (or configured via profile) |
//!
//! # Running
//!
//! ```bash
//! SSM_TEST_INSTANCE_ID=i-0123456789abcdef0 \
//! SSM_TEST_REMOTE_PORT=80 \
//! AWS_DEFAULT_REGION=eu-central-1 \
//! cargo test --test integration_ssm_port_forward -- --ignored --nocapture
//! ```
//!
//! Run a single test:
//!
//! ```bash
//! ... cargo test --test integration_ssm_port_forward connect -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use aws_ssm_bridge::documents::PortForwardingSession;
use aws_ssm_bridge::session::SessionManager;
use aws_ssm_bridge::shutdown::ShutdownSignal;
use aws_ssm_bridge::{PortForwardConfig, PortForwarder, SessionBuilder};

// ── Skip helper ────────────────────────────────────────────────────────────

/// Permanent AWS errors that indicate an infrastructure/configuration problem
/// rather than a library bug.  Tests should skip gracefully in these cases.
fn is_infrastructure_error(e: &aws_ssm_bridge::errors::Error) -> bool {
    let s = e.to_string();
    s.contains("TargetNotConnected")
        || s.contains("InvalidInstanceId")
        || s.contains("InvalidTarget")
        || s.contains("AccessDeniedException")
        || s.contains("UnauthorizedException")
}

/// Unwrap a `Result<Session>`, skipping the test gracefully when the error
/// indicates the target instance is unavailable or credentials lack permission.
/// Any other error is treated as a real failure and panics.
macro_rules! session_or_skip {
    ($result:expr, $label:literal) => {
        match $result {
            Ok(s) => s,
            Err(ref e) if is_infrastructure_error(e) => {
                eprintln!(
                    "SKIPPED ({}): {} — \
                     set SSM_TEST_INSTANCE_ID to a running, SSM-connected instance",
                    $label, e
                );
                return;
            }
            Err(e) => panic!("{}: unexpected error: {e}", $label),
        }
    };
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Parameters extracted from the environment.  Tests call [`env_params()`]
/// and early-return via the `?` unwrap if any variable is absent; the test is
/// skipped in that case because Rust marks an `#[ignore]` test as `ok (ignored)`.
struct EnvParams {
    instance_id: String,
    remote_port: u16,
}

/// Read required env vars.  Panics with a clear message when a variable is
/// absent so the failure is immediately actionable in CI output.
fn env_params() -> EnvParams {
    let instance_id = std::env::var("SSM_TEST_INSTANCE_ID").unwrap_or_else(|_| {
        panic!(
            "SSM_TEST_INSTANCE_ID is not set — \
             set it to an EC2 instance ID to run SSM integration tests"
        )
    });
    let remote_port_str = std::env::var("SSM_TEST_REMOTE_PORT").unwrap_or_else(|_| {
        panic!(
            "SSM_TEST_REMOTE_PORT is not set — \
             set it to a port reachable on the target instance (e.g. 80)"
        )
    });
    let remote_port: u16 = remote_port_str.parse().unwrap_or_else(|_| {
        panic!("SSM_TEST_REMOTE_PORT={remote_port_str} is not a valid port number")
    });
    EnvParams {
        instance_id,
        remote_port,
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("aws_ssm_bridge=debug")),
        )
        .try_init();
}

/// Bind a forwarder and start forwarding in a background task.
/// Returns `(local_addr, shutdown_signal, join_handle)`.
async fn spawn_forwarder(
    session: Arc<aws_ssm_bridge::Session>,
) -> (
    std::net::SocketAddr,
    ShutdownSignal,
    tokio::task::JoinHandle<aws_ssm_bridge::errors::Result<()>>,
) {
    let shutdown = ShutdownSignal::new();
    let forwarder = PortForwarder::bind(PortForwardConfig {
        local_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
    })
    .await
    .expect("bind local port");

    let local_addr = forwarder.local_addr();
    let shutdown_clone = shutdown.clone();
    let handle = tokio::spawn(async move { forwarder.forward(session, shutdown_clone).await });

    (local_addr, shutdown, handle)
}

// ── Test: session start + ready ────────────────────────────────────────────

/// Verify that the SSM port forwarding session can be started and reaches the
/// `ready` (start_publication received) state within 30 seconds.
#[tokio::test]
#[ignore = "requires real AWS credentials and SSM_TEST_INSTANCE_ID / SSM_TEST_REMOTE_PORT"]
async fn test_pf_session_starts_and_becomes_ready() {
    init_tracing();
    let p = env_params();

    let manager = SessionManager::new()
        .await
        .expect("create session manager — check AWS credentials");

    let session = session_or_skip!(
        SessionBuilder::new(&p.instance_id)
            .document(PortForwardingSession::new(p.remote_port))
            .reason("aws-ssm-bridge integration test: session_ready")
            .build_with(&manager)
            .await,
        "start SSM port forwarding session"
    );

    let ready = timeout(
        Duration::from_secs(30),
        session.wait_for_ready(Duration::from_secs(30)),
    )
    .await
    .expect("wait_for_ready did not complete within 30 s");

    assert!(ready, "session did not reach ready state within 30 s");

    session.terminate().await.expect("terminate session");
}

// ── Test: TCP connect through tunnel ──────────────────────────────────────

/// Start a port forwarding session, connect a TCP client to the local port,
/// and verify the connection is accepted (TCP handshake completes).
#[tokio::test]
#[ignore = "requires real AWS credentials and SSM_TEST_INSTANCE_ID / SSM_TEST_REMOTE_PORT"]
async fn test_pf_tcp_connect() {
    init_tracing();
    let p = env_params();

    let manager = SessionManager::new().await.expect("create session manager");

    let session = Arc::new(session_or_skip!(
        SessionBuilder::new(&p.instance_id)
            .document(PortForwardingSession::new(p.remote_port))
            .reason("aws-ssm-bridge integration test: tcp_connect")
            .build_with(&manager)
            .await,
        "start SSM port forwarding session"
    ));

    let ready = session.wait_for_ready(Duration::from_secs(30)).await;
    assert!(ready, "session not ready");

    let (local_addr, shutdown, forward_handle) = spawn_forwarder(Arc::clone(&session)).await;

    // Give the forwarder a moment to enter its accept loop.
    sleep(Duration::from_millis(200)).await;

    // TCP connect must succeed within 10 s.
    let conn = timeout(Duration::from_secs(10), TcpStream::connect(local_addr))
        .await
        .expect("TCP connect did not complete within 10 s")
        .expect("TCP connect failed");

    drop(conn); // close connection

    shutdown.shutdown();
    let _ = timeout(Duration::from_secs(5), forward_handle)
        .await
        .expect("forwarder task did not finish within 5 s");

    session.terminate().await.expect("terminate session");
}

// ── Test: round-trip data ──────────────────────────────────────────────────

/// Send an HTTP/1.0 HEAD request through the tunnel and verify we receive
/// *some* bytes back.  This exercises the full smux data path
/// (SYN → PSH → FIN) and confirms both directions of the tunnel carry data.
///
/// Requires an HTTP server (nginx, Apache, or similar) listening on
/// `SSM_TEST_REMOTE_PORT` on the target instance.
#[tokio::test]
#[ignore = "requires real AWS credentials, SSM_TEST_INSTANCE_ID / SSM_TEST_REMOTE_PORT, and an HTTP server on the remote port"]
async fn test_pf_http_roundtrip() {
    init_tracing();
    let p = env_params();

    let manager = SessionManager::new().await.expect("create session manager");

    let session = Arc::new(session_or_skip!(
        SessionBuilder::new(&p.instance_id)
            .document(PortForwardingSession::new(p.remote_port))
            .reason("aws-ssm-bridge integration test: http_roundtrip")
            .build_with(&manager)
            .await,
        "start SSM port forwarding session"
    ));

    let ready = session.wait_for_ready(Duration::from_secs(30)).await;
    assert!(ready, "session not ready");

    let (local_addr, shutdown, forward_handle) = spawn_forwarder(Arc::clone(&session)).await;
    sleep(Duration::from_millis(200)).await;

    // Send a minimal HTTP/1.0 HEAD request and read until the end of the HTTP
    // header block (\r\n\r\n).  We do NOT use read_to_end here: HTTP/1.1
    // servers with keep-alive never send a FIN after a HEAD response, so
    // waiting for EOF would deadlock against the port-forward biased select
    // (which only exits when one copy direction completes).  Breaking on
    // \r\n\r\n lets us verify the full smux data path without requiring the
    // server to close the connection.
    let result = timeout(Duration::from_secs(15), async {
        let mut conn = TcpStream::connect(local_addr).await?;
        conn.write_all(b"HEAD / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await?;

        let mut buf = Vec::with_capacity(4096);
        let mut tmp = [0u8; 1024];
        loop {
            let n = conn.read(&mut tmp).await?;
            if n == 0 {
                break; // server closed connection (FIN received)
            }
            buf.extend_from_slice(&tmp[..n]);
            // A HEAD response ends with the header terminator; stop once we
            // have the complete header block rather than waiting for a FIN.
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    })
    .await
    .expect("HTTP round-trip timed out after 15 s")
    .expect("I/O error during HTTP round-trip");

    assert!(
        !result.is_empty(),
        "received zero bytes from remote — smux data path appears broken"
    );

    // Loosely validate that something HTTP-shaped came back.
    let response_text = String::from_utf8_lossy(&result);
    assert!(
        response_text.starts_with("HTTP/"),
        "expected HTTP response, got: {response_text:.200}"
    );

    shutdown.shutdown();
    let _ = timeout(Duration::from_secs(5), forward_handle).await;
    session.terminate().await.expect("terminate session");
}

// ── Test: multiple concurrent connections ─────────────────────────────────

/// Open several TCP connections through the same smux session simultaneously
/// and verify each receives an independent response.  This exercises smux
/// stream multiplexing and the per-stream channel isolation.
#[tokio::test]
#[ignore = "requires real AWS credentials, SSM_TEST_INSTANCE_ID / SSM_TEST_REMOTE_PORT, and an HTTP server on the remote port"]
async fn test_pf_concurrent_connections() {
    init_tracing();
    let p = env_params();

    const CONCURRENCY: usize = 5;

    let manager = SessionManager::new().await.expect("create session manager");

    let session = Arc::new(session_or_skip!(
        SessionBuilder::new(&p.instance_id)
            .document(PortForwardingSession::new(p.remote_port))
            .reason("aws-ssm-bridge integration test: concurrent_connections")
            .build_with(&manager)
            .await,
        "start SSM port forwarding session"
    ));

    let ready = session.wait_for_ready(Duration::from_secs(30)).await;
    assert!(ready, "session not ready");

    let (local_addr, shutdown, forward_handle) = spawn_forwarder(Arc::clone(&session)).await;
    sleep(Duration::from_millis(200)).await;

    let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
    let mut handles = Vec::with_capacity(CONCURRENCY);

    for i in 0..CONCURRENCY {
        let b = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            b.wait().await; // all connections start simultaneously
            let result = timeout(Duration::from_secs(15), async {
                let mut conn = TcpStream::connect(local_addr).await?;
                conn.write_all(b"HEAD / HTTP/1.0\r\nHost: localhost\r\n\r\n")
                    .await?;
                let mut buf = Vec::with_capacity(4096);
                let mut tmp = [0u8; 1024];
                loop {
                    let n = conn.read(&mut tmp).await?;
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Ok::<Vec<u8>, std::io::Error>(buf)
            })
            .await
            .unwrap_or_else(|_| panic!("connection {i} timed out"))
            .unwrap_or_else(|e| panic!("connection {i} I/O error: {e}"));

            assert!(!result.is_empty(), "connection {i} received zero bytes");
            result
        }));
    }

    let results: Vec<Vec<u8>> = futures::future::try_join_all(handles)
        .await
        .expect("a concurrent connection task panicked");

    assert_eq!(
        results.len(),
        CONCURRENCY,
        "expected {CONCURRENCY} responses"
    );
    for (i, r) in results.iter().enumerate() {
        let text = String::from_utf8_lossy(r);
        assert!(
            text.starts_with("HTTP/"),
            "connection {i}: unexpected response: {text:.200}"
        );
    }

    shutdown.shutdown();
    let _ = timeout(Duration::from_secs(5), forward_handle).await;
    session.terminate().await.expect("terminate session");
}

// ── Test: graceful shutdown stops the accept loop ─────────────────────────

/// Trigger the `ShutdownSignal` while the forwarder is running and verify
/// that the forward task completes cleanly (no panic, no hang).
#[tokio::test]
#[ignore = "requires real AWS credentials and SSM_TEST_INSTANCE_ID / SSM_TEST_REMOTE_PORT"]
async fn test_pf_graceful_shutdown() {
    init_tracing();
    let p = env_params();

    let manager = SessionManager::new().await.expect("create session manager");

    let session = Arc::new(session_or_skip!(
        SessionBuilder::new(&p.instance_id)
            .document(PortForwardingSession::new(p.remote_port))
            .reason("aws-ssm-bridge integration test: graceful_shutdown")
            .build_with(&manager)
            .await,
        "start SSM port forwarding session"
    ));

    let ready = session.wait_for_ready(Duration::from_secs(30)).await;
    assert!(ready, "session not ready");

    let (_local_addr, shutdown, forward_handle) = spawn_forwarder(Arc::clone(&session)).await;
    sleep(Duration::from_millis(300)).await;

    // Trigger shutdown while no connections are active.
    shutdown.shutdown();

    let result = timeout(Duration::from_secs(5), forward_handle)
        .await
        .expect("forwarder did not shut down within 5 s after signal")
        .expect("forwarder task panicked");

    result.expect("forwarder returned an error on graceful shutdown");

    session.terminate().await.expect("terminate session");
}

// ── Test: session terminate propagates to forwarder ───────────────────────

/// Terminate the underlying SSM session while the forwarder accept loop is
/// running and verify the `forward()` future resolves (either Ok or a
/// transport error — not a hang).
#[tokio::test]
#[ignore = "requires real AWS credentials and SSM_TEST_INSTANCE_ID / SSM_TEST_REMOTE_PORT"]
async fn test_pf_session_terminate_unblocks_forwarder() {
    init_tracing();
    let p = env_params();

    let manager = SessionManager::new().await.expect("create session manager");

    let session = Arc::new(session_or_skip!(
        SessionBuilder::new(&p.instance_id)
            .document(PortForwardingSession::new(p.remote_port))
            .reason("aws-ssm-bridge integration test: terminate_unblocks")
            .build_with(&manager)
            .await,
        "start SSM port forwarding session"
    ));

    let ready = session.wait_for_ready(Duration::from_secs(30)).await;
    assert!(ready, "session not ready");

    let (_local_addr, _shutdown, forward_handle) = spawn_forwarder(Arc::clone(&session)).await;
    sleep(Duration::from_millis(300)).await;

    // Terminate the session from under the forwarder.
    session.terminate().await.expect("terminate session");

    // The forwarder must unblock within 10 s — it may return Ok or an error.
    let _ = timeout(Duration::from_secs(10), forward_handle)
        .await
        .expect("forwarder did not unblock within 10 s after session termination")
        .expect("forwarder task panicked");
}
