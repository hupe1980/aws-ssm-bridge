//! End-to-end port forwarding against a mock gateway that speaks smux.
//!
//! These cover the layer nothing else does: `SmuxSession`, `SmuxStream` and
//! `PortForwarder::forward` moving real bytes over a real WebSocket, through
//! real TCP sockets.
//!
//! The gateway's smux framing is transcribed from `xtaci/smux` rather than from
//! this crate's encoder, so a framing mistake here would show up as a failure
//! rather than as agreement.

mod common;

use std::sync::Arc;
use std::time::Duration;

use aws_ssm_bridge::documents::{PortForwardingSession, SsmDocument};
use aws_ssm_bridge::mux::StreamCloseReason;
use aws_ssm_bridge::{
    DocumentSpec, EndpointPolicy, PortForwardConfig, PortForwarder, Session, SessionConfig,
    ShutdownSignal, SmuxConfig, SmuxSession,
};
use common::{Behaviour, MockGateway};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A config for a **port-forwarding** session pointed at a local mock.
///
/// The document is what decides `SessionType`, and `PortForwarder` checks it
/// before anything else.
fn port_config() -> SessionConfig {
    SessionConfig {
        endpoint_policy: EndpointPolicy::AllowAny,
        ready_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_millis(500),
        idle_timeout: Duration::from_secs(5),
        document: Some(DocumentSpec::new(&PortForwardingSession::new(5432))),
        ..SessionConfig::new("i-0123456789abcdef0")
    }
}

async fn port_session(gateway: &MockGateway) -> Arc<Session> {
    let session = Session::attach("mock-session", gateway.url(), "mock-token", port_config())
        .await
        .expect("the data channel must open");
    session.wait_ready().await.expect("handshake must complete");
    Arc::new(session)
}

/// Read exactly `n` bytes, failing the test rather than hanging forever.
async fn read_exactly<R: AsyncReadExt + Unpin>(reader: &mut R, n: usize, what: &str) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|e| panic!("reading {what} failed: {e}"));
    buf
}

// ---------------------------------------------------------------------------
// The multiplexer
// ---------------------------------------------------------------------------

/// Two streams over one session keep their bytes to themselves.
///
/// This is the whole point of multiplexing, and until now nothing demonstrated
/// it: a demultiplexer that ignored the stream id would pass every other test
/// in the suite.
#[tokio::test]
async fn concurrent_streams_do_not_interleave() {
    let gateway = MockGateway::start(Behaviour::SmuxEcho).await;
    let session = port_session(&gateway).await;
    let mux = SmuxSession::new(Arc::clone(&session), SmuxConfig::default());

    let mut first = mux.open_stream().expect("stream one");
    let mut second = mux.open_stream().expect("stream two");
    assert_ne!(first.id(), second.id(), "streams must have distinct ids");

    // Same length, different content: a demultiplexer that crossed the wires
    // would still satisfy a length check.
    first.write_all(b"aaaaaaaaaa").await.expect("write one");
    second.write_all(b"bbbbbbbbbb").await.expect("write two");

    assert_eq!(
        read_exactly(&mut first, 10, "stream one").await,
        b"aaaaaaaaaa"
    );
    assert_eq!(
        read_exactly(&mut second, 10, "stream two").await,
        b"bbbbbbbbbb"
    );

    session.terminate().await.expect("terminate");
}

/// A stream whose consumer stops reading is evicted, and **the other streams
/// keep flowing**.
///
/// This is the assertion the design argument rests on. smux v1 has no per-stream
/// flow control, so the multiplexer's only options are to block everyone or to
/// drop the one stream that is not keeping up. It drops that one — and if it
/// ever blocked instead, a single stalled consumer would freeze every other
/// connection sharing the tunnel, which is exactly what this test would catch.
///
/// Eviction is observable only after the reader drains what was buffered and
/// reaches EOF, so the flood has to happen *before* the first read.
#[tokio::test]
async fn a_stalled_stream_is_evicted_and_the_others_keep_flowing() {
    let gateway = MockGateway::start(Behaviour::SmuxFlood).await;
    let session = port_session(&gateway).await;
    let mux = SmuxSession::new(Arc::clone(&session), SmuxConfig::default());

    // The gateway floods the first stream it sees opened. Nothing reads it.
    let mut stalled = mux.open_stream().expect("the stalled stream");

    // A second stream, opened after the flood was queued. Its echo arriving is
    // proof the whole flood has already been processed — the session delivers
    // in order — so the eviction has happened by the time this returns.
    let mut healthy = mux.open_stream().expect("the healthy stream");
    healthy.write_all(b"still-here").await.expect("write");
    assert_eq!(
        read_exactly(&mut healthy, 10, "the healthy stream").await,
        b"still-here",
        "one stalled stream must not stop another"
    );

    // Now drain the stalled stream. It yields what was buffered and then ends.
    let mut drained = 0usize;
    let mut buf = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(5), stalled.read(&mut buf))
            .await
            .expect("draining the stalled stream must not hang")
            .expect("reading the stalled stream must not error");
        if n == 0 {
            break;
        }
        drained += n;
    }

    assert_eq!(
        stalled.close_reason(),
        Some(StreamCloseReason::SlowConsumer),
        "the stalled stream must report that bytes were lost, not a clean close"
    );
    assert!(
        drained < common::FLOOD_FRAMES * 4,
        "eviction must actually drop data: drained {drained} of {} bytes sent",
        common::FLOOD_FRAMES * 4
    );

    session.terminate().await.expect("terminate");
}

// ---------------------------------------------------------------------------
// The forwarder
// ---------------------------------------------------------------------------

/// Bind, accept, and move bytes both ways over a real TCP socket.
#[tokio::test]
async fn a_forwarded_connection_round_trips() {
    let gateway = MockGateway::start(Behaviour::SmuxEcho).await;
    let session = port_session(&gateway).await;
    let shutdown = ShutdownSignal::new();

    let forwarder = PortForwarder::bind(PortForwardConfig::default())
        .await
        .expect("bind");
    let local = forwarder.local_addr();
    let forwarding = tokio::spawn(forwarder.forward(Arc::clone(&session), shutdown.clone()));

    let mut client = TcpStream::connect(local).await.expect("connect");
    client.write_all(b"SELECT 1").await.expect("write");
    assert_eq!(read_exactly(&mut client, 8, "the echo").await, b"SELECT 1");

    shutdown.shutdown();
    forwarding.await.expect("join").expect("forward");
    session.terminate().await.expect("terminate");
}

/// Closing one direction must not truncate the reply on the other.
///
/// HTTP without keep-alive, `nc`, and anything shell-piped all signal "request
/// finished" by shutting down their write half and then reading. A forwarder
/// that tore the connection down on the first EOF would lose the response.
#[tokio::test]
async fn half_close_does_not_truncate_the_reply() {
    let gateway = MockGateway::start(Behaviour::SmuxEcho).await;
    let session = port_session(&gateway).await;
    let shutdown = ShutdownSignal::new();

    let forwarder = PortForwarder::bind(PortForwardConfig::default())
        .await
        .expect("bind");
    let local = forwarder.local_addr();
    let forwarding = tokio::spawn(forwarder.forward(Arc::clone(&session), shutdown.clone()));

    let mut client = TcpStream::connect(local).await.expect("connect");
    client.write_all(b"request").await.expect("write");
    client.shutdown().await.expect("half-close the write side");

    // The reply has to survive the local half-close.
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut reply))
        .await
        .expect("reading the reply must not hang")
        .expect("read");
    assert_eq!(reply, b"request", "the reply must survive a half-close");

    shutdown.shutdown();
    forwarding.await.expect("join").expect("forward");
    session.terminate().await.expect("terminate");
}

/// Over the limit, a connection is refused immediately rather than queued.
///
/// An unbounded queue would turn "too many connections" into "every connection
/// is slow", which is far harder to diagnose than a reset.
#[tokio::test]
async fn the_connection_limit_refuses_rather_than_queueing() {
    let gateway = MockGateway::start(Behaviour::SmuxEcho).await;
    let session = port_session(&gateway).await;
    let shutdown = ShutdownSignal::new();

    let forwarder = PortForwarder::bind(PortForwardConfig {
        max_connections: 1,
        ..PortForwardConfig::default()
    })
    .await
    .expect("bind");
    let local = forwarder.local_addr();
    let forwarding = tokio::spawn(forwarder.forward(Arc::clone(&session), shutdown.clone()));

    // The first connection takes the only permit and keeps it.
    let mut first = TcpStream::connect(local).await.expect("connect one");
    first.write_all(b"holding").await.expect("write");
    assert_eq!(
        read_exactly(&mut first, 7, "the first echo").await,
        b"holding"
    );

    // The second is accepted by the OS and then dropped by the forwarder, so it
    // reaches EOF without ever carrying data.
    let mut second = TcpStream::connect(local).await.expect("connect two");
    let mut buf = Vec::new();
    let refused = tokio::time::timeout(Duration::from_secs(5), second.read_to_end(&mut buf)).await;
    match refused {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        other => panic!("the second connection should have been refused, got {other:?}"),
    }
    assert!(buf.is_empty(), "a refused connection must carry no data");

    shutdown.shutdown();
    forwarding.await.expect("join").expect("forward");
    session.terminate().await.expect("terminate");
}

/// A shell session handed to the forwarder is refused up front.
///
/// The agent only speaks smux for port-forwarding documents, so forwarding over
/// a shell session would frame terminal output as smux frames and hang. Failing
/// at `forward()` turns a silent hang into one sentence.
#[tokio::test]
async fn a_shell_session_is_refused_before_it_can_hang() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = Session::attach(
        "mock-session",
        gateway.url(),
        "mock-token",
        SessionConfig {
            endpoint_policy: EndpointPolicy::AllowAny,
            ready_timeout: Duration::from_secs(5),
            ..SessionConfig::new("i-0123456789abcdef0")
        },
    )
    .await
    .expect("the data channel must open");

    let forwarder = PortForwarder::bind(PortForwardConfig::default())
        .await
        .expect("bind");
    // Bounded on purpose. Without the guard, `forward` does not return an error
    // — it waits forever on a stream it will never parse as smux, which is the
    // hang the guard exists to prevent. An unbounded await here would reproduce
    // that hang in CI instead of reporting it.
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        forwarder.forward(Arc::new(session), ShutdownSignal::new()),
    )
    .await
    .expect("forward() must refuse a shell session rather than hang on it")
    .expect_err("a shell session must be refused");

    let message = error.to_string();
    assert!(
        message.contains("port-forwarding session"),
        "the error must name the problem: {message}"
    );
    assert!(
        message.contains("port_forward") || message.contains("PortForwardingToRemoteHost"),
        "the error must name a way to fix it: {message}"
    );
}

/// The document really is what decides the session type — the guard above is
/// only as good as this.
#[test]
fn the_port_forwarding_document_produces_a_port_session() {
    use aws_ssm_bridge::SessionType;
    assert_eq!(
        PortForwardingSession::new(5432).session_type(),
        SessionType::Port
    );
    assert_eq!(port_config().session_type(), SessionType::Port);
}
