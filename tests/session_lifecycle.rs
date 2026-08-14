//! End-to-end tests against a mock gateway.
//!
//! These drive a real [`Session`] over a real WebSocket, so they cover the
//! layers that unit tests cannot: framing, sequencing, acknowledgement,
//! reordering, the handshake, and — most importantly — that the session ends
//! and says why whenever something goes wrong.

mod common;

use std::time::Duration;

use aws_ssm_bridge::{CloseReason, EndpointPolicy, Session, SessionConfig};
use common::{Behaviour, MockGateway};
use futures_util::StreamExt;

/// Build a config pointed at a local mock rather than AWS.
fn config() -> SessionConfig {
    SessionConfig {
        endpoint_policy: EndpointPolicy::AllowAny,
        ready_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_millis(200),
        idle_timeout: Duration::from_millis(700),
        ..SessionConfig::new("i-0123456789abcdef0")
    }
}

async fn connect(gateway: &MockGateway) -> Session {
    Session::attach("mock-session", gateway.url(), "mock-token", config())
        .await
        .expect("the data channel must open")
}

/// The whole happy path: handshake, banner, send, echo, terminate.
#[tokio::test]
async fn handshake_completes_and_data_round_trips() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;

    let mut output = session.output();
    session.wait_ready().await.expect("handshake must complete");

    assert!(session.is_ready());
    assert!(!session.is_closed());
    assert_eq!(session.agent_version(), Some("3.3.1345.0"));
    assert_eq!(session.banner().as_deref(), Some("mock gateway ready"));

    session.send(&b"echo hello\r"[..]).await.unwrap();

    let echoed = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the echo must arrive")
        .expect("the stream must not end");
    assert_eq!(&echoed[..], b"echo hello\r");

    session.terminate().await.unwrap();
    assert_eq!(session.close_reason(), Some(CloseReason::Terminated));
}

/// The token belongs in the open message, never in the URL: a URL is logged by
/// proxies and recorded in traces.
#[tokio::test]
async fn the_session_token_is_sent_in_the_open_message_only() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;
    session.wait_ready().await.unwrap();

    let observed = gateway.observed().await;
    let open = observed.open_message.expect("an open message is required");
    let parsed: serde_json::Value = serde_json::from_str(&open).unwrap();

    assert_eq!(parsed["TokenValue"], "mock-token");
    assert_eq!(parsed["ClientId"], "mock-session");
    assert_eq!(parsed["MessageSchemaVersion"], "1.0");
    assert!(parsed["RequestId"].is_string());
    assert!(
        !gateway.url().contains("mock-token"),
        "the token must not appear in the stream URL"
    );

    session.terminate().await.unwrap();
}

/// The handshake response must carry a plugin version the agent recognises,
/// otherwise it silently refuses to multiplex port forwards.
#[tokio::test]
async fn the_handshake_response_reports_a_supported_client_version() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;
    session.wait_ready().await.unwrap();

    let observed = gateway
        .wait_for(Duration::from_secs(5), "handshake response", |o| {
            o.handshake_response.is_some()
        })
        .await;
    let response: serde_json::Value =
        serde_json::from_str(&observed.handshake_response.unwrap()).unwrap();

    assert_eq!(
        response["ClientVersion"],
        aws_ssm_bridge::handshake::CLIENT_PROTOCOL_VERSION
    );
    assert_eq!(response["ProcessedClientActions"][0]["ActionStatus"], 1);

    session.terminate().await.unwrap();
}

/// Data queued before the handshake finishes must be held, not dropped: the
/// gateway discards anything sent before it is ready, which looks to the caller
/// like lost keystrokes.
#[tokio::test]
async fn data_sent_before_the_handshake_is_delivered_not_dropped() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;

    // Deliberately no wait_ready() first.
    session.send(&b"queued-before-ready\r"[..]).await.unwrap();

    let observed = gateway
        .wait_for(Duration::from_secs(5), "the queued input", |o| {
            !o.inputs.is_empty()
        })
        .await;
    assert_eq!(observed.inputs[0], b"queued-before-ready\r");

    session.terminate().await.unwrap();
}

/// Large writes are split into protocol-sized chunks and must reassemble in
/// order on the far side.
#[tokio::test]
async fn large_writes_are_chunked_and_arrive_in_order() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = Session::attach(
        "mock-session",
        gateway.url(),
        "mock-token",
        SessionConfig {
            payload_chunk_size: 64,
            ..config()
        },
    )
    .await
    .unwrap();
    session.wait_ready().await.unwrap();

    let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
    session.send(payload.clone()).await.unwrap();

    let observed = gateway
        .wait_for(Duration::from_secs(5), "every chunk", |o| {
            o.inputs.iter().map(Vec::len).sum::<usize>() >= payload.len()
        })
        .await;

    assert!(observed.inputs.len() >= 16, "1000 bytes / 64 = 16 chunks");
    assert!(
        observed.inputs.iter().all(|c| c.len() <= 64),
        "no chunk may exceed the configured size"
    );
    assert_eq!(
        observed.inputs.concat(),
        payload,
        "chunks must reassemble to the original bytes, in order"
    );

    session.terminate().await.unwrap();
}

/// Client sequence numbers must be a gapless run starting at zero; the agent
/// processes them strictly in order and stalls on a gap.
#[tokio::test]
async fn client_sequence_numbers_are_contiguous_from_zero() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = Session::attach(
        "mock-session",
        gateway.url(),
        "mock-token",
        SessionConfig {
            payload_chunk_size: 8,
            ..config()
        },
    )
    .await
    .unwrap();
    session.wait_ready().await.unwrap();

    session.send(vec![b'x'; 40]).await.unwrap();

    let observed = gateway
        .wait_for(Duration::from_secs(5), "all five chunks", |o| {
            o.inputs.len() >= 5
        })
        .await;

    // Retransmissions repeat a sequence number, so collapse runs before
    // checking the shape of the series.
    let mut sequences = observed.sequences.clone();
    sequences.dedup();
    assert_eq!(
        sequences,
        (0..sequences.len() as i64).collect::<Vec<_>>(),
        "sequence numbers must run 0,1,2,… with no gaps"
    );

    // A gap is what the agent notices; a *duplicate* is how the gap gets
    // created. Two senders racing in `send_payload` would stamp one number on
    // two different messages and skip the next, and only this catches that.
    let mut distinct = observed.sequences.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        sequences.len(),
        "every sequence number must be issued exactly once"
    );

    session.terminate().await.unwrap();
}

/// Out-of-order delivery must be reassembled before the caller sees it.
#[tokio::test]
async fn out_of_order_output_is_reordered_before_delivery() {
    let gateway = MockGateway::start(Behaviour::OutOfOrder).await;
    let session = connect(&gateway).await;

    let mut output = session.output();
    session.wait_ready().await.unwrap();

    let mut received = String::new();
    while received.len() < "first second third ".len() {
        let chunk = tokio::time::timeout(Duration::from_secs(5), output.next())
            .await
            .expect("reordered output must arrive")
            .expect("the stream must not end");
        received.push_str(&String::from_utf8_lossy(&chunk));
    }

    assert_eq!(
        received, "first second third ",
        "the gateway sent 2,1,0; the client must deliver 0,1,2"
    );

    session.terminate().await.unwrap();
}

/// A gateway that hangs up must end the session and say so, rather than leaving
/// a handle that looks alive with nothing behind it.
#[tokio::test]
async fn a_gateway_close_ends_the_session() {
    let gateway = MockGateway::start(Behaviour::CloseAfterHandshake).await;
    let session = connect(&gateway).await;

    tokio::time::timeout(Duration::from_secs(5), session.closed())
        .await
        .expect("the session must notice the close");

    assert!(session.is_closed());
    assert!(matches!(
        session.close_reason(),
        Some(CloseReason::AgentClosed { .. })
    ));
    assert!(session.send(&b"anything"[..]).await.is_err());
}

/// A refused connection must fail immediately, not hang.
#[tokio::test]
async fn a_dropped_connection_ends_the_session() {
    let gateway = MockGateway::start(Behaviour::DropConnection).await;

    // Either the dial fails or the session opens and immediately dies; both are
    // correct, and both must be prompt.
    match Session::attach("mock-session", gateway.url(), "mock-token", config()).await {
        Err(_) => {}
        Ok(session) => {
            tokio::time::timeout(Duration::from_secs(5), session.closed())
                .await
                .expect("a dropped socket must end the session");
            assert!(session.is_closed());
        }
    }
}

/// A black-holed connection is the failure mode that hangs naive clients
/// forever: the socket is open, nothing comes back. The idle timeout must catch
/// it and report the reason.
#[tokio::test]
async fn a_silent_gateway_is_detected_as_dead() {
    let gateway = MockGateway::start(Behaviour::GoSilent).await;
    let session = connect(&gateway).await;
    session.wait_ready().await.unwrap();

    tokio::time::timeout(Duration::from_secs(10), session.closed())
        .await
        .expect("the idle timeout must fire");

    assert!(matches!(
        session.close_reason(),
        Some(CloseReason::PeerUnresponsive { .. }),
    ));
}

/// Unacknowledged messages must be retransmitted rather than silently lost.
#[tokio::test]
async fn unacknowledged_messages_are_retransmitted() {
    let gateway = MockGateway::start(Behaviour::NeverAcknowledge).await;
    let session = connect(&gateway).await;
    session.wait_ready().await.unwrap();

    session.send(&b"needs-an-ack\r"[..]).await.unwrap();

    // The first send plus at least one retransmission of the same sequence.
    let observed = gateway
        .wait_for(Duration::from_secs(5), "a retransmission", |o| {
            o.inputs.iter().filter(|i| i == &b"needs-an-ack\r").count() >= 2
        })
        .await;

    let first = observed
        .sequences
        .iter()
        .filter(|s| **s == *observed.sequences.last().unwrap())
        .count();
    assert!(first >= 2, "the same sequence must be sent more than once");

    session.terminate().await.unwrap();
}

/// Agents older than the handshake protocol just start streaming. The client
/// must notice and start sending rather than waiting forever for a handshake.
#[tokio::test]
async fn a_legacy_agent_without_a_handshake_still_works() {
    let gateway = MockGateway::start(Behaviour::LegacyNoHandshake).await;
    let session = connect(&gateway).await;

    let mut output = session.output();
    let banner = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("legacy output must arrive")
        .unwrap();
    assert_eq!(&banner[..], b"legacy-shell$ ");

    session.wait_ready().await.expect("sending must be enabled");
    session.send(&b"pwd\r"[..]).await.unwrap();

    let observed = gateway
        .wait_for(Duration::from_secs(5), "input from a legacy session", |o| {
            !o.inputs.is_empty()
        })
        .await;
    assert_eq!(observed.inputs[0], b"pwd\r");

    session.terminate().await.unwrap();
}

/// Terminal resizes travel as a `Size` payload with the agent's exact key names.
#[tokio::test]
async fn terminal_size_is_sent_in_the_agents_format() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;
    session.wait_ready().await.unwrap();

    session.send_terminal_size(203, 51).await.unwrap();

    let observed = gateway
        .wait_for(Duration::from_secs(5), "a size message", |o| {
            !o.terminal_sizes.is_empty()
        })
        .await;

    let size: serde_json::Value = serde_json::from_str(&observed.terminal_sizes[0]).unwrap();
    assert_eq!(size["cols"], 203);
    assert_eq!(size["rows"], 51);

    session.terminate().await.unwrap();
}

/// Every subscriber sees the whole stream from the point it subscribes.
#[tokio::test]
async fn multiple_subscribers_each_see_the_output() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;

    let mut first = session.output();
    let mut second = session.output();
    session.wait_ready().await.unwrap();
    session.send(&b"broadcast\r"[..]).await.unwrap();

    for stream in [&mut first, &mut second] {
        let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("both subscribers must be fed")
            .unwrap();
        assert_eq!(&chunk[..], b"broadcast\r");
    }

    session.terminate().await.unwrap();
}

/// Terminating twice, or from two tasks at once, must not error or panic.
#[tokio::test]
async fn terminate_is_idempotent() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = std::sync::Arc::new(connect(&gateway).await);
    session.wait_ready().await.unwrap();

    let concurrent: Vec<_> = (0..4)
        .map(|_| {
            let session = std::sync::Arc::clone(&session);
            tokio::spawn(async move { session.terminate().await })
        })
        .collect();

    for handle in concurrent {
        handle.await.unwrap().expect("every terminate must succeed");
    }
    assert!(session.is_closed());
    session
        .terminate()
        .await
        .expect("a later call is still fine");
}

/// Dropping a session must not leave its background tasks running or leave a
/// subscriber's stream hanging open.
#[tokio::test]
async fn dropping_a_session_ends_its_output_stream() {
    let gateway = MockGateway::start(Behaviour::Echo).await;
    let session = connect(&gateway).await;
    let mut output = session.output();
    session.wait_ready().await.unwrap();

    drop(session);

    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while output.next().await.is_some() {}
    })
    .await;
    assert!(ended.is_ok(), "the stream must end when the session drops");
}

/// When the agent explains why it closed the channel, that explanation must
/// reach the caller. "the session ended" alone is rarely actionable; "Connection
/// refused by 10.0.0.7:5432" is the whole diagnosis.
#[tokio::test]
async fn the_agents_reason_for_closing_reaches_the_caller() {
    let gateway = MockGateway::start(Behaviour::ChannelClosedWithReason).await;
    let session = connect(&gateway).await;

    tokio::time::timeout(Duration::from_secs(5), session.closed())
        .await
        .expect("the session must notice the close");

    let Some(CloseReason::AgentClosed { detail, .. }) = session.close_reason() else {
        panic!("expected AgentClosed, got {:?}", session.close_reason());
    };
    assert_eq!(
        detail.as_deref(),
        Some("Connection refused by 10.0.0.7:5432"),
        "the agent's Output field must be surfaced, not just logged"
    );
    assert!(session
        .close_reason()
        .unwrap()
        .to_string()
        .contains("refused"));
}

/// Go marshals `[]byte` as base64; serde would marshal `Vec<u8>` as `[1,2,3]`.
/// Getting this wrong makes every encryption challenge unparseable, which fails
/// the session with a *decoding* error rather than reaching the crypto layer at
/// all. Asserting on the error we do get proves the base64 half works.
#[tokio::test]
async fn a_base64_encryption_challenge_is_decoded_not_rejected_as_malformed() {
    let gateway = MockGateway::start(Behaviour::Base64Challenge).await;
    let session = connect(&gateway).await;

    tokio::time::timeout(Duration::from_secs(5), session.closed())
        .await
        .expect("an unnegotiated challenge must end the session");

    let reason = session.close_reason().expect("a reason must be recorded");
    let text = reason.to_string();
    assert!(
        text.contains("no key was negotiated"),
        "the challenge must parse and reach the crypto layer; got: {text}"
    );
    assert!(
        !text.contains("malformed"),
        "a base64 Challenge must not be treated as malformed JSON: {text}"
    );
}

/// Endpoint validation guards the session token. A tampered `StartSession`
/// response must not be able to redirect the channel to an attacker's host.
#[tokio::test]
async fn the_default_endpoint_policy_refuses_non_aws_hosts() {
    let err = Session::attach(
        "mock-session",
        "wss://attacker.example.com/v1/data-channel/x",
        "mock-token",
        SessionConfig::new("i-0123456789abcdef0"),
    )
    .await
    .expect_err("a non-AWS host must be refused");

    assert!(
        err.to_string().contains("SSM messages endpoint"),
        "the error should name the problem: {err}"
    );
}

/// A misconfigured session must fail at construction, not at some later,
/// harder-to-diagnose moment.
#[tokio::test]
async fn invalid_configuration_is_rejected_before_connecting() {
    let bad = SessionConfig {
        heartbeat_interval: Duration::from_secs(30),
        idle_timeout: Duration::from_secs(10),
        ..SessionConfig::new("i-0123456789abcdef0")
    };
    assert!(Session::attach("s", "ws://127.0.0.1:1/x", "t", bad)
        .await
        .is_err());
}
