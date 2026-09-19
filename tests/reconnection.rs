//! Reconnection, driven through a real `SessionManager`.
//!
//! The supervisor builds every session with `SessionManager::start_session`, so
//! these point a real manager at a fake SSM endpoint rather than injecting a
//! test-only session factory. That keeps the code under test the code that
//! actually runs — the `StartSession` round trip included — and it is what makes
//! "the new session has a different id" observable at all.

mod common;

use std::time::Duration;

use aws_ssm_bridge::{
    EndpointPolicy, ReconnectConfig, ReconnectEvent, ReconnectingSession, SessionConfig,
    SessionManager,
};
use common::ssm_api::{MockSsmApi, Resumable};
use common::{Behaviour, MockGateway};
use futures_util::StreamExt;

/// A manager whose `StartSession` reaches `api` instead of AWS.
fn manager_for(api: &MockSsmApi) -> SessionManager {
    let config = aws_config::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("eu-central-1"))
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new("AKIDEXAMPLE", "secret", None, None, "test"),
            ),
        )
        .endpoint_url(api.endpoint_url())
        .build();
    SessionManager::from_conf(&config)
}

/// Retry fast: the backoff is what is being driven, not what is being measured.
fn reconnect_config() -> ReconnectConfig {
    ReconnectConfig {
        max_attempts: 5,
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
        session: SessionConfig {
            endpoint_policy: EndpointPolicy::AllowAny,
            ready_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_millis(200),
            idle_timeout: Duration::from_millis(700),
            ..SessionConfig::new("i-0123456789abcdef0")
        },
    }
}

/// Wait for an event matching `want`, or fail rather than hang.
async fn wait_for_event(
    events: &mut tokio::sync::broadcast::Receiver<ReconnectEvent>,
    what: &str,
    want: impl Fn(&ReconnectEvent) -> bool,
) -> ReconnectEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {what}");
        match tokio::time::timeout(remaining, events.recv()).await {
            Ok(Ok(event)) if want(&event) => return event,
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("event stream ended while waiting for {what}: {e}"),
            Err(_) => panic!("timed out waiting for {what}"),
        }
    }
}

/// A session that **cannot be resumed** is replaced, and the handle survives it.
///
/// The supervisor tries `ResumeSession` first, so reaching the replacement path
/// at all means telling the service the session has timed out. Every part of the
/// promise is checked here — the durable output stream, the generation counter,
/// the *new* session id, the event — because each is separately easy to get
/// wrong.
#[tokio::test]
async fn a_dead_session_is_rebuilt_and_the_handle_survives() {
    // The first data channel completes its handshake and then goes silent, so
    // there is a whole idle timeout in which to subscribe before anything
    // happens. `DropConnection` would kill the channel before this test could
    // attach a receiver, and `events()` is a broadcast — it never replays.
    // The Transport path is covered by `send_waits_through_a_reconnect`.
    let gateway = MockGateway::scripted(vec![Behaviour::GoSilent, Behaviour::Echo]).await;
    let api = MockSsmApi::with_resume(gateway.url(), Resumable::TimedOut).await;

    let session = ReconnectingSession::connect_with(
        "i-0123456789abcdef0",
        reconnect_config(),
        manager_for(&api),
    )
    .await
    .expect("the initial connect must succeed");

    // Subscribed while the first channel is still nominally alive: this stream
    // must span the gap, and these events must all be seen.
    let mut output = session.output();
    let mut events = session.events();

    let disconnected = wait_for_event(&mut events, "Disconnected", |e| {
        matches!(e, ReconnectEvent::Disconnected { .. })
    })
    .await;
    let ReconnectEvent::Disconnected { reason } = &disconnected else {
        unreachable!("filtered above")
    };
    assert!(
        reason.is_recoverable(),
        "a silent peer must be classed as recoverable: {reason}"
    );

    let reconnected = wait_for_event(&mut events, "Reconnected", |e| {
        matches!(e, ReconnectEvent::Reconnected { .. })
    })
    .await;
    let ReconnectEvent::Reconnected { session_id, .. } = &reconnected else {
        unreachable!("filtered above")
    };

    // The supervisor replaced the session rather than revived it.
    assert_eq!(
        session.generation(),
        2,
        "the session must have been replaced"
    );
    let current = session
        .current()
        .await
        .expect("a session must be connected");
    assert_eq!(
        &current.id(),
        session_id,
        "the event must name the new session"
    );
    assert_eq!(
        current.id(),
        "mock-session-2",
        "the replacement must be a different session, not the old one"
    );

    // The durable stream did not end, and carries the *new* session's output.
    session.send(&b"after-reconnect"[..]).await.expect("send");
    let chunk = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the durable output stream must not stall")
        .expect("the durable output stream must not end at a reconnect");
    assert_eq!(&chunk[..], b"after-reconnect");

    // Two data channels, two StartSession calls.
    assert_eq!(gateway.observed().await.connections, 2);
    assert_eq!(api.calls().await.started.len(), 2);

    session.terminate().await.expect("terminate");
}

/// A close a reconnect could not fix is not retried.
///
/// The agent hanging up is a decision, not a fault. Retrying it would reopen a
/// session the other end just refused, so the supervisor gives up immediately —
/// **without spending an attempt** — and the durable stream ends.
#[tokio::test]
async fn a_non_recoverable_close_gives_up_without_an_attempt() {
    // Closes after a pause, so this test subscribes before the close rather than
    // racing it. With an immediate close the event is simply unobservable — see
    // the note on `Behaviour::CloseAfterPause`.
    let gateway = MockGateway::scripted(vec![Behaviour::CloseAfterPause]).await;
    let api = MockSsmApi::start(gateway.url()).await;

    let session = ReconnectingSession::connect_with(
        "i-0123456789abcdef0",
        reconnect_config(),
        manager_for(&api),
    )
    .await
    .expect("the initial connect must succeed");

    let mut output = session.output();
    let mut events = session.events();

    let gave_up = wait_for_event(&mut events, "GaveUp", |e| {
        matches!(e, ReconnectEvent::GaveUp { .. })
    })
    .await;
    let ReconnectEvent::GaveUp { attempts, reason } = &gave_up else {
        unreachable!("filtered above")
    };
    assert_eq!(
        *attempts, 0,
        "an unrecoverable close must not consume an attempt"
    );
    assert!(
        reason.contains("agent closed"),
        "the reason must say what happened: {reason}"
    );

    // No second session was ever started.
    assert_eq!(session.generation(), 1, "nothing may replace the session");
    assert_eq!(api.calls().await.started.len(), 1);

    // The durable stream ends once the supervisor stops, rather than hanging.
    let ended = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the output stream must end rather than hang");
    assert!(
        ended.is_none(),
        "the stream must end when the supervisor does"
    );
}

/// `send` waits through a reconnect instead of failing on the dead session.
///
/// A caller holding the durable handle should not have to know a reconnect is in
/// flight; the alternative is making every caller write the retry themselves.
#[tokio::test]
async fn send_waits_through_a_reconnect() {
    // `Echo` presents a *fresh* data channel, counters and all, so the session
    // behind it has to be a fresh one too — hence the timed-out resume.
    let gateway = MockGateway::scripted(vec![Behaviour::DropConnection, Behaviour::Echo]).await;
    let api = MockSsmApi::with_resume(gateway.url(), Resumable::TimedOut).await;

    let session = ReconnectingSession::connect_with(
        "i-0123456789abcdef0",
        reconnect_config(),
        manager_for(&api),
    )
    .await
    .expect("the initial connect must succeed");

    let mut output = session.output();

    // Sent without waiting for the reconnect to be announced: the first session
    // is already dead or dying, and this must still land on its replacement.
    session
        .send(&b"through-the-gap"[..])
        .await
        .expect("send must wait through the reconnect rather than fail");

    let chunk = tokio::time::timeout(Duration::from_secs(10), output.next())
        .await
        .expect("the echo must arrive")
        .expect("the stream must not end");
    assert_eq!(&chunk[..], b"through-the-gap");

    session.terminate().await.expect("terminate");
}

/// Terminating releases the AWS-side session rather than leaving it to expire.
#[tokio::test]
async fn terminate_releases_the_session_through_the_api() {
    let gateway = MockGateway::scripted(vec![Behaviour::Echo]).await;
    let api = MockSsmApi::start(gateway.url()).await;

    let session = ReconnectingSession::connect_with(
        "i-0123456789abcdef0",
        reconnect_config(),
        manager_for(&api),
    )
    .await
    .expect("the initial connect must succeed");

    session.terminate().await.expect("terminate");

    let calls = api.calls().await;
    assert_eq!(
        calls.terminated,
        vec!["mock-session-1".to_owned()],
        "terminate must call ssm:TerminateSession for the live session"
    );
}

// ---------------------------------------------------------------------------
// Resuming, rather than replacing
// ---------------------------------------------------------------------------

/// A disconnected session is **resumed**, and the byte stream carries on.
///
/// This is the property the whole design turns on. The agent keeps counting
/// where it left off — neither side of the reference resets its sequence
/// counters on reconnect — so a resumed channel that restarted at zero would
/// have its input discarded as already-seen and its output parked in a reorder
/// buffer, in silence. The assertions below are chosen so that either failure
/// shows up: the echo after the resume can only arrive if the *inbound* counter
/// continued, and the recorded sequence numbers can only be contiguous if the
/// *outbound* one did.
#[tokio::test]
async fn a_disconnected_session_is_resumed_and_the_stream_continues() {
    let gateway = MockGateway::scripted(vec![Behaviour::EchoThenDrop, Behaviour::ResumeEcho]).await;
    let api = MockSsmApi::start(gateway.url()).await;

    let session = ReconnectingSession::connect_with(
        "i-0123456789abcdef0",
        reconnect_config(),
        manager_for(&api),
    )
    .await
    .expect("the initial connect must succeed");

    let mut output = session.output();
    let mut events = session.events();

    // Gets echoed, and then the gateway drops the channel.
    session.send(&b"before"[..]).await.expect("send");
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the first echo must arrive")
        .expect("the stream must not end");
    assert_eq!(&first[..], b"before");

    let resumed = wait_for_event(&mut events, "Resumed", |e| {
        matches!(e, ReconnectEvent::Resumed { .. })
    })
    .await;
    let ReconnectEvent::Resumed { session_id, .. } = &resumed else {
        unreachable!("filtered above")
    };

    // The same session, not a replacement: that is the whole point.
    assert_eq!(
        session_id, "mock-session-1",
        "a resume keeps the session it had"
    );
    assert_eq!(
        api.calls().await.resumed,
        vec!["mock-session-1".to_owned()],
        "ResumeSession must have been called for the disconnected session"
    );
    assert_eq!(
        api.calls().await.started.len(),
        1,
        "a resume must not start a second session"
    );

    // Only deliverable if the inbound counter carried over: the gateway numbers
    // this echo from where the dead channel stopped, not from zero.
    session
        .send(&b"after"[..])
        .await
        .expect("send after resume");
    let second = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the echo after a resume must arrive — a reset counter parks it forever")
        .expect("the durable stream must not end");
    assert_eq!(&second[..], b"after");

    // And the outbound counter carried over: contiguous from zero across both
    // channels, with no restart in the middle.
    let sequences = gateway.observed().await.sequences;
    let expected: Vec<i64> = (0..sequences.len() as i64).collect();
    assert_eq!(
        sequences, expected,
        "client sequence numbers must continue across the resume, not restart"
    );

    session.terminate().await.expect("terminate");
}

/// A session that has **timed out** cannot be resumed, so a new one is started.
///
/// The service reports this by answering `ResumeSession` with no token. The
/// reference plugin prints "Session … timed out." and exits; a library falls
/// back, and the caller sees a `Reconnected` with a *different* id rather than a
/// `Resumed` with the same one.
#[tokio::test]
async fn a_timed_out_session_falls_back_to_a_new_one() {
    let gateway = MockGateway::scripted(vec![Behaviour::EchoThenDrop, Behaviour::Echo]).await;
    let api = MockSsmApi::with_resume(gateway.url(), Resumable::TimedOut).await;

    let session = ReconnectingSession::connect_with(
        "i-0123456789abcdef0",
        reconnect_config(),
        manager_for(&api),
    )
    .await
    .expect("the initial connect must succeed");

    let mut events = session.events();
    session.send(&b"before"[..]).await.expect("send");

    let reconnected = wait_for_event(&mut events, "Reconnected", |e| {
        matches!(e, ReconnectEvent::Reconnected { .. })
    })
    .await;
    let ReconnectEvent::Reconnected { session_id, .. } = &reconnected else {
        unreachable!("filtered above")
    };
    assert_eq!(
        session_id, "mock-session-2",
        "a timed-out session must be replaced, not resumed"
    );

    let calls = api.calls().await;
    assert_eq!(
        calls.resumed,
        vec!["mock-session-1".to_owned()],
        "resume is tried first"
    );
    assert_eq!(calls.started.len(), 2, "and then a new session is started");

    session.terminate().await.expect("terminate");
}
