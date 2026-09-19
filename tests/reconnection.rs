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
use common::ssm_api::MockSsmApi;
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

/// A dropped connection is rebuilt, and the caller's handle survives it.
///
/// This is the whole promise of `ReconnectingSession`. Every part of it —
/// the durable output stream, the generation counter, the new session id, the
/// event — is checked here, because each is separately easy to get wrong and
/// none was covered before.
#[tokio::test]
async fn a_dead_session_is_rebuilt_and_the_handle_survives() {
    // The first data channel completes its handshake and then goes silent, so
    // there is a whole idle timeout in which to subscribe before anything
    // happens. `DropConnection` would kill the channel before this test could
    // attach a receiver, and `events()` is a broadcast — it never replays.
    // The Transport path is covered by `send_waits_through_a_reconnect`.
    let gateway = MockGateway::scripted(vec![Behaviour::GoSilent, Behaviour::Echo]).await;
    let api = MockSsmApi::start(gateway.url()).await;

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
    let gateway = MockGateway::scripted(vec![Behaviour::DropConnection, Behaviour::Echo]).await;
    let api = MockSsmApi::start(gateway.url()).await;

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
