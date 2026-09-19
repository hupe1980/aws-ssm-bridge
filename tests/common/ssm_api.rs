//! A stand-in for the `ssm:StartSession` API.
//!
//! The reconnection supervisor builds every session through
//! `SessionManager::start_session`, so testing it means giving a **real**
//! `SessionManager` something to call. The alternative — an injectable session
//! factory inside `reconnect` — would be public API that exists only for tests,
//! and it would exercise the supervisor against a stub rather than against the
//! code that actually runs.
//!
//! SSM speaks `awsJson1_1`: a `POST` with the operation in `x-amz-target` and a
//! JSON body, answered with JSON. Requests are signed; nothing here checks the
//! signature, because that is not what these tests are about.
//!
//! Every `StartSession` hands out a **fresh session id** pointing at the same
//! mock gateway, which is what makes "the reconnected session is a different
//! session" observable.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// What the fake API was asked to do.
#[derive(Debug, Default, Clone)]
pub struct ApiCalls {
    /// Session ids handed out, in order.
    pub started: Vec<String>,
    /// Session ids passed to `TerminateSession`.
    pub terminated: Vec<String>,
}

/// A running fake SSM endpoint.
pub struct MockSsmApi {
    addr: SocketAddr,
    calls: Arc<Mutex<ApiCalls>>,
}

impl MockSsmApi {
    /// Serve `StartSession` and `TerminateSession`, handing out data channels
    /// on `gateway_url`.
    pub async fn start(gateway_url: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let calls = Arc::new(Mutex::new(ApiCalls::default()));

        tokio::spawn({
            let calls = Arc::clone(&calls);
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let calls = Arc::clone(&calls);
                    let gateway_url = gateway_url.clone();
                    // One task per connection: the SDK opens a fresh one for
                    // each request, and a serial loop would deadlock the moment
                    // it decided not to.
                    tokio::spawn(async move {
                        let _ = handle(stream, gateway_url, calls).await;
                    });
                }
            }
        });

        Self { addr, calls }
    }

    /// The `endpoint_url` to build an `SdkConfig` with.
    pub fn endpoint_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// A snapshot of what has been called so far.
    pub async fn calls(&self) -> ApiCalls {
        self.calls.lock().await.clone()
    }
}

/// Serve one HTTP/1.1 request, then close.
///
/// `Connection: close` on every response, so there is no keep-alive state to
/// get wrong — the SDK simply dials again.
async fn handle(
    mut stream: tokio::net::TcpStream,
    gateway_url: String,
    calls: Arc<Mutex<ApiCalls>>,
) -> std::io::Result<()> {
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];

    // Headers first, then exactly `Content-Length` more bytes.
    let head_end = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        raw.extend_from_slice(&buf[..n]);
        if let Some(at) = find(&raw, b"\r\n\r\n") {
            break at + 4;
        }
    };

    let head = String::from_utf8_lossy(&raw[..head_end]).to_ascii_lowercase();
    let length: usize = head
        .split("content-length:")
        .nth(1)
        .and_then(|rest| rest.split("\r\n").next())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);

    while raw.len() < head_end + length {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    let body = String::from_utf8_lossy(&raw[head_end..]).into_owned();

    let response = if head.contains("amazonssm.startsession") {
        let mut calls = calls.lock().await;
        let session_id = format!("mock-session-{}", calls.started.len() + 1);
        calls.started.push(session_id.clone());
        serde_json::json!({
            "SessionId": session_id,
            "StreamUrl": gateway_url,
            "TokenValue": "mock-token",
        })
    } else if head.contains("amazonssm.terminatesession") {
        let session_id = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("SessionId")?.as_str().map(str::to_owned))
            .unwrap_or_default();
        calls.lock().await.terminated.push(session_id.clone());
        serde_json::json!({ "SessionId": session_id })
    } else {
        serde_json::json!({})
    }
    .to_string();

    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/x-amz-json-1.1\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .as_bytes(),
        )
        .await?;
    stream.flush().await
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
