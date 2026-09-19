//! SigV4 signing of the data-channel WebSocket upgrade.
//!
//! Since plugin version 1.2.764.0 the reference client signs the upgrade request
//! with SigV4 over the `ssmmessages` service whenever the stream URL is not
//! already presigned (`getV4SignatureHeader` and `Open` in
//! `communicator/websocketchannel.go`).
//!
//! Three details decide whether the gateway can reproduce the signature, and
//! all three are copied from the reference rather than chosen:
//!
//! * **Only `Host` is signed.** The reference signs a bare request built with
//!   `http.NewRequest("GET", Url, nil)`; the WebSocket handshake headers
//!   (`Sec-WebSocket-Key` and friends) are added afterwards by the dialer and
//!   were never part of the canonical request. Signing them here would produce
//!   a signature nothing else can recompute.
//! * **The body is empty**, so the payload hash is the SHA-256 of zero bytes.
//! * **The service name is `ssmmessages`**, matching `config.ServiceName`.
//!
//! A URL that already carries presigned query parameters is left alone, exactly
//! as `IsPresignedURL` decides in the reference.

use std::time::SystemTime;

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tracing::debug;

use crate::errors::{Error, Result};

/// SigV4 service name for the Message Gateway.
///
/// `config.ServiceName` in the reference plugin.
const SERVICE_NAME: &str = "ssmmessages";

/// Query parameters whose presence means the URL is already presigned.
///
/// Mirrors `IsPresignedURL` in `communicator/websocketchannel.go`.
const PRESIGNED_PARAMS: [&str; 7] = [
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
];

/// Whether `url` already carries a presigned signature.
///
/// A presigned URL authenticates itself, so signing it again would add a second,
/// conflicting credential to the same request.
pub(crate) fn is_presigned(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    parsed
        .query_pairs()
        .any(|(key, _)| PRESIGNED_PARAMS.iter().any(|p| key.eq_ignore_ascii_case(p)))
}

/// Resolves credentials and signs data-channel upgrades with them.
///
/// Held by the connection layer for the life of a session. Credentials are
/// resolved on **every** dial rather than cached, because the provider chain
/// rotates them and a retry after a long backoff must not present an expired
/// key.
#[derive(Clone)]
pub(crate) struct UpgradeSigner {
    credentials: SharedCredentialsProvider,
    region: String,
}

impl std::fmt::Debug for UpgradeSigner {
    /// Deliberately opaque: the provider can be asked for a secret key, and a
    /// derived `Debug` invites that into a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpgradeSigner")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl UpgradeSigner {
    /// Build a signer from a credentials provider and the region the data
    /// channel lives in.
    pub(crate) fn new(credentials: SharedCredentialsProvider, region: impl Into<String>) -> Self {
        Self {
            credentials,
            region: region.into(),
        }
    }

    /// Sign `request` in place, adding the SigV4 headers.
    pub(crate) async fn sign(&self, request: &mut Request) -> Result<()> {
        let credentials = self.credentials.provide_credentials().await.map_err(|e| {
            Error::aws(format!(
                "could not resolve credentials to sign the data channel: {e}"
            ))
        })?;
        sign_request(request, &self.region, &credentials, SystemTime::now())
    }
}

/// Build the WebSocket upgrade request for `stream_url`.
///
/// This is the object handed to the dialer, so the `Host` header it carries is
/// the one that will be sent — which is why [`sign_request`] reads the host back
/// out of it rather than deriving it separately. Two derivations of one value is
/// how a signature ends up covering a host that was never transmitted.
pub(crate) fn upgrade_request(stream_url: &str) -> Result<Request> {
    stream_url
        .into_client_request()
        .map_err(|e| Error::Config(format!("stream URL is not a valid WebSocket URL: {e}")))
}

/// Sign an already-built upgrade request, appending the SigV4 headers to it.
///
/// Split from [`UpgradeSigner::sign`] so the signature can be tested against a
/// fixed clock and fixed credentials without a provider.
fn sign_request(
    request: &mut Request,
    region: &str,
    credentials: &Credentials,
    time: SystemTime,
) -> Result<()> {
    let host = request
        .headers()
        .get(tokio_tungstenite::tungstenite::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Error::Config("upgrade request has no Host header to sign".into()))?
        .to_owned();

    let identity = credentials.clone().into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(SERVICE_NAME)
        .time(time)
        .settings(SigningSettings::default())
        .build()
        .map_err(|e| Error::Config(format!("could not build SigV4 signing parameters: {e}")))?
        .into();

    // Only `host`. See the module docs: the handshake headers are added by the
    // dialer after this point and are not part of the canonical request.
    let signed_headers = [("host", host.as_str())];
    let signable = SignableRequest::new(
        "GET",
        request.uri().to_string(),
        signed_headers.iter().copied(),
        SignableBody::Bytes(&[]),
    )
    .map_err(|e| Error::Config(format!("could not build the signable request: {e}")))?;

    let (instructions, _signature) = sign(signable, &params)
        .map_err(|e| Error::Config(format!("could not sign the data channel upgrade: {e}")))?
        .into_parts();

    let headers = request.headers_mut();
    for header in instructions.into_parts().0 {
        let name =
            tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(header.name().as_bytes())
                .map_err(|e| {
                    Error::Config(format!("SigV4 produced an invalid header name: {e}"))
                })?;
        let value = header
            .value()
            .parse()
            .map_err(|e| Error::Config(format!("SigV4 produced an invalid header value: {e}")))?;
        headers.insert(name, value);
    }

    debug!("data channel upgrade signed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str =
        "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/session-id-123?stream=input";
    const REGION: &str = "eu-central-1";
    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    /// 2026-09-19T12:34:56Z, the instant both known-answer vectors were
    /// computed for.
    fn fixed_time() -> SystemTime {
        SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_789_821_296)
    }

    fn credentials(token: Option<&str>) -> Credentials {
        Credentials::new(AKID, SECRET, token.map(str::to_owned), None, "test")
    }

    fn signed(token: Option<&str>) -> Request {
        let mut request = upgrade_request(URL).expect("a valid wss URL");
        sign_request(&mut request, REGION, &credentials(token), fixed_time())
            .expect("signing succeeds");
        request
    }

    fn header(request: &Request, name: &str) -> String {
        request
            .headers()
            .get(name)
            .unwrap_or_else(|| panic!("header {name} is present"))
            .to_str()
            .expect("header is valid UTF-8")
            .to_owned()
    }

    /// Known-answer vector, computed from the SigV4 specification rather than
    /// from this code — by a separate implementation that reproduces AWS's own
    /// published `get-vanilla` vector, so the expectation is not circular.
    ///
    /// A change to what is signed — the service name, the header set, the
    /// payload hash, the region — moves this signature.
    #[test]
    fn signature_matches_the_known_answer_vector() {
        assert_eq!(
            header(&signed(None), "authorization"),
            "AWS4-HMAC-SHA256 \
             Credential=AKIDEXAMPLE/20260919/eu-central-1/ssmmessages/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=fe5731e6ed4bcc139e96459886d639419a90db39f18849dc88796a2f34585ff3"
        );
    }

    /// Session credentials add `x-amz-security-token`, and it is *signed* rather
    /// than merely sent — so it appears in `SignedHeaders` and the signature
    /// differs from the long-lived-credential case.
    #[test]
    fn session_credentials_sign_the_security_token() {
        let request = signed(Some("FwoGZXIvYXdzEXAMPLE"));
        assert_eq!(
            header(&request, "x-amz-security-token"),
            "FwoGZXIvYXdzEXAMPLE"
        );
        assert_eq!(
            header(&request, "authorization"),
            "AWS4-HMAC-SHA256 \
             Credential=AKIDEXAMPLE/20260919/eu-central-1/ssmmessages/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-security-token, \
             Signature=57a642a0ecb3a8a71bd3fb52808ab29f032e68e7e615d6d2e42703c00f58df28"
        );
    }

    #[test]
    fn the_date_header_is_the_signing_instant() {
        assert_eq!(header(&signed(None), "x-amz-date"), "20260919T123456Z");
    }

    /// The handshake headers must survive signing untouched, and must **not**
    /// appear in `SignedHeaders` — the gateway recomputes the signature without
    /// them.
    #[test]
    fn handshake_headers_are_preserved_and_left_unsigned() {
        let request = signed(None);
        assert_eq!(header(&request, "upgrade"), "websocket");
        assert_eq!(header(&request, "connection"), "Upgrade");
        assert_eq!(header(&request, "sec-websocket-version"), "13");
        assert!(request.headers().contains_key("sec-websocket-key"));

        let authorization = header(&request, "authorization");
        assert!(
            authorization.contains("SignedHeaders=host;x-amz-date,"),
            "only host and x-amz-date may be signed: {authorization}"
        );
    }

    /// The host that is signed is the host that is sent. Deriving it twice is
    /// how a signature comes to cover something the wire never carried.
    #[test]
    fn the_signed_host_is_the_host_header_on_the_request() {
        let request = signed(None);
        assert_eq!(
            header(&request, "host"),
            "ssmmessages.eu-central-1.amazonaws.com"
        );
    }

    #[test]
    fn an_unsigned_request_carries_no_credential() {
        let request = upgrade_request(URL).expect("a valid wss URL");
        assert!(!request.headers().contains_key("authorization"));
        assert!(!request.headers().contains_key("x-amz-date"));
    }

    #[test]
    fn presigned_urls_are_recognised_case_insensitively() {
        assert!(is_presigned(
            "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/s?X-Amz-Signature=abc"
        ));
        assert!(is_presigned(
            "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/s?x-amz-credential=abc"
        ));
        assert!(is_presigned(
            "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/s?stream=input&X-Amz-Expires=60"
        ));
    }

    #[test]
    fn an_ordinary_stream_url_is_not_presigned() {
        assert!(!is_presigned(URL));
        assert!(!is_presigned(
            "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/s"
        ));
        // `tokenValue` is the session token, not a presigned signature.
        assert!(!is_presigned(
            "wss://ssmmessages.eu-central-1.amazonaws.com/v1/data-channel/s?tokenValue=abc"
        ));
    }

    /// Unparseable input is not presigned — and must not panic on the way to
    /// that answer.
    #[test]
    fn garbage_is_not_presigned() {
        assert!(!is_presigned("not a url"));
        assert!(!is_presigned(""));
    }

    #[test]
    fn the_signer_does_not_print_its_credentials() {
        let signer = UpgradeSigner::new(
            SharedCredentialsProvider::new(credentials(Some("token"))),
            REGION,
        );
        let rendered = format!("{signer:?}");
        assert!(rendered.contains(REGION), "{rendered}");
        assert!(!rendered.contains(SECRET), "{rendered}");
        assert!(!rendered.contains("token"), "{rendered}");
    }
}
