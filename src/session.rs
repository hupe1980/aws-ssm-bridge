//! Session lifecycle: starting, using and ending an SSM session.
//!
//! [`SessionManager`] calls `ssm:StartSession` and hands back a [`Session`] with
//! the data channel already open. A session is alive until something ends it,
//! and *anything* that ends it — a clean [`terminate`](Session::terminate), the
//! agent hanging up, a dead network, a protocol violation — resolves
//! [`Session::closed`] and records a [`CloseReason`].
//!
//! That single rule is what makes the higher layers work: the port forwarder
//! stops accepting when the tunnel dies, the pool reaps dead entries, and
//! [`ReconnectingSession`] knows when to rebuild. A session cannot be
//! simultaneously "connected" and not running.
//!
//! [`ReconnectingSession`]: crate::ReconnectingSession

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use zeroize::Zeroizing;

use crate::binary_protocol::{PayloadType, DEFAULT_PAYLOAD_CHUNK_SIZE};
use crate::channels::{OutputFanout, OutputStream, DEFAULT_SUBSCRIBER_CAPACITY};
use crate::connection::{self, Command, ConnectionParams, EndpointPolicy};
use crate::crypto::SessionCrypto;
use crate::documents::{SessionType, SsmDocument};
use crate::errors::{Error, Result};
use crate::metrics::{self, names};

/// Why a session ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    /// The local application called [`Session::terminate`].
    Terminated,
    /// The agent closed the channel.
    AgentClosed {
        /// Exit status of the remote process, when the agent reported one.
        exit_code: Option<i32>,
        /// The agent's own explanation, when it sent one.
        ///
        /// A `channel_closed` message carries a JSON body whose `Output` field
        /// says *why* — "Connection refused" from a port forward whose target
        /// is down, a plugin error, an operator termination. Without this the
        /// only signal is "the session ended", which is rarely enough to act
        /// on.
        detail: Option<String>,
    },
    /// No traffic arrived from the agent within the idle timeout.
    PeerUnresponsive {
        /// How long the channel had been silent.
        idle: Duration,
    },
    /// The agent never acknowledged a message despite repeated retransmission.
    DeliveryFailed {
        /// Sequence number that was never acknowledged.
        sequence: i64,
        /// Number of transmission attempts made.
        attempts: u32,
    },
    /// The WebSocket transport failed.
    Transport(String),
    /// The peer violated the SSM protocol, or required something unsupported.
    Protocol(String),
}

impl std::fmt::Display for CloseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloseReason::Terminated => f.write_str("terminated by the local application"),
            CloseReason::AgentClosed { exit_code, detail } => {
                f.write_str("the agent closed the session")?;
                if let Some(detail) = detail {
                    write!(f, ": {detail}")?;
                }
                if let Some(code) = exit_code {
                    write!(f, " (exit code {code})")?;
                }
                Ok(())
            }
            CloseReason::PeerUnresponsive { idle } => {
                write!(f, "no traffic from the agent for {idle:?}")
            }
            CloseReason::DeliveryFailed { sequence, attempts } => write!(
                f,
                "message {sequence} was never acknowledged after {attempts} attempts"
            ),
            CloseReason::Transport(e) => write!(f, "transport failure: {e}"),
            CloseReason::Protocol(e) => write!(f, "protocol failure: {e}"),
        }
    }
}

impl CloseReason {
    /// Whether the session ended for a reason a reconnect could plausibly fix.
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            CloseReason::PeerUnresponsive { .. }
                | CloseReason::DeliveryFailed { .. }
                | CloseReason::Transport(_)
        )
    }
}

// ---------------------------------------------------------------------------
// SessionCore
// ---------------------------------------------------------------------------

/// State shared between a [`Session`] handle and the tasks driving its channel.
pub(crate) struct SessionCore {
    session_id: String,
    fanout: OutputFanout,
    subscriber_capacity: usize,

    /// Set once the agent will accept data; cleared by `pause_publication`.
    sendable: AtomicBool,
    sendable_notify: Notify,

    closed: AtomicBool,
    closed_notify: Notify,
    close_reason: Mutex<Option<CloseReason>>,

    /// Woken whenever an acknowledgement frees space in the outgoing buffer.
    ack_notify: Notify,

    crypto: OnceLock<Arc<SessionCrypto>>,
    agent_version: OnceLock<String>,
    banner: Mutex<Option<String>>,
    exit_code: Mutex<Option<i32>>,
}

impl SessionCore {
    fn new(session_id: String, subscriber_capacity: usize) -> Self {
        Self {
            session_id,
            fanout: OutputFanout::new(),
            subscriber_capacity,
            sendable: AtomicBool::new(false),
            sendable_notify: Notify::new(),
            closed: AtomicBool::new(false),
            closed_notify: Notify::new(),
            close_reason: Mutex::new(None),
            ack_notify: Notify::new(),
            crypto: OnceLock::new(),
            agent_version: OnceLock::new(),
            banner: Mutex::new(None),
            exit_code: Mutex::new(None),
        }
    }

    /// End the session, recording why. Only the first caller's reason is kept.
    pub(crate) fn close(&self, reason: CloseReason) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        // Publish the reason before waking anyone, so a waiter that observes the
        // close is guaranteed to be able to read why.
        *lock(&self.close_reason) = Some(reason.clone());
        if reason == CloseReason::Terminated {
            info!(session_id = %self.session_id, "session terminated");
        } else {
            warn!(session_id = %self.session_id, %reason, "session ended");
        }
        metrics::counter(names::SESSIONS_ENDED, 1);

        self.fanout.close();
        self.closed_notify.notify_waiters();
        // Release anyone parked on readiness or buffer space so they observe the
        // close instead of waiting for a signal that will never come.
        self.sendable_notify.notify_waiters();
        self.ack_notify.notify_waiters();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Resolves once the session has ended, immediately if it already has.
    pub(crate) async fn closed(&self) {
        // Register before the flag check. `notify_waiters` only wakes futures
        // that are *already enqueued*, and merely constructing a `Notified` does
        // not enqueue it — that happens on the first poll, or eagerly via
        // `enable()`. Without the `enable()` a close landing between the check
        // and the first await wakes nobody and parks this task forever.
        let notified = self.closed_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_closed() {
            return;
        }
        notified.await;
    }

    pub(crate) fn close_reason(&self) -> Option<CloseReason> {
        lock(&self.close_reason).clone()
    }

    pub(crate) fn set_sendable(&self, sendable: bool) {
        let previous = self.sendable.swap(sendable, Ordering::Release);
        if sendable && !previous {
            self.sendable_notify.notify_waiters();
        }
    }

    pub(crate) fn is_sendable(&self) -> bool {
        self.sendable.load(Ordering::Acquire)
    }

    /// Resolves once the agent will accept data.
    pub(crate) async fn wait_sendable(&self) {
        loop {
            // `enable()` before the check, for the reason spelled out on
            // [`Self::closed`]: constructing the future does not enqueue it.
            let notified = self.sendable_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_sendable() || self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn notify_ack(&self) {
        self.ack_notify.notify_waiters();
    }

    /// A future that resolves on the next acknowledgement.
    ///
    /// Returned already enqueued, so a caller can create it, test whether it
    /// still needs to wait, and only then await — without racing an
    /// acknowledgement that lands in between. Awaiting a stale one simply parks
    /// until the *next* acknowledgement, which is why the ordering matters:
    /// the one that frees buffer space must not be the one that is missed.
    pub(crate) fn ack_notified(&self) -> impl std::future::Future<Output = ()> + '_ {
        let notified = self.ack_notify.notified();
        // `Box::pin` so the caller gets a nameable, `Unpin` future it can hold
        // across the `track()` attempt.
        let mut notified = Box::pin(notified);
        notified.as_mut().enable();
        notified
    }

    pub(crate) fn emit_output(&self, data: Bytes) {
        self.fanout.send(data);
    }

    pub(crate) fn subscribe(&self) -> OutputStream {
        self.fanout.subscribe(self.subscriber_capacity)
    }

    pub(crate) fn crypto(&self) -> Option<Arc<SessionCrypto>> {
        self.crypto.get().cloned()
    }

    pub(crate) fn set_crypto(&self, crypto: Arc<SessionCrypto>) {
        let _ = self.crypto.set(crypto);
    }

    pub(crate) fn set_agent_version(&self, version: Option<&str>) {
        if let Some(v) = version {
            let _ = self.agent_version.set(v.to_owned());
        }
    }

    pub(crate) fn agent_version(&self) -> Option<&str> {
        self.agent_version.get().map(String::as_str)
    }

    pub(crate) fn set_session_banner(&self, banner: Option<String>) {
        *lock(&self.banner) = banner;
    }

    pub(crate) fn banner(&self) -> Option<String> {
        lock(&self.banner).clone()
    }

    pub(crate) fn set_exit_code(&self, code: Option<i32>) {
        if code.is_some() {
            *lock(&self.exit_code) = code;
        }
    }

    pub(crate) fn exit_code(&self) -> Option<i32> {
        *lock(&self.exit_code)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How a session is started and how its data channel behaves.
///
/// Prefer [`SessionBuilder`] for anything beyond the defaults.
///
/// [`SessionBuilder`]: crate::SessionBuilder
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// What to connect to: an instance ID, `mi-` managed instance, ECS target
    /// or ARN. See [`SessionManager::start_session`] for the accepted forms.
    pub target: String,

    /// The SSM document to run, and its parameters.
    ///
    /// `None` starts a plain shell session, which is what `aws ssm
    /// start-session` does with no `--document-name`.
    pub document: Option<DocumentSpec>,

    /// Free-text reason recorded in CloudTrail for this session.
    pub reason: Option<String>,

    /// How long to wait for the agent handshake before giving up.
    pub ready_timeout: Duration,

    /// How often to send a WebSocket keep-alive ping.
    pub heartbeat_interval: Duration,

    /// Declare the connection dead after this long with no inbound traffic.
    ///
    /// Must exceed [`heartbeat_interval`](Self::heartbeat_interval); the
    /// gateway replies to pings, so a healthy channel is never idle for longer
    /// than one interval plus a round trip.
    pub idle_timeout: Duration,

    /// Bytes per protocol message when splitting caller data.
    ///
    /// Defaults to [`DEFAULT_PAYLOAD_CHUNK_SIZE`], matching the reference
    /// plugin. Larger chunks trade acknowledgement granularity for throughput
    /// and are worth raising for bulk port-forward traffic.
    pub payload_chunk_size: usize,

    /// Queue depth for each [`Session::output`] subscriber, in messages.
    ///
    /// A subscriber that exceeds this backlog is evicted; see [`OutputStream`].
    pub output_buffer: usize,

    /// Which endpoints the data channel may connect to.
    pub endpoint_policy: EndpointPolicy,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            target: String::new(),
            document: None,
            reason: None,
            ready_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(120),
            payload_chunk_size: DEFAULT_PAYLOAD_CHUNK_SIZE,
            output_buffer: DEFAULT_SUBSCRIBER_CAPACITY,
            endpoint_policy: EndpointPolicy::AwsOnly,
        }
    }
}

impl SessionConfig {
    /// Start a session against `target` with default settings.
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            ..Default::default()
        }
    }

    /// The session type implied by the configured document.
    pub fn session_type(&self) -> SessionType {
        self.document
            .as_ref()
            .map_or(SessionType::StandardStream, |d| d.session_type)
    }

    fn validate(&self) -> Result<()> {
        if self.target.trim().is_empty() {
            return Err(Error::Config("target must not be empty".into()));
        }
        if self.payload_chunk_size == 0 {
            return Err(Error::Config(
                "payload_chunk_size must be at least 1".into(),
            ));
        }
        if self.payload_chunk_size > crate::binary_protocol::MAX_PAYLOAD_SIZE {
            return Err(Error::Config(format!(
                "payload_chunk_size {} exceeds the {}-byte protocol limit",
                self.payload_chunk_size,
                crate::binary_protocol::MAX_PAYLOAD_SIZE
            )));
        }
        if self.idle_timeout <= self.heartbeat_interval {
            return Err(Error::Config(format!(
                "idle_timeout ({:?}) must exceed heartbeat_interval ({:?}), \
                 otherwise a healthy connection is declared dead between pings",
                self.idle_timeout, self.heartbeat_interval
            )));
        }
        Ok(())
    }
}

/// A resolved SSM document name and its parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentSpec {
    /// Document name, e.g. `AWS-StartPortForwardingSession`.
    pub name: String,
    /// Document parameters as the `StartSession` API expects them.
    pub parameters: std::collections::HashMap<String, Vec<String>>,
    /// The session type this document produces.
    pub session_type: SessionType,
}

impl DocumentSpec {
    /// Build a spec from any [`SsmDocument`].
    pub fn new(document: &impl SsmDocument) -> Self {
        Self {
            name: document.document_name().to_owned(),
            parameters: document.parameters(),
            session_type: document.session_type(),
        }
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

/// An open SSM session.
///
/// Cheap to share: wrap it in an [`Arc`] and hand it to as many tasks as you
/// like. Every method takes `&self`.
///
/// # Ending a session
///
/// Call [`terminate`](Self::terminate) to shut down cleanly — it closes the
/// data channel *and* calls `ssm:TerminateSession` so AWS releases the session
/// immediately. Dropping a `Session` without terminating aborts the local tasks
/// but leaves the AWS-side session to expire on its own idle timeout.
pub struct Session {
    core: Arc<SessionCore>,
    commands: mpsc::Sender<Command>,
    config: SessionConfig,
    ssm: Option<Arc<aws_sdk_ssm::Client>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Session {
    pub(crate) async fn open(
        session_id: String,
        config: SessionConfig,
        stream_url: String,
        token: Zeroizing<String>,
        ssm: Option<Arc<aws_sdk_ssm::Client>>,
        #[cfg(feature = "kms")] kms: Option<aws_sdk_kms::Client>,
    ) -> Result<Self> {
        let core = Arc::new(SessionCore::new(session_id.clone(), config.output_buffer));

        let (commands, tasks) = connection::connect(
            Arc::clone(&core),
            ConnectionParams {
                session_id,
                target: config.target.clone(),
                stream_url,
                token,
                chunk_size: config.payload_chunk_size,
                endpoint_policy: config.endpoint_policy,
                heartbeat_interval: config.heartbeat_interval,
                idle_timeout: config.idle_timeout,
                #[cfg(feature = "kms")]
                kms,
            },
        )
        .await?;

        metrics::counter(names::SESSIONS_STARTED, 1);
        Ok(Self {
            core,
            commands,
            config,
            ssm,
            tasks: Mutex::new(tasks),
        })
    }

    /// Open a session on a data channel someone else obtained.
    ///
    /// `ssm:StartSession` returns a session ID, a stream URL and a token. When
    /// that call happens somewhere else — a broker service that holds the IAM
    /// permissions, a Lambda that mints short-lived sessions, a test harness
    /// standing in for the gateway — pass its three values here and skip the
    /// AWS round trip entirely.
    ///
    /// The resulting session has no SSM client, so [`terminate`](Self::terminate)
    /// closes the data channel but cannot call `ssm:TerminateSession`. Whoever
    /// started the session owns releasing it.
    ///
    /// Session encryption is unavailable on an attached session: negotiating a
    /// KMS data key needs the session's target ID, which `StartSession` knows
    /// and this entry point does not. An agent that requires encryption will
    /// fail the handshake.
    ///
    /// ```no_run
    /// use aws_ssm_bridge::{Session, SessionConfig};
    ///
    /// # async fn example(id: String, url: String, token: String) -> aws_ssm_bridge::Result<()> {
    /// let session = Session::attach(id, url, token, SessionConfig::new("i-0123456789abcdef0"))
    ///     .await?;
    /// session.wait_ready().await?;
    /// # Ok(()) }
    /// ```
    pub async fn attach(
        session_id: impl Into<String>,
        stream_url: impl Into<String>,
        token: impl Into<String>,
        config: SessionConfig,
    ) -> Result<Self> {
        config.validate()?;
        Self::open(
            session_id.into(),
            config,
            stream_url.into(),
            Zeroizing::new(token.into()),
            None,
            #[cfg(feature = "kms")]
            None,
        )
        .await
    }

    /// The AWS session ID, as returned by `StartSession`.
    pub fn id(&self) -> &str {
        &self.core.session_id
    }

    /// The configuration this session was started with.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Version of the SSM agent on the target, once the handshake has run.
    pub fn agent_version(&self) -> Option<&str> {
        self.core.agent_version()
    }

    /// The agent's operator message (login banner), if it sent one.
    pub fn banner(&self) -> Option<String> {
        self.core.banner()
    }

    /// Exit status of the remote process, once it has exited.
    pub fn exit_code(&self) -> Option<i32> {
        self.core.exit_code()
    }

    /// Whether session data is encrypted end-to-end with a KMS-derived key.
    pub fn is_encrypted(&self) -> bool {
        self.core.crypto().is_some()
    }

    /// Whether the agent has finished its handshake and will accept data.
    pub fn is_ready(&self) -> bool {
        self.core.is_sendable()
    }

    /// Whether the session has ended.
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    /// Why the session ended, or `None` while it is still open.
    pub fn close_reason(&self) -> Option<CloseReason> {
        self.core.close_reason()
    }

    /// Resolve once the session has ended, for whatever reason.
    ///
    /// Cancel-safe, so it composes in `tokio::select!`:
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # async fn f(session: Arc<aws_ssm_bridge::Session>) {
    /// tokio::select! {
    ///     () = session.closed() => println!("gone: {:?}", session.close_reason()),
    ///     _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
    /// }
    /// # }
    /// ```
    pub async fn closed(&self) {
        self.core.closed().await;
    }

    /// Wait for the agent handshake to finish.
    ///
    /// Returns an error if the session ends first or
    /// [`SessionConfig::ready_timeout`] elapses. You do not need to call this
    /// before [`send`](Self::send) — outbound data is held until the agent is
    /// ready — but port forwarding and anything that must not race the
    /// handshake should await it explicitly.
    pub async fn wait_ready(&self) -> Result<()> {
        let deadline = self.config.ready_timeout;
        tokio::select! {
            biased;
            () = self.core.wait_sendable() => {}
            () = self.core.closed() => {}
            _ = tokio::time::sleep(deadline) => {
                return Err(Error::Timeout(deadline));
            }
        }
        if self.core.is_sendable() {
            Ok(())
        } else {
            Err(self.closed_error())
        }
    }

    /// Subscribe to the session's output.
    ///
    /// Every subscriber sees the whole stream from the moment it subscribes;
    /// earlier output is not replayed. Subscribe before sending anything you
    /// want to see the response to.
    pub fn output(&self) -> OutputStream {
        self.core.subscribe()
    }

    /// Send bytes to the remote process's standard input.
    ///
    /// Returns once the data is queued, not once the agent has acknowledged it;
    /// delivery is guaranteed by the retransmission layer, and a permanent
    /// failure ends the session with [`CloseReason::DeliveryFailed`].
    ///
    /// Large buffers are split into [`SessionConfig::payload_chunk_size`]
    /// chunks automatically. Data sent before the handshake completes is held,
    /// not dropped.
    ///
    /// Send `\r`, not `\n`, for Enter: a remote pty maps CR to NL, but Windows
    /// shells behind winpty do not accept a bare LF.
    pub async fn send(&self, data: impl Into<Bytes>) -> Result<()> {
        self.dispatch(Command::Data(data.into())).await
    }

    /// Tell the remote pty the terminal has been resized.
    pub async fn send_terminal_size(&self, cols: u16, rows: u16) -> Result<()> {
        let payload = serde_json::to_vec(&serde_json::json!({ "cols": cols, "rows": rows }))?;
        debug!(cols, rows, "sending terminal size");
        self.dispatch(Command::Control {
            payload_type: PayloadType::Size,
            data: Bytes::from(payload),
        })
        .await
    }

    async fn dispatch(&self, command: Command) -> Result<()> {
        if self.core.is_closed() {
            return Err(self.closed_error());
        }
        // Race the enqueue against closure so a caller is never parked forever
        // on a queue whose consumer has already exited.
        tokio::select! {
            biased;
            () = self.core.closed() => Err(self.closed_error()),
            result = self.commands.send(command) => {
                result.map_err(|_| self.closed_error())
            }
        }
    }

    fn closed_error(&self) -> Error {
        Error::SessionClosed(
            self.core
                .close_reason()
                .map_or_else(|| "session is closing".to_owned(), |r| r.to_string()),
        )
    }

    /// Shut the session down cleanly.
    ///
    /// Closes the data channel and calls `ssm:TerminateSession` so AWS releases
    /// the session immediately rather than waiting for its idle timeout.
    /// Idempotent: extra calls are no-ops that still return `Ok`.
    pub async fn terminate(&self) -> Result<()> {
        let first = !self.core.is_closed();
        self.core.close(CloseReason::Terminated);
        self.join_tasks().await;

        if !first {
            return Ok(());
        }

        if let Some(ssm) = &self.ssm {
            // Best-effort: the channel is already down, so a failure here only
            // means AWS reclaims the session on its own schedule.
            if let Err(e) = ssm
                .terminate_session()
                .session_id(self.id())
                .send()
                .await
                .map_err(Error::from)
            {
                warn!(error = %e, "ssm:TerminateSession failed; AWS will reclaim the session on timeout");
            } else {
                debug!("session terminated through the SSM API");
            }
        }
        Ok(())
    }

    /// Wait for the channel tasks to wind down, then drop their handles.
    async fn join_tasks(&self) {
        let tasks: Vec<_> = std::mem::take(&mut *lock(&self.tasks));
        for task in tasks {
            // Every task exits promptly on close; the timeout only guards
            // against a socket write that cannot make progress.
            if tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .is_err()
            {
                debug!("a data-channel task did not exit within 5s");
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Mark closed so any surviving OutputStream sees EOF rather than hanging.
        self.core.close(CloseReason::Terminated);
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id())
            .field("target", &self.config.target)
            .field("ready", &self.is_ready())
            .field("closed", &self.is_closed())
            .field("encrypted", &self.is_encrypted())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// SessionManager
// ---------------------------------------------------------------------------

/// Starts sessions against an AWS account.
///
/// Holds the SSM (and, with the `kms` feature, KMS) clients, so create one and
/// reuse it for every session — each client owns a connection pool.
#[derive(Clone)]
pub struct SessionManager {
    ssm: Arc<aws_sdk_ssm::Client>,
    #[cfg(feature = "kms")]
    kms: Option<aws_sdk_kms::Client>,
}

impl SessionManager {
    /// Build a manager from the ambient AWS configuration.
    ///
    /// Uses the standard credential and region resolution chain: environment
    /// variables, `~/.aws/config`, SSO, instance metadata.
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_from_env().await;
        Ok(Self::from_conf(&config))
    }

    /// Build a manager pinned to a region, resolving credentials as usual.
    pub async fn for_region(region: impl Into<String>) -> Result<Self> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.into()))
            .load()
            .await;
        Ok(Self::from_conf(&config))
    }

    /// Build a manager from an existing [`aws_config::SdkConfig`].
    ///
    /// Use this to share credential providers, retry settings or an HTTP client
    /// with the rest of your application.
    pub fn from_conf(config: &aws_config::SdkConfig) -> Self {
        Self {
            ssm: Arc::new(aws_sdk_ssm::Client::new(config)),
            #[cfg(feature = "kms")]
            kms: Some(aws_sdk_kms::Client::new(config)),
        }
    }

    /// The underlying SSM client, for API calls this crate does not wrap.
    pub fn ssm_client(&self) -> &aws_sdk_ssm::Client {
        &self.ssm
    }

    /// Start a session and open its data channel.
    ///
    /// `target` may be any of:
    ///
    /// | Form | Example |
    /// |------|---------|
    /// | EC2 instance | `i-0123456789abcdef0` |
    /// | Managed instance | `mi-0123456789abcdef0` |
    /// | ECS Exec task | `ecs:cluster_task-id_container-runtime-id` |
    /// | ARN | `arn:aws:ec2:eu-central-1:111122223333:instance/i-0…` |
    ///
    /// Targets are not pattern-matched client-side beyond a non-empty check:
    /// AWS validates them authoritatively, and guessing the format here only
    /// breaks new target types before this crate learns about them.
    pub async fn start_session(&self, config: SessionConfig) -> Result<Session> {
        config.validate()?;

        info!(
            target = %config.target,
            document = config.document.as_ref().map(|d| d.name.as_str()).unwrap_or("<shell>"),
            "starting SSM session"
        );

        let mut request = self.ssm.start_session().target(&config.target);
        if let Some(document) = &config.document {
            // `ShellSession` is spelled as the empty document name, because a
            // plain shell is what `StartSession` gives you when the field is
            // absent. Sending `DocumentName: ""` is not the same thing — the
            // API rejects it — so an empty name must mean "omit", not "send".
            if !document.name.is_empty() {
                request = request.document_name(&document.name);
            }
            if !document.parameters.is_empty() {
                request = request.set_parameters(Some(document.parameters.clone()));
            }
        }
        if let Some(reason) = &config.reason {
            request = request.reason(reason);
        }

        let response = request.send().await.map_err(Error::from)?;

        let session_id = response
            .session_id()
            .ok_or_else(|| Error::aws("StartSession returned no session ID"))?
            .to_owned();
        let stream_url = response
            .stream_url()
            .ok_or_else(|| Error::aws("StartSession returned no stream URL"))?
            .to_owned();
        let token = Zeroizing::new(
            response
                .token_value()
                .ok_or_else(|| Error::aws("StartSession returned no token"))?
                .to_owned(),
        );

        info!(session_id = %session_id, "SSM session created");

        match Session::open(
            session_id.clone(),
            config,
            stream_url,
            token,
            Some(Arc::clone(&self.ssm)),
            #[cfg(feature = "kms")]
            self.kms.clone(),
        )
        .await
        {
            Ok(session) => Ok(session),
            Err(e) => {
                // `StartSession` succeeded, so the session exists on the AWS
                // side and counts against the account's concurrent-session
                // quota even though no data channel was ever opened. Release it
                // now rather than leaving it to expire on its own timeout.
                warn!(error = %e, "data channel failed to open; releasing the AWS session");
                if let Err(cleanup) = self.terminate_session(&session_id).await {
                    warn!(
                        error = %cleanup,
                        "could not release the orphaned session; AWS will reclaim it on timeout"
                    );
                }
                Err(e)
            }
        }
    }

    /// Terminate a session by ID without holding a [`Session`] handle.
    pub async fn terminate_session(&self, session_id: &str) -> Result<()> {
        self.ssm
            .terminate_session()
            .session_id(session_id)
            .send()
            .await
            .map_err(Error::from)?;
        Ok(())
    }
}

impl std::fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        SessionConfig::new("i-0123456789abcdef0")
            .validate()
            .expect("defaults must be usable");
    }

    #[test]
    fn empty_target_is_rejected() {
        assert!(SessionConfig::new("   ").validate().is_err());
        assert!(SessionConfig::new("").validate().is_err());
    }

    /// An idle timeout at or below the ping interval declares healthy
    /// connections dead, so it is refused at construction rather than
    /// producing mysterious disconnects in production.
    #[test]
    fn idle_timeout_must_exceed_the_heartbeat_interval() {
        let mut config = SessionConfig::new("i-abc");
        config.heartbeat_interval = Duration::from_secs(30);
        config.idle_timeout = Duration::from_secs(30);
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("idle_timeout"), "{err}");

        config.idle_timeout = Duration::from_secs(31);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn chunk_size_bounds_are_enforced() {
        let mut config = SessionConfig::new("i-abc");
        config.payload_chunk_size = 0;
        assert!(config.validate().is_err());

        config.payload_chunk_size = crate::binary_protocol::MAX_PAYLOAD_SIZE + 1;
        assert!(config.validate().is_err());

        config.payload_chunk_size = crate::binary_protocol::MAX_PAYLOAD_SIZE;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn session_type_follows_the_document() {
        let shell = SessionConfig::new("i-abc");
        assert_eq!(shell.session_type(), SessionType::StandardStream);

        let forward = SessionConfig {
            document: Some(DocumentSpec::new(
                &crate::documents::PortForwardingSession::new(3306),
            )),
            ..SessionConfig::new("i-abc")
        };
        assert_eq!(forward.session_type(), SessionType::Port);
    }

    #[tokio::test]
    async fn close_records_the_first_reason_only() {
        let core = SessionCore::new("s-1".into(), 8);
        core.close(CloseReason::Transport("socket reset".into()));
        core.close(CloseReason::Terminated);
        assert_eq!(
            core.close_reason(),
            Some(CloseReason::Transport("socket reset".into())),
            "the original cause must survive a later terminate"
        );
    }

    #[tokio::test]
    async fn closed_resolves_for_waiters_registered_before_and_after() {
        let core = Arc::new(SessionCore::new("s-1".into(), 8));

        let waiter = tokio::spawn({
            let core = Arc::clone(&core);
            async move { core.closed().await }
        });
        tokio::task::yield_now().await;
        core.close(CloseReason::Terminated);

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("a registered waiter must be woken")
            .unwrap();

        // And a waiter arriving afterwards returns immediately.
        tokio::time::timeout(Duration::from_secs(1), core.closed())
            .await
            .expect("closing must be latched, not edge-triggered");
    }

    /// Anything parked waiting to send must be released when the session dies,
    /// otherwise a caller hangs forever on a channel that will never open.
    #[tokio::test]
    async fn closing_releases_tasks_waiting_to_send() {
        let core = Arc::new(SessionCore::new("s-1".into(), 8));
        let waiter = tokio::spawn({
            let core = Arc::clone(&core);
            async move { core.wait_sendable().await }
        });
        tokio::task::yield_now().await;
        core.close(CloseReason::Terminated);

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("wait_sendable must not outlive the session")
            .unwrap();
    }

    #[tokio::test]
    async fn output_stops_when_the_session_closes() {
        use futures_util::StreamExt;

        let core = SessionCore::new("s-1".into(), 8);
        let mut output = core.subscribe();
        core.emit_output(Bytes::from_static(b"hello"));
        core.close(CloseReason::Terminated);

        assert_eq!(output.next().await.unwrap(), Bytes::from_static(b"hello"));
        assert!(output.next().await.is_none());
    }

    #[test]
    fn close_reason_recoverability_is_classified() {
        assert!(CloseReason::Transport("reset".into()).is_recoverable());
        assert!(CloseReason::PeerUnresponsive {
            idle: Duration::from_secs(120)
        }
        .is_recoverable());
        assert!(!CloseReason::Terminated.is_recoverable());
        assert!(!CloseReason::AgentClosed {
            exit_code: Some(0),
            detail: None
        }
        .is_recoverable());
        assert!(!CloseReason::Protocol("bad digest".into()).is_recoverable());
    }

    #[test]
    fn close_reason_messages_name_the_cause() {
        assert!(CloseReason::AgentClosed {
            exit_code: Some(3),
            detail: None
        }
        .to_string()
        .contains('3'));
        assert!(CloseReason::DeliveryFailed {
            sequence: 9,
            attempts: 3000
        }
        .to_string()
        .contains("3000"));
    }

    #[tokio::test]
    async fn pause_publication_stops_sending_until_resumed() {
        let core = Arc::new(SessionCore::new("s-1".into(), 8));
        core.set_sendable(true);
        assert!(core.is_sendable());

        core.set_sendable(false);
        assert!(!core.is_sendable());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), core.wait_sendable())
                .await
                .is_err(),
            "a paused session must not report itself sendable"
        );

        core.set_sendable(true);
        tokio::time::timeout(Duration::from_millis(50), core.wait_sendable())
            .await
            .expect("resuming must release waiters");
    }
}
