//! Session, output stream and port forwarder bindings.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;

use super::to_py_err;
use crate::documents::SessionType;
use crate::{
    DocumentSpec, OutputStream, PortForwardConfig, PortForwarder, Session, SessionConfig,
    SessionManager, ShutdownSignal,
};

/// Starts SSM sessions.
///
/// ```python
/// manager = await SessionManager.new(region="eu-central-1")
/// session = await manager.start_session("i-0123456789abcdef0")
/// ```
#[pyclass(name = "SessionManager", frozen)]
#[derive(Debug)]
pub struct PySessionManager {
    inner: SessionManager,
}

#[pymethods]
impl PySessionManager {
    /// Build a manager, optionally pinned to a region.
    ///
    /// Credentials come from the standard AWS chain: environment variables,
    /// `~/.aws/config`, SSO, or instance metadata.
    #[staticmethod]
    #[pyo3(signature = (region = None))]
    #[allow(clippy::new_ret_no_self)]
    fn new(py: Python<'_>, region: Option<String>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let inner = match region {
                Some(region) => SessionManager::for_region(region).await,
                None => SessionManager::new().await,
            }
            .map_err(to_py_err)?;
            Ok(PySessionManager { inner })
        })
    }

    /// Start a session.
    ///
    /// Args:
    ///     target: instance ID, `mi-` managed instance, `ecs:` task or ARN.
    ///     document_name: SSM document, e.g. `AWS-StartInteractiveCommand`.
    ///         Omit for a plain shell.
    ///     parameters: document parameters, `{"portNumber": ["3306"]}`.
    ///     reason: recorded in CloudTrail.
    ///     ready_timeout: seconds to wait for the agent handshake.
    #[pyo3(signature = (
        target,
        document_name = None,
        parameters = None,
        reason = None,
        ready_timeout = 30.0,
    ))]
    fn start_session<'py>(
        &self,
        py: Python<'py>,
        target: String,
        document_name: Option<String>,
        parameters: Option<HashMap<String, Vec<String>>>,
        reason: Option<String>,
        ready_timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();
        let config = build_config(target, document_name, parameters, reason, ready_timeout)?;

        future_into_py(py, async move {
            let session = manager.start_session(config).await.map_err(to_py_err)?;
            Ok(PySession {
                inner: Arc::new(session),
            })
        })
    }

    /// Start a port-forwarding session to a port on the instance itself.
    #[pyo3(signature = (target, remote_port, reason = None))]
    fn start_port_forward<'py>(
        &self,
        py: Python<'py>,
        target: String,
        remote_port: u16,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();
        let config = SessionConfig {
            document: Some(DocumentSpec::new(
                &crate::documents::PortForwardingSession::new(remote_port),
            )),
            reason,
            ..SessionConfig::new(target)
        };
        future_into_py(py, async move {
            let session = manager.start_session(config).await.map_err(to_py_err)?;
            Ok(PySession {
                inner: Arc::new(session),
            })
        })
    }

    /// Start a port-forwarding session that reaches `host:remote_port` through
    /// the target instance.
    #[pyo3(signature = (target, host, remote_port, reason = None))]
    fn start_remote_port_forward<'py>(
        &self,
        py: Python<'py>,
        target: String,
        host: String,
        remote_port: u16,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();
        let config = SessionConfig {
            document: Some(DocumentSpec::new(
                &crate::documents::PortForwardingToRemoteHost::new(host, remote_port),
            )),
            reason,
            ..SessionConfig::new(target)
        };
        future_into_py(py, async move {
            let session = manager.start_session(config).await.map_err(to_py_err)?;
            Ok(PySession {
                inner: Arc::new(session),
            })
        })
    }

    /// Terminate a session by ID without holding a session object.
    fn terminate_session<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = self.inner.clone();
        future_into_py(py, async move {
            manager
                .terminate_session(&session_id)
                .await
                .map_err(to_py_err)
        })
    }

    fn __repr__(&self) -> String {
        "SessionManager()".to_owned()
    }
}

fn build_config(
    target: String,
    document_name: Option<String>,
    parameters: Option<HashMap<String, Vec<String>>>,
    reason: Option<String>,
    ready_timeout: f64,
) -> PyResult<SessionConfig> {
    let parameters = parameters.unwrap_or_default();
    let document = document_name.map(|name| {
        // The session type only decides whether PortForwarder will accept this
        // session, and only the two forwarding documents are multiplexed.
        let session_type = if name.starts_with("AWS-StartPortForwardingSession") {
            SessionType::Port
        } else {
            SessionType::StandardStream
        };
        DocumentSpec {
            name,
            parameters,
            session_type,
        }
    });

    Ok(SessionConfig {
        document,
        reason,
        ready_timeout: seconds(ready_timeout, "ready_timeout")?,
        ..SessionConfig::new(target)
    })
}

/// Convert a Python float to a `Duration`, rejecting the values that would
/// otherwise panic inside `Duration::from_secs_f64`.
fn seconds(value: f64, name: &str) -> PyResult<Duration> {
    if !value.is_finite() || value < 0.0 || value > Duration::MAX.as_secs_f64() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{name} must be a finite, non-negative number of seconds, got {value}"
        )));
    }
    Ok(Duration::from_secs_f64(value))
}

/// An open SSM session.
///
/// Use it as an async context manager to guarantee termination:
///
/// ```python
/// async with await manager.start_session("i-0123456789abcdef0") as session:
///     await session.send(b"uname -a\r")
///     async for chunk in session.output():
///         print(chunk.decode(errors="replace"), end="")
/// ```
#[pyclass(name = "Session", frozen)]
#[derive(Debug)]
pub struct PySession {
    pub(crate) inner: Arc<Session>,
}

#[pymethods]
impl PySession {
    /// The AWS session ID.
    #[getter]
    fn id(&self) -> &str {
        self.inner.id()
    }

    /// The target this session connects to.
    #[getter]
    fn target(&self) -> &str {
        &self.inner.config().target
    }

    /// Version of the SSM agent, once the handshake has run.
    #[getter]
    fn agent_version(&self) -> Option<&str> {
        self.inner.agent_version()
    }

    /// The agent's login banner, if it sent one.
    #[getter]
    fn banner(&self) -> Option<String> {
        self.inner.banner()
    }

    /// Exit status of the remote process, once it has exited.
    #[getter]
    fn exit_code(&self) -> Option<i32> {
        self.inner.exit_code()
    }

    /// Whether session data is encrypted end-to-end with a KMS-derived key.
    #[getter]
    fn is_encrypted(&self) -> bool {
        self.inner.is_encrypted()
    }

    /// Whether the agent handshake has completed.
    #[getter]
    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    /// Whether the session has ended.
    #[getter]
    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Why the session ended, or `None` while it is still open.
    #[getter]
    fn close_reason(&self) -> Option<String> {
        self.inner.close_reason().map(|r| r.to_string())
    }

    /// Wait for the agent handshake to finish.
    fn wait_ready<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(
            py,
            async move { session.wait_ready().await.map_err(to_py_err) },
        )
    }

    /// Wait for the session to end.
    fn wait_closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            session.closed().await;
            Ok(())
        })
    }

    /// Send bytes to the remote process's standard input.
    ///
    /// Send `\r`, not `\n`, for Enter — see the Rust docs for why.
    fn send<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            session
                .send(bytes::Bytes::from(data))
                .await
                .map_err(to_py_err)
        })
    }

    /// Tell the remote pty the terminal has been resized.
    fn send_terminal_size<'py>(
        &self,
        py: Python<'py>,
        cols: u16,
        rows: u16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            session
                .send_terminal_size(cols, rows)
                .await
                .map_err(to_py_err)
        })
    }

    /// Subscribe to the session's output.
    ///
    /// Returns an async iterator of `bytes`. Subscribe before sending anything
    /// you want to see the response to; earlier output is not replayed.
    fn output(&self) -> PyOutputStream {
        PyOutputStream {
            inner: Arc::new(tokio::sync::Mutex::new(self.inner.output())),
        }
    }

    /// End the session and release it on the AWS side. Idempotent.
    fn terminate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(
            py,
            async move { session.terminate().await.map_err(to_py_err) },
        )
    }

    /// Wait for readiness on entry, so the body can send immediately.
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&slf.inner);
        let handle: Py<PySession> = slf.into();
        future_into_py(py, async move {
            session.wait_ready().await.map_err(to_py_err)?;
            Ok(handle)
        })
    }

    /// Terminate on exit, even when the body raised.
    #[pyo3(signature = (_exc_type = None, _exc_value = None, _traceback = None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            // Best effort: an exception propagating out of __aexit__ would mask
            // whatever the body was already raising.
            let _ = session.terminate().await;
            Ok(false) // do not suppress exceptions
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Session(id={:?}, target={:?}, ready={}, closed={})",
            self.inner.id(),
            self.inner.config().target,
            self.inner.is_ready(),
            self.inner.is_closed(),
        )
    }
}

/// An async iterator over a session's output.
#[pyclass(name = "OutputStream", frozen)]
#[derive(Debug)]
pub struct PyOutputStream {
    inner: Arc<tokio::sync::Mutex<OutputStream>>,
}

#[pymethods]
impl PyOutputStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        use pyo3::exceptions::PyStopAsyncIteration;

        let stream = Arc::clone(&self.inner);
        let future = future_into_py(py, async move {
            match stream.lock().await.recv().await {
                // Re-attach to the interpreter to build a real `bytes` object.
                // Returning `Vec<u8>` would hand Python a list of ints instead.
                Some(chunk) => Ok(Python::attach(|py| {
                    PyBytes::new(py, &chunk).unbind().into_any()
                })),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })?;
        Ok(Some(future))
    }

    fn __repr__(&self) -> String {
        "OutputStream()".to_owned()
    }
}

/// Forwards a local TCP port over a port-forwarding session.
///
/// ```python
/// session = await manager.start_port_forward("i-0123456789abcdef0", 3306)
/// forwarder = await PortForwarder.bind("127.0.0.1:13306")
/// print("mysql -h 127.0.0.1 -P", forwarder.port)
/// await forwarder.forward(session)   # runs until the session ends
/// ```
#[pyclass(name = "PortForwarder")]
#[derive(Debug)]
pub struct PyPortForwarder {
    forwarder: Option<PortForwarder>,
    local_addr: std::net::SocketAddr,
    shutdown: ShutdownSignal,
}

#[pymethods]
impl PyPortForwarder {
    /// Bind a local address.
    ///
    /// Use port `0` to let the OS pick; read it back from `port`.
    #[staticmethod]
    #[pyo3(signature = (local_addr = "127.0.0.1:0", max_connections = 100))]
    fn bind<'py>(
        py: Python<'py>,
        local_addr: &str,
        max_connections: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let addr: std::net::SocketAddr = local_addr.parse().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "{local_addr:?} is not a valid address: {e}"
            ))
        })?;

        future_into_py(py, async move {
            let forwarder = PortForwarder::bind(PortForwardConfig {
                local_addr: addr,
                max_connections,
                ..Default::default()
            })
            .await
            .map_err(to_py_err)?;

            Ok(PyPortForwarder {
                local_addr: forwarder.local_addr(),
                forwarder: Some(forwarder),
                shutdown: ShutdownSignal::new(),
            })
        })
    }

    /// The bound address, with the OS-assigned port resolved.
    #[getter]
    fn address(&self) -> String {
        self.local_addr.to_string()
    }

    /// The bound port.
    #[getter]
    fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Accept and forward connections until the session ends or `stop` is called.
    fn forward<'py>(
        &mut self,
        py: Python<'py>,
        session: PyRef<'_, PySession>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let forwarder = self.forwarder.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "this PortForwarder has already been used; bind a new one",
            )
        })?;
        let session = Arc::clone(&session.inner);
        let shutdown = self.shutdown.clone();

        future_into_py(py, async move {
            forwarder
                .forward(session, shutdown)
                .await
                .map_err(to_py_err)
        })
    }

    /// Stop forwarding and release the local port.
    fn stop(&self) {
        self.shutdown.shutdown();
    }

    fn __repr__(&self) -> String {
        format!("PortForwarder(address={:?})", self.local_addr.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A NaN or infinite timeout would panic inside `Duration::from_secs_f64`
    /// and take the whole interpreter down.
    #[test]
    fn invalid_timeouts_raise_instead_of_panicking() {
        for bad in [f64::NAN, f64::INFINITY, -1.0, f64::MAX] {
            assert!(
                seconds(bad, "ready_timeout").is_err(),
                "{bad} must be rejected"
            );
        }
        assert_eq!(seconds(1.5, "t").unwrap(), Duration::from_millis(1500));
        assert_eq!(seconds(0.0, "t").unwrap(), Duration::ZERO);
    }

    /// Only the forwarding documents may be handed to `PortForwarder`, so the
    /// document name has to map to the right session type.
    #[test]
    fn document_names_map_to_the_right_session_type() {
        let port = build_config(
            "i-abc".into(),
            Some("AWS-StartPortForwardingSessionToRemoteHost".into()),
            None,
            None,
            30.0,
        )
        .unwrap();
        assert_eq!(port.session_type(), SessionType::Port);

        let ssh = build_config(
            "i-abc".into(),
            Some("AWS-StartSSHSession".into()),
            None,
            None,
            30.0,
        )
        .unwrap();
        assert_eq!(ssh.session_type(), SessionType::StandardStream);

        let shell = build_config("i-abc".into(), None, None, None, 30.0).unwrap();
        assert!(shell.document.is_none());
    }
}
