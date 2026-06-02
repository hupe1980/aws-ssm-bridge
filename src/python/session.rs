//! Python bindings for session management

use crate::{SessionConfig, SessionManager, SessionState, SessionType};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::to_py_err;

/// Python wrapper for SessionType
#[pyclass(name = "SessionType")]
#[derive(Clone)]
pub struct PySessionType {
    inner: SessionType,
}

#[pymethods]
impl PySessionType {
    /// Standard shell session
    #[classattr]
    const STANDARD_STREAM: &'static str = "standard_stream";

    /// Port forwarding session
    #[classattr]
    const PORT: &'static str = "port";

    /// Interactive commands (AWS-StartInteractiveCommand)
    #[classattr]
    const INTERACTIVE_COMMANDS: &'static str = "interactive_commands";

    #[new]
    fn new(session_type: &str) -> PyResult<Self> {
        let inner = match session_type {
            "standard_stream" => SessionType::StandardStream,
            "port" => SessionType::Port,
            "interactive_commands" => SessionType::InteractiveCommands,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid session type: '{}'. Valid types: 'standard_stream', 'port', 'interactive_commands'",
                    session_type
                )))
            }
        };

        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        format!("SessionType({:?})", self.inner)
    }
}

/// Python wrapper for SessionConfig
#[pyclass(name = "SessionConfig")]
#[derive(Clone)]
pub struct PySessionConfig {
    inner: SessionConfig,
}

#[pymethods]
impl PySessionConfig {
    #[new]
    #[pyo3(signature = (target, region=None, session_type=None, document_name=None, parameters=None, reason=None))]
    fn new(
        target: String,
        region: Option<String>,
        session_type: Option<PySessionType>,
        document_name: Option<String>,
        parameters: Option<HashMap<String, Vec<String>>>,
        reason: Option<String>,
    ) -> Self {
        let inner = SessionConfig {
            target,
            region,
            session_type: session_type
                .map(|t| t.inner)
                .unwrap_or(SessionType::StandardStream),
            document_name,
            parameters: parameters.unwrap_or_default(),
            reason,
            ..Default::default() // Uses default timeouts
        };

        Self { inner }
    }

    #[getter]
    fn target(&self) -> String {
        self.inner.target.clone()
    }

    #[getter]
    fn region(&self) -> Option<String> {
        self.inner.region.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "SessionConfig(target='{}', region={:?})",
            self.inner.target, self.inner.region
        )
    }
}

/// Python wrapper for Session
#[pyclass(name = "Session")]
pub struct PySession {
    inner: Arc<tokio::sync::Mutex<crate::Session>>,
    /// Cached session ID (avoids async lock for a read-only field)
    session_id: String,
    /// Cached ready signal — avoids holding the session lock during wait_for_ready.
    protocol_can_send: Arc<std::sync::atomic::AtomicBool>,
    /// Cached ready notify — avoids holding the session lock in __aenter__.
    ready_notify: Arc<tokio::sync::Notify>,
    /// Cached terminated notify — avoids holding the lock in wait_terminated.
    terminated_notify: Arc<tokio::sync::Notify>,
    /// Cached terminated latch — fast-path for wait_terminated when already done.
    terminated_flag: Arc<std::sync::atomic::AtomicBool>,
}

#[pymethods]
impl PySession {
    /// Get session ID (synchronous — no await needed)
    #[getter]
    fn id(&self) -> String {
        self.session_id.clone()
    }

    /// Get session state
    fn state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let session_guard = session.lock().await;
            let state = session_guard.state().await;
            let state_str = match state {
                SessionState::Initializing => "initializing",
                SessionState::Connected => "connected",
                SessionState::Disconnecting => "disconnecting",
                SessionState::Terminated => "terminated",
            };
            Ok(state_str)
        })
    }

    /// Check if the session is ready to send data (synchronous — no await needed).
    ///
    /// Reads a cached `AtomicBool` — no lock, no false negatives under contention.
    fn is_ready(&self) -> bool {
        self.protocol_can_send.load(Ordering::SeqCst)
    }

    /// Wait for the session to become ready.
    ///
    /// Does **not** acquire the session lock — uses a cached `Notify` and
    /// `AtomicBool` so all other Python operations on this session remain
    /// unblocked during the wait.
    #[pyo3(signature = (timeout_secs = 30.0))]
    fn wait_for_ready<'py>(
        &self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Validate before entering the async block: Duration::from_secs_f64
        // panics on NaN, infinite, negative, or overflow values, which would
        // abort the entire Python process.
        if !timeout_secs.is_finite()
            || timeout_secs < 0.0
            || timeout_secs > std::time::Duration::MAX.as_secs_f64()
        {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "timeout_secs must be a finite value in [0, {:.0}], got {timeout_secs}",
                std::time::Duration::MAX.as_secs_f64(),
            )));
        }
        let can_send = Arc::clone(&self.protocol_can_send);
        let ready_notify = Arc::clone(&self.ready_notify);
        future_into_py(py, async move {
            // Create the notified() future BEFORE the atomic load so that a
            // notification fired between the load and the await is not lost.
            let notified = ready_notify.notified();
            // Fast path: already ready.
            if can_send.load(Ordering::SeqCst) {
                return Ok(true);
            }
            let timeout = std::time::Duration::from_secs_f64(timeout_secs);
            match tokio::time::timeout(timeout, notified).await {
                Ok(_) => Ok(true),
                // Timeout — check once more (notification may have raced with timeout)
                Err(_) => Ok(can_send.load(Ordering::SeqCst)),
            }
        })
    }

    /// Get output stream for reading session output
    ///
    /// Returns an async iterator that yields bytes from the session output.
    ///
    /// Example:
    ///     async for chunk in session.output():
    ///         print(chunk.decode())
    fn output<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let session_guard = session.lock().await;
            let stream = session_guard.output();
            Ok(PyOutputStream {
                inner: Arc::new(tokio::sync::Mutex::new(stream)),
            })
        })
    }

    /// Send data to the session
    fn send<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let session_guard = session.lock().await;
            session_guard
                .send(bytes::Bytes::from(data))
                .await
                .map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Terminate the session
    fn terminate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut session_guard = session.lock().await;
            session_guard.terminate().await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Wait for session to terminate.
    ///
    /// Does **not** hold the session lock while waiting.
    fn wait_terminated<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let terminated_notify = Arc::clone(&self.terminated_notify);
        let terminated_flag = Arc::clone(&self.terminated_flag);
        future_into_py(py, async move {
            // Create the notified() future BEFORE the flag check to avoid the
            // race where terminated_flag is set between the load and the await.
            let notified = terminated_notify.notified();
            if !terminated_flag.load(Ordering::SeqCst) {
                notified.await;
            }
            Ok(())
        })
    }

    /// Async context manager entry — waits for ready without holding the lock.
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let can_send = Arc::clone(&slf.protocol_can_send);
        let ready_notify = Arc::clone(&slf.ready_notify);
        let self_obj: Py<PySession> = slf.into();
        future_into_py(py, async move {
            // Create the notified() future BEFORE the atomic load to avoid a
            // race where the session becomes ready between the check and the await.
            let notified = ready_notify.notified();
            if !can_send.load(Ordering::SeqCst) {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(30), notified).await;
            }
            Ok(self_obj)
        })
    }

    /// Async context manager exit - terminates the session.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_val: Option<Bound<'_, PyAny>>,
        _exc_tb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut session_guard = session.lock().await;
            // Best-effort termination, ignore errors on exit
            let _ = session_guard.terminate().await;
            Ok(false) // Don't suppress exceptions
        })
    }

    fn __repr__(&self) -> String {
        format!("Session(id='{}')", self.session_id)
    }
}

/// Python wrapper for SessionManager
#[pyclass(name = "SessionManager")]
pub struct PySessionManager {
    inner: Arc<RwLock<SessionManager>>,
}

#[pymethods]
impl PySessionManager {
    /// Create a new session manager
    ///
    /// If `region` is provided, it overrides the default AWS region.
    #[staticmethod]
    #[pyo3(signature = (region=None))]
    #[allow(clippy::new_ret_no_self)]
    fn new(py: Python<'_>, region: Option<String>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let config = if let Some(ref region) = region {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_config::Region::new(region.clone()))
                    .load()
                    .await
            } else {
                aws_config::load_from_env().await
            };
            let manager = SessionManager::with_config(&config);
            Ok(PySessionManager {
                inner: Arc::new(RwLock::new(manager)),
            })
        })
    }

    /// Start a new SSM session with individual parameters
    #[pyo3(signature = (target, region=None, session_type=None, document_name=None, parameters=None, reason=None))]
    #[allow(clippy::too_many_arguments)]
    fn start_session<'py>(
        &self,
        py: Python<'py>,
        target: String,
        region: Option<String>,
        session_type: Option<String>,
        document_name: Option<String>,
        parameters: Option<HashMap<String, Vec<String>>>,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = Arc::clone(&self.inner);

        // Parse session type
        let session_type_enum = if let Some(ref st) = session_type {
            match st.as_str() {
                "standard_stream" => SessionType::StandardStream,
                "port" => SessionType::Port,
                "interactive_commands" => SessionType::InteractiveCommands,
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid session type: '{}'. Valid types: 'standard_stream', 'port', 'interactive_commands'",
                        st
                    )))
                }
            }
        } else {
            SessionType::StandardStream
        };

        future_into_py(py, async move {
            let config = SessionConfig {
                target,
                region,
                session_type: session_type_enum,
                document_name,
                parameters: parameters.unwrap_or_default(),
                reason,
                ..Default::default()
            };

            let manager_guard = manager.read().await;
            let session = manager_guard
                .start_session(config)
                .await
                .map_err(to_py_err)?;

            let session_id = session.id().to_string();
            let protocol_can_send = session.can_send_signal();
            let ready_notify = session.ready_signal();
            let terminated_notify = session.terminated_signal();
            let terminated_flag = session.terminated_flag();
            Ok(PySession {
                inner: Arc::new(tokio::sync::Mutex::new(session)),
                session_id,
                protocol_can_send,
                ready_notify,
                terminated_notify,
                terminated_flag,
            })
        })
    }

    /// Start a new SSM session from a SessionConfig object
    fn start_session_with_config<'py>(
        &self,
        py: Python<'py>,
        config: PySessionConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let manager_guard = manager.read().await;
            let session = manager_guard
                .start_session(config.inner)
                .await
                .map_err(to_py_err)?;

            let session_id = session.id().to_string();
            let protocol_can_send = session.can_send_signal();
            let ready_notify = session.ready_signal();
            let terminated_notify = session.terminated_signal();
            let terminated_flag = session.terminated_flag();
            Ok(PySession {
                inner: Arc::new(tokio::sync::Mutex::new(session)),
                session_id,
                protocol_can_send,
                ready_notify,
                terminated_notify,
                terminated_flag,
            })
        })
    }

    /// Terminate a session by ID
    fn terminate_session<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manager = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let manager_guard = manager.read().await;
            manager_guard
                .terminate_session(&session_id)
                .await
                .map_err(to_py_err)?;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        "SessionManager()".to_string()
    }
}

/// Python wrapper for output stream
#[pyclass(name = "OutputStream")]
pub struct PyOutputStream {
    inner: Arc<tokio::sync::Mutex<crate::OutputStream>>,
}

#[pymethods]
impl PyOutputStream {
    /// Make this an async iterator
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get next chunk of output
    ///
    /// Returns the next bytes from the stream, or raises StopAsyncIteration when exhausted.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        use futures::StreamExt;
        use pyo3::exceptions::PyStopAsyncIteration;

        let stream = Arc::clone(&self.inner);
        let fut = future_into_py(py, async move {
            let mut stream_guard = stream.lock().await;
            match stream_guard.next().await {
                Some(bytes) => Ok(bytes.to_vec()),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })?;

        Ok(Some(fut))
    }

    fn __repr__(&self) -> String {
        "OutputStream()".to_string()
    }
}
