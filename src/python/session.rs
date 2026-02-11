//! Python bindings for session management

use crate::{SessionConfig, SessionManager, SessionState, SessionType};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
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
    /// The session is ready once the SSM agent has completed the handshake
    /// and sent the start_publication message.
    fn is_ready(&self) -> bool {
        // is_ready() only reads an AtomicBool — no lock needed
        // We access it through a short lock, but since we cached session_id
        // and is_ready is atomic, we can use try_lock for a non-blocking check
        if let Ok(guard) = self.inner.try_lock() {
            guard.is_ready()
        } else {
            false
        }
    }

    /// Wait for the session to become ready.
    ///
    /// Blocks until the session is ready or timeout expires.
    /// Call this after start_session() before sending data.
    #[pyo3(signature = (timeout_secs = 30.0))]
    fn wait_for_ready<'py>(
        &self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let timeout = std::time::Duration::from_secs_f64(timeout_secs);
            // Acquire lock briefly to call wait_for_ready, which uses Notify
            // internally (no spin-polling, so holding the lock is fine now)
            let session_guard = session.lock().await;
            Ok(session_guard.wait_for_ready(timeout).await)
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

    /// Wait for session to terminate
    fn wait_terminated<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let session_guard = session.lock().await;
            session_guard.wait_terminated().await;
            Ok(())
        })
    }

    /// Async context manager entry — returns `self` (preserves object identity).
    ///
    /// Allows using `async with` syntax:
    /// ```python
    /// async with await manager.start_session("i-xxx") as session:
    ///     await session.send(b"ls\n")
    /// # Session automatically terminated
    /// ```
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&slf.inner);
        let self_obj: Py<PySession> = slf.into();
        future_into_py(py, async move {
            // Wait for ready with default timeout
            let session_guard = session.lock().await;
            let _ = session_guard
                .wait_for_ready(std::time::Duration::from_secs(30))
                .await;
            drop(session_guard);
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
            Ok(PySession {
                inner: Arc::new(tokio::sync::Mutex::new(session)),
                session_id,
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
            Ok(PySession {
                inner: Arc::new(tokio::sync::Mutex::new(session)),
                session_id,
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
