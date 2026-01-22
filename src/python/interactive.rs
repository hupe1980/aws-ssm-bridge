//! Python bindings for interactive shell

use crate::interactive::{InteractiveConfig, InteractiveShell};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::to_py_err;

/// Configuration for interactive shell sessions
#[pyclass(name = "InteractiveConfig")]
#[derive(Clone)]
pub struct PyInteractiveConfig {
    pub(crate) inner: InteractiveConfig,
}

#[pymethods]
impl PyInteractiveConfig {
    /// Create a new interactive config with default settings
    #[new]
    #[pyo3(signature = (show_banner=true, send_initial_size=true, forward_signals=true))]
    fn new(show_banner: bool, send_initial_size: bool, forward_signals: bool) -> Self {
        Self {
            inner: InteractiveConfig {
                show_banner,
                send_initial_size,
                forward_signals,
                ..Default::default()
            },
        }
    }

    /// Create default config
    #[staticmethod]
    fn default() -> Self {
        Self {
            inner: InteractiveConfig::default(),
        }
    }

    #[getter]
    fn show_banner(&self) -> bool {
        self.inner.show_banner
    }

    #[getter]
    fn send_initial_size(&self) -> bool {
        self.inner.send_initial_size
    }

    #[getter]
    fn forward_signals(&self) -> bool {
        self.inner.forward_signals
    }

    fn __repr__(&self) -> String {
        format!(
            "InteractiveConfig(show_banner={}, send_initial_size={}, forward_signals={})",
            self.inner.show_banner, self.inner.send_initial_size, self.inner.forward_signals
        )
    }
}

/// Interactive shell session with full terminal support.
///
/// Provides a complete interactive shell experience with:
/// - Raw terminal mode (no echo, immediate input)
/// - Terminal resize detection (SIGWINCH on Unix)
/// - Signal forwarding (Ctrl+C, Ctrl+D, Ctrl+Z)
/// - Automatic terminal restoration on exit/crash
///
/// Example:
///     >>> from aws_ssm_bridge import InteractiveShell, InteractiveConfig
///     >>>
///     >>> async def main():
///     ...     config = InteractiveConfig.default()
///     ...     shell = InteractiveShell(config)
///     ...     await shell.connect("i-0123456789abcdef0")
///     ...     await shell.run()  # Blocks until Ctrl+D or session closes
///     >>>
///     >>> import asyncio
///     >>> asyncio.run(main())
#[pyclass(name = "InteractiveShell")]
pub struct PyInteractiveShell {
    inner: Arc<Mutex<Option<InteractiveShell>>>,
}

#[pymethods]
impl PyInteractiveShell {
    /// Create a new interactive shell with the given configuration
    #[new]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<PyInteractiveConfig>) -> PyResult<Self> {
        let config = config.map(|c| c.inner).unwrap_or_default();
        let shell = InteractiveShell::new(config).map_err(to_py_err)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(shell))),
        })
    }

    /// Connect to an EC2 instance
    ///
    /// Args:
    ///     target: Instance ID to connect to (e.g., "i-0123456789abcdef0")
    ///
    /// Raises:
    ///     RuntimeError: If connection fails or already connected
    fn connect<'py>(&self, py: Python<'py>, target: String) -> PyResult<Bound<'py, PyAny>> {
        let shell = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = shell.lock().await;
            let shell = guard.as_mut().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("Shell already consumed")
            })?;
            shell.connect(&target).await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Run the interactive session.
    ///
    /// This method blocks until:
    /// - User presses Ctrl+D (EOF)
    /// - Session is closed by remote
    /// - An error occurs
    ///
    /// The terminal is automatically restored to its original state on exit,
    /// even if an error or panic occurs.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let shell = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = shell.lock().await;
            let shell = guard.as_mut().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("Shell already consumed")
            })?;
            shell.run().await.map_err(to_py_err)?;
            Ok(())
        })
    }

    /// Check if connected to an instance
    fn is_connected<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let shell = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let guard = shell.lock().await;
            if let Some(shell) = guard.as_ref() {
                Ok(shell.is_connected().await)
            } else {
                Ok(false)
            }
        })
    }

    fn __repr__(&self) -> String {
        "InteractiveShell()".to_string()
    }
}

/// Convenience function for quick interactive session.
///
/// This is the simplest way to start an interactive shell:
///
/// Example:
///     >>> from aws_ssm_bridge import run_shell
///     >>> import asyncio
///     >>> asyncio.run(run_shell("i-0123456789abcdef0"))
#[pyfunction]
#[pyo3(name = "run_shell")]
fn py_run_shell(py: Python<'_>, target: String) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        crate::interactive::run_shell(&target)
            .await
            .map_err(to_py_err)?;
        Ok(())
    })
}

/// Get the run_shell function and classes for registration
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyInteractiveConfig>()?;
    m.add_class::<PyInteractiveShell>()?;
    m.add_function(wrap_pyfunction!(py_run_shell, m)?)?;
    Ok(())
}
