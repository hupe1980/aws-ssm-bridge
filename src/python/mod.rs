//! Python bindings for aws-ssm-bridge
//!
//! This module provides Python bindings using PyO3 for the aws-ssm-bridge library.

use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tracing_subscriber;

mod interactive;
mod session;

use session::{PyOutputStream, PySession, PySessionConfig, PySessionManager, PySessionType};

/// Initialize the Python module
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing subscriber for logging.
    // Respects RUST_LOG env var; defaults to WARN (quiet).
    // Users can set RUST_LOG=info or RUST_LOG=debug for verbose output.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::WARN.into()),
        )
        .try_init();

    // Register classes
    m.add_class::<PySessionManager>()?;
    m.add_class::<PySession>()?;
    m.add_class::<PySessionConfig>()?;
    m.add_class::<PySessionType>()?;
    m.add_class::<PyOutputStream>()?;

    // Register interactive shell classes
    interactive::register(m)?;

    // Register utility functions
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;

    // Add module version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}

/// Configure logging verbosity.
///
/// Call this before creating a SessionManager to control log output.
/// Levels: "error", "warn", "info", "debug", "trace"
///
/// Alternatively, set the RUST_LOG environment variable (e.g. RUST_LOG=debug).
/// Note: logging can only be configured once per process.
#[pyfunction]
#[pyo3(signature = (level="warn"))]
fn configure_logging(level: &str) -> PyResult<()> {
    let directive: tracing::level_filters::LevelFilter = match level.to_lowercase().as_str() {
        "error" => tracing::Level::ERROR.into(),
        "warn" => tracing::Level::WARN.into(),
        "info" => tracing::Level::INFO.into(),
        "debug" => tracing::Level::DEBUG.into(),
        "trace" => tracing::Level::TRACE.into(),
        "off" => tracing::level_filters::LevelFilter::OFF,
        _ => {
            return Err(PyValueError::new_err(format!(
                "Invalid log level '{}'. Use: off, error, warn, info, debug, trace",
                level
            )));
        }
    };

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive(directive.into()),
        )
        .try_init();

    Ok(())
}

/// Convert Rust errors to Python exceptions
pub(crate) fn to_py_err(err: crate::Error) -> PyErr {
    use crate::Error;

    match err {
        Error::Config(msg) => PyValueError::new_err(msg),
        Error::InvalidState(msg) => PyRuntimeError::new_err(msg),
        Error::Timeout => PyException::new_err("Operation timed out"),
        Error::Cancelled => PyException::new_err("Operation was cancelled"),
        _ => PyRuntimeError::new_err(format!("{}", err)),
    }
}
