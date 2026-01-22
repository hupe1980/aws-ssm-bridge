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
fn _internal(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // Initialize tracing subscriber for logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
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

    // Add module version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

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
