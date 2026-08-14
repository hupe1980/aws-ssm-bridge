//! PyO3 bindings.
//!
//! Built with `maturin develop --features python`. The Python surface mirrors
//! the Rust API: a [`SessionManager`](crate::SessionManager) starts sessions,
//! a session streams bytes, and everything is `async`.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

mod interactive;
mod session;

use interactive::PyInteractiveShell;
use session::{PyOutputStream, PyPortForwarder, PySession, PySessionManager};

/// Python exception hierarchy.
///
/// ```text
/// SsmError
///  ├── SsmAwsError         AWS API failures
///  ├── SsmProtocolError    the peer violated the protocol
///  ├── SsmTransportError   the WebSocket failed
///  ├── SsmClosedError      the session is no longer usable
///  ├── SsmCryptoError      KMS session encryption failed
///  └── SsmTimeoutError     an operation ran out of time
/// ```
///
/// Catch `SsmError` for everything, or a subclass to react to one failure mode.
#[allow(missing_docs)]
mod exceptions {
    pyo3::create_exception!(_internal, SsmError, pyo3::exceptions::PyException);
    pyo3::create_exception!(_internal, SsmAwsError, SsmError);
    pyo3::create_exception!(_internal, SsmProtocolError, SsmError);
    pyo3::create_exception!(_internal, SsmTransportError, SsmError);
    pyo3::create_exception!(_internal, SsmClosedError, SsmError);
    pyo3::create_exception!(_internal, SsmCryptoError, SsmError);
    pyo3::create_exception!(_internal, SsmTimeoutError, SsmError);
}
use exceptions::*;

/// Map a Rust error onto the most specific Python exception.
pub(crate) fn to_py_err(error: crate::Error) -> PyErr {
    use crate::Error;
    match error {
        Error::Aws { message, .. } => SsmAwsError::new_err(message),
        Error::Protocol(message) => SsmProtocolError::new_err(message),
        Error::Transport(message) => SsmTransportError::new_err(message),
        Error::SessionClosed(message) => SsmClosedError::new_err(message),
        Error::Crypto(message) => SsmCryptoError::new_err(message),
        Error::Timeout(duration) => {
            SsmTimeoutError::new_err(format!("operation timed out after {duration:?}"))
        }
        Error::Config(message) => PyValueError::new_err(message),
        Error::Unsupported(message) => PyRuntimeError::new_err(message),
        other => SsmError::new_err(other.to_string()),
    }
}

/// Route the crate's `tracing` output to stderr.
///
/// Call once before opening a session. `RUST_LOG` still applies and takes
/// precedence for finer-grained filtering. Subsequent calls are ignored, because
/// the global subscriber can only be installed once per process.
#[pyfunction]
#[pyo3(signature = (level = "warn"))]
fn configure_logging(level: &str) -> PyResult<()> {
    use tracing::level_filters::LevelFilter;

    let filter = match level.to_ascii_lowercase().as_str() {
        "off" => LevelFilter::OFF,
        "error" => LevelFilter::ERROR,
        "warn" => LevelFilter::WARN,
        "info" => LevelFilter::INFO,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown log level {other:?}; use off, error, warn, info, debug or trace"
            )))
        }
    };

    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(filter.into())
                .from_env_lossy(),
        )
        .try_init();
    Ok(())
}

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySessionManager>()?;
    m.add_class::<PySession>()?;
    m.add_class::<PyOutputStream>()?;
    m.add_class::<PyPortForwarder>()?;
    m.add_class::<PyInteractiveShell>()?;

    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;

    m.add("SsmError", m.py().get_type::<SsmError>())?;
    m.add("SsmAwsError", m.py().get_type::<SsmAwsError>())?;
    m.add("SsmProtocolError", m.py().get_type::<SsmProtocolError>())?;
    m.add("SsmTransportError", m.py().get_type::<SsmTransportError>())?;
    m.add("SsmClosedError", m.py().get_type::<SsmClosedError>())?;
    m.add("SsmCryptoError", m.py().get_type::<SsmCryptoError>())?;
    m.add("SsmTimeoutError", m.py().get_type::<SsmTimeoutError>())?;

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
