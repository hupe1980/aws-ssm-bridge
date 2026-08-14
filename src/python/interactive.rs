//! Interactive-shell binding.

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use super::to_py_err;

/// Runs a full interactive shell against the local terminal.
///
/// ```python
/// import asyncio
/// from aws_ssm_bridge import InteractiveShell
///
/// asyncio.run(InteractiveShell().run("i-0123456789abcdef0"))
/// ```
///
/// Requires stdin and stdout to be a real terminal; raises `ValueError`
/// otherwise. Not available when the crate is built without the `interactive`
/// feature.
#[pyclass(name = "InteractiveShell", frozen)]
#[derive(Debug)]
pub struct PyInteractiveShell {
    #[cfg(feature = "interactive")]
    config: crate::InteractiveConfig,
}

#[pymethods]
impl PyInteractiveShell {
    /// Build a shell.
    ///
    /// Args:
    ///     show_banner: print the session ID and the exit notice.
    ///     region: AWS region; `None` uses the ambient configuration.
    ///     reason: recorded in CloudTrail.
    #[new]
    #[pyo3(signature = (show_banner = true, region = None, reason = None))]
    #[allow(unused_variables)]
    fn new(show_banner: bool, region: Option<String>, reason: Option<String>) -> Self {
        Self {
            #[cfg(feature = "interactive")]
            config: crate::InteractiveConfig {
                show_banner,
                send_initial_size: true,
                region,
                reason,
            },
        }
    }

    /// Connect to `target` and run until the session ends.
    ///
    /// Returns the remote process's exit code when the agent reported one.
    #[allow(unused_variables)]
    fn run<'py>(&self, py: Python<'py>, target: String) -> PyResult<Bound<'py, PyAny>> {
        #[cfg(feature = "interactive")]
        {
            let config = self.config.clone();
            future_into_py(py, async move {
                crate::InteractiveShell::new(config)
                    .run(&target)
                    .await
                    .map_err(to_py_err)
            })
        }
        #[cfg(not(feature = "interactive"))]
        {
            let _ = to_py_err;
            future_into_py(py, async move {
                Err::<Option<i32>, _>(pyo3::exceptions::PyRuntimeError::new_err(
                    "this build was compiled without the `interactive` feature",
                ))
            })
        }
    }

    fn __repr__(&self) -> String {
        "InteractiveShell()".to_owned()
    }
}
