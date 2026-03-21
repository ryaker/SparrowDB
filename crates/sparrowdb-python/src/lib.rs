/// PyO3 bindings for SparrowDB.
///
/// Phase 0: stub only. Build with `--features python` (or via `maturin`) to
/// activate the extension module.  Without that feature the crate is an empty
/// `rlib` so that `cargo test --workspace` never fails due to a missing pyo3
/// installation.
#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Python extension module entry point.
///
/// Activated only when compiled with `--features python`.
#[cfg(feature = "python")]
#[pymodule]
fn sparrowdb_python(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Phase 0 stub — bindings implemented in Phase 6.
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Verify the crate builds without pyo3 installed.
        // No assertion needed — if this function is reached, the crate compiled.
    }
}
