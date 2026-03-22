//! PyO3 Python bindings for SparrowDB.
//!
//! Exposes [`GraphDb`], [`PyReadTx`], and [`PyWriteTx`] to Python under the
//! module name `sparrowdb`.
//!
//! Build with `maturin develop` (or `maturin build --release`) to produce the
//! importable extension module.  The `python` feature is required; it is
//! activated automatically by maturin via `pyproject.toml`.

// ── Python feature guard ──────────────────────────────────────────────────────

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::{PyDict, PyList};

// ── Value → Python conversion ─────────────────────────────────────────────────

/// Convert a `sparrowdb_execution::Value` into a Python object.
#[cfg(feature = "python")]
fn value_to_py(py: Python<'_>, v: &sparrowdb_execution::Value) -> PyObject {
    use sparrowdb_execution::Value;
    match v {
        Value::Null => py.None(),
        Value::Int64(i) => i.into_py(py),
        Value::Float64(f) => f.into_py(py),
        Value::Bool(b) => b.into_py(py),
        Value::String(s) => s.into_py(py),
        // NodeRef / EdgeRef: expose the packed u64 id to Python.
        Value::NodeRef(n) => n.0.into_py(py),
        Value::EdgeRef(e) => e.0.into_py(py),
        // List: produced by collect() aggregation — convert to a Python list.
        Value::List(items) => {
            let py_list = PyList::empty_bound(py);
            for item in items {
                py_list.append(value_to_py(py, item)).unwrap();
            }
            py_list.into()
        }
    }
}

// ── PyGraphDb ─────────────────────────────────────────────────────────────────

/// Python wrapper around the top-level SparrowDB handle.
///
/// ```python
/// import sparrowdb, tempfile, os
/// with tempfile.TemporaryDirectory() as d:
///     db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
///     db.checkpoint()
/// ```
#[cfg(feature = "python")]
#[pyclass(name = "GraphDb")]
struct PyGraphDb {
    inner: ::sparrowdb::GraphDb,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyGraphDb {
    /// Open (or create) a SparrowDB database at *path*.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let db = ::sparrowdb::GraphDb::open(std::path::Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyGraphDb { inner: db })
    }

    /// Run a WAL checkpoint — folds the delta log into the CSR base files.
    fn checkpoint(&self) -> PyResult<()> {
        self.inner
            .checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Run an OPTIMIZE — like checkpoint but also sorts neighbour lists.
    fn optimize(&self) -> PyResult<()> {
        self.inner
            .optimize()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Open a read-only snapshot transaction.
    fn begin_read(&self) -> PyResult<PyReadTx> {
        let tx = self
            .inner
            .begin_read()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyReadTx { inner: tx })
    }

    /// Open a write transaction.
    ///
    /// Raises ``RuntimeError`` if another write transaction is already active
    /// (single-writer semantics).
    fn begin_write(&self) -> PyResult<PyWriteTx> {
        // SPA-181: WriteTx no longer carries a lifetime parameter — the write
        // lock is managed internally by an AtomicBool WriteGuard, so no unsafe
        // transmute is required here.
        let tx = self
            .inner
            .begin_write()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyWriteTx { inner: Some(tx) })
    }

    /// Execute a read-only Cypher query and return a list of row-dicts.
    ///
    /// Each row is a ``dict`` mapping column name → value.  Supported value
    /// types: ``None``, ``int``, ``float``, ``bool``, ``str``.
    /// ``NodeRef`` and ``EdgeRef`` are returned as ``int`` (their packed id).
    ///
    /// Example::
    ///
    ///     results = db.execute("MATCH (n) RETURN n.name LIMIT 10")
    ///     # [{"n.name": "Alice"}, ...]
    fn execute(&self, py: Python<'_>, cypher: &str) -> PyResult<PyObject> {
        let result = self
            .inner
            .execute(cypher)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let list = PyList::empty_bound(py);
        for row in &result.rows {
            let dict = PyDict::new_bound(py);
            for (col, val) in result.columns.iter().zip(row.iter()) {
                dict.set_item(col, value_to_py(py, val))?;
            }
            list.append(dict)?;
        }
        Ok(list.into())
    }

    fn __repr__(&self) -> &'static str {
        "GraphDb(<sparrowdb>)"
    }
}

// ── PyReadTx ──────────────────────────────────────────────────────────────────

/// Python wrapper around a read-only snapshot transaction.
///
/// Obtained via :meth:`GraphDb.begin_read`.
#[cfg(feature = "python")]
#[pyclass(name = "ReadTx")]
struct PyReadTx {
    inner: ::sparrowdb::ReadTx,
}

// ReadTx contains Arc<DbInner> (Send) + NodeStore (file handles).
// PyO3 requires pyclass types to be Send.  NodeStore only holds a PathBuf and
// opens files per-operation, making cross-thread use safe in practice.
#[cfg(feature = "python")]
unsafe impl Send for PyReadTx {}

#[cfg(feature = "python")]
#[pymethods]
impl PyReadTx {
    /// The committed ``txn_id`` this reader is pinned to.
    #[getter]
    fn snapshot_txn_id(&self) -> u64 {
        self.inner.snapshot_txn_id
    }

    fn __repr__(&self) -> String {
        format!("ReadTx(snapshot_txn_id={})", self.inner.snapshot_txn_id)
    }
}

// ── PyWriteTx ─────────────────────────────────────────────────────────────────

/// Python wrapper around a write transaction.
///
/// Obtained via :meth:`GraphDb.begin_write`.  Commit explicitly with
/// :meth:`commit`; dropping without committing discards staged changes
/// (rollback semantics).
#[cfg(feature = "python")]
#[pyclass(unsendable, name = "WriteTx")]
struct PyWriteTx {
    /// `None` after commit/rollback so use-after-commit is detected.
    inner: Option<::sparrowdb::WriteTx>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyWriteTx {
    /// Commit the transaction and return the new transaction id (``int``).
    ///
    /// Raises ``RuntimeError`` if already committed or rolled back.
    fn commit(&mut self) -> PyResult<u64> {
        match self.inner.take() {
            Some(tx) => {
                let txn_id = tx
                    .commit()
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok(txn_id.0)
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "transaction already committed or rolled back",
            )),
        }
    }

    fn __repr__(&self) -> &'static str {
        "WriteTx(<sparrowdb>)"
    }
}

// ── Module entry point ────────────────────────────────────────────────────────

/// Python extension module.  Import as ``import sparrowdb``.
#[cfg(feature = "python")]
#[pymodule]
fn sparrowdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGraphDb>()?;
    m.add_class::<PyReadTx>()?;
    m.add_class::<PyWriteTx>()?;
    Ok(())
}

// ── Tests (no pyo3 needed) ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Verify the crate builds without pyo3 installed (no `python` feature).
    }
}
