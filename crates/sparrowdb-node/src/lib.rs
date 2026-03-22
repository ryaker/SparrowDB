//! napi-rs native Node.js/TypeScript bindings for SparrowDB.
//!
//! Build with:
//!   cargo build --release -p sparrowdb-node
//!   # or via napi-cli: napi build --platform --release
//!
//! The resulting `sparrowdb.node` file can be loaded from Node.js:
//!   const { SparrowDB } = require('./sparrowdb.node')

#![deny(clippy::all)]

use napi_derive::napi;
use sparrowdb_execution::query_result_to_json;

// ── Error helper ──────────────────────────────────────────────────────────────

/// Convert any `Display`-able error into a `napi::Error`.
#[inline]
fn to_napi<E: std::fmt::Display>(e: E) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

// ── SparrowDB ─────────────────────────────────────────────────────────────────

/// Top-level database handle.
///
/// ```typescript
/// import { SparrowDB } from 'sparrowdb'
///
/// const db = SparrowDB.open('/path/to/my.db')
/// const result = db.execute('MATCH (n:Person) RETURN n.name LIMIT 5')
/// for (const row of result.rows) {
///   console.log(row['n.name'])
/// }
/// db.checkpoint()
/// ```
#[napi(js_name = "SparrowDB")]
pub struct SparrowDB {
    inner: ::sparrowdb::GraphDb,
}

// GraphDb uses Arc<DbInner> internally; cross-thread use is safe.
unsafe impl Send for SparrowDB {}

#[napi]
impl SparrowDB {
    /// Open (or create) a SparrowDB database at `path`.
    ///
    /// Throws if the path cannot be created or the database files are corrupt.
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let db = ::sparrowdb::GraphDb::open(std::path::Path::new(&path)).map_err(to_napi)?;
        Ok(SparrowDB { inner: db })
    }

    /// Execute a Cypher query and return `{ columns, rows }`.
    ///
    /// Each row is a plain object mapping column name → value.
    /// Supported value types: `null`, `number`, `boolean`, `string`,
    /// `{ $type: "node", id: string }`, `{ $type: "edge", id: string }`.
    /// Note: `Int64` values outside `±(2^53-1)` are also returned as strings.
    #[napi]
    pub fn execute(&self, cypher: String) -> napi::Result<serde_json::Value> {
        let result = self.inner.execute(&cypher).map_err(to_napi)?;
        Ok(query_result_to_json(&result))
    }

    /// Flush the WAL and compact the database.
    #[napi]
    pub fn checkpoint(&self) -> napi::Result<()> {
        self.inner.checkpoint().map_err(to_napi)
    }

    /// Checkpoint + sort neighbour lists for faster traversal.
    #[napi]
    pub fn optimize(&self) -> napi::Result<()> {
        self.inner.optimize().map_err(to_napi)
    }

    /// Open a read-only snapshot transaction pinned to the current committed
    /// state.  Multiple readers may coexist with an active writer.
    #[napi]
    pub fn begin_read(&self) -> napi::Result<ReadTx> {
        let tx = self.inner.begin_read().map_err(to_napi)?;
        Ok(ReadTx { inner: tx })
    }

    /// Open a write transaction.  Only one writer may be active at a time;
    /// throws `WriterBusy` if another writer is already open.
    #[napi]
    pub fn begin_write(&self) -> napi::Result<WriteTx> {
        // Safety: same transmute as in the PyO3 bindings.  WriteTx<'db> only
        // holds a MutexGuard whose lifetime is constrained to the GraphDb.
        // SparrowDB wraps GraphDb directly (not behind a shared reference that
        // could be dropped independently), so the guard lives as long as this
        // SparrowDB object, which napi-rs keeps alive while JS holds the handle.
        let tx: ::sparrowdb::WriteTx<'static> =
            unsafe { std::mem::transmute(self.inner.begin_write().map_err(to_napi)?) };
        Ok(WriteTx { inner: Some(tx) })
    }
}

// ── ReadTx ────────────────────────────────────────────────────────────────────

/// Read-only snapshot transaction.  Sees only data committed at or before the
/// snapshot point; immune to concurrent writes.
#[napi]
pub struct ReadTx {
    inner: ::sparrowdb::ReadTx,
}

unsafe impl Send for ReadTx {}

#[napi]
impl ReadTx {
    /// The committed `txn_id` this reader is pinned to.
    ///
    /// Returned as a decimal **string** to preserve the full u64 range —
    /// JavaScript's `Number` can only represent integers up to 2^53-1 safely.
    #[napi(getter)]
    pub fn snapshot_txn_id(&self) -> String {
        self.inner.snapshot_txn_id.to_string()
    }

    /// Execute a read-only Cypher query against this snapshot.
    ///
    /// **Not yet implemented.** The underlying `GraphDb::execute` always reads
    /// from the latest committed state; snapshot-pinned execution is tracked
    /// in SPA-100.  Use `SparrowDB.execute()` for reads against current state.
    ///
    /// Throws always until SPA-100 lands.
    #[napi]
    pub fn execute(&self, _cypher: String) -> napi::Result<serde_json::Value> {
        Err(napi::Error::from_reason(
            "ReadTx.execute not yet available; snapshot-pinned query execution is tracked \
             in SPA-100. Use SparrowDB.execute() to query the latest committed state.",
        ))
    }
}

// ── WriteTx ───────────────────────────────────────────────────────────────────

/// Write transaction.  Commit explicitly; dropping without committing rolls
/// back all staged changes.
#[napi]
pub struct WriteTx {
    /// `None` after commit or rollback so use-after-commit is detected.
    inner: Option<::sparrowdb::WriteTx<'static>>,
}

// SAFETY: napi-rs requires Send on all #[napi] types.  WriteTx<'static> wraps a
// MutexGuard whose lock was acquired on the JS main thread.  We uphold safety by
// documenting that WriteTx MUST NOT be transferred to a napi worker thread (e.g.
// via AsyncTask); it is only safe to use from the same thread that opened it.
// napi-rs currently does not guarantee single-threaded access for object methods
// without explicit AsyncTask, so callers must not call beginWrite() inside an
// async napi task.
unsafe impl Send for WriteTx {}

#[napi]
impl WriteTx {
    /// Execute a Cypher mutation statement inside this transaction.
    ///
    /// Returns `{ columns, rows }` (typically empty for write statements).
    #[napi]
    pub fn execute(&mut self, _cypher: String) -> napi::Result<serde_json::Value> {
        match &self.inner {
            Some(_) => {
                // WriteTx doesn't expose a Cypher execute method directly;
                // mutations go through GraphDb.execute in an implicit tx.
                Err(napi::Error::from_reason(
                    "WriteTx.execute not yet available; use SparrowDB.execute for mutations",
                ))
            }
            None => Err(napi::Error::from_reason(
                "transaction already committed or rolled back",
            )),
        }
    }

    /// Commit all staged changes and return the new transaction id.
    ///
    /// The id is returned as a decimal **string** to preserve the full u64
    /// range — JavaScript's `Number` can only represent integers up to 2^53-1.
    ///
    /// Throws if the transaction was already committed or rolled back, or if
    /// a write-write conflict is detected.
    #[napi]
    pub fn commit(&mut self) -> napi::Result<String> {
        match self.inner.take() {
            Some(tx) => {
                let txn_id = tx.commit().map_err(to_napi)?;
                Ok(txn_id.0.to_string())
            }
            None => Err(napi::Error::from_reason(
                "transaction already committed or rolled back",
            )),
        }
    }

    /// Roll back all staged changes explicitly.  Equivalent to dropping the
    /// transaction without committing.
    #[napi]
    pub fn rollback(&mut self) {
        self.inner = None;
    }
}
