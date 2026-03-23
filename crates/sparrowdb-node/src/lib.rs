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

// SAFETY: napi-rs requires `Send` on all `#[napi]` struct types.  `SparrowDB`
// wraps `sparrowdb::GraphDb` which is a newtype over `Arc<DbInner>`.  Every
// field of `DbInner` is `Send`: `PathBuf`, `AtomicU64`, `AtomicBool`,
// `RwLock<VersionStore>`, `RwLock<NodeVersions>`, and `Option<[u8; 32]>`.
// There are no raw pointers or `Rc<T>` fields anywhere in the chain.
// `GraphDb` itself derives `Clone` via `Arc`-clone and is therefore genuinely
// safe to transfer across threads.
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
        // SPA-181: WriteTx no longer carries a lifetime parameter — the write
        // lock is managed internally by an AtomicBool WriteGuard, so no unsafe
        // transmute is required here.
        let tx = self.inner.begin_write().map_err(to_napi)?;
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

// SAFETY: napi-rs requires `Send` on all `#[napi]` struct types.  `ReadTx`
// wraps `sparrowdb::ReadTx` which holds: `snapshot_txn_id: u64`,
// `store: NodeStore` (a `PathBuf` + `HashMap<u32, u64>` — both `Send`), and
// `inner: Arc<DbInner>` (see `SparrowDB` SAFETY note above).  No raw pointers
// or `Rc<T>` are present anywhere in the ownership chain.  A `ReadTx` only
// reads committed state and holds no exclusive resources, so cross-thread
// transfer is safe as long as the caller does not share a single `ReadTx`
// reference across threads simultaneously (i.e. `Send` but not `Sync`).
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
    inner: Option<::sparrowdb::WriteTx>,
}

// SAFETY: napi-rs requires `Send` on all `#[napi]` struct types.  `WriteTx`
// wraps `Option<sparrowdb::WriteTx>`.  `sparrowdb::WriteTx` contains:
//   - `inner: Arc<DbInner>` — `Send` (see `SparrowDB` SAFETY note above)
//   - `store: NodeStore`    — `Send` (`PathBuf` + `HashMap`, no raw pointers)
//   - `catalog: Catalog`    — `Send` (`PathBuf` + `Vec` fields, no raw pointers)
//   - `write_buf: WriteBuffer`           — `Send` (`HashMap` over owned values)
//   - `wal_mutations: Vec<WalMutation>`  — `Send` (owned enum variants)
//   - `dirty_nodes: HashSet<u64>`        — `Send`
//   - `snapshot_txn_id: u64`             — `Send`
//   - `_guard: WriteGuard`               — `Send` (holds `Arc<DbInner>`)
//   - `committed: bool`                  — `Send`
//   - `fulltext_pending: HashMap<String, FulltextIndex>` — `Send` (owned)
//   - `pending_ops: Vec<PendingOp>`      — `Send` (owned enum variants)
// No raw pointers or `Rc<T>` anywhere in the chain.
//
// Usage constraint: a `WriteTx` holds the exclusive database writer lock via
// `WriteGuard` (an `AtomicBool` CAS).  It MUST NOT be passed to an async
// napi `AsyncTask` worker, because doing so would allow multiple threads to
// drive the same write transaction.  Callers must open and use `WriteTx` on
// a single JS thread (the napi main thread or a dedicated worker that owns it
// exclusively).
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
