//! SparrowDB — top-level public API.
//!
//! ## Transaction model (Phase 5 — SWMR)
//!
//! * **Single Writer**: at most one [`WriteTx`] may be active at a time.
//!   [`GraphDb::begin_write`] returns [`Error::WriterBusy`] if a writer is
//!   already open.
//!
//! * **Multiple Readers**: any number of [`ReadTx`] handles may coexist with
//!   an active writer.  Each reader pins the committed `txn_id` at open time
//!   and sees only data committed at or before that snapshot.
//!
//! ## Snapshot isolation for `set_node_col`
//!
//! Property updates (`set_node_col`) are recorded in an **in-memory version
//! chain** keyed by `(NodeId, col_id)`.  Each entry is a `(txn_id, Value)`
//! pair.  When a `ReadTx` reads a property it first consults the version chain
//! and returns the most-recent value with `txn_id <= snapshot_txn_id`, falling
//! back to the on-disk value written by `create_node` if no such entry exists.
//!
//! ## Quick start
//!
//! ```no_run
//! use sparrowdb::GraphDb;
//! let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
//! db.checkpoint().unwrap();
//! db.optimize().unwrap();
//! ```

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::{Error, NodeId, Result, TxnId};
use sparrowdb_execution::{Engine, QueryResult};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::maintenance::MaintenanceEngine;
use sparrowdb_storage::node_store::{NodeStore, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

// ── Version chain ─────────────────────────────────────────────────────────────

/// Key for the version map: (packed NodeId value, column id).
type VersionKey = (u64, u32);

/// One committed version of a property value.
#[derive(Clone)]
struct Version {
    /// The `txn_id` at which this value was committed.
    committed_at: u64,
    value: Value,
}

/// In-memory MVCC version store for property updates.
///
/// A `Vec<Version>` per `(NodeId, col_id)` key, sorted ascending by
/// `committed_at`.  Readers binary-search for the latest version ≤ their
/// snapshot.
#[derive(Default)]
struct VersionStore {
    map: HashMap<VersionKey, Vec<Version>>,
}

impl VersionStore {
    /// Record a committed property update.
    fn insert(&mut self, node_id: NodeId, col_id: u32, committed_at: u64, value: Value) {
        let versions = self.map.entry((node_id.0, col_id)).or_default();
        versions.push(Version {
            committed_at,
            value,
        });
        // Keep sorted — writers are serialized so this is always appended in
        // order, but sort to be safe.
        versions.sort_by_key(|v| v.committed_at);
    }

    /// Return the most-recent value committed at or before `snapshot_txn_id`,
    /// or `None` if no update exists within the snapshot window.
    fn get_at(&self, node_id: NodeId, col_id: u32, snapshot_txn_id: u64) -> Option<Value> {
        let versions = self.map.get(&(node_id.0, col_id))?;
        // Binary search for the rightmost version with committed_at ≤ snapshot.
        let idx = versions.partition_point(|v| v.committed_at <= snapshot_txn_id);
        if idx == 0 {
            None
        } else {
            Some(versions[idx - 1].value.clone())
        }
    }
}

// ── Pending write buffer ──────────────────────────────────────────────────────

/// One staged update: the new value plus the before-image read from disk/
/// version-chain at the time the update was staged.
struct StagedUpdate {
    /// Before-image — the value a reader with `snapshot ≤ prev_txn_id` should
    /// see.  Stored at `prev_txn_id` in the version chain on commit so that
    /// pre-existing readers retain correct snapshot access even after the
    /// on-disk column has been overwritten.
    ///
    /// `None` if the key already had an entry in the version chain (so the
    /// chain already carries the correct before-image).
    before_image: Option<(u64, Value)>, // (txn_id_to_store_at, value)
    /// The new value to commit.
    new_value: Value,
}

/// Staged (uncommitted) property updates for an active `WriteTx`.
///
/// Keyed by `(NodeId, col_id)`; only the latest staged value for each key
/// matters.
#[derive(Default)]
struct WriteBuffer {
    updates: HashMap<VersionKey, StagedUpdate>,
}

// ── Shared inner state ────────────────────────────────────────────────────────

struct DbInner {
    path: PathBuf,
    /// Monotonically increasing; starts at 0 (no writes committed yet).
    /// Incremented atomically after each successful `WriteTx` commit.
    current_txn_id: AtomicU64,
    /// Ensures at most one writer exists at a time.
    write_lock: Mutex<()>,
    /// MVCC version chains for property updates (snapshot-isolation support).
    versions: RwLock<VersionStore>,
}

// ── GraphDb ───────────────────────────────────────────────────────────────────

/// The top-level SparrowDB handle.
///
/// Cheaply cloneable — all clones share the same underlying state.
/// Obtain an instance via [`GraphDb::open`] or the free [`open`] function.
#[derive(Clone)]
pub struct GraphDb {
    inner: Arc<DbInner>,
}

impl GraphDb {
    /// Open (or create) a SparrowDB database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(0),
                write_lock: Mutex::new(()),
                versions: RwLock::new(VersionStore::default()),
            }),
        })
    }

    /// Open a read-only snapshot transaction.
    ///
    /// The returned [`ReadTx`] sees all data committed at or before the
    /// current `txn_id` at the moment of this call.
    pub fn begin_read(&self) -> Result<ReadTx> {
        let snapshot_txn_id = self.inner.current_txn_id.load(Ordering::Acquire);
        let store = NodeStore::open(&self.inner.path)?;
        Ok(ReadTx {
            snapshot_txn_id,
            store,
            inner: Arc::clone(&self.inner),
        })
    }

    /// Open a write transaction.
    ///
    /// Returns [`Error::WriterBusy`] immediately if another [`WriteTx`] is
    /// already active (try-lock semantics — does not block).
    pub fn begin_write(&self) -> Result<WriteTx<'_>> {
        // try_lock: non-blocking — returns Err if already locked.
        let guard: MutexGuard<'static, ()> = match self.inner.write_lock.try_lock() {
            Ok(g) => {
                // SAFETY: We extend the lifetime to 'static only so the guard
                // can be stored inside WriteTx alongside the Arc<DbInner>.
                // The guard is released when WriteTx is dropped, which happens
                // before the Arc (and the Mutex it contains) is destroyed.
                unsafe { std::mem::transmute::<MutexGuard<'_, ()>, MutexGuard<'static, ()>>(g) }
            }
            Err(_) => return Err(Error::WriterBusy),
        };
        let store = NodeStore::open(&self.inner.path)?;
        let catalog = Catalog::open(&self.inner.path)?;
        Ok(WriteTx {
            inner: Arc::clone(&self.inner),
            store,
            catalog,
            write_buf: WriteBuffer::default(),
            _guard: guard,
            committed: false,
        })
    }

    /// Run a CHECKPOINT: fold the delta log into CSR base files, emit WAL
    /// records, and publish the new `wal_checkpoint_lsn` to the metapage.
    ///
    /// Acquires the writer lock for the duration — no concurrent writes.
    pub fn checkpoint(&self) -> Result<()> {
        let _guard = self.inner.write_lock.lock().unwrap();
        let catalog = Catalog::open(&self.inner.path)?;
        let node_store = NodeStore::open(&self.inner.path)?;
        let (rel_table_ids, n_nodes) =
            collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.checkpoint(&rel_table_ids, n_nodes)
    }

    /// Run an OPTIMIZE: same as CHECKPOINT but additionally sorts each source
    /// node's neighbor list by `(dst_node_id)` ascending.
    ///
    /// Acquires the writer lock for the duration — no concurrent writes.
    pub fn optimize(&self) -> Result<()> {
        let _guard = self.inner.write_lock.lock().unwrap();
        let catalog = Catalog::open(&self.inner.path)?;
        let node_store = NodeStore::open(&self.inner.path)?;
        let (rel_table_ids, n_nodes) =
            collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.optimize(&rel_table_ids, n_nodes)
    }

    /// Execute a read-only Cypher query and return the result.
    pub fn execute(&self, cypher: &str) -> Result<QueryResult> {
        let csr = open_csr_forward(&self.inner.path);
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            Catalog::open(&self.inner.path)?,
            csr,
            &self.inner.path,
        );
        engine.execute(cypher)
    }
}

/// Convenience wrapper — equivalent to [`GraphDb::open`].
pub fn open(path: &Path) -> Result<GraphDb> {
    GraphDb::open(path)
}

// ── Legacy alias ──────────────────────────────────────────────────────────────

/// Legacy alias kept for backward compatibility with Phase 0 tests.
pub type SparrowDB = GraphDb;

// ── ReadTx ────────────────────────────────────────────────────────────────────

/// A read-only snapshot transaction.
///
/// Pinned at the `txn_id` current when this handle was opened; immune to
/// subsequent writer commits for the lifetime of this handle.
pub struct ReadTx {
    /// The committed `txn_id` this reader is pinned to.
    pub snapshot_txn_id: u64,
    store: NodeStore,
    inner: Arc<DbInner>,
}

impl ReadTx {
    /// Read the `Int64` property values of a node at the pinned snapshot.
    ///
    /// For each column the version chain is consulted first; if a value was
    /// committed at or before `snapshot_txn_id` it shadows the on-disk value.
    pub fn get_node(&self, node_id: NodeId, col_ids: &[u32]) -> Result<Vec<(u32, Value)>> {
        let versions = self.inner.versions.read().expect("version lock poisoned");
        let raw = self.store.get_node_raw(node_id, col_ids)?;
        let result = raw
            .into_iter()
            .map(|(col_id, raw_val)| {
                // Check version chain first.
                if let Some(v) = versions.get_at(node_id, col_id, self.snapshot_txn_id) {
                    (col_id, v)
                } else {
                    (col_id, Value::int64_from_u64(raw_val))
                }
            })
            .collect();
        Ok(result)
    }

    /// Return the snapshot `TxnId` this reader is pinned to.
    pub fn snapshot(&self) -> TxnId {
        TxnId(self.snapshot_txn_id)
    }
}

// ── WriteTx ───────────────────────────────────────────────────────────────────

/// A write transaction.
///
/// Only one may be active at a time (writer-lock held for the lifetime of
/// this struct).  Commit by calling [`WriteTx::commit`]; uncommitted changes
/// are discarded on drop.
pub struct WriteTx<'db> {
    inner: Arc<DbInner>,
    store: NodeStore,
    catalog: Catalog,
    /// Staged property updates (not yet visible to readers).
    write_buf: WriteBuffer,
    /// Held for the lifetime of this WriteTx; released on drop.
    _guard: MutexGuard<'db, ()>,
    committed: bool,
}

impl<'db> WriteTx<'db> {
    /// Create a new node under `label_id` with the given properties.
    ///
    /// Returns the packed [`NodeId`].
    pub fn create_node(&mut self, label_id: u32, props: &[(u32, Value)]) -> Result<NodeId> {
        self.store.create_node(label_id, props)
    }

    /// Stage a property update.
    ///
    /// The new value is not visible to readers until [`commit`] is called.
    ///
    /// On the first update to a `(node_id, col_id)` key during this
    /// transaction, the current on-disk value is read and stored as the
    /// before-image so that readers with older snapshots continue to see the
    /// correct value after commit overwrites the column file.
    pub fn set_node_col(&mut self, node_id: NodeId, col_id: u32, value: Value) {
        let key = (node_id.0, col_id);
        if self.write_buf.updates.contains_key(&key) {
            // Already staged this key — just update the new_value.
            self.write_buf.updates.get_mut(&key).unwrap().new_value = value;
            return;
        }

        // First update to this key in this transaction.  Capture the
        // before-image so readers pinned before our commit retain access.
        let prev_txn_id = self.inner.current_txn_id.load(Ordering::Acquire);

        // Check whether the version chain already has an entry for this key.
        // If so, the chain is already correct and we don't need to add the
        // before-image separately.
        let already_in_chain = {
            let vs = self.inner.versions.read().expect("version lock");
            vs.map.contains_key(&key)
        };

        let before_image = if already_in_chain {
            None
        } else {
            // Read the current on-disk value as the before-image.
            let disk_val = self
                .store
                .get_node_raw(node_id, &[col_id])
                .ok()
                .and_then(|mut v| v.pop())
                .map(|(_, raw)| Value::int64_from_u64(raw));
            disk_val.map(|v| (prev_txn_id, v))
        };

        self.write_buf.updates.insert(
            key,
            StagedUpdate {
                before_image,
                new_value: value,
            },
        );
    }

    /// Create a label in the schema catalog.
    pub fn create_label(&mut self, name: &str) -> Result<u16> {
        self.catalog.create_label(name)
    }

    /// Commit the transaction.
    ///
    /// 1. Flushes all staged `set_node_col` updates to disk.
    /// 2. Records before-images (if any) in the version chain at the previous
    ///    `txn_id`, preserving snapshot access for currently-open readers.
    /// 3. Atomically increments the global `current_txn_id`.
    /// 4. Records the new values in the version chain at the new `txn_id`.
    pub fn commit(mut self) -> Result<TxnId> {
        // Drain staged updates.
        let updates: Vec<((u64, u32), StagedUpdate)> = self.write_buf.updates.drain().collect();

        // 1. Flush updates to disk.
        for ((node_raw, col_id), ref staged) in &updates {
            self.store
                .set_node_col(NodeId(*node_raw), *col_id, &staged.new_value)?;
        }

        // 2 & 3. Increment txn_id with Release ordering.
        let new_id = self.inner.current_txn_id.fetch_add(1, Ordering::Release) + 1;

        // 4. Publish versions.
        {
            let mut vs = self.inner.versions.write().expect("version lock poisoned");
            for ((node_raw, col_id), staged) in updates {
                // Publish before-image at the previous txn_id so that
                // readers pinned at that snapshot (or earlier) continue to
                // see the correct value even though the disk file is updated.
                if let Some((prev_txn_id, before_val)) = staged.before_image {
                    vs.insert(NodeId(node_raw), col_id, prev_txn_id, before_val);
                }
                // Publish new value at the current txn_id.
                vs.insert(NodeId(node_raw), col_id, new_id, staged.new_value);
            }
        }

        self.committed = true;
        Ok(TxnId(new_id))
    }
}

impl<'db> Drop for WriteTx<'db> {
    fn drop(&mut self) {
        // Uncommitted staged updates are simply discarded here.
        // On-disk data is unchanged (set_node_col writes happen only in commit).
        let _ = self.committed;
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn collect_maintenance_params(
    catalog: &Catalog,
    node_store: &NodeStore,
    db_root: &Path,
) -> (Vec<u32>, u64) {
    use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};

    let rel_table_ids = vec![0u32]; // default table; extend for multi-rel-table support

    // n_nodes must cover the highest node-store HWM AND the highest node ID
    // present in any delta record, so that the CSR bounds check passes even
    // when edges were inserted without going through the node-store API.
    let hwm_n_nodes: u64 = catalog
        .list_labels()
        .unwrap_or_default()
        .iter()
        .map(|(label_id, _name)| node_store.hwm_for_label(*label_id as u32).unwrap_or(0))
        .sum::<u64>();

    // Also scan delta records for the maximum node ID used.
    let delta_max: u64 = rel_table_ids
        .iter()
        .filter_map(|&rel_id| {
            EdgeStore::open(db_root, RelTableId(rel_id))
                .ok()
                .and_then(|s| s.read_delta().ok())
        })
        .flat_map(|records| {
            records
                .into_iter()
                .flat_map(|r| [r.src.0, r.dst.0].into_iter())
        })
        .max()
        .map(|max_id| max_id + 1)
        .unwrap_or(0);

    let n_nodes = hwm_n_nodes.max(delta_max).max(1);

    (rel_table_ids, n_nodes)
}

fn open_csr_forward(path: &Path) -> CsrForward {
    use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
    // Try to open the CSR forward file; fall back to an empty CSR.
    match EdgeStore::open(path, RelTableId(0)).and_then(|s| s.open_fwd()) {
        Ok(csr) => csr,
        Err(_) => CsrForward::build(0, &[]),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Phase 0 gate: create a DB directory, open it, and let it drop cleanly.
    #[test]
    fn open_and_close_empty_db() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.sparrow");

        assert!(!db_path.exists());
        let db = GraphDb::open(&db_path).expect("open must succeed");
        assert!(db_path.is_dir());
        drop(db);
    }

    #[test]
    fn open_existing_dir_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("existing.sparrow");
        std::fs::create_dir_all(&db_path).unwrap();
        let db = GraphDb::open(&db_path).expect("open existing must succeed");
        drop(db);
    }

    #[test]
    fn open_fn_signature() {
        let _: fn(&Path) -> Result<GraphDb> = open;
    }

    #[test]
    fn begin_read_returns_snapshot_zero_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        let rx = db.begin_read().unwrap();
        assert_eq!(rx.snapshot_txn_id, 0);
    }

    #[test]
    fn begin_write_increments_txn_id_on_commit() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        let tx = db.begin_write().unwrap();
        let committed = tx.commit().unwrap();
        assert_eq!(committed.0, 1);

        let tx2 = db.begin_write().unwrap();
        let committed2 = tx2.commit().unwrap();
        assert_eq!(committed2.0, 2);
    }

    #[test]
    fn second_begin_write_returns_writer_busy() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        let _tx1 = db.begin_write().unwrap();
        let err = db.begin_write().err().expect("expected Err");
        assert!(
            matches!(err, Error::WriterBusy),
            "expected WriterBusy, got {err}"
        );
    }

    #[test]
    fn write_lock_released_after_commit() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        let tx = db.begin_write().unwrap();
        tx.commit().unwrap();

        // Should succeed because the lock was released on commit/drop.
        let tx2 = db.begin_write().unwrap();
        tx2.commit().unwrap();
    }

    #[test]
    fn graphdb_checkpoint_on_empty_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        // Checkpoint on empty DB must not panic or error.
        db.checkpoint().expect("checkpoint must succeed on empty DB");
    }

    #[test]
    fn graphdb_optimize_on_empty_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        db.optimize().expect("optimize must succeed on empty DB");
    }
}
