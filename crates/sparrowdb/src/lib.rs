// Suppress rustdoc warnings for Markdown links that are valid on GitHub/docs.rs
// but cannot be resolved as Rust item paths (e.g., [LICENSE], [docs/...]).
#![allow(rustdoc::bare_urls)]
#![allow(rustdoc::broken_intra_doc_links)]
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
use sparrowdb_common::{col_id_of, TxnId};
use sparrowdb_execution::Engine;

// ── Public re-exports ─────────────────────────────────────────────────────────
//
// Re-export the types that consumers of the top-level `sparrowdb` crate need
// without also having to depend on the sub-crates directly.

/// Query result returned by [`GraphDb::execute`] and [`GraphDb::execute_write`].
/// Contains column names and rows of scalar [`Value`] cells.
pub use sparrowdb_execution::QueryResult;

/// Scalar value type used in query results and node property reads/writes.
pub use sparrowdb_storage::node_store::Value;

/// Opaque 64-bit node identifier.
pub use sparrowdb_common::NodeId;

/// Opaque 64-bit edge identifier.
pub use sparrowdb_common::EdgeId;

/// Error type returned by all SparrowDB operations.
pub use sparrowdb_common::Error;

/// Convenience alias: `std::result::Result<T, sparrowdb::Error>`.
pub use sparrowdb_common::Result;

// ── DbStats ───────────────────────────────────────────────────────────────────

/// Storage-size snapshot returned by [`GraphDb::stats`] (SPA-171).
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    /// Total bytes on disk across all database files.
    pub total_bytes: u64,
    /// Bytes consumed by node column files for each label.
    pub bytes_per_label: std::collections::HashMap<String, u64>,
    /// High-water mark (HWM) per label — the highest slot index allocated for
    /// that label, plus one.  Soft-deleted nodes whose slots have not yet been
    /// reclaimed by compaction are still included in this count.  The value
    /// converges to the true live-node count once compaction / GC runs.
    pub node_count_per_label: std::collections::HashMap<String, u64>,
    /// Total edges across all relationship types (delta log + CSR).
    pub edge_count: u64,
    /// Bytes used by all WAL segment files under `wal/`.
    pub wal_bytes: u64,
}

use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::maintenance::MaintenanceEngine;
use sparrowdb_storage::node_store::NodeStore;
use sparrowdb_storage::wal::codec::{WalPayload, WalRecordKind};
use sparrowdb_storage::wal::writer::WalWriter;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::info_span;

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
    /// Human-readable property key name used for WAL emission.
    /// For columns staged by col_id directly (e.g. from create_node), this is
    /// the synthesized form `"col_{col_id}"`.
    key_name: String,
}

/// Staged (uncommitted) property updates for an active `WriteTx`.
///
/// Keyed by `(NodeId, col_id)`; only the latest staged value for each key
/// matters.
#[derive(Default)]
struct WriteBuffer {
    updates: HashMap<VersionKey, StagedUpdate>,
}

// ── WAL mutation log entries ──────────────────────────────────────────────────

/// A mutation staged in a `WriteTx` for WAL emission at commit time.
enum WalMutation {
    /// A node was created (SPA-123, SPA-127).
    NodeCreate {
        node_id: NodeId,
        label_id: u32,
        /// Property data as `(col_id, value)` pairs used for on-disk writes.
        props: Vec<(u32, Value)>,
        /// Human-readable property names parallel to `props`.
        ///
        /// When property names are available (e.g. from a Cypher literal) they
        /// are recorded here so the WAL can store them for schema introspection
        /// (`CALL db.schema()`).  Empty when names are not known (e.g. low-level
        /// `create_node` calls that only have col_ids).
        prop_names: Vec<String>,
    },
    /// A node was deleted (SPA-125, SPA-127).
    NodeDelete { node_id: NodeId },
    /// An edge was created (SPA-126, SPA-127).
    EdgeCreate {
        edge_id: EdgeId,
        src: NodeId,
        dst: NodeId,
        rel_type: String,
    },
}

// ── Pending structural operations (SPA-181 buffering) ────────────────────────

/// A buffered structural mutation that will be applied to storage on commit.
///
/// Property updates (`set_node_col`) already go through [`WriteBuffer`] and
/// are flushed by the existing commit machinery.  These `PendingOp` entries
/// cover the remaining mutations that previously wrote directly to disk
/// before commit.
///
/// Note: catalog schema changes (`create_label`, `get_or_create_rel_type_id`)
/// are still written immediately because labels are idempotent metadata.
/// Full schema-change atomicity is deferred to a future phase.
enum PendingOp {
    /// Create a node: write to the node-store at the pre-reserved `slot` and
    /// advance the on-disk HWM.
    NodeCreate {
        label_id: u32,
        slot: u32,
        props: Vec<(u32, Value)>,
    },
    /// Delete a node: tombstone col_0 with `u64::MAX`.
    NodeDelete { node_id: NodeId },
    /// Create an edge: append to the edge delta log.
    EdgeCreate {
        src: NodeId,
        dst: NodeId,
        rel_table_id: RelTableId,
    },
}

// ── Node version tracker (for MVCC conflict detection, SPA-128) ───────────────

/// Tracks the `txn_id` of the last committed write to each node.
#[derive(Default)]
struct NodeVersions {
    /// `node_id.0 → last_committed_txn_id`.
    map: HashMap<u64, u64>,
}

impl NodeVersions {
    fn set(&mut self, node_id: NodeId, txn_id: u64) {
        self.map.insert(node_id.0, txn_id);
    }

    fn get(&self, node_id: NodeId) -> u64 {
        self.map.get(&node_id.0).copied().unwrap_or(0)
    }
}

// ── Shared inner state ────────────────────────────────────────────────────────

struct DbInner {
    path: PathBuf,
    /// Monotonically increasing; starts at 0 (no writes committed yet).
    /// Incremented atomically after each successful `WriteTx` commit.
    current_txn_id: AtomicU64,
    /// Ensures at most one writer exists at a time.
    ///
    /// `false` = no active writer; `true` = writer active.
    /// Use `compare_exchange(false, true)` for try-lock semantics.
    write_locked: AtomicBool,
    /// MVCC version chains for property updates (snapshot-isolation support).
    versions: RwLock<VersionStore>,
    /// Per-node last-committed txn_id (write-write conflict detection, SPA-128).
    node_versions: RwLock<NodeVersions>,
    /// Optional 32-byte encryption key (SPA-98).  When `Some`, WAL payloads
    /// are encrypted with XChaCha20-Poly1305 via [`WalWriter::open_encrypted`].
    encryption_key: Option<[u8; 32]>,
    unique_constraints: RwLock<HashSet<(u32, u32)>>,
    /// Shared property-index cache (SPA-187).
    ///
    /// Read queries clone from this at `Engine::new_with_cached_index` time and
    /// write their lazily-populated index back after execution.  Write
    /// transactions invalidate it via `clear()` on commit so the next read
    /// re-populates from the updated column files.
    prop_index: RwLock<sparrowdb_storage::property_index::PropertyIndex>,
    /// Shared catalog cache (SPA-188).
    ///
    /// Read queries clone from this to avoid re-parsing the TLV catalog file
    /// on every query.  DDL operations (label/rel-type creation, constraints)
    /// refresh the cache via `invalidate_catalog()`.
    catalog: RwLock<Catalog>,
    /// Cached per-type CSR forward map (SPA-189).
    ///
    /// Populated at `GraphDb::open` time and cloned into each `Engine`
    /// instance.  Invalidated after CHECKPOINT / OPTIMIZE since those compact
    /// the delta log into new CSR base files.
    csr_map: RwLock<HashMap<u32, CsrForward>>,
}

// ── Write-lock guard (SPA-181: replaces 'static transmute UB) ────────────────

/// RAII guard that holds the exclusive writer lock for a [`DbInner`].
///
/// Acquired by [`GraphDb::begin_write`] via an atomic compare-exchange
/// (`false → true`).  Released on `Drop` by resetting the flag to `false`.
/// Because the guard holds an [`Arc<DbInner>`] the `DbInner` (and the
/// `AtomicBool` it contains) is guaranteed to outlive the guard — no unsafe
/// lifetime extension needed.
struct WriteGuard {
    inner: Arc<DbInner>,
}

impl WriteGuard {
    /// Try to acquire the write lock.  Returns `None` if already held.
    fn try_acquire(inner: &Arc<DbInner>) -> Option<Self> {
        inner
            .write_locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| WriteGuard {
                inner: Arc::clone(inner),
            })
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Release the lock.
        self.inner.write_locked.store(false, Ordering::Release);
    }
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
    /// Return the filesystem path of this database.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Open (or create) a SparrowDB database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let catalog = Catalog::open(path)?;
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(0),
                write_locked: AtomicBool::new(false),
                versions: RwLock::new(VersionStore::default()),
                node_versions: RwLock::new(NodeVersions::default()),
                encryption_key: None,
                unique_constraints: RwLock::new(HashSet::new()),
                prop_index: RwLock::new(sparrowdb_storage::property_index::PropertyIndex::new()),
                catalog: RwLock::new(catalog),
                csr_map: RwLock::new(open_csr_map(path)),
            }),
        })
    }

    /// Open (or create) an encrypted SparrowDB database at `path` (SPA-98).
    ///
    /// WAL payloads are encrypted with XChaCha20-Poly1305 using `key`.
    /// The same 32-byte key must be supplied on every subsequent open of this
    /// database — opening with a wrong or missing key will cause WAL replay to
    /// fail with [`Error::EncryptionAuthFailed`].
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created.
    pub fn open_encrypted(path: &Path, key: [u8; 32]) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let catalog = Catalog::open(path)?;
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(0),
                write_locked: AtomicBool::new(false),
                versions: RwLock::new(VersionStore::default()),
                node_versions: RwLock::new(NodeVersions::default()),
                encryption_key: Some(key),
                unique_constraints: RwLock::new(HashSet::new()),
                prop_index: RwLock::new(sparrowdb_storage::property_index::PropertyIndex::new()),
                catalog: RwLock::new(catalog),
                csr_map: RwLock::new(open_csr_map(path)),
            }),
        })
    }

    /// Invalidate the shared property-index cache (SPA-187).
    ///
    /// Called after every write-transaction commit so the next read query
    /// re-populates from the updated column files on disk.
    fn invalidate_prop_index(&self) {
        self.inner
            .prop_index
            .write()
            .expect("prop_index RwLock poisoned")
            .clear();
    }

    /// Refresh the shared catalog cache from disk (SPA-188).
    ///
    /// Called after DDL operations (label/rel-type creation, constraints) so
    /// the next read query sees the updated schema without re-parsing the TLV
    /// file itself.
    fn invalidate_catalog(&self) {
        if let Ok(fresh) = Catalog::open(&self.inner.path) {
            *self.inner.catalog.write().expect("catalog RwLock poisoned") = fresh;
        }
    }

    /// Clone the cached catalog for use in a single query (SPA-188).
    fn catalog_snapshot(&self) -> Catalog {
        self.inner
            .catalog
            .read()
            .expect("catalog RwLock poisoned")
            .clone()
    }

    /// Refresh the cached CSR forward map from disk (SPA-189).
    ///
    /// Called after CHECKPOINT / OPTIMIZE since those compact the delta log
    /// into new CSR base files.  Regular writes do NOT need this because
    /// the delta log is read separately by the engine.
    ///
    /// Only replaces the cache when `open_csr_map` succeeds in loading the
    /// catalog; a failed reload (e.g. transient I/O error) leaves the
    /// existing in-memory map intact rather than replacing it with an empty
    /// map that would cause subsequent queries to miss all checkpointed edges.
    fn invalidate_csr_map(&self) {
        if let Ok(fresh) = try_open_csr_map(&self.inner.path) {
            *self.inner.csr_map.write().expect("csr_map RwLock poisoned") = fresh;
        }
    }

    /// Clone the cached CSR map for use in a single query (SPA-189).
    fn cached_csr_map(&self) -> HashMap<u32, CsrForward> {
        self.inner
            .csr_map
            .read()
            .expect("csr_map RwLock poisoned")
            .clone()
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
    pub fn begin_write(&self) -> Result<WriteTx> {
        // Atomically acquire the write lock (false → true).
        // No unsafe transmute — WriteGuard holds Arc<DbInner> and resets
        // the AtomicBool on Drop, so the lock flag always outlives the guard.
        let guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
        let snapshot_txn_id = self.inner.current_txn_id.load(Ordering::Acquire);
        let store = NodeStore::open(&self.inner.path)?;
        // WriteTx opens a fresh catalog from disk because it may create labels
        // or rel types, and needs the latest on-disk state to avoid ID conflicts.
        let catalog = Catalog::open(&self.inner.path)?;
        Ok(WriteTx {
            inner: Arc::clone(&self.inner),
            store,
            catalog,
            write_buf: WriteBuffer::default(),
            wal_mutations: Vec::new(),
            dirty_nodes: HashSet::new(),
            snapshot_txn_id,
            _guard: guard,
            committed: false,
            fulltext_pending: HashMap::new(),
            pending_ops: Vec::new(),
        })
    }

    /// Run a CHECKPOINT: fold the delta log into CSR base files, emit WAL
    /// records, and publish the new `wal_checkpoint_lsn` to the metapage.
    ///
    /// Acquires the writer lock for the duration — no concurrent writes.
    ///
    /// Returns [`Error::WriterBusy`] immediately if a [`WriteTx`] is currently
    /// active, rather than blocking indefinitely.  This prevents deadlocks on
    /// single-threaded callers and avoids unbounded waits on multi-threaded ones.
    pub fn checkpoint(&self) -> Result<()> {
        let _guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
        let catalog = self.catalog_snapshot();
        let node_store = NodeStore::open(&self.inner.path)?;
        let (rel_table_ids, n_nodes) =
            collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.checkpoint(&rel_table_ids, n_nodes)?;
        self.invalidate_csr_map();
        Ok(())
    }

    /// Run an OPTIMIZE: same as CHECKPOINT but additionally sorts each source
    /// node's neighbor list by `(dst_node_id)` ascending.
    ///
    /// Acquires the writer lock for the duration — no concurrent writes.
    ///
    /// Returns [`Error::WriterBusy`] immediately if a [`WriteTx`] is currently
    /// active, rather than blocking indefinitely.  This prevents deadlocks on
    /// single-threaded callers and avoids unbounded waits on multi-threaded ones.
    pub fn optimize(&self) -> Result<()> {
        let _guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
        let catalog = self.catalog_snapshot();
        let node_store = NodeStore::open(&self.inner.path)?;
        let (rel_table_ids, n_nodes) =
            collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.optimize(&rel_table_ids, n_nodes)?;
        self.invalidate_csr_map();
        Ok(())
    }

    /// Return a storage-size snapshot for this database (SPA-171).
    ///
    /// Pure read — no locks acquired, no writes performed.
    ///
    /// # Best-effort semantics
    ///
    /// Filesystem entries that cannot be read (missing directories, permission
    /// errors, partially corrupt files) are silently skipped.  The returned
    /// snapshot may therefore undercount bytes or edges if the database
    /// directory is in an inconsistent state.  Callers that need exact
    /// accounting should treat the reported values as a lower bound.
    pub fn stats(&self) -> Result<DbStats> {
        let db_root = &self.inner.path;
        let catalog = self.catalog_snapshot();
        let node_store = NodeStore::open(db_root)?;
        let mut stats = DbStats::default();

        for (label_id, label_name) in catalog.list_labels()? {
            let lid = label_id as u32;
            stats.node_count_per_label.insert(
                label_name.clone(),
                node_store.hwm_for_label(lid).unwrap_or(0),
            );
            let mut lb: u64 = 0;
            if let Ok(es) = std::fs::read_dir(db_root.join("nodes").join(lid.to_string())) {
                for e in es.flatten() {
                    if let Ok(m) = e.metadata() {
                        lb += m.len();
                    }
                }
            }
            stats.bytes_per_label.insert(label_name, lb);
        }

        if let Ok(es) = std::fs::read_dir(db_root.join("wal")) {
            for e in es.flatten() {
                let n = e.file_name();
                let ns = n.to_string_lossy();
                if ns.starts_with("segment-") && ns.ends_with(".wal") {
                    if let Ok(m) = e.metadata() {
                        stats.wal_bytes += m.len();
                    }
                }
            }
        }

        const DR: u64 = 20; // DeltaRecord: src(8) + dst(8) + rel_id(4) = 20 bytes
        if let Ok(ts) = std::fs::read_dir(db_root.join("edges")) {
            for t in ts.flatten() {
                if !t.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }
                let rd = t.path();
                if let Ok(m) = std::fs::metadata(rd.join("delta.log")) {
                    stats.edge_count += m.len().checked_div(DR).unwrap_or(0);
                }
                let fp = rd.join("base.fwd.csr");
                if fp.exists() {
                    if let Ok(b) = std::fs::read(&fp) {
                        if let Ok(csr) = CsrForward::decode(&b) {
                            stats.edge_count += csr.n_edges();
                        }
                    }
                }
            }
        }

        stats.total_bytes = dir_size_bytes(db_root);
        Ok(stats)
    }

    /// Execute a Cypher query.
    ///
    /// Read-only statements (`MATCH … RETURN`) are executed against a
    /// point-in-time snapshot.  Mutation statements (`MERGE`, `MATCH … SET`,
    /// `MATCH … DELETE`) open a write transaction internally and commit on
    /// success.  `CREATE` statements auto-register labels and write nodes (SPA-156).
    /// `CHECKPOINT` and `OPTIMIZE` delegate to the maintenance engine (SPA-189).
    pub fn execute(&self, cypher: &str) -> Result<QueryResult> {
        use sparrowdb_cypher::ast::Statement;
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;
        let catalog_snap = self.catalog_snapshot();
        let bound = bind(stmt, &catalog_snap)?;

        // SPA-189: wire CHECKPOINT and OPTIMIZE to their real implementations
        // before entering the mutation / read-only dispatch below.
        match &bound.inner {
            Statement::Checkpoint => {
                self.checkpoint()?;
                return Ok(QueryResult::empty(vec![]));
            }
            Statement::Optimize => {
                self.optimize()?;
                return Ok(QueryResult::empty(vec![]));
            }
            Statement::CreateConstraint { label, property } => {
                return self.register_unique_constraint(label, property);
            }
            _ => {}
        }

        if Engine::is_mutation(&bound.inner) {
            match bound.inner {
                Statement::Merge(ref m) => self.execute_merge(m),
                Statement::MatchMergeRel(ref mm) => self.execute_match_merge_rel(mm),
                Statement::MatchMutate(ref mm) => self.execute_match_mutate(mm),
                Statement::MatchCreate(ref mc) => self.execute_match_create(mc),
                // Standalone CREATE with edges — must go through WriteTx so
                // create_edge can register the rel type and write the WAL.
                Statement::Create(ref c) => self.execute_create_standalone(c),
                _ => unreachable!(),
            }
        } else {
            let _span = info_span!("sparrowdb.query").entered();

            let mut engine = {
                let _open_span = info_span!("sparrowdb.open_engine").entered();
                let csrs = self.cached_csr_map();
                Engine::new_with_cached_index(
                    NodeStore::open(&self.inner.path)?,
                    catalog_snap,
                    csrs,
                    &self.inner.path,
                    Some(&self.inner.prop_index),
                )
            };

            let result = {
                let _exec_span = info_span!("sparrowdb.execute").entered();
                engine.execute_statement(bound.inner)?
            };

            // Write lazily-loaded columns back to the shared cache.
            engine.write_back_prop_index(&self.inner.prop_index);

            tracing::debug!(rows = result.rows.len(), "query complete");
            Ok(result)
        }
    }

    /// Execute a Cypher query with a per-query timeout (SPA-254).
    ///
    /// Identical to [`execute`](Self::execute) but sets a deadline of
    /// `Instant::now() + timeout` before dispatching to the engine.  The
    /// engine checks the deadline at the top of every hot scan / traversal
    /// loop.  If the deadline passes before the query completes,
    /// [`Error::QueryTimeout`] is returned.
    ///
    /// `execute_with_timeout` only supports read-only (`MATCH … RETURN`)
    /// statements today; mutation statements are forwarded to the existing
    /// write-transaction code path **without** a timeout (they typically
    /// complete in O(1) writes and are not the source of runaway queries).
    ///
    /// # Example
    /// ```no_run
    /// use sparrowdb::GraphDb;
    /// use std::time::Duration;
    ///
    /// let db = GraphDb::open(std::path::Path::new("/tmp/g.sparrow")).unwrap();
    /// let result = db.execute_with_timeout(
    ///     "MATCH (n:Person) RETURN n.name",
    ///     Duration::from_secs(5),
    /// );
    /// match result {
    ///     Ok(qr) => println!("{} rows", qr.rows.len()),
    ///     Err(sparrowdb::Error::QueryTimeout) => eprintln!("query timed out"),
    ///     Err(e) => eprintln!("error: {e}"),
    /// }
    /// ```
    pub fn execute_with_timeout(
        &self,
        cypher: &str,
        timeout: std::time::Duration,
    ) -> Result<QueryResult> {
        use sparrowdb_cypher::ast::Statement;
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;
        let catalog_snap = self.catalog_snapshot();
        let bound = bind(stmt, &catalog_snap)?;

        // Delegate CHECKPOINT/OPTIMIZE immediately — no timeout needed.
        match &bound.inner {
            Statement::Checkpoint => {
                self.checkpoint()?;
                return Ok(QueryResult::empty(vec![]));
            }
            Statement::Optimize => {
                self.optimize()?;
                return Ok(QueryResult::empty(vec![]));
            }
            Statement::CreateConstraint { label, property } => {
                return self.register_unique_constraint(label, property);
            }
            _ => {}
        }

        if Engine::is_mutation(&bound.inner) {
            // Mutation statements go through the standard write-transaction
            // path; they are not the source of runaway queries.
            return match bound.inner {
                Statement::Merge(ref m) => self.execute_merge(m),
                Statement::MatchMutate(ref mm) => self.execute_match_mutate(mm),
                Statement::MatchCreate(ref mc) => self.execute_match_create(mc),
                Statement::Create(ref c) => self.execute_create_standalone(c),
                _ => unreachable!(),
            };
        }

        let _span = info_span!("sparrowdb.query_with_timeout").entered();
        let deadline = std::time::Instant::now() + timeout;

        let mut engine = {
            let _open_span = info_span!("sparrowdb.open_engine").entered();
            let csrs = self.cached_csr_map();
            Engine::new_with_cached_index(
                NodeStore::open(&self.inner.path)?,
                catalog_snap,
                csrs,
                &self.inner.path,
                Some(&self.inner.prop_index),
            )
            .with_deadline(deadline)
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.execute").entered();
            engine.execute_statement(bound.inner)?
        };

        engine.write_back_prop_index(&self.inner.prop_index);

        tracing::debug!(rows = result.rows.len(), "query_with_timeout complete");
        Ok(result)
    }

    /// Internal: execute a standalone `CREATE (a)-[:R]->(b)` statement.
    ///
    /// Creates all declared nodes, then for each edge in the pattern looks up
    /// the freshly-created node IDs by variable name and calls
    /// `WriteTx::create_edge`.  This is needed when the CREATE clause contains
    /// relationship patterns, because edge creation must be routed through a
    /// write transaction so the relationship type is registered in the catalog
    /// and the edge is appended to the WAL (SPA-182).
    fn register_unique_constraint(&self, label: &str, property: &str) -> Result<QueryResult> {
        // SPA-234: auto-create label if absent so the constraint is registered
        // even before any nodes of this label exist.  Subsequent CREATE
        // statements look up the label_id from the persisted catalog and compare
        // against the same unique_constraints set.
        //
        // Acquire the writer lock so that catalog mutations here cannot race
        // with an active WriteTx that also holds an open Catalog handle.
        // Both paths derive next_label_id from their own in-memory counter, so
        // concurrent catalog writes without this guard can assign duplicate IDs.
        let _guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
        let mut catalog = sparrowdb_catalog::catalog::Catalog::open(&self.inner.path)?;
        let label_id: u32 = match catalog.get_label(label)? {
            Some(id) => id as u32,
            None => {
                let id = catalog.create_label(label)? as u32;
                self.invalidate_catalog();
                id
            }
        };
        let col_id = sparrowdb_common::col_id_of(property);
        self.inner
            .unique_constraints
            .write()
            .unwrap()
            .insert((label_id, col_id));
        Ok(QueryResult::empty(vec![]))
    }

    fn execute_create_standalone(
        &self,
        create: &sparrowdb_cypher::ast::CreateStatement,
    ) -> Result<QueryResult> {
        // Pre-flight: verify that every variable referenced by an edge is
        // declared as a named node in this CREATE clause.  Doing this before
        // opening the write transaction ensures that a malformed CREATE fails
        // cleanly without leaving orphaned nodes on disk.
        let declared_vars: HashSet<&str> = create
            .nodes
            .iter()
            .filter(|n| !n.var.is_empty())
            .map(|n| n.var.as_str())
            .collect();

        for (left_var, _, right_var) in &create.edges {
            if !left_var.is_empty() && !declared_vars.contains(left_var.as_str()) {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "CREATE edge references undeclared variable '{left_var}'"
                )));
            }
            if !right_var.is_empty() && !declared_vars.contains(right_var.as_str()) {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "CREATE edge references undeclared variable '{right_var}'"
                )));
            }
        }

        // SPA-208: reject reserved __SO_ label prefix on nodes.
        for node in &create.nodes {
            if let Some(label) = node.labels.first() {
                if is_reserved_label(label) {
                    return Err(reserved_label_error(label));
                }
            }
        }

        // SPA-208: reject reserved __SO_ rel-type prefix on edges.
        for (_, rel_pat, _) in &create.edges {
            if is_reserved_label(&rel_pat.rel_type) {
                return Err(reserved_label_error(&rel_pat.rel_type));
            }
        }

        let mut tx = self.begin_write()?;

        // Map variable name → NodeId for all newly created nodes.
        let mut var_to_node: HashMap<String, NodeId> = HashMap::new();

        for node in &create.nodes {
            let label = node.labels.first().cloned().unwrap_or_default();
            let label_id: u32 = match tx.catalog.get_label(&label)? {
                Some(id) => id as u32,
                None => tx.catalog.create_label(&label)? as u32,
            };

            let named_props: Vec<(String, Value)> = node
                .props
                .iter()
                .map(|entry| {
                    let val = match &entry.value {
                        sparrowdb_cypher::ast::Expr::Literal(
                            sparrowdb_cypher::ast::Literal::Null,
                        ) => Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE property '{}' is null; use a concrete value",
                            entry.key
                        ))),
                        sparrowdb_cypher::ast::Expr::Literal(
                            sparrowdb_cypher::ast::Literal::Param(p),
                        ) => Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE property '{}' references parameter ${p}; runtime parameters are not yet supported in standalone CREATE",
                            entry.key
                        ))),
                        sparrowdb_cypher::ast::Expr::Literal(lit) => {
                            Ok(literal_to_value(lit))
                        }
                        _ => Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE property '{}' must be a literal value (int, float, bool, or string)",
                            entry.key
                        ))),
                    }?;
                    Ok((entry.key.clone(), val))
                })
                .collect::<Result<Vec<_>>>()?;

            // SPA-234: enforce UNIQUE constraints before writing.
            //
            // We compare decoded Values rather than raw u64 heap pointers.
            // Strings longer than 7 bytes are stored as store-specific heap
            // pointers: the same string written by two different NodeStore
            // handles produces two different raw u64 values.  Decoding each
            // stored raw u64 back to Value and comparing is correct for all
            // value types (inline integers/short strings AND heap-overflow strings).
            //
            // We check both the committed on-disk values (via check_store) AND
            // nodes already buffered in tx.pending_ops (but not yet committed).
            // Without the pending-ops check, two nodes with the same constrained
            // value in one CREATE statement would both pass the on-disk check and
            // then be committed together, violating the constraint.
            {
                let constraints = self.inner.unique_constraints.read().unwrap();
                if !constraints.is_empty() {
                    let check_store = NodeStore::open(&self.inner.path)?;
                    for (prop_name, val) in &named_props {
                        let col_id = sparrowdb_common::col_id_of(prop_name);
                        if constraints.contains(&(label_id, col_id)) {
                            // Check committed on-disk values.
                            // Propagate I/O errors — silencing them would disable
                            // the constraint check on read failure.
                            let existing_raws = check_store.read_col_all(label_id, col_id)?;
                            let conflict_on_disk = existing_raws.iter().any(|&raw| {
                                if raw == 0 || raw == u64::MAX {
                                    return false;
                                }
                                check_store.decode_raw_value(raw) == *val
                            });
                            // Check nodes buffered earlier in this same statement.
                            let conflict_in_tx = tx.pending_ops.iter().any(|op| match op {
                                PendingOp::NodeCreate {
                                    label_id: pending_label_id,
                                    props,
                                    ..
                                } => {
                                    *pending_label_id == label_id
                                        && props.iter().any(|(existing_col_id, existing_val)| {
                                            *existing_col_id == col_id && *existing_val == *val
                                        })
                                }
                                _ => false,
                            });
                            if conflict_on_disk || conflict_in_tx {
                                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                                    "UNIQUE constraint violation: label \"{label}\" already has a node with {prop_name} = {:?}", val
                                )));
                            }
                        }
                    }
                }
            }

            let node_id = tx.create_node_named(label_id, &named_props)?;

            // Record the binding so edge patterns can resolve (src_var, dst_var).
            if !node.var.is_empty() {
                var_to_node.insert(node.var.clone(), node_id);
            }
        }

        // Create edges between the freshly-created nodes.
        for (left_var, rel_pat, right_var) in &create.edges {
            let src = var_to_node.get(left_var).copied().ok_or_else(|| {
                sparrowdb_common::Error::InvalidArgument(format!(
                    "CREATE edge references unresolved variable '{left_var}'"
                ))
            })?;
            let dst = var_to_node.get(right_var).copied().ok_or_else(|| {
                sparrowdb_common::Error::InvalidArgument(format!(
                    "CREATE edge references unresolved variable '{right_var}'"
                ))
            })?;
            tx.create_edge(src, dst, &rel_pat.rel_type, HashMap::new())?;
        }

        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Execute a Cypher query with runtime parameter bindings.
    ///
    /// This is the parameterised variant of [`execute`].  Parameters are
    /// supplied as a `HashMap<String, sparrowdb_execution::Value>` and are
    /// available inside the query as `$name` expressions.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sparrowdb::GraphDb;
    /// use sparrowdb_execution::Value;
    /// use std::collections::HashMap;
    ///
    /// let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
    /// let mut params = HashMap::new();
    /// params.insert("names".into(), Value::List(vec![
    ///     Value::String("Alice".into()),
    ///     Value::String("Bob".into()),
    /// ]));
    /// let result = db.execute_with_params("UNWIND $names AS name RETURN name", params).unwrap();
    /// ```
    pub fn execute_with_params(
        &self,
        cypher: &str,
        params: HashMap<String, sparrowdb_execution::Value>,
    ) -> Result<QueryResult> {
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;
        let catalog_snap = self.catalog_snapshot();
        let bound = bind(stmt, &catalog_snap)?;

        use sparrowdb_cypher::ast::Statement;
        if let Statement::CreateConstraint { label, property } = &bound.inner {
            return self.register_unique_constraint(label, property);
        }
        if Engine::is_mutation(&bound.inner) {
            // Route mutations through params-aware helpers (SPA-218).
            use sparrowdb_cypher::ast::Statement as Stmt;
            return match bound.inner {
                Stmt::Merge(ref m) => self.execute_merge_with_params(m, &params),
                Stmt::MatchMutate(ref mm) => self.execute_match_mutate_with_params(mm, &params),
                _ => Err(Error::InvalidArgument(
                    "execute_with_params: parameterized MATCH...CREATE and standalone CREATE \
                     are not yet supported; use MERGE or MATCH...SET with $params"
                        .into(),
                )),
            };
        }

        let _span = info_span!("sparrowdb.query_with_params").entered();

        let mut engine = {
            let _open_span = info_span!("sparrowdb.open_engine").entered();
            let csrs = self.cached_csr_map();
            Engine::new_with_cached_index(
                NodeStore::open(&self.inner.path)?,
                catalog_snap,
                csrs,
                &self.inner.path,
                Some(&self.inner.prop_index),
            )
            .with_params(params)
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.execute").entered();
            engine.execute_statement(bound.inner)?
        };

        engine.write_back_prop_index(&self.inner.prop_index);

        tracing::debug!(rows = result.rows.len(), "query_with_params complete");
        Ok(result)
    }

    /// Internal: execute a MERGE statement by opening a write transaction.
    fn execute_merge(&self, m: &sparrowdb_cypher::ast::MergeStatement) -> Result<QueryResult> {
        let props: HashMap<String, Value> = m
            .props
            .iter()
            .map(|pe| (pe.key.clone(), expr_to_value(&pe.value)))
            .collect();
        let mut tx = self.begin_write()?;
        let node_id = tx.merge_node(&m.label, props.clone())?;
        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();

        // If the statement has a RETURN clause, project the merged node's properties.
        if let Some(ref ret) = m.return_clause {
            use sparrowdb_cypher::ast::Expr;
            // Use the execution-layer Value type for building QueryResult rows.
            type ExecValue = sparrowdb_execution::Value;

            let var = if m.var.is_empty() {
                "n"
            } else {
                m.var.as_str()
            };

            // Collect all property names referenced in the RETURN clause so we
            // can look them up by col_id from the actual on-disk node state.
            // This is correct even when the node already existed with extra
            // properties not present in the merge pattern (SPA-215 / CodeAnt bug).
            let return_props: Vec<String> = ret
                .items
                .iter()
                .filter_map(|item| match &item.expr {
                    Expr::PropAccess { var: v, prop } if v.as_str() == var => Some(prop.clone()),
                    _ => None,
                })
                .collect();

            // Derive col_ids for every property name referenced in RETURN.
            let return_col_ids: Vec<u32> =
                return_props.iter().map(|name| fnv1a_col_id(name)).collect();

            // Read the actual on-disk node state via NodeStore::get_node, which
            // uses Value::from_u64 (type-tag-aware decoding) to correctly
            // reconstruct Bytes/String values rather than misinterpreting them as
            // raw integers.  We open a fresh NodeStore after the write committed
            // so we see the fully-merged node state.
            let store = NodeStore::open(&self.inner.path)?;
            let stored = store.get_node(node_id, &return_col_ids).unwrap_or_default();

            // Build an eval map: "{var}.{prop_name}" -> ExecValue from the
            // actual stored values rather than the input pattern props.
            let mut row_vals: HashMap<String, ExecValue> = HashMap::new();
            for (prop_name, col_id) in return_props.iter().zip(return_col_ids.iter()) {
                if let Some((_, val)) = stored.iter().find(|(c, _)| c == col_id) {
                    let exec_val = storage_value_to_exec(val);
                    row_vals.insert(format!("{var}.{prop_name}"), exec_val);
                }
            }

            // Derive column names from the RETURN items.
            let columns: Vec<String> = ret
                .items
                .iter()
                .map(|item| {
                    item.alias.clone().unwrap_or_else(|| match &item.expr {
                        Expr::PropAccess { var: v, prop } => format!("{v}.{prop}"),
                        Expr::Var(v) => v.clone(),
                        _ => "?".to_string(),
                    })
                })
                .collect();

            // Evaluate each RETURN expression using the actual-state prop map.
            let row: Vec<ExecValue> = ret
                .items
                .iter()
                .map(|item| eval_expr_merge(&item.expr, &row_vals))
                .collect();

            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }

        Ok(QueryResult::empty(vec![]))
    }

    /// Params-aware MERGE with $param support (SPA-218).
    fn execute_merge_with_params(
        &self,
        m: &sparrowdb_cypher::ast::MergeStatement,
        params: &HashMap<String, sparrowdb_execution::Value>,
    ) -> Result<QueryResult> {
        let props: HashMap<String, Value> = m
            .props
            .iter()
            .map(|pe| {
                let val = expr_to_value_with_params(&pe.value, params)?;
                Ok((pe.key.clone(), val))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut tx = self.begin_write()?;
        let node_id = tx.merge_node(&m.label, props.clone())?;
        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();
        if let Some(ref ret) = m.return_clause {
            use sparrowdb_cypher::ast::Expr;
            type ExecValue = sparrowdb_execution::Value;
            let var = if m.var.is_empty() {
                "n"
            } else {
                m.var.as_str()
            };
            let return_props: Vec<String> = ret
                .items
                .iter()
                .filter_map(|item| match &item.expr {
                    Expr::PropAccess { var: v, prop } if v.as_str() == var => Some(prop.clone()),
                    _ => None,
                })
                .collect();
            let return_col_ids: Vec<u32> =
                return_props.iter().map(|name| fnv1a_col_id(name)).collect();
            let store = NodeStore::open(&self.inner.path)?;
            let stored = store.get_node(node_id, &return_col_ids).unwrap_or_default();
            let mut row_vals: HashMap<String, ExecValue> = HashMap::new();
            for (prop_name, col_id) in return_props.iter().zip(return_col_ids.iter()) {
                if let Some((_, val)) = stored.iter().find(|(c, _)| c == col_id) {
                    row_vals.insert(format!("{var}.{prop_name}"), storage_value_to_exec(val));
                }
            }
            let columns: Vec<String> = ret
                .items
                .iter()
                .map(|item| {
                    item.alias.clone().unwrap_or_else(|| match &item.expr {
                        Expr::PropAccess { var: v, prop } => format!("{v}.{prop}"),
                        Expr::Var(v) => v.clone(),
                        _ => "?".to_string(),
                    })
                })
                .collect();
            let row: Vec<ExecValue> = ret
                .items
                .iter()
                .map(|item| eval_expr_merge(&item.expr, &row_vals))
                .collect();
            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }
        Ok(QueryResult::empty(vec![]))
    }

    /// Params-aware MATCH...SET with $param support (SPA-218).
    fn execute_match_mutate_with_params(
        &self,
        mm: &sparrowdb_cypher::ast::MatchMutateStatement,
        params: &HashMap<String, sparrowdb_execution::Value>,
    ) -> Result<QueryResult> {
        let mut tx = self.begin_write()?;
        let csrs = self.cached_csr_map();
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            self.catalog_snapshot(),
            csrs,
            &self.inner.path,
        );
        let matching_ids = engine.scan_match_mutate(mm)?;
        if matching_ids.is_empty() {
            return Ok(QueryResult::empty(vec![]));
        }
        match &mm.mutation {
            sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                let sv = expr_to_value_with_params(value, params)?;
                for node_id in matching_ids {
                    tx.set_property(node_id, prop, sv.clone())?;
                }
            }
            sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                for node_id in matching_ids {
                    tx.delete_node(node_id)?;
                }
            }
        }
        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Internal: execute a MATCH … SET / DELETE by scanning then writing.
    ///
    /// The write lock is acquired **before** the scan so that no concurrent
    /// writer can commit between the scan and the mutation, preventing stale
    /// matches from being mutated.
    fn execute_match_mutate(
        &self,
        mm: &sparrowdb_cypher::ast::MatchMutateStatement,
    ) -> Result<QueryResult> {
        // Acquire the write lock first.  From this point on no other writer
        // can commit until we call tx.commit() or drop tx.
        let mut tx = self.begin_write()?;

        // Build an Engine that reads from the same on-disk snapshot the write
        // transaction was opened against.  Because we hold the write lock,
        // the data on disk cannot change between this scan and the mutations
        // below.
        let csrs = self.cached_csr_map();
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            self.catalog_snapshot(),
            csrs,
            &self.inner.path,
        );

        // Collect matching node ids via the engine's scan (lock already held).
        let matching_ids = engine.scan_match_mutate(mm)?;

        if matching_ids.is_empty() {
            return Ok(QueryResult::empty(vec![]));
        }

        match &mm.mutation {
            sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                let sv = expr_to_value(value);
                for node_id in matching_ids {
                    tx.set_property(node_id, prop, sv.clone())?;
                }
            }
            sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                for node_id in matching_ids {
                    tx.delete_node(node_id)?;
                }
            }
        }

        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Internal: execute a `MATCH … CREATE (a)-[:R]->(b)` statement.
    ///
    /// 1. Acquires the write lock (preventing concurrent writes).
    /// 2. Opens a read-capable Engine to execute the MATCH clause, producing
    ///    one correlated binding row per match result (`var → NodeId`).
    /// 3. For each result row, calls `WriteTx::create_edge` using the exact
    ///    node IDs bound by that row — never re-scans or builds a Cartesian
    ///    product across all node candidates (SPA-183).
    /// 4. Commits the write transaction.
    ///
    /// If the MATCH finds no rows the CREATE is a no-op (no edges created,
    /// no error — SPA-168).
    fn execute_match_create(
        &self,
        mc: &sparrowdb_cypher::ast::MatchCreateStatement,
    ) -> Result<QueryResult> {
        // Acquire the write lock first so no concurrent writer can commit
        // between the scan and the edge creation.
        let mut tx = self.begin_write()?;

        // Build an Engine for the read-scan phase.
        let csrs = self.cached_csr_map();
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            self.catalog_snapshot(),
            csrs,
            &self.inner.path,
        );

        // SPA-208: reject reserved __SO_ rel-type prefix on edges in the CREATE clause.
        // Check before the Unimplemented guard so the reserved-name error takes priority.
        for (_, rel_pat, _) in &mc.create.edges {
            if is_reserved_label(&rel_pat.rel_type) {
                return Err(reserved_label_error(&rel_pat.rel_type));
            }
        }

        // MATCH…CREATE only supports edge creation from matched bindings.
        // Standalone node creation (CREATE clause with no edges) is not yet
        // implemented.  Note: the parser includes edge-endpoint nodes in
        // mc.create.nodes even for pure edge patterns, so we guard on
        // mc.create.edges being empty rather than mc.create.nodes being
        // non-empty (SPA-183 had the wrong guard).
        if mc.create.edges.is_empty() {
            return Err(Error::Unimplemented);
        }

        // Execute the MATCH clause to get correlated binding rows.
        // Each row is a HashMap<variable_name, NodeId> representing one
        // matched combination.  This replaces the old `scan_match_create`
        // approach which collected candidates per variable independently and
        // then took a full Cartesian product — causing N² edge creation when
        // multiple nodes of the same label existed (SPA-183).

        let matched_rows = engine.scan_match_create_rows(mc)?;

        if matched_rows.is_empty() {
            return Ok(QueryResult::empty(vec![]));
        }

        // For each matched row, create every edge declared in the CREATE clause
        // using the NodeIds bound in that specific row.
        for row in &matched_rows {
            for (left_var, rel_pat, right_var) in &mc.create.edges {
                let src = row.get(left_var).copied().ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "CREATE references unbound variable: {left_var}"
                    ))
                })?;
                let dst = row.get(right_var).copied().ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "CREATE references unbound variable: {right_var}"
                    ))
                })?;
                tx.create_edge(src, dst, &rel_pat.rel_type, HashMap::new())?;
            }
        }

        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Execute `MATCH … MERGE (a)-[r:TYPE]->(b)`: find-or-create a relationship.
    ///
    /// Algorithm (SPA-233):
    /// 1. MATCH the node patterns to get correlated (src, dst) NodeId pairs.
    /// 2. For each pair, look up the rel-type in the catalog.  If the rel table
    ///    does not exist yet, the relationship cannot exist → create it.
    /// 3. If the rel table exists, scan the delta log and (if checkpointed) the
    ///    CSR forward file to check whether a (src_slot, dst_slot) edge already
    ///    exists for this rel type.
    /// 4. If no existing edge is found, call `create_edge` to create it.
    ///
    /// The write lock is acquired before the scan so that no concurrent writer
    /// can race between the existence check and the edge creation.
    fn execute_match_merge_rel(
        &self,
        mm: &sparrowdb_cypher::ast::MatchMergeRelStatement,
    ) -> Result<QueryResult> {
        use sparrowdb_storage::edge_store::EdgeStore;

        // Acquire the write lock first.
        let mut tx = self.begin_write()?;

        // Build an Engine for the read-scan phase.
        let csrs = self.cached_csr_map();
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            self.catalog_snapshot(),
            csrs,
            &self.inner.path,
        );

        // Guard: reject reserved rel-type prefix used by internal system edges.
        if mm.rel_type.starts_with("__SO_") {
            return Err(Error::InvalidArgument(format!(
                "relationship type '{}' is reserved",
                mm.rel_type
            )));
        }

        // Scan MATCH patterns to get correlated (src_var, dst_var) NodeId rows.
        let matched_rows = engine.scan_match_merge_rel_rows(mm)?;
        if matched_rows.is_empty() {
            // No matched nodes → nothing to merge; this is a no-op (not an error).
            return Ok(QueryResult::empty(vec![]));
        }

        for row in &matched_rows {
            // Resolve the bound node IDs for this row.
            let src = if mm.src_var.is_empty() {
                // Anonymous source — cannot merge without a variable binding.
                return Err(Error::InvalidArgument(
                    "MERGE relationship pattern source variable is anonymous; \
                     use a named variable bound by the MATCH clause"
                        .into(),
                ));
            } else {
                *row.get(&mm.src_var).ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "MERGE references unbound source variable: {}",
                        mm.src_var
                    ))
                })?
            };

            let dst = if mm.dst_var.is_empty() {
                return Err(Error::InvalidArgument(
                    "MERGE relationship pattern destination variable is anonymous; \
                     use a named variable bound by the MATCH clause"
                        .into(),
                ));
            } else {
                *row.get(&mm.dst_var).ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "MERGE references unbound destination variable: {}",
                        mm.dst_var
                    ))
                })?
            };

            // Derive label IDs from the packed NodeIds.
            let src_label_id = (src.0 >> 32) as u16;
            let dst_label_id = (dst.0 >> 32) as u16;
            let src_slot = src.0 & 0xFFFF_FFFF;
            let dst_slot = dst.0 & 0xFFFF_FFFF;

            // Look up whether a rel table for this type already exists in the catalog.
            // `get_rel_table` returns `Ok(None)` if no such table is registered yet,
            // meaning no edge of this type can exist.
            let catalog_rel_id_opt =
                tx.catalog
                    .get_rel_table(src_label_id, dst_label_id, &mm.rel_type)?;

            let edge_already_exists = if let Some(catalog_rel_id) = catalog_rel_id_opt {
                let rel_table_id = RelTableId(catalog_rel_id as u32);

                if let Ok(store) = EdgeStore::open(&self.inner.path, rel_table_id) {
                    // Check delta log.
                    let in_delta = store
                        .read_delta()?
                        .iter()
                        .any(|rec| rec.src.0 == src.0 && rec.dst.0 == dst.0);

                    // Check CSR base (post-checkpoint edges).
                    let in_csr = if let Ok(csr) = store.open_fwd() {
                        csr.neighbors(src_slot).contains(&dst_slot)
                    } else {
                        false
                    };

                    // Also check pending (uncommitted in this same tx) edge creates.
                    let in_pending = tx.pending_ops.iter().any(|op| {
                        matches!(
                            op,
                            PendingOp::EdgeCreate {
                                src: ps,
                                dst: pd,
                                rel_table_id: prt,
                            } if *ps == src && *pd == dst && *prt == rel_table_id
                        )
                    });

                    in_delta || in_csr || in_pending
                } else {
                    false
                }
            } else {
                false
            };

            if !edge_already_exists {
                tx.create_edge(src, dst, &mm.rel_type, HashMap::new())?;
            }
        }

        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Create (or overwrite) a named full-text index.
    ///
    /// Creates the on-disk backing file for the named index so that
    /// `CALL db.index.fulltext.queryNodes(name, query)` can find it.
    /// If an index with the same name already exists its contents are cleared
    /// (overwrite semantics).  Document ingestion happens separately via
    /// [`WriteTx::add_to_fulltext_index`].
    ///
    /// Acquires the writer lock — returns [`Error::WriterBusy`] if a
    /// [`WriteTx`] is already active.
    ///
    /// # Example
    /// ```no_run
    /// # use sparrowdb::GraphDb;
    /// # let db = GraphDb::open(std::path::Path::new("/tmp/test")).unwrap();
    /// db.create_fulltext_index("searchIndex")?;
    /// # Ok::<(), sparrowdb_common::Error>(())
    /// ```
    pub fn create_fulltext_index(&self, name: &str) -> Result<()> {
        use sparrowdb_storage::fulltext_index::FulltextIndex;
        // Acquire the writer lock so this cannot race with an active WriteTx
        // that is reading or flushing the same index file.
        let _guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
        FulltextIndex::create(&self.inner.path, name)?;
        Ok(())
    }

    /// Execute multiple Cypher queries as a single atomic batch.
    ///
    /// All queries share one [`WriteTx`] and a **single WAL fsync** at the end,
    /// making this significantly faster than N individual [`execute`] calls when
    /// ingesting many nodes (e.g. Neo4j import, bulk data load).
    ///
    /// ## Semantics
    ///
    /// * **Atomicity** — if any query in the batch fails, the entire batch is
    ///   rolled back (the [`WriteTx`] is dropped without committing).
    /// * **Ordering** — queries are executed in slice order; each query sees the
    ///   mutations of all preceding queries in the batch.
    /// * **Single fsync** — the WAL is synced exactly once, after the last
    ///   query in the batch has been applied successfully.
    ///
    /// ## Restrictions
    ///
    /// * Only **write** statements are accepted (CREATE, MERGE, MATCH…SET,
    ///   MATCH…DELETE, MATCH…CREATE).  Read-only `MATCH … RETURN` queries are
    ///   allowed but their results are included in the returned `Vec`.
    /// * `CHECKPOINT` and `OPTIMIZE` inside a batch are executed immediately
    ///   and do **not** participate in the shared transaction; they act as
    ///   no-ops for atomicity purposes.
    /// * Parameters are not supported — use [`execute_with_params`] for that.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use sparrowdb::GraphDb;
    ///
    /// let db = GraphDb::open(std::path::Path::new("/tmp/batch.sparrow")).unwrap();
    ///
    /// let queries: Vec<&str> = (0..100)
    ///     .map(|i| Box::leak(format!("CREATE (n:Person {{id: {i}}})", i = i).into_boxed_str()) as &str)
    ///     .collect();
    ///
    /// let results = db.execute_batch(&queries).unwrap();
    /// assert_eq!(results.len(), 100);
    /// ```
    ///
    /// [`execute`]: GraphDb::execute
    /// [`execute_with_params`]: GraphDb::execute_with_params
    pub fn execute_batch(&self, queries: &[&str]) -> Result<Vec<QueryResult>> {
        use sparrowdb_cypher::ast::Statement;
        use sparrowdb_cypher::{bind, parse};

        if queries.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-parse and bind all queries before acquiring the writer lock.
        // A syntax or bind error fails fast without ever locking.
        let catalog_snap = self.catalog_snapshot();
        let bound_stmts: Vec<_> = queries
            .iter()
            .map(|q| {
                let stmt = parse(q)?;
                bind(stmt, &catalog_snap)
            })
            .collect::<Result<Vec<_>>>()?;

        // CHECKPOINT and OPTIMIZE cannot run inside a WriteTx (they acquire
        // the writer lock themselves).  Execute them eagerly in document order
        // and record placeholder results; they do not participate in the
        // shared transaction's atomicity.
        let mut early_results: Vec<Option<QueryResult>> = vec![None; bound_stmts.len()];
        for (i, bound) in bound_stmts.iter().enumerate() {
            match &bound.inner {
                Statement::Checkpoint => {
                    self.checkpoint()?;
                    early_results[i] = Some(QueryResult::empty(vec![]));
                }
                Statement::Optimize => {
                    self.optimize()?;
                    early_results[i] = Some(QueryResult::empty(vec![]));
                }
                Statement::CreateConstraint { label, property } => {
                    early_results[i] = Some(self.register_unique_constraint(label, property)?);
                }
                _ => {}
            }
        }

        // Acquire a single WriteTx for all structural + property mutations.
        let mut tx = self.begin_write()?;
        let mut results: Vec<Option<QueryResult>> = vec![None; bound_stmts.len()];

        for (i, bound) in bound_stmts.into_iter().enumerate() {
            if early_results[i].is_some() {
                continue;
            }

            let result = if Engine::is_mutation(&bound.inner) {
                self.execute_batch_mutation(bound.inner, &mut tx)?
            } else {
                // Read-only statement: execute against current snapshot.
                let csrs = self.cached_csr_map();
                let batch_catalog = self.catalog_snapshot();
                let mut engine = Engine::new(
                    NodeStore::open(&self.inner.path)?,
                    batch_catalog,
                    csrs,
                    &self.inner.path,
                );
                engine.execute_statement(bound.inner)?
            };
            results[i] = Some(result);
        }

        // Single fsync commit — all mutations land in one WAL transaction.
        tx.commit()?;
        self.invalidate_prop_index();
        self.invalidate_catalog();

        // Merge early (CHECKPOINT/OPTIMIZE) and mutation results in order.
        let final_results = results
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                r.or_else(|| early_results[i].take())
                    .unwrap_or_else(|| QueryResult::empty(vec![]))
            })
            .collect();

        Ok(final_results)
    }

    /// Internal: apply a single mutation statement to an already-open [`WriteTx`].
    ///
    /// Called by [`execute_batch`] to accumulate multiple mutations into one
    /// transaction (and therefore one WAL fsync on commit).
    fn execute_batch_mutation(
        &self,
        stmt: sparrowdb_cypher::ast::Statement,
        tx: &mut WriteTx,
    ) -> Result<QueryResult> {
        use sparrowdb_cypher::ast::Statement;

        match stmt {
            Statement::Create(ref c) => {
                // Inline the logic from execute_create_standalone using the
                // shared tx rather than opening a new WriteTx.
                let declared_vars: HashSet<&str> = c
                    .nodes
                    .iter()
                    .filter(|n| !n.var.is_empty())
                    .map(|n| n.var.as_str())
                    .collect();
                for (left_var, _, right_var) in &c.edges {
                    if !left_var.is_empty() && !declared_vars.contains(left_var.as_str()) {
                        return Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE edge references undeclared variable '{left_var}'"
                        )));
                    }
                    if !right_var.is_empty() && !declared_vars.contains(right_var.as_str()) {
                        return Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE edge references undeclared variable '{right_var}'"
                        )));
                    }
                }
                for node in &c.nodes {
                    if let Some(label) = node.labels.first() {
                        if is_reserved_label(label) {
                            return Err(reserved_label_error(label));
                        }
                    }
                }
                for (_, rel_pat, _) in &c.edges {
                    if is_reserved_label(&rel_pat.rel_type) {
                        return Err(reserved_label_error(&rel_pat.rel_type));
                    }
                }

                let mut var_to_node: HashMap<String, NodeId> = HashMap::new();
                for node in &c.nodes {
                    let label = node.labels.first().cloned().unwrap_or_default();
                    let label_id: u32 = match tx.catalog.get_label(&label)? {
                        Some(id) => id as u32,
                        None => tx.catalog.create_label(&label)? as u32,
                    };
                    let named_props: Vec<(String, Value)> = node
                        .props
                        .iter()
                        .map(|entry| {
                            let val = match &entry.value {
                                sparrowdb_cypher::ast::Expr::Literal(
                                    sparrowdb_cypher::ast::Literal::Null,
                                ) => Err(sparrowdb_common::Error::InvalidArgument(format!(
                                    "CREATE property '{}' is null",
                                    entry.key
                                ))),
                                sparrowdb_cypher::ast::Expr::Literal(
                                    sparrowdb_cypher::ast::Literal::Param(p),
                                ) => Err(sparrowdb_common::Error::InvalidArgument(format!(
                                    "CREATE property '{}' references parameter ${p}",
                                    entry.key
                                ))),
                                sparrowdb_cypher::ast::Expr::Literal(lit) => {
                                    Ok(literal_to_value(lit))
                                }
                                _ => Err(sparrowdb_common::Error::InvalidArgument(format!(
                                    "CREATE property '{}' must be a literal",
                                    entry.key
                                ))),
                            }?;
                            Ok((entry.key.clone(), val))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let node_id = tx.create_node_named(label_id, &named_props)?;
                    if !node.var.is_empty() {
                        var_to_node.insert(node.var.clone(), node_id);
                    }
                }
                for (left_var, rel_pat, right_var) in &c.edges {
                    let src = var_to_node.get(left_var).copied().ok_or_else(|| {
                        sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE edge references unresolved variable '{left_var}'"
                        ))
                    })?;
                    let dst = var_to_node.get(right_var).copied().ok_or_else(|| {
                        sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE edge references unresolved variable '{right_var}'"
                        ))
                    })?;
                    tx.create_edge(src, dst, &rel_pat.rel_type, HashMap::new())?;
                }
                Ok(QueryResult::empty(vec![]))
            }

            Statement::Merge(ref m) => {
                let props: HashMap<String, Value> = m
                    .props
                    .iter()
                    .map(|pe| (pe.key.clone(), expr_to_value(&pe.value)))
                    .collect();
                tx.merge_node(&m.label, props)?;
                Ok(QueryResult::empty(vec![]))
            }

            Statement::MatchMutate(ref mm) => {
                let csrs = self.cached_csr_map();
                let engine = Engine::new(
                    NodeStore::open(&self.inner.path)?,
                    self.catalog_snapshot(),
                    csrs,
                    &self.inner.path,
                );
                let matching_ids = engine.scan_match_mutate(mm)?;
                for node_id in matching_ids {
                    match &mm.mutation {
                        sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                            let sv = expr_to_value(value);
                            tx.set_property(node_id, prop, sv)?;
                        }
                        sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                            tx.delete_node(node_id)?;
                        }
                    }
                }
                Ok(QueryResult::empty(vec![]))
            }

            Statement::MatchCreate(ref mc) => {
                for (_, rel_pat, _) in &mc.create.edges {
                    if is_reserved_label(&rel_pat.rel_type) {
                        return Err(reserved_label_error(&rel_pat.rel_type));
                    }
                }
                if mc.create.edges.is_empty() {
                    return Err(Error::Unimplemented);
                }
                let csrs = self.cached_csr_map();
                let engine = Engine::new(
                    NodeStore::open(&self.inner.path)?,
                    self.catalog_snapshot(),
                    csrs,
                    &self.inner.path,
                );
                let matched_rows = engine.scan_match_create_rows(mc)?;
                for row in &matched_rows {
                    for (left_var, rel_pat, right_var) in &mc.create.edges {
                        let src = row.get(left_var).copied().ok_or_else(|| {
                            Error::InvalidArgument(format!(
                                "CREATE references unbound variable: {left_var}"
                            ))
                        })?;
                        let dst = row.get(right_var).copied().ok_or_else(|| {
                            Error::InvalidArgument(format!(
                                "CREATE references unbound variable: {right_var}"
                            ))
                        })?;
                        tx.create_edge(src, dst, &rel_pat.rel_type, HashMap::new())?;
                    }
                }
                Ok(QueryResult::empty(vec![]))
            }

            Statement::MatchMergeRel(ref mm) => {
                // Find-or-create relationship batch variant (SPA-233).
                //
                // NOTE: MATCH...MERGE in a batch reads committed on-disk state only;
                // nodes/edges created earlier in the same batch are not visible to
                // the MERGE existence check.  If in-batch visibility is required,
                // flush the transaction to disk before issuing MATCH...MERGE.
                let csrs = self.cached_csr_map();
                let engine = Engine::new(
                    NodeStore::open(&self.inner.path)?,
                    self.catalog_snapshot(),
                    csrs,
                    &self.inner.path,
                );
                // Guard: reject reserved rel-type prefix used by internal system edges.
                if mm.rel_type.starts_with("__SO_") {
                    return Err(Error::InvalidArgument(format!(
                        "relationship type '{}' is reserved",
                        mm.rel_type
                    )));
                }
                let matched_rows = engine.scan_match_merge_rel_rows(mm)?;
                for row in &matched_rows {
                    let src = *row.get(&mm.src_var).ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "MERGE references unbound source variable: {}",
                            mm.src_var
                        ))
                    })?;
                    let dst = *row.get(&mm.dst_var).ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "MERGE references unbound destination variable: {}",
                            mm.dst_var
                        ))
                    })?;
                    let src_label_id = (src.0 >> 32) as u16;
                    let dst_label_id = (dst.0 >> 32) as u16;
                    let src_slot = src.0 & 0xFFFF_FFFF;
                    let dst_slot = dst.0 & 0xFFFF_FFFF;

                    let catalog_rel_id_opt =
                        tx.catalog
                            .get_rel_table(src_label_id, dst_label_id, &mm.rel_type)?;

                    let edge_exists = if let Some(crid) = catalog_rel_id_opt {
                        let rtid = RelTableId(crid as u32);
                        if let Ok(store) = EdgeStore::open(&self.inner.path, rtid) {
                            let in_delta = store
                                .read_delta()?
                                .iter()
                                .any(|rec| rec.src.0 == src.0 && rec.dst.0 == dst.0);
                            let in_csr = if let Ok(csr) = store.open_fwd() {
                                csr.neighbors(src_slot).contains(&dst_slot)
                            } else {
                                false
                            };
                            let in_pending = tx.pending_ops.iter().any(|op| {
                                matches!(
                                    op,
                                    PendingOp::EdgeCreate {
                                        src: ps, dst: pd, rel_table_id: prt,
                                    } if *ps == src && *pd == dst && *prt == rtid
                                )
                            });
                            in_delta || in_csr || in_pending
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !edge_exists {
                        tx.create_edge(src, dst, &mm.rel_type, HashMap::new())?;
                    }
                }
                Ok(QueryResult::empty(vec![]))
            }

            _ => Err(Error::Unimplemented),
        }
    }

    /// Return `(node_count, edge_count)` by summing the high-water marks
    /// across all catalog labels (nodes) and delta-log record counts across
    /// all registered relationship tables (edges).
    ///
    /// Both counts reflect committed storage state; in-flight write
    /// transactions are not visible.
    ///
    /// **Note:** `node_count` is based on per-label high-water marks and
    /// therefore includes soft-deleted nodes whose slots have not yet been
    /// reclaimed.  The count will converge to the true live-node count once
    /// compaction / GC is implemented.
    pub fn db_counts(&self) -> Result<(u64, u64)> {
        let path = &self.inner.path;
        let catalog = self.catalog_snapshot();
        let node_store = NodeStore::open(path)?;

        // Node count: sum hwm_for_label across every registered label.
        let node_count: u64 = catalog
            .list_labels()
            .unwrap_or_default()
            .iter()
            .map(|(label_id, _name)| node_store.hwm_for_label(*label_id as u32).unwrap_or(0))
            .sum();

        // Edge count: for each registered rel table, sum CSR base edges (post-checkpoint)
        // plus delta-log records (pre-checkpoint / unflushed).
        let rel_table_ids = catalog.list_rel_table_ids();
        let ids_to_scan: Vec<u64> = if rel_table_ids.is_empty() {
            // No rel tables in catalog yet — fall back to the default table (id=0).
            vec![0]
        } else {
            rel_table_ids.iter().map(|(id, _, _, _)| *id).collect()
        };
        let edge_count: u64 = ids_to_scan
            .iter()
            .map(|&id| {
                let Ok(store) = EdgeStore::open(path, RelTableId(id as u32)) else {
                    return 0;
                };
                let csr_edges = store.open_fwd().map(|csr| csr.n_edges()).unwrap_or(0);
                let delta_edges = store
                    .read_delta()
                    .map(|records| records.len() as u64)
                    .unwrap_or(0);
                csr_edges + delta_edges
            })
            .sum();

        Ok((node_count, edge_count))
    }

    /// Return all node label names currently registered in the catalog (SPA-209).
    ///
    /// Labels are registered automatically the first time a node with that label
    /// is created. The returned list is ordered by insertion (i.e. by `LabelId`).
    pub fn labels(&self) -> Result<Vec<String>> {
        let catalog = self.catalog_snapshot();
        Ok(catalog
            .list_labels()?
            .into_iter()
            .map(|(_id, name)| name)
            .collect())
    }

    /// Return all relationship type names currently registered in the catalog (SPA-209).
    ///
    /// The returned list is deduplicated and sorted alphabetically.
    pub fn relationship_types(&self) -> Result<Vec<String>> {
        let catalog = self.catalog_snapshot();
        let mut types: Vec<String> = catalog
            .list_rel_tables()?
            .into_iter()
            .map(|(_src, _dst, rel_type)| rel_type)
            .collect();
        types.sort();
        types.dedup();
        Ok(types)
    }

    /// Return the top-`limit` nodes of the given `label` ordered by out-degree
    /// descending (SPA-168).
    ///
    /// Each element of the returned `Vec` is `(node_id, out_degree)`.  Ties are
    /// broken by ascending node id for determinism.
    ///
    /// This is an O(N log k) operation backed by the pre-computed
    /// [`DegreeCache`](sparrowdb_execution::DegreeCache) — no edge scan is
    /// performed at query time.
    ///
    /// Returns an empty `Vec` when `limit == 0`, the label is unknown, or the
    /// label has no nodes.
    ///
    /// # Errors
    /// Propagates I/O errors from opening the node store, CSR files, or
    /// catalog.
    pub fn top_degree_nodes(&self, label: &str, limit: usize) -> Result<Vec<(u64, u32)>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let path = &self.inner.path;
        let catalog = self.catalog_snapshot();
        let label_id: u32 = match catalog.get_label(label)? {
            Some(id) => id as u32,
            None => return Ok(vec![]),
        };
        let csrs = open_csr_map(path);
        let engine = Engine::new(NodeStore::open(path)?, catalog, csrs, path);
        engine.top_k_by_degree(label_id, limit)
    }

    /// Export the graph as a DOT (Graphviz) string for visualization.
    ///
    /// Queries all nodes via Cypher and reads edges directly from storage so
    /// that the output is correct regardless of whether the database has been
    /// checkpointed (CSR) or not (delta log).  The result can be piped through
    /// `dot -Tsvg` to produce an SVG, or written to a `.dot` file for offline
    /// rendering.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sparrowdb::GraphDb;
    /// # let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
    /// let dot = db.export_dot().unwrap();
    /// std::fs::write("graph.dot", &dot).unwrap();
    /// // Then: dot -Tsvg graph.dot -o graph.svg
    /// ```
    pub fn export_dot(&self) -> Result<String> {
        /// Escape a string for use inside a DOT label (backslash + double-quote).
        fn dot_escape(s: &str) -> String {
            s.replace('\\', "\\\\").replace('"', "\\\"")
        }

        /// Format a `Value` as a human-readable string for DOT labels / edge
        /// labels.  Falls back to the `Display` impl for most variants.
        fn fmt_val(v: &sparrowdb_execution::types::Value) -> String {
            match v {
                sparrowdb_execution::types::Value::String(s) => s.clone(),
                sparrowdb_execution::types::Value::Int64(i) => i.to_string(),
                sparrowdb_execution::types::Value::List(items) => {
                    // labels(n) returns a List of String items.
                    items
                        .iter()
                        .filter_map(|it| {
                            if let sparrowdb_execution::types::Value::String(s) = it {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(":")
                }
                other => format!("{other}"),
            }
        }

        let path = &self.inner.path;
        let catalog = self.catalog_snapshot();

        // Query all nodes via Cypher — id(n) is reliably Int64 from node scans.
        let nodes =
            self.execute("MATCH (n) RETURN id(n) AS nid, labels(n) AS lbls, n.name AS nm")?;

        // Read edges directly from storage.
        //
        // The Cypher engine's project_hop_row does not expose id(a)/id(b) in
        // hop patterns (SPA-149 follow-up), so we read the delta log
        // (pre-checkpoint) and CSR (post-checkpoint) directly.
        //
        // NodeId encoding: (label_id as u64) << 32 | slot
        // This is the same as the value returned by id(n) for node scans.
        let rel_tables = catalog.list_rel_tables_with_ids();
        let mut edge_triples: Vec<(i64, String, i64)> = Vec::new();

        for (catalog_id, src_label_id, dst_label_id, rel_type) in &rel_tables {
            let storage_rel_id = sparrowdb_storage::edge_store::RelTableId(*catalog_id as u32);

            if let Ok(store) = sparrowdb_storage::edge_store::EdgeStore::open(path, storage_rel_id)
            {
                // Delta log: stores full NodeId pairs directly.
                if let Ok(records) = store.read_delta() {
                    for rec in records {
                        edge_triples.push((rec.src.0 as i64, rel_type.clone(), rec.dst.0 as i64));
                    }
                }

                // CSR: (src_slot, dst_slot) relative to label IDs.
                if let Ok(csr) = store.open_fwd() {
                    let n_nodes = csr.n_nodes();
                    for src_slot in 0..n_nodes {
                        let src_id = ((*src_label_id as u64) << 32 | src_slot) as i64;
                        for &dst_slot in csr.neighbors(src_slot) {
                            let dst_id = ((*dst_label_id as u64) << 32 | dst_slot) as i64;
                            edge_triples.push((src_id, rel_type.clone(), dst_id));
                        }
                    }
                }
            }
        }

        // Build DOT output.
        let mut dot =
            String::from("digraph SparrowDB {\n  rankdir=LR;\n  node [shape=ellipse];\n\n");

        for row in &nodes.rows {
            let node_id = match &row[0] {
                sparrowdb_execution::types::Value::Int64(i) => *i,
                _ => continue,
            };
            let label_str = fmt_val(&row[1]);
            let name_str = match &row[2] {
                sparrowdb_execution::types::Value::Null => String::new(),
                v => fmt_val(v),
            };
            let display_label = if name_str.is_empty() {
                format!("{}\\nid={}", dot_escape(&label_str), node_id)
            } else {
                format!("{}\\n{}", dot_escape(&label_str), dot_escape(&name_str))
            };
            dot.push_str(&format!("  n{node_id} [label=\"{display_label}\"];\n"));
        }

        dot.push('\n');

        for (src_id, rel_type, dst_id) in &edge_triples {
            dot.push_str(&format!(
                "  n{src_id} -> n{dst_id} [label=\"{}\"];\n",
                dot_escape(rel_type)
            ));
        }

        dot.push_str("}\n");
        Ok(dot)
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
                    (col_id, self.store.decode_raw_value(raw_val))
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
///
/// # Atomicity guarantee (SPA-181)
///
/// All mutations (`create_node`, `create_edge`, `delete_node`, `create_label`,
/// `merge_node`) are buffered in memory until [`commit`] is called.  If the
/// transaction is dropped without committing, **no changes are persisted** —
/// the database remains in the state it was at the time [`begin_write`] was
/// called.
///
/// [`begin_write`]: GraphDb::begin_write
/// [`commit`]: WriteTx::commit
#[must_use = "call commit() to persist changes, or drop to discard"]
pub struct WriteTx {
    inner: Arc<DbInner>,
    store: NodeStore,
    catalog: Catalog,
    /// Staged property updates (not yet visible to readers).
    write_buf: WriteBuffer,
    /// Staged WAL mutation records to emit on commit.
    wal_mutations: Vec<WalMutation>,
    /// Set of node IDs written by this transaction (for MVCC conflict detection).
    dirty_nodes: HashSet<u64>,
    /// The committed txn_id at the time this WriteTx was opened (MVCC snapshot).
    snapshot_txn_id: u64,
    /// Held for the lifetime of this WriteTx; released on drop.
    /// Uses AtomicBool-based guard — no unsafe lifetime extension needed (SPA-181).
    _guard: WriteGuard,
    committed: bool,
    /// In-flight fulltext index updates — flushed to disk on commit.
    ///
    /// Caching open indexes here avoids one open+flush per `add_to_fulltext_index`
    /// call; instead we batch all additions and flush each index exactly once.
    fulltext_pending: HashMap<String, sparrowdb_storage::fulltext_index::FulltextIndex>,
    /// Buffered structural mutations (create_node, delete_node, create_edge,
    /// create_label) not yet written to disk.  Flushed atomically on commit.
    pending_ops: Vec<PendingOp>,
}

impl WriteTx {
    // ── Core node/property API (pre-Phase 7) ─────────────────────────────────

    /// Create a new node under `label_id` with the given properties.
    ///
    /// Returns the packed [`NodeId`].
    ///
    /// The node is **not** written to disk until [`commit`] is called.
    /// Dropping the transaction without committing discards this operation.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn create_node(&mut self, label_id: u32, props: &[(u32, Value)]) -> Result<NodeId> {
        // Allocate a node ID by consulting the in-memory HWM.  We peek at
        // the current HWM to compute the future NodeId without actually
        // writing anything to disk yet.
        let slot = self.store.peek_next_slot(label_id)?;
        let node_id = NodeId((label_id as u64) << 32 | slot as u64);
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeCreate {
            label_id,
            slot,
            props: props.to_vec(),
        });
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props: props.to_vec(),
            // Low-level create_node: no property names available.
            prop_names: Vec::new(),
        });
        Ok(node_id)
    }

    /// Create a new node with named properties, recording names in the WAL.
    ///
    /// Like [`create_node`] but accepts `(name, value)` pairs so that the
    /// property names are preserved in the WAL record for schema introspection
    /// (`CALL db.schema()`).  Col-ids are derived via [`col_id_of`].
    pub fn create_node_named(
        &mut self,
        label_id: u32,
        named_props: &[(String, Value)],
    ) -> Result<NodeId> {
        let props: Vec<(u32, Value)> = named_props
            .iter()
            .map(|(name, v)| (col_id_of(name), v.clone()))
            .collect();
        let prop_names: Vec<String> = named_props.iter().map(|(n, _)| n.clone()).collect();
        let slot = self.store.peek_next_slot(label_id)?;
        let node_id = NodeId((label_id as u64) << 32 | slot as u64);
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeCreate {
            label_id,
            slot,
            props: props.clone(),
        });
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props,
            prop_names,
        });
        Ok(node_id)
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
        self.set_node_col_named(node_id, col_id, format!("col_{col_id}"), value);
    }

    /// Stage a property update with an explicit human-readable key name for WAL.
    fn set_node_col_named(&mut self, node_id: NodeId, col_id: u32, key_name: String, value: Value) {
        let key = (node_id.0, col_id);
        self.dirty_nodes.insert(node_id.0);

        if self.write_buf.updates.contains_key(&key) {
            // Already staged this key — just update the new_value.
            let entry = self.write_buf.updates.get_mut(&key).unwrap();
            entry.new_value = value;
            // Keep the key_name from the first staging (it's the same column).
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
                .map(|(_, raw)| self.store.decode_raw_value(raw));
            disk_val.map(|v| (prev_txn_id, v))
        };

        self.write_buf.updates.insert(
            key,
            StagedUpdate {
                before_image,
                new_value: value,
                key_name,
            },
        );
    }

    /// Create a label in the schema catalog.
    ///
    /// # Note on atomicity
    ///
    /// Schema changes (label creation) are written to the catalog file
    /// immediately and are not rolled back if the transaction is later
    /// dropped without committing.  Label creation is idempotent at the
    /// catalog level: a duplicate name returns `Error::AlreadyExists`.
    /// Full schema-change atomicity is deferred to a future phase.
    pub fn create_label(&mut self, name: &str) -> Result<u16> {
        self.catalog.create_label(name)
    }

    /// Look up `name` in the catalog, creating it if it does not yet exist.
    ///
    /// Returns the `label_id` as a `u32` (upper 32 bits of a packed NodeId).
    /// Unlike [`create_label`], this method is idempotent: calling it multiple
    /// times with the same name always returns the same id.
    ///
    /// Primarily used by the bulk-import path (SPA-148) where labels may be
    /// seen for the first time on any row.
    ///
    /// [`create_label`]: WriteTx::create_label
    pub fn get_or_create_label_id(&mut self, name: &str) -> Result<u32> {
        match self.catalog.get_label(name)? {
            Some(id) => Ok(id as u32),
            None => Ok(self.catalog.create_label(name)? as u32),
        }
    }

    // ── Phase 7 mutation API (SPA-123 … SPA-126) ─────────────────────────────

    /// SPA-123: Find or create a node matching `label` + `props`.
    ///
    /// Scans the node store for a slot whose columns match every key→value
    /// pair in `props` (using [`fnv1a_col_id`] to derive column IDs from
    /// key strings).  Returns the existing [`NodeId`] if found, or creates a
    /// new node and returns the new id.
    ///
    /// The label is resolved (or created) in the catalog.
    pub fn merge_node(&mut self, label: &str, props: HashMap<String, Value>) -> Result<NodeId> {
        // Resolve / create label.
        let label_id: u32 = match self.catalog.get_label(label)? {
            Some(id) => id as u32,
            None => self.catalog.create_label(label)? as u32,
        };

        // Build col list from props keys.
        let col_kv: Vec<(String, u32, Value)> = props
            .into_iter()
            .map(|(k, v)| {
                let col_id = fnv1a_col_id(&k);
                (k, col_id, v)
            })
            .collect();
        let col_ids: Vec<u32> = col_kv.iter().map(|&(_, col_id, _)| col_id).collect();

        // First, check buffered (not-yet-committed) node creates in pending_ops.
        // This ensures merge_node is idempotent within a single transaction.
        for op in &self.pending_ops {
            if let PendingOp::NodeCreate {
                label_id: op_label_id,
                slot: op_slot,
                props: op_props,
            } = op
            {
                if *op_label_id == label_id {
                    let candidate = NodeId((label_id as u64) << 32 | *op_slot as u64);
                    let matches = col_kv.iter().all(|(_, col_id, want_val)| {
                        op_props
                            .iter()
                            .find(|&&(c, _)| c == *col_id)
                            // Compare in-memory Value objects directly so long
                            // strings (> 7 bytes) are not truncated (SPA-212).
                            .map(|(_, v)| v == want_val)
                            .unwrap_or(false)
                    });
                    if matches {
                        return Ok(candidate);
                    }
                }
            }
        }

        // Scan on-disk slots for a match (only checks committed/on-disk nodes).
        // Use disk_hwm_for_label to avoid scanning slots that were only reserved
        // in-memory by peek_next_slot but not yet flushed to disk.
        let disk_hwm = self.store.disk_hwm_for_label(label_id)?;
        for slot in 0..disk_hwm {
            let candidate = NodeId((label_id as u64) << 32 | slot);
            if let Ok(stored) = self.store.get_node_raw(candidate, &col_ids) {
                let matches = col_kv.iter().all(|(_, col_id, want_val)| {
                    stored
                        .iter()
                        .find(|&&(c, _)| c == *col_id)
                        .map(|&(_, raw)| {
                            // Compare decoded values so overflow strings (> 7 bytes)
                            // match correctly (SPA-212).
                            self.store.decode_raw_value(raw) == *want_val
                        })
                        .unwrap_or(false)
                });
                if matches {
                    return Ok(candidate);
                }
            }
        }

        // Not found — create a new node (buffered, same as create_node).
        let disk_props: Vec<(u32, Value)> = col_kv
            .iter()
            .map(|(_, col_id, v)| (*col_id, v.clone()))
            .collect();
        // Preserve property names from the col_kv tuples (key, col_id, value).
        let disk_prop_names: Vec<String> = col_kv.iter().map(|(k, _, _)| k.clone()).collect();
        let slot = self.store.peek_next_slot(label_id)?;
        let node_id = NodeId((label_id as u64) << 32 | slot as u64);
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeCreate {
            label_id,
            slot,
            props: disk_props.clone(),
        });
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props: disk_props,
            prop_names: disk_prop_names,
        });
        Ok(node_id)
    }

    /// SPA-124: Update a named property on a node.
    ///
    /// Derives a stable column ID from `key` via [`fnv1a_col_id`] and stages
    /// the update through [`set_node_col`] (which records the before-image in
    /// the write buffer).  WAL emission happens once at commit time via the
    /// `updates` loop in `write_mutation_wal`.
    pub fn set_property(&mut self, node_id: NodeId, key: &str, val: Value) -> Result<()> {
        let col_id = fnv1a_col_id(key);
        self.dirty_nodes.insert(node_id.0);

        // Stage the update through the write buffer (records before-image for
        // WAL and MVCC). WAL emission happens exactly once at commit time via
        // the `updates` loop in `write_mutation_wal`.  Pass the human-readable
        // key name so the WAL record carries it (not the synthesized col_{id}).
        self.set_node_col_named(node_id, col_id, key.to_string(), val);

        Ok(())
    }

    /// SPA-125: Delete a node, with edge-attachment check.
    ///
    /// Returns [`Error::NodeHasEdges`] if the node is referenced by any edge
    /// in the delta log.  On success, queues a `NodeDelete` WAL record and
    /// buffers the tombstone write; the on-disk tombstone is only applied when
    /// [`commit`] is called.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        // SPA-185: check ALL per-type delta logs for attached edges, not just
        // the hardcoded RelTableId(0).  Always include table-0 so that any
        // edges written before the catalog had entries are still detected.
        let rel_entries = self.catalog.list_rel_table_ids();
        let mut rel_ids_to_check: Vec<u32> =
            rel_entries.iter().map(|(id, _, _, _)| *id as u32).collect();
        // Always include the legacy table-0 slot.  If it is already in the
        // catalog list this dedup prevents a double-read.
        if !rel_ids_to_check.contains(&0u32) {
            rel_ids_to_check.push(0u32);
        }
        for rel_id in rel_ids_to_check {
            let delta = EdgeStore::open(&self.inner.path, RelTableId(rel_id))
                .and_then(|s| s.read_delta())
                .unwrap_or_default();
            if delta.iter().any(|r| r.src == node_id || r.dst == node_id) {
                return Err(Error::NodeHasEdges { node_id: node_id.0 });
            }
        }

        // Also check buffered (not-yet-committed) edge creates in this
        // transaction — a node that has already been connected in the current
        // transaction cannot be deleted before commit.
        let has_buffered_edge = self.pending_ops.iter().any(|op| {
            matches!(op, PendingOp::EdgeCreate { src, dst, .. } if *src == node_id || *dst == node_id)
        });
        if has_buffered_edge {
            return Err(Error::NodeHasEdges { node_id: node_id.0 });
        }

        // Buffer the tombstone — do NOT write to disk yet.
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeDelete { node_id });
        self.wal_mutations.push(WalMutation::NodeDelete { node_id });
        Ok(())
    }

    /// SPA-126: Create a directed edge `src → dst` with the given type.
    ///
    /// Buffers the edge creation; the delta-log append and WAL record are only
    /// written to disk when [`commit`] is called.  If the transaction is dropped
    /// without committing, no edge is persisted.
    ///
    /// Registers the relationship type name in the catalog so that queries
    /// like `MATCH (a)-[:REL]->(b)` can resolve the type (SPA-158).
    /// Returns the new [`EdgeId`].
    ///
    /// [`commit`]: WriteTx::commit
    pub fn create_edge(
        &mut self,
        src: NodeId,
        dst: NodeId,
        rel_type: &str,
        _props: HashMap<String, Value>,
    ) -> Result<EdgeId> {
        // Derive label IDs from the packed node IDs (upper 32 bits).
        let src_label_id = (src.0 >> 32) as u16;
        let dst_label_id = (dst.0 >> 32) as u16;

        // Register (or retrieve) the rel type in the catalog.
        // Catalog mutation is immediate and not transactional. This is acceptable
        // for now as rel type creation is idempotent. Full schema-change
        // atomicity is deferred to a future phase.
        let catalog_rel_id =
            self.catalog
                .get_or_create_rel_type_id(src_label_id, dst_label_id, rel_type)?;
        let rel_table_id = RelTableId(catalog_rel_id as u32);

        // Compute the edge ID from the on-disk delta log size, offset by the
        // number of edges already buffered in this transaction for the same
        // rel_table_id.  Without this offset, multiple create_edge calls in the
        // same transaction would all derive the same on-disk base and collide.
        let base_edge_id = EdgeStore::peek_next_edge_id(&self.inner.path, rel_table_id)?;
        let buffered_count = self
            .pending_ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    PendingOp::EdgeCreate {
                        rel_table_id: pending_rel_table_id,
                        ..
                    } if *pending_rel_table_id == rel_table_id
                )
            })
            .count() as u64;
        let edge_id = EdgeId(base_edge_id.0 + buffered_count);

        // Buffer the edge append — do NOT write to disk yet.
        self.pending_ops.push(PendingOp::EdgeCreate {
            src,
            dst,
            rel_table_id,
        });
        self.wal_mutations.push(WalMutation::EdgeCreate {
            edge_id,
            src,
            dst,
            rel_type: rel_type.to_string(),
        });
        Ok(edge_id)
    }

    // ── MVCC conflict detection (SPA-128) ────────────────────────────────────

    /// Check for write-write conflicts before committing.
    ///
    /// A conflict is detected when another `WriteTx` has committed a change
    /// to a node that this transaction also dirtied, at a `txn_id` greater
    /// than our snapshot.
    fn detect_conflicts(&self) -> Result<()> {
        let nv = self.inner.node_versions.read().expect("node_versions lock");
        for &raw in &self.dirty_nodes {
            let last_write = nv.get(NodeId(raw));
            if last_write > self.snapshot_txn_id {
                return Err(Error::WriteWriteConflict { node_id: raw });
            }
        }
        Ok(())
    }

    // ── Full-text index maintenance ──────────────────────────────────────────

    /// Add a node document to a named full-text index.
    ///
    /// Call after creating or updating a node to keep the index current.
    /// The `text` should be the concatenated string value(s) of the indexed
    /// properties.  Changes are flushed to disk immediately (no WAL for v1).
    ///
    /// # Example
    /// ```no_run
    /// # use sparrowdb::GraphDb;
    /// # use sparrowdb_common::NodeId;
    /// # let db = GraphDb::open(std::path::Path::new("/tmp/test")).unwrap();
    /// # let mut tx = db.begin_write().unwrap();
    /// # let node_id = NodeId(0);
    /// tx.add_to_fulltext_index("searchIndex", node_id, "some searchable text")?;
    /// # Ok::<(), sparrowdb_common::Error>(())
    /// ```
    pub fn add_to_fulltext_index(
        &mut self,
        index_name: &str,
        node_id: NodeId,
        text: &str,
    ) -> Result<()> {
        use sparrowdb_storage::fulltext_index::FulltextIndex;
        // Lazily open the index and cache it for the lifetime of this
        // transaction.  All additions are batched and flushed once on commit,
        // avoiding an open+flush round-trip per document.
        let idx = match self.fulltext_pending.get_mut(index_name) {
            Some(existing) => existing,
            None => {
                let opened = FulltextIndex::open(&self.inner.path, index_name)?;
                self.fulltext_pending.insert(index_name.to_owned(), opened);
                self.fulltext_pending.get_mut(index_name).unwrap()
            }
        };
        idx.add_document(node_id.0, text);
        Ok(())
    }

    // ── Commit ───────────────────────────────────────────────────────────────

    /// Commit the transaction.
    ///
    /// WAL-first protocol (SPA-184):
    ///
    /// 1. Detects write-write conflicts (SPA-128).
    /// 2. Drains staged property updates (does NOT apply to disk yet).
    /// 3. Atomically increments the global `current_txn_id` to obtain the new
    ///    transaction ID that will label all WAL records.
    /// 4. **Writes WAL records and fsyncs** (Begin + all structural mutations +
    ///    all property updates + Commit) so that the intent is durable before
    ///    any data page is touched (SPA-184).
    /// 5. Applies all buffered structural operations to storage (SPA-181):
    ///    node creates, node deletes, edge creates.
    /// 6. Flushes all staged `set_node_col` updates to disk.
    /// 7. Records before-images in the version chain at the previous `txn_id`,
    ///    preserving snapshot access for currently-open readers.
    /// 8. Records the new values in the version chain at the new `txn_id`.
    /// 9. Updates per-node version table for future conflict detection.
    #[must_use = "check the Result; a failed commit means nothing was written"]
    pub fn commit(mut self) -> Result<TxnId> {
        // Step 1: MVCC conflict abort (SPA-128).
        self.detect_conflicts()?;

        // Step 2: Drain staged property updates — collect but do NOT write to
        // disk yet.  We need them in hand to emit WAL records before touching
        // any data page.
        let updates: Vec<((u64, u32), StagedUpdate)> = self.write_buf.updates.drain().collect();

        // Step 3: Increment txn_id with Release ordering.  We need the ID now
        // so that WAL records (emitted next) carry the correct txn_id.
        let new_id = self.inner.current_txn_id.fetch_add(1, Ordering::Release) + 1;

        // Step 4: WAL-first — write all mutation records and fsync before
        // touching any data page (SPA-184).  A crash between here and the end
        // of Step 6 is recoverable: WAL replay will re-apply the ops.
        write_mutation_wal(
            &self.inner.path,
            new_id,
            &updates,
            &self.wal_mutations,
            self.inner.encryption_key,
        )?;

        // Step 5: Apply buffered structural operations to disk (SPA-181).
        // WAL is already durable at this point; a crash here is safe.
        for op in self.pending_ops.drain(..) {
            match op {
                PendingOp::NodeCreate {
                    label_id,
                    slot,
                    props,
                } => {
                    self.store.create_node_at_slot(label_id, slot, &props)?;
                }
                PendingOp::NodeDelete { node_id } => {
                    self.store.tombstone_node(node_id)?;
                }
                PendingOp::EdgeCreate {
                    src,
                    dst,
                    rel_table_id,
                } => {
                    let mut es = EdgeStore::open(&self.inner.path, rel_table_id)?;
                    es.create_edge(src, rel_table_id, dst)?;
                }
            }
        }

        // Step 6: Flush property updates to disk.
        // Use `upsert_node_col` so that columns added by `set_property` (which
        // may not have been initialised during `create_node`) are created and
        // zero-padded automatically.
        for ((node_raw, col_id), ref staged) in &updates {
            self.store
                .upsert_node_col(NodeId(*node_raw), *col_id, &staged.new_value)?;
        }

        // Step 7+8: Publish versions.
        {
            let mut vs = self.inner.versions.write().expect("version lock poisoned");
            for ((node_raw, col_id), ref staged) in &updates {
                // Publish before-image at the previous txn_id so that readers
                // pinned at that snapshot continue to see the correct value.
                if let Some((prev_txn_id, ref before_val)) = staged.before_image {
                    vs.insert(NodeId(*node_raw), *col_id, prev_txn_id, before_val.clone());
                }
                // Publish new value at the current txn_id.
                vs.insert(NodeId(*node_raw), *col_id, new_id, staged.new_value.clone());
            }
        }

        // Step 9: Advance per-node version table.
        {
            let mut nv = self
                .inner
                .node_versions
                .write()
                .expect("node_versions lock");
            for &raw in &self.dirty_nodes {
                nv.set(NodeId(raw), new_id);
            }
        }

        // Step 10: Flush any pending fulltext index updates.
        // The primary DB mutations above are already durable (WAL written,
        // txn_id advanced).  A flush failure here must NOT return Err and
        // cause the caller to retry an already-committed transaction.  Log the
        // error so operators can investigate but treat the commit as successful.
        for (name, mut idx) in self.fulltext_pending.drain() {
            if let Err(e) = idx.flush() {
                tracing::error!(index = %name, error = %e, "fulltext index flush failed post-commit; index may be stale until next write");
            }
        }

        // Step 11: Refresh the shared catalog cache so subsequent reads see
        // any labels / rel types created in this transaction (SPA-188).
        if let Ok(fresh) = Catalog::open(&self.inner.path) {
            *self.inner.catalog.write().expect("catalog RwLock poisoned") = fresh;
        }

        self.committed = true;
        Ok(TxnId(new_id))
    }
}

impl Drop for WriteTx {
    fn drop(&mut self) {
        // Uncommitted staged updates are discarded here — no writes to disk.
        // The write lock is released by dropping `_guard` (WriteGuard).
        let _ = self.committed;
    }
}

// ── WAL mutation emission (SPA-127) ───────────────────────────────────────────

/// Append mutation WAL records for a committed transaction.
///
/// Opens (or continues) the WAL writer in `{db_root}/wal/` and emits:
/// - `Begin`
/// - `NodeUpdate` for each staged property change in `updates`
/// - `NodeCreate` / `NodeUpdate` / `NodeDelete` / `EdgeCreate` for each structural mutation
/// - `Commit`
/// - fsync
fn write_mutation_wal(
    db_root: &Path,
    txn_id: u64,
    updates: &[((u64, u32), StagedUpdate)],
    mutations: &[WalMutation],
    encryption_key: Option<[u8; 32]>,
) -> Result<()> {
    // Nothing to record if there were no mutations or property updates.
    if updates.is_empty() && mutations.is_empty() {
        return Ok(());
    }

    let wal_dir = db_root.join("wal");
    let mut wal = match encryption_key {
        Some(key) => WalWriter::open_encrypted(&wal_dir, key)?,
        None => WalWriter::open(&wal_dir)?,
    };
    let txn = TxnId(txn_id);

    wal.append(WalRecordKind::Begin, txn, WalPayload::Empty)?;

    // NodeUpdate records for each property change in the write buffer.
    for ((node_raw, col_id), staged) in updates {
        let before_bytes = staged
            .before_image
            .as_ref()
            .map(|(_, v)| value_to_wal_bytes(v))
            .unwrap_or_else(|| 0u64.to_le_bytes().to_vec());
        let after_bytes = value_to_wal_bytes(&staged.new_value);
        wal.append(
            WalRecordKind::NodeUpdate,
            txn,
            WalPayload::NodeUpdate {
                node_id: *node_raw,
                key: staged.key_name.clone(),
                col_id: *col_id,
                before: before_bytes,
                after: after_bytes,
            },
        )?;
    }

    // Structural mutations.
    for m in mutations {
        match m {
            WalMutation::NodeCreate {
                node_id,
                label_id,
                props,
                prop_names,
            } => {
                // Use human-readable names when available; fall back to
                // "col_{hash}" when only the col_id is known (low-level API).
                let wal_props: Vec<(String, Vec<u8>)> = props
                    .iter()
                    .enumerate()
                    .map(|(i, (col_id, v))| {
                        let name = prop_names
                            .get(i)
                            .filter(|n| !n.is_empty())
                            .cloned()
                            .unwrap_or_else(|| format!("col_{col_id}"));
                        (name, value_to_wal_bytes(v))
                    })
                    .collect();
                wal.append(
                    WalRecordKind::NodeCreate,
                    txn,
                    WalPayload::NodeCreate {
                        node_id: node_id.0,
                        label_id: *label_id,
                        props: wal_props,
                    },
                )?;
            }
            WalMutation::NodeDelete { node_id } => {
                wal.append(
                    WalRecordKind::NodeDelete,
                    txn,
                    WalPayload::NodeDelete { node_id: node_id.0 },
                )?;
            }
            WalMutation::EdgeCreate {
                edge_id,
                src,
                dst,
                rel_type,
            } => {
                wal.append(
                    WalRecordKind::EdgeCreate,
                    txn,
                    WalPayload::EdgeCreate {
                        edge_id: edge_id.0,
                        src: src.0,
                        dst: dst.0,
                        rel_type: rel_type.clone(),
                        props: vec![],
                    },
                )?;
            }
        }
    }

    wal.append(WalRecordKind::Commit, txn, WalPayload::Empty)?;
    wal.fsync()?;
    Ok(())
}

// ── FNV-1a col_id derivation ─────────────────────────────────────────────────

/// Derive a stable `u32` column ID from a property key name.
///
/// Delegates to [`sparrowdb_common::col_id_of`] — the single canonical
/// FNV-1a implementation shared by storage and execution (SPA-160).
pub fn fnv1a_col_id(key: &str) -> u32 {
    col_id_of(key)
}

// ── Cypher string utilities ────────────────────────────────────────────────────

/// Escape a Rust `&str` so it can be safely interpolated inside a single-quoted
/// Cypher string literal.
///
/// Two characters require escaping inside Cypher single-quoted strings:
/// * `\` → `\\`  (backslash must be doubled first to avoid misinterpreting
///   the subsequent escape sequence)
/// * `'` → `\'`  (prevents premature termination of the string literal)
///
/// # Example
///
/// ```
/// use sparrowdb::cypher_escape_string;
/// let safe = cypher_escape_string("O'Reilly");
/// let cypher = format!("MATCH (n {{name: '{safe}'}}) RETURN n");
/// assert_eq!(cypher, "MATCH (n {name: 'O\\'Reilly'}) RETURN n");
/// ```
///
/// **Prefer parameterized queries** (`execute_with_params`) over string
/// interpolation whenever possible — this function is provided for the cases
/// where dynamic query construction cannot be avoided (SPA-218).
pub fn cypher_escape_string(s: &str) -> String {
    // Process backslash first so that the apostrophe replacement below does
    // not accidentally double-escape newly-inserted backslashes.
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

// ── Mutation value helpers ─────────────────────────────────────────────────────

/// Convert a Cypher [`Literal`] to a storage [`Value`].
fn literal_to_value(lit: &sparrowdb_cypher::ast::Literal) -> Value {
    use sparrowdb_cypher::ast::Literal;
    match lit {
        Literal::Int(n) => Value::Int64(*n),
        // Float stored as Value::Float — NodeStore::encode_value writes the full
        // 8 IEEE-754 bytes to the overflow heap (SPA-267).
        Literal::Float(f) => Value::Float(*f),
        Literal::Bool(b) => Value::Int64(if *b { 1 } else { 0 }),
        Literal::String(s) => Value::Bytes(s.as_bytes().to_vec()),
        Literal::Null | Literal::Param(_) => Value::Int64(0),
    }
}

/// Convert a Cypher [`Expr`] to a storage [`Value`].
///
/// Only literal expressions are supported for SET values at this stage.
fn expr_to_value(expr: &sparrowdb_cypher::ast::Expr) -> Value {
    use sparrowdb_cypher::ast::Expr;
    match expr {
        Expr::Literal(lit) => literal_to_value(lit),
        _ => Value::Int64(0),
    }
}

fn literal_to_value_with_params(
    lit: &sparrowdb_cypher::ast::Literal,
    params: &HashMap<String, sparrowdb_execution::Value>,
) -> Result<Value> {
    use sparrowdb_cypher::ast::Literal;
    match lit {
        Literal::Int(n) => Ok(Value::Int64(*n)),
        Literal::Float(f) => Ok(Value::Float(*f)),
        Literal::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
        Literal::String(s) => Ok(Value::Bytes(s.as_bytes().to_vec())),
        Literal::Null => Ok(Value::Int64(0)),
        Literal::Param(p) => match params.get(p.as_str()) {
            Some(v) => Ok(exec_value_to_storage(v)),
            None => Err(Error::InvalidArgument(format!(
                "parameter ${p} was referenced in the query but not supplied"
            ))),
        },
    }
}

fn expr_to_value_with_params(
    expr: &sparrowdb_cypher::ast::Expr,
    params: &HashMap<String, sparrowdb_execution::Value>,
) -> Result<Value> {
    use sparrowdb_cypher::ast::Expr;
    match expr {
        Expr::Literal(lit) => literal_to_value_with_params(lit, params),
        _ => Err(Error::InvalidArgument(
            "property value must be a literal or $parameter".into(),
        )),
    }
}

fn exec_value_to_storage(v: &sparrowdb_execution::Value) -> Value {
    use sparrowdb_execution::Value as EV;
    match v {
        EV::Int64(n) => Value::Int64(*n),
        EV::Float64(f) => Value::Float(*f),
        EV::Bool(b) => Value::Int64(if *b { 1 } else { 0 }),
        EV::String(s) => Value::Bytes(s.as_bytes().to_vec()),
        _ => Value::Int64(0),
    }
}

/// Encode a storage [`Value`] to a raw little-endian byte vector for WAL
/// logging.  Unlike [`Value::to_u64`], this never panics for `Float` values —
/// it emits the full 8 IEEE-754 bytes so the WAL payload is lossless.
///
/// The bytes are for observability only (schema introspection uses only the
/// property *name*, not the value); correctness of on-disk storage is
/// handled separately via [`NodeStore::encode_value`].
fn value_to_wal_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Float(f) => f.to_bits().to_le_bytes().to_vec(),
        // For Int64 and Bytes the existing to_u64() encoding is fine.
        other => other.to_u64().to_le_bytes().to_vec(),
    }
}

/// Convert a storage-layer `Value` (Int64 / Bytes / Float) to the execution-layer
/// `Value` (Int64 / String / Float64 / Null / …) used in `QueryResult` rows.
fn storage_value_to_exec(val: &Value) -> sparrowdb_execution::Value {
    match val {
        Value::Int64(n) => sparrowdb_execution::Value::Int64(*n),
        Value::Bytes(b) => {
            sparrowdb_execution::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        Value::Float(f) => sparrowdb_execution::Value::Float64(*f),
    }
}

/// Evaluate a RETURN expression against a simple name→ExecValue map built
/// from the merged node's properties.  Used exclusively by `execute_merge`.
///
/// Supports `PropAccess` (e.g. `n.name`) and `Literal`; everything else
/// falls back to `Null`.
fn eval_expr_merge(
    expr: &sparrowdb_cypher::ast::Expr,
    vals: &HashMap<String, sparrowdb_execution::Value>,
) -> sparrowdb_execution::Value {
    use sparrowdb_cypher::ast::{Expr, Literal};
    match expr {
        Expr::PropAccess { var, prop } => {
            let key = format!("{var}.{prop}");
            vals.get(&key)
                .cloned()
                .unwrap_or(sparrowdb_execution::Value::Null)
        }
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => sparrowdb_execution::Value::Int64(*n),
            Literal::Float(f) => sparrowdb_execution::Value::Float64(*f),
            Literal::Bool(b) => sparrowdb_execution::Value::Bool(*b),
            Literal::String(s) => sparrowdb_execution::Value::String(s.clone()),
            Literal::Null | Literal::Param(_) => sparrowdb_execution::Value::Null,
        },
        Expr::Var(v) => vals
            .get(v.as_str())
            .cloned()
            .unwrap_or(sparrowdb_execution::Value::Null),
        _ => sparrowdb_execution::Value::Null,
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn collect_maintenance_params(
    catalog: &Catalog,
    node_store: &NodeStore,
    db_root: &Path,
) -> (Vec<u32>, u64) {
    // SPA-185: collect all registered rel table IDs from the catalog instead
    // of hardcoding [0].  This ensures every per-type edge store is checkpointed.
    // Always include table-0 so that any edges written before the catalog had
    // entries (legacy data or pre-SPA-185 databases) are also checkpointed.
    let rel_table_entries = catalog.list_rel_table_ids();
    let mut rel_table_ids: Vec<u32> = rel_table_entries
        .iter()
        .map(|(id, _, _, _)| *id as u32)
        .collect();
    // Always include the legacy table-0 slot.  Dedup if already present.
    if !rel_table_ids.contains(&0u32) {
        rel_table_ids.push(0u32);
    }

    // n_nodes must cover the highest node-store HWM AND the highest node ID
    // present in any delta record, so that the CSR bounds check passes even
    // when edges were inserted without going through the node-store API.
    let hwm_n_nodes: u64 = catalog
        .list_labels()
        .unwrap_or_default()
        .iter()
        .map(|(label_id, _name)| node_store.hwm_for_label(*label_id as u32).unwrap_or(0))
        .sum::<u64>();

    // Also scan delta records for the maximum *slot* index used.
    // NodeIds encode `(label_id << 32) | slot`; the CSR is indexed by slot,
    // so we must strip the label bits before computing n_nodes.  Using the
    // full NodeId here would inflate n_nodes into the billions (e.g. label_id=1
    // → slot 0 appears as 0x0000_0001_0000_0000) and the CSR would be built
    // with nonsensical degree arrays — the SPA-186 root cause.
    let delta_max: u64 = rel_table_ids
        .iter()
        .filter_map(|&rel_id| {
            EdgeStore::open(db_root, RelTableId(rel_id))
                .ok()
                .and_then(|s| s.read_delta().ok())
        })
        .flat_map(|records| {
            records.into_iter().flat_map(|r| {
                // Strip label bits — CSR needs slot indices only.
                let src_slot = r.src.0 & 0xFFFF_FFFF;
                let dst_slot = r.dst.0 & 0xFFFF_FFFF;
                [src_slot, dst_slot].into_iter()
            })
        })
        .max()
        .map(|max_slot| max_slot + 1)
        .unwrap_or(0);

    let n_nodes = hwm_n_nodes.max(delta_max).max(1);

    (rel_table_ids, n_nodes)
}

/// Open all per-type CSR forward files that exist on disk, keyed by rel_table_id.
///
/// SPA-185: replaces the old `open_csr_forward` that only opened `RelTableId(0)`.
/// The catalog is used to discover all registered rel types.
fn open_csr_map(path: &Path) -> HashMap<u32, CsrForward> {
    let catalog = match Catalog::open(path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };
    let mut map = HashMap::new();

    // Collect rel IDs from catalog.
    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();

    // Always include the legacy table-0 slot so that checkpointed CSRs
    // written before the catalog had entries (pre-SPA-185 data) are loaded.
    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }

    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    map
}

/// Like [`open_csr_map`] but surfaces the catalog-open error so callers can
/// decide whether to replace an existing cache.  Used by
/// [`GraphDb::invalidate_csr_map`] to avoid clobbering a valid in-memory map
/// with an empty one when the catalog is transiently unreadable.
fn try_open_csr_map(path: &Path) -> Result<HashMap<u32, CsrForward>> {
    let catalog = Catalog::open(path)?;
    let mut map = HashMap::new();

    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();

    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }

    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    Ok(map)
}

// ── Storage-size helpers (SPA-171) ────────────────────────────────────────────

fn dir_size_bytes(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            total += dir_size_bytes(&p);
        } else if let Ok(m) = std::fs::metadata(&p) {
            total += m.len();
        }
    }
    total
}

// ── Reserved label/type protection (SPA-208) ──────────────────────────────────

/// Returns `true` if `label` starts with the reserved `__SO_` prefix.
///
/// The `__SO_` namespace is reserved for internal SparrowDB system objects.
/// Any attempt to CREATE a node or relationship using a label/type in this
/// namespace is rejected with an [`Error::InvalidArgument`].
#[inline]
fn is_reserved_label(label: &str) -> bool {
    label.starts_with("__SO_")
}

/// Return an [`Error::InvalidArgument`] for a reserved label/type.
fn reserved_label_error(label: &str) -> sparrowdb_common::Error {
    sparrowdb_common::Error::InvalidArgument(format!(
        "invalid argument: label \"{label}\" is reserved — the __SO_ prefix is for internal use only"
    ))
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
        db.checkpoint()
            .expect("checkpoint must succeed on empty DB");
    }

    #[test]
    fn graphdb_optimize_on_empty_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        db.optimize().expect("optimize must succeed on empty DB");
    }

    /// SPA-162: checkpoint() must not deadlock after a write transaction has
    /// been committed and dropped.
    #[test]
    fn checkpoint_does_not_deadlock_after_write() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(GraphDb::open(dir.path()).unwrap());

        // Run a write transaction and commit it.
        let mut tx = db.begin_write().unwrap();
        tx.create_node(0, &[]).unwrap();
        tx.commit().unwrap();

        // checkpoint() must complete without hanging — run it on a thread so
        // the test runner can time out rather than block the whole suite.
        let db2 = Arc::clone(&db);
        let handle = std::thread::spawn(move || {
            db2.checkpoint().unwrap();
        });
        handle
            .join()
            .expect("checkpoint thread must complete without panic");
    }

    /// SPA-162: checkpoint() must return WriterBusy (not deadlock) when a
    /// WriteTx is currently active.  Before the fix, calling lock() from
    /// checkpoint() while the same thread held the mutex would hang forever.
    #[test]
    fn checkpoint_returns_writer_busy_while_write_tx_active() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        // Hold an active write transaction without committing it.
        let tx = db.begin_write().unwrap();

        // checkpoint() must return WriterBusy immediately — not deadlock.
        let result = db.checkpoint();
        assert!(
            matches!(result, Err(Error::WriterBusy)),
            "expected WriterBusy while WriteTx active, got: {result:?}"
        );

        // Drop the transaction; checkpoint must now succeed.
        drop(tx);
        db.checkpoint()
            .expect("checkpoint must succeed after WriteTx dropped");
    }

    /// SPA-181: Dropping a WriteTx without calling commit() must not persist
    /// any mutations.  The node-store HWM must remain at 0 after a dropped tx.
    #[test]
    fn dropped_write_tx_persists_no_nodes() {
        use sparrowdb_storage::node_store::NodeStore;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        {
            let mut tx = db.begin_write().unwrap();
            // Stage a node creation — should NOT be written to disk.
            let _node_id = tx.create_node(0, &[]).unwrap();
            // Drop WITHOUT calling commit().
        }

        // Verify no node was persisted.
        let store = NodeStore::open(dir.path()).unwrap();
        let hwm = store.hwm_for_label(0).unwrap();
        assert_eq!(hwm, 0, "dropped tx must not persist any nodes (SPA-181)");

        // A subsequent write transaction must be obtainable (lock released).
        let tx2 = db
            .begin_write()
            .expect("write lock must be released after drop");
        tx2.commit().unwrap();
    }

    /// SPA-181: A sequence of create_node + create_edge where the whole
    /// transaction is dropped leaves the store entirely unchanged.
    #[test]
    fn dropped_write_tx_persists_no_edges() {
        use sparrowdb_storage::edge_store::EdgeStore;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        // First, commit two nodes so we have valid src/dst IDs.
        let (src, dst) = {
            let mut tx = db.begin_write().unwrap();
            let src = tx.create_node(0, &[]).unwrap();
            let dst = tx.create_node(0, &[]).unwrap();
            tx.commit().unwrap();
            (src, dst)
        };

        {
            let mut tx = db.begin_write().unwrap();
            // Stage an edge — should NOT be written to disk.
            tx.create_edge(src, dst, "KNOWS", std::collections::HashMap::new())
                .unwrap();
            // Drop WITHOUT calling commit().
        }

        // Verify no edge was persisted (delta log must be empty or absent).
        let delta = EdgeStore::open(dir.path(), sparrowdb_storage::edge_store::RelTableId(0))
            .and_then(|s| s.read_delta())
            .unwrap_or_default();
        assert_eq!(
            delta.len(),
            0,
            "dropped tx must not persist any edges (SPA-181)"
        );
    }

    /// SPA-181: The old UB transmute is gone.  Verify the write lock cycles
    /// correctly: acquire → use → drop → acquire again.
    #[test]
    fn write_guard_releases_lock_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        for _ in 0..5 {
            let tx = db.begin_write().expect("lock must be free");
            assert!(matches!(db.begin_write(), Err(Error::WriterBusy)));
            drop(tx);
        }
    }

    // ── SPA-171: GraphDb::stats() ─────────────────────────────────────────────

    #[test]
    fn stats_empty_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        let stats = db.stats().unwrap();
        assert_eq!(stats.edge_count, 0);
        assert_eq!(stats.node_count_per_label.len(), 0);
        assert_eq!(stats.wal_bytes, 0);
    }

    #[test]
    fn stats_after_writes() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        // Create two nodes and one edge via low-level API.
        let (src, dst) = {
            let mut tx = db.begin_write().unwrap();
            let a = tx.create_node(0, &[]).unwrap();
            let b = tx.create_node(0, &[]).unwrap();
            tx.commit().unwrap();
            (a, b)
        };
        {
            let mut tx = db.begin_write().unwrap();
            tx.create_edge(src, dst, "KNOWS", std::collections::HashMap::new())
                .unwrap();
            tx.commit().unwrap();
        }
        // Also create a labeled node via Cypher so catalog has a label entry.
        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        let stats = db.stats().unwrap();
        assert!(
            stats
                .node_count_per_label
                .get("Person")
                .copied()
                .unwrap_or(0)
                >= 1
        );
        assert_eq!(
            stats.edge_count, 1,
            "expected 1 edge, got {}",
            stats.edge_count
        );
        assert!(stats.wal_bytes > 0);
        assert!(stats.total_bytes >= stats.wal_bytes);
        assert!(stats.bytes_per_label.get("Person").copied().unwrap_or(0) > 0);
    }

    #[test]
    fn call_db_stats_cypher() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        db.execute("CREATE (n:Widget {name: 'w1'})").unwrap();
        let result = db.execute("CALL db.stats() YIELD metric, value").unwrap();
        assert_eq!(result.columns, vec!["metric", "value"]);
        assert!(!result.rows.is_empty());
        let metrics: std::collections::HashMap<_, _> = result
            .rows
            .iter()
            .filter_map(|row| match (&row[0], &row[1]) {
                (sparrowdb_execution::Value::String(m), sparrowdb_execution::Value::Int64(v)) => {
                    Some((m.clone(), *v))
                }
                _ => None,
            })
            .collect();
        assert!(metrics.contains_key("total_bytes"));
        assert!(metrics.contains_key("wal_bytes"));
        assert!(metrics.contains_key("edge_count"));
        assert!(metrics.contains_key("nodes.Widget"));
        assert!(metrics.contains_key("label_bytes.Widget"));
        assert!(metrics["total_bytes"] > 0);
    }

    #[test]
    fn stats_edge_count_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        let (n1, n2) = {
            let mut tx = db.begin_write().unwrap();
            let a = tx.create_node(0, &[]).unwrap();
            let b = tx.create_node(0, &[]).unwrap();
            tx.commit().unwrap();
            (a, b)
        };
        {
            let mut tx = db.begin_write().unwrap();
            tx.create_edge(n1, n2, "LINK", std::collections::HashMap::new())
                .unwrap();
            tx.commit().unwrap();
        }
        let before = db.stats().unwrap();
        assert!(
            before.edge_count > 0,
            "edge_count must be > 0 before checkpoint"
        );
        db.checkpoint().unwrap();
        let after = db.stats().unwrap();
        assert_eq!(after.edge_count, before.edge_count);
    }
}
