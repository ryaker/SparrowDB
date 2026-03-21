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
use sparrowdb_common::{col_id_of, EdgeId, Error, NodeId, Result, TxnId};
use sparrowdb_execution::{Engine, QueryResult};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::maintenance::MaintenanceEngine;
use sparrowdb_storage::node_store::{NodeStore, Value};
use sparrowdb_storage::wal::codec::{WalPayload, WalRecordKind};
use sparrowdb_storage::wal::writer::WalWriter;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
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
        props: Vec<(u32, Value)>,
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
    write_lock: Mutex<()>,
    /// MVCC version chains for property updates (snapshot-isolation support).
    versions: RwLock<VersionStore>,
    /// Per-node last-committed txn_id (write-write conflict detection, SPA-128).
    node_versions: RwLock<NodeVersions>,
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
                node_versions: RwLock::new(NodeVersions::default()),
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
        let snapshot_txn_id = self.inner.current_txn_id.load(Ordering::Acquire);
        let store = NodeStore::open(&self.inner.path)?;
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
        let _guard = self
            .inner
            .write_lock
            .try_lock()
            .map_err(|_| Error::WriterBusy)?;
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
    ///
    /// Returns [`Error::WriterBusy`] immediately if a [`WriteTx`] is currently
    /// active, rather than blocking indefinitely.  This prevents deadlocks on
    /// single-threaded callers and avoids unbounded waits on multi-threaded ones.
    pub fn optimize(&self) -> Result<()> {
        let _guard = self
            .inner
            .write_lock
            .try_lock()
            .map_err(|_| Error::WriterBusy)?;
        let catalog = Catalog::open(&self.inner.path)?;
        let node_store = NodeStore::open(&self.inner.path)?;
        let (rel_table_ids, n_nodes) =
            collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.optimize(&rel_table_ids, n_nodes)
    }

    /// Execute a read-only Cypher query and return the result.
    pub fn execute(&self, cypher: &str) -> Result<QueryResult> {
        let _span = info_span!("sparrowdb.query").entered();

        let engine = {
            let _open_span = info_span!("sparrowdb.open_engine").entered();
            let csr = open_csr_forward(&self.inner.path);
            Engine::new(
                NodeStore::open(&self.inner.path)?,
                Catalog::open(&self.inner.path)?,
                csr,
                &self.inner.path,
            )
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.execute").entered();
            engine.execute(cypher)?
        };

        tracing::debug!(rows = result.rows.len(), "query complete");
        Ok(result)
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
    /// Staged WAL mutation records to emit on commit.
    wal_mutations: Vec<WalMutation>,
    /// Set of node IDs written by this transaction (for MVCC conflict detection).
    dirty_nodes: HashSet<u64>,
    /// The committed txn_id at the time this WriteTx was opened (MVCC snapshot).
    snapshot_txn_id: u64,
    /// Held for the lifetime of this WriteTx; released on drop.
    _guard: MutexGuard<'db, ()>,
    committed: bool,
}

impl<'db> WriteTx<'db> {
    // ── Core node/property API (pre-Phase 7) ─────────────────────────────────

    /// Create a new node under `label_id` with the given properties.
    ///
    /// Returns the packed [`NodeId`].
    pub fn create_node(&mut self, label_id: u32, props: &[(u32, Value)]) -> Result<NodeId> {
        let node_id = self.store.create_node(label_id, props)?;
        self.dirty_nodes.insert(node_id.0);
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props: props.to_vec(),
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
                .map(|(_, raw)| Value::int64_from_u64(raw));
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
    pub fn create_label(&mut self, name: &str) -> Result<u16> {
        self.catalog.create_label(name)
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

        // Scan existing slots for a match.
        let hwm = self.store.hwm_for_label(label_id)?;
        for slot in 0..hwm {
            let candidate = NodeId((label_id as u64) << 32 | slot);
            if let Ok(stored) = self.store.get_node_raw(candidate, &col_ids) {
                let matches = col_kv.iter().all(|(_, col_id, want_val)| {
                    stored
                        .iter()
                        .find(|&&(c, _)| c == *col_id)
                        .map(|&(_, raw)| raw == want_val.to_u64())
                        .unwrap_or(false)
                });
                if matches {
                    return Ok(candidate);
                }
            }
        }

        // Not found — create a new node.
        let disk_props: Vec<(u32, Value)> = col_kv
            .iter()
            .map(|(_, col_id, v)| (*col_id, v.clone()))
            .collect();
        let node_id = self.store.create_node(label_id, &disk_props)?;
        self.dirty_nodes.insert(node_id.0);
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props: disk_props,
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
    /// in the delta log.  On success, tombstones col_0 of the node's slot
    /// with `u64::MAX` (a sentinel that callers must treat as "deleted") and
    /// queues a `NodeDelete` WAL record.
    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        // Check delta log for attached edges.
        let delta = EdgeStore::open(&self.inner.path, RelTableId(0))
            .and_then(|s| s.read_delta())
            .unwrap_or_default();
        if delta.iter().any(|r| r.src == node_id || r.dst == node_id) {
            return Err(Error::NodeHasEdges { node_id: node_id.0 });
        }

        // Tombstone col_0 with sentinel `u64::MAX`.
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;
        let col0 = self
            .inner
            .path
            .join("nodes")
            .join(label_id.to_string())
            .join("col_0.bin");
        if col0.exists() {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&col0)
                .map_err(Error::Io)?;
            f.seek(SeekFrom::Start(slot as u64 * 8))
                .map_err(Error::Io)?;
            f.write_all(&u64::MAX.to_le_bytes()).map_err(Error::Io)?;
        }

        self.dirty_nodes.insert(node_id.0);
        self.wal_mutations.push(WalMutation::NodeDelete { node_id });
        Ok(())
    }

    /// SPA-126: Create a directed edge `src → dst` with the given type.
    ///
    /// Appends to the edge delta log and queues an `EdgeCreate` WAL record.
    /// Registers the relationship type name in the catalog so that queries
    /// like `MATCH (a)-[:REL]->(b)` can resolve the type (SPA-158).
    /// Returns the new [`EdgeId`].
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

        // Register (or retrieve) the rel type in the catalog so that the
        // binder can resolve `[:REL_TYPE]` patterns in Cypher queries.
        // The catalog returns a u64 id; the storage layer uses a u32 newtype.
        let catalog_rel_id =
            self.catalog
                .get_or_create_rel_type_id(src_label_id, dst_label_id, rel_type)?;
        let rel_table_id = RelTableId(catalog_rel_id as u32);

        let mut es = EdgeStore::open(&self.inner.path, rel_table_id)?;
        let edge_id = es.create_edge(src, rel_table_id, dst)?;
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

    // ── Commit ───────────────────────────────────────────────────────────────

    /// Commit the transaction.
    ///
    /// 1. Detects write-write conflicts (SPA-128).
    /// 2. Flushes all staged `set_node_col` updates to disk.
    /// 3. Records before-images in the version chain at the previous `txn_id`,
    ///    preserving snapshot access for currently-open readers.
    /// 4. Atomically increments the global `current_txn_id`.
    /// 5. Records the new values in the version chain at the new `txn_id`.
    /// 6. Updates per-node version table for future conflict detection.
    /// 7. Appends WAL records for all mutations (SPA-127).
    pub fn commit(mut self) -> Result<TxnId> {
        // Step 1: MVCC conflict abort (SPA-128).
        self.detect_conflicts()?;

        // Drain staged updates.
        let updates: Vec<((u64, u32), StagedUpdate)> = self.write_buf.updates.drain().collect();

        // Step 2: Flush updates to disk.
        // Use `upsert_node_col` so that columns added by `set_property` (which
        // may not have been initialised during `create_node`) are created and
        // zero-padded automatically.
        for ((node_raw, col_id), ref staged) in &updates {
            self.store
                .upsert_node_col(NodeId(*node_raw), *col_id, &staged.new_value)?;
        }

        // Step 3+4: Increment txn_id with Release ordering.
        let new_id = self.inner.current_txn_id.fetch_add(1, Ordering::Release) + 1;

        // Step 5: Publish versions.
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

        // Step 6: Advance per-node version table.
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

        // Step 7: Append WAL mutation records (SPA-127).
        write_mutation_wal(&self.inner.path, new_id, &updates, &self.wal_mutations)?;

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
) -> Result<()> {
    // Nothing to record if there were no mutations or property updates.
    if updates.is_empty() && mutations.is_empty() {
        return Ok(());
    }

    let wal_dir = db_root.join("wal");
    let mut wal = WalWriter::open(&wal_dir)?;
    let txn = TxnId(txn_id);

    wal.append(WalRecordKind::Begin, txn, WalPayload::Empty)?;

    // NodeUpdate records for each property change in the write buffer.
    for ((node_raw, col_id), staged) in updates {
        let before_bytes = staged
            .before_image
            .as_ref()
            .map(|(_, v)| v.to_u64().to_le_bytes().to_vec())
            .unwrap_or_else(|| 0u64.to_le_bytes().to_vec());
        let after_bytes = staged.new_value.to_u64().to_le_bytes().to_vec();
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
            } => {
                let wal_props: Vec<(String, Vec<u8>)> = props
                    .iter()
                    .map(|(col_id, v)| (format!("col_{col_id}"), v.to_u64().to_le_bytes().to_vec()))
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

// ── Private helpers ───────────────────────────────────────────────────────────

fn collect_maintenance_params(
    catalog: &Catalog,
    node_store: &NodeStore,
    db_root: &Path,
) -> (Vec<u32>, u64) {
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
}
