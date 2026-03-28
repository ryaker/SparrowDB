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

use sparrowdb_catalog::catalog::{Catalog, LabelId};
use sparrowdb_common::{col_id_of, TxnId};
use sparrowdb_execution::Engine;

// ── Export / import module ────────────────────────────────────────────────────
pub mod export;
pub use export::{EdgeDump, GraphDump, NodeDump};

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

/// Per-rel-table edge property cache (SPA-261).
type EdgePropsCache = Arc<RwLock<HashMap<u32, HashMap<(u64, u64), Vec<(u32, u64)>>>>>;

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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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

    /// Garbage-collect version chains older than `min_active_txn_id`.
    ///
    /// For each key, retain:
    /// - All versions with `committed_at >= min_active_txn_id` (any active
    ///   reader pinned at exactly that snapshot needs them).
    /// - The single most-recent version with `committed_at < min_active_txn_id`
    ///   (the latest committed state visible to all readers, including future
    ///   ones with snapshot ≥ min_active_txn_id that arrive before the next
    ///   write).
    ///
    /// Returns the number of version entries pruned.
    fn gc(&mut self, min_active_txn_id: u64) -> usize {
        let mut pruned = 0usize;
        self.map.retain(|_, versions| {
            // Find the last version index whose committed_at < min_active_txn_id.
            // Entries [0..cutoff) are all older than the minimum active snapshot.
            let cutoff = versions.partition_point(|v| v.committed_at < min_active_txn_id);
            if cutoff > 1 {
                // Keep entry at index (cutoff - 1) as the "last visible before
                // min_active_txn_id" anchor; drop everything before it.
                let keep_from = cutoff - 1;
                pruned += keep_from;
                versions.drain(..keep_from);
            }
            // Drop entries whose chain is now empty (should not happen but be safe).
            !versions.is_empty()
        });
        pruned
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
        /// Human-readable (name, value) pairs for WAL observability and schema introspection.
        prop_entries: Vec<(String, Value)>,
    },
    /// A specific directed edge was deleted.
    EdgeDelete {
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
        /// Encoded (col_id, value_u64) pairs to persist in edge_props.bin.
        props: Vec<(u32, u64)>,
    },
    /// Delete an edge: rewrite the delta log excluding this record.
    EdgeDelete {
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
    /// Optional 32-byte encryption key (SPA-98).  Retained for future use
    /// (e.g., reopening the WAL writer after a full segment rotation triggered
    /// by an external checkpoint).  The key is encoded into the WAL writer at
    /// open time via [`WalWriter::open_encrypted`].
    #[allow(dead_code)]
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
    /// Cached label row counts (SPA-190).
    ///
    /// Maps `LabelId → node count` (high-water mark).  Built once at open
    /// time and refreshed after any write that creates or deletes nodes.
    /// Passed to `Engine::new_with_all_caches` to avoid per-label HWM disk
    /// reads on every read query.
    label_row_counts: RwLock<HashMap<LabelId, usize>>,
    /// Persistent WAL writer (SPA-210).
    ///
    /// Reused across transaction commits to avoid paying segment-scan and
    /// file-open overhead on every write.  Protected by a `Mutex` because
    /// only one writer exists at a time (the `write_locked` flag enforces
    /// SWMR, so contention is zero in practice).
    wal_writer: Mutex<WalWriter>,
    /// Shared edge-property cache (SPA-261).
    edge_props_cache: EdgePropsCache,
    /// Tracks active `ReadTx` snapshots for GC watermark computation.
    ///
    /// Maps `snapshot_txn_id → active_reader_count`.  A reader registers its
    /// snapshot on open and unregisters on drop.  The minimum key in this map
    /// is the oldest pinned snapshot; versions older than that watermark (and
    /// fully superseded by a newer committed version) can be pruned by GC.
    active_readers: Mutex<BTreeMap<u64, usize>>,
    /// Number of `WriteTx` commits since the last GC run.
    ///
    /// GC is triggered every [`GC_COMMIT_INTERVAL`] commits.
    commits_since_gc: AtomicU64,
}

/// Run GC on the version store every this many commits.
const GC_COMMIT_INTERVAL: u64 = 100;

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

impl DbInner {
    /// Invalidate the shared property-index cache.
    ///
    /// Clears the in-memory index (bumping its generation so stale Engine
    /// clones are detectable) and removes the persisted `prop_index.bin` file
    /// so a subsequent `open` does not load stale index data (SPA-286).
    fn invalidate_prop_index(&self) {
        self.prop_index
            .write()
            .expect("prop_index RwLock poisoned")
            .clear();
        sparrowdb_storage::property_index::PropertyIndex::remove_persisted(&self.path);
    }

    /// Persist the shared property-index cache to disk if new columns have
    /// been loaded since the last persist (SPA-286).
    ///
    /// Only writes when the index has grown, avoiding redundant I/O on queries
    /// that hit only already-persisted columns.
    fn persist_prop_index(&self) {
        if let Ok(mut guard) = self.prop_index.write() {
            guard.persist_if_grew(&self.path);
        }
    }
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
        let wal_dir = path.join("wal");
        let wal_writer = WalWriter::open(&wal_dir)?;
        let label_row_counts = RwLock::new(build_label_row_counts_from_disk(&catalog, path));
        // SPA-286: load persisted property index from disk if available,
        // otherwise start with an empty index (lazy rebuild from column files).
        //
        // Crash-safety: if the WAL has any prior activity (last_lsn > 0), a
        // previous session may have written to column files after persisting the
        // index — or may have crashed before removing the stale index file.
        // Remove the on-disk snapshot and start with an empty index so we never
        // serve stale data after a crash.
        if wal_writer.last_lsn().0 > 0 {
            sparrowdb_storage::property_index::PropertyIndex::remove_persisted(path);
        }
        let prop_index = sparrowdb_storage::property_index::PropertyIndex::load_from_dir(path)
            .unwrap_or_default();
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(0),
                write_locked: AtomicBool::new(false),
                versions: RwLock::new(VersionStore::default()),
                node_versions: RwLock::new(NodeVersions::default()),
                encryption_key: None,
                unique_constraints: RwLock::new(load_constraints(path)),
                prop_index: RwLock::new(prop_index),
                catalog: RwLock::new(catalog),
                csr_map: RwLock::new(open_csr_map(path)),
                label_row_counts,
                wal_writer: Mutex::new(wal_writer),
                edge_props_cache: Arc::new(RwLock::new(HashMap::new())),
                active_readers: Mutex::new(BTreeMap::new()),
                commits_since_gc: AtomicU64::new(0),
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
        let wal_dir = path.join("wal");
        let wal_writer = WalWriter::open_encrypted(&wal_dir, key)?;
        let label_row_counts = RwLock::new(build_label_row_counts_from_disk(&catalog, path));
        // SPA-286: load persisted property index (not encrypted; index data is
        // derived from column files which are also unencrypted on disk).
        //
        // Crash-safety: if the WAL has any prior activity (last_lsn > 0), a
        // previous session may have written to column files after persisting the
        // index — or may have crashed before removing the stale index file.
        // Remove the on-disk snapshot and start with an empty index so we never
        // serve stale data after a crash.
        if wal_writer.last_lsn().0 > 0 {
            sparrowdb_storage::property_index::PropertyIndex::remove_persisted(path);
        }
        let prop_index = sparrowdb_storage::property_index::PropertyIndex::load_from_dir(path)
            .unwrap_or_default();
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(0),
                write_locked: AtomicBool::new(false),
                versions: RwLock::new(VersionStore::default()),
                node_versions: RwLock::new(NodeVersions::default()),
                encryption_key: Some(key),
                unique_constraints: RwLock::new(load_constraints(path)),
                prop_index: RwLock::new(prop_index),
                catalog: RwLock::new(catalog),
                csr_map: RwLock::new(open_csr_map(path)),
                label_row_counts,
                wal_writer: Mutex::new(wal_writer),
                edge_props_cache: Arc::new(RwLock::new(HashMap::new())),
                active_readers: Mutex::new(BTreeMap::new()),
                commits_since_gc: AtomicU64::new(0),
            }),
        })
    }

    /// Persist the shared property-index cache to disk if new columns have
    /// been loaded since the last persist (SPA-286).
    ///
    /// Called after `write_back_prop_index` so the next `GraphDb::open` can
    /// load the index from disk instead of rebuilding from column files.
    /// Only writes when the index has grown (new `(label_id, col_id)` pairs
    /// were loaded), avoiding redundant I/O on repeated queries that hit the
    /// same already-persisted columns.
    fn persist_prop_index(&self) {
        self.inner.persist_prop_index();
    }

    /// Refresh the shared catalog cache from disk (SPA-188) and simultaneously
    /// rebuild the cached label row counts (SPA-190).
    ///
    /// Called after DDL operations and any write commit so the next read query
    /// sees the updated schema and fresh node counts without extra disk reads.
    fn invalidate_catalog(&self) {
        if let Ok(fresh) = Catalog::open(&self.inner.path) {
            // Rebuild row counts while we have the fresh catalog in hand —
            // no extra I/O to open it again.
            let new_counts = build_label_row_counts_from_disk(&fresh, &self.inner.path);
            *self.inner.catalog.write().expect("catalog RwLock poisoned") = fresh;
            *self
                .inner
                .label_row_counts
                .write()
                .expect("label_row_counts RwLock poisoned") = new_counts;
        }
    }

    /// Build a read-optimised `Engine` using all available shared caches
    /// (PropertyIndex, CSR map, label row counts) — SPA-190.
    ///
    /// All read query paths should use this instead of calling
    /// `Engine::new_with_cached_index` directly.
    fn build_read_engine(
        &self,
        store: NodeStore,
        catalog: Catalog,
        csrs: HashMap<u32, CsrForward>,
    ) -> Engine {
        Engine::new_with_all_caches(
            store,
            catalog,
            csrs,
            &self.inner.path,
            Some(&self.inner.prop_index),
            Some(self.cached_label_row_counts()),
            Some(Arc::clone(&self.inner.edge_props_cache)),
        )
    }

    /// Return a clone of the cached label row counts (SPA-190).
    fn cached_label_row_counts(&self) -> HashMap<LabelId, usize> {
        self.inner
            .label_row_counts
            .read()
            .expect("label_row_counts RwLock poisoned")
            .clone()
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
    /// Clear the entire edge-props cache (SPA-261).
    fn invalidate_edge_props_cache(&self) {
        self.inner
            .edge_props_cache
            .write()
            .expect("edge_props_cache poisoned")
            .clear();
    }

    /// Check whether `deadline` has passed, returning `Err(QueryTimeout)` if
    /// so.  Used by the deadline-aware mutation paths (fixes #310).
    #[inline]
    fn check_deadline(deadline: std::time::Instant) -> Result<()> {
        if std::time::Instant::now() >= deadline {
            return Err(Error::QueryTimeout);
        }
        Ok(())
    }

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
        // Register this reader's snapshot so GC knows the minimum safe watermark.
        {
            let mut ar = self
                .inner
                .active_readers
                .lock()
                .expect("active_readers lock poisoned");
            *ar.entry(snapshot_txn_id).or_insert(0) += 1;
        }
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
        let rel_tables = collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.checkpoint(&rel_tables)?;
        self.invalidate_csr_map();
        self.invalidate_edge_props_cache();
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
        let rel_tables = collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.optimize(&rel_tables)?;
        self.invalidate_csr_map();
        self.invalidate_edge_props_cache();
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
                self.build_read_engine(NodeStore::open(&self.inner.path)?, catalog_snap, csrs)
            };

            let result = {
                let _exec_span = info_span!("sparrowdb.execute").entered();
                engine.execute_statement(bound.inner)?
            };

            // Write lazily-loaded columns back to the shared cache.
            engine.write_back_prop_index(&self.inner.prop_index);
            self.persist_prop_index();

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
    /// Both read-only and mutation statements (`MATCH … SET`,
    /// `MATCH … DELETE`, `MATCH … CREATE`) honour the deadline.  The
    /// engine checks elapsed time in every hot scan/traversal loop and
    /// in mutation iteration loops (fixes #310).
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

        let deadline = std::time::Instant::now() + timeout;

        if Engine::is_mutation(&bound.inner) {
            // Thread the deadline into mutation code paths so that
            // MATCH…SET / MATCH…DELETE / MATCH…CREATE honour the caller's
            // timeout instead of running unbounded (fixes #310).
            return match bound.inner {
                Statement::Merge(ref m) => self.execute_merge(m),
                Statement::MatchMutate(ref mm) => self.execute_match_mutate_deadline(mm, deadline),
                Statement::MatchCreate(ref mc) => self.execute_match_create_deadline(mc, deadline),
                Statement::Create(ref c) => self.execute_create_standalone(c),
                _ => unreachable!(),
            };
        }

        let _span = info_span!("sparrowdb.query_with_timeout").entered();

        let mut engine = {
            let _open_span = info_span!("sparrowdb.open_engine").entered();
            let csrs = self.cached_csr_map();
            self.build_read_engine(NodeStore::open(&self.inner.path)?, catalog_snap, csrs)
                .with_deadline(deadline)
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.execute").entered();
            engine.execute_statement(bound.inner)?
        };

        engine.write_back_prop_index(&self.inner.prop_index);
        self.persist_prop_index();

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
        {
            let mut guard = self.inner.unique_constraints.write().unwrap();
            guard.insert((label_id, col_id));
            // Persist to disk so the constraint survives restart (issue #306).
            save_constraints(&self.inner.path, &guard)?;
        }
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

            // SPA-289: record secondary labels (labels[1..]) in the catalog
            // side table so that MATCH on secondary labels and labels(n)
            // return the full label set.
            if node.labels.len() > 1 {
                // Reject reserved __SO_ prefix on secondary labels.
                for secondary_name in node.labels.iter().skip(1) {
                    if is_reserved_label(secondary_name) {
                        return Err(reserved_label_error(secondary_name));
                    }
                }
                let mut secondary_label_ids: Vec<LabelId> = Vec::new();
                for secondary_name in node.labels.iter().skip(1) {
                    let sid = match tx.catalog.get_label(secondary_name)? {
                        Some(id) => id,
                        None => tx.catalog.create_label(secondary_name)?,
                    };
                    secondary_label_ids.push(sid);
                }
                tx.catalog
                    .record_secondary_labels(node_id, &secondary_label_ids)?;
            }

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
            // Convert rel_pat.props (AST PropEntries) to HashMap<String, Value>.
            let edge_props: HashMap<String, Value> = rel_pat
                .props
                .iter()
                .map(|pe| {
                    let val = match &pe.value {
                        sparrowdb_cypher::ast::Expr::Literal(lit) => literal_to_value(lit),
                        _ => {
                            return Err(sparrowdb_common::Error::InvalidArgument(format!(
                                "CREATE edge property '{}' must be a literal value",
                                pe.key
                            )))
                        }
                    };
                    Ok((pe.key.clone(), val))
                })
                .collect::<Result<HashMap<_, _>>>()?;
            tx.create_edge(src, dst, &rel_pat.rel_type, edge_props)?;
        }

        tx.commit()?;
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
            self.build_read_engine(NodeStore::open(&self.inner.path)?, catalog_snap, csrs)
                .with_params(params)
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.execute").entered();
            engine.execute_statement(bound.inner)?
        };

        engine.write_back_prop_index(&self.inner.prop_index);
        self.persist_prop_index();

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

        // SPA-219: `MATCH (a)-[r:REL]->(b) DELETE r` — edge delete path.
        if is_edge_delete_mutation(mm) {
            let edges = engine.scan_match_mutate_edges(mm)?;
            for (src, dst, rel_type) in edges {
                tx.delete_edge(src, dst, &rel_type)?;
            }
            tx.commit()?;
            self.invalidate_csr_map();
            self.invalidate_catalog();
            return Ok(QueryResult::empty(vec![]));
        }

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

        // SPA-219: `MATCH (a)-[r:REL]->(b) DELETE r` — edge delete path.
        if is_edge_delete_mutation(mm) {
            let edges = engine.scan_match_mutate_edges(mm)?;
            for (src, dst, rel_type) in edges {
                tx.delete_edge(src, dst, &rel_type)?;
            }
            tx.commit()?;
            self.invalidate_csr_map();
            self.invalidate_catalog();
            return Ok(QueryResult::empty(vec![]));
        }

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
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Deadline-aware variant of [`execute_match_mutate`] (fixes #310).
    ///
    /// Identical to the non-deadline version but the engine is configured
    /// with a deadline so that the MATCH scan checks elapsed time, and the
    /// SET / DELETE iteration loop checks the deadline between writes.
    fn execute_match_mutate_deadline(
        &self,
        mm: &sparrowdb_cypher::ast::MatchMutateStatement,
        deadline: std::time::Instant,
    ) -> Result<QueryResult> {
        let mut tx = self.begin_write()?;

        let csrs = self.cached_csr_map();
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            self.catalog_snapshot(),
            csrs,
            &self.inner.path,
        )
        .with_deadline(deadline);

        // SPA-219: edge delete path.
        if is_edge_delete_mutation(mm) {
            let edges = engine.scan_match_mutate_edges(mm)?;
            for (src, dst, rel_type) in edges {
                Self::check_deadline(deadline)?;
                tx.delete_edge(src, dst, &rel_type)?;
            }
            tx.commit()?;
            self.invalidate_csr_map();
            self.invalidate_catalog();
            return Ok(QueryResult::empty(vec![]));
        }

        let matching_ids = engine.scan_match_mutate(mm)?;
        if matching_ids.is_empty() {
            return Ok(QueryResult::empty(vec![]));
        }

        match &mm.mutation {
            sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                let sv = expr_to_value(value);
                for node_id in matching_ids {
                    Self::check_deadline(deadline)?;
                    tx.set_property(node_id, prop, sv.clone())?;
                }
            }
            sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                for node_id in matching_ids {
                    Self::check_deadline(deadline)?;
                    tx.delete_node(node_id)?;
                }
            }
        }

        tx.commit()?;
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
        //
        // Use build_read_engine so that the lazy property index and cached
        // label row counts are reused.  MATCH…CREATE only creates edges — it
        // never changes node property columns — so the cached data remains
        // valid across calls.
        let csrs = self.cached_csr_map();
        let engine = self.build_read_engine(
            NodeStore::open(&self.inner.path)?,
            self.catalog_snapshot(),
            csrs,
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

        // Write any newly-loaded index columns back to the shared cache so
        // subsequent MATCH…CREATE calls can reuse them without re-reading disk.
        // Node property columns are NOT changed by this operation (only edges
        // are created), so the cache remains valid.
        engine.write_back_prop_index(&self.inner.prop_index);
        self.persist_prop_index();

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
                let edge_props: HashMap<String, Value> = rel_pat
                    .props
                    .iter()
                    .map(|pe| {
                        let val = match &pe.value {
                            sparrowdb_cypher::ast::Expr::Literal(lit) => literal_to_value(lit),
                            _ => {
                                return Err(Error::InvalidArgument(format!(
                                    "CREATE edge property '{}' must be a literal value",
                                    pe.key
                                )))
                            }
                        };
                        Ok((pe.key.clone(), val))
                    })
                    .collect::<Result<HashMap<_, _>>>()?;
                tx.create_edge(src, dst, &rel_pat.rel_type, edge_props)?;
            }
        }

        tx.commit()?;
        // Note: do NOT call invalidate_prop_index() here — MATCH…CREATE only
        // creates edges, not node properties, so the cached column data remains
        // valid.  Only invalidate the catalog in case a new rel-type was registered.
        self.invalidate_catalog();
        Ok(QueryResult::empty(vec![]))
    }

    /// Deadline-aware variant of [`execute_match_create`] (fixes #310).
    fn execute_match_create_deadline(
        &self,
        mc: &sparrowdb_cypher::ast::MatchCreateStatement,
        deadline: std::time::Instant,
    ) -> Result<QueryResult> {
        let mut tx = self.begin_write()?;

        let csrs = self.cached_csr_map();
        let engine = self
            .build_read_engine(
                NodeStore::open(&self.inner.path)?,
                self.catalog_snapshot(),
                csrs,
            )
            .with_deadline(deadline);

        for (_, rel_pat, _) in &mc.create.edges {
            if is_reserved_label(&rel_pat.rel_type) {
                return Err(reserved_label_error(&rel_pat.rel_type));
            }
        }
        if mc.create.edges.is_empty() {
            return Err(Error::Unimplemented);
        }

        let matched_rows = engine.scan_match_create_rows(mc)?;
        engine.write_back_prop_index(&self.inner.prop_index);
        self.persist_prop_index();

        if matched_rows.is_empty() {
            return Ok(QueryResult::empty(vec![]));
        }

        for row in &matched_rows {
            Self::check_deadline(deadline)?;
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
                let edge_props: HashMap<String, Value> = rel_pat
                    .props
                    .iter()
                    .map(|pe| {
                        let val = match &pe.value {
                            sparrowdb_cypher::ast::Expr::Literal(lit) => literal_to_value(lit),
                            _ => {
                                return Err(Error::InvalidArgument(format!(
                                    "CREATE edge property '{}' must be a literal value",
                                    pe.key
                                )))
                            }
                        };
                        Ok((pe.key.clone(), val))
                    })
                    .collect::<Result<HashMap<_, _>>>()?;
                tx.create_edge(src, dst, &rel_pat.rel_type, edge_props)?;
            }
        }

        tx.commit()?;
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
                                ..
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
                // SPA-219: edge delete path.
                if is_edge_delete_mutation(mm) {
                    let edges = engine.scan_match_mutate_edges(mm)?;
                    for (src, dst, rel_type) in edges {
                        tx.delete_edge(src, dst, &rel_type)?;
                    }
                } else {
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
                // SPA-308: augment on-disk scan with nodes created earlier in this
                // batch (they live in pending_ops but are not yet visible to the
                // on-disk NodeStore reader used by the engine).
                let mut matched_rows = engine.scan_match_create_rows(mc)?;
                matched_rows.extend(augment_rows_with_pending(
                    &mc.match_patterns,
                    &tx.pending_ops,
                    &tx.catalog,
                    &matched_rows,
                )?);
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
                // SPA-308: augment on-disk scan with nodes created earlier in this
                // batch (they live in pending_ops but are not yet visible to the
                // on-disk NodeStore reader used by the engine).
                let mut matched_rows = engine.scan_match_merge_rel_rows(mm)?;
                matched_rows.extend(augment_rows_with_pending(
                    &mm.match_patterns,
                    &tx.pending_ops,
                    &tx.catalog,
                    &matched_rows,
                )?);
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
                                        src: ps, dst: pd, rel_table_id: prt, ..
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
        let ids_to_scan: Vec<u64> = rel_table_ids.iter().map(|(id, _, _, _)| *id).collect();
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
                // Deduplicate within this rel_type: delta log edges may also
                // appear in CSR after checkpoint.  Keying on (src, dst) is
                // sufficient because rel_type is constant for this iteration.
                let mut seen: std::collections::HashSet<(i64, i64)> =
                    std::collections::HashSet::new();

                // Delta log: stores full NodeId pairs directly.
                if let Ok(records) = store.read_delta() {
                    for rec in records {
                        let src = rec.src.0 as i64;
                        let dst = rec.dst.0 as i64;
                        if seen.insert((src, dst)) {
                            edge_triples.push((src, rel_type.clone(), dst));
                        }
                    }
                }

                // CSR: (src_slot, dst_slot) relative to label IDs.
                if let Ok(csr) = store.open_fwd() {
                    let n_nodes = csr.n_nodes();
                    for src_slot in 0..n_nodes {
                        let src_id = ((*src_label_id as u64) << 32 | src_slot) as i64;
                        for &dst_slot in csr.neighbors(src_slot) {
                            let dst_id = ((*dst_label_id as u64) << 32 | dst_slot) as i64;
                            if seen.insert((src_id, dst_id)) {
                                edge_triples.push((src_id, rel_type.clone(), dst_id));
                            }
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

    /// SPA-247: Convenience wrapper for upsert — find an existing node with
    /// `(label, match_key=match_value)` and update its properties, or create a
    /// new one if none exists.
    ///
    /// Opens a single-operation write transaction, calls
    /// [`WriteTx::merge_node_by_property`], and commits.
    ///
    /// Returns `(NodeId, created)`.
    pub fn merge_node_by_property(
        &self,
        label: &str,
        match_key: &str,
        match_value: &Value,
        properties: HashMap<String, Value>,
    ) -> Result<(NodeId, bool)> {
        let mut tx = self.begin_write()?;
        let result = tx.merge_node_by_property(label, match_key, match_value, properties)?;
        tx.commit()?;
        self.invalidate_catalog();
        Ok(result)
    }

    /// Convenience wrapper: remove the directed edge `src → dst` of `rel_type`.
    ///
    /// Opens a single-operation write transaction, calls
    /// [`WriteTx::delete_edge`], and commits.  Suitable for callers that need
    /// to remove a specific edge without managing a transaction explicitly.
    ///
    /// Returns [`Error::WriterBusy`] if another write transaction is already
    /// active; returns [`Error::InvalidArgument`] if the rel type or edge is
    /// not found.
    pub fn delete_edge(&self, src: NodeId, dst: NodeId, rel_type: &str) -> Result<()> {
        let mut tx = self.begin_write()?;
        tx.delete_edge(src, dst, rel_type)?;
        tx.commit()?;
        self.invalidate_csr_map();
        Ok(())
    }
}

/// Convenience wrapper — equivalent to [`GraphDb::open`].
pub fn open(path: &Path) -> Result<GraphDb> {
    GraphDb::open(path)
}

/// Migrate WAL segments from legacy v21 (CRC32 IEEE) to v2 (CRC32C Castagnoli).
///
/// Call this on a database path **before** opening it with [`GraphDb::open`].
/// The database must not be open by any other process.
///
/// This is safe to run on a database that is already at v2 — those segments are
/// simply skipped.  The migration is idempotent.
///
/// # Example
///
/// ```no_run
/// use std::path::Path;
/// let result = sparrowdb::migrate_wal(Path::new("/path/to/my.sparrow")).unwrap();
/// println!("Converted {} segments", result.segments_converted);
/// ```
pub fn migrate_wal(db_path: &Path) -> Result<sparrowdb_storage::wal::migrate::MigrationResult> {
    let wal_dir = db_path.join("wal");
    if !wal_dir.exists() {
        return Ok(sparrowdb_storage::wal::migrate::MigrationResult {
            segments_inspected: 0,
            segments_converted: 0,
            segments_skipped: 0,
            records_converted: 0,
        });
    }
    sparrowdb_storage::wal::migrate::migrate_wal(&wal_dir)
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

    /// Execute a read-only Cypher query against the pinned snapshot.
    ///
    /// ## Snapshot isolation
    ///
    /// The query sees exactly the committed state at the moment
    /// [`begin_read`](GraphDb::begin_read) was called.  Any writes committed
    /// after that point — even fully committed ones — are invisible until a
    /// new `ReadTx` is opened.
    ///
    /// ## Concurrency
    ///
    /// Multiple `ReadTx` handles may run `query` concurrently.  No write lock
    /// is acquired; only the shared read-paths of the catalog, CSR, and
    /// property-index caches are accessed.
    ///
    /// ## Mutation statements rejected
    ///
    /// Passing a mutation statement (`CREATE`, `MERGE`, `MATCH … SET`,
    /// `MATCH … DELETE`, `CHECKPOINT`, `OPTIMIZE`, etc.) returns
    /// [`Error::ReadOnly`].  Use [`GraphDb::execute`] for mutations.
    ///
    /// # Example
    /// ```no_run
    /// use sparrowdb::GraphDb;
    ///
    /// let db = GraphDb::open(std::path::Path::new("/tmp/g.sparrow")).unwrap();
    /// let tx = db.begin_read().unwrap();
    /// let result = tx.query("MATCH (n:Person) RETURN n.name").unwrap();
    /// println!("{} rows", result.rows.len());
    /// ```
    pub fn query(&self, cypher: &str) -> crate::Result<QueryResult> {
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;

        // Take a snapshot of the catalog from the shared cache (no disk I/O if
        // the catalog is already warm).
        let catalog_snap = self
            .inner
            .catalog
            .read()
            .expect("catalog RwLock poisoned")
            .clone();

        let bound = bind(stmt, &catalog_snap)?;

        // Reject any statement that would mutate state — ReadTx is read-only.
        if Engine::is_mutation(&bound.inner) {
            return Err(crate::Error::ReadOnly);
        }

        // Also reject DDL / maintenance statements.
        use sparrowdb_cypher::ast::Statement;
        match &bound.inner {
            Statement::Checkpoint | Statement::Optimize | Statement::CreateConstraint { .. } => {
                return Err(crate::Error::ReadOnly);
            }
            _ => {}
        }

        let _span = info_span!("sparrowdb.readtx.query").entered();

        let csrs = self
            .inner
            .csr_map
            .read()
            .expect("csr_map RwLock poisoned")
            .clone();

        let mut engine = {
            let _open_span = info_span!("sparrowdb.readtx.open_engine").entered();
            let row_counts = self
                .inner
                .label_row_counts
                .read()
                .expect("label_row_counts RwLock poisoned")
                .clone();
            Engine::new_with_all_caches(
                NodeStore::open(&self.inner.path)?,
                catalog_snap,
                csrs,
                &self.inner.path,
                Some(&self.inner.prop_index),
                Some(row_counts),
                Some(Arc::clone(&self.inner.edge_props_cache)),
            )
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.readtx.execute").entered();
            engine.execute_statement(bound.inner)?
        };

        // Write lazily-loaded columns back to the shared property-index cache
        // so subsequent queries benefit from warm column data.
        engine.write_back_prop_index(&self.inner.prop_index);
        // SPA-286: persist updated index to disk if new columns were loaded.
        self.inner.persist_prop_index();

        tracing::debug!(
            rows = result.rows.len(),
            snapshot_txn_id = self.snapshot_txn_id,
            "readtx query complete"
        );
        Ok(result)
    }
}

impl Drop for ReadTx {
    fn drop(&mut self) {
        // Unregister this reader's snapshot from the active-readers map.
        // When the count drops to zero the entry is removed so GC can advance
        // the watermark past this snapshot.
        if let Ok(mut ar) = self.inner.active_readers.lock() {
            if let std::collections::btree_map::Entry::Occupied(mut e) =
                ar.entry(self.snapshot_txn_id)
            {
                let count = e.get_mut();
                if *count <= 1 {
                    e.remove();
                } else {
                    *count -= 1;
                }
            }
        }
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

    /// SPA-247: Upsert a node — find an existing node with `(label, match_key=match_value)`
    /// and update its properties, or create a new one if none exists.
    ///
    /// Returns `(NodeId, created)` where `created` is `true` when a new node
    /// was inserted and `false` when an existing node was found and updated.
    ///
    /// The lookup scans both pending (in-transaction) nodes and committed
    /// on-disk nodes for a slot whose label matches and whose `match_key`
    /// column equals `match_value`.  On a hit the remaining `properties` are
    /// applied via [`set_property`]; on a miss a new node is created via
    /// [`merge_node`] with `match_key=match_value` merged into `properties`.
    pub fn merge_node_by_property(
        &mut self,
        label: &str,
        match_key: &str,
        match_value: &Value,
        properties: HashMap<String, Value>,
    ) -> Result<(NodeId, bool)> {
        let label_id: u32 = match self.catalog.get_label(label)? {
            Some(id) => id as u32,
            None => {
                // Label doesn't exist yet — no node can match. Create it.
                let mut full_props = properties;
                full_props.insert(match_key.to_string(), match_value.clone());
                let node_id = self.merge_node(label, full_props)?;
                return Ok((node_id, true));
            }
        };

        let match_col_id = fnv1a_col_id(match_key);
        let match_col_ids = vec![match_col_id];

        // Step 1: Check pending (in-transaction) nodes.
        for op in &self.pending_ops {
            if let PendingOp::NodeCreate {
                label_id: op_label_id,
                slot: op_slot,
                props: op_props,
            } = op
            {
                if *op_label_id == label_id {
                    let matches = op_props
                        .iter()
                        .find(|&&(c, _)| c == match_col_id)
                        .map(|(_, v)| v == match_value)
                        .unwrap_or(false);
                    if matches {
                        let node_id = NodeId((label_id as u64) << 32 | *op_slot as u64);
                        // Update remaining properties on the existing node.
                        for (k, v) in &properties {
                            self.set_property(node_id, k, v.clone())?;
                        }
                        return Ok((node_id, false));
                    }
                }
            }
        }

        // Step 2: Scan on-disk committed nodes.
        let disk_hwm = self.store.disk_hwm_for_label(label_id)?;
        for slot in 0..disk_hwm {
            let candidate = NodeId((label_id as u64) << 32 | slot);
            if let Ok(stored) = self.store.get_node_raw(candidate, &match_col_ids) {
                let matches = stored
                    .iter()
                    .find(|&&(c, _)| c == match_col_id)
                    .map(|&(_, raw)| self.store.decode_raw_value(raw) == *match_value)
                    .unwrap_or(false);
                if matches {
                    // Update remaining properties on the existing node.
                    for (k, v) in &properties {
                        self.set_property(candidate, k, v.clone())?;
                    }
                    return Ok((candidate, false));
                }
            }
        }

        // Step 3: Not found — create new node with match_key included.
        let mut full_props = properties;
        full_props.insert(match_key.to_string(), match_value.clone());
        let node_id = self.merge_node(label, full_props)?;
        Ok((node_id, true))
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
    /// in the delta log or checkpointed CSR files (SPA-304).  On success,
    /// queues a `NodeDelete` WAL record and
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
            let store = EdgeStore::open(&self.inner.path, RelTableId(rel_id));

            // Check delta log (un-checkpointed edges).
            if let Ok(ref s) = store {
                let delta = s.read_delta().unwrap_or_default();
                if delta.iter().any(|r| r.src == node_id || r.dst == node_id) {
                    return Err(Error::NodeHasEdges { node_id: node_id.0 });
                }
            }

            // SPA-304: Check CSR forward file — the node may be a *source* of
            // checkpointed edges that are no longer in the delta log.
            if let Ok(ref s) = store {
                if let Ok(csr) = s.open_fwd() {
                    if !csr.neighbors(node_id.0).is_empty() {
                        return Err(Error::NodeHasEdges { node_id: node_id.0 });
                    }
                }
            }

            // SPA-304: Check CSR backward file — the node may be a *destination*
            // of checkpointed edges.
            if let Ok(ref s) = store {
                if let Ok(csr) = s.open_bwd() {
                    if !csr.predecessors(node_id.0).is_empty() {
                        return Err(Error::NodeHasEdges { node_id: node_id.0 });
                    }
                }
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
        props: HashMap<String, Value>,
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

        // Convert HashMap<String, Value> props to (col_id, value_u64) pairs
        // using the canonical FNV-1a col_id derivation so read and write agree.
        // SPA-229: use NodeStore::encode_value (not val.to_u64()) so that
        // Value::Float is stored via f64::to_bits() in the heap rather than
        // panicking with "cannot be inline-encoded".
        let encoded_props: Vec<(u32, u64)> = props
            .iter()
            .map(|(name, val)| -> Result<(u32, u64)> {
                Ok((col_id_of(name), self.store.encode_value(val)?))
            })
            .collect::<Result<Vec<_>>>()?;

        // Human-readable entries for WAL schema introspection.
        let prop_entries: Vec<(String, Value)> = props.into_iter().collect();

        // Buffer the edge append — do NOT write to disk yet.
        self.pending_ops.push(PendingOp::EdgeCreate {
            src,
            dst,
            rel_table_id,
            props: encoded_props,
        });
        self.wal_mutations.push(WalMutation::EdgeCreate {
            edge_id,
            src,
            dst,
            rel_type: rel_type.to_string(),
            prop_entries,
        });
        Ok(edge_id)
    }

    /// Delete the directed edge `src → dst` with the given relationship type.
    ///
    /// Resolves the relationship type to a `RelTableId` via the catalog, then
    /// buffers an `EdgeDelete` operation.  At commit time the edge is excised
    /// from the on-disk delta log.
    ///
    /// Returns [`Error::InvalidArgument`] if the relationship type is not
    /// registered in the catalog, or if no matching edge record exists in the
    /// delta log for the resolved table.
    ///
    /// Unblocks `SparrowOntology::init(force=true)` which needs to remove all
    /// existing edges before re-seeding the ontology graph.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn delete_edge(&mut self, src: NodeId, dst: NodeId, rel_type: &str) -> Result<()> {
        let src_label_id = (src.0 >> 32) as u16;
        let dst_label_id = (dst.0 >> 32) as u16;

        // Resolve the rel type in the catalog.  We do not create a new entry —
        // deleting a non-existent rel type is always an error.
        // The catalog's RelTableId is u64; EdgeStore's is RelTableId(u32).
        let catalog_rel_id = self
            .catalog
            .get_rel_table(src_label_id, dst_label_id, rel_type)?
            .ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "relationship type '{}' not found in catalog for labels ({src_label_id}, {dst_label_id})",
                    rel_type
                ))
            })?;
        let rel_table_id = RelTableId(catalog_rel_id as u32);

        // If the edge was created in this same transaction (not yet committed),
        // cancel the create rather than scheduling a delete.
        let buffered_pos = self.pending_ops.iter().position(|op| {
            matches!(
                op,
                PendingOp::EdgeCreate {
                    src: os,
                    dst: od,
                    rel_table_id: ort,
                    ..
                } if *os == src && *od == dst && *ort == rel_table_id
            )
        });

        if let Some(pos) = buffered_pos {
            // Remove the buffered create and its corresponding WAL mutation.
            self.pending_ops.remove(pos);
            // The WalMutation vec is parallel to pending_ops only for structural
            // ops; find and remove the matching EdgeCreate entry.
            if let Some(wpos) = self.wal_mutations.iter().position(|m| {
                matches!(m, WalMutation::EdgeCreate { src: ms, dst: md, .. } if *ms == src && *md == dst)
            }) {
                self.wal_mutations.remove(wpos);
            }
            return Ok(());
        }

        // Edge is on disk — schedule the deletion for commit time.
        self.pending_ops.push(PendingOp::EdgeDelete {
            src,
            dst,
            rel_table_id,
        });
        self.wal_mutations.push(WalMutation::EdgeDelete {
            src,
            dst,
            rel_type: rel_type.to_string(),
        });
        Ok(())
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
            &self.inner.wal_writer,
            new_id,
            &updates,
            &self.wal_mutations,
        )?;

        // Step 5: Apply buffered structural operations to disk (SPA-181).
        // WAL is already durable at this point; a crash here is safe.
        //
        // SPA-212 (write-amplification fix): NodeCreate ops are collected into a
        // single batch and written via `batch_write_node_creates`, which opens
        // each (label_id, col_id) file only once regardless of how many nodes
        // share that column.  This reduces file-open syscalls from O(nodes×cols)
        // to O(labels×cols) per transaction commit.
        let mut col_writes: Vec<(u32, u32, u32, u64, bool)> = Vec::new();
        // All (label_id, slot) pairs for created nodes — needed for HWM
        // advancement, even when a node has zero properties.
        let mut node_slots: Vec<(u32, u32)> = Vec::new();

        for op in self.pending_ops.drain(..) {
            match op {
                PendingOp::NodeCreate {
                    label_id,
                    slot,
                    props,
                } => {
                    // Track this node's (label_id, slot) for HWM advancement.
                    node_slots.push((label_id, slot));
                    // Encode each property value and push (label_id, col_id,
                    // slot, raw_u64, is_present) into the batch buffer.
                    for (col_id, ref val) in props {
                        let raw = self.store.encode_value(val)?;
                        col_writes.push((label_id, col_id, slot, raw, true));
                    }
                }
                PendingOp::NodeDelete { node_id } => {
                    self.store.tombstone_node(node_id)?;
                }
                PendingOp::EdgeCreate {
                    src,
                    dst,
                    rel_table_id,
                    props,
                } => {
                    let mut es = EdgeStore::open(&self.inner.path, rel_table_id)?;
                    es.create_edge(src, rel_table_id, dst)?;
                    // Persist edge properties keyed by (src_slot, dst_slot) so
                    // that reads work correctly after CHECKPOINT (SPA-240).
                    if !props.is_empty() {
                        let src_slot = src.0 & 0xFFFF_FFFF;
                        let dst_slot = dst.0 & 0xFFFF_FFFF;
                        for (col_id, value) in &props {
                            es.set_edge_prop(src_slot, dst_slot, *col_id, *value)?;
                        }
                        // SPA-261: invalidate cached edge props for this rel table.
                        self.inner
                            .edge_props_cache
                            .write()
                            .expect("edge_props_cache poisoned")
                            .remove(&rel_table_id.0);
                    }
                }
                PendingOp::EdgeDelete {
                    src,
                    dst,
                    rel_table_id,
                } => {
                    let mut es = EdgeStore::open(&self.inner.path, rel_table_id)?;
                    es.delete_edge(src, dst)?;
                }
            }
        }

        // Flush all NodeCreate column writes in one batched call.
        // O(labels × cols) file opens instead of O(nodes × cols).
        self.store
            .batch_write_node_creates(col_writes, &node_slots)?;

        // Step 5b: Persist HWMs for all labels that received new nodes in Step 5.
        //
        // batch_write_node_creates() advances the in-memory HWM and marks
        // labels dirty (hwm_dirty).  We flush all dirty HWMs here — once per
        // commit — avoiding an fsync storm during bulk imports (SPA-217
        // regression fix).  Crash safety is preserved: the WAL record written
        // in Step 4 is already durable; on crash-recovery, the WAL replayer
        // re-applies all NodeCreate ops and re-advances the HWM.
        self.store.flush_hwms()?;

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

        // Step 9b: Periodically garbage-collect the version store (issue #307).
        //
        // Every GC_COMMIT_INTERVAL commits we compute the minimum active reader
        // snapshot and prune fully-superseded old versions below that watermark.
        // This bounds VersionStore memory to O(live_keys × active_reader_span)
        // rather than O(live_keys × total_write_count).
        {
            let prev = self.inner.commits_since_gc.fetch_add(1, Ordering::Relaxed);
            if prev + 1 >= GC_COMMIT_INTERVAL {
                self.inner.commits_since_gc.store(0, Ordering::Relaxed);
                // Compute min active snapshot watermark.
                let min_active = {
                    let ar = self
                        .inner
                        .active_readers
                        .lock()
                        .expect("active_readers lock poisoned");
                    // If there are no active readers, every version older than
                    // the current committed txn_id is safe to prune (keeping
                    // only the most recent).  Use current_txn_id + 1 so the
                    // gc() "last version before watermark" logic retains the
                    // latest version for future readers.
                    ar.keys().copied().next().unwrap_or(new_id + 1)
                };
                let pruned = self
                    .inner
                    .versions
                    .write()
                    .expect("version lock poisoned")
                    .gc(min_active);
                if pruned > 0 {
                    tracing::debug!(pruned, min_active, "versionstore gc complete");
                }
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
        // Also rebuild the label-row-count cache (SPA-190) from the freshly
        // opened catalog to avoid a second I/O trip.
        if let Ok(fresh) = Catalog::open(&self.inner.path) {
            let new_counts = build_label_row_counts_from_disk(&fresh, &self.inner.path);
            *self.inner.catalog.write().expect("catalog RwLock poisoned") = fresh;
            *self
                .inner
                .label_row_counts
                .write()
                .expect("label_row_counts RwLock poisoned") = new_counts;
        }

        // Step 12: Invalidate the shared property-index cache (SPA-259).
        //
        // This is the single canonical invalidation point (#312).
        // Callers using WriteTx directly (without GraphDb::execute) still
        // get correct cache invalidation.
        self.inner.invalidate_prop_index();

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
/// Uses the persistent `wal_writer` cached in [`DbInner`] — no segment scan
/// or file-open overhead on each call (SPA-210).  Emits:
/// - `Begin`
/// - `NodeUpdate` for each staged property change in `updates`
/// - `NodeCreate` / `NodeUpdate` / `NodeDelete` / `EdgeCreate` for each structural mutation
/// - `Commit`
/// - fsync
fn write_mutation_wal(
    wal_writer: &Mutex<WalWriter>,
    txn_id: u64,
    updates: &[((u64, u32), StagedUpdate)],
    mutations: &[WalMutation],
) -> Result<()> {
    // Nothing to record if there were no mutations or property updates.
    if updates.is_empty() && mutations.is_empty() {
        return Ok(());
    }

    let mut wal = wal_writer.lock().unwrap_or_else(|e| e.into_inner());
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
                prop_entries,
            } => {
                let wal_props: Vec<(String, Vec<u8>)> = prop_entries
                    .iter()
                    .map(|(name, val)| (name.clone(), value_to_wal_bytes(val)))
                    .collect();
                wal.append(
                    WalRecordKind::EdgeCreate,
                    txn,
                    WalPayload::EdgeCreate {
                        edge_id: edge_id.0,
                        src: src.0,
                        dst: dst.0,
                        rel_type: rel_type.clone(),
                        props: wal_props,
                    },
                )?;
            }
            WalMutation::EdgeDelete { src, dst, rel_type } => {
                wal.append(
                    WalRecordKind::EdgeDelete,
                    txn,
                    WalPayload::EdgeDelete {
                        src: src.0,
                        dst: dst.0,
                        rel_type: rel_type.clone(),
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
/// Returns `true` when the `DELETE` clause variable in a `MatchMutateStatement`
/// refers to a relationship pattern variable rather than a node variable.
///
/// Used to route `MATCH (a)-[r:REL]->(b) DELETE r` to the edge-delete path
/// instead of the node-delete path.
fn is_edge_delete_mutation(mm: &sparrowdb_cypher::ast::MatchMutateStatement) -> bool {
    let sparrowdb_cypher::ast::Mutation::Delete { var } = &mm.mutation else {
        return false;
    };
    mm.match_patterns
        .iter()
        .any(|p| p.rels.iter().any(|r| !r.var.is_empty() && &r.var == var))
}

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

// ── Intra-batch visibility helpers (SPA-308) ─────────────────────────────────

/// Collect `NodeId`s from `pending_ops` that match `label_id` and all
/// property filters in `node_props`.
///
/// `pending_ops` stores props as `Vec<(col_id: u32, Value)>`.
/// The prop entries in `node_props` carry key names; we derive the same
/// `col_id` via [`col_id_of`] to make the comparison.
fn pending_candidates_for(
    pending_ops: &[PendingOp],
    label_id: u32,
    node_props: &[sparrowdb_cypher::ast::PropEntry],
) -> Vec<NodeId> {
    pending_ops
        .iter()
        .filter_map(|op| {
            let PendingOp::NodeCreate {
                label_id: op_lid,
                slot,
                props: op_props,
            } = op
            else {
                return None;
            };
            if *op_lid != label_id {
                return None;
            }
            // All pattern props must match a corresponding pending prop.
            let all_match = node_props.iter().all(|pe| {
                let wanted_col = col_id_of(&pe.key);
                let wanted_val = expr_to_value(&pe.value);
                op_props
                    .iter()
                    .any(|&(c, ref v)| c == wanted_col && *v == wanted_val)
            });
            if all_match {
                Some(NodeId((label_id as u64) << 32 | *slot as u64))
            } else {
                None
            }
        })
        .collect()
}

/// Supplement a set of already-matched rows with rows that include
/// nodes created earlier in the same batch (`pending_ops`).
///
/// For each named node variable in `patterns`, the function scans
/// `pending_ops` for `NodeCreate` entries whose label and properties match
/// the pattern.  Any pending candidates not already present in `existing_rows`
/// are cross-joined with on-disk candidates for the other variables to form
/// new rows.
///
/// This is the fix for issue #308: `MATCH...MERGE` statements executed inside
/// `execute_batch` could not see nodes created by earlier `CREATE` statements
/// in the same batch because the Engine only reads committed on-disk state.
fn augment_rows_with_pending(
    patterns: &[sparrowdb_cypher::ast::PathPattern],
    pending_ops: &[PendingOp],
    catalog: &sparrowdb_catalog::catalog::Catalog,
    existing_rows: &[HashMap<String, NodeId>],
) -> Result<Vec<HashMap<String, NodeId>>> {
    // Collect the named node variables present in the patterns.
    // Patterns with relationships (multi-node path patterns) are not yet
    // augmented — only independent node-only patterns are handled here.
    // That covers the most common batch scenario:
    //   CREATE (:A {k:1})
    //   CREATE (:B {k:1})
    //   MATCH (a:A {k:1}), (b:B {k:1}) MERGE (a)-[:R]->(b)
    let mut var_candidates: HashMap<String, Vec<NodeId>> = HashMap::new();
    for pat in patterns {
        // Only process simple node-only patterns (no relationships in the path).
        if pat.rels.is_empty() {
            for node_pat in &pat.nodes {
                if node_pat.var.is_empty() {
                    continue;
                }
                if var_candidates.contains_key(&node_pat.var) {
                    continue;
                }
                let label = node_pat.labels.first().cloned().unwrap_or_default();
                let label_id: u32 = match catalog.get_label(&label)? {
                    Some(id) => id as u32,
                    None => {
                        // Label not registered at all — no pending nodes either.
                        var_candidates.insert(node_pat.var.clone(), vec![]);
                        continue;
                    }
                };
                let pending = pending_candidates_for(pending_ops, label_id, &node_pat.props);
                var_candidates.insert(node_pat.var.clone(), pending);
            }
        }
    }

    if var_candidates.is_empty() {
        return Ok(vec![]);
    }

    // Collect the set of NodeIds already present in existing_rows for each
    // variable so we can skip duplicates.
    let mut already_seen: HashMap<String, HashSet<NodeId>> = HashMap::new();
    for row in existing_rows {
        for (var, nid) in row {
            already_seen.entry(var.clone()).or_default().insert(*nid);
        }
    }

    // Build new rows: cross-join the per-variable pending candidates,
    // but only include a row if at least one variable has a pending candidate
    // not already seen in existing_rows.  This avoids duplicating rows that
    // the engine already returned.
    let vars: Vec<String> = var_candidates.keys().cloned().collect();
    let mut new_rows: Vec<HashMap<String, NodeId>> = vec![HashMap::new()];

    for var in &vars {
        let candidates = var_candidates.get(var).map(Vec::as_slice).unwrap_or(&[]);
        if candidates.is_empty() {
            // No pending candidates for this variable; keep current partial rows
            // alive by not expanding (they will be filtered below).
            continue;
        }
        let mut expanded: Vec<HashMap<String, NodeId>> = Vec::new();
        for partial in &new_rows {
            for &cand in candidates {
                let mut row = partial.clone();
                row.insert(var.clone(), cand);
                expanded.push(row);
            }
        }
        new_rows = expanded;
    }

    // Filter out rows that are fully contained in existing_rows (all variables
    // match a node already seen from the on-disk scan).
    let added: Vec<HashMap<String, NodeId>> = new_rows
        .into_iter()
        .filter(|row| {
            if row.is_empty() {
                return false;
            }
            // The row is "new" if ANY of its node assignments is a pending node
            // not already present in existing_rows for that variable.
            row.iter().any(|(var, nid)| {
                !already_seen
                    .get(var)
                    .map(|s| s.contains(nid))
                    .unwrap_or(false)
            })
        })
        .collect();

    Ok(added)
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
) -> Vec<(u32, u64)> {
    // SPA-185: collect all registered rel table IDs from the catalog instead
    // of hardcoding [0].  This ensures every per-type edge store is checkpointed.
    // Always include table-0 so that any edges written before the catalog had
    // entries (legacy data or pre-SPA-185 databases) are also checkpointed.
    let rel_table_entries = catalog.list_rel_table_ids();
    // Build (rel_table_id, src_label_id, dst_label_id) triples.
    let mut rel_triples: Vec<(u32, Option<u16>, Option<u16>)> = rel_table_entries
        .iter()
        .map(|(id, src, dst, _)| (*id as u32, Some(*src), Some(*dst)))
        .collect();
    // Always include the legacy table-0 slot.  Dedup if already present.
    if !rel_triples.iter().any(|(id, _, _)| *id == 0u32) {
        rel_triples.push((0u32, None, None));
    }

    // Fallback: max HWM across all known labels (for legacy table-0 or when
    // label HWMs are not available from the catalog).
    let global_max_hwm: u64 = catalog
        .list_labels()
        .unwrap_or_default()
        .iter()
        .map(|(label_id, _name)| node_store.hwm_for_label(*label_id as u32).unwrap_or(0))
        .max()
        .unwrap_or(0);

    // For each rel table, compute n_nodes as max(hwm(src_label), hwm(dst_label)).
    // This replaces the old sum-of-all-labels approach that overcounted (#309).
    rel_triples
        .iter()
        .map(|&(rel_id, src_label, dst_label)| {
            // Per-label HWM: max of src and dst label HWMs.
            // Query the node store directly -- labels may not be formally registered
            // in the catalog (e.g. low-level create_node by label_id).
            let hwm_n_nodes = match (src_label, dst_label) {
                (Some(src), Some(dst)) => {
                    let src_hwm = node_store.hwm_for_label(src as u32).unwrap_or(0);
                    let dst_hwm = node_store.hwm_for_label(dst as u32).unwrap_or(0);
                    src_hwm.max(dst_hwm)
                }
                // Legacy table-0 or unknown: use global max.
                _ => global_max_hwm,
            };

            // Also scan this rel table's delta records for the maximum slot index,
            // so the CSR bounds check passes even when edges were inserted without
            // going through the node-store API.
            let delta_max: u64 = EdgeStore::open(db_root, RelTableId(rel_id))
                .ok()
                .and_then(|s| s.read_delta().ok())
                .map(|records| {
                    records
                        .iter()
                        .flat_map(|r| {
                            // Strip label bits -- CSR needs slot indices only.
                            let src_slot = r.src.0 & 0xFFFF_FFFF;
                            let dst_slot = r.dst.0 & 0xFFFF_FFFF;
                            [src_slot, dst_slot].into_iter()
                        })
                        .max()
                        .map(|max_slot| max_slot + 1)
                        .unwrap_or(0)
                })
                .unwrap_or(0);

            let n_nodes = hwm_n_nodes.max(delta_max).max(1);
            (rel_id, n_nodes)
        })
        .collect()
}

// Open all per-type CSR forward files that exist on disk, keyed by rel_table_id.
//
// SPA-185: replaces the old `open_csr_forward` that only opened `RelTableId(0)`.
// The catalog is used to discover all registered rel types.
// ── Constraint persistence helpers (issue #306) ─────────────────────────────

const CONSTRAINTS_FILE: &str = "constraints.bin";

/// Serialize the unique-constraint set to `<db_root>/constraints.bin`.
///
/// Format: `[count: u32 LE][label_id: u32 LE, col_id: u32 LE]*`
fn save_constraints(db_root: &Path, constraints: &HashSet<(u32, u32)>) -> Result<()> {
    use std::io::Write;
    let path = db_root.join(CONSTRAINTS_FILE);
    let mut buf = Vec::with_capacity(4 + constraints.len() * 8);
    buf.extend_from_slice(&(constraints.len() as u32).to_le_bytes());
    for &(label_id, col_id) in constraints {
        buf.extend_from_slice(&label_id.to_le_bytes());
        buf.extend_from_slice(&col_id.to_le_bytes());
    }
    // Atomic write: write to a temp file then rename so a crash mid-write
    // never leaves a truncated constraints file.
    let tmp_path = db_root.join("constraints.bin.tmp");
    let mut f = std::fs::File::create(&tmp_path)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load the unique-constraint set from `<db_root>/constraints.bin`.
///
/// Returns an empty set if the file does not exist (fresh database).
fn load_constraints(db_root: &Path) -> HashSet<(u32, u32)> {
    let path = db_root.join(CONSTRAINTS_FILE);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(_) => return HashSet::new(),
    };
    if data.len() < 4 {
        return HashSet::new();
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let expected_len = 4 + count * 8;
    if data.len() < expected_len {
        return HashSet::new();
    }
    let mut set = HashSet::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 8;
        let label_id = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
        let col_id =
            u32::from_le_bytes([data[off + 4], data[off + 5], data[off + 6], data[off + 7]]);
        set.insert((label_id, col_id));
    }
    set
}

/// Build a `LabelId → node count` map by reading each label's HWM from disk
/// (SPA-190).  Called at `GraphDb::open()` and after node-mutating writes.
fn build_label_row_counts_from_disk(catalog: &Catalog, db_root: &Path) -> HashMap<LabelId, usize> {
    let store = match NodeStore::open(db_root) {
        Ok(s) => s,
        Err(_) => return HashMap::new(),
    };
    catalog
        .list_labels()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(lid, _name)| {
            let hwm = store.hwm_for_label(lid as u32).unwrap_or(0);
            if hwm > 0 {
                Some((lid, hwm as usize))
            } else {
                None
            }
        })
        .collect()
}

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

    /// Issue #309: checkpoint must use per-label HWM, not the sum of all labels.
    ///
    /// When a graph has two labels (e.g. Person with 5 nodes, City with 3 nodes),
    /// the CSR for a (:Person)-[:LIVES_IN]->(:City) rel table should be sized to
    /// max(5, 3) = 5, not 5 + 3 = 8.  The overcounted sum wastes memory and can
    /// produce incorrect degree arrays.
    #[test]
    fn checkpoint_uses_per_label_hwm_not_sum() {
        use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        // Create nodes with two distinct labels.
        // Label 0 ("Person"): 5 nodes  ->  HWM = 5
        // Label 1 ("City"):   3 nodes  ->  HWM = 3
        {
            let mut tx = db.begin_write().unwrap();
            for _ in 0..5 {
                tx.create_node(0, &[]).unwrap();
            }
            for _ in 0..3 {
                tx.create_node(1, &[]).unwrap();
            }
            // Create edges from Person -> City (LIVES_IN).
            // Person slot 0 -> City slot 0
            // Person slot 1 -> City slot 1
            let person_0 = sparrowdb_common::NodeId(0);
            let person_1 = sparrowdb_common::NodeId(1);
            let city_0 = sparrowdb_common::NodeId(1u64 << 32);
            let city_1 = sparrowdb_common::NodeId((1u64 << 32) | 1);
            tx.create_edge(
                person_0,
                city_0,
                "LIVES_IN",
                std::collections::HashMap::new(),
            )
            .unwrap();
            tx.create_edge(
                person_1,
                city_1,
                "LIVES_IN",
                std::collections::HashMap::new(),
            )
            .unwrap();
            tx.commit().unwrap();
        }

        // Checkpoint should succeed and produce correctly sized CSR.
        db.checkpoint()
            .expect("checkpoint with multi-label graph must succeed");

        // Verify the CSR n_nodes is max(5, 3) = 5, not 5 + 3 = 8.
        // Find the LIVES_IN rel table and open its CSR.
        let catalog = sparrowdb_catalog::catalog::Catalog::open(dir.path()).unwrap();
        let rel_tables = catalog.list_rel_table_ids();
        let lives_in = rel_tables
            .iter()
            .find(|(_, _, _, rt)| rt == "LIVES_IN")
            .expect("LIVES_IN rel table must exist");
        let store = EdgeStore::open(dir.path(), RelTableId(lives_in.0 as u32)).unwrap();
        let fwd = store
            .open_fwd()
            .expect("CSR forward must exist after checkpoint");

        // The key assertion: n_nodes should be max(5, 3) = 5, not 8.
        assert_eq!(
            fwd.n_nodes(),
            5,
            "CSR n_nodes should be max of per-label HWMs (5), not sum (8)"
        );

        // Verify the edges are actually correct.
        let neighbors_0 = fwd.neighbors(0);
        let neighbors_1 = fwd.neighbors(1);
        assert_eq!(neighbors_0, &[0], "Person 0 -> City 0 (slot 0)");
        assert_eq!(neighbors_1, &[1], "Person 1 -> City 1 (slot 1)");
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
        // SPA-210: WalWriter is opened eagerly at GraphDb::open time, creating
        // the initial segment file (1-byte version header).  wal_bytes may be
        // a small non-zero value before any commits.
        // (Previously the WAL directory was only created on first commit.)
        let _ = stats.wal_bytes; // accept any value — presence is fine
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

    /// Regression test for #310: MATCH…SET must respect the deadline passed
    /// to `execute_with_timeout`.  We insert enough nodes that the scan +
    /// mutation loop will exceed a near-zero timeout, then assert that the
    /// call returns `Error::QueryTimeout` instead of silently completing.
    #[test]
    fn mutation_respects_timeout() {
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        // Insert a batch of nodes so the MATCH…SET has work to do.
        for i in 0..500 {
            db.execute(&format!("CREATE (n:Sensor {{id: {i}, reading: 0}})"))
                .unwrap();
        }

        // A near-zero timeout should cause the mutation scan to exceed the
        // deadline before it finishes scanning + mutating all 500 nodes.
        let result = db.execute_with_timeout(
            "MATCH (n:Sensor) SET n.reading = 42",
            Duration::from_nanos(1),
        );

        assert!(
            matches!(result, Err(Error::QueryTimeout)),
            "expected QueryTimeout, got {result:?}"
        );
    }

    // ── Issue #307: VersionStore GC ───────────────────────────────────────────

    /// GC prunes old version entries so the store does not grow proportionally
    /// with the number of writes.  After 1 000 SET operations and enough commits
    /// to trigger GC several times, the total number of version entries across
    /// all keys must be strictly less than 1 000 (the unbounded baseline).
    #[test]
    fn versionstore_gc_bounds_memory() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        let col_id = sparrowdb_common::col_id_of("counter");

        // Create the node once.
        let node_id = {
            let mut tx = db.begin_write().unwrap();
            let nid = tx.create_node(0, &[(col_id, Value::Int64(0))]).unwrap();
            tx.commit().unwrap();
            nid
        };

        // Update the same property 1 000 times.
        for i in 1i64..=1_000 {
            let mut tx = db.begin_write().unwrap();
            tx.set_node_col(node_id, col_id, Value::Int64(i));
            tx.commit().unwrap();
        }

        // Count total Version entries across the entire VersionStore.
        let total_entries: usize = db
            .inner
            .versions
            .read()
            .unwrap()
            .map
            .values()
            .map(|v| v.len())
            .sum();

        // GC_COMMIT_INTERVAL = 100, so GC ran ~10 times.  The chain must be
        // far shorter than 1 000 entries.
        assert!(
            total_entries < 1_000,
            "VersionStore grew to {total_entries} entries — GC is not running"
        );
    }

    /// Readers pinned before GC runs must still see the correct snapshot value
    /// after GC has pruned versions below their watermark.
    #[test]
    fn versionstore_gc_preserves_snapshot_reads() {
        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        let col_id = sparrowdb_common::col_id_of("val");

        // Create a node with initial value 0.
        let node_id = {
            let mut tx = db.begin_write().unwrap();
            let nid = tx.create_node(0, &[(col_id, Value::Int64(0))]).unwrap();
            tx.commit().unwrap();
            nid
        };

        // Open a long-lived reader pinned at this snapshot.
        let reader = db.begin_read().unwrap();
        let reader_snapshot = reader.snapshot_txn_id;

        // Perform enough writes to trigger GC several times.
        for i in 1i64..=200 {
            let mut tx = db.begin_write().unwrap();
            tx.set_node_col(node_id, col_id, Value::Int64(i));
            tx.commit().unwrap();
        }

        // The pinned reader must still observe a value consistent with its snapshot.
        let seen = reader.get_node(node_id, &[col_id]).unwrap();
        let seen_val = seen
            .iter()
            .find(|(c, _)| *c == col_id)
            .map(|(_, v)| v.clone());

        match seen_val {
            Some(Value::Int64(v)) => {
                assert!(
                    v <= 200,
                    "reader at snapshot {reader_snapshot} saw future value {v}"
                );
            }
            other => panic!("unexpected value from pinned reader: {other:?}"),
        }

        // Explicitly drop the reader — cleanly unregisters it.
        drop(reader);
    }
}
