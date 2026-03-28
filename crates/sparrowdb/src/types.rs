// ── Internal shared types ─────────────────────────────────────────────────────
//
// This module contains the in-memory data structures shared across the other
// submodules.  Nothing here is `pub` outside the crate.

use sparrowdb_catalog::catalog::{Catalog, LabelId};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::wal::writer::WalWriter;
use sparrowdb_storage::edge_store::RelTableId;
use sparrowdb_common::{NodeId, EdgeId};
use sparrowdb_storage::node_store::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};

/// Per-rel-table edge property cache (SPA-261).
pub(crate) type EdgePropsCache = Arc<RwLock<HashMap<u32, HashMap<(u64, u64), Vec<(u32, u64)>>>>>;

// ── Version chain ─────────────────────────────────────────────────────────────

/// Key for the version map: (packed NodeId value, column id).
pub(crate) type VersionKey = (u64, u32);

/// One committed version of a property value.
#[derive(Clone)]
pub(crate) struct Version {
    /// The `txn_id` at which this value was committed.
    pub committed_at: u64,
    pub value: Value,
}

/// In-memory MVCC version store for property updates.
///
/// A `Vec<Version>` per `(NodeId, col_id)` key, sorted ascending by
/// `committed_at`.  Readers binary-search for the latest version ≤ their
/// snapshot.
#[derive(Default)]
pub(crate) struct VersionStore {
    pub map: HashMap<VersionKey, Vec<Version>>,
}

impl VersionStore {
    /// Record a committed property update.
    pub fn insert(&mut self, node_id: NodeId, col_id: u32, committed_at: u64, value: Value) {
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
    pub fn get_at(&self, node_id: NodeId, col_id: u32, snapshot_txn_id: u64) -> Option<Value> {
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
    pub fn gc(&mut self, min_active_txn_id: u64) -> usize {
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
pub(crate) struct StagedUpdate {
    /// Before-image — the value a reader with `snapshot ≤ prev_txn_id` should
    /// see.  Stored at `prev_txn_id` in the version chain on commit so that
    /// pre-existing readers retain correct snapshot access even after the
    /// on-disk column has been overwritten.
    ///
    /// `None` if the key already had an entry in the version chain (so the
    /// chain already carries the correct before-image).
    pub before_image: Option<(u64, Value)>, // (txn_id_to_store_at, value)
    /// The new value to commit.
    pub new_value: Value,
    /// Human-readable property key name used for WAL emission.
    /// For columns staged by col_id directly (e.g. from create_node), this is
    /// the synthesized form `"col_{col_id}"`.
    pub key_name: String,
}

/// Staged (uncommitted) property updates for an active `WriteTx`.
///
/// Keyed by `(NodeId, col_id)`; only the latest staged value for each key
/// matters.
#[derive(Default)]
pub(crate) struct WriteBuffer {
    pub updates: HashMap<VersionKey, StagedUpdate>,
}

// ── WAL mutation log entries ──────────────────────────────────────────────────

/// A mutation staged in a `WriteTx` for WAL emission at commit time.
pub(crate) enum WalMutation {
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
/// are staged in the `WriteTx` pending buffers and flushed to disk at commit,
/// so a dropped transaction leaves no ghost labels or rel-type entries (closes #305).
pub(crate) enum PendingOp {
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
pub(crate) struct NodeVersions {
    /// `node_id.0 → last_committed_txn_id`.
    pub map: HashMap<u64, u64>,
}

impl NodeVersions {
    pub fn set(&mut self, node_id: NodeId, txn_id: u64) {
        self.map.insert(node_id.0, txn_id);
    }

    pub fn get(&self, node_id: NodeId) -> u64 {
        self.map.get(&node_id.0).copied().unwrap_or(0)
    }
}

// ── Shared inner state ────────────────────────────────────────────────────────

pub(crate) struct DbInner {
    pub path: PathBuf,
    /// Monotonically increasing; starts at 0 (no writes committed yet).
    /// Incremented atomically after each successful `WriteTx` commit.
    pub current_txn_id: AtomicU64,
    /// Ensures at most one writer exists at a time.
    ///
    /// `false` = no active writer; `true` = writer active.
    /// Use `compare_exchange(false, true)` for try-lock semantics.
    pub write_locked: AtomicBool,
    /// MVCC version chains for property updates (snapshot-isolation support).
    pub versions: RwLock<VersionStore>,
    /// Per-node last-committed txn_id (write-write conflict detection, SPA-128).
    pub node_versions: RwLock<NodeVersions>,
    /// Optional 32-byte encryption key (SPA-98).  Retained for future use
    /// (e.g., reopening the WAL writer after a full segment rotation triggered
    /// by an external checkpoint).  The key is encoded into the WAL writer at
    /// open time via [`WalWriter::open_encrypted`].
    #[allow(dead_code)]
    pub encryption_key: Option<[u8; 32]>,
    pub unique_constraints: RwLock<HashSet<(u32, u32)>>,
    /// Shared property-index cache (SPA-187).
    ///
    /// Read queries clone from this at `Engine::new_with_cached_index` time and
    /// write their lazily-populated index back after execution.  Write
    /// transactions invalidate it via `clear()` on commit so the next read
    /// re-populates from the updated column files.
    pub prop_index: RwLock<sparrowdb_storage::property_index::PropertyIndex>,
    /// Shared catalog cache (SPA-188).
    ///
    /// Read queries clone from this to avoid re-parsing the TLV catalog file
    /// on every query.  DDL operations (label/rel-type creation, constraints)
    /// refresh the cache via `invalidate_catalog()`.
    pub catalog: RwLock<Catalog>,
    /// Cached per-type CSR forward map (SPA-189).
    ///
    /// Populated at `GraphDb::open` time and cloned into each `Engine`
    /// instance.  Invalidated after CHECKPOINT / OPTIMIZE since those compact
    /// the delta log into new CSR base files.
    pub csr_map: RwLock<HashMap<u32, CsrForward>>,
    /// Cached label row counts (SPA-190).
    ///
    /// Maps `LabelId → node count` (high-water mark).  Built once at open
    /// time and refreshed after any write that creates or deletes nodes.
    /// Passed to `Engine::new_with_all_caches` to avoid per-label HWM disk
    /// reads on every read query.
    pub label_row_counts: RwLock<HashMap<LabelId, usize>>,
    /// Persistent WAL writer (SPA-210).
    ///
    /// Reused across transaction commits to avoid paying segment-scan and
    /// file-open overhead on every write.  Protected by a `Mutex` because
    /// only one writer exists at a time (the `write_locked` flag enforces
    /// SWMR, so contention is zero in practice).
    pub wal_writer: Mutex<WalWriter>,
    /// Shared edge-property cache (SPA-261).
    pub edge_props_cache: EdgePropsCache,
    /// Tracks active `ReadTx` snapshots for GC watermark computation.
    ///
    /// Maps `snapshot_txn_id → active_reader_count`.  A reader registers its
    /// snapshot on open and unregisters on drop.  The minimum key in this map
    /// is the oldest pinned snapshot; versions older than that watermark (and
    /// fully superseded by a newer committed version) can be pruned by GC.
    pub active_readers: Mutex<BTreeMap<u64, usize>>,
    /// Number of `WriteTx` commits since the last GC run.
    ///
    /// GC is triggered every [`GC_COMMIT_INTERVAL`] commits.
    pub commits_since_gc: AtomicU64,
}

/// Run GC on the version store every this many commits.
pub(crate) const GC_COMMIT_INTERVAL: u64 = 100;

impl DbInner {
    /// Invalidate the shared property-index cache.
    ///
    /// Clears the in-memory index (bumping its generation so stale Engine
    /// clones are detectable) and removes the persisted `prop_index.bin` file
    /// so a subsequent `open` does not load stale index data (SPA-286).
    pub fn invalidate_prop_index(&self) {
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
    pub fn persist_prop_index(&self) {
        if let Ok(mut guard) = self.prop_index.write() {
            guard.persist_if_grew(&self.path);
        }
    }
}

// ── Write-lock guard (SPA-181: replaces 'static transmute UB) ────────────────

/// RAII guard that holds the exclusive writer lock for a [`DbInner`].
///
/// Acquired by [`GraphDb::begin_write`] via an atomic compare-exchange
/// (`false → true`).  Released on `Drop` by resetting the flag to `false`.
/// Because the guard holds an [`Arc<DbInner>`] the `DbInner` (and the
/// `AtomicBool` it contains) is guaranteed to outlive the guard — no unsafe
/// lifetime extension needed.
pub(crate) struct WriteGuard {
    pub inner: Arc<DbInner>,
}

impl WriteGuard {
    /// Try to acquire the write lock.  Returns `None` if already held.
    pub fn try_acquire(inner: &Arc<DbInner>) -> Option<Self> {
        inner
            .write_locked
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .ok()
            .map(|_| WriteGuard {
                inner: Arc::clone(inner),
            })
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Release the lock.
        self.inner
            .write_locked
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
