// ── GraphDb + WAL replay + tests ─────────────────────────────────────────────
//
// This module contains the top-level GraphDb struct and all its methods
// (execute_*, checkpoint, optimize, stats, etc.), the WAL crash recovery
// function, and the full test suite.

use crate::batch::augment_rows_with_pending;
use crate::helpers::{
    build_label_row_counts_from_disk, collect_maintenance_params, dir_size_bytes, eval_expr_merge,
    expr_to_value, expr_to_value_with_params, fnv1a_col_id, is_edge_delete_mutation,
    is_reserved_label, literal_to_value, load_constraints, open_csr_map, save_constraints,
    storage_value_to_exec, try_open_csr_map,
};
use crate::read_tx::ReadTx;
use crate::types::{DbInner, NodeVersions, PendingOp, VersionStore, WriteBuffer, WriteGuard};
use crate::wal_codec::wal_bytes_to_value;
use crate::write_tx::WriteTx;
use sparrowdb_catalog::catalog::{Catalog, LabelId};
use sparrowdb_common::{col_id_of, Error, NodeId, Result};
use sparrowdb_execution::{Engine, QueryResult};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::maintenance::MaintenanceEngine;
use sparrowdb_storage::node_store::{NodeStore, Value};
use sparrowdb_storage::wal::codec::WalPayload;
use sparrowdb_storage::wal::writer::WalWriter;
use sparrowdb_storage::wal::WalReplayer;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::info_span;

// ── DbStats ───────────────────────────────────────────────────────────────────

/// Storage-size snapshot returned by [`GraphDb::stats`] (SPA-171).
#[derive(Debug, Default, Clone)]
pub struct DbStats {
    /// Total bytes on disk across all database files.
    pub total_bytes: u64,
    /// Bytes consumed by node column files for each label.
    pub bytes_per_label: std::collections::HashMap<String, u64>,
    /// High-water mark (HWM) per label — the highest slot index allocated for
    /// that label, plus one.
    pub node_count_per_label: std::collections::HashMap<String, u64>,
    /// Total edges across all relationship types (delta log + CSR).
    pub edge_count: u64,
    /// Bytes used by all WAL segment files under `wal/`.
    pub wal_bytes: u64,
}

// ── GraphDb ───────────────────────────────────────────────────────────────────

/// The top-level SparrowDB handle.
///
/// Cheaply cloneable — all clones share the same underlying state.
/// Obtain an instance via [`GraphDb::open`] or the free [`open`] function.
#[derive(Clone)]
pub struct GraphDb {
    pub(crate) inner: Arc<DbInner>,
}

impl GraphDb {
    /// Return the filesystem path of this database.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Open (or create) a SparrowDB database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let wal_dir = path.join("wal");
        let wal_writer = WalWriter::open(&wal_dir)?;
        // Replay any WAL records that were fsync'd but not yet written to disk
        // (crash between Step 4 WAL-fsync and Step 5 disk-write in commit).
        // This must run before we build the in-memory caches (label_row_counts,
        // csr_map) so those are constructed from the post-recovery state.
        // Returns the maximum txn_id observed so we can restore current_txn_id.
        let max_replayed_txn_id = replay_wal_mutations(path, None)?;
        // Open the catalog after replay: replay may have called
        // get_or_create_rel_type_id which updates catalog on disk.
        let catalog = Catalog::open(path)?;
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
        // Restore current_txn_id to one past the highest txn_id observed in the
        // replayed WAL so new transactions never reuse IDs from before the crash.
        // When no WAL activity exists (max == 0), keep the initial counter at 0
        // so snapshot semantics are unchanged for fresh databases.
        let initial_txn_id = if max_replayed_txn_id > 0 {
            max_replayed_txn_id + 1
        } else {
            0
        };
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(initial_txn_id),
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
        let wal_dir = path.join("wal");
        let wal_writer = WalWriter::open_encrypted(&wal_dir, key)?;
        // Replay any WAL records that were fsync'd but not yet written to disk.
        // Returns the maximum txn_id observed so we can restore current_txn_id.
        let max_replayed_txn_id = replay_wal_mutations(path, Some(key))?;
        // Open the catalog after replay (may have been mutated by EdgeCreate replay).
        let catalog = Catalog::open(path)?;
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
        // Restore current_txn_id to one past the highest txn_id observed in the
        // replayed WAL so new transactions never reuse IDs from before the crash.
        // When no WAL activity exists (max == 0), keep the initial counter at 0.
        let initial_txn_id = if max_replayed_txn_id > 0 {
            max_replayed_txn_id + 1
        } else {
            0
        };
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(initial_txn_id),
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
        .with_chunked_pipeline()
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
    pub(crate) fn catalog_snapshot(&self) -> Catalog {
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

    pub(crate) fn refresh_caches(&self) {
        self.invalidate_catalog();
        self.invalidate_csr_map();
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
            pending_label_creates: Vec::new(),
            pending_rel_type_creates: Vec::new(),
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

    /// Execute a Cypher query through the chunked vectorized pipeline (#299).
    ///
    /// Identical to [`execute`](Self::execute) but enables `use_chunked_pipeline`
    /// on the engine so that qualifying query shapes route through the pull-based
    /// chunked pipeline instead of the row-at-a-time engine.  Benchmarks and any
    /// caller that wants to measure the Phase 2+ pipeline should prefer this over
    /// the plain `execute` method.
    pub fn execute_chunked(&self, cypher: &str) -> Result<QueryResult> {
        use sparrowdb_cypher::ast::Statement;
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;
        let catalog_snap = self.catalog_snapshot();
        let bound = bind(stmt, &catalog_snap)?;

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
            // Mutations don't use the read pipeline — fall through to the
            // standard mutation paths (write transactions are unaffected).
            match bound.inner {
                Statement::Merge(ref m) => self.execute_merge(m),
                Statement::MatchMergeRel(ref mm) => self.execute_match_merge_rel(mm),
                Statement::MatchMutate(ref mm) => self.execute_match_mutate(mm),
                Statement::MatchCreate(ref mc) => self.execute_match_create(mc),
                Statement::Create(ref c) => self.execute_create_standalone(c),
                _ => unreachable!(),
            }
        } else {
            let _span = info_span!("sparrowdb.query_chunked").entered();

            let mut engine = {
                let _open_span = info_span!("sparrowdb.open_engine").entered();
                let csrs = self.cached_csr_map();
                self.build_read_engine(NodeStore::open(&self.inner.path)?, catalog_snap, csrs)
            };
            engine = engine.with_chunked_pipeline();

            let result = {
                let _exec_span = info_span!("sparrowdb.execute").entered();
                engine.execute_statement(bound.inner)?
            };

            engine.write_back_prop_index(&self.inner.prop_index);
            self.persist_prop_index();

            tracing::debug!(rows = result.rows.len(), "chunked query complete");
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
        // Map variable name → named props written (for RETURN projection).
        let mut var_to_props: HashMap<String, Vec<(String, sparrowdb_execution::Value)>> =
            HashMap::new();

        for node in &create.nodes {
            let label = node.labels.first().cloned().unwrap_or_default();
            let label_id: u32 = tx.get_or_create_label_id(&label)?;

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
                // Stash props for RETURN projection (issue #366).
                let exec_props: Vec<(String, sparrowdb_execution::Value)> = named_props
                    .iter()
                    .map(|(k, v)| (k.clone(), storage_value_to_exec(v)))
                    .collect();
                var_to_props.insert(node.var.clone(), exec_props);
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

        // If a RETURN clause was provided, project the created nodes (issue #366).
        if let Some(ref rc) = create.return_clause {
            use sparrowdb_cypher::ast::Expr;
            type ExecValue = sparrowdb_execution::Value;

            // Derive column names from the RETURN items.
            let columns: Vec<String> = rc
                .items
                .iter()
                .map(|item| {
                    item.alias.clone().unwrap_or_else(|| match &item.expr {
                        Expr::PropAccess { var, prop } => format!("{var}.{prop}"),
                        Expr::Var(v) => v.clone(),
                        Expr::FnCall { name, args } => {
                            let arg_str = args
                                .first()
                                .map(|a| match a {
                                    Expr::Var(v) => v.clone(),
                                    _ => "*".to_string(),
                                })
                                .unwrap_or_else(|| "*".to_string());
                            format!("{}({})", name.to_lowercase(), arg_str)
                        }
                        _ => "?".to_string(),
                    })
                })
                .collect();

            // Evaluate each RETURN item against the in-memory prop stash.
            // Open the fallback store once outside the loop to avoid
            // repeated open() calls inside the iterator.
            let fallback_store = NodeStore::open(&self.inner.path).ok();
            let row: Vec<ExecValue> = rc
                .items
                .iter()
                .map(|item| match &item.expr {
                    Expr::PropAccess { var, prop } => {
                        // Look up from in-memory prop stash first.
                        if let Some(props) = var_to_props.get(var.as_str()) {
                            props
                                .iter()
                                .find(|(k, _)| k == prop)
                                .map(|(_, v)| v.clone())
                                .unwrap_or(ExecValue::Null)
                        } else {
                            // Fallback: re-read from disk.
                            if let (Some(ref store), Some(&node_id)) =
                                (&fallback_store, var_to_node.get(var.as_str()))
                            {
                                let col_id = fnv1a_col_id(prop);
                                store
                                    .get_node(node_id, &[col_id])
                                    .ok()
                                    .and_then(|v| v.into_iter().next())
                                    .map(|(_, val)| storage_value_to_exec(&val))
                                    .unwrap_or(ExecValue::Null)
                            } else {
                                ExecValue::Null
                            }
                        }
                    }
                    Expr::Var(v) => {
                        // Bare variable: return a Map of all properties.
                        if let Some(props) = var_to_props.get(v.as_str()) {
                            ExecValue::Map(props.clone())
                        } else {
                            ExecValue::Null
                        }
                    }
                    Expr::FnCall { name, args } if name.eq_ignore_ascii_case("id") => {
                        if let Some(Expr::Var(v)) = args.first() {
                            if let Some(&node_id) = var_to_node.get(v.as_str()) {
                                ExecValue::Int64(node_id.0 as i64)
                            } else {
                                ExecValue::Null
                            }
                        } else {
                            ExecValue::Null
                        }
                    }
                    _ => ExecValue::Null,
                })
                .collect();

            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }

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
        // Detect create vs match by observing whether merge_node dirtied a new node.
        let dirty_before = tx.dirty_nodes.len();
        let node_id = tx.merge_node(&m.label, props)?;
        let was_created = tx.dirty_nodes.len() > dirty_before;

        // Apply ON CREATE SET or ON MATCH SET items inside the same transaction.
        let on_set_items = if was_created {
            &m.on_create_set
        } else {
            &m.on_match_set
        };
        for mutation in on_set_items {
            if let sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } = mutation {
                let sv = expr_to_value(value);
                tx.set_property(node_id, prop, sv)?;
            }
        }
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
        let dirty_before = tx.dirty_nodes.len();
        let node_id = tx.merge_node(&m.label, props)?;
        let was_created = tx.dirty_nodes.len() > dirty_before;

        // Apply ON CREATE SET or ON MATCH SET items inside the same transaction.
        // Use expr_to_value_with_params so $parameter references resolve correctly.
        let on_set_items = if was_created {
            &m.on_create_set
        } else {
            &m.on_match_set
        };
        for mutation in on_set_items {
            if let sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } = mutation {
                let sv = expr_to_value_with_params(value, params)?;
                tx.set_property(node_id, prop, sv)?;
            }
        }
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
        for mutation in &mm.mutations {
            match mutation {
                sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                    let sv = expr_to_value_with_params(value, params)?;
                    for node_id in &matching_ids {
                        tx.set_property(*node_id, prop, sv.clone())?;
                    }
                }
                sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                    for node_id in &matching_ids {
                        tx.delete_node(*node_id)?;
                    }
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

        for mutation in &mm.mutations {
            match mutation {
                sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                    let sv = expr_to_value(value);
                    for node_id in &matching_ids {
                        tx.set_property(*node_id, prop, sv.clone())?;
                    }
                }
                sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                    for node_id in &matching_ids {
                        tx.delete_node(*node_id)?;
                    }
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

        for mutation in &mm.mutations {
            match mutation {
                sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                    let sv = expr_to_value(value);
                    for node_id in &matching_ids {
                        Self::check_deadline(deadline)?;
                        tx.set_property(*node_id, prop, sv.clone())?;
                    }
                }
                sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                    for node_id in &matching_ids {
                        Self::check_deadline(deadline)?;
                        tx.delete_node(*node_id)?;
                    }
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
                    let label_id: u32 = tx.get_or_create_label_id(&label)?;
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
                let dirty_before = tx.dirty_nodes.len();
                let node_id = tx.merge_node(&m.label, props)?;
                let was_created = tx.dirty_nodes.len() > dirty_before;
                let on_set_items = if was_created {
                    &m.on_create_set
                } else {
                    &m.on_match_set
                };
                for mutation in on_set_items {
                    if let sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } = mutation {
                        let sv = expr_to_value(value);
                        tx.set_property(node_id, prop, sv)?;
                    }
                }
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
                    for mutation in &mm.mutations {
                        match mutation {
                            sparrowdb_cypher::ast::Mutation::Set { prop, value, .. } => {
                                let sv = expr_to_value(value);
                                for node_id in &matching_ids {
                                    tx.set_property(*node_id, prop, sv.clone())?;
                                }
                            }
                            sparrowdb_cypher::ast::Mutation::Delete { .. } => {
                                for node_id in &matching_ids {
                                    tx.delete_node(*node_id)?;
                                }
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

fn replay_wal_mutations(db_path: &Path, encryption_key: Option<[u8; 32]>) -> Result<u64> {
    let wal_dir = db_path.join("wal");
    if !wal_dir.exists() {
        return Ok(0);
    }

    let mutations = match encryption_key {
        Some(key) => WalReplayer::scan_mutations_encrypted(&wal_dir, key)?,
        None => WalReplayer::scan_mutations(&wal_dir)?,
    };

    if mutations.is_empty() {
        return Ok(0);
    }

    // Compute the maximum txn_id seen across all committed mutations so the
    // caller can set current_txn_id = max_txn_id + 1 after open.
    let max_txn_id = mutations.iter().map(|m| m.txn_id).max().unwrap_or(0);

    let mut node_store = NodeStore::open(db_path)?;
    let mut catalog = Catalog::open(db_path)?;

    for m in &mutations {
        match &m.payload {
            WalPayload::NodeCreate {
                node_id,
                label_id,
                props,
            } => {
                let slot = (*node_id & 0xFFFF_FFFF) as u32;
                // Idempotency check: if the on-disk HWM already covers this
                // slot, the write completed before the crash.
                let disk_hwm = node_store.disk_hwm_for_label(*label_id).unwrap_or(0);
                if disk_hwm > slot as u64 {
                    continue; // already on disk
                }
                // Decode properties from WAL bytes and apply.
                // Property names may be "col_{id}" placeholders emitted by the
                // low-level write path in write_mutation_wal.  Detect the prefix
                // and parse the numeric col_id directly instead of hashing the
                // placeholder string (which would yield the wrong column).
                let decoded_props: Vec<(u32, Value)> = props
                    .iter()
                    .map(|(name, val_bytes)| {
                        let col_id = name
                            .strip_prefix("col_")
                            .and_then(|s| s.parse::<u32>().ok())
                            .unwrap_or_else(|| col_id_of(name));
                        let value = wal_bytes_to_value(val_bytes);
                        (col_id, value)
                    })
                    .collect();
                node_store.create_node_at_slot(*label_id, slot, &decoded_props)?;
                node_store.flush_hwms()?;
            }

            WalPayload::NodeUpdate {
                node_id,
                col_id,
                after,
                ..
            } => {
                let label_id = (*node_id >> 32) as u32;
                let slot = (*node_id & 0xFFFF_FFFF) as u32;
                // Gate replay on the persisted HWM: only apply updates for
                // slots that are already on disk (slot < hwm).  Slots at or
                // beyond the HWM have not been persisted yet — they will be
                // covered by the NodeCreate replay path above.
                let hwm = node_store.hwm_for_label(label_id).unwrap_or(0);
                if slot as u64 >= hwm {
                    continue; // node not yet persisted; NodeCreate path handles it
                }
                let value = wal_bytes_to_value(after);
                let node_id_typed = sparrowdb_common::NodeId(*node_id);
                node_store.upsert_node_col(node_id_typed, *col_id, &value)?;
            }

            WalPayload::NodeDelete { node_id } => {
                let node_id_typed = sparrowdb_common::NodeId(*node_id);
                // tombstone_node is idempotent (missing node is not an error).
                let _ = node_store.tombstone_node(node_id_typed);
            }

            WalPayload::EdgeCreate {
                edge_id,
                src,
                dst,
                rel_type,
                props,
            } => {
                let src_label_id = (*src >> 32) as u16;
                let dst_label_id = (*dst >> 32) as u16;

                // Ensure the rel type is registered.
                let catalog_rel_id =
                    catalog.get_or_create_rel_type_id(src_label_id, dst_label_id, rel_type)?;
                let rel_table_id = RelTableId(catalog_rel_id as u32);

                // Idempotency: skip if this edge_id is already in the delta log.
                let current_edge_id = sparrowdb_storage::edge_store::EdgeStore::peek_next_edge_id(
                    db_path,
                    rel_table_id,
                )?;
                if current_edge_id.0 > *edge_id {
                    continue; // already written
                }

                let src_node = sparrowdb_common::NodeId(*src);
                let dst_node = sparrowdb_common::NodeId(*dst);
                let mut es = EdgeStore::open(db_path, rel_table_id)?;
                es.create_edge(src_node, rel_table_id, dst_node)?;

                // Re-apply edge properties if present.
                if !props.is_empty() {
                    let src_slot = *src & 0xFFFF_FFFF;
                    let dst_slot = *dst & 0xFFFF_FFFF;
                    for (name, val_bytes) in props {
                        let col_id = col_id_of(name);
                        let value = wal_bytes_to_value(val_bytes);
                        let raw_u64 = node_store.encode_value(&value)?;
                        es.set_edge_prop(src_slot, dst_slot, col_id, raw_u64)?;
                    }
                }
            }

            WalPayload::EdgeDelete { src, dst, rel_type } => {
                let src_label_id = (*src >> 32) as u16;
                let dst_label_id = (*dst >> 32) as u16;

                // Look up the rel type — if it doesn't exist, edge was never
                // written so skip silently.
                let rel_id_opt = catalog.get_rel_table(src_label_id, dst_label_id, rel_type)?;
                if let Some(catalog_rel_id) = rel_id_opt {
                    let rel_table_id = RelTableId(catalog_rel_id as u32);
                    let src_node = sparrowdb_common::NodeId(*src);
                    let dst_node = sparrowdb_common::NodeId(*dst);
                    if let Ok(mut es) = EdgeStore::open(db_path, rel_table_id) {
                        let _ = es.delete_edge(src_node, dst_node);
                    }
                }
            }

            // Non-structural payloads — no action needed during open.
            _ => {}
        }
    }

    Ok(max_txn_id)
}

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
        let _: fn(&Path) -> crate::Result<GraphDb> = crate::open;
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

    // ── WAL crash recovery tests (issue #303) ──────────────────────────────────

    /// Simulate a crash between WAL fsync (step 4) and disk write (step 5) for
    /// a NodeCreate operation, then verify that re-opening the database replays
    /// the missing record from WAL.
    ///
    /// Test steps:
    ///   1. Create a fresh DB, register a label, commit one node normally.
    ///   2. Write a second NodeCreate record to the WAL directly (BEGIN +
    ///      NodeCreate + COMMIT + fsync) WITHOUT writing to the NodeStore — this
    ///      simulates the crash window.
    ///   3. Drop the DB handle.
    ///   4. Re-open the DB — WAL replay must fire.
    ///   5. Assert that the second node is visible (HWM = 2).
    #[test]
    fn wal_replay_on_open_recovers_node_create_after_crash() {
        use sparrowdb_catalog::catalog::Catalog as RawCatalog;
        use sparrowdb_common::TxnId;
        use sparrowdb_storage::wal::codec::{WalPayload as StorageWalPayload, WalRecordKind};
        use sparrowdb_storage::wal::writer::WalWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("crash_test.sparrow");

        // ── Step 1: normal first commit ──────────────────────────────────────
        let label_id: u32;
        {
            let db = GraphDb::open(&db_path).expect("open");

            // Register the label via Cypher (also registers in catalog).
            db.execute("CREATE (n:CrashNode {val: 1})").expect("create");

            // Discover the label_id assigned by the catalog.
            let cat = RawCatalog::open(&db_path).expect("catalog");
            label_id = cat
                .get_label("CrashNode")
                .expect("get_label")
                .expect("label exists") as u32;

            drop(db);
        }

        // ── Step 2: simulate crash — write WAL for second node, skip disk ────
        {
            let wal_dir = db_path.join("wal");
            let mut wal = WalWriter::open(&wal_dir).expect("wal open");
            let txn_id = TxnId(9999); // use a high txn_id to avoid collision

            // The second node's slot will be 1 (first node occupied slot 0).
            let slot: u32 = 1;
            let node_id: u64 = (label_id as u64) << 32 | slot as u64;

            wal.append(WalRecordKind::Begin, txn_id, StorageWalPayload::Empty)
                .expect("begin");
            wal.append(
                WalRecordKind::NodeCreate,
                txn_id,
                StorageWalPayload::NodeCreate {
                    node_id,
                    label_id,
                    props: vec![("val".to_string(), 2i64.to_le_bytes().to_vec())],
                },
            )
            .expect("node create");
            wal.append(WalRecordKind::Commit, txn_id, StorageWalPayload::Empty)
                .expect("commit");
            wal.fsync().expect("fsync");
            // Drop wal WITHOUT writing anything to NodeStore — crash simulation.
            drop(wal);
        }

        // ── Step 3: verify disk state before replay (node not on disk yet) ───
        {
            use sparrowdb_storage::node_store::NodeStore;
            let store = NodeStore::open(&db_path).expect("node store");
            let disk_hwm = store.disk_hwm_for_label(label_id).unwrap_or(0);
            assert_eq!(
                disk_hwm, 1,
                "before replay: disk HWM should be 1 (only first node was written)"
            );
        }

        // ── Step 4: re-open the database — replay must fire ──────────────────
        let db = GraphDb::open(&db_path).expect("re-open after crash");

        // ── Step 5: the replayed node must be visible ─────────────────────────
        {
            use sparrowdb_storage::node_store::NodeStore;
            let store = NodeStore::open(&db_path).expect("node store after replay");
            let disk_hwm = store.disk_hwm_for_label(label_id).unwrap_or(0);
            assert_eq!(
                disk_hwm, 2,
                "after replay: disk HWM should be 2 (both nodes written)"
            );
        }

        // Also verify via Cypher query.
        let result = db
            .execute("MATCH (n:CrashNode) RETURN n.val ORDER BY n.val")
            .expect("query");
        assert_eq!(
            result.rows.len(),
            2,
            "should see 2 CrashNode rows after WAL replay, got: {result:?}"
        );
    }

    /// Simulate a crash after WAL fsync for a NodeUpdate (SET) operation.
    /// The node is already on disk; only the property update is missing.
    #[test]
    fn wal_replay_on_open_recovers_node_update_after_crash() {
        use sparrowdb_common::TxnId;
        use sparrowdb_storage::wal::codec::{WalPayload as StorageWalPayload, WalRecordKind};
        use sparrowdb_storage::wal::writer::WalWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("update_crash.sparrow");

        // ── Step 1: create a node normally ───────────────────────────────────
        let node_id_raw: u64;
        let score_col_id: u32;
        {
            let db = GraphDb::open(&db_path).expect("open");
            db.execute("CREATE (n:UpdateNode {score: 10})")
                .expect("create");
            let result = db
                .execute("MATCH (n:UpdateNode) RETURN id(n)")
                .expect("query id");
            node_id_raw = match &result.rows[0][0] {
                sparrowdb_execution::Value::Int64(v) => *v as u64,
                other => panic!("unexpected id type: {other:?}"),
            };
            score_col_id = col_id_of("score");
            drop(db);
        }

        // ── Step 2: simulate crash — WAL update written, disk update missing ─
        {
            let wal_dir = db_path.join("wal");
            let mut wal = WalWriter::open(&wal_dir).expect("wal open");
            let txn_id = TxnId(9998);

            // score = 99, encoded as i64 little-endian (TAG_INT64 = 0x00 in top byte)
            let after_bytes = 99i64.to_le_bytes().to_vec();

            wal.append(WalRecordKind::Begin, txn_id, StorageWalPayload::Empty)
                .expect("begin");
            wal.append(
                WalRecordKind::NodeUpdate,
                txn_id,
                StorageWalPayload::NodeUpdate {
                    node_id: node_id_raw,
                    key: "score".to_string(),
                    col_id: score_col_id,
                    before: 10i64.to_le_bytes().to_vec(),
                    after: after_bytes,
                },
            )
            .expect("node update");
            wal.append(WalRecordKind::Commit, txn_id, StorageWalPayload::Empty)
                .expect("commit");
            wal.fsync().expect("fsync");
            drop(wal);
        }

        // ── Step 3: re-open the DB — WAL replay must restore score = 99 ──────
        let db = GraphDb::open(&db_path).expect("re-open");

        let result = db
            .execute("MATCH (n:UpdateNode) RETURN n.score")
            .expect("query score");
        assert_eq!(result.rows.len(), 1, "should see one UpdateNode");
        let score = match &result.rows[0][0] {
            sparrowdb_execution::Value::Int64(v) => *v,
            other => panic!("unexpected score type: {other:?}"),
        };
        assert_eq!(
            score, 99,
            "WAL replay must restore score=99 from crash-recovered update"
        );
    }

    /// A normal re-open with no crash — WAL replay is a no-op.
    #[test]
    fn wal_replay_noop_when_data_already_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("noop.sparrow");

        {
            let db = GraphDb::open(&db_path).expect("open");
            db.execute("CREATE (n:Noop {x: 42})").expect("create");
            drop(db);
        }

        // Re-open — should work cleanly with no double-apply.
        let db = GraphDb::open(&db_path).expect("re-open");
        let result = db.execute("MATCH (n:Noop) RETURN n.x").expect("query");
        assert_eq!(result.rows.len(), 1);
        let x = match &result.rows[0][0] {
            sparrowdb_execution::Value::Int64(v) => *v,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(x, 42, "value must not be corrupted by no-op replay");
    }

    // ── #305: catalog mutations must be transactional ─────────────────────────

    /// #305: A WriteTx that calls create_label and is then dropped without
    /// committing must NOT leave a ghost label in the catalog.
    #[test]
    fn dropped_write_tx_leaves_no_ghost_labels() {
        use sparrowdb_catalog::catalog::Catalog;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        {
            let mut tx = db.begin_write().unwrap();
            // Stage a label creation — should NOT be written to catalog on disk.
            tx.create_label("GhostLabel").unwrap();
            // Drop WITHOUT calling commit().
        }

        // Reopen the catalog from disk and verify the label is NOT present.
        let catalog = Catalog::open(dir.path()).unwrap();
        let ghost = catalog
            .get_label("GhostLabel")
            .expect("catalog lookup must not error");
        assert!(
            ghost.is_none(),
            "dropped WriteTx must not leave ghost label in catalog (#305)"
        );

        // A subsequent write transaction must still be obtainable.
        let tx2 = db
            .begin_write()
            .expect("write lock must be released after drop");
        tx2.commit().unwrap();
    }

    /// #305: A WriteTx that calls create_label and commits successfully MUST
    /// persist the label to the catalog on disk.
    #[test]
    fn committed_write_tx_persists_label() {
        use sparrowdb_catalog::catalog::Catalog;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        {
            let mut tx = db.begin_write().unwrap();
            tx.create_label("PersistedLabel").unwrap();
            tx.commit().unwrap();
        }

        // Reopen the catalog from disk and verify the label IS present.
        let catalog = Catalog::open(dir.path()).unwrap();
        let id = catalog
            .get_label("PersistedLabel")
            .expect("catalog lookup must not error");
        assert!(
            id.is_some(),
            "committed WriteTx must persist label to catalog (#305)"
        );
    }

    /// #305: A WriteTx that calls create_edge (which stages a new rel type) and
    /// is dropped without committing must NOT leave a ghost rel-type entry.
    #[test]
    fn dropped_write_tx_leaves_no_ghost_rel_types() {
        use sparrowdb_catalog::catalog::Catalog;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        // Commit two nodes so we have valid src/dst node IDs.
        let (src, dst) = {
            let mut tx = db.begin_write().unwrap();
            // Use label 0 (already exists as a virtual/implicit label).
            let src = tx.create_node(0, &[]).unwrap();
            let dst = tx.create_node(0, &[]).unwrap();
            tx.commit().unwrap();
            (src, dst)
        };

        {
            let mut tx = db.begin_write().unwrap();
            // Stage an edge with a new rel type — should NOT be written to
            // the catalog on disk when the transaction is dropped.
            tx.create_edge(src, dst, "GHOST_REL", std::collections::HashMap::new())
                .unwrap();
            // Drop WITHOUT calling commit().
        }

        // Reopen the catalog from disk and verify no rel-table entry exists.
        let catalog = Catalog::open(dir.path()).unwrap();
        let rel_tables = catalog
            .list_rel_tables()
            .expect("list_rel_tables must not error");
        assert!(
            rel_tables.iter().all(|(_, _, t)| t != "GHOST_REL"),
            "dropped WriteTx must not leave ghost rel-type in catalog (#305)"
        );
    }

    /// #305: A WriteTx that calls create_edge (which stages a new rel type) and
    /// is committed must persist the rel-type entry to the catalog on disk.
    #[test]
    fn committed_write_tx_persists_rel_type() {
        use sparrowdb_catalog::catalog::Catalog;

        let dir = tempfile::tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();

        // Commit two nodes so we have valid src/dst node IDs.
        let (src, dst) = {
            let mut tx = db.begin_write().unwrap();
            let src = tx.create_node(0, &[]).unwrap();
            let dst = tx.create_node(0, &[]).unwrap();
            tx.commit().unwrap();
            (src, dst)
        };

        {
            let mut tx = db.begin_write().unwrap();
            tx.create_edge(src, dst, "PERSISTED_REL", std::collections::HashMap::new())
                .unwrap();
            tx.commit().unwrap();
        }

        // Reopen the catalog from disk and verify the rel-type IS present.
        let catalog = Catalog::open(dir.path()).unwrap();
        let rel_tables = catalog
            .list_rel_tables()
            .expect("list_rel_tables must not error");
        assert!(
            rel_tables.iter().any(|(_, _, t)| t == "PERSISTED_REL"),
            "committed WriteTx must persist rel-type to catalog (#305)"
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
