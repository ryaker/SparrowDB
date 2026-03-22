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
    /// Open (or create) a SparrowDB database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        Ok(GraphDb {
            inner: Arc::new(DbInner {
                path: path.to_path_buf(),
                current_txn_id: AtomicU64::new(0),
                write_locked: AtomicBool::new(false),
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
    pub fn begin_write(&self) -> Result<WriteTx> {
        // Atomically acquire the write lock (false → true).
        // No unsafe transmute — WriteGuard holds Arc<DbInner> and resets
        // the AtomicBool on Drop, so the lock flag always outlives the guard.
        let guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
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
        let _guard = WriteGuard::try_acquire(&self.inner).ok_or(Error::WriterBusy)?;
        let catalog = Catalog::open(&self.inner.path)?;
        let node_store = NodeStore::open(&self.inner.path)?;
        let (rel_table_ids, n_nodes) =
            collect_maintenance_params(&catalog, &node_store, &self.inner.path);
        let engine = MaintenanceEngine::new(&self.inner.path);
        engine.optimize(&rel_table_ids, n_nodes)
    }

    /// Execute a Cypher query.
    ///
    /// Read-only statements (`MATCH … RETURN`) are executed against a
    /// point-in-time snapshot.  Mutation statements (`MERGE`, `MATCH … SET`,
    /// `MATCH … DELETE`) open a write transaction internally and commit on
    /// success.  `CREATE` statements auto-register labels and write nodes (SPA-156).
    pub fn execute(&self, cypher: &str) -> Result<QueryResult> {
        use sparrowdb_cypher::ast::Statement;
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;
        let catalog_snap = Catalog::open(&self.inner.path)?;
        let bound = bind(stmt, &catalog_snap)?;

        if Engine::is_mutation(&bound.inner) {
            match bound.inner {
                Statement::Merge(ref m) => self.execute_merge(m),
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
                let csr = open_csr_forward(&self.inner.path);
                Engine::new(
                    NodeStore::open(&self.inner.path)?,
                    catalog_snap,
                    csr,
                    &self.inner.path,
                )
            };

            let result = {
                let _exec_span = info_span!("sparrowdb.execute").entered();
                engine.execute_statement(bound.inner)?
            };

            tracing::debug!(rows = result.rows.len(), "query complete");
            Ok(result)
        }
    }

    /// Internal: execute a standalone `CREATE (a)-[:R]->(b)` statement.
    ///
    /// Creates all declared nodes, then for each edge in the pattern looks up
    /// the freshly-created node IDs by variable name and calls
    /// `WriteTx::create_edge`.  This is needed when the CREATE clause contains
    /// relationship patterns, because edge creation must be routed through a
    /// write transaction so the relationship type is registered in the catalog
    /// and the edge is appended to the WAL (SPA-182).
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

        let mut tx = self.begin_write()?;

        // Map variable name → NodeId for all newly created nodes.
        let mut var_to_node: HashMap<String, NodeId> = HashMap::new();

        for node in &create.nodes {
            let label = node.labels.first().cloned().unwrap_or_default();
            let label_id: u32 = match tx.catalog.get_label(&label)? {
                Some(id) => id as u32,
                None => tx.catalog.create_label(&label)? as u32,
            };

            let props: Vec<(u32, Value)> = node
                .props
                .iter()
                .map(|entry| {
                    let col_id = col_id_of(&entry.key);
                    match &entry.value {
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
                            Ok((col_id, literal_to_value(lit)))
                        }
                        _ => Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "CREATE property '{}' must be a literal value (int, float, bool, or string)",
                            entry.key
                        ))),
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let node_id = tx.create_node(label_id, &props)?;

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
        let catalog_snap = Catalog::open(&self.inner.path)?;
        let bound = bind(stmt, &catalog_snap)?;

        if Engine::is_mutation(&bound.inner) {
            // Mutation executors (execute_merge, execute_match_mutate, execute_match_create)
            // call expr_to_value which maps Literal::Param(_) → Int64(0), so any $param
            // in a MERGE/SET/CREATE would be silently written as 0/null rather than the
            // supplied value.  Fail fast until mutation parameter binding is implemented.
            return Err(Error::InvalidArgument(
                "execute_with_params does not support mutation statements (MERGE/SET/CREATE); \
                 use execute() for mutations without parameters"
                    .into(),
            ));
        }

        let _span = info_span!("sparrowdb.query_with_params").entered();

        let mut engine = {
            let _open_span = info_span!("sparrowdb.open_engine").entered();
            let csr = open_csr_forward(&self.inner.path);
            Engine::new(
                NodeStore::open(&self.inner.path)?,
                catalog_snap,
                csr,
                &self.inner.path,
            )
            .with_params(params)
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.execute").entered();
            engine.execute_statement(bound.inner)?
        };

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
        tx.merge_node(&m.label, props)?;
        tx.commit()?;
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
        let csr = open_csr_forward(&self.inner.path);
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            Catalog::open(&self.inner.path)?,
            csr,
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
        let csr = open_csr_forward(&self.inner.path);
        let engine = Engine::new(
            NodeStore::open(&self.inner.path)?,
            Catalog::open(&self.inner.path)?,
            csr,
            &self.inner.path,
        );

        // Execute the MATCH clause to get correlated binding rows.
        // Each row is a HashMap<variable_name, NodeId> representing one
        // matched combination.  This replaces the old `scan_match_create`
        // approach which collected candidates per variable independently and
        // then took a full Cartesian product — causing N² edge creation when
        // multiple nodes of the same label existed (SPA-183).
        // MATCH…CREATE only supports edge creation from matched bindings.
        // Node creation in the CREATE body is not yet implemented — reject early
        // to avoid silently dropping the requested node writes.
        if !mc.create.nodes.is_empty() {
            return Err(Error::Unimplemented);
        }

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
                            .map(|(_, v)| v.to_u64() == want_val.to_u64())
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
                        .map(|&(_, raw)| raw == want_val.to_u64())
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
        // Check the persisted delta log for attached edges.
        let delta = EdgeStore::open(&self.inner.path, RelTableId(0))
            .and_then(|s| s.read_delta())
            .unwrap_or_default();
        if delta.iter().any(|r| r.src == node_id || r.dst == node_id) {
            return Err(Error::NodeHasEdges { node_id: node_id.0 });
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
    /// 1. Detects write-write conflicts (SPA-128).
    /// 2. Applies all buffered structural operations to storage (SPA-181):
    ///    node creates, node deletes, edge creates, label creates.
    /// 3. Flushes all staged `set_node_col` updates to disk.
    /// 4. Records before-images in the version chain at the previous `txn_id`,
    ///    preserving snapshot access for currently-open readers.
    /// 5. Atomically increments the global `current_txn_id`.
    /// 6. Records the new values in the version chain at the new `txn_id`.
    /// 7. Updates per-node version table for future conflict detection.
    /// 8. Appends WAL records for all mutations (SPA-127).
    #[must_use = "check the Result; a failed commit means nothing was written"]
    pub fn commit(mut self) -> Result<TxnId> {
        // Step 1: MVCC conflict abort (SPA-128).
        self.detect_conflicts()?;

        // Step 2: Flush buffered structural operations to disk (SPA-181).
        // These were previously written immediately; now they are deferred to
        // here so that a transaction that errors mid-way leaves no partial state.
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

        // Step 8: Flush any pending fulltext index updates.
        // The primary DB mutations above are already durable (WAL written,
        // txn_id advanced).  A flush failure here must NOT return Err and
        // cause the caller to retry an already-committed transaction.  Log the
        // error so operators can investigate but treat the commit as successful.
        for (name, mut idx) in self.fulltext_pending.drain() {
            if let Err(e) = idx.flush() {
                tracing::error!(index = %name, error = %e, "fulltext index flush failed post-commit; index may be stale until next write");
            }
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

// ── Mutation value helpers ─────────────────────────────────────────────────────

/// Convert a Cypher [`Literal`] to a storage [`Value`].
fn literal_to_value(lit: &sparrowdb_cypher::ast::Literal) -> Value {
    use sparrowdb_cypher::ast::Literal;
    match lit {
        Literal::Int(n) => Value::Int64(*n),
        // Float stored as bitcast; String stored as Bytes.
        // Full overflow-store support is Phase 9+.
        Literal::Float(f) => Value::Int64(f.to_bits() as i64),
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
            // Concurrent attempt must fail.
            assert!(matches!(db.begin_write(), Err(Error::WriterBusy)));
            drop(tx);
            // After drop lock is free again.
        }
    }
}
