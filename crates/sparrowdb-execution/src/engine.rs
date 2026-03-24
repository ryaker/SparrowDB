//! Query execution engine.
//!
//! Converts a bound Cypher AST into an operator tree and executes it,
//! returning a materialized `QueryResult`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tracing::info_span;

use sparrowdb_catalog::catalog::{Catalog, LabelId};
use sparrowdb_common::{col_id_of, NodeId, Result};
use sparrowdb_cypher::ast::{
    BinOpKind, CallStatement, CreateStatement, Expr, ListPredicateKind, Literal,
    MatchCreateStatement, MatchMergeRelStatement, MatchMutateStatement,
    MatchOptionalMatchStatement, MatchStatement, MatchWithStatement, Mutation,
    OptionalMatchStatement, PathPattern, PipelineStage, PipelineStatement, ReturnItem, SortDir,
    Statement, UnionStatement, UnwindStatement, WithClause,
};
use sparrowdb_cypher::{bind, parse};
use sparrowdb_storage::csr::{CsrBackward, CsrForward};
use sparrowdb_storage::edge_store::{DeltaRecord, EdgeStore, RelTableId};
use sparrowdb_storage::fulltext_index::FulltextIndex;
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};
use sparrowdb_storage::property_index::PropertyIndex;
use sparrowdb_storage::text_index::TextIndex;
use sparrowdb_storage::wal::WalReplayer;

use crate::types::{QueryResult, Value};

// ── DegreeCache (SPA-272) ─────────────────────────────────────────────────────

/// Pre-computed out-degree for every node slot across all relationship types.
///
/// Built **lazily** on first call to [`Engine::top_k_by_degree`] or
/// [`Engine::out_degree`] by scanning:
/// 1. CSR forward files (checkpointed edges) — contribution per slot from offsets.
/// 2. Delta log records (uncheckpointed edges) — each `DeltaRecord.src` increments
///    the source slot's count.
///
/// Keyed by the lower-32-bit slot extracted from `NodeId.0`
/// (i.e. `node_id & 0xFFFF_FFFF`).
///
/// Lookup is O(1).  [`Engine::top_k_by_degree`] uses this cache to answer
/// "top-k highest-degree nodes of label L" in O(N log k) where N is the
/// label's node count (HWM), rather than O(N × E) full edge scans.
///
/// Queries that never call `top_k_by_degree` (e.g. point lookups, scans,
/// hop traversals) pay zero cost: no CSR iteration, no delta-log reads.
#[derive(Debug, Default)]
pub struct DegreeCache {
    /// Maps slot → total out-degree across all relationship types.
    inner: HashMap<u64, u32>,
}

impl DegreeCache {
    /// Return the total out-degree for `slot` across all relationship types.
    ///
    /// Returns `0` for slots that have no outgoing edges.
    pub fn out_degree(&self, slot: u64) -> u32 {
        self.inner.get(&slot).copied().unwrap_or(0)
    }

    /// Increment the out-degree counter for `slot` by 1.
    fn increment(&mut self, slot: u64) {
        *self.inner.entry(slot).or_insert(0) += 1;
    }

    /// Build a `DegreeCache` from a set of CSR forward files and delta records.
    ///
    /// `csrs` — all per-rel-type CSR forward files loaded at engine open.
    /// `delta` — all delta-log records (uncommitted/uncheckpointed edges).
    fn build(csrs: &HashMap<u32, CsrForward>, delta: &[DeltaRecord]) -> Self {
        let mut cache = DegreeCache::default();

        // 1. Accumulate from CSR: for each rel type, for each src slot, add
        //    the slot's out-degree (= neighbors slice length).
        for csr in csrs.values() {
            for slot in 0..csr.n_nodes() {
                let deg = csr.neighbors(slot).len() as u32;
                if deg > 0 {
                    *cache.inner.entry(slot).or_insert(0) += deg;
                }
            }
        }

        // 2. Accumulate from delta log: each record increments src's slot.
        //    Lower 32 bits of NodeId = within-label slot number.
        for rec in delta {
            let src_slot = rec.src.0 & 0xFFFF_FFFF;
            cache.increment(src_slot);
        }

        cache
    }
}

// ── DegreeStats (SPA-273) ─────────────────────────────────────────────────────

/// Per-relationship-type degree statistics collected at engine open time.
///
/// Built once from CSR forward files (checkpointed edges) by scanning every
/// source slot's out-degree for each relationship type.  Delta-log edges are
/// included via the same `delta_all` scan already performed for `DegreeCache`.
///
/// Used by future join-order heuristics: `mean()` gives an estimate of how
/// many hops a traversal on this relationship type will produce per source node.
#[derive(Debug, Default, Clone)]
pub struct DegreeStats {
    /// Minimum out-degree seen across all source nodes for this rel type.
    pub min: u32,
    /// Maximum out-degree seen across all source nodes for this rel type.
    pub max: u32,
    /// Sum of all per-node out-degrees (numerator of the mean).
    pub total: u64,
    /// Number of source nodes contributing to `total` (denominator of the mean).
    pub count: u64,
}

impl DegreeStats {
    /// Mean out-degree for this relationship type.
    ///
    /// Returns `1.0` when no edges exist to avoid division-by-zero and because
    /// an unknown degree is conservatively assumed to be at least 1.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            1.0
        } else {
            self.total as f64 / self.count as f64
        }
    }
}

/// Tri-state result for relationship table lookup.
///
/// Distinguishes three cases that previously both returned `Option::None` from
/// `resolve_rel_table_id`, causing typed queries to fall back to scanning
/// all edge stores when the rel type was not yet in the catalog (SPA-185).
#[derive(Debug, Clone, Copy)]
enum RelTableLookup {
    /// The query has no rel-type filter — scan all rel types.
    All,
    /// The rel type was found in the catalog; use this specific store.
    Found(u32),
    /// The rel type was specified but not found in the catalog — the
    /// edge cannot exist, so return empty results immediately.
    NotFound,
}

/// Immutable snapshot of storage state required to execute a read query.
///
/// Groups the fields that are needed for read-only access to the graph so they
/// can eventually be cloned/shared across parallel executor threads without
/// bundling the mutable per-query state that lives in [`Engine`].
pub struct ReadSnapshot {
    pub store: NodeStore,
    pub catalog: Catalog,
    /// Per-relationship-type CSR forward files, keyed by `RelTableId` (u32).
    pub csrs: HashMap<u32, CsrForward>,
    pub db_root: std::path::PathBuf,
    /// Cached live node count per label, updated on every node creation.
    ///
    /// Used by the planner to estimate cardinality without re-scanning the
    /// node store's high-water-mark file on every query.
    pub label_row_counts: HashMap<LabelId, usize>,
    /// Per-relationship-type out-degree statistics (SPA-273).
    ///
    /// Keyed by `RelTableId` (u32).  Initialized **lazily** on first access
    /// via [`ReadSnapshot::rel_degree_stats`].  Simple traversal queries
    /// (Q3, Q4) that never consult the planner heuristics pay zero CSR-scan
    /// cost.  The scan is only triggered when a query actually needs degree
    /// statistics (e.g. join-order planning).
    ///
    /// `OnceLock` is used instead of `OnceCell` so that `ReadSnapshot` remains
    /// `Sync` and can safely be shared across parallel BFS threads.
    rel_degree_stats: std::sync::OnceLock<HashMap<u32, DegreeStats>>,
}

impl ReadSnapshot {
    /// Return per-relationship-type out-degree statistics, computing them on
    /// first call and caching the result for all subsequent calls.
    ///
    /// The CSR forward scan is only triggered once per `ReadSnapshot` instance,
    /// and only when a caller actually needs degree statistics.  Queries that
    /// never access this (e.g. simple traversals Q3/Q4) pay zero overhead.
    pub fn rel_degree_stats(&self) -> &HashMap<u32, DegreeStats> {
        self.rel_degree_stats.get_or_init(|| {
            self.csrs
                .iter()
                .map(|(&rel_table_id, csr)| {
                    let mut stats = DegreeStats::default();
                    let mut first = true;
                    for slot in 0..csr.n_nodes() {
                        let deg = csr.neighbors(slot).len() as u32;
                        if deg > 0 {
                            if first {
                                stats.min = deg;
                                stats.max = deg;
                                first = false;
                            } else {
                                if deg < stats.min {
                                    stats.min = deg;
                                }
                                if deg > stats.max {
                                    stats.max = deg;
                                }
                            }
                            stats.total += deg as u64;
                            stats.count += 1;
                        }
                    }
                    (rel_table_id, stats)
                })
                .collect()
        })
    }
}

/// The execution engine holds references to the storage layer.
pub struct Engine {
    pub snapshot: ReadSnapshot,
    /// Runtime query parameters supplied by the caller (e.g. `$name` → Value).
    pub params: HashMap<String, Value>,
    /// In-memory B-tree property equality index (SPA-249).
    ///
    /// Loaded **lazily** on first use for each `(label_id, col_id)` pair that a
    /// query actually filters on.  Queries with no property filter (e.g.
    /// `COUNT(*)`, hop traversals) never touch this and pay zero build cost.
    /// `RefCell` provides interior mutability so that `build_for` can be called
    /// from `&self` scan helpers without changing every method signature.
    pub prop_index: std::cell::RefCell<PropertyIndex>,
    /// In-memory text search index for CONTAINS and STARTS WITH (SPA-251, SPA-274).
    ///
    /// Loaded **lazily** — only when a query has a CONTAINS or STARTS WITH
    /// predicate on a specific `(label_id, col_id)` pair, via
    /// `TextIndex::build_for`.  Queries with no text predicates (e.g.
    /// `COUNT(*)`, hop traversals) never trigger any TextIndex I/O.
    /// `RefCell` provides interior mutability so that `build_for` can be called
    /// from `&self` scan helpers without changing every method signature.
    /// Stores sorted `(decoded_string, slot)` pairs per `(label_id, col_id)`.
    /// - CONTAINS: linear scan avoids per-slot property-decode overhead.
    /// - STARTS WITH: binary-search prefix range — O(log n + k).
    pub text_index: std::cell::RefCell<TextIndex>,
    /// Optional per-query deadline (SPA-254).
    ///
    /// When `Some`, the engine checks this deadline at the top of each hot
    /// scan / traversal loop iteration.  If `Instant::now() >= deadline`,
    /// `Error::QueryTimeout` is returned immediately.  `None` means no
    /// deadline (backward-compatible default).
    pub deadline: Option<std::time::Instant>,
    /// Pre-computed out-degree for every node slot across all relationship types
    /// (SPA-272).
    ///
    /// Initialized **lazily** on first call to [`Engine::top_k_by_degree`] or
    /// [`Engine::out_degree`].  Queries that never need degree information
    /// (point lookups, full scans, hop traversals) pay zero cost at engine-open
    /// time: no CSR iteration, no delta-log I/O.
    ///
    /// `RefCell` provides interior mutability so the cache can be populated
    /// from `&self` methods without changing the signature of `top_k_by_degree`.
    pub degree_cache: std::cell::RefCell<Option<DegreeCache>>,
    /// Set of `(label_id, col_id)` pairs that carry a UNIQUE constraint (SPA-234).
    ///
    /// Populated by `CREATE CONSTRAINT ON (n:Label) ASSERT n.property IS UNIQUE`.
    /// Checked in `execute_create` before writing each node: if the property
    /// value already exists in `prop_index` for that `(label_id, col_id)`, the
    /// insert is rejected with `Error::InvalidArgument`.
    pub unique_constraints: HashSet<(u32, u32)>,
}

impl Engine {
    /// Create an engine with a pre-built per-type CSR map.
    ///
    /// The `csrs` map associates each `RelTableId` (u32) with its forward CSR.
    /// Use [`Engine::with_single_csr`] in tests or legacy code that only has
    /// one CSR.
    pub fn new(
        store: NodeStore,
        catalog: Catalog,
        csrs: HashMap<u32, CsrForward>,
        db_root: &Path,
    ) -> Self {
        Self::new_with_cached_index(store, catalog, csrs, db_root, None)
    }

    /// Create an engine, optionally seeding the property index from a shared
    /// cache.  When `cached_index` is `Some`, the index is cloned out of the
    /// `RwLock` at construction time so the engine can use `RefCell` internally
    /// without holding the lock.
    pub fn new_with_cached_index(
        store: NodeStore,
        catalog: Catalog,
        csrs: HashMap<u32, CsrForward>,
        db_root: &Path,
        cached_index: Option<&std::sync::RwLock<PropertyIndex>>,
    ) -> Self {
        // SPA-249 (lazy fix): property index is built on demand per
        // (label_id, col_id) pair via PropertyIndex::build_for, called from
        // execute_scan just before the first lookup for that pair.  Queries
        // with no property filter (COUNT(*), hop traversals) never trigger
        // any index I/O at all.
        //
        // SPA-274 (lazy text index): text search index is now also built lazily,
        // mirroring the PropertyIndex pattern.  Only (label_id, col_id) pairs
        // that appear in an actual CONTAINS or STARTS WITH predicate are loaded.
        // Queries with no text predicates (COUNT(*), hop traversals, property
        // lookups) pay zero TextIndex I/O cost.
        //
        // SPA-272 / perf fix: DegreeCache is now initialized lazily on first
        // call to top_k_by_degree() or out_degree().  Queries that never need
        // degree information (point lookups, full scans, hop traversals) pay
        // zero cost at engine-open time: no CSR iteration, no delta-log I/O.
        //
        // SPA-Q1-perf: build label_row_counts ONCE at Engine::new() by reading
        // each label's high-water-mark from the NodeStore HWM file (an O(1)
        // in-memory map lookup after the first call per label).  Previously this
        // map was left empty and only populated via the write path, so every
        // read query started with an empty map, forcing the planner to fall back
        // to per-scan HWM reads on every execution.  Now the snapshot carries a
        // pre-populated map; the write path continues to increment it on creation.
        let label_row_counts: HashMap<LabelId, usize> = catalog
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
            .collect();

        // SPA-273 (lazy): rel_degree_stats is now computed on first access via
        // ReadSnapshot::rel_degree_stats().  Simple traversal queries (Q3/Q4)
        // that never consult degree statistics pay zero CSR-scan overhead here.
        let snapshot = ReadSnapshot {
            store,
            catalog,
            csrs,
            db_root: db_root.to_path_buf(),
            label_row_counts,
            rel_degree_stats: std::sync::OnceLock::new(),
        };

        // If a shared cached index was provided, clone it out so we start
        // with pre-loaded columns.  Otherwise start fresh.
        let idx = cached_index
            .and_then(|lock| lock.read().ok())
            .map(|guard| guard.clone())
            .unwrap_or_default();

        Engine {
            snapshot,
            params: HashMap::new(),
            prop_index: std::cell::RefCell::new(idx),
            text_index: std::cell::RefCell::new(TextIndex::new()),
            deadline: None,
            degree_cache: std::cell::RefCell::new(None),
            unique_constraints: HashSet::new(),
        }
    }

    /// Convenience constructor for tests and legacy callers that have a single
    /// [`CsrForward`] (stored at `RelTableId(0)`).
    ///
    /// SPA-185: prefer `Engine::new` with a full `HashMap<u32, CsrForward>` for
    /// production use so that per-type filtering is correct.
    pub fn with_single_csr(
        store: NodeStore,
        catalog: Catalog,
        csr: CsrForward,
        db_root: &Path,
    ) -> Self {
        let mut csrs = HashMap::new();
        csrs.insert(0u32, csr);
        Self::new(store, catalog, csrs, db_root)
    }

    /// Attach runtime query parameters to this engine instance.
    ///
    /// Parameters are looked up when evaluating `$name` expressions (e.g. in
    /// `UNWIND $items AS x`).
    pub fn with_params(mut self, params: HashMap<String, Value>) -> Self {
        self.params = params;
        self
    }

    /// Set a per-query deadline (SPA-254).
    ///
    /// The engine will return [`sparrowdb_common::Error::QueryTimeout`] if
    /// `Instant::now() >= deadline` during any hot scan or traversal loop.
    pub fn with_deadline(mut self, deadline: std::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Write back the engine's lazily-populated property index into the shared
    /// cache so that future read queries can skip I/O for columns we already
    /// loaded.  Called from `GraphDb` read paths after `execute_statement`.
    pub fn write_back_prop_index(&self, shared: &std::sync::RwLock<PropertyIndex>) {
        if let Ok(mut guard) = shared.write() {
            *guard = self.prop_index.borrow().clone();
        }
    }

    /// Check whether the per-query deadline has passed (SPA-254).
    ///
    /// Returns `Err(QueryTimeout)` if a deadline is set and has expired,
    /// `Ok(())` otherwise.  Inline so the hot-path cost when `deadline` is
    /// `None` compiles down to a single branch-not-taken.
    #[inline]
    fn check_deadline(&self) -> sparrowdb_common::Result<()> {
        if let Some(dl) = self.deadline {
            if std::time::Instant::now() >= dl {
                return Err(sparrowdb_common::Error::QueryTimeout);
            }
        }
        Ok(())
    }

    // ── Per-type CSR / delta helpers ─────────────────────────────────────────

    /// Return the relationship table lookup state for `(src_label_id, dst_label_id, rel_type)`.
    ///
    /// - Empty `rel_type` → [`RelTableLookup::All`] (no type filter).
    /// - Rel type found in catalog → [`RelTableLookup::Found(id)`].
    /// - Rel type specified but not in catalog → [`RelTableLookup::NotFound`]
    ///   (the typed edge cannot exist; callers must return empty results).
    fn resolve_rel_table_id(
        &self,
        src_label_id: u32,
        dst_label_id: u32,
        rel_type: &str,
    ) -> RelTableLookup {
        if rel_type.is_empty() {
            return RelTableLookup::All;
        }
        match self
            .snapshot
            .catalog
            .get_rel_table(src_label_id as u16, dst_label_id as u16, rel_type)
            .ok()
            .flatten()
        {
            Some(id) => RelTableLookup::Found(id as u32),
            None => RelTableLookup::NotFound,
        }
    }

    /// Read delta records for a specific relationship type.
    ///
    /// Returns an empty `Vec` if the rel type has not been registered yet, or
    /// if the delta file does not exist.
    fn read_delta_for(&self, rel_table_id: u32) -> Vec<sparrowdb_storage::edge_store::DeltaRecord> {
        EdgeStore::open(&self.snapshot.db_root, RelTableId(rel_table_id))
            .and_then(|s| s.read_delta())
            .unwrap_or_default()
    }

    /// Read delta records across **all** registered rel types.
    ///
    /// Used by code paths that traverse edges without a type filter.
    fn read_delta_all(&self) -> Vec<sparrowdb_storage::edge_store::DeltaRecord> {
        let ids = self.snapshot.catalog.list_rel_table_ids();
        if ids.is_empty() {
            // No rel types in catalog yet; fall back to table-id 0 (legacy).
            return EdgeStore::open(&self.snapshot.db_root, RelTableId(0))
                .and_then(|s| s.read_delta())
                .unwrap_or_default();
        }
        ids.into_iter()
            .flat_map(|(id, _, _, _)| {
                EdgeStore::open(&self.snapshot.db_root, RelTableId(id as u32))
                    .and_then(|s| s.read_delta())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Return neighbor slots from the CSR for a given src slot and rel table.
    fn csr_neighbors(&self, rel_table_id: u32, src_slot: u64) -> Vec<u64> {
        self.snapshot
            .csrs
            .get(&rel_table_id)
            .map(|csr| csr.neighbors(src_slot).to_vec())
            .unwrap_or_default()
    }

    /// Return neighbor slots merged across **all** registered rel types.
    fn csr_neighbors_all(&self, src_slot: u64) -> Vec<u64> {
        let mut out: Vec<u64> = Vec::new();
        for csr in self.snapshot.csrs.values() {
            out.extend_from_slice(csr.neighbors(src_slot));
        }
        out
    }

    /// Ensure the [`DegreeCache`] is populated, building it lazily on first call.
    ///
    /// Reads all delta-log records for every known rel type and scans every CSR
    /// forward file to tally out-degrees per source slot.  Subsequent calls are
    /// O(1) — the cache is stored in `self.degree_cache` and reused.
    ///
    /// Called automatically by [`top_k_by_degree`] and [`out_degree`].
    /// Queries that never call those methods (point lookups, full scans,
    /// hop traversals) pay **zero** cost.
    fn ensure_degree_cache(&self) {
        let mut guard = self.degree_cache.borrow_mut();
        if guard.is_some() {
            return; // already built
        }

        // Read all delta-log records (uncheckpointed edges).
        let delta_all: Vec<DeltaRecord> = {
            let ids = self.snapshot.catalog.list_rel_table_ids();
            if ids.is_empty() {
                EdgeStore::open(&self.snapshot.db_root, RelTableId(0))
                    .and_then(|s| s.read_delta())
                    .unwrap_or_default()
            } else {
                ids.into_iter()
                    .flat_map(|(id, _, _, _)| {
                        EdgeStore::open(&self.snapshot.db_root, RelTableId(id as u32))
                            .and_then(|s| s.read_delta())
                            .unwrap_or_default()
                    })
                    .collect()
            }
        };

        *guard = Some(DegreeCache::build(&self.snapshot.csrs, &delta_all));
    }

    /// Return the total out-degree for `slot` across all relationship types.
    ///
    /// Triggers lazy initialization of the [`DegreeCache`] on first call.
    /// Returns `0` for slots with no outgoing edges.
    pub fn out_degree(&self, slot: u64) -> u32 {
        self.ensure_degree_cache();
        self.degree_cache
            .borrow()
            .as_ref()
            .expect("degree_cache populated by ensure_degree_cache")
            .out_degree(slot)
    }

    /// Return the top-`k` nodes of `label_id` ordered by out-degree descending.
    ///
    /// Each element of the returned `Vec` is `(slot, out_degree)`.  Ties in
    /// degree are broken by slot number (lower slot first) for determinism.
    ///
    /// Returns an empty `Vec` when `k == 0` or the label has no nodes.
    ///
    /// Uses [`DegreeCache`] for O(1) per-node lookups (SPA-272).
    /// The cache is built lazily on first call — queries that never call this
    /// method pay zero cost.
    pub fn top_k_by_degree(&self, label_id: u32, k: usize) -> Result<Vec<(u64, u32)>> {
        if k == 0 {
            return Ok(vec![]);
        }
        let hwm = self.snapshot.store.hwm_for_label(label_id)?;
        if hwm == 0 {
            return Ok(vec![]);
        }

        self.ensure_degree_cache();
        let cache = self.degree_cache.borrow();
        let cache = cache
            .as_ref()
            .expect("degree_cache populated by ensure_degree_cache");

        let mut pairs: Vec<(u64, u32)> = (0..hwm)
            .map(|slot| (slot, cache.out_degree(slot)))
            .collect();

        // Sort descending by degree; break ties by ascending slot for determinism.
        pairs.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        pairs.truncate(k);
        Ok(pairs)
    }

    /// Parse, bind, plan, and execute a Cypher query.
    ///
    /// Takes `&mut self` because `CREATE` statements auto-register labels in
    /// the catalog and write nodes to the node store (SPA-156).
    pub fn execute(&mut self, cypher: &str) -> Result<QueryResult> {
        let stmt = {
            let _parse_span = info_span!("sparrowdb.parse", cypher = cypher).entered();
            parse(cypher)?
        };

        let bound = {
            let _bind_span = info_span!("sparrowdb.bind").entered();
            bind(stmt, &self.snapshot.catalog)?
        };

        {
            let _plan_span = info_span!("sparrowdb.plan_execute").entered();
            self.execute_bound(bound.inner)
        }
    }

    /// Execute an already-bound [`Statement`] directly.
    ///
    /// Useful for callers (e.g. `WriteTx`) that have already parsed and bound
    /// the statement and want to dispatch CHECKPOINT/OPTIMIZE themselves.
    pub fn execute_statement(&mut self, stmt: Statement) -> Result<QueryResult> {
        self.execute_bound(stmt)
    }

    fn execute_bound(&mut self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Match(m) => self.execute_match(&m),
            Statement::MatchWith(mw) => self.execute_match_with(&mw),
            Statement::Unwind(u) => self.execute_unwind(&u),
            Statement::Create(c) => self.execute_create(&c),
            // Mutation statements require a write transaction owned by the
            // caller (GraphDb). They are dispatched via the public helpers
            // below and should not reach execute_bound in normal use.
            Statement::Merge(_)
            | Statement::MatchMergeRel(_)
            | Statement::MatchMutate(_)
            | Statement::MatchCreate(_) => Err(sparrowdb_common::Error::InvalidArgument(
                "mutation statements must be executed via execute_mutation".into(),
            )),
            Statement::OptionalMatch(om) => self.execute_optional_match(&om),
            Statement::MatchOptionalMatch(mom) => self.execute_match_optional_match(&mom),
            Statement::Union(u) => self.execute_union(u),
            Statement::Checkpoint | Statement::Optimize => Ok(QueryResult::empty(vec![])),
            Statement::Call(c) => self.execute_call(&c),
            Statement::Pipeline(p) => self.execute_pipeline(&p),
            Statement::CreateIndex { label, property } => {
                self.execute_create_index(&label, &property)
            }
            Statement::CreateConstraint { label, property } => {
                self.execute_create_constraint(&label, &property)
            }
        }
    }

    // ── CALL procedure dispatch ──────────────────────────────────────────────

    /// Dispatch a `CALL` statement to the appropriate built-in procedure.
    ///
    /// Currently implemented procedures:
    /// - `db.index.fulltext.queryNodes(indexName, query)` — full-text search
    fn execute_call(&self, c: &CallStatement) -> Result<QueryResult> {
        match c.procedure.as_str() {
            "db.index.fulltext.queryNodes" => self.call_fulltext_query_nodes(c),
            "db.schema" => self.call_db_schema(c),
            "db.stats" => self.call_db_stats(c),
            other => Err(sparrowdb_common::Error::InvalidArgument(format!(
                "unknown procedure: {other}"
            ))),
        }
    }

    /// Implementation of `CALL db.index.fulltext.queryNodes(indexName, query)`.
    ///
    /// Args:
    ///   0 — index name (string literal or param)
    ///   1 — query string (string literal or param)
    ///
    /// Returns one row per matching node with columns declared in YIELD
    /// (typically `node`).  Each `node` value is a `NodeRef`.
    fn call_fulltext_query_nodes(&self, c: &CallStatement) -> Result<QueryResult> {
        // Validate argument count — must be exactly 2.
        if c.args.len() != 2 {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "db.index.fulltext.queryNodes requires exactly 2 arguments: (indexName, query)"
                    .into(),
            ));
        }

        // Evaluate arg 0 → index name.
        let index_name = eval_expr_to_string(&c.args[0])?;
        // Evaluate arg 1 → query string.
        let query = eval_expr_to_string(&c.args[1])?;

        // Open the fulltext index (read-only; no flush on this path).
        // `FulltextIndex::open` validates the name for path traversal.
        let index = FulltextIndex::open(&self.snapshot.db_root, &index_name)?;

        // Determine which column names to project.
        // Default to ["node"] when no YIELD clause was specified.
        let yield_cols: Vec<String> = if c.yield_columns.is_empty() {
            vec!["node".to_owned()]
        } else {
            c.yield_columns.clone()
        };

        // Validate YIELD columns — "node" and "score" are supported.
        if let Some(bad_col) = yield_cols
            .iter()
            .find(|c| c.as_str() != "node" && c.as_str() != "score")
        {
            return Err(sparrowdb_common::Error::InvalidArgument(format!(
                "unsupported YIELD column for db.index.fulltext.queryNodes: {bad_col}"
            )));
        }

        // Build result rows: one per matching node.
        // Use search_with_scores so we can populate the `score` YIELD column.
        let node_ids_with_scores = index.search_with_scores(&query);
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for (raw_id, score) in node_ids_with_scores {
            let node_id = sparrowdb_common::NodeId(raw_id);
            let row: Vec<Value> = yield_cols
                .iter()
                .map(|col| match col.as_str() {
                    "node" => Value::NodeRef(node_id),
                    "score" => Value::Float64(score),
                    _ => Value::Null,
                })
                .collect();
            rows.push(row);
        }

        // If a RETURN clause follows, project its items over the YIELD rows.
        let (columns, rows) = if let Some(ref ret) = c.return_clause {
            self.project_call_return(ret, &yield_cols, rows)?
        } else {
            (yield_cols, rows)
        };

        Ok(QueryResult { columns, rows })
    }

    /// Implementation of `CALL db.schema()`.
    ///
    /// Returns one row per node label and one row per relationship type with
    /// columns `["type", "name", "properties"]` where:
    ///   - `type` is `"node"` or `"relationship"`
    ///   - `name` is the label or rel-type string
    ///   - `properties` is a `List` of property name strings (sorted, may be empty)
    ///
    /// Property names are collected by scanning committed WAL records so the
    /// caller does not need to have created any nodes yet for labels to appear.
    fn call_db_schema(&self, c: &CallStatement) -> Result<QueryResult> {
        if !c.args.is_empty() {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "db.schema requires exactly 0 arguments".into(),
            ));
        }
        let columns = vec![
            "type".to_owned(),
            "name".to_owned(),
            "properties".to_owned(),
        ];

        // Collect property names per label_id and rel_type from the WAL.
        let wal_dir = self.snapshot.db_root.join("wal");
        let schema = WalReplayer::scan_schema(&wal_dir)?;

        let mut rows: Vec<Vec<Value>> = Vec::new();

        // Node labels — from catalog.
        let labels = self.snapshot.catalog.list_labels()?;
        for (label_id, label_name) in &labels {
            let mut prop_names: Vec<String> = schema
                .node_props
                .get(&(*label_id as u32))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            prop_names.sort();
            let props_value = Value::List(prop_names.into_iter().map(Value::String).collect());
            rows.push(vec![
                Value::String("node".to_owned()),
                Value::String(label_name.clone()),
                props_value,
            ]);
        }

        // Relationship types — from catalog.
        let rel_tables = self.snapshot.catalog.list_rel_tables()?;
        // Deduplicate by rel_type name since the same type can appear across multiple src/dst pairs.
        let mut seen_rel_types: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (_, _, rel_type) in &rel_tables {
            if seen_rel_types.insert(rel_type.clone()) {
                let mut prop_names: Vec<String> = schema
                    .rel_props
                    .get(rel_type)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                prop_names.sort();
                let props_value = Value::List(prop_names.into_iter().map(Value::String).collect());
                rows.push(vec![
                    Value::String("relationship".to_owned()),
                    Value::String(rel_type.clone()),
                    props_value,
                ]);
            }
        }

        Ok(QueryResult { columns, rows })
    }

    /// Implementation of `CALL db.stats()` (SPA-171).
    ///
    /// Returns metric/value rows for `total_bytes`, `wal_bytes`, `edge_count`,
    /// and per-label `nodes.<Label>` and `label_bytes.<Label>`.
    ///
    /// `nodes.<Label>` reports the per-label high-water mark (HWM), not the
    /// live-node count.  Tombstoned slots are included until compaction runs.
    ///
    /// Filesystem entries that cannot be read are silently skipped; values may
    /// be lower bounds when the database directory is partially inaccessible.
    fn call_db_stats(&self, c: &CallStatement) -> Result<QueryResult> {
        if !c.args.is_empty() {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "db.stats requires exactly 0 arguments".into(),
            ));
        }
        let db_root = &self.snapshot.db_root;
        let mut rows: Vec<Vec<Value>> = Vec::new();

        rows.push(vec![
            Value::String("total_bytes".to_owned()),
            Value::Int64(dir_size_bytes(db_root) as i64),
        ]);

        let mut wal_bytes: u64 = 0;
        if let Ok(es) = std::fs::read_dir(db_root.join("wal")) {
            for e in es.flatten() {
                let n = e.file_name();
                let ns = n.to_string_lossy();
                if ns.starts_with("segment-") && ns.ends_with(".wal") {
                    if let Ok(m) = e.metadata() {
                        wal_bytes += m.len();
                    }
                }
            }
        }
        rows.push(vec![
            Value::String("wal_bytes".to_owned()),
            Value::Int64(wal_bytes as i64),
        ]);

        const DR: u64 = 20; // DeltaRecord: src(8) + dst(8) + rel_id(4) = 20 bytes
        let mut edge_count: u64 = 0;
        if let Ok(ts) = std::fs::read_dir(db_root.join("edges")) {
            for t in ts.flatten() {
                if !t.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }
                let rd = t.path();
                if let Ok(m) = std::fs::metadata(rd.join("delta.log")) {
                    edge_count += m.len().checked_div(DR).unwrap_or(0);
                }
                let fp = rd.join("base.fwd.csr");
                if fp.exists() {
                    if let Ok(b) = std::fs::read(&fp) {
                        if let Ok(csr) = sparrowdb_storage::csr::CsrForward::decode(&b) {
                            edge_count += csr.n_edges();
                        }
                    }
                }
            }
        }
        rows.push(vec![
            Value::String("edge_count".to_owned()),
            Value::Int64(edge_count as i64),
        ]);

        for (label_id, label_name) in self.snapshot.catalog.list_labels()? {
            let lid = label_id as u32;
            let hwm = self.snapshot.store.hwm_for_label(lid).unwrap_or(0);
            rows.push(vec![
                Value::String(format!("nodes.{label_name}")),
                Value::Int64(hwm as i64),
            ]);
            let mut lb: u64 = 0;
            if let Ok(es) = std::fs::read_dir(db_root.join("nodes").join(lid.to_string())) {
                for e in es.flatten() {
                    if let Ok(m) = e.metadata() {
                        lb += m.len();
                    }
                }
            }
            rows.push(vec![
                Value::String(format!("label_bytes.{label_name}")),
                Value::Int64(lb as i64),
            ]);
        }

        let columns = vec!["metric".to_owned(), "value".to_owned()];
        let yield_cols: Vec<String> = if c.yield_columns.is_empty() {
            columns.clone()
        } else {
            c.yield_columns.clone()
        };
        for col in &yield_cols {
            if col != "metric" && col != "value" {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "unsupported YIELD column for db.stats: {col}"
                )));
            }
        }
        let idxs: Vec<usize> = yield_cols
            .iter()
            .map(|c| if c == "metric" { 0 } else { 1 })
            .collect();
        let projected: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|r| idxs.iter().map(|&i| r[i].clone()).collect())
            .collect();
        let (fc, fr) = if let Some(ref ret) = c.return_clause {
            self.project_call_return(ret, &yield_cols, projected)?
        } else {
            (yield_cols, projected)
        };
        Ok(QueryResult {
            columns: fc,
            rows: fr,
        })
    }

    /// Project a RETURN clause over rows produced by a CALL statement.
    ///
    /// The YIELD columns from the CALL become the row environment.  Each
    /// return item is evaluated against those columns:
    ///   - `Var(name)` — returns the raw yield-column value
    ///   - `PropAccess { var, prop }` — reads a property from the NodeRef
    ///
    /// This covers the primary KMS pattern:
    /// `CALL … YIELD node RETURN node.content, node.title`
    fn project_call_return(
        &self,
        ret: &sparrowdb_cypher::ast::ReturnClause,
        yield_cols: &[String],
        rows: Vec<Vec<Value>>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        // Column names from return items.
        let out_cols: Vec<String> = ret
            .items
            .iter()
            .map(|item| {
                item.alias
                    .clone()
                    .unwrap_or_else(|| expr_to_col_name(&item.expr))
            })
            .collect();

        let mut out_rows = Vec::new();
        for row in rows {
            // Build a name → Value map for this row.
            let env: HashMap<String, Value> = yield_cols
                .iter()
                .zip(row.iter())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let projected: Vec<Value> = ret
                .items
                .iter()
                .map(|item| eval_call_expr(&item.expr, &env, &self.snapshot.store))
                .collect();
            out_rows.push(projected);
        }
        Ok((out_cols, out_rows))
    }

    /// Returns `true` if `stmt` is a mutation (MERGE, MATCH+SET, MATCH+DELETE,
    /// MATCH+CREATE edge).
    ///
    /// Used by `GraphDb::execute` to route the statement to the write path.
    pub fn is_mutation(stmt: &Statement) -> bool {
        match stmt {
            Statement::Merge(_)
            | Statement::MatchMergeRel(_)
            | Statement::MatchMutate(_)
            | Statement::MatchCreate(_) => true,
            // All standalone CREATE statements must go through the
            // write-transaction path to ensure WAL durability and correct
            // single-writer semantics, regardless of whether edges are present.
            Statement::Create(_) => true,
            _ => false,
        }
    }

    // ── Mutation execution (called by GraphDb with a write transaction) ────────

    /// Scan nodes matching the MATCH patterns in a `MatchMutate` statement and
    /// return the list of matching `NodeId`s.  The caller is responsible for
    /// applying the actual mutations inside a write transaction.
    pub fn scan_match_mutate(&self, mm: &MatchMutateStatement) -> Result<Vec<NodeId>> {
        if mm.match_patterns.is_empty() {
            return Ok(vec![]);
        }

        // Guard: only single-node patterns (no multi-pattern, no relationship hops)
        // are supported.  Silently ignoring extra patterns would mutate the wrong
        // nodes; instead we surface a clear error.
        if mm.match_patterns.len() != 1 || !mm.match_patterns[0].rels.is_empty() {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "MATCH...SET/DELETE currently supports only single-node patterns (no relationships)"
                    .into(),
            ));
        }

        let pat = &mm.match_patterns[0];
        if pat.nodes.is_empty() {
            return Ok(vec![]);
        }
        let node_pat = &pat.nodes[0];
        let label = node_pat.labels.first().cloned().unwrap_or_default();

        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            // SPA-266: unknown label → no nodes can match; return empty result.
            None => return Ok(vec![]),
        };

        let hwm = self.snapshot.store.hwm_for_label(label_id)?;

        // Collect prop filter col_ids.
        let filter_col_ids: Vec<u32> = node_pat
            .props
            .iter()
            .map(|pe| prop_name_to_col_id(&pe.key))
            .collect();

        // Col_ids referenced by the WHERE clause.
        let mut all_col_ids: Vec<u32> = filter_col_ids;
        if let Some(ref where_expr) = mm.where_clause {
            collect_col_ids_from_expr(where_expr, &mut all_col_ids);
        }

        let var_name = node_pat.var.as_str();
        let mut matching_ids = Vec::new();

        for slot in 0..hwm {
            let node_id = NodeId(((label_id as u64) << 32) | slot);

            // SPA-216: skip tombstoned nodes so that already-deleted nodes are
            // not re-deleted and are not matched by SET mutations either.
            if self.is_node_tombstoned(node_id) {
                continue;
            }

            let props = read_node_props(&self.snapshot.store, node_id, &all_col_ids)?;

            if !matches_prop_filter_static(
                &props,
                &node_pat.props,
                &self.dollar_params(),
                &self.snapshot.store,
            ) {
                continue;
            }

            if let Some(ref where_expr) = mm.where_clause {
                let mut row_vals =
                    build_row_vals(&props, var_name, &all_col_ids, &self.snapshot.store);
                row_vals.extend(self.dollar_params());
                if !self.eval_where_graph(where_expr, &row_vals) {
                    continue;
                }
            }

            matching_ids.push(node_id);
        }

        Ok(matching_ids)
    }

    /// Return the mutation carried by a `MatchMutate` statement, exposing it
    /// to the caller (GraphDb) so it can apply it inside a write transaction.
    pub fn mutation_from_match_mutate(mm: &MatchMutateStatement) -> &Mutation {
        &mm.mutation
    }

    // ── Node-scan helpers (shared by scan_match_create and scan_match_create_rows) ──

    /// Returns `true` if the given node has been tombstoned (col 0 == u64::MAX).
    ///
    /// `NotFound` is expected for new/sparse nodes where col_0 has not been
    /// written yet and is treated as "not tombstoned".  All other errors are
    /// logged as warnings and also treated as "not tombstoned" so that
    /// transient storage issues do not suppress valid nodes during a scan.
    fn is_node_tombstoned(&self, node_id: NodeId) -> bool {
        match self.snapshot.store.get_node_raw(node_id, &[0u32]) {
            Ok(col0) => col0.iter().any(|&(c, v)| c == 0 && v == u64::MAX),
            Err(sparrowdb_common::Error::NotFound) => false,
            Err(e) => {
                tracing::warn!(
                    node_id = node_id.0,
                    error = ?e,
                    "tombstone check failed; treating node as not tombstoned"
                );
                false
            }
        }
    }

    /// Returns `true` if `node_id` satisfies every inline prop predicate in
    /// `filter_col_ids` / `props`.
    ///
    /// `filter_col_ids` must be pre-computed from `props` with
    /// `prop_name_to_col_id`.  Pass an empty slice when there are no filters
    /// (the method returns `true` immediately).
    fn node_matches_prop_filter(
        &self,
        node_id: NodeId,
        filter_col_ids: &[u32],
        props: &[sparrowdb_cypher::ast::PropEntry],
    ) -> bool {
        if props.is_empty() {
            return true;
        }
        match self.snapshot.store.get_node_raw(node_id, filter_col_ids) {
            Ok(raw_props) => matches_prop_filter_static(
                &raw_props,
                props,
                &self.dollar_params(),
                &self.snapshot.store,
            ),
            Err(_) => false,
        }
    }

    // ── Scan for MATCH…CREATE (called by GraphDb with a write transaction) ──────

    /// Scan nodes matching the MATCH patterns in a `MatchCreateStatement` and
    /// return a map of variable name → Vec<NodeId> for each named node pattern.
    ///
    /// The caller (GraphDb) uses this to resolve variable bindings before
    /// calling `WriteTx::create_edge` for each edge in the CREATE clause.
    pub fn scan_match_create(
        &self,
        mc: &MatchCreateStatement,
    ) -> Result<HashMap<String, Vec<NodeId>>> {
        let mut var_candidates: HashMap<String, Vec<NodeId>> = HashMap::new();

        for pat in &mc.match_patterns {
            for node_pat in &pat.nodes {
                if node_pat.var.is_empty() {
                    continue;
                }
                // Skip if already resolved (same var can appear in multiple patterns).
                if var_candidates.contains_key(&node_pat.var) {
                    continue;
                }

                let label = node_pat.labels.first().cloned().unwrap_or_default();
                let label_id: u32 = match self.snapshot.catalog.get_label(&label)? {
                    Some(id) => id as u32,
                    None => {
                        // Label not found → no matching nodes for this variable.
                        var_candidates.insert(node_pat.var.clone(), vec![]);
                        continue;
                    }
                };

                let hwm = self.snapshot.store.hwm_for_label(label_id)?;

                // Collect col_ids needed for inline prop filtering.
                let filter_col_ids: Vec<u32> = node_pat
                    .props
                    .iter()
                    .map(|p| prop_name_to_col_id(&p.key))
                    .collect();

                let mut matching_ids: Vec<NodeId> = Vec::new();
                for slot in 0..hwm {
                    let node_id = NodeId(((label_id as u64) << 32) | slot);

                    // Skip tombstoned nodes (col_0 == u64::MAX).
                    // Treat a missing-file error as "not tombstoned".
                    match self.snapshot.store.get_node_raw(node_id, &[0u32]) {
                        Ok(col0) if col0.iter().any(|&(c, v)| c == 0 && v == u64::MAX) => {
                            continue;
                        }
                        Ok(_) | Err(_) => {}
                    }

                    // Apply inline prop filter if any.
                    if !node_pat.props.is_empty() {
                        match self.snapshot.store.get_node_raw(node_id, &filter_col_ids) {
                            Ok(props) => {
                                if !matches_prop_filter_static(
                                    &props,
                                    &node_pat.props,
                                    &self.dollar_params(),
                                    &self.snapshot.store,
                                ) {
                                    continue;
                                }
                            }
                            // If a filter column doesn't exist on disk, the node
                            // cannot satisfy the filter.
                            Err(_) => continue,
                        }
                    }

                    matching_ids.push(node_id);
                }

                var_candidates.insert(node_pat.var.clone(), matching_ids);
            }
        }

        Ok(var_candidates)
    }

    /// Execute the MATCH portion of a `MatchCreateStatement` and return one
    /// binding map per matched row.
    ///
    /// Each element of the returned `Vec` is a `HashMap<variable_name, NodeId>`
    /// that represents one fully-correlated result row from the MATCH clause.
    /// The caller uses these to drive `WriteTx::create_edge` — one call per row.
    ///
    /// # Algorithm
    ///
    /// For each `PathPattern` in `match_patterns`:
    /// - **No relationships** (node-only pattern): scan the node store applying
    ///   inline prop filters; collect one candidate set per named variable.
    ///   Cross-join these sets with the rows accumulated so far.
    /// - **One relationship hop** (`(a)-[:R]->(b)`): traverse the CSR + delta
    ///   log to enumerate actual (src, dst) pairs that are connected by an edge,
    ///   then filter each node against its inline prop predicates.  Only
    ///   correlated pairs are yielded — this is the key difference from the old
    ///   `scan_match_create` which treated every node as an independent
    ///   candidate and then took a full Cartesian product.
    ///
    /// Patterns beyond a single hop are not yet supported and return an error.
    pub fn scan_match_create_rows(
        &self,
        mc: &MatchCreateStatement,
    ) -> Result<Vec<HashMap<String, NodeId>>> {
        // Start with a single empty row (identity for cross-join).
        let mut accumulated: Vec<HashMap<String, NodeId>> = vec![HashMap::new()];

        for pat in &mc.match_patterns {
            if pat.rels.is_empty() {
                // ── Node-only pattern: collect candidates per variable, then
                //    cross-join into accumulated rows. ──────────────────────
                //
                // Collect each named node variable's candidate list.
                let mut per_var: Vec<(String, Vec<NodeId>)> = Vec::new();

                for node_pat in &pat.nodes {
                    if node_pat.var.is_empty() {
                        continue;
                    }

                    // SPA-211: when no label is specified, scan all registered
                    // labels so that unlabeled MATCH patterns find nodes of
                    // any type (instead of silently returning empty).
                    let scan_label_ids: Vec<u32> = if node_pat.labels.is_empty() {
                        self.snapshot
                            .catalog
                            .list_labels()?
                            .into_iter()
                            .map(|(id, _)| id as u32)
                            .collect()
                    } else {
                        let label = node_pat.labels.first().cloned().unwrap_or_default();
                        match self.snapshot.catalog.get_label(&label)? {
                            Some(id) => vec![id as u32],
                            None => {
                                // No nodes can match → entire MATCH yields nothing.
                                return Ok(vec![]);
                            }
                        }
                    };

                    let filter_col_ids: Vec<u32> = node_pat
                        .props
                        .iter()
                        .map(|p| prop_name_to_col_id(&p.key))
                        .collect();

                    let mut matching_ids: Vec<NodeId> = Vec::new();
                    for label_id in scan_label_ids {
                        let hwm = self.snapshot.store.hwm_for_label(label_id)?;
                        for slot in 0..hwm {
                            let node_id = NodeId(((label_id as u64) << 32) | slot);

                            if self.is_node_tombstoned(node_id) {
                                continue;
                            }
                            if !self.node_matches_prop_filter(
                                node_id,
                                &filter_col_ids,
                                &node_pat.props,
                            ) {
                                continue;
                            }

                            matching_ids.push(node_id);
                        }
                    }

                    if matching_ids.is_empty() {
                        // No matching nodes → entire MATCH is empty.
                        return Ok(vec![]);
                    }

                    per_var.push((node_pat.var.clone(), matching_ids));
                }

                // Cross-join the per_var candidates into accumulated.
                // `candidates` is guaranteed non-empty (checked above), so the result
                // will be non-empty as long as `accumulated` is non-empty.
                for (var, candidates) in per_var {
                    let mut next: Vec<HashMap<String, NodeId>> = Vec::new();
                    for row in &accumulated {
                        for &node_id in &candidates {
                            let mut new_row = row.clone();
                            new_row.insert(var.clone(), node_id);
                            next.push(new_row);
                        }
                    }
                    accumulated = next;
                }
            } else if pat.rels.len() == 1 && pat.nodes.len() == 2 {
                // ── Single-hop relationship pattern: traverse CSR + delta edges
                //    to produce correlated (src, dst) pairs. ─────────────────
                let src_node_pat = &pat.nodes[0];
                let dst_node_pat = &pat.nodes[1];
                let rel_pat = &pat.rels[0];

                // Only outgoing direction is supported for MATCH…CREATE traversal.
                if rel_pat.dir != sparrowdb_cypher::ast::EdgeDir::Outgoing {
                    return Err(sparrowdb_common::Error::Unimplemented);
                }

                let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
                let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();

                let src_label_id: u32 = match self.snapshot.catalog.get_label(&src_label)? {
                    Some(id) => id as u32,
                    None => return Ok(vec![]),
                };
                let dst_label_id: u32 = match self.snapshot.catalog.get_label(&dst_label)? {
                    Some(id) => id as u32,
                    None => return Ok(vec![]),
                };

                let src_filter_cols: Vec<u32> = src_node_pat
                    .props
                    .iter()
                    .map(|p| prop_name_to_col_id(&p.key))
                    .collect();
                let dst_filter_cols: Vec<u32> = dst_node_pat
                    .props
                    .iter()
                    .map(|p| prop_name_to_col_id(&p.key))
                    .collect();

                // SPA-185: resolve per-type rel table for delta and CSR reads.
                let rel_lookup =
                    self.resolve_rel_table_id(src_label_id, dst_label_id, &rel_pat.rel_type);
                if matches!(rel_lookup, RelTableLookup::NotFound) {
                    return Ok(vec![]);
                }

                // Build a src_slot → Vec<dst_slot> adjacency map from the delta log once,
                // filtering by src_label to avoid O(N*M) scanning inside the outer loop.
                let delta_adj: HashMap<u64, Vec<u64>> = {
                    let records: Vec<DeltaRecord> = match rel_lookup {
                        RelTableLookup::Found(rtid) => self.read_delta_for(rtid),
                        _ => self.read_delta_all(),
                    };
                    let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
                    for r in records {
                        let s = r.src.0;
                        let s_label = (s >> 32) as u32;
                        if s_label == src_label_id {
                            let s_slot = s & 0xFFFF_FFFF;
                            adj.entry(s_slot).or_default().push(r.dst.0 & 0xFFFF_FFFF);
                        }
                    }
                    adj
                };

                let hwm_src = self.snapshot.store.hwm_for_label(src_label_id)?;

                // Pairs yielded by this pattern for cross-join below.
                let mut pattern_rows: Vec<HashMap<String, NodeId>> = Vec::new();

                for src_slot in 0..hwm_src {
                    // SPA-254: check per-query deadline at every slot boundary.
                    self.check_deadline()?;

                    let src_node = NodeId(((src_label_id as u64) << 32) | src_slot);

                    if self.is_node_tombstoned(src_node) {
                        continue;
                    }
                    if !self.node_matches_prop_filter(
                        src_node,
                        &src_filter_cols,
                        &src_node_pat.props,
                    ) {
                        continue;
                    }

                    // Collect outgoing neighbours (CSR + delta adjacency map).
                    let csr_neighbors_vec: Vec<u64> = match rel_lookup {
                        RelTableLookup::Found(rtid) => self.csr_neighbors(rtid, src_slot),
                        _ => self.csr_neighbors_all(src_slot),
                    };
                    let empty: Vec<u64> = Vec::new();
                    let delta_neighbors: &[u64] =
                        delta_adj.get(&src_slot).map_or(&empty, |v| v.as_slice());

                    let mut seen: HashSet<u64> = HashSet::new();
                    for &dst_slot in csr_neighbors_vec.iter().chain(delta_neighbors.iter()) {
                        if !seen.insert(dst_slot) {
                            continue;
                        }
                        let dst_node = NodeId(((dst_label_id as u64) << 32) | dst_slot);

                        if self.is_node_tombstoned(dst_node) {
                            continue;
                        }
                        if !self.node_matches_prop_filter(
                            dst_node,
                            &dst_filter_cols,
                            &dst_node_pat.props,
                        ) {
                            continue;
                        }

                        let mut row: HashMap<String, NodeId> = HashMap::new();

                        // When src and dst use the same variable (self-loop pattern),
                        // the edge must actually be a self-loop (src == dst).
                        if !src_node_pat.var.is_empty()
                            && !dst_node_pat.var.is_empty()
                            && src_node_pat.var == dst_node_pat.var
                        {
                            if src_node != dst_node {
                                continue;
                            }
                            row.insert(src_node_pat.var.clone(), src_node);
                        } else {
                            if !src_node_pat.var.is_empty() {
                                row.insert(src_node_pat.var.clone(), src_node);
                            }
                            if !dst_node_pat.var.is_empty() {
                                row.insert(dst_node_pat.var.clone(), dst_node);
                            }
                        }
                        pattern_rows.push(row);
                    }
                }

                if pattern_rows.is_empty() {
                    return Ok(vec![]);
                }

                // Cross-join pattern_rows into accumulated, enforcing shared-variable
                // constraints: if a variable appears in both acc_row and pat_row, only
                // keep combinations where they agree on the same NodeId.
                let mut next: Vec<HashMap<String, NodeId>> = Vec::new();
                for acc_row in &accumulated {
                    'outer: for pat_row in &pattern_rows {
                        // Reject combinations where shared variables disagree.
                        for (k, v) in pat_row {
                            if let Some(existing) = acc_row.get(k) {
                                if existing != v {
                                    continue 'outer;
                                }
                            }
                        }
                        let mut new_row = acc_row.clone();
                        new_row.extend(pat_row.iter().map(|(k, v)| (k.clone(), *v)));
                        next.push(new_row);
                    }
                }
                accumulated = next;
            } else {
                // Multi-hop patterns not yet supported for MATCH…CREATE.
                return Err(sparrowdb_common::Error::Unimplemented);
            }
        }

        Ok(accumulated)
    }

    /// Scan the MATCH patterns of a `MatchMergeRelStatement` and return
    /// correlated `(variable → NodeId)` binding rows — identical semantics to
    /// `scan_match_create_rows` but taking the MERGE form's match patterns (SPA-233).
    pub fn scan_match_merge_rel_rows(
        &self,
        mm: &MatchMergeRelStatement,
    ) -> Result<Vec<HashMap<String, NodeId>>> {
        // Reuse scan_match_create_rows by wrapping the MERGE patterns in a
        // MatchCreateStatement with an empty (no-op) CREATE body.
        let proxy = MatchCreateStatement {
            match_patterns: mm.match_patterns.clone(),
            match_props: vec![],
            create: CreateStatement {
                nodes: vec![],
                edges: vec![],
            },
        };
        self.scan_match_create_rows(&proxy)
    }

    // ── UNWIND ─────────────────────────────────────────────────────────────────

    fn execute_unwind(&self, u: &UnwindStatement) -> Result<QueryResult> {
        use crate::operators::{Operator, UnwindOperator};

        // Evaluate the list expression to a Vec<Value>.
        let values = eval_list_expr(&u.expr, &self.params)?;

        // Determine the output column name from the RETURN clause.
        let column_names = extract_return_column_names(&u.return_clause.items);

        if values.is_empty() {
            return Ok(QueryResult::empty(column_names));
        }

        let mut op = UnwindOperator::new(u.alias.clone(), values);
        let chunks = op.collect_all()?;

        // Materialize: for each chunk/group/row, project the RETURN columns.
        //
        // Only fall back to the UNWIND alias value when the output column
        // actually corresponds to the alias variable.  Returning a value for
        // an unrelated variable (e.g. `RETURN y` when alias is `x`) would
        // silently produce wrong results instead of NULL.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for chunk in &chunks {
            for group in &chunk.groups {
                let n = group.len();
                for row_idx in 0..n {
                    let row = u
                        .return_clause
                        .items
                        .iter()
                        .map(|item| {
                            // Determine whether this RETURN item refers to the
                            // alias variable produced by UNWIND.
                            let is_alias = match &item.expr {
                                Expr::Var(name) => name == &u.alias,
                                _ => false,
                            };
                            if is_alias {
                                group.get_value(&u.alias, row_idx).unwrap_or(Value::Null)
                            } else {
                                // Variable is not in scope for this UNWIND —
                                // return NULL rather than leaking the alias value.
                                Value::Null
                            }
                        })
                        .collect();
                    rows.push(row);
                }
            }
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
    }

    // ── CREATE node execution ─────────────────────────────────────────────────

    /// Execute a `CREATE` statement, auto-registering labels as needed (SPA-156).
    ///
    /// For each node in the CREATE clause:
    /// 1. Look up (or create) its primary label in the catalog.
    /// 2. Convert inline properties to `(col_id, StoreValue)` pairs using the
    ///    same FNV-1a hash used by `WriteTx::merge_node`.
    /// 3. Write the node to the node store.
    fn execute_create(&mut self, create: &CreateStatement) -> Result<QueryResult> {
        for node in &create.nodes {
            // Resolve the primary label, creating it if absent.
            let label = node.labels.first().cloned().unwrap_or_default();

            // SPA-208: reject reserved __SO_ label prefix.
            if is_reserved_label(&label) {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "invalid argument: label \"{label}\" is reserved — the __SO_ prefix is for internal use only"
                )));
            }

            let label_id: u32 = match self.snapshot.catalog.get_label(&label)? {
                Some(id) => id as u32,
                None => self.snapshot.catalog.create_label(&label)? as u32,
            };

            // Convert AST props to (col_id, StoreValue) pairs.
            // Property values are full expressions (e.g. `datetime()`),
            // evaluated with an empty binding map.
            let empty_bindings: HashMap<String, Value> = HashMap::new();
            let props: Vec<(u32, StoreValue)> = node
                .props
                .iter()
                .map(|entry| {
                    let col_id = prop_name_to_col_id(&entry.key);
                    let val = eval_expr(&entry.value, &empty_bindings);
                    let store_val = value_to_store_value(val);
                    (col_id, store_val)
                })
                .collect();

            // SPA-234: enforce UNIQUE constraints declared via
            // `CREATE CONSTRAINT ON (n:Label) ASSERT n.property IS UNIQUE`.
            // For each constrained (label_id, col_id) pair, check whether the
            // incoming value already exists in the property index.  If so,
            // return a constraint-violation error before writing the node.
            //
            // Only inline-encodable types (Int64 and short Bytes ≤ 7 bytes)
            // are checked via the prop_index fast path.  Float values and
            // long strings require heap storage and cannot be encoded with
            // to_u64(); for those types we return an explicit error rather
            // than panicking (StoreValue::Float::to_u64 is documented to
            // panic for heap-backed values).
            for (col_id, store_val) in &props {
                if self.unique_constraints.contains(&(label_id, *col_id)) {
                    let raw = match store_val {
                        StoreValue::Int64(_) => store_val.to_u64(),
                        StoreValue::Bytes(b) if b.len() <= 7 => store_val.to_u64(),
                        StoreValue::Bytes(_) => {
                            return Err(sparrowdb_common::Error::InvalidArgument(
                                "UNIQUE constraints on string values longer than 7 bytes are not yet supported".into(),
                            ));
                        }
                        StoreValue::Float(_) => {
                            return Err(sparrowdb_common::Error::InvalidArgument(
                                "UNIQUE constraints on float values are not yet supported".into(),
                            ));
                        }
                    };
                    if !self
                        .prop_index
                        .borrow()
                        .lookup(label_id, *col_id, raw)
                        .is_empty()
                    {
                        return Err(sparrowdb_common::Error::InvalidArgument(format!(
                            "unique constraint violation: label \"{label}\" already has a node with the same value for this property"
                        )));
                    }
                }
            }

            let node_id = self.snapshot.store.create_node(label_id, &props)?;
            // SPA-234: after writing, insert new values into the prop_index so
            // that subsequent creates in the same session also respect the
            // UNIQUE constraint (the index may be stale if built before this
            // node was written).
            {
                let slot =
                    sparrowdb_storage::property_index::PropertyIndex::node_id_to_slot(node_id);
                let mut idx = self.prop_index.borrow_mut();
                for (col_id, store_val) in &props {
                    if self.unique_constraints.contains(&(label_id, *col_id)) {
                        // Only insert inline-encodable values; Float/long Bytes
                        // were already rejected above before create_node was called.
                        let raw = match store_val {
                            StoreValue::Int64(_) => store_val.to_u64(),
                            StoreValue::Bytes(b) if b.len() <= 7 => store_val.to_u64(),
                            _ => continue,
                        };
                        idx.insert(label_id, *col_id, slot, raw);
                    }
                }
            }
            // Update cached row count for the planner (SPA-new).
            *self
                .snapshot
                .label_row_counts
                .entry(label_id as LabelId)
                .or_insert(0) += 1;
        }
        Ok(QueryResult::empty(vec![]))
    }

    fn execute_create_index(&mut self, label: &str, property: &str) -> Result<QueryResult> {
        let label_id: u32 = match self.snapshot.catalog.get_label(label)? {
            Some(id) => id as u32,
            None => return Ok(QueryResult::empty(vec![])),
        };
        let col_id = col_id_of(property);
        self.prop_index
            .borrow_mut()
            .build_for(&self.snapshot.store, label_id, col_id)?;
        Ok(QueryResult::empty(vec![]))
    }

    /// Execute `CREATE CONSTRAINT ON (n:Label) ASSERT n.property IS UNIQUE` (SPA-234).
    ///
    /// Records `(label_id, col_id)` in `self.unique_constraints` so that
    /// subsequent `execute_create` calls reject duplicate values.  Also builds
    /// the backing prop-index for that pair (needed to check existence cheaply).
    /// If the label does not yet exist in the catalog it is auto-created so that
    /// later `CREATE` statements can register against the constraint.
    fn execute_create_constraint(&mut self, label: &str, property: &str) -> Result<QueryResult> {
        let label_id: u32 = match self.snapshot.catalog.get_label(label)? {
            Some(id) => id as u32,
            None => self.snapshot.catalog.create_label(label)? as u32,
        };
        let col_id = col_id_of(property);

        // Build the property index for this (label_id, col_id) pair so that
        // uniqueness checks in execute_create can use O(log n) lookups.
        self.prop_index
            .borrow_mut()
            .build_for(&self.snapshot.store, label_id, col_id)?;

        // Register the constraint.
        self.unique_constraints.insert((label_id, col_id));

        Ok(QueryResult::empty(vec![]))
    }

    // ── UNION ─────────────────────────────────────────────────────────────────

    /// Execute `stmt1 UNION [ALL] stmt2`.
    ///
    /// Concatenates the row sets from both sides.  When `!all`, duplicate rows
    /// are eliminated using the same `deduplicate_rows` logic used by DISTINCT.
    /// Both sides must produce the same number of columns; column names are taken
    /// from the left side.
    fn execute_union(&mut self, u: UnionStatement) -> Result<QueryResult> {
        let left_result = self.execute_bound(*u.left)?;
        let right_result = self.execute_bound(*u.right)?;

        // Validate column counts match.
        if !left_result.columns.is_empty()
            && !right_result.columns.is_empty()
            && left_result.columns.len() != right_result.columns.len()
        {
            return Err(sparrowdb_common::Error::InvalidArgument(format!(
                "UNION: left side has {} columns, right side has {}",
                left_result.columns.len(),
                right_result.columns.len()
            )));
        }

        let columns = if !left_result.columns.is_empty() {
            left_result.columns.clone()
        } else {
            right_result.columns.clone()
        };

        let mut rows = left_result.rows;
        rows.extend(right_result.rows);

        if !u.all {
            deduplicate_rows(&mut rows);
        }

        Ok(QueryResult { columns, rows })
    }

    // ── WITH clause pipeline ──────────────────────────────────────────────────

    /// Execute `MATCH … WITH expr AS alias [WHERE pred] … RETURN …`.
    ///
    /// 1. Scan MATCH patterns → collect intermediate rows as `Vec<HashMap<String, Value>>`.
    /// 2. Project each row through the WITH items (evaluate expr, bind to alias).
    /// 3. Apply WITH WHERE predicate on the projected map.
    /// 4. Evaluate RETURN expressions against the projected map.
    fn execute_match_with(&self, m: &MatchWithStatement) -> Result<QueryResult> {
        // Step 1: collect intermediate rows from MATCH scan.
        let intermediate = self.collect_match_rows_for_with(
            &m.match_patterns,
            m.match_where.as_ref(),
            &m.with_clause,
        )?;

        // Step 2: check if WITH clause has aggregate expressions.
        // If so, we aggregate the intermediate rows first, producing one output row
        // per unique grouping key.
        let has_agg = m
            .with_clause
            .items
            .iter()
            .any(|item| is_aggregate_expr(&item.expr));

        let projected: Vec<HashMap<String, Value>> = if has_agg {
            // Aggregate the intermediate rows into a set of projected rows.
            let agg_rows = self.aggregate_with_items(&intermediate, &m.with_clause.items);
            // Apply WHERE filter on the aggregated rows.
            agg_rows
                .into_iter()
                .filter(|with_vals| {
                    if let Some(ref where_expr) = m.with_clause.where_clause {
                        let mut with_vals_p = with_vals.clone();
                        with_vals_p.extend(self.dollar_params());
                        self.eval_where_graph(where_expr, &with_vals_p)
                    } else {
                        true
                    }
                })
                .map(|mut with_vals| {
                    with_vals.extend(self.dollar_params());
                    with_vals
                })
                .collect()
        } else {
            // Non-aggregate path: project each row through the WITH items.
            let mut projected: Vec<HashMap<String, Value>> = Vec::new();
            for row_vals in &intermediate {
                let mut with_vals: HashMap<String, Value> = HashMap::new();
                for item in &m.with_clause.items {
                    let val = self.eval_expr_graph(&item.expr, row_vals);
                    with_vals.insert(item.alias.clone(), val);
                    // SPA-134: if the WITH item is a bare Var (e.g. `n AS person`),
                    // also inject the NodeRef under the alias so that EXISTS subqueries
                    // in a subsequent WHERE clause can resolve the source node.
                    if let sparrowdb_cypher::ast::Expr::Var(ref src_var) = item.expr {
                        if let Some(node_ref) = row_vals.get(src_var) {
                            if matches!(node_ref, Value::NodeRef(_)) {
                                with_vals.insert(item.alias.clone(), node_ref.clone());
                                with_vals.insert(
                                    format!("{}.__node_id__", item.alias),
                                    node_ref.clone(),
                                );
                            }
                        }
                        // Also check __node_id__ key.
                        let nid_key = format!("{src_var}.__node_id__");
                        if let Some(node_ref) = row_vals.get(&nid_key) {
                            with_vals
                                .insert(format!("{}.__node_id__", item.alias), node_ref.clone());
                        }
                    }
                }
                if let Some(ref where_expr) = m.with_clause.where_clause {
                    let mut with_vals_p = with_vals.clone();
                    with_vals_p.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &with_vals_p) {
                        continue;
                    }
                }
                // Merge dollar_params into the projected row so that downstream
                // RETURN/ORDER-BY/SKIP/LIMIT expressions can resolve $param references.
                with_vals.extend(self.dollar_params());
                projected.push(with_vals);
            }
            projected
        };

        // Step 3: project RETURN from the WITH-projected rows.
        let column_names = extract_return_column_names(&m.return_clause.items);

        // Apply ORDER BY on the projected rows (which still have all WITH aliases)
        // before projecting down to RETURN columns — this allows ORDER BY on columns
        // that are not in the RETURN clause (e.g. ORDER BY age when only name is returned).
        let mut ordered_projected = projected;
        if !m.order_by.is_empty() {
            ordered_projected.sort_by(|a, b| {
                for (expr, dir) in &m.order_by {
                    let val_a = eval_expr(expr, a);
                    let val_b = eval_expr(expr, b);
                    let cmp = compare_values(&val_a, &val_b);
                    let cmp = if *dir == SortDir::Desc {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply SKIP / LIMIT before final projection.
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(ordered_projected.len());
            ordered_projected.drain(0..skip);
        }
        if let Some(lim) = m.limit {
            ordered_projected.truncate(lim as usize);
        }

        let mut rows: Vec<Vec<Value>> = ordered_projected
            .iter()
            .map(|with_vals| {
                m.return_clause
                    .items
                    .iter()
                    .map(|item| self.eval_expr_graph(&item.expr, with_vals))
                    .collect()
            })
            .collect();

        if m.distinct {
            deduplicate_rows(&mut rows);
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
    }

    /// Aggregate a set of raw scan rows through a list of WITH items that
    /// include aggregate expressions (COUNT(*), collect(), etc.).
    ///
    /// Returns one `HashMap<String, Value>` per unique grouping key.
    fn aggregate_with_items(
        &self,
        rows: &[HashMap<String, Value>],
        items: &[sparrowdb_cypher::ast::WithItem],
    ) -> Vec<HashMap<String, Value>> {
        // Classify each WITH item as key or aggregate.
        let key_indices: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, item)| !is_aggregate_expr(&item.expr))
            .map(|(i, _)| i)
            .collect();
        let agg_indices: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, item)| is_aggregate_expr(&item.expr))
            .map(|(i, _)| i)
            .collect();

        // Build groups.
        let mut group_keys: Vec<Vec<Value>> = Vec::new();
        let mut group_accum: Vec<Vec<Vec<Value>>> = Vec::new(); // [group][agg_pos] → values

        for row_vals in rows {
            let key: Vec<Value> = key_indices
                .iter()
                .map(|&i| eval_expr(&items[i].expr, row_vals))
                .collect();
            let group_idx = if let Some(pos) = group_keys.iter().position(|k| k == &key) {
                pos
            } else {
                group_keys.push(key);
                group_accum.push(vec![vec![]; agg_indices.len()]);
                group_keys.len() - 1
            };
            for (ai, &ri) in agg_indices.iter().enumerate() {
                match &items[ri].expr {
                    sparrowdb_cypher::ast::Expr::CountStar => {
                        group_accum[group_idx][ai].push(Value::Int64(1));
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, args }
                        if name.to_lowercase() == "collect" =>
                    {
                        let val = if !args.is_empty() {
                            eval_expr(&args[0], row_vals)
                        } else {
                            Value::Null
                        };
                        if !matches!(val, Value::Null) {
                            group_accum[group_idx][ai].push(val);
                        }
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, args }
                        if matches!(
                            name.to_lowercase().as_str(),
                            "count" | "sum" | "avg" | "min" | "max"
                        ) =>
                    {
                        let val = if !args.is_empty() {
                            eval_expr(&args[0], row_vals)
                        } else {
                            Value::Null
                        };
                        if !matches!(val, Value::Null) {
                            group_accum[group_idx][ai].push(val);
                        }
                    }
                    _ => {}
                }
            }
        }

        // If no rows were seen, still produce one output row for global aggregates
        // (e.g. COUNT(*) over an empty scan returns 0).
        if rows.is_empty() && key_indices.is_empty() {
            let mut out_row: HashMap<String, Value> = HashMap::new();
            for &ri in &agg_indices {
                let val = match &items[ri].expr {
                    sparrowdb_cypher::ast::Expr::CountStar => Value::Int64(0),
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "collect" =>
                    {
                        Value::List(vec![])
                    }
                    _ => Value::Int64(0),
                };
                out_row.insert(items[ri].alias.clone(), val);
            }
            return vec![out_row];
        }

        // Finalize each group.
        let mut result: Vec<HashMap<String, Value>> = Vec::new();
        for (gi, key_vals) in group_keys.iter().enumerate() {
            let mut out_row: HashMap<String, Value> = HashMap::new();
            // Insert key values.
            for (ki, &ri) in key_indices.iter().enumerate() {
                out_row.insert(items[ri].alias.clone(), key_vals[ki].clone());
            }
            // Finalize aggregates.
            for (ai, &ri) in agg_indices.iter().enumerate() {
                let accum = &group_accum[gi][ai];
                let val = match &items[ri].expr {
                    sparrowdb_cypher::ast::Expr::CountStar => Value::Int64(accum.len() as i64),
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "collect" =>
                    {
                        Value::List(accum.clone())
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "count" =>
                    {
                        Value::Int64(accum.len() as i64)
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "sum" =>
                    {
                        let sum: i64 = accum
                            .iter()
                            .filter_map(|v| {
                                if let Value::Int64(n) = v {
                                    Some(*n)
                                } else {
                                    None
                                }
                            })
                            .sum();
                        Value::Int64(sum)
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "min" =>
                    {
                        accum
                            .iter()
                            .min_by(|a, b| compare_values(a, b))
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "max" =>
                    {
                        accum
                            .iter()
                            .max_by(|a, b| compare_values(a, b))
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                };
                out_row.insert(items[ri].alias.clone(), val);
            }
            result.push(out_row);
        }
        result
    }

    /// Execute a multi-clause Cypher pipeline (SPA-134).
    ///
    /// Executes stages left-to-right, passing the intermediate row set from
    /// one stage to the next, then projects the final RETURN clause.
    fn execute_pipeline(&self, p: &PipelineStatement) -> Result<QueryResult> {
        // Step 1: Produce the initial row set from the leading clause.
        let mut current_rows: Vec<HashMap<String, Value>> =
            if let Some((expr, alias)) = &p.leading_unwind {
                // UNWIND-led pipeline: expand the list into individual rows.
                let values = eval_list_expr(expr, &self.params)?;
                values
                    .into_iter()
                    .map(|v| {
                        let mut m = HashMap::new();
                        m.insert(alias.clone(), v);
                        m
                    })
                    .collect()
            } else if let Some(ref patterns) = p.leading_match {
                // MATCH-led pipeline: scan the graph.
                // For the pipeline we need a dummy WithClause (scan will collect all
                // col IDs needed by subsequent stages).  Use a wide scan that includes
                // NodeRefs for EXISTS support.
                self.collect_pipeline_match_rows(patterns, p.leading_where.as_ref())?
            } else {
                vec![HashMap::new()]
            };

        // Step 2: Execute pipeline stages in order.
        for stage in &p.stages {
            match stage {
                PipelineStage::With {
                    clause,
                    order_by,
                    skip,
                    limit,
                } => {
                    // SPA-134: ORDER BY in a WITH clause can reference variables from the
                    // PRECEDING stage (before projection).  Apply ORDER BY / SKIP / LIMIT
                    // on current_rows (pre-projection) first, then project.
                    if !order_by.is_empty() {
                        current_rows.sort_by(|a, b| {
                            for (expr, dir) in order_by {
                                let va = eval_expr(expr, a);
                                let vb = eval_expr(expr, b);
                                let cmp = compare_values(&va, &vb);
                                let cmp = if *dir == SortDir::Desc {
                                    cmp.reverse()
                                } else {
                                    cmp
                                };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                    if let Some(s) = skip {
                        let s = (*s as usize).min(current_rows.len());
                        current_rows.drain(0..s);
                    }
                    if let Some(l) = limit {
                        current_rows.truncate(*l as usize);
                    }

                    // Check for aggregates.
                    let has_agg = clause
                        .items
                        .iter()
                        .any(|item| is_aggregate_expr(&item.expr));
                    let next_rows: Vec<HashMap<String, Value>> = if has_agg {
                        let agg_rows = self.aggregate_with_items(&current_rows, &clause.items);
                        agg_rows
                            .into_iter()
                            .filter(|with_vals| {
                                if let Some(ref where_expr) = clause.where_clause {
                                    let mut wv = with_vals.clone();
                                    wv.extend(self.dollar_params());
                                    self.eval_where_graph(where_expr, &wv)
                                } else {
                                    true
                                }
                            })
                            .map(|mut with_vals| {
                                with_vals.extend(self.dollar_params());
                                with_vals
                            })
                            .collect()
                    } else {
                        let mut next_rows: Vec<HashMap<String, Value>> = Vec::new();
                        for row_vals in &current_rows {
                            let mut with_vals: HashMap<String, Value> = HashMap::new();
                            for item in &clause.items {
                                let val = self.eval_expr_graph(&item.expr, row_vals);
                                with_vals.insert(item.alias.clone(), val);
                                // Propagate NodeRef for bare variable aliases.
                                if let sparrowdb_cypher::ast::Expr::Var(ref src_var) = item.expr {
                                    if let Some(nr @ Value::NodeRef(_)) = row_vals.get(src_var) {
                                        with_vals.insert(item.alias.clone(), nr.clone());
                                        with_vals.insert(
                                            format!("{}.__node_id__", item.alias),
                                            nr.clone(),
                                        );
                                    }
                                    let nid_key = format!("{src_var}.__node_id__");
                                    if let Some(nr) = row_vals.get(&nid_key) {
                                        with_vals.insert(
                                            format!("{}.__node_id__", item.alias),
                                            nr.clone(),
                                        );
                                    }
                                }
                            }
                            if let Some(ref where_expr) = clause.where_clause {
                                let mut wv = with_vals.clone();
                                wv.extend(self.dollar_params());
                                if !self.eval_where_graph(where_expr, &wv) {
                                    continue;
                                }
                            }
                            with_vals.extend(self.dollar_params());
                            next_rows.push(with_vals);
                        }
                        next_rows
                    };
                    current_rows = next_rows;
                }
                PipelineStage::Match {
                    patterns,
                    where_clause,
                } => {
                    // Re-traverse the graph for each row in current_rows,
                    // substituting WITH-projected values for inline prop filters.
                    let mut next_rows: Vec<HashMap<String, Value>> = Vec::new();
                    for binding in &current_rows {
                        let new_rows = self.execute_pipeline_match_stage(
                            patterns,
                            where_clause.as_ref(),
                            binding,
                        )?;
                        next_rows.extend(new_rows);
                    }
                    current_rows = next_rows;
                }
                PipelineStage::Unwind { alias, new_alias } => {
                    // Unwind a list variable from the current row set.
                    let mut next_rows: Vec<HashMap<String, Value>> = Vec::new();
                    for row_vals in &current_rows {
                        let list_val = row_vals.get(alias.as_str()).cloned().unwrap_or(Value::Null);
                        let items = match list_val {
                            Value::List(v) => v,
                            other => vec![other],
                        };
                        for item in items {
                            let mut new_row = row_vals.clone();
                            new_row.insert(new_alias.clone(), item);
                            next_rows.push(new_row);
                        }
                    }
                    current_rows = next_rows;
                }
            }
        }

        // Step 3: PROJECT the RETURN clause.
        let column_names = extract_return_column_names(&p.return_clause.items);

        // Apply ORDER BY on the fully-projected rows before narrowing to RETURN columns.
        if !p.return_order_by.is_empty() {
            current_rows.sort_by(|a, b| {
                for (expr, dir) in &p.return_order_by {
                    let va = eval_expr(expr, a);
                    let vb = eval_expr(expr, b);
                    let cmp = compare_values(&va, &vb);
                    let cmp = if *dir == SortDir::Desc {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        if let Some(skip) = p.return_skip {
            let skip = (skip as usize).min(current_rows.len());
            current_rows.drain(0..skip);
        }
        if let Some(lim) = p.return_limit {
            current_rows.truncate(lim as usize);
        }

        let mut rows: Vec<Vec<Value>> = current_rows
            .iter()
            .map(|row_vals| {
                p.return_clause
                    .items
                    .iter()
                    .map(|item| self.eval_expr_graph(&item.expr, row_vals))
                    .collect()
            })
            .collect();

        if p.distinct {
            deduplicate_rows(&mut rows);
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
    }

    /// Collect all rows for a leading MATCH in a pipeline without a bound WithClause.
    ///
    /// Unlike `collect_match_rows_for_with`, this performs a wide scan that includes
    /// all stored column IDs for each label, and always injects NodeRef entries so
    /// EXISTS subqueries and subsequent MATCH stages can resolve node references.
    fn collect_pipeline_match_rows(
        &self,
        patterns: &[PathPattern],
        where_clause: Option<&Expr>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        if patterns.is_empty() {
            return Ok(vec![HashMap::new()]);
        }

        // For simplicity handle single-node pattern (no relationship hops in leading MATCH).
        let pat = &patterns[0];
        let node = &pat.nodes[0];
        let var_name = node.var.as_str();
        let label = node.labels.first().cloned().unwrap_or_default();

        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => return Ok(vec![]),
        };
        let hwm = self.snapshot.store.hwm_for_label(label_id)?;
        let col_ids: Vec<u32> = self
            .snapshot
            .store
            .col_ids_for_label(label_id)
            .unwrap_or_default();

        let mut result: Vec<HashMap<String, Value>> = Vec::new();
        for slot in 0..hwm {
            let node_id = NodeId(((label_id as u64) << 32) | slot);
            if self.is_node_tombstoned(node_id) {
                continue;
            }
            let props = match self.snapshot.store.get_node_raw(node_id, &col_ids) {
                Ok(p) => p,
                Err(_) => continue,
            };
            if !self.matches_prop_filter(&props, &node.props) {
                continue;
            }
            let mut row_vals = build_row_vals(&props, var_name, &col_ids, &self.snapshot.store);
            // Always inject NodeRef for EXISTS and next-stage MATCH.
            row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
            row_vals.insert(format!("{var_name}.__node_id__"), Value::NodeRef(node_id));

            if let Some(wexpr) = where_clause {
                let mut row_vals_p = row_vals.clone();
                row_vals_p.extend(self.dollar_params());
                if !self.eval_where_graph(wexpr, &row_vals_p) {
                    continue;
                }
            }
            result.push(row_vals);
        }
        Ok(result)
    }

    /// Execute a MATCH stage within a pipeline, given a set of variable bindings
    /// from the preceding WITH stage.
    ///
    /// For each node pattern in `patterns`:
    /// - Scan the label.
    /// - Filter by inline prop filters, substituting any value that matches
    ///   a variable name from `binding` (e.g. `{name: pname}` where `pname`
    ///   is bound in the preceding WITH).
    fn execute_pipeline_match_stage(
        &self,
        patterns: &[PathPattern],
        where_clause: Option<&Expr>,
        binding: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        if patterns.is_empty() {
            return Ok(vec![binding.clone()]);
        }

        let pat = &patterns[0];

        // Check if this is a relationship hop pattern.
        if !pat.rels.is_empty() {
            // Relationship traversal in a pipeline MATCH stage.
            // Currently supports single-hop: (src)-[:REL]->(dst)
            return self.execute_pipeline_match_hop(pat, where_clause, binding);
        }

        let node = &pat.nodes[0];
        let var_name = node.var.as_str();
        let label = node.labels.first().cloned().unwrap_or_default();

        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => return Ok(vec![]),
        };
        let hwm = self.snapshot.store.hwm_for_label(label_id)?;
        let col_ids: Vec<u32> = self
            .snapshot
            .store
            .col_ids_for_label(label_id)
            .unwrap_or_default();

        let mut result: Vec<HashMap<String, Value>> = Vec::new();
        let params = self.dollar_params();
        for slot in 0..hwm {
            let node_id = NodeId(((label_id as u64) << 32) | slot);
            if self.is_node_tombstoned(node_id) {
                continue;
            }
            let props = match self.snapshot.store.get_node_raw(node_id, &col_ids) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Evaluate inline prop filters, resolving variable references from binding.
            if !self.matches_prop_filter_with_binding(&props, &node.props, binding, &params) {
                continue;
            }

            let mut row_vals = build_row_vals(&props, var_name, &col_ids, &self.snapshot.store);
            // Merge binding variables so upstream aliases remain in scope.
            row_vals.extend(binding.clone());
            row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
            row_vals.insert(format!("{var_name}.__node_id__"), Value::NodeRef(node_id));

            if let Some(wexpr) = where_clause {
                let mut row_vals_p = row_vals.clone();
                row_vals_p.extend(params.clone());
                if !self.eval_where_graph(wexpr, &row_vals_p) {
                    continue;
                }
            }
            result.push(row_vals);
        }
        Ok(result)
    }

    /// Execute a single-hop relationship traversal in a pipeline MATCH stage.
    ///
    /// Handles `(src:Label {props})-[:REL]->(dst:Label {props})` where `src` or `dst`
    /// variable names may already be bound in `binding`.
    fn execute_pipeline_match_hop(
        &self,
        pat: &sparrowdb_cypher::ast::PathPattern,
        where_clause: Option<&Expr>,
        binding: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        if pat.nodes.len() < 2 || pat.rels.is_empty() {
            return Ok(vec![]);
        }
        let src_pat = &pat.nodes[0];
        let dst_pat = &pat.nodes[1];
        let rel_pat = &pat.rels[0];

        let src_label = src_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_pat.labels.first().cloned().unwrap_or_default();

        let src_label_id = match self.snapshot.catalog.get_label(&src_label)? {
            Some(id) => id as u32,
            None => return Ok(vec![]),
        };
        let dst_label_id = match self.snapshot.catalog.get_label(&dst_label)? {
            Some(id) => id as u32,
            None => return Ok(vec![]),
        };

        let src_col_ids: Vec<u32> = self
            .snapshot
            .store
            .col_ids_for_label(src_label_id)
            .unwrap_or_default();
        let dst_col_ids: Vec<u32> = self
            .snapshot
            .store
            .col_ids_for_label(dst_label_id)
            .unwrap_or_default();
        let params = self.dollar_params();

        // Find candidate src nodes.
        let src_candidates: Vec<NodeId> = {
            // If the src var is already bound as a NodeRef, use that directly.
            let bound_src = binding
                .get(&src_pat.var)
                .or_else(|| binding.get(&format!("{}.__node_id__", src_pat.var)));
            if let Some(Value::NodeRef(nid)) = bound_src {
                vec![*nid]
            } else {
                let hwm = self.snapshot.store.hwm_for_label(src_label_id)?;
                let mut cands = Vec::new();
                for slot in 0..hwm {
                    let node_id = NodeId(((src_label_id as u64) << 32) | slot);
                    if self.is_node_tombstoned(node_id) {
                        continue;
                    }
                    if let Ok(props) = self.snapshot.store.get_node_raw(node_id, &src_col_ids) {
                        if self.matches_prop_filter_with_binding(
                            &props,
                            &src_pat.props,
                            binding,
                            &params,
                        ) {
                            cands.push(node_id);
                        }
                    }
                }
                cands
            }
        };

        let rel_table_id = self.resolve_rel_table_id(src_label_id, dst_label_id, &rel_pat.rel_type);

        let mut result: Vec<HashMap<String, Value>> = Vec::new();
        for src_id in src_candidates {
            let src_slot = src_id.0 & 0xFFFF_FFFF;
            let dst_slots: Vec<u64> = match &rel_table_id {
                RelTableLookup::Found(rtid) => self.csr_neighbors(*rtid, src_slot),
                RelTableLookup::NotFound => continue,
                RelTableLookup::All => self.csr_neighbors_all(src_slot),
            };
            // Also check the delta.
            let delta_slots: Vec<u64> = self
                .read_delta_all()
                .into_iter()
                .filter(|r| {
                    let r_src_label = (r.src.0 >> 32) as u32;
                    let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                    r_src_label == src_label_id && r_src_slot == src_slot
                })
                .map(|r| r.dst.0 & 0xFFFF_FFFF)
                .collect();
            let all_slots: std::collections::HashSet<u64> =
                dst_slots.into_iter().chain(delta_slots).collect();

            for dst_slot in all_slots {
                let dst_id = NodeId(((dst_label_id as u64) << 32) | dst_slot);
                if self.is_node_tombstoned(dst_id) {
                    continue;
                }
                if let Ok(dst_props) = self.snapshot.store.get_node_raw(dst_id, &dst_col_ids) {
                    if !self.matches_prop_filter_with_binding(
                        &dst_props,
                        &dst_pat.props,
                        binding,
                        &params,
                    ) {
                        continue;
                    }
                    let src_props = self
                        .snapshot
                        .store
                        .get_node_raw(src_id, &src_col_ids)
                        .unwrap_or_default();
                    let mut row_vals = build_row_vals(
                        &src_props,
                        &src_pat.var,
                        &src_col_ids,
                        &self.snapshot.store,
                    );
                    row_vals.extend(build_row_vals(
                        &dst_props,
                        &dst_pat.var,
                        &dst_col_ids,
                        &self.snapshot.store,
                    ));
                    // Merge upstream bindings.
                    row_vals.extend(binding.clone());
                    row_vals.insert(src_pat.var.clone(), Value::NodeRef(src_id));
                    row_vals.insert(
                        format!("{}.__node_id__", src_pat.var),
                        Value::NodeRef(src_id),
                    );
                    row_vals.insert(dst_pat.var.clone(), Value::NodeRef(dst_id));
                    row_vals.insert(
                        format!("{}.__node_id__", dst_pat.var),
                        Value::NodeRef(dst_id),
                    );

                    if let Some(wexpr) = where_clause {
                        let mut row_vals_p = row_vals.clone();
                        row_vals_p.extend(params.clone());
                        if !self.eval_where_graph(wexpr, &row_vals_p) {
                            continue;
                        }
                    }
                    result.push(row_vals);
                }
            }
        }
        Ok(result)
    }

    /// Filter a node's props against a set of PropEntry filters, resolving variable
    /// references from `binding` before comparing.
    ///
    /// For example, `{name: pname}` where `pname` is a variable in `binding` will
    /// look up `binding["pname"]` and use it as the expected value.
    fn matches_prop_filter_with_binding(
        &self,
        props: &[(u32, u64)],
        filters: &[sparrowdb_cypher::ast::PropEntry],
        binding: &HashMap<String, Value>,
        params: &HashMap<String, Value>,
    ) -> bool {
        for f in filters {
            let col_id = prop_name_to_col_id(&f.key);
            let stored_raw = props.iter().find(|(c, _)| *c == col_id).map(|(_, v)| *v);

            // Evaluate the filter expression, first substituting from binding.
            let filter_val = match &f.value {
                sparrowdb_cypher::ast::Expr::Var(v) => {
                    // Variable reference — look up in binding.
                    binding.get(v).cloned().unwrap_or(Value::Null)
                }
                other => eval_expr(other, params),
            };

            let stored_val = stored_raw.map(|raw| decode_raw_val(raw, &self.snapshot.store));
            let matches = match (stored_val, &filter_val) {
                (Some(Value::String(a)), Value::String(b)) => &a == b,
                (Some(Value::Int64(a)), Value::Int64(b)) => a == *b,
                (Some(Value::Bool(a)), Value::Bool(b)) => a == *b,
                (Some(Value::Float64(a)), Value::Float64(b)) => a == *b,
                (None, Value::Null) => true,
                _ => false,
            };
            if !matches {
                return false;
            }
        }
        true
    }

    /// Scan a MATCH pattern and return one `HashMap<String, Value>` per matching row.
    ///
    /// Only simple single-node scans (no relationship hops) are supported for
    /// the WITH pipeline; complex patterns return `Err(Unimplemented)`.
    ///
    /// Keys in the returned map follow the `build_row_vals` convention:
    /// `"{var}.col_{col_id}"` → `Value::Int64(raw)`, plus any `"{var}.{prop}"` entries
    /// added for direct lookup in WITH expressions.
    fn collect_match_rows_for_with(
        &self,
        patterns: &[PathPattern],
        where_clause: Option<&Expr>,
        with_clause: &WithClause,
    ) -> Result<Vec<HashMap<String, Value>>> {
        if patterns.is_empty() || patterns[0].rels.is_empty() {
            let pat = &patterns[0];
            let node = &pat.nodes[0];
            let var_name = node.var.as_str();
            let label = node.labels.first().cloned().unwrap_or_default();
            let label_id = self
                .snapshot
                .catalog
                .get_label(&label)?
                .ok_or(sparrowdb_common::Error::NotFound)?;
            let label_id_u32 = label_id as u32;
            let hwm = self.snapshot.store.hwm_for_label(label_id_u32)?;

            // Collect col_ids needed by WHERE + WITH projections + inline prop filters.
            let mut all_col_ids: Vec<u32> = Vec::new();
            if let Some(wexpr) = &where_clause {
                collect_col_ids_from_expr(wexpr, &mut all_col_ids);
            }
            for item in &with_clause.items {
                collect_col_ids_from_expr(&item.expr, &mut all_col_ids);
            }
            for p in &node.props {
                let col_id = prop_name_to_col_id(&p.key);
                if !all_col_ids.contains(&col_id) {
                    all_col_ids.push(col_id);
                }
            }

            let mut result: Vec<HashMap<String, Value>> = Vec::new();
            for slot in 0..hwm {
                let node_id = NodeId(((label_id_u32 as u64) << 32) | slot);
                // SPA-216: use is_node_tombstoned() to avoid spurious NotFound
                // when tombstone_node() wrote col_0 only for the deleted slot.
                if self.is_node_tombstoned(node_id) {
                    continue;
                }
                let props = read_node_props(&self.snapshot.store, node_id, &all_col_ids)?;
                if !self.matches_prop_filter(&props, &node.props) {
                    continue;
                }
                let mut row_vals =
                    build_row_vals(&props, var_name, &all_col_ids, &self.snapshot.store);
                // SPA-134: inject NodeRef so eval_exists_subquery can resolve the
                // source node ID when EXISTS { } appears in MATCH WHERE or WITH WHERE.
                row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                row_vals.insert(format!("{var_name}.__node_id__"), Value::NodeRef(node_id));
                if let Some(wexpr) = &where_clause {
                    let mut row_vals_p = row_vals.clone();
                    row_vals_p.extend(self.dollar_params());
                    if !self.eval_where_graph(wexpr, &row_vals_p) {
                        continue;
                    }
                }
                result.push(row_vals);
            }
            Ok(result)
        } else {
            Err(sparrowdb_common::Error::Unimplemented)
        }
    }

    fn execute_match(&self, m: &MatchStatement) -> Result<QueryResult> {
        if m.pattern.is_empty() {
            // Standalone RETURN with no MATCH: evaluate each item as a scalar expression.
            let column_names = extract_return_column_names(&m.return_clause.items);
            let empty_vals: HashMap<String, Value> = HashMap::new();
            let row: Vec<Value> = m
                .return_clause
                .items
                .iter()
                .map(|item| eval_expr(&item.expr, &empty_vals))
                .collect();
            return Ok(QueryResult {
                columns: column_names,
                rows: vec![row],
            });
        }

        // Determine if this is a 2-hop query.
        let is_two_hop = m.pattern.len() == 1 && m.pattern[0].rels.len() == 2;
        let is_one_hop = m.pattern.len() == 1 && m.pattern[0].rels.len() == 1;
        // N-hop (3+): generalised iterative traversal (SPA-252).
        let is_n_hop = m.pattern.len() == 1 && m.pattern[0].rels.len() >= 3;
        // Detect variable-length path: single pattern with exactly 1 rel that has min_hops set.
        let is_var_len = m.pattern.len() == 1
            && m.pattern[0].rels.len() == 1
            && m.pattern[0].rels[0].min_hops.is_some();

        let column_names = extract_return_column_names(&m.return_clause.items);

        // SPA-136: multi-node-pattern MATCH (e.g. MATCH (a), (b) RETURN shortestPath(...))
        // requires a cross-product join across all patterns.
        let is_multi_pattern = m.pattern.len() > 1 && m.pattern.iter().all(|p| p.rels.is_empty());

        // ── Q7 degree-cache fast-path (SPA-272 wiring) ────────────────────────
        // Detect `MATCH (n:Label) RETURN … ORDER BY out_degree(n) DESC LIMIT k`
        // and short-circuit to `top_k_by_degree` — O(N log k) vs full edge scan.
        // Preconditions: single node pattern, no rels, no WHERE, DESC LIMIT set.
        if !is_var_len
            && !is_two_hop
            && !is_one_hop
            && !is_n_hop
            && !is_multi_pattern
            && m.pattern.len() == 1
            && m.pattern[0].rels.is_empty()
        {
            if let Some(result) = self.try_degree_sort_fastpath(m, &column_names)? {
                return Ok(result);
            }
        }

        if is_var_len {
            self.execute_variable_length(m, &column_names)
        } else if is_two_hop {
            self.execute_two_hop(m, &column_names)
        } else if is_one_hop {
            self.execute_one_hop(m, &column_names)
        } else if is_n_hop {
            self.execute_n_hop(m, &column_names)
        } else if is_multi_pattern {
            self.execute_multi_pattern_scan(m, &column_names)
        } else if m.pattern[0].rels.is_empty() {
            self.execute_scan(m, &column_names)
        } else {
            // Multi-pattern or complex query — fallback to sequential execution.
            self.execute_scan(m, &column_names)
        }
    }

    // ── Q7 degree-cache fast-path (SPA-272 Cypher wiring) ─────────────────────
    //
    // Detects `MATCH (n:Label) RETURN … ORDER BY out_degree(n) DESC LIMIT k`
    // and answers it directly from the pre-computed DegreeCache without scanning
    // edges.  Returns `None` when the pattern does not qualify; the caller then
    // falls through to the normal execution path.
    //
    // Qualifying conditions:
    //   1. Exactly one label on the node pattern.
    //   2. No WHERE clause (no post-filter that would change cardinality).
    //   3. No inline prop filters on the node pattern.
    //   4. ORDER BY has exactly one key: `out_degree(n)` or `degree(n)` DESC.
    //   5. LIMIT is Some(k) with k > 0.
    //   6. The variable in the ORDER BY call matches the node pattern variable.
    fn try_degree_sort_fastpath(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<Option<QueryResult>> {
        use sparrowdb_cypher::ast::SortDir;

        let pat = &m.pattern[0];
        let node = &pat.nodes[0];

        // Condition 1: exactly one label.
        let label = match node.labels.first() {
            Some(l) => l.clone(),
            None => return Ok(None),
        };

        // Condition 2: no WHERE clause.
        if m.where_clause.is_some() {
            return Ok(None);
        }

        // Condition 3: no inline prop filters.
        if !node.props.is_empty() {
            return Ok(None);
        }

        // Condition 4: ORDER BY has exactly one key that is out_degree(var) or degree(var) DESC.
        if m.order_by.len() != 1 {
            return Ok(None);
        }
        let (sort_expr, sort_dir) = &m.order_by[0];
        if *sort_dir != SortDir::Desc {
            return Ok(None);
        }
        let order_var = match sort_expr {
            Expr::FnCall { name, args } => {
                let name_lc = name.to_lowercase();
                if name_lc != "out_degree" && name_lc != "degree" {
                    return Ok(None);
                }
                match args.first() {
                    Some(Expr::Var(v)) => v.clone(),
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };

        // Condition 5: LIMIT must be set and > 0.
        let k = match m.limit {
            Some(k) if k > 0 => k as usize,
            _ => return Ok(None),
        };

        // Condition 6: ORDER BY variable must match the node pattern variable.
        let node_var = node.var.as_str();
        if !order_var.is_empty() && !node_var.is_empty() && order_var != node_var {
            return Ok(None);
        }

        // All conditions met — resolve label_id and call the cache.
        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => {
                return Ok(Some(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                }))
            }
        };

        tracing::debug!(
            label = %label,
            k = k,
            "SPA-272: degree-cache fast-path activated"
        );

        let top_k = self.top_k_by_degree(label_id, k)?;

        // Apply SKIP if present.
        let skip = m.skip.unwrap_or(0) as usize;
        let top_k = if skip >= top_k.len() {
            &[][..]
        } else {
            &top_k[skip..]
        };

        // Build result rows.  For each (slot, degree) project the RETURN clause.
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(top_k.len());
        for &(slot, degree) in top_k {
            let node_id = NodeId(((label_id as u64) << 32) | slot);

            // Skip tombstoned nodes (deleted nodes may still appear in cache).
            if self.is_node_tombstoned(node_id) {
                continue;
            }

            // Fetch all properties we might need for RETURN projection.
            let all_col_ids: Vec<u32> = collect_col_ids_from_columns(column_names);
            let nullable_props = self
                .snapshot
                .store
                .get_node_raw_nullable(node_id, &all_col_ids)?;
            let props: Vec<(u32, u64)> = nullable_props
                .iter()
                .filter_map(|&(col_id, opt)| opt.map(|v| (col_id, v)))
                .collect();

            // Project the RETURN columns.
            let row: Vec<Value> = column_names
                .iter()
                .map(|col_name| {
                    // Resolve out_degree(var) / degree(var) → degree value.
                    let degree_col_name_out = format!("out_degree({node_var})");
                    let degree_col_name_deg = format!("degree({node_var})");
                    if col_name == &degree_col_name_out
                        || col_name == &degree_col_name_deg
                        || col_name == "degree"
                        || col_name == "out_degree"
                    {
                        return Value::Int64(degree as i64);
                    }
                    // Resolve property accesses: "var.prop" or "prop".
                    let prop = col_name
                        .split_once('.')
                        .map(|(_, p)| p)
                        .unwrap_or(col_name.as_str());
                    let col_id = prop_name_to_col_id(prop);
                    props
                        .iter()
                        .find(|(c, _)| *c == col_id)
                        .map(|(_, v)| decode_raw_val(*v, &self.snapshot.store))
                        .unwrap_or(Value::Null)
                })
                .collect();

            rows.push(row);
        }

        Ok(Some(QueryResult {
            columns: column_names.to_vec(),
            rows,
        }))
    }

    // ── OPTIONAL MATCH (standalone) ───────────────────────────────────────────

    /// Execute `OPTIONAL MATCH pattern RETURN …`.
    ///
    /// Left-outer-join semantics: if the scan finds zero rows (label missing or
    /// no nodes), return exactly one row with NULL for every RETURN column.
    fn execute_optional_match(&self, om: &OptionalMatchStatement) -> Result<QueryResult> {
        use sparrowdb_common::Error;

        // Re-use execute_match by constructing a temporary MatchStatement.
        let match_stmt = MatchStatement {
            pattern: om.pattern.clone(),
            where_clause: om.where_clause.clone(),
            return_clause: om.return_clause.clone(),
            order_by: om.order_by.clone(),
            skip: om.skip,
            limit: om.limit,
            distinct: om.distinct,
        };

        let column_names = extract_return_column_names(&om.return_clause.items);

        let result = self.execute_match(&match_stmt);

        match result {
            Ok(qr) if !qr.rows.is_empty() => Ok(qr),
            // Empty result or label-not-found → one NULL row.
            Ok(_) | Err(Error::NotFound) | Err(Error::InvalidArgument(_)) => {
                let null_row = vec![Value::Null; column_names.len()];
                Ok(QueryResult {
                    columns: column_names,
                    rows: vec![null_row],
                })
            }
            Err(e) => Err(e),
        }
    }

    // ── MATCH … OPTIONAL MATCH … RETURN ──────────────────────────────────────

    /// Execute `MATCH (n) OPTIONAL MATCH (n)-[:R]->(m) RETURN …`.
    ///
    /// For each row produced by the leading MATCH, attempt to join against the
    /// OPTIONAL MATCH sub-pattern.  Rows with no join hits contribute one row
    /// with NULL values for the OPTIONAL MATCH variables.
    fn execute_match_optional_match(
        &self,
        mom: &MatchOptionalMatchStatement,
    ) -> Result<QueryResult> {
        let column_names = extract_return_column_names(&mom.return_clause.items);

        // ── Step 1: scan the leading MATCH to get all left-side rows ─────────
        // Build a temporary MatchStatement for the leading MATCH.
        let lead_return_items: Vec<ReturnItem> = mom
            .return_clause
            .items
            .iter()
            .filter(|item| {
                // Include items whose var is defined by the leading MATCH patterns.
                let lead_vars: Vec<&str> = mom
                    .match_patterns
                    .iter()
                    .flat_map(|p| p.nodes.iter().map(|n| n.var.as_str()))
                    .collect();
                match &item.expr {
                    Expr::PropAccess { var, .. } => lead_vars.contains(&var.as_str()),
                    Expr::Var(v) => lead_vars.contains(&v.as_str()),
                    _ => false,
                }
            })
            .cloned()
            .collect();

        // We need all column names from leading MATCH variables for the scan.
        // Collect all column names referenced by lead-side return items.
        let lead_col_names = extract_return_column_names(&lead_return_items);

        // Check that the leading MATCH label exists.
        if mom.match_patterns.is_empty() || mom.match_patterns[0].nodes.is_empty() {
            let null_row = vec![Value::Null; column_names.len()];
            return Ok(QueryResult {
                columns: column_names,
                rows: vec![null_row],
            });
        }
        let lead_node_pat = &mom.match_patterns[0].nodes[0];
        let lead_label = lead_node_pat.labels.first().cloned().unwrap_or_default();
        let lead_label_id = match self.snapshot.catalog.get_label(&lead_label)? {
            Some(id) => id as u32,
            None => {
                // The leading MATCH is non-optional: unknown label → 0 rows (not null).
                return Ok(QueryResult {
                    columns: column_names,
                    rows: vec![],
                });
            }
        };

        // Collect all col_ids needed for lead scan.
        let lead_all_col_ids: Vec<u32> = {
            let mut ids = collect_col_ids_from_columns(&lead_col_names);
            if let Some(ref wexpr) = mom.match_where {
                collect_col_ids_from_expr(wexpr, &mut ids);
            }
            for p in &lead_node_pat.props {
                let col_id = prop_name_to_col_id(&p.key);
                if !ids.contains(&col_id) {
                    ids.push(col_id);
                }
            }
            ids
        };

        let lead_hwm = self.snapshot.store.hwm_for_label(lead_label_id)?;
        let lead_var = lead_node_pat.var.as_str();

        // Collect lead rows as (slot, props) pairs.
        let mut lead_rows: Vec<(u64, Vec<(u32, u64)>)> = Vec::new();
        for slot in 0..lead_hwm {
            let node_id = NodeId(((lead_label_id as u64) << 32) | slot);
            // SPA-216: use is_node_tombstoned() to avoid spurious NotFound
            // when tombstone_node() wrote col_0 only for the deleted slot.
            if self.is_node_tombstoned(node_id) {
                continue;
            }
            let props = read_node_props(&self.snapshot.store, node_id, &lead_all_col_ids)?;
            if !self.matches_prop_filter(&props, &lead_node_pat.props) {
                continue;
            }
            if let Some(ref wexpr) = mom.match_where {
                let mut row_vals =
                    build_row_vals(&props, lead_var, &lead_all_col_ids, &self.snapshot.store);
                row_vals.extend(self.dollar_params());
                if !self.eval_where_graph(wexpr, &row_vals) {
                    continue;
                }
            }
            lead_rows.push((slot, props));
        }

        // ── Step 2: for each lead row, run the optional sub-pattern ──────────

        // Determine optional-side node variable and label.
        let opt_patterns = &mom.optional_patterns;

        // Determine optional-side variables from return clause.
        let opt_vars: Vec<String> = opt_patterns
            .iter()
            .flat_map(|p| p.nodes.iter().map(|n| n.var.clone()))
            .filter(|v| !v.is_empty())
            .collect();

        let mut result_rows: Vec<Vec<Value>> = Vec::new();

        for (lead_slot, lead_props) in &lead_rows {
            let lead_row_vals = build_row_vals(
                lead_props,
                lead_var,
                &lead_all_col_ids,
                &self.snapshot.store,
            );

            // Attempt the optional sub-pattern.
            // We only support the common case:
            //   (lead_var)-[:REL_TYPE]->(opt_var:Label)
            // where opt_patterns has exactly one path with one rel hop.
            let opt_sub_rows: Vec<HashMap<String, Value>> = if opt_patterns.len() == 1
                && opt_patterns[0].rels.len() == 1
                && opt_patterns[0].nodes.len() == 2
            {
                let opt_pat = &opt_patterns[0];
                let opt_src_pat = &opt_pat.nodes[0];
                let opt_dst_pat = &opt_pat.nodes[1];
                let opt_rel_pat = &opt_pat.rels[0];

                // Destination label — if not found, treat as 0 (no matches).
                let opt_dst_label = opt_dst_pat.labels.first().cloned().unwrap_or_default();
                let opt_dst_label_id: Option<u32> =
                    match self.snapshot.catalog.get_label(&opt_dst_label) {
                        Ok(Some(id)) => Some(id as u32),
                        _ => None,
                    };

                self.optional_one_hop_sub_rows(
                    *lead_slot,
                    lead_label_id,
                    opt_dst_label_id,
                    opt_src_pat,
                    opt_dst_pat,
                    opt_rel_pat,
                    &opt_vars,
                    &column_names,
                )
                .unwrap_or_default()
            } else {
                // Unsupported optional pattern → treat as no matches.
                vec![]
            };

            if opt_sub_rows.is_empty() {
                // No matches: emit lead row with NULLs for optional vars.
                let row: Vec<Value> = mom
                    .return_clause
                    .items
                    .iter()
                    .map(|item| {
                        let v = eval_expr(&item.expr, &lead_row_vals);
                        if v == Value::Null {
                            // Check if it's a lead-side expr that returned null
                            // because we don't have the value, vs an opt-side expr.
                            match &item.expr {
                                Expr::PropAccess { var, .. } | Expr::Var(var) => {
                                    if opt_vars.contains(var) {
                                        Value::Null
                                    } else {
                                        eval_expr(&item.expr, &lead_row_vals)
                                    }
                                }
                                _ => eval_expr(&item.expr, &lead_row_vals),
                            }
                        } else {
                            v
                        }
                    })
                    .collect();
                result_rows.push(row);
            } else {
                // Matches: emit one row per match with both sides populated.
                for opt_row_vals in opt_sub_rows {
                    let mut combined = lead_row_vals.clone();
                    combined.extend(opt_row_vals);
                    let row: Vec<Value> = mom
                        .return_clause
                        .items
                        .iter()
                        .map(|item| eval_expr(&item.expr, &combined))
                        .collect();
                    result_rows.push(row);
                }
            }
        }

        if mom.distinct {
            deduplicate_rows(&mut result_rows);
        }
        if let Some(skip) = mom.skip {
            let skip = (skip as usize).min(result_rows.len());
            result_rows.drain(0..skip);
        }
        if let Some(lim) = mom.limit {
            result_rows.truncate(lim as usize);
        }

        Ok(QueryResult {
            columns: column_names,
            rows: result_rows,
        })
    }

    /// Scan neighbors of `src_slot` via delta log + CSR for the optional 1-hop,
    /// returning one `HashMap<String,Value>` per matching destination node.
    #[allow(clippy::too_many_arguments)]
    fn optional_one_hop_sub_rows(
        &self,
        src_slot: u64,
        src_label_id: u32,
        dst_label_id: Option<u32>,
        _src_pat: &sparrowdb_cypher::ast::NodePattern,
        dst_node_pat: &sparrowdb_cypher::ast::NodePattern,
        rel_pat: &sparrowdb_cypher::ast::RelPattern,
        opt_vars: &[String],
        column_names: &[String],
    ) -> Result<Vec<HashMap<String, Value>>> {
        let dst_label_id = match dst_label_id {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let dst_var = dst_node_pat.var.as_str();
        let col_ids_dst = collect_col_ids_for_var(dst_var, column_names, dst_label_id);
        let _ = opt_vars;

        // SPA-185: resolve rel-type lookup once; use for both delta and CSR reads.
        let rel_lookup = self.resolve_rel_table_id(src_label_id, dst_label_id, &rel_pat.rel_type);

        // If the rel type was specified but not registered, no edges can exist.
        if matches!(rel_lookup, RelTableLookup::NotFound) {
            return Ok(vec![]);
        }

        let delta_neighbors: Vec<u64> = {
            let records: Vec<DeltaRecord> = match rel_lookup {
                RelTableLookup::Found(rtid) => self.read_delta_for(rtid),
                _ => self.read_delta_all(),
            };
            records
                .into_iter()
                .filter(|r| {
                    let r_src_label = (r.src.0 >> 32) as u32;
                    let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                    r_src_label == src_label_id && r_src_slot == src_slot
                })
                .map(|r| r.dst.0 & 0xFFFF_FFFF)
                .collect()
        };

        let csr_neighbors = match rel_lookup {
            RelTableLookup::Found(rtid) => self.csr_neighbors(rtid, src_slot),
            _ => self.csr_neighbors_all(src_slot),
        };
        let all_neighbors: Vec<u64> = csr_neighbors.into_iter().chain(delta_neighbors).collect();

        let mut seen: HashSet<u64> = HashSet::new();
        let mut sub_rows: Vec<HashMap<String, Value>> = Vec::new();

        for dst_slot in all_neighbors {
            if !seen.insert(dst_slot) {
                continue;
            }
            let dst_node = NodeId(((dst_label_id as u64) << 32) | dst_slot);
            let dst_props = read_node_props(&self.snapshot.store, dst_node, &col_ids_dst)?;
            if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                continue;
            }
            let row_vals = build_row_vals(&dst_props, dst_var, &col_ids_dst, &self.snapshot.store);
            sub_rows.push(row_vals);
        }

        Ok(sub_rows)
    }

    // ── Node-only scan (no relationships) ─────────────────────────────────────

    /// Execute a multi-pattern node-only MATCH by cross-joining each pattern's candidates.
    ///
    /// `MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) RETURN shortestPath(...)`
    /// produces one merged row per combination of matching nodes.  Each row contains both
    /// `"{var}" → Value::NodeRef(node_id)` (for `resolve_node_id_from_var`) and
    /// `"{var}.col_{hash}" → Value` entries (for property access via `eval_expr`).
    fn execute_multi_pattern_scan(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        // Collect candidate NodeIds per variable across all patterns.
        let mut per_var: Vec<(String, u32, Vec<NodeId>)> = Vec::new(); // (var, label_id, candidates)

        for pat in &m.pattern {
            if pat.nodes.is_empty() {
                continue;
            }
            let node = &pat.nodes[0];
            if node.var.is_empty() {
                continue;
            }
            let label = node.labels.first().cloned().unwrap_or_default();
            let label_id = match self.snapshot.catalog.get_label(&label)? {
                Some(id) => id as u32,
                None => return Ok(QueryResult::empty(column_names.to_vec())),
            };
            let filter_col_ids: Vec<u32> = node
                .props
                .iter()
                .map(|p| prop_name_to_col_id(&p.key))
                .collect();
            let params = self.dollar_params();
            let hwm = self.snapshot.store.hwm_for_label(label_id)?;
            let mut candidates: Vec<NodeId> = Vec::new();
            for slot in 0..hwm {
                let node_id = NodeId(((label_id as u64) << 32) | slot);
                if self.is_node_tombstoned(node_id) {
                    continue;
                }
                if filter_col_ids.is_empty() {
                    candidates.push(node_id);
                } else if let Ok(raw_props) =
                    self.snapshot.store.get_node_raw(node_id, &filter_col_ids)
                {
                    if matches_prop_filter_static(
                        &raw_props,
                        &node.props,
                        &params,
                        &self.snapshot.store,
                    ) {
                        candidates.push(node_id);
                    }
                }
            }
            if candidates.is_empty() {
                return Ok(QueryResult::empty(column_names.to_vec()));
            }
            per_var.push((node.var.clone(), label_id, candidates));
        }

        // Cross-product all candidates into row_vals maps.
        let mut accumulated: Vec<HashMap<String, Value>> = vec![HashMap::new()];
        for (var, _label_id, candidates) in &per_var {
            let mut next: Vec<HashMap<String, Value>> = Vec::new();
            for base_row in &accumulated {
                for &node_id in candidates {
                    let mut row = base_row.clone();
                    // Bind var as NodeRef (needed by resolve_node_id_from_var for shortestPath).
                    row.insert(var.clone(), Value::NodeRef(node_id));
                    row.insert(format!("{var}.__node_id__"), Value::NodeRef(node_id));
                    // Also store properties under "var.col_N" keys for eval_expr PropAccess.
                    let label_id = (node_id.0 >> 32) as u32;
                    let label_col_ids = self
                        .snapshot
                        .store
                        .col_ids_for_label(label_id)
                        .unwrap_or_default();
                    let nullable = self
                        .snapshot
                        .store
                        .get_node_raw_nullable(node_id, &label_col_ids)
                        .unwrap_or_default();
                    for &(col_id, opt_raw) in &nullable {
                        if let Some(raw) = opt_raw {
                            row.insert(
                                format!("{var}.col_{col_id}"),
                                decode_raw_val(raw, &self.snapshot.store),
                            );
                        }
                    }
                    next.push(row);
                }
            }
            accumulated = next;
        }

        // Apply WHERE clause.
        if let Some(ref where_expr) = m.where_clause {
            accumulated.retain(|row| self.eval_where_graph(where_expr, row));
        }

        // Inject runtime params into each row before projection.
        let dollar_params = self.dollar_params();
        if !dollar_params.is_empty() {
            for row in &mut accumulated {
                row.extend(dollar_params.clone());
            }
        }

        let mut rows = self.aggregate_rows_graph(&accumulated, &m.return_clause.items);

        // ORDER BY / LIMIT / SKIP.
        apply_order_by(&mut rows, m, column_names);
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(rows.len());
            rows.drain(0..skip);
        }
        if let Some(limit) = m.limit {
            rows.truncate(limit as usize);
        }

        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    fn execute_scan(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let node = &pat.nodes[0];

        // SPA-192/SPA-194: when no label is specified, scan ALL known labels and union
        // the results.  Delegate to the per-label helper for each label.
        if node.labels.is_empty() {
            return self.execute_scan_all_labels(m, column_names);
        }

        let label = node.labels.first().cloned().unwrap_or_default();
        // SPA-245: unknown label → 0 rows (standard Cypher semantics, not an error).
        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                })
            }
        };
        let label_id_u32 = label_id;

        let hwm = self.snapshot.store.hwm_for_label(label_id_u32)?;
        tracing::debug!(label = %label, hwm = hwm, "node scan start");

        // Collect all col_ids we need: RETURN columns + WHERE clause columns +
        // inline prop filter columns.
        let col_ids = collect_col_ids_from_columns(column_names);
        let mut all_col_ids: Vec<u32> = col_ids.clone();
        // Add col_ids referenced by the WHERE clause.
        if let Some(ref where_expr) = m.where_clause {
            collect_col_ids_from_expr(where_expr, &mut all_col_ids);
        }
        // Add col_ids for inline prop filters on the node pattern.
        for p in &node.props {
            let col_id = prop_name_to_col_id(&p.key);
            if !all_col_ids.contains(&col_id) {
                all_col_ids.push(col_id);
            }
        }

        let use_agg = has_aggregate_in_return(&m.return_clause.items);
        // SPA-196: id(n) requires a NodeRef in the row map.  The fast
        // project_row path only stores individual property columns, so it
        // cannot evaluate id().  Force the eval path whenever id() appears in
        // any RETURN item, even when no aggregation is requested.
        // SPA-213: bare variable projection also requires the eval path.
        let use_eval_path = use_agg || needs_node_ref_in_return(&m.return_clause.items);
        if use_eval_path {
            // Aggregate / eval expressions reference properties not captured by
            // column_names (e.g. collect(p.name) -> column "collect(p.name)").
            // Extract col_ids from every RETURN expression so the scan reads
            // all necessary columns.
            for item in &m.return_clause.items {
                collect_col_ids_from_expr(&item.expr, &mut all_col_ids);
            }
        }

        // SPA-213: bare node variable projection needs ALL stored columns for the label.
        // Collect them once before the scan loop so we can build a Value::Map per node.
        let bare_vars = bare_var_names_in_return(&m.return_clause.items);
        let all_label_col_ids: Vec<u32> = if !bare_vars.is_empty() {
            self.snapshot.store.col_ids_for_label(label_id_u32)?
        } else {
            vec![]
        };

        let mut raw_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        // SPA-249 (lazy build): ensure the property index is loaded for every
        // column referenced by inline prop filters before attempting a lookup.
        // Each build_for call is a cache-hit no-op after the first time.
        // We acquire and drop the mutable borrow before the immutable lookup below.
        for p in &node.props {
            let col_id = sparrowdb_common::col_id_of(&p.key);
            // Errors are suppressed inside build_for; index falls back to full scan.
            let _ =
                self.prop_index
                    .borrow_mut()
                    .build_for(&self.snapshot.store, label_id_u32, col_id);
        }

        // SPA-273: selectivity threshold — if the index would return more than
        // 10% of all rows for this label, it's cheaper to do a full scan and
        // avoid the extra slot-set construction overhead.  We use `hwm` as the
        // denominator (high-water mark = total allocated slots, which is an
        // upper bound on live row count).  When hwm == 0 the threshold never
        // fires (no rows exist).
        let selectivity_threshold: u64 = if hwm > 0 { (hwm / 10).max(1) } else { u64::MAX };

        // SPA-249: try to use the property equality index when there is exactly
        // one inline prop filter with an inline-encodable literal value.
        // Overflow strings (> 7 bytes) cannot be indexed, so they fall back to
        // full scan.  A WHERE clause is always applied per-slot afterward.
        //
        // SPA-273: discard candidates when they exceed the selectivity threshold
        // (index would scan >10% of rows — full scan is preferred).
        let index_candidate_slots: Option<Vec<u32>> = {
            let prop_index_ref = self.prop_index.borrow();
            let candidates = try_index_lookup_for_props(&node.props, label_id_u32, &prop_index_ref);
            match candidates {
                Some(ref slots) if slots.len() as u64 > selectivity_threshold => {
                    tracing::debug!(
                        label = %label,
                        candidates = slots.len(),
                        threshold = selectivity_threshold,
                        "SPA-273: index exceeds selectivity threshold — falling back to full scan"
                    );
                    None
                }
                other => other,
            }
        };

        // SPA-249 Phase 1b: when the inline-prop index has no candidates, try to
        // use the property index for a WHERE-clause equality predicate
        // (`WHERE n.prop = literal`).  The WHERE clause is still re-evaluated
        // per slot for correctness.
        //
        // We pre-build the index for any single-equality WHERE prop so the lazy
        // cache is populated before the immutable borrow below.
        if index_candidate_slots.is_none() {
            if let Some(wexpr) = m.where_clause.as_ref() {
                for prop_name in where_clause_eq_prop_names(wexpr, node.var.as_str()) {
                    let col_id = sparrowdb_common::col_id_of(prop_name);
                    let _ = self.prop_index.borrow_mut().build_for(
                        &self.snapshot.store,
                        label_id_u32,
                        col_id,
                    );
                }
            }
        }
        // SPA-273: apply the same selectivity threshold to WHERE-clause equality
        // index candidates.
        let where_eq_candidate_slots: Option<Vec<u32>> = if index_candidate_slots.is_none() {
            let prop_index_ref = self.prop_index.borrow();
            let candidates = m.where_clause.as_ref().and_then(|wexpr| {
                try_where_eq_index_lookup(wexpr, node.var.as_str(), label_id_u32, &prop_index_ref)
            });
            match candidates {
                Some(ref slots) if slots.len() as u64 > selectivity_threshold => {
                    tracing::debug!(
                        label = %label,
                        candidates = slots.len(),
                        threshold = selectivity_threshold,
                        "SPA-273: WHERE-eq index exceeds selectivity threshold — falling back to full scan"
                    );
                    None
                }
                other => other,
            }
        } else {
            None
        };

        // SPA-249 Phase 2: when neither equality path fired, try to use the
        // property index for a WHERE-clause range predicate (`>`, `>=`, `<`, `<=`,
        // or a compound AND of two half-open bounds on the same property).
        //
        // Pre-build for any range-predicate WHERE props before the immutable borrow.
        if index_candidate_slots.is_none() && where_eq_candidate_slots.is_none() {
            if let Some(wexpr) = m.where_clause.as_ref() {
                for prop_name in where_clause_range_prop_names(wexpr, node.var.as_str()) {
                    let col_id = sparrowdb_common::col_id_of(prop_name);
                    let _ = self.prop_index.borrow_mut().build_for(
                        &self.snapshot.store,
                        label_id_u32,
                        col_id,
                    );
                }
            }
        }
        let where_range_candidate_slots: Option<Vec<u32>> =
            if index_candidate_slots.is_none() && where_eq_candidate_slots.is_none() {
                let prop_index_ref = self.prop_index.borrow();
                m.where_clause.as_ref().and_then(|wexpr| {
                    try_where_range_index_lookup(
                        wexpr,
                        node.var.as_str(),
                        label_id_u32,
                        &prop_index_ref,
                    )
                })
            } else {
                None
            };

        // SPA-251 / SPA-274 (lazy text index): when the equality index has no
        // candidates (None), check whether the WHERE clause is a simple CONTAINS
        // or STARTS WITH predicate on a labeled node property, and use the text
        // index to narrow the slot set.  The WHERE clause is always re-evaluated
        // per slot afterward for correctness (tombstone filtering, compound
        // predicates, etc.).
        //
        // Pre-warm the text index for any text-predicate columns before the
        // immutable borrow below, mirroring the PropertyIndex lazy pattern.
        // Queries with no text predicates never call build_for and pay zero I/O.
        if index_candidate_slots.is_none()
            && where_eq_candidate_slots.is_none()
            && where_range_candidate_slots.is_none()
        {
            if let Some(wexpr) = m.where_clause.as_ref() {
                for prop_name in where_clause_text_prop_names(wexpr, node.var.as_str()) {
                    let col_id = sparrowdb_common::col_id_of(prop_name);
                    self.text_index.borrow_mut().build_for(
                        &self.snapshot.store,
                        label_id_u32,
                        col_id,
                    );
                }
            }
        }
        let text_candidate_slots: Option<Vec<u32>> = if index_candidate_slots.is_none()
            && where_eq_candidate_slots.is_none()
            && where_range_candidate_slots.is_none()
        {
            m.where_clause.as_ref().and_then(|wexpr| {
                let text_index_ref = self.text_index.borrow();
                try_text_index_lookup(wexpr, node.var.as_str(), label_id_u32, &text_index_ref)
            })
        } else {
            None
        };

        // Build an iterator over candidate slot values.  When the equality index
        // or text index narrows the set, iterate only those slots; otherwise
        // iterate 0..hwm.
        let slot_iter: Box<dyn Iterator<Item = u64>> =
            if let Some(ref slots) = index_candidate_slots {
                tracing::debug!(
                    label = %label,
                    candidates = slots.len(),
                    "SPA-249: property index fast path"
                );
                Box::new(slots.iter().map(|&s| s as u64))
            } else if let Some(ref slots) = where_eq_candidate_slots {
                tracing::debug!(
                    label = %label,
                    candidates = slots.len(),
                    "SPA-249 Phase 1b: WHERE equality index fast path"
                );
                Box::new(slots.iter().map(|&s| s as u64))
            } else if let Some(ref slots) = where_range_candidate_slots {
                tracing::debug!(
                    label = %label,
                    candidates = slots.len(),
                    "SPA-249 Phase 2: WHERE range index fast path"
                );
                Box::new(slots.iter().map(|&s| s as u64))
            } else if let Some(ref slots) = text_candidate_slots {
                tracing::debug!(
                    label = %label,
                    candidates = slots.len(),
                    "SPA-251: text index fast path"
                );
                Box::new(slots.iter().map(|&s| s as u64))
            } else {
                Box::new(0..hwm)
            };

        for slot in slot_iter {
            // SPA-254: check per-query deadline at every slot boundary.
            self.check_deadline()?;

            let node_id = NodeId(((label_id_u32 as u64) << 32) | slot);
            if slot < 1024 || slot % 10_000 == 0 {
                tracing::trace!(slot = slot, node_id = node_id.0, "scan emit");
            }

            // SPA-164/SPA-216: skip tombstoned nodes.  delete_node writes
            // u64::MAX into col_0 as the deletion sentinel; nodes in that state
            // must not appear in scan results.  Use is_node_tombstoned() rather
            // than a raw `get_node_raw(...)?` so that a short col_0 file (e.g.
            // when tombstone_node only wrote the deleted slot and did not
            // zero-pad up to the HWM) does not propagate a spurious NotFound
            // error for un-deleted nodes whose slots are beyond the file end.
            if self.is_node_tombstoned(node_id) {
                continue;
            }

            // Use nullable reads so that absent columns (property never written
            // for this node) are omitted from the row map rather than surfacing
            // as Err(NotFound).  Absent columns will evaluate to Value::Null in
            // eval_expr, enabling correct IS NULL / IS NOT NULL semantics.
            let nullable_props = self
                .snapshot
                .store
                .get_node_raw_nullable(node_id, &all_col_ids)?;
            let props: Vec<(u32, u64)> = nullable_props
                .iter()
                .filter_map(|&(col_id, opt)| opt.map(|v| (col_id, v)))
                .collect();

            // Apply inline prop filter from the pattern.
            if !self.matches_prop_filter(&props, &node.props) {
                continue;
            }

            // Apply WHERE clause.
            let var_name = node.var.as_str();
            if let Some(ref where_expr) = m.where_clause {
                let mut row_vals =
                    build_row_vals(&props, var_name, &all_col_ids, &self.snapshot.store);
                // Inject label metadata so labels(n) works in WHERE.
                if !var_name.is_empty() && !label.is_empty() {
                    row_vals.insert(
                        format!("{}.__labels__", var_name),
                        Value::List(vec![Value::String(label.clone())]),
                    );
                }
                // SPA-196: inject NodeRef so id(n) works in WHERE clauses.
                if !var_name.is_empty() {
                    row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                }
                // Inject runtime params so $param references in WHERE work.
                row_vals.extend(self.dollar_params());
                if !self.eval_where_graph(where_expr, &row_vals) {
                    continue;
                }
            }

            if use_eval_path {
                // Build eval_expr-compatible map for aggregation / id() path.
                let mut row_vals =
                    build_row_vals(&props, var_name, &all_col_ids, &self.snapshot.store);
                // Inject label metadata for aggregation.
                if !var_name.is_empty() && !label.is_empty() {
                    row_vals.insert(
                        format!("{}.__labels__", var_name),
                        Value::List(vec![Value::String(label.clone())]),
                    );
                }
                if !var_name.is_empty() {
                    // SPA-213: when this variable is returned bare, read all properties
                    // for the node and expose them as a Value::Map under the var key.
                    // Also keep NodeRef under __node_id__ so id(n) continues to work.
                    if bare_vars.contains(&var_name.to_string()) && !all_label_col_ids.is_empty() {
                        let all_nullable = self
                            .snapshot
                            .store
                            .get_node_raw_nullable(node_id, &all_label_col_ids)?;
                        let all_props: Vec<(u32, u64)> = all_nullable
                            .iter()
                            .filter_map(|&(col_id, opt)| opt.map(|v| (col_id, v)))
                            .collect();
                        row_vals.insert(
                            var_name.to_string(),
                            build_node_map(&all_props, &self.snapshot.store),
                        );
                    } else {
                        row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                    }
                    // Always store NodeRef under __node_id__ so id(n) works even when
                    // the var itself is a Map (SPA-213).
                    row_vals.insert(format!("{}.__node_id__", var_name), Value::NodeRef(node_id));
                }
                raw_rows.push(row_vals);
            } else {
                // Project RETURN columns directly (fast path).
                let row = project_row(
                    &props,
                    column_names,
                    &all_col_ids,
                    var_name,
                    &label,
                    &self.snapshot.store,
                );
                rows.push(row);
            }
        }

        if use_eval_path {
            rows = self.aggregate_rows_graph(&raw_rows, &m.return_clause.items);
        } else {
            if m.distinct {
                deduplicate_rows(&mut rows);
            }

            // ORDER BY
            apply_order_by(&mut rows, m, column_names);

            // SKIP
            if let Some(skip) = m.skip {
                let skip = (skip as usize).min(rows.len());
                rows.drain(0..skip);
            }

            // LIMIT
            if let Some(lim) = m.limit {
                rows.truncate(lim as usize);
            }
        }

        tracing::debug!(rows = rows.len(), "node scan complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── Label-less full scan: MATCH (n) RETURN … — SPA-192/SPA-194 ─────────
    //
    // When the node pattern carries no label filter we must scan every label
    // that is registered in the catalog and union the results.  Aggregation,
    // ORDER BY and LIMIT are applied once after the union so that e.g.
    // `count(n)` counts all nodes and `LIMIT k` returns exactly k rows across
    // all labels rather than k rows per label.

    fn execute_scan_all_labels(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        let all_labels = self.snapshot.catalog.list_labels()?;
        tracing::debug!(label_count = all_labels.len(), "label-less full scan start");

        let pat = &m.pattern[0];
        let node = &pat.nodes[0];
        let var_name = node.var.as_str();

        // Collect col_ids needed across all labels (same set for every label).
        let mut all_col_ids: Vec<u32> = collect_col_ids_from_columns(column_names);
        if let Some(ref where_expr) = m.where_clause {
            collect_col_ids_from_expr(where_expr, &mut all_col_ids);
        }
        for p in &node.props {
            let col_id = prop_name_to_col_id(&p.key);
            if !all_col_ids.contains(&col_id) {
                all_col_ids.push(col_id);
            }
        }

        let use_agg = has_aggregate_in_return(&m.return_clause.items);
        // SPA-213: bare variable also needs the eval path in label-less scan.
        let use_eval_path_all = use_agg || needs_node_ref_in_return(&m.return_clause.items);
        if use_eval_path_all {
            for item in &m.return_clause.items {
                collect_col_ids_from_expr(&item.expr, &mut all_col_ids);
            }
        }

        // SPA-213: detect bare var names for property-map projection.
        let bare_vars_all = bare_var_names_in_return(&m.return_clause.items);

        let mut raw_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for (label_id, label_name) in &all_labels {
            let label_id_u32 = *label_id as u32;
            let hwm = self.snapshot.store.hwm_for_label(label_id_u32)?;
            tracing::debug!(label = %label_name, hwm = hwm, "label-less scan: label slot");

            // SPA-213: read all col_ids for this label once per label.
            let all_label_col_ids_here: Vec<u32> = if !bare_vars_all.is_empty() {
                self.snapshot.store.col_ids_for_label(label_id_u32)?
            } else {
                vec![]
            };

            for slot in 0..hwm {
                // SPA-254: check per-query deadline at every slot boundary.
                self.check_deadline()?;

                let node_id = NodeId(((label_id_u32 as u64) << 32) | slot);

                // Skip tombstoned nodes (SPA-164/SPA-216): use
                // is_node_tombstoned() to avoid spurious NotFound when
                // tombstone_node() wrote col_0 only for the deleted slot.
                if self.is_node_tombstoned(node_id) {
                    continue;
                }

                let nullable_props = self
                    .snapshot
                    .store
                    .get_node_raw_nullable(node_id, &all_col_ids)?;
                let props: Vec<(u32, u64)> = nullable_props
                    .iter()
                    .filter_map(|&(col_id, opt)| opt.map(|v| (col_id, v)))
                    .collect();

                // Apply inline prop filter.
                if !self.matches_prop_filter(&props, &node.props) {
                    continue;
                }

                // Apply WHERE clause.
                if let Some(ref where_expr) = m.where_clause {
                    let mut row_vals =
                        build_row_vals(&props, var_name, &all_col_ids, &self.snapshot.store);
                    if !var_name.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", var_name),
                            Value::List(vec![Value::String(label_name.clone())]),
                        );
                        row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                    }
                    row_vals.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &row_vals) {
                        continue;
                    }
                }

                if use_eval_path_all {
                    let mut row_vals =
                        build_row_vals(&props, var_name, &all_col_ids, &self.snapshot.store);
                    if !var_name.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", var_name),
                            Value::List(vec![Value::String(label_name.clone())]),
                        );
                        // SPA-213: bare variable → Value::Map; otherwise NodeRef.
                        if bare_vars_all.contains(&var_name.to_string())
                            && !all_label_col_ids_here.is_empty()
                        {
                            let all_nullable = self
                                .snapshot
                                .store
                                .get_node_raw_nullable(node_id, &all_label_col_ids_here)?;
                            let all_props: Vec<(u32, u64)> = all_nullable
                                .iter()
                                .filter_map(|&(col_id, opt)| opt.map(|v| (col_id, v)))
                                .collect();
                            row_vals.insert(
                                var_name.to_string(),
                                build_node_map(&all_props, &self.snapshot.store),
                            );
                        } else {
                            row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                        }
                        row_vals
                            .insert(format!("{}.__node_id__", var_name), Value::NodeRef(node_id));
                    }
                    raw_rows.push(row_vals);
                } else {
                    let row = project_row(
                        &props,
                        column_names,
                        &all_col_ids,
                        var_name,
                        label_name,
                        &self.snapshot.store,
                    );
                    rows.push(row);
                }
            }
        }

        if use_eval_path_all {
            rows = self.aggregate_rows_graph(&raw_rows, &m.return_clause.items);
        }

        // DISTINCT / ORDER BY / SKIP / LIMIT apply regardless of which path
        // built the rows (eval or fast path).
        if m.distinct {
            deduplicate_rows(&mut rows);
        }
        apply_order_by(&mut rows, m, column_names);
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(rows.len());
            rows.drain(0..skip);
        }
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(rows = rows.len(), "label-less full scan complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── 1-hop traversal: (a)-[:R]->(f) ───────────────────────────────────────

    fn execute_one_hop(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        let dst_node_pat = &pat.nodes[1];
        let rel_pat = &pat.rels[0];

        let dir = &rel_pat.dir;
        // Incoming-only: swap the logical src/dst and recurse as Outgoing by
        // swapping pattern roles.  We handle it by falling through with the
        // node patterns in swapped order below.
        // Both (undirected): handled by running forward + backward passes.
        // Unknown directions remain unimplemented.
        use sparrowdb_cypher::ast::EdgeDir;

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();
        // Resolve src/dst label IDs.  Either may be absent (unlabeled pattern node).
        let src_label_id_opt: Option<u32> = if src_label.is_empty() {
            None
        } else {
            self.snapshot
                .catalog
                .get_label(&src_label)?
                .map(|id| id as u32)
        };
        let dst_label_id_opt: Option<u32> = if dst_label.is_empty() {
            None
        } else {
            self.snapshot
                .catalog
                .get_label(&dst_label)?
                .map(|id| id as u32)
        };

        // Build the list of rel tables to scan.
        //
        // Each entry is (catalog_rel_table_id, effective_src_label_id,
        // effective_dst_label_id, rel_type_name).
        //
        // * If the pattern specifies a rel type, filter to matching tables only.
        // * If src/dst labels are given, filter to matching label IDs.
        // * Otherwise include all registered rel tables.
        //
        // SPA-195: this also fixes the previous hardcoded RelTableId(0) bug —
        // every rel table now reads from its own correctly-named delta log file.
        let all_rel_tables = self.snapshot.catalog.list_rel_tables_with_ids();
        let rel_tables_to_scan: Vec<(u64, u32, u32, String)> = all_rel_tables
            .into_iter()
            .filter(|(_, sid, did, rt)| {
                let type_ok = rel_pat.rel_type.is_empty() || rt == &rel_pat.rel_type;
                let src_ok = src_label_id_opt.map(|id| id == *sid as u32).unwrap_or(true);
                let dst_ok = dst_label_id_opt.map(|id| id == *did as u32).unwrap_or(true);
                type_ok && src_ok && dst_ok
            })
            .map(|(catalog_id, sid, did, rt)| (catalog_id, sid as u32, did as u32, rt))
            .collect();

        let use_agg = has_aggregate_in_return(&m.return_clause.items);
        let mut raw_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        // For undirected (Both), track seen (src_slot, dst_slot) pairs from the
        // forward pass so we don't re-emit them in the backward pass.
        let mut seen_undirected: HashSet<(u64, u64)> = HashSet::new();

        // Pre-compute label name lookup for unlabeled patterns.
        let label_id_to_name: Vec<(u16, String)> = if src_label.is_empty() || dst_label.is_empty() {
            self.snapshot.catalog.list_labels().unwrap_or_default()
        } else {
            vec![]
        };

        // Iterate each qualifying rel table.
        for (catalog_rel_id, tbl_src_label_id, tbl_dst_label_id, tbl_rel_type) in
            &rel_tables_to_scan
        {
            let storage_rel_id = RelTableId(*catalog_rel_id as u32);
            let effective_src_label_id = *tbl_src_label_id;
            let effective_dst_label_id = *tbl_dst_label_id;

            // SPA-195: the rel type name for this edge comes from the catalog
            // entry, not from rel_pat.rel_type (which may be empty for [r]).
            let effective_rel_type: &str = tbl_rel_type.as_str();

            // Compute the effective src/dst label names for metadata injection.
            let effective_src_label: &str = if src_label.is_empty() {
                label_id_to_name
                    .iter()
                    .find(|(id, _)| *id as u32 == effective_src_label_id)
                    .map(|(_, name)| name.as_str())
                    .unwrap_or("")
            } else {
                src_label.as_str()
            };
            let effective_dst_label: &str = if dst_label.is_empty() {
                label_id_to_name
                    .iter()
                    .find(|(id, _)| *id as u32 == effective_dst_label_id)
                    .map(|(_, name)| name.as_str())
                    .unwrap_or("")
            } else {
                dst_label.as_str()
            };

            let hwm_src = match self.snapshot.store.hwm_for_label(effective_src_label_id) {
                Ok(h) => h,
                Err(_) => continue,
            };
            tracing::debug!(
                src_label = %effective_src_label,
                dst_label = %effective_dst_label,
                rel_type = %effective_rel_type,
                hwm_src = hwm_src,
                "one-hop traversal start"
            );

            let mut col_ids_src =
                collect_col_ids_for_var(&src_node_pat.var, column_names, effective_src_label_id);
            let mut col_ids_dst =
                collect_col_ids_for_var(&dst_node_pat.var, column_names, effective_dst_label_id);
            if use_agg {
                for item in &m.return_clause.items {
                    collect_col_ids_from_expr(&item.expr, &mut col_ids_src);
                    collect_col_ids_from_expr(&item.expr, &mut col_ids_dst);
                }
            }
            // Ensure WHERE-only columns are fetched so predicates can evaluate them.
            if let Some(ref where_expr) = m.where_clause {
                collect_col_ids_from_expr(where_expr, &mut col_ids_src);
                collect_col_ids_from_expr(where_expr, &mut col_ids_dst);
            }

            // Read ALL delta records for this specific rel table once (outside
            // the per-src-slot loop) so we open the file only once per table.
            let delta_records_all = {
                let edge_store = EdgeStore::open(&self.snapshot.db_root, storage_rel_id);
                edge_store.and_then(|s| s.read_delta()).unwrap_or_default()
            };

            // Scan source nodes for this label.
            for src_slot in 0..hwm_src {
                // SPA-254: check per-query deadline at every slot boundary.
                self.check_deadline()?;

                let src_node = NodeId(((effective_src_label_id as u64) << 32) | src_slot);
                let src_props = if !col_ids_src.is_empty() || !src_node_pat.props.is_empty() {
                    let all_needed: Vec<u32> = {
                        let mut v = col_ids_src.clone();
                        for p in &src_node_pat.props {
                            let col_id = prop_name_to_col_id(&p.key);
                            if !v.contains(&col_id) {
                                v.push(col_id);
                            }
                        }
                        v
                    };
                    self.snapshot.store.get_node_raw(src_node, &all_needed)?
                } else {
                    vec![]
                };

                // Apply src inline prop filter.
                if !self.matches_prop_filter(&src_props, &src_node_pat.props) {
                    continue;
                }

                // SPA-163 / SPA-195: read delta edges for this src node from
                // the correct per-rel-table delta log (no longer hardcoded to 0).
                let delta_neighbors: Vec<u64> = delta_records_all
                    .iter()
                    .filter(|r| {
                        let r_src_label = (r.src.0 >> 32) as u32;
                        let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                        r_src_label == effective_src_label_id && r_src_slot == src_slot
                    })
                    .map(|r| r.dst.0 & 0xFFFF_FFFF)
                    .collect();

                // Look up the CSR for this specific rel table.  open_csr_map
                // builds a per-table map keyed by catalog_rel_id, so each rel
                // type's checkpointed edges are found under its own key.
                let csr_neighbors: &[u64] = self
                    .snapshot
                    .csrs
                    .get(&u32::try_from(*catalog_rel_id).expect("rel_table_id overflowed u32"))
                    .map(|c| c.neighbors(src_slot))
                    .unwrap_or(&[]);
                let all_neighbors: Vec<u64> = csr_neighbors
                    .iter()
                    .copied()
                    .chain(delta_neighbors.into_iter())
                    .collect();
                let mut seen_neighbors: HashSet<u64> = HashSet::new();
                for &dst_slot in &all_neighbors {
                    if !seen_neighbors.insert(dst_slot) {
                        continue;
                    }
                    // For undirected (Both) track emitted (src,dst) pairs so the
                    // backward pass can skip them to avoid double-emission.
                    if *dir == EdgeDir::Both {
                        seen_undirected.insert((src_slot, dst_slot));
                    }
                    let dst_node = NodeId(((effective_dst_label_id as u64) << 32) | dst_slot);
                    let dst_props = if !col_ids_dst.is_empty() || !dst_node_pat.props.is_empty() {
                        let all_needed: Vec<u32> = {
                            let mut v = col_ids_dst.clone();
                            for p in &dst_node_pat.props {
                                let col_id = prop_name_to_col_id(&p.key);
                                if !v.contains(&col_id) {
                                    v.push(col_id);
                                }
                            }
                            v
                        };
                        self.snapshot.store.get_node_raw(dst_node, &all_needed)?
                    } else {
                        vec![]
                    };

                    // Apply dst inline prop filter.
                    if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                        continue;
                    }

                    // For undirected (Both), record (src_slot, dst_slot) so the
                    // backward pass skips already-emitted pairs.
                    if *dir == EdgeDir::Both {
                        seen_undirected.insert((src_slot, dst_slot));
                    }

                    // Apply WHERE clause.
                    if let Some(ref where_expr) = m.where_clause {
                        let mut row_vals = build_row_vals(
                            &src_props,
                            &src_node_pat.var,
                            &col_ids_src,
                            &self.snapshot.store,
                        );
                        row_vals.extend(build_row_vals(
                            &dst_props,
                            &dst_node_pat.var,
                            &col_ids_dst,
                            &self.snapshot.store,
                        ));
                        // Inject relationship metadata so type(r) works in WHERE.
                        if !rel_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__type__", rel_pat.var),
                                Value::String(effective_rel_type.to_string()),
                            );
                        }
                        // Inject node label metadata so labels(n) works in WHERE.
                        if !src_node_pat.var.is_empty() && !effective_src_label.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", src_node_pat.var),
                                Value::List(vec![Value::String(effective_src_label.to_string())]),
                            );
                        }
                        if !dst_node_pat.var.is_empty() && !effective_dst_label.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", dst_node_pat.var),
                                Value::List(vec![Value::String(effective_dst_label.to_string())]),
                            );
                        }
                        row_vals.extend(self.dollar_params());
                        if !self.eval_where_graph(where_expr, &row_vals) {
                            continue;
                        }
                    }

                    if use_agg {
                        let mut row_vals = build_row_vals(
                            &src_props,
                            &src_node_pat.var,
                            &col_ids_src,
                            &self.snapshot.store,
                        );
                        row_vals.extend(build_row_vals(
                            &dst_props,
                            &dst_node_pat.var,
                            &col_ids_dst,
                            &self.snapshot.store,
                        ));
                        // Inject relationship and label metadata for aggregate path.
                        if !rel_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__type__", rel_pat.var),
                                Value::String(effective_rel_type.to_string()),
                            );
                        }
                        if !src_node_pat.var.is_empty() && !effective_src_label.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", src_node_pat.var),
                                Value::List(vec![Value::String(effective_src_label.to_string())]),
                            );
                        }
                        if !dst_node_pat.var.is_empty() && !effective_dst_label.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", dst_node_pat.var),
                                Value::List(vec![Value::String(effective_dst_label.to_string())]),
                            );
                        }
                        if !src_node_pat.var.is_empty() {
                            row_vals.insert(src_node_pat.var.clone(), Value::NodeRef(src_node));
                        }
                        if !dst_node_pat.var.is_empty() {
                            row_vals.insert(dst_node_pat.var.clone(), Value::NodeRef(dst_node));
                        }
                        // SPA-242: bind the relationship variable as a non-null
                        // EdgeRef so COUNT(r) counts matched edges correctly.
                        if !rel_pat.var.is_empty() {
                            // Encode a unique edge identity: high 32 bits = rel
                            // table id, low 32 bits = dst_slot.  src_slot is
                            // already implicit in the traversal nesting order but
                            // we mix it in via XOR to keep uniqueness within the
                            // same rel table.
                            let edge_id = sparrowdb_common::EdgeId(
                                (*catalog_rel_id << 32) | (src_slot ^ dst_slot) & 0xFFFF_FFFF,
                            );
                            row_vals.insert(rel_pat.var.clone(), Value::EdgeRef(edge_id));
                        }
                        raw_rows.push(row_vals);
                    } else {
                        // Build result row.
                        // SPA-195: use effective_rel_type (from the catalog per
                        // rel table) so unlabeled / untyped patterns return the
                        // correct relationship type name rather than empty string.
                        let rel_var_type = if !rel_pat.var.is_empty() {
                            Some((rel_pat.var.as_str(), effective_rel_type))
                        } else {
                            None
                        };
                        let src_label_meta =
                            if !src_node_pat.var.is_empty() && !effective_src_label.is_empty() {
                                Some((src_node_pat.var.as_str(), effective_src_label))
                            } else {
                                None
                            };
                        let dst_label_meta =
                            if !dst_node_pat.var.is_empty() && !effective_dst_label.is_empty() {
                                Some((dst_node_pat.var.as_str(), effective_dst_label))
                            } else {
                                None
                            };
                        let row = project_hop_row(
                            &src_props,
                            &dst_props,
                            column_names,
                            &src_node_pat.var,
                            &dst_node_pat.var,
                            rel_var_type,
                            src_label_meta,
                            dst_label_meta,
                            &self.snapshot.store,
                        );
                        rows.push(row);
                    }
                }
            }
        }

        // ── Backward pass for undirected (Both) — SPA-193 ───────────────────
        // For (a)-[r]-(b), the forward pass emitted rows for edges a→b.
        // Now scan each rel table in reverse (dst→src) to find backward edges
        // (b→a) that were not already emitted in the forward pass.
        if *dir == EdgeDir::Both {
            for (catalog_rel_id, tbl_src_label_id, tbl_dst_label_id, tbl_rel_type) in
                &rel_tables_to_scan
            {
                let storage_rel_id = RelTableId(*catalog_rel_id as u32);
                // In the backward pass, scan "dst" label nodes (b-side) as src.
                let bwd_scan_label_id = *tbl_dst_label_id;
                let bwd_dst_label_id = *tbl_src_label_id;
                let effective_rel_type: &str = tbl_rel_type.as_str();

                let effective_src_label: &str = if src_label.is_empty() {
                    label_id_to_name
                        .iter()
                        .find(|(id, _)| *id as u32 == bwd_scan_label_id)
                        .map(|(_, name)| name.as_str())
                        .unwrap_or("")
                } else {
                    src_label.as_str()
                };
                let effective_dst_label: &str = if dst_label.is_empty() {
                    label_id_to_name
                        .iter()
                        .find(|(id, _)| *id as u32 == bwd_dst_label_id)
                        .map(|(_, name)| name.as_str())
                        .unwrap_or("")
                } else {
                    dst_label.as_str()
                };

                let hwm_bwd = match self.snapshot.store.hwm_for_label(bwd_scan_label_id) {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                let mut col_ids_src =
                    collect_col_ids_for_var(&src_node_pat.var, column_names, bwd_scan_label_id);
                let mut col_ids_dst =
                    collect_col_ids_for_var(&dst_node_pat.var, column_names, bwd_dst_label_id);
                if use_agg {
                    for item in &m.return_clause.items {
                        collect_col_ids_from_expr(&item.expr, &mut col_ids_src);
                        collect_col_ids_from_expr(&item.expr, &mut col_ids_dst);
                    }
                }

                // Read delta records for this rel table (physical edges stored
                // as src=a, dst=b that we want to traverse in reverse b→a).
                let delta_records_bwd = EdgeStore::open(&self.snapshot.db_root, storage_rel_id)
                    .and_then(|s| s.read_delta())
                    .unwrap_or_default();

                // Load the backward CSR for this rel table (written by
                // checkpoint).  Falls back to None gracefully when no
                // checkpoint has been run yet so pre-checkpoint databases
                // still return correct results via the delta log path.
                let csr_bwd: Option<CsrBackward> =
                    EdgeStore::open(&self.snapshot.db_root, storage_rel_id)
                        .and_then(|s| s.open_bwd())
                        .ok();

                // Scan the b-side (physical dst label = tbl_dst_label_id).
                for b_slot in 0..hwm_bwd {
                    let b_node = NodeId(((bwd_scan_label_id as u64) << 32) | b_slot);
                    let b_props = if !col_ids_src.is_empty() || !src_node_pat.props.is_empty() {
                        let all_needed: Vec<u32> = {
                            let mut v = col_ids_src.clone();
                            for p in &src_node_pat.props {
                                let col_id = prop_name_to_col_id(&p.key);
                                if !v.contains(&col_id) {
                                    v.push(col_id);
                                }
                            }
                            v
                        };
                        self.snapshot.store.get_node_raw(b_node, &all_needed)?
                    } else {
                        vec![]
                    };
                    // Apply src-side (a-side pattern) prop filter — note: in the
                    // undirected backward pass the pattern variables are swapped,
                    // so src_node_pat corresponds to the "a" role which is the
                    // b-slot we are scanning.
                    if !self.matches_prop_filter(&b_props, &src_node_pat.props) {
                        continue;
                    }

                    // Find edges in delta log where b_slot is the *destination*
                    // (physical edge: some_src → b_slot), giving us predecessors.
                    let delta_predecessors: Vec<u64> = delta_records_bwd
                        .iter()
                        .filter(|r| {
                            let r_dst_label = (r.dst.0 >> 32) as u32;
                            let r_dst_slot = r.dst.0 & 0xFFFF_FFFF;
                            r_dst_label == bwd_scan_label_id && r_dst_slot == b_slot
                        })
                        .map(|r| r.src.0 & 0xFFFF_FFFF)
                        .collect();

                    // Also include checkpointed predecessors from the backward
                    // CSR (populated after checkpoint; empty/None before first
                    // checkpoint).  Combine with delta predecessors so that
                    // undirected matching works for both pre- and post-checkpoint
                    // databases.
                    let csr_predecessors: &[u64] = csr_bwd
                        .as_ref()
                        .map(|c| c.predecessors(b_slot))
                        .unwrap_or(&[]);
                    let all_predecessors: Vec<u64> = csr_predecessors
                        .iter()
                        .copied()
                        .chain(delta_predecessors.into_iter())
                        .collect();

                    let mut seen_preds: HashSet<u64> = HashSet::new();
                    for a_slot in all_predecessors {
                        if !seen_preds.insert(a_slot) {
                            continue;
                        }
                        // Skip pairs already emitted in the forward pass.
                        // The backward row being emitted is (b_slot, a_slot) --
                        // b is the node being scanned (physical dst of the edge),
                        // a is its predecessor (physical src).
                        // Only suppress this row if that exact reversed pair was
                        // already produced by the forward pass (i.e. a physical
                        // b->a edge was stored and traversed).
                        // SPA-257: using (a_slot, b_slot) was wrong -- it
                        // suppressed the legitimate backward traversal of a->b.
                        if seen_undirected.contains(&(b_slot, a_slot)) {
                            continue;
                        }

                        let a_node = NodeId(((bwd_dst_label_id as u64) << 32) | a_slot);
                        let a_props = if !col_ids_dst.is_empty() || !dst_node_pat.props.is_empty() {
                            let all_needed: Vec<u32> = {
                                let mut v = col_ids_dst.clone();
                                for p in &dst_node_pat.props {
                                    let col_id = prop_name_to_col_id(&p.key);
                                    if !v.contains(&col_id) {
                                        v.push(col_id);
                                    }
                                }
                                v
                            };
                            self.snapshot.store.get_node_raw(a_node, &all_needed)?
                        } else {
                            vec![]
                        };

                        if !self.matches_prop_filter(&a_props, &dst_node_pat.props) {
                            continue;
                        }

                        // Apply WHERE clause.
                        if let Some(ref where_expr) = m.where_clause {
                            let mut row_vals = build_row_vals(
                                &b_props,
                                &src_node_pat.var,
                                &col_ids_src,
                                &self.snapshot.store,
                            );
                            row_vals.extend(build_row_vals(
                                &a_props,
                                &dst_node_pat.var,
                                &col_ids_dst,
                                &self.snapshot.store,
                            ));
                            if !rel_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__type__", rel_pat.var),
                                    Value::String(effective_rel_type.to_string()),
                                );
                            }
                            if !src_node_pat.var.is_empty() && !effective_src_label.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", src_node_pat.var),
                                    Value::List(vec![Value::String(
                                        effective_src_label.to_string(),
                                    )]),
                                );
                            }
                            if !dst_node_pat.var.is_empty() && !effective_dst_label.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", dst_node_pat.var),
                                    Value::List(vec![Value::String(
                                        effective_dst_label.to_string(),
                                    )]),
                                );
                            }
                            row_vals.extend(self.dollar_params());
                            if !self.eval_where_graph(where_expr, &row_vals) {
                                continue;
                            }
                        }

                        if use_agg {
                            let mut row_vals = build_row_vals(
                                &b_props,
                                &src_node_pat.var,
                                &col_ids_src,
                                &self.snapshot.store,
                            );
                            row_vals.extend(build_row_vals(
                                &a_props,
                                &dst_node_pat.var,
                                &col_ids_dst,
                                &self.snapshot.store,
                            ));
                            if !rel_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__type__", rel_pat.var),
                                    Value::String(effective_rel_type.to_string()),
                                );
                            }
                            if !src_node_pat.var.is_empty() && !effective_src_label.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", src_node_pat.var),
                                    Value::List(vec![Value::String(
                                        effective_src_label.to_string(),
                                    )]),
                                );
                            }
                            if !dst_node_pat.var.is_empty() && !effective_dst_label.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", dst_node_pat.var),
                                    Value::List(vec![Value::String(
                                        effective_dst_label.to_string(),
                                    )]),
                                );
                            }
                            if !src_node_pat.var.is_empty() {
                                row_vals.insert(src_node_pat.var.clone(), Value::NodeRef(b_node));
                            }
                            if !dst_node_pat.var.is_empty() {
                                row_vals.insert(dst_node_pat.var.clone(), Value::NodeRef(a_node));
                            }
                            // SPA-242: bind the relationship variable as a non-null
                            // EdgeRef so COUNT(r) counts matched edges correctly.
                            if !rel_pat.var.is_empty() {
                                let edge_id = sparrowdb_common::EdgeId(
                                    (*catalog_rel_id << 32) | (b_slot ^ a_slot) & 0xFFFF_FFFF,
                                );
                                row_vals.insert(rel_pat.var.clone(), Value::EdgeRef(edge_id));
                            }
                            raw_rows.push(row_vals);
                        } else {
                            let rel_var_type = if !rel_pat.var.is_empty() {
                                Some((rel_pat.var.as_str(), effective_rel_type))
                            } else {
                                None
                            };
                            let src_label_meta = if !src_node_pat.var.is_empty()
                                && !effective_src_label.is_empty()
                            {
                                Some((src_node_pat.var.as_str(), effective_src_label))
                            } else {
                                None
                            };
                            let dst_label_meta = if !dst_node_pat.var.is_empty()
                                && !effective_dst_label.is_empty()
                            {
                                Some((dst_node_pat.var.as_str(), effective_dst_label))
                            } else {
                                None
                            };
                            let row = project_hop_row(
                                &b_props,
                                &a_props,
                                column_names,
                                &src_node_pat.var,
                                &dst_node_pat.var,
                                rel_var_type,
                                src_label_meta,
                                dst_label_meta,
                                &self.snapshot.store,
                            );
                            rows.push(row);
                        }
                    }
                }
            }
        }

        if use_agg {
            rows = self.aggregate_rows_graph(&raw_rows, &m.return_clause.items);
        } else {
            // DISTINCT
            if m.distinct {
                deduplicate_rows(&mut rows);
            }

            // ORDER BY
            apply_order_by(&mut rows, m, column_names);

            // SKIP
            if let Some(skip) = m.skip {
                let skip = (skip as usize).min(rows.len());
                rows.drain(0..skip);
            }

            // LIMIT
            if let Some(lim) = m.limit {
                rows.truncate(lim as usize);
            }
        }

        tracing::debug!(rows = rows.len(), "one-hop traversal complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── 2-hop traversal: (a)-[:R]->()-[:R]->(fof) ────────────────────────────

    fn execute_two_hop(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        use crate::join::AspJoin;

        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        // nodes[1] is the anonymous mid node
        let fof_node_pat = &pat.nodes[2];

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let fof_label = fof_node_pat.labels.first().cloned().unwrap_or_default();
        let src_label_id = self
            .snapshot
            .catalog
            .get_label(&src_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;
        let fof_label_id = self
            .snapshot
            .catalog
            .get_label(&fof_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;

        let hwm_src = self.snapshot.store.hwm_for_label(src_label_id)?;
        tracing::debug!(src_label = %src_label, fof_label = %fof_label, hwm_src = hwm_src, "two-hop traversal start");

        // Collect col_ids for fof: projected columns plus any columns referenced by prop filters.
        // Also include any columns referenced by the WHERE clause, scoped to the fof variable so
        // that src-only predicates do not cause spurious column fetches from fof nodes.
        let col_ids_fof = {
            let mut ids = collect_col_ids_for_var(&fof_node_pat.var, column_names, fof_label_id);
            for p in &fof_node_pat.props {
                let col_id = prop_name_to_col_id(&p.key);
                if !ids.contains(&col_id) {
                    ids.push(col_id);
                }
            }
            if let Some(ref where_expr) = m.where_clause {
                collect_col_ids_from_expr_for_var(where_expr, &fof_node_pat.var, &mut ids);
            }
            ids
        };

        // Collect col_ids for src: columns referenced in RETURN (for projection)
        // plus columns referenced in WHERE for the src variable.
        // SPA-252: projection columns must be included so that project_fof_row
        // can resolve src-variable columns (e.g. `RETURN a.name` when src_var = "a").
        let col_ids_src_where: Vec<u32> = {
            let mut ids = collect_col_ids_for_var(&src_node_pat.var, column_names, src_label_id);
            if let Some(ref where_expr) = m.where_clause {
                collect_col_ids_from_expr_for_var(where_expr, &src_node_pat.var, &mut ids);
            }
            ids
        };

        // SPA-163 + SPA-185: build a slot-level adjacency map from all delta
        // logs so that edges written since the last checkpoint are visible for
        // 2-hop queries.  We aggregate across all rel types here because the
        // 2-hop executor does not currently filter on rel_type.
        // Map: src_slot → Vec<dst_slot> (only records whose src label matches).
        let delta_adj: HashMap<u64, Vec<u64>> = {
            let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
            for r in self.read_delta_all() {
                let r_src_label = (r.src.0 >> 32) as u32;
                let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                if r_src_label == src_label_id {
                    adj.entry(r_src_slot)
                        .or_default()
                        .push(r.dst.0 & 0xFFFF_FFFF);
                }
            }
            adj
        };

        // SPA-185: build a merged CSR that union-combines edges from all
        // per-type CSRs so the 2-hop traversal sees paths through any rel type.
        // AspJoin requires a single &CsrForward; we construct a combined one
        // rather than using an arbitrary first entry.
        let merged_csr = {
            let max_nodes = self
                .snapshot
                .csrs
                .values()
                .map(|c| c.n_nodes())
                .max()
                .unwrap_or(0);
            let mut edges: Vec<(u64, u64)> = Vec::new();
            for csr in self.snapshot.csrs.values() {
                for src in 0..csr.n_nodes() {
                    for &dst in csr.neighbors(src) {
                        edges.push((src, dst));
                    }
                }
            }
            // CsrForward::build requires a sorted edge list.
            edges.sort_unstable();
            edges.dedup();
            CsrForward::build(max_nodes, &edges)
        };
        let join = AspJoin::new(&merged_csr);
        let mut rows = Vec::new();

        // Scan source nodes.
        for src_slot in 0..hwm_src {
            // SPA-254: check per-query deadline at every slot boundary.
            self.check_deadline()?;

            let src_node = NodeId(((src_label_id as u64) << 32) | src_slot);
            let src_needed: Vec<u32> = {
                let mut v = vec![];
                for p in &src_node_pat.props {
                    let col_id = prop_name_to_col_id(&p.key);
                    if !v.contains(&col_id) {
                        v.push(col_id);
                    }
                }
                for &col_id in &col_ids_src_where {
                    if !v.contains(&col_id) {
                        v.push(col_id);
                    }
                }
                v
            };

            let src_props = read_node_props(&self.snapshot.store, src_node, &src_needed)?;

            // Apply src inline prop filter.
            if !self.matches_prop_filter(&src_props, &src_node_pat.props) {
                continue;
            }

            // Use ASP-Join to get 2-hop fof from CSR.
            let mut fof_slots = join.two_hop(src_slot)?;

            // SPA-163: extend with delta-log 2-hop paths.
            // First-hop delta neighbors of src_slot:
            let first_hop_delta = delta_adj
                .get(&src_slot)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            if !first_hop_delta.is_empty() {
                let mut delta_fof: HashSet<u64> = HashSet::new();
                for &mid_slot in first_hop_delta {
                    // CSR second hop from mid (use merged multi-type CSR):
                    for &fof in merged_csr.neighbors(mid_slot) {
                        delta_fof.insert(fof);
                    }
                    // Delta second hop from mid:
                    if let Some(mid_neighbors) = delta_adj.get(&mid_slot) {
                        for &fof in mid_neighbors {
                            delta_fof.insert(fof);
                        }
                    }
                }
                fof_slots.extend(delta_fof);
                // Re-deduplicate the combined set.
                let unique: HashSet<u64> = fof_slots.into_iter().collect();
                fof_slots = unique.into_iter().collect();
                fof_slots.sort_unstable();
            }

            for fof_slot in fof_slots {
                let fof_node = NodeId(((fof_label_id as u64) << 32) | fof_slot);
                let fof_props = read_node_props(&self.snapshot.store, fof_node, &col_ids_fof)?;

                // Apply fof inline prop filter.
                if !self.matches_prop_filter(&fof_props, &fof_node_pat.props) {
                    continue;
                }

                // Apply WHERE clause predicate.
                if let Some(ref where_expr) = m.where_clause {
                    let mut row_vals = build_row_vals(
                        &src_props,
                        &src_node_pat.var,
                        &col_ids_src_where,
                        &self.snapshot.store,
                    );
                    row_vals.extend(build_row_vals(
                        &fof_props,
                        &fof_node_pat.var,
                        &col_ids_fof,
                        &self.snapshot.store,
                    ));
                    // Inject label metadata so labels(n) works in WHERE.
                    if !src_node_pat.var.is_empty() && !src_label.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", src_node_pat.var),
                            Value::List(vec![Value::String(src_label.clone())]),
                        );
                    }
                    if !fof_node_pat.var.is_empty() && !fof_label.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", fof_node_pat.var),
                            Value::List(vec![Value::String(fof_label.clone())]),
                        );
                    }
                    // Inject relationship type metadata so type(r) works in WHERE.
                    if !pat.rels[0].var.is_empty() {
                        row_vals.insert(
                            format!("{}.__type__", pat.rels[0].var),
                            Value::String(pat.rels[0].rel_type.clone()),
                        );
                    }
                    if !pat.rels[1].var.is_empty() {
                        row_vals.insert(
                            format!("{}.__type__", pat.rels[1].var),
                            Value::String(pat.rels[1].rel_type.clone()),
                        );
                    }
                    row_vals.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &row_vals) {
                        continue;
                    }
                }

                let row = project_fof_row(
                    &src_props,
                    &fof_props,
                    column_names,
                    &src_node_pat.var,
                    &self.snapshot.store,
                );
                rows.push(row);
            }
        }

        // DISTINCT
        if m.distinct {
            deduplicate_rows(&mut rows);
        }

        // ORDER BY
        apply_order_by(&mut rows, m, column_names);

        // SKIP
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(rows.len());
            rows.drain(0..skip);
        }

        // LIMIT
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(rows = rows.len(), "two-hop traversal complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── N-hop traversal (SPA-252): (a)-[:R]->(b)-[:R]->...-(z) ──────────────

    /// General N-hop traversal for inline chains with 3 or more relationship
    /// hops in a single MATCH pattern, e.g.:
    ///   MATCH (a)-[:R]->(b)-[:R]->(c)-[:R]->(d) RETURN a.name, b.name, c.name, d.name
    ///
    /// The algorithm iterates forward hop by hop.  At each level it maintains
    /// a "frontier" of `(slot, props)` tuples for the current boundary nodes,
    /// plus an accumulated `row_vals` map that records all variable→property
    /// bindings seen so far.  When the frontier advances to the final node, a
    /// result row is projected from the accumulated map.
    ///
    /// This replaces the previous fallthrough to `execute_scan` which only
    /// scanned the first node and ignored all relationship hops.
    fn execute_n_hop(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let n_nodes = pat.nodes.len();
        let n_rels = pat.rels.len();

        // Sanity: nodes.len() == rels.len() + 1 always holds for a linear chain.
        if n_nodes != n_rels + 1 {
            return Err(sparrowdb_common::Error::Unimplemented);
        }

        // Pre-compute col_ids needed per node variable so we only read the
        // property columns that are actually projected or filtered.
        let col_ids_per_node: Vec<Vec<u32>> = (0..n_nodes)
            .map(|i| {
                let node_pat = &pat.nodes[i];
                let var = &node_pat.var;
                let mut ids = if var.is_empty() {
                    vec![]
                } else {
                    collect_col_ids_for_var(var, column_names, 0)
                };
                // Include columns required by WHERE predicates for this var.
                if let Some(ref where_expr) = m.where_clause {
                    if !var.is_empty() {
                        collect_col_ids_from_expr_for_var(where_expr, var, &mut ids);
                    }
                }
                // Include columns required by inline prop filters.
                for p in &node_pat.props {
                    let col_id = prop_name_to_col_id(&p.key);
                    if !ids.contains(&col_id) {
                        ids.push(col_id);
                    }
                }
                // Always read at least col_0 so the node can be identified.
                if ids.is_empty() {
                    ids.push(0);
                }
                ids
            })
            .collect();

        // Resolve label_ids for all node positions.
        let label_ids_per_node: Vec<Option<u32>> = (0..n_nodes)
            .map(|i| {
                let label = pat.nodes[i].labels.first().cloned().unwrap_or_default();
                if label.is_empty() {
                    None
                } else {
                    self.snapshot
                        .catalog
                        .get_label(&label)
                        .ok()
                        .flatten()
                        .map(|id| id as u32)
                }
            })
            .collect();

        // Scan the first (source) node and kick off the recursive hop chain.
        let src_label_id = match label_ids_per_node[0] {
            Some(id) => id,
            None => return Err(sparrowdb_common::Error::Unimplemented),
        };
        let hwm_src = self.snapshot.store.hwm_for_label(src_label_id)?;

        // We read all delta edges once up front to avoid repeated file I/O.
        let delta_all = self.read_delta_all();

        let mut rows: Vec<Vec<Value>> = Vec::new();

        for src_slot in 0..hwm_src {
            // SPA-254: check per-query deadline at every slot boundary.
            self.check_deadline()?;

            let src_node_id = NodeId(((src_label_id as u64) << 32) | src_slot);

            // Skip tombstoned nodes.
            if self.is_node_tombstoned(src_node_id) {
                continue;
            }

            let src_props =
                read_node_props(&self.snapshot.store, src_node_id, &col_ids_per_node[0])?;

            // Apply inline prop filter for the source node.
            if !self.matches_prop_filter(&src_props, &pat.nodes[0].props) {
                continue;
            }

            // Seed the frontier with the source node binding.
            let mut row_vals: HashMap<String, Value> = HashMap::new();
            if !pat.nodes[0].var.is_empty() {
                for &(col_id, raw) in &src_props {
                    let key = format!("{}.col_{col_id}", pat.nodes[0].var);
                    row_vals.insert(key, decode_raw_val(raw, &self.snapshot.store));
                }
            }

            // `frontier` holds (slot, accumulated_vals) pairs for the current
            // boundary of the traversal.  Each entry represents one in-progress
            // path; cloning ensures bindings are isolated across branches.
            let mut frontier: Vec<(u64, HashMap<String, Value>)> = vec![(src_slot, row_vals)];

            for hop_idx in 0..n_rels {
                let next_node_pat = &pat.nodes[hop_idx + 1];
                let next_label_id_opt = label_ids_per_node[hop_idx + 1];
                let next_col_ids = &col_ids_per_node[hop_idx + 1];
                let cur_label_id = label_ids_per_node[hop_idx].unwrap_or(src_label_id);

                let mut next_frontier: Vec<(u64, HashMap<String, Value>)> = Vec::new();

                for (cur_slot, cur_vals) in frontier {
                    // Gather neighbors from CSR + delta for this hop.
                    let csr_nb: Vec<u64> = self.csr_neighbors_all(cur_slot);
                    let delta_nb: Vec<u64> = delta_all
                        .iter()
                        .filter(|r| {
                            let r_src_label = (r.src.0 >> 32) as u32;
                            let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                            r_src_label == cur_label_id && r_src_slot == cur_slot
                        })
                        .map(|r| r.dst.0 & 0xFFFF_FFFF)
                        .collect();

                    let mut seen: HashSet<u64> = HashSet::new();
                    let all_nb: Vec<u64> = csr_nb
                        .into_iter()
                        .chain(delta_nb)
                        .filter(|&nb| seen.insert(nb))
                        .collect();

                    for next_slot in all_nb {
                        let next_node_id = if let Some(lbl_id) = next_label_id_opt {
                            NodeId(((lbl_id as u64) << 32) | next_slot)
                        } else {
                            NodeId(next_slot)
                        };

                        let next_props =
                            read_node_props(&self.snapshot.store, next_node_id, next_col_ids)?;

                        // Apply inline prop filter for this hop's destination node.
                        if !self.matches_prop_filter(&next_props, &next_node_pat.props) {
                            continue;
                        }

                        // Clone the accumulated bindings and extend with this node's
                        // properties, keyed under its own variable name.
                        let mut new_vals = cur_vals.clone();
                        if !next_node_pat.var.is_empty() {
                            for &(col_id, raw) in &next_props {
                                let key = format!("{}.col_{col_id}", next_node_pat.var);
                                new_vals.insert(key, decode_raw_val(raw, &self.snapshot.store));
                            }
                        }

                        next_frontier.push((next_slot, new_vals));
                    }
                }

                frontier = next_frontier;
            }

            // `frontier` now contains complete paths.  Project result rows.
            for (_final_slot, path_vals) in frontier {
                // Apply WHERE clause using the full accumulated binding map.
                if let Some(ref where_expr) = m.where_clause {
                    let mut eval_vals = path_vals.clone();
                    eval_vals.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &eval_vals) {
                        continue;
                    }
                }

                // Project column values from the accumulated binding map.
                // Each column name is "var.prop" — look up "var.col_<id>" in the map.
                let row: Vec<Value> = column_names
                    .iter()
                    .map(|col_name| {
                        if let Some((var, prop)) = col_name.split_once('.') {
                            let key = format!("{var}.col_{}", col_id_of(prop));
                            path_vals.get(&key).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    })
                    .collect();

                rows.push(row);
            }
        }

        // DISTINCT
        if m.distinct {
            deduplicate_rows(&mut rows);
        }

        // ORDER BY
        apply_order_by(&mut rows, m, column_names);

        // SKIP
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(rows.len());
            rows.drain(0..skip);
        }

        // LIMIT
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(
            rows = rows.len(),
            n_rels = n_rels,
            "n-hop traversal complete"
        );
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── Variable-length path traversal: (a)-[:R*M..N]->(b) ──────────────────

    /// Collect all neighbor slot-ids reachable from `src_slot` via the delta
    /// log and CSR adjacency.  src_label_id is used to filter delta records.
    ///
    /// SPA-185: reads across all rel types (used by variable-length path
    /// traversal which does not currently filter on rel_type).
    /// Return the labeled outgoing neighbors of `(src_slot, src_label_id)`.
    ///
    /// Each entry is `(dst_slot, dst_label_id)`.  The delta log encodes the full
    /// NodeId in `r.dst`, so label_id is recovered precisely.  For CSR-only
    /// destinations the label is looked up in the `node_label` hint map (built
    /// from the delta by the caller); if absent, `src_label_id` is used as a
    /// conservative fallback (correct for homogeneous graphs).
    fn get_node_neighbors_labeled(
        &self,
        src_slot: u64,
        src_label_id: u32,
        delta_all: &[sparrowdb_storage::edge_store::DeltaRecord],
        node_label: &std::collections::HashSet<(u64, u32)>,
        all_label_ids: &[u32],
        out: &mut std::collections::HashSet<(u64, u32)>,
    ) {
        out.clear();

        // ── CSR neighbors (slot only; label recovered by scanning all label HWMs
        //    or falling back to src_label_id for homogeneous graphs) ────────────
        let csr_slots: Vec<u64> = self.csr_neighbors_all(src_slot);

        // ── Delta neighbors (full NodeId available) ───────────────────────────
        // Insert delta neighbors first — their labels are authoritative.
        for r in delta_all.iter().filter(|r| {
            let r_src_label = (r.src.0 >> 32) as u32;
            let r_src_slot = r.src.0 & 0xFFFF_FFFF;
            r_src_label == src_label_id && r_src_slot == src_slot
        }) {
            let dst_slot = r.dst.0 & 0xFFFF_FFFF;
            let dst_label = (r.dst.0 >> 32) as u32;
            out.insert((dst_slot, dst_label));
        }

        // For each CSR slot, determine label: prefer a delta-confirmed label,
        // else scan all known label ids to find one whose HWM covers that slot.
        // If no label confirms it, fall back to src_label_id.
        'csr: for dst_slot in csr_slots {
            // Check if delta already gave us a label for this slot.
            for &lid in all_label_ids {
                if out.contains(&(dst_slot, lid)) {
                    continue 'csr; // already recorded with correct label
                }
            }
            // Try to determine the dst label from the delta node_label registry.
            // node_label contains (slot, label_id) pairs seen anywhere in delta.
            let mut found = false;
            for &lid in all_label_ids {
                if node_label.contains(&(dst_slot, lid)) {
                    out.insert((dst_slot, lid));
                    found = true;
                    break;
                }
            }
            if !found {
                // No label info available — fallback to src_label_id (correct for
                // homogeneous graphs, gracefully wrong for unmapped CSR-only nodes
                // in heterogeneous graphs with no delta activity on those nodes).
                out.insert((dst_slot, src_label_id));
            }
        }
    }

    /// DFS traversal for variable-length path patterns `(src)-[:R*min..max]->(dst)`.
    ///
    /// Returns a `Vec<(dst_slot, dst_label_id)>` with **one entry per simple path**
    /// that ends at `depth ∈ [min_hops, max_hops]`.  The same destination node can
    /// appear multiple times when it is reachable via distinct simple paths
    /// (enumerative semantics, as required by OpenCypher).
    ///
    /// A simple path never visits the same node twice.  "Visited" is tracked per
    /// path using a stack that is pushed on entry and popped on backtrack — the
    /// classic DFS-with-backtracking pattern.
    ///
    /// Safety cap: `max_hops` is clamped to 10 to bound worst-case traversal.
    /// Result cap: at most `PATH_RESULT_CAP` entries are returned; a warning is
    /// printed to stderr if the cap is hit.
    ///
    /// Replaces the former global-visited BFS (existential semantics) that was
    /// correct for `shortestPath` but wrong for enumerative MATCH traversal:
    ///   - Diamond A→B→D, A→C→D: old BFS returned D once; DFS returns D twice.
    ///   - Zero-hop (`min_hops == 0`): source node still returned as-is.
    ///   - Self-loop A→A: correctly excluded (A is already in the path visited set).
    #[allow(clippy::too_many_arguments)]
    fn execute_variable_hops(
        &self,
        src_slot: u64,
        src_label_id: u32,
        min_hops: u32,
        max_hops: u32,
        delta_all: &[sparrowdb_storage::edge_store::DeltaRecord],
        node_label: &std::collections::HashSet<(u64, u32)>,
        all_label_ids: &[u32],
        neighbors_buf: &mut std::collections::HashSet<(u64, u32)>,
        use_reachability: bool,
    ) -> Vec<(u64, u32)> {
        const SAFETY_CAP: u32 = 10;
        let max_hops = max_hops.min(SAFETY_CAP);

        let mut results: Vec<(u64, u32)> = Vec::new();

        // Zero-hop match: source node itself is the only result.
        if min_hops == 0 {
            results.push((src_slot, src_label_id));
            if max_hops == 0 {
                return results;
            }
        }

        if use_reachability {
            // ── Reachability BFS (existential fast-path, issue #165) ──────────────────
            //
            // Global visited set: each node is enqueued at most once.
            // O(V + E) — correct when RETURN DISTINCT is present and no path
            // variable is bound, so per-path enumeration is not needed.
            let mut global_visited: std::collections::HashSet<(u64, u32)> =
                std::collections::HashSet::new();
            global_visited.insert((src_slot, src_label_id));

            let mut frontier: std::collections::VecDeque<(u64, u32, u32)> =
                std::collections::VecDeque::new();
            frontier.push_back((src_slot, src_label_id, 0));

            while let Some((cur_slot, cur_label, depth)) = frontier.pop_front() {
                if depth >= max_hops {
                    continue;
                }
                self.get_node_neighbors_labeled(
                    cur_slot,
                    cur_label,
                    delta_all,
                    node_label,
                    all_label_ids,
                    neighbors_buf,
                );
                for (nb_slot, nb_label) in neighbors_buf.iter().copied().collect::<Vec<_>>() {
                    if global_visited.insert((nb_slot, nb_label)) {
                        let nb_depth = depth + 1;
                        if nb_depth >= min_hops {
                            results.push((nb_slot, nb_label));
                        }
                        frontier.push_back((nb_slot, nb_label, nb_depth));
                    }
                }
            }
        } else {
            // ── Enumerative DFS (full path semantics) ─────────────────────────────────
            //
            // Maximum number of result entries returned per source node.
            // Prevents unbounded memory growth on highly-connected graphs.
            const PATH_RESULT_CAP: usize = 100_000;

            // Each stack frame is `(node_slot, node_label_id, depth, neighbors)`.
            // The `neighbors` vec holds all outgoing neighbors of `node`; we consume
            // them one by one with `pop()`.  When the vec is empty we backtrack by
            // popping the frame and removing the node from `path_visited`.
            //
            // `path_visited` tracks nodes on the *current path* only (not globally),
            // so nodes that appear in two separate paths (e.g. diamond D) are each
            // visited once per path, yielding one result entry per path.
            type Frame = (u64, u32, u32, Vec<(u64, u32)>);

            // Per-path visited set — (slot, label_id) to handle heterogeneous graphs.
            let mut path_visited: std::collections::HashSet<(u64, u32)> =
                std::collections::HashSet::new();
            path_visited.insert((src_slot, src_label_id));

            // Build neighbors of source.
            self.get_node_neighbors_labeled(
                src_slot,
                src_label_id,
                delta_all,
                node_label,
                all_label_ids,
                neighbors_buf,
            );
            let src_nbrs: Vec<(u64, u32)> = neighbors_buf.iter().copied().collect();

            // Push the source frame at depth 1 (the neighbors are the hop-1 candidates).
            let mut stack: Vec<Frame> = vec![(src_slot, src_label_id, 1, src_nbrs)];

            while let Some(frame) = stack.last_mut() {
                let (_, _, depth, ref mut nbrs) = *frame;

                match nbrs.pop() {
                    None => {
                        // All neighbors exhausted — backtrack.
                        let (popped_slot, popped_label, popped_depth, _) = stack.pop().unwrap();
                        // Remove this node from path_visited only if it was added when we
                        // entered it (depth > 1; the source is seeded before the loop).
                        if popped_depth > 1 {
                            path_visited.remove(&(popped_slot, popped_label));
                        }
                    }
                    Some((nb_slot, nb_label)) => {
                        // Skip nodes already on the current path (simple path constraint).
                        if path_visited.contains(&(nb_slot, nb_label)) {
                            continue;
                        }

                        // Emit if depth is within the result window.
                        if depth >= min_hops {
                            results.push((nb_slot, nb_label));
                            if results.len() >= PATH_RESULT_CAP {
                                eprintln!(
                                    "sparrowdb: variable-length path result cap \
                                     ({PATH_RESULT_CAP}) hit; truncating results.  \
                                     Consider RETURN DISTINCT or a tighter *M..N bound."
                                );
                                return results;
                            }
                        }

                        // Recurse deeper if max_hops not yet reached.
                        if depth < max_hops {
                            path_visited.insert((nb_slot, nb_label));
                            self.get_node_neighbors_labeled(
                                nb_slot,
                                nb_label,
                                delta_all,
                                node_label,
                                all_label_ids,
                                neighbors_buf,
                            );
                            let next_nbrs: Vec<(u64, u32)> =
                                neighbors_buf.iter().copied().collect();
                            stack.push((nb_slot, nb_label, depth + 1, next_nbrs));
                        }
                    }
                }
            }
        }

        results
    }

    /// Compatibility shim used by callers that do not need per-node label tracking.
    fn get_node_neighbors_by_slot(
        &self,
        src_slot: u64,
        src_label_id: u32,
        delta_all: &[sparrowdb_storage::edge_store::DeltaRecord],
    ) -> Vec<u64> {
        let csr_neighbors: Vec<u64> = self.csr_neighbors_all(src_slot);
        let delta_neighbors: Vec<u64> = delta_all
            .iter()
            .filter(|r| {
                let r_src_label = (r.src.0 >> 32) as u32;
                let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                r_src_label == src_label_id && r_src_slot == src_slot
            })
            .map(|r| r.dst.0 & 0xFFFF_FFFF)
            .collect();
        let mut all: std::collections::HashSet<u64> = csr_neighbors.into_iter().collect();
        all.extend(delta_neighbors);
        all.into_iter().collect()
    }

    /// Execute a variable-length path query: `MATCH (a:L1)-[:R*M..N]->(b:L2) RETURN …`.
    fn execute_variable_length(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        let dst_node_pat = &pat.nodes[1];
        let rel_pat = &pat.rels[0];

        if rel_pat.dir != sparrowdb_cypher::ast::EdgeDir::Outgoing {
            return Err(sparrowdb_common::Error::Unimplemented);
        }

        let min_hops = rel_pat.min_hops.unwrap_or(1);
        let max_hops = rel_pat.max_hops.unwrap_or(10); // unbounded → cap at 10

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();

        let src_label_id = self
            .snapshot
            .catalog
            .get_label(&src_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;
        // dst_label_id is None when the destination pattern has no label constraint.
        let dst_label_id: Option<u32> = if dst_label.is_empty() {
            None
        } else {
            Some(
                self.snapshot
                    .catalog
                    .get_label(&dst_label)?
                    .ok_or(sparrowdb_common::Error::NotFound)? as u32,
            )
        };

        let hwm_src = self.snapshot.store.hwm_for_label(src_label_id)?;

        let col_ids_src = collect_col_ids_for_var(&src_node_pat.var, column_names, src_label_id);
        let col_ids_dst =
            collect_col_ids_for_var(&dst_node_pat.var, column_names, dst_label_id.unwrap_or(0));

        // Build dst read set: projection columns + dst inline-prop filter columns +
        // WHERE-clause columns on the dst variable.  Mirrors the 1-hop code (SPA-224).
        let dst_all_col_ids: Vec<u32> = {
            let mut v = col_ids_dst.clone();
            for p in &dst_node_pat.props {
                let col_id = prop_name_to_col_id(&p.key);
                if !v.contains(&col_id) {
                    v.push(col_id);
                }
            }
            if let Some(ref where_expr) = m.where_clause {
                collect_col_ids_from_expr(where_expr, &mut v);
            }
            v
        };

        let mut rows: Vec<Vec<Value>> = Vec::new();
        // NOTE: No deduplication by (src, dst) here.  With DFS-with-backtracking
        // the traversal returns one entry per *simple path*, so the same destination
        // can appear multiple times when reachable via distinct paths (enumerative
        // semantics required by OpenCypher).  The old global-visited BFS never
        // produced duplicates and needed this guard; the DFS replacement does not.

        // Precompute label-id → name map once so that the hot path inside
        // `for dst_slot in dst_nodes` does not call `list_labels()` per node.
        let labels_by_id: std::collections::HashMap<u16, String> = self
            .snapshot
            .catalog
            .list_labels()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // SPA-275: hoist delta read and node_label map out of the per-source loop.
        // Previously execute_variable_hops rebuilt these on every call — O(sources)
        // delta reads and O(sources × delta_records) HashMap insertions per query.
        // Now we build them once and pass references into the BFS.
        let delta_all = self.read_delta_all();
        let mut node_label: std::collections::HashSet<(u64, u32)> =
            std::collections::HashSet::new();
        for r in &delta_all {
            let src_s = r.src.0 & 0xFFFF_FFFF;
            let src_l = (r.src.0 >> 32) as u32;
            node_label.insert((src_s, src_l));
            let dst_s = r.dst.0 & 0xFFFF_FFFF;
            let dst_l = (r.dst.0 >> 32) as u32;
            node_label.insert((dst_s, dst_l));
        }
        let mut all_label_ids: Vec<u32> = node_label.iter().map(|&(_, l)| l).collect();
        all_label_ids.sort_unstable();
        all_label_ids.dedup();

        // Reusable neighbors buffer: allocated once, cleared between frontier nodes.
        let mut neighbors_buf: std::collections::HashSet<(u64, u32)> =
            std::collections::HashSet::new();

        for src_slot in 0..hwm_src {
            // SPA-254: check per-query deadline at every slot boundary.
            self.check_deadline()?;

            let src_node = NodeId(((src_label_id as u64) << 32) | src_slot);

            // Fetch source props (for filter + projection).
            let src_all_col_ids: Vec<u32> = {
                let mut v = col_ids_src.clone();
                for p in &src_node_pat.props {
                    let col_id = prop_name_to_col_id(&p.key);
                    if !v.contains(&col_id) {
                        v.push(col_id);
                    }
                }
                if let Some(ref where_expr) = m.where_clause {
                    collect_col_ids_from_expr(where_expr, &mut v);
                }
                v
            };
            let src_props = read_node_props(&self.snapshot.store, src_node, &src_all_col_ids)?;

            if !self.matches_prop_filter(&src_props, &src_node_pat.props) {
                continue;
            }

            // BFS to find all reachable (slot, label_id) pairs within [min_hops, max_hops].
            // delta_all, node_label, all_label_ids, and neighbors_buf are hoisted out of
            // this loop (SPA-275) and reused across all source nodes.
            // Use reachability BFS when RETURN DISTINCT is present and no path variable
            // is bound (issue #165). Otherwise use enumerative DFS for full path semantics.
            let use_reachability = m.distinct && rel_pat.var.is_empty();
            let dst_nodes = self.execute_variable_hops(
                src_slot,
                src_label_id,
                min_hops,
                max_hops,
                &delta_all,
                &node_label,
                &all_label_ids,
                &mut neighbors_buf,
                use_reachability,
            );

            for (dst_slot, actual_label_id) in dst_nodes {
                // When the destination pattern specifies a label, only include nodes
                // whose actual label (recovered from the delta) matches.
                if let Some(required_label) = dst_label_id {
                    if actual_label_id != required_label {
                        continue;
                    }
                }

                // Use the actual label_id to construct the NodeId so that
                // heterogeneous graph nodes are addressed correctly.
                let resolved_dst_label_id = dst_label_id.unwrap_or(actual_label_id);

                let dst_node = NodeId(((resolved_dst_label_id as u64) << 32) | dst_slot);
                // SPA-224: read dst props using the full column set (projection +
                // inline filter + WHERE), not just the projection set.  Without the
                // filter columns the inline prop check below always fails silently
                // when the dst variable is not referenced in RETURN.
                let dst_props = read_node_props(&self.snapshot.store, dst_node, &dst_all_col_ids)?;

                if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                    continue;
                }

                // Resolve the actual label name for this destination node so that
                // labels(x) and label metadata work even when the pattern is unlabeled.
                // Use the precomputed map to avoid calling list_labels() per node.
                let resolved_dst_label_name: String = if !dst_label.is_empty() {
                    dst_label.clone()
                } else {
                    labels_by_id
                        .get(&(actual_label_id as u16))
                        .cloned()
                        .unwrap_or_default()
                };

                // Apply WHERE clause.
                if let Some(ref where_expr) = m.where_clause {
                    let mut row_vals = build_row_vals(
                        &src_props,
                        &src_node_pat.var,
                        &col_ids_src,
                        &self.snapshot.store,
                    );
                    row_vals.extend(build_row_vals(
                        &dst_props,
                        &dst_node_pat.var,
                        &col_ids_dst,
                        &self.snapshot.store,
                    ));
                    // Inject relationship metadata so type(r) works in WHERE.
                    if !rel_pat.var.is_empty() {
                        row_vals.insert(
                            format!("{}.__type__", rel_pat.var),
                            Value::String(rel_pat.rel_type.clone()),
                        );
                    }
                    // Inject node label metadata so labels(n) works in WHERE.
                    if !src_node_pat.var.is_empty() && !src_label.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", src_node_pat.var),
                            Value::List(vec![Value::String(src_label.clone())]),
                        );
                    }
                    // Use resolved_dst_label_name so labels(x) works even for unlabeled
                    // destination patterns (dst_label is empty but actual_label_id is known).
                    if !dst_node_pat.var.is_empty() && !resolved_dst_label_name.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", dst_node_pat.var),
                            Value::List(vec![Value::String(resolved_dst_label_name.clone())]),
                        );
                    }
                    row_vals.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &row_vals) {
                        continue;
                    }
                }

                let rel_var_type = if !rel_pat.var.is_empty() {
                    Some((rel_pat.var.as_str(), rel_pat.rel_type.as_str()))
                } else {
                    None
                };
                let src_label_meta = if !src_node_pat.var.is_empty() && !src_label.is_empty() {
                    Some((src_node_pat.var.as_str(), src_label.as_str()))
                } else {
                    None
                };
                let dst_label_meta =
                    if !dst_node_pat.var.is_empty() && !resolved_dst_label_name.is_empty() {
                        Some((dst_node_pat.var.as_str(), resolved_dst_label_name.as_str()))
                    } else {
                        None
                    };
                let row = project_hop_row(
                    &src_props,
                    &dst_props,
                    column_names,
                    &src_node_pat.var,
                    &dst_node_pat.var,
                    rel_var_type,
                    src_label_meta,
                    dst_label_meta,
                    &self.snapshot.store,
                );
                rows.push(row);
            }
        }

        // DISTINCT
        if m.distinct {
            deduplicate_rows(&mut rows);
        }

        // ORDER BY
        apply_order_by(&mut rows, m, column_names);

        // SKIP
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(rows.len());
            rows.drain(0..skip);
        }

        // LIMIT
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(
            rows = rows.len(),
            min_hops,
            max_hops,
            "variable-length traversal complete"
        );
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── Property filter helpers ───────────────────────────────────────────────

    fn matches_prop_filter(
        &self,
        props: &[(u32, u64)],
        filters: &[sparrowdb_cypher::ast::PropEntry],
    ) -> bool {
        matches_prop_filter_static(props, filters, &self.dollar_params(), &self.snapshot.store)
    }

    /// Build a map of runtime parameters keyed with a `$` prefix,
    /// suitable for passing to `eval_expr` / `eval_where`.
    ///
    /// For example, `params["name"] = Value::String("Alice")` becomes
    /// `{"$name": Value::String("Alice")}` in the returned map.
    fn dollar_params(&self) -> HashMap<String, Value> {
        self.params
            .iter()
            .map(|(k, v)| (format!("${k}"), v.clone()))
            .collect()
    }

    // ── Graph-aware expression evaluation (SPA-136, SPA-137, SPA-138) ────────

    /// Evaluate an expression that may require graph access (EXISTS, ShortestPath).
    fn eval_expr_graph(&self, expr: &Expr, vals: &HashMap<String, Value>) -> Value {
        match expr {
            Expr::ExistsSubquery(ep) => Value::Bool(self.eval_exists_subquery(ep, vals)),
            Expr::ShortestPath(sp) => self.eval_shortest_path_expr(sp, vals),
            Expr::CaseWhen {
                branches,
                else_expr,
            } => {
                for (cond, then_val) in branches {
                    if let Value::Bool(true) = self.eval_expr_graph(cond, vals) {
                        return self.eval_expr_graph(then_val, vals);
                    }
                }
                else_expr
                    .as_ref()
                    .map(|e| self.eval_expr_graph(e, vals))
                    .unwrap_or(Value::Null)
            }
            Expr::And(l, r) => {
                match (self.eval_expr_graph(l, vals), self.eval_expr_graph(r, vals)) {
                    (Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
                    _ => Value::Null,
                }
            }
            Expr::Or(l, r) => {
                match (self.eval_expr_graph(l, vals), self.eval_expr_graph(r, vals)) {
                    (Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),
                    _ => Value::Null,
                }
            }
            Expr::Not(inner) => match self.eval_expr_graph(inner, vals) {
                Value::Bool(b) => Value::Bool(!b),
                _ => Value::Null,
            },
            // SPA-134: PropAccess where the variable resolves to a NodeRef (e.g. `WITH n AS person
            // RETURN person.name`).  Fetch the property from the node store directly.
            Expr::PropAccess { var, prop } => {
                // Try normal key first (col_N or direct "var.prop" entry).
                let normal = eval_expr(expr, vals);
                if !matches!(normal, Value::Null) {
                    return normal;
                }
                // Fallback: if the variable is a NodeRef, read the property from the store.
                if let Some(Value::NodeRef(node_id)) = vals
                    .get(var.as_str())
                    .or_else(|| vals.get(&format!("{var}.__node_id__")))
                {
                    let col_id = prop_name_to_col_id(prop);
                    if let Ok(props) = self.snapshot.store.get_node_raw(*node_id, &[col_id]) {
                        if let Some(&(_, raw)) = props.iter().find(|(c, _)| *c == col_id) {
                            return decode_raw_val(raw, &self.snapshot.store);
                        }
                    }
                }
                Value::Null
            }
            _ => eval_expr(expr, vals),
        }
    }

    /// Graph-aware WHERE evaluation — falls back to eval_where for pure expressions.
    fn eval_where_graph(&self, expr: &Expr, vals: &HashMap<String, Value>) -> bool {
        match self.eval_expr_graph(expr, vals) {
            Value::Bool(b) => b,
            _ => eval_where(expr, vals),
        }
    }

    /// Evaluate `EXISTS { (n)-[:REL]->(:DstLabel) }` — SPA-137.
    fn eval_exists_subquery(
        &self,
        ep: &sparrowdb_cypher::ast::ExistsPattern,
        vals: &HashMap<String, Value>,
    ) -> bool {
        let path = &ep.path;
        if path.nodes.len() < 2 || path.rels.is_empty() {
            return false;
        }
        let src_pat = &path.nodes[0];
        let dst_pat = &path.nodes[1];
        let rel_pat = &path.rels[0];

        let src_node_id = match self.resolve_node_id_from_var(&src_pat.var, vals) {
            Some(id) => id,
            None => return false,
        };
        let src_slot = src_node_id.0 & 0xFFFF_FFFF;
        let src_label_id = (src_node_id.0 >> 32) as u32;

        let dst_label = dst_pat.labels.first().map(String::as_str).unwrap_or("");
        let dst_label_id_opt: Option<u32> = if dst_label.is_empty() {
            None
        } else {
            self.snapshot
                .catalog
                .get_label(dst_label)
                .ok()
                .flatten()
                .map(|id| id as u32)
        };

        let rel_lookup = if let Some(dst_lid) = dst_label_id_opt {
            self.resolve_rel_table_id(src_label_id, dst_lid, &rel_pat.rel_type)
        } else {
            RelTableLookup::All
        };

        let csr_nb: Vec<u64> = match rel_lookup {
            RelTableLookup::Found(rtid) => self.csr_neighbors(rtid, src_slot),
            RelTableLookup::NotFound => return false,
            RelTableLookup::All => self.csr_neighbors_all(src_slot),
        };
        let delta_nb: Vec<u64> = self
            .read_delta_all()
            .into_iter()
            .filter(|r| {
                let r_src_label = (r.src.0 >> 32) as u32;
                let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                if r_src_label != src_label_id || r_src_slot != src_slot {
                    return false;
                }
                // When a destination label is known, only keep edges that point
                // to nodes of that label — slots are label-relative so mixing
                // labels causes false positive matches.
                if let Some(dst_lid) = dst_label_id_opt {
                    let r_dst_label = (r.dst.0 >> 32) as u32;
                    r_dst_label == dst_lid
                } else {
                    true
                }
            })
            .map(|r| r.dst.0 & 0xFFFF_FFFF)
            .collect();

        let all_nb: std::collections::HashSet<u64> = csr_nb.into_iter().chain(delta_nb).collect();

        for dst_slot in all_nb {
            if let Some(did) = dst_label_id_opt {
                let probe_id = NodeId(((did as u64) << 32) | dst_slot);
                if self.snapshot.store.get_node_raw(probe_id, &[]).is_err() {
                    continue;
                }
                if !dst_pat.props.is_empty() {
                    let col_ids: Vec<u32> = dst_pat
                        .props
                        .iter()
                        .map(|p| prop_name_to_col_id(&p.key))
                        .collect();
                    match self.snapshot.store.get_node_raw(probe_id, &col_ids) {
                        Ok(props) => {
                            let params = self.dollar_params();
                            if !matches_prop_filter_static(
                                &props,
                                &dst_pat.props,
                                &params,
                                &self.snapshot.store,
                            ) {
                                continue;
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }
            return true;
        }
        false
    }

    /// Resolve a NodeId from `vals` for a variable name.
    fn resolve_node_id_from_var(&self, var: &str, vals: &HashMap<String, Value>) -> Option<NodeId> {
        let id_key = format!("{var}.__node_id__");
        if let Some(Value::NodeRef(nid)) = vals.get(&id_key) {
            return Some(*nid);
        }
        if let Some(Value::NodeRef(nid)) = vals.get(var) {
            return Some(*nid);
        }
        None
    }

    /// Evaluate `shortestPath((src)-[:REL*]->(dst))` — SPA-136.
    fn eval_shortest_path_expr(
        &self,
        sp: &sparrowdb_cypher::ast::ShortestPathExpr,
        vals: &HashMap<String, Value>,
    ) -> Value {
        // Resolve src: if the variable is already bound as a NodeRef, extract
        // label_id and slot from the NodeId directly (high 32 bits = label_id,
        // low 32 bits = slot). This handles the case where shortestPath((a)-...)
        // refers to a variable bound in the outer MATCH without repeating its label.
        let (src_label_id, src_slot) =
            if let Some(nid) = self.resolve_node_id_from_var(&sp.src_var, vals) {
                let label_id = (nid.0 >> 32) as u32;
                let slot = nid.0 & 0xFFFF_FFFF;
                (label_id, slot)
            } else {
                // Fall back to label lookup + property scan.
                let label_id = match self.snapshot.catalog.get_label(&sp.src_label) {
                    Ok(Some(id)) => id as u32,
                    _ => return Value::Null,
                };
                match self.find_node_by_props(label_id, &sp.src_props) {
                    Some(slot) => (label_id, slot),
                    None => return Value::Null,
                }
            };

        let dst_slot = if let Some(nid) = self.resolve_node_id_from_var(&sp.dst_var, vals) {
            nid.0 & 0xFFFF_FFFF
        } else {
            let dst_label_id = match self.snapshot.catalog.get_label(&sp.dst_label) {
                Ok(Some(id)) => id as u32,
                _ => return Value::Null,
            };
            match self.find_node_by_props(dst_label_id, &sp.dst_props) {
                Some(slot) => slot,
                None => return Value::Null,
            }
        };

        match self.bfs_shortest_path(src_slot, src_label_id, dst_slot, 10) {
            Some(hops) => Value::Int64(hops as i64),
            None => Value::Null,
        }
    }

    /// Scan a label for the first node matching all property filters.
    fn find_node_by_props(
        &self,
        label_id: u32,
        props: &[sparrowdb_cypher::ast::PropEntry],
    ) -> Option<u64> {
        if props.is_empty() {
            return None;
        }
        let hwm = self.snapshot.store.hwm_for_label(label_id).ok()?;
        let col_ids: Vec<u32> = props.iter().map(|p| prop_name_to_col_id(&p.key)).collect();
        let params = self.dollar_params();
        for slot in 0..hwm {
            let node_id = NodeId(((label_id as u64) << 32) | slot);
            if let Ok(raw_props) = self.snapshot.store.get_node_raw(node_id, &col_ids) {
                if matches_prop_filter_static(&raw_props, props, &params, &self.snapshot.store) {
                    return Some(slot);
                }
            }
        }
        None
    }

    /// BFS from `src_slot` to `dst_slot`, returning the hop count or None.
    ///
    /// `src_label_id` is used to look up edges in the WAL delta for every hop.
    /// When all nodes in the shortest path share the same label (the typical
    /// single-label homogeneous graph), this is correct.  Heterogeneous graphs
    /// with intermediate nodes of a different label will still find paths via
    /// the CSR (`csr_neighbors_all`), which is label-agnostic; only in-flight
    /// WAL edges from intermediate nodes of a different label may be missed.
    fn bfs_shortest_path(
        &self,
        src_slot: u64,
        src_label_id: u32,
        dst_slot: u64,
        max_hops: u32,
    ) -> Option<u32> {
        if src_slot == dst_slot {
            return Some(0);
        }
        // Hoist delta read out of the BFS loop to avoid repeated I/O.
        let delta_all = self.read_delta_all();
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(src_slot);
        let mut frontier: Vec<u64> = vec![src_slot];

        for depth in 1..=max_hops {
            let mut next_frontier: Vec<u64> = Vec::new();
            for &node_slot in &frontier {
                let neighbors =
                    self.get_node_neighbors_by_slot(node_slot, src_label_id, &delta_all);
                for nb in neighbors {
                    if nb == dst_slot {
                        return Some(depth);
                    }
                    if visited.insert(nb) {
                        next_frontier.push(nb);
                    }
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }
        None
    }

    /// Engine-aware aggregate_rows: evaluates graph-dependent RETURN expressions
    /// (ShortestPath, EXISTS) via self before delegating to the standalone helper.
    fn aggregate_rows_graph(
        &self,
        rows: &[HashMap<String, Value>],
        return_items: &[ReturnItem],
    ) -> Vec<Vec<Value>> {
        // Check if any return item needs graph access.
        let needs_graph = return_items.iter().any(|item| expr_needs_graph(&item.expr));
        if !needs_graph {
            return aggregate_rows(rows, return_items);
        }
        // For graph-dependent items, project each row using eval_expr_graph.
        rows.iter()
            .map(|row_vals| {
                return_items
                    .iter()
                    .map(|item| self.eval_expr_graph(&item.expr, row_vals))
                    .collect()
            })
            .collect()
    }
}

// ── Free-standing prop-filter helper (usable without &self) ───────────────────

fn matches_prop_filter_static(
    props: &[(u32, u64)],
    filters: &[sparrowdb_cypher::ast::PropEntry],
    params: &HashMap<String, Value>,
    store: &NodeStore,
) -> bool {
    for f in filters {
        let col_id = prop_name_to_col_id(&f.key);
        let stored_val = props.iter().find(|(c, _)| *c == col_id).map(|(_, v)| *v);

        // Evaluate the filter expression (supports literals, function calls, and
        // runtime parameters via `$name` — params are keyed as `"$name"` in the map).
        let filter_val = eval_expr(&f.value, params);
        let matches = match filter_val {
            Value::Int64(n) => {
                // Int64 values are stored with TAG_INT64 (0x00) in the top byte.
                // Use StoreValue::to_u64() for canonical encoding (SPA-169).
                stored_val == Some(StoreValue::Int64(n).to_u64())
            }
            Value::Bool(b) => {
                // Booleans are stored as Int64(1) for true, Int64(0) for false
                // (see value_to_store_value / literal_to_store_value).
                let expected = StoreValue::Int64(if b { 1 } else { 0 }).to_u64();
                stored_val == Some(expected)
            }
            Value::String(s) => {
                // Use store.raw_str_matches to handle both inline (≤7 bytes) and
                // overflow (>7 bytes) string encodings (SPA-212).
                stored_val.is_some_and(|raw| store.raw_str_matches(raw, &s))
            }
            Value::Float64(f) => {
                // Float values are stored via TAG_FLOAT in the overflow heap (SPA-267).
                // Decode the raw stored u64 back to a Value::Float and compare.
                stored_val.is_some_and(|raw| {
                    matches!(store.decode_raw_value(raw), StoreValue::Float(stored_f) if stored_f == f)
                })
            }
            Value::Null => true, // null filter passes (param-like behaviour)
            _ => false,
        };
        if !matches {
            return false;
        }
    }
    true
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Evaluate an UNWIND list expression to a concrete `Vec<Value>`.
///
/// Supports:
/// - `Expr::List([...])` — list literal
/// - `Expr::Literal(Param(name))` — looks up `name` in `params`; expects `Value::List`
/// - `Expr::FnCall { name: "range", args }` — integer range expansion
fn eval_list_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Vec<Value>> {
    match expr {
        Expr::List(elems) => {
            let mut values = Vec::with_capacity(elems.len());
            for elem in elems {
                values.push(eval_scalar_expr(elem));
            }
            Ok(values)
        }
        Expr::Literal(Literal::Param(name)) => {
            // Look up the parameter in the runtime params map.
            match params.get(name) {
                Some(Value::List(items)) => Ok(items.clone()),
                Some(other) => {
                    // Non-list value: wrap as a single-element list so the
                    // caller can still iterate (matches Neo4j behaviour).
                    Ok(vec![other.clone()])
                }
                None => {
                    // Parameter not supplied — produce an empty list (no rows).
                    Ok(vec![])
                }
            }
        }
        Expr::FnCall { name, args } => {
            // Expand function calls that produce lists.
            // Currently only `range(start, end[, step])` is supported here.
            let name_lc = name.to_lowercase();
            if name_lc == "range" {
                let empty_vals: std::collections::HashMap<String, Value> =
                    std::collections::HashMap::new();
                let evaluated: Vec<Value> =
                    args.iter().map(|a| eval_expr(a, &empty_vals)).collect();
                // range(start, end[, step]) → Vec<Int64>
                let start = match evaluated.first() {
                    Some(Value::Int64(n)) => *n,
                    _ => {
                        return Err(sparrowdb_common::Error::InvalidArgument(
                            "range() expects integer arguments".into(),
                        ))
                    }
                };
                let end = match evaluated.get(1) {
                    Some(Value::Int64(n)) => *n,
                    _ => {
                        return Err(sparrowdb_common::Error::InvalidArgument(
                            "range() expects at least 2 integer arguments".into(),
                        ))
                    }
                };
                let step: i64 = match evaluated.get(2) {
                    Some(Value::Int64(n)) => *n,
                    None => 1,
                    _ => 1,
                };
                if step == 0 {
                    return Err(sparrowdb_common::Error::InvalidArgument(
                        "range(): step must not be zero".into(),
                    ));
                }
                let mut values = Vec::new();
                if step > 0 {
                    let mut i = start;
                    while i <= end {
                        values.push(Value::Int64(i));
                        i += step;
                    }
                } else {
                    let mut i = start;
                    while i >= end {
                        values.push(Value::Int64(i));
                        i += step;
                    }
                }
                Ok(values)
            } else {
                // Other function calls are not list-producing.
                Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "UNWIND: function '{name}' does not return a list"
                )))
            }
        }
        other => Err(sparrowdb_common::Error::InvalidArgument(format!(
            "UNWIND expression is not a list: {:?}",
            other
        ))),
    }
}

/// Evaluate a scalar expression to a `Value` (no row context needed).
fn eval_scalar_expr(expr: &Expr) -> Value {
    match expr {
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => Value::Int64(*n),
            Literal::Float(f) => Value::Float64(*f),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Null => Value::Null,
            Literal::Param(_) => Value::Null,
        },
        _ => Value::Null,
    }
}

fn extract_return_column_names(items: &[ReturnItem]) -> Vec<String> {
    items
        .iter()
        .map(|item| match &item.alias {
            Some(alias) => alias.clone(),
            None => match &item.expr {
                Expr::PropAccess { var, prop } => format!("{var}.{prop}"),
                Expr::Var(v) => v.clone(),
                Expr::CountStar => "count(*)".to_string(),
                Expr::FnCall { name, args } => {
                    let arg_str = args
                        .first()
                        .map(|a| match a {
                            Expr::PropAccess { var, prop } => format!("{var}.{prop}"),
                            Expr::Var(v) => v.clone(),
                            _ => "*".to_string(),
                        })
                        .unwrap_or_else(|| "*".to_string());
                    format!("{}({})", name.to_lowercase(), arg_str)
                }
                _ => "?".to_string(),
            },
        })
        .collect()
}

/// Collect all column IDs referenced by property accesses in an expression,
/// scoped to a specific variable name.
///
/// Only `PropAccess` nodes whose `var` field matches `target_var` contribute
/// column IDs, so callers can separate src-side from fof-side columns without
/// accidentally fetching unrelated properties from the wrong node.
fn collect_col_ids_from_expr_for_var(expr: &Expr, target_var: &str, out: &mut Vec<u32>) {
    match expr {
        Expr::PropAccess { var, prop } => {
            if var == target_var {
                let col_id = prop_name_to_col_id(prop);
                if !out.contains(&col_id) {
                    out.push(col_id);
                }
            }
        }
        Expr::BinOp { left, right, .. } => {
            collect_col_ids_from_expr_for_var(left, target_var, out);
            collect_col_ids_from_expr_for_var(right, target_var, out);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_col_ids_from_expr_for_var(l, target_var, out);
            collect_col_ids_from_expr_for_var(r, target_var, out);
        }
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_col_ids_from_expr_for_var(inner, target_var, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_col_ids_from_expr_for_var(expr, target_var, out);
            for item in list {
                collect_col_ids_from_expr_for_var(item, target_var, out);
            }
        }
        Expr::FnCall { args, .. } | Expr::List(args) => {
            for arg in args {
                collect_col_ids_from_expr_for_var(arg, target_var, out);
            }
        }
        Expr::ListPredicate {
            list_expr,
            predicate,
            ..
        } => {
            collect_col_ids_from_expr_for_var(list_expr, target_var, out);
            collect_col_ids_from_expr_for_var(predicate, target_var, out);
        }
        // SPA-138: CASE WHEN branches may reference property accesses.
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            for (cond, then_val) in branches {
                collect_col_ids_from_expr_for_var(cond, target_var, out);
                collect_col_ids_from_expr_for_var(then_val, target_var, out);
            }
            if let Some(e) = else_expr {
                collect_col_ids_from_expr_for_var(e, target_var, out);
            }
        }
        _ => {}
    }
}

/// Collect all column IDs referenced by property accesses in an expression.
///
/// Used to ensure that every column needed by a WHERE clause is read from
/// disk before predicate evaluation, even when it is not in the RETURN list.
fn collect_col_ids_from_expr(expr: &Expr, out: &mut Vec<u32>) {
    match expr {
        Expr::PropAccess { prop, .. } => {
            let col_id = prop_name_to_col_id(prop);
            if !out.contains(&col_id) {
                out.push(col_id);
            }
        }
        Expr::BinOp { left, right, .. } => {
            collect_col_ids_from_expr(left, out);
            collect_col_ids_from_expr(right, out);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_col_ids_from_expr(l, out);
            collect_col_ids_from_expr(r, out);
        }
        Expr::Not(inner) => collect_col_ids_from_expr(inner, out),
        Expr::InList { expr, list, .. } => {
            collect_col_ids_from_expr(expr, out);
            for item in list {
                collect_col_ids_from_expr(item, out);
            }
        }
        // FnCall arguments (e.g. collect(p.name)) may reference properties.
        Expr::FnCall { args, .. } => {
            for arg in args {
                collect_col_ids_from_expr(arg, out);
            }
        }
        Expr::ListPredicate {
            list_expr,
            predicate,
            ..
        } => {
            collect_col_ids_from_expr(list_expr, out);
            collect_col_ids_from_expr(predicate, out);
        }
        // Inline list literal: recurse into each element so property references are loaded.
        Expr::List(items) => {
            for item in items {
                collect_col_ids_from_expr(item, out);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_col_ids_from_expr(inner, out);
        }
        // SPA-138: CASE WHEN branches may reference property accesses.
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            for (cond, then_val) in branches {
                collect_col_ids_from_expr(cond, out);
                collect_col_ids_from_expr(then_val, out);
            }
            if let Some(e) = else_expr {
                collect_col_ids_from_expr(e, out);
            }
        }
        _ => {}
    }
}

/// Convert an AST `Literal` to the `StoreValue` used by the node store.
///
/// Integers are stored as `Int64`; strings are stored as `Bytes` (up to 8 bytes
/// inline, matching the storage layer's encoding in `Value::to_u64`).
#[allow(dead_code)]
fn literal_to_store_value(lit: &Literal) -> StoreValue {
    match lit {
        Literal::Int(n) => StoreValue::Int64(*n),
        Literal::String(s) => StoreValue::Bytes(s.as_bytes().to_vec()),
        Literal::Float(f) => StoreValue::Float(*f),
        Literal::Bool(b) => StoreValue::Int64(if *b { 1 } else { 0 }),
        Literal::Null | Literal::Param(_) => StoreValue::Int64(0),
    }
}

/// Convert an evaluated `Value` to the `StoreValue` used by the node store.
///
/// Used when a node property value is an arbitrary expression (e.g.
/// `datetime()`), rather than a bare literal.
fn value_to_store_value(val: Value) -> StoreValue {
    match val {
        Value::Int64(n) => StoreValue::Int64(n),
        Value::Float64(f) => StoreValue::Float(f),
        Value::Bool(b) => StoreValue::Int64(if b { 1 } else { 0 }),
        Value::String(s) => StoreValue::Bytes(s.into_bytes()),
        Value::Null => StoreValue::Int64(0),
        Value::NodeRef(id) => StoreValue::Int64(id.0 as i64),
        Value::EdgeRef(id) => StoreValue::Int64(id.0 as i64),
        Value::List(_) => StoreValue::Int64(0),
        Value::Map(_) => StoreValue::Int64(0),
    }
}

/// Encode a string literal using the type-tagged storage encoding (SPA-169).
///
/// Returns the `u64` that `StoreValue::Bytes(s.as_bytes()).to_u64()` produces
/// with the new tagged encoding, allowing prop-filter and WHERE-clause
/// comparisons against stored raw column values.
fn string_to_raw_u64(s: &str) -> u64 {
    StoreValue::Bytes(s.as_bytes().to_vec()).to_u64()
}

/// SPA-249: attempt an O(log n) index lookup for a node pattern's prop filters.
///
/// Returns `Some(slots)` when *all* of the following hold:
/// 1. There is exactly one inline prop filter in `props`.
/// 2. The filter value is a `Literal::Int` or a short `Literal::String` (≤ 7 bytes,
///    i.e., it can be represented inline without a heap pointer).
/// 3. The `(label_id, col_id)` pair is present in the index.
///
/// In all other cases (multiple filters, overflow string, param literal, no
/// index entry) returns `None` so the caller falls back to a full O(n) scan.
fn try_index_lookup_for_props(
    props: &[sparrowdb_cypher::ast::PropEntry],
    label_id: u32,
    prop_index: &sparrowdb_storage::property_index::PropertyIndex,
) -> Option<Vec<u32>> {
    // Only handle the single-equality-filter case.
    if props.len() != 1 {
        return None;
    }
    let filter = &props[0];

    // Encode the filter literal as a raw u64 (the same encoding used on disk).
    let raw_value: u64 = match &filter.value {
        Expr::Literal(Literal::Int(n)) => StoreValue::Int64(*n).to_u64(),
        Expr::Literal(Literal::String(s)) if s.len() <= 7 => {
            StoreValue::Bytes(s.as_bytes().to_vec()).to_u64()
        }
        // Overflow strings (> 7 bytes) carry a heap pointer; not indexable.
        // Params and other expression types also fall back to full scan.
        _ => return None,
    };

    let col_id = prop_name_to_col_id(&filter.key);
    if !prop_index.is_indexed(label_id, col_id) {
        return None;
    }
    Some(prop_index.lookup(label_id, col_id, raw_value).to_vec())
}

/// SPA-251: Try to use the text index for a simple CONTAINS or STARTS WITH
/// predicate in the WHERE clause.
///
/// Returns `Some(slots)` when:
/// 1. The WHERE expression is a single `BinOp` with `Contains` or `StartsWith`.
/// 2. The left operand is a `PropAccess { var, prop }` where `var` matches
///    the node variable name (`node_var`).
/// 3. The right operand is a `Literal::String`.
/// 4. The `(label_id, col_id)` pair is present in the text index.
///
/// Returns `None` for compound predicates, non-string literals, or when the
/// column has not been indexed — the caller falls back to a full O(n) scan.
fn try_text_index_lookup(
    expr: &Expr,
    node_var: &str,
    label_id: u32,
    text_index: &TextIndex,
) -> Option<Vec<u32>> {
    let (left, op, right) = match expr {
        Expr::BinOp { left, op, right }
            if matches!(op, BinOpKind::Contains | BinOpKind::StartsWith) =>
        {
            (left.as_ref(), op, right.as_ref())
        }
        _ => return None,
    };

    // Left must be a property access on the node variable.
    let prop_name = match left {
        Expr::PropAccess { var, prop } if var.as_str() == node_var => prop.as_str(),
        _ => return None,
    };

    // Right must be a string literal.
    let pattern = match right {
        Expr::Literal(Literal::String(s)) => s.as_str(),
        _ => return None,
    };

    let col_id = prop_name_to_col_id(prop_name);
    if !text_index.is_indexed(label_id, col_id) {
        return None;
    }

    let slots = match op {
        BinOpKind::Contains => text_index.lookup_contains(label_id, col_id, pattern),
        BinOpKind::StartsWith => text_index.lookup_starts_with(label_id, col_id, pattern),
        _ => return None,
    };

    Some(slots)
}

/// SPA-274 (lazy text index): Extract the property name referenced in a
/// WHERE-clause CONTAINS or STARTS WITH predicate (`n.prop CONTAINS 'str'` or
/// `n.prop STARTS WITH 'str'`) so the caller can pre-build the lazy text index
/// for that `(label_id, col_id)` pair.
///
/// Returns an empty vec if the expression is not a simple text predicate on
/// the given node variable.
fn where_clause_text_prop_names<'a>(expr: &'a Expr, node_var: &str) -> Vec<&'a str> {
    let left = match expr {
        Expr::BinOp {
            left,
            op: BinOpKind::Contains | BinOpKind::StartsWith,
            right: _,
        } => left.as_ref(),
        _ => return vec![],
    };
    if let Expr::PropAccess { var, prop } = left {
        if var.as_str() == node_var {
            return vec![prop.as_str()];
        }
    }
    vec![]
}

/// SPA-249 (lazy build): Extract all property names referenced in a WHERE-clause
/// equality predicate (`n.prop = literal` or `literal = n.prop`) so the caller
/// can pre-build the lazy index for those `(label_id, col_id)` pairs.
///
/// Returns an empty vec if the expression does not match the pattern.
fn where_clause_eq_prop_names<'a>(expr: &'a Expr, node_var: &str) -> Vec<&'a str> {
    let (left, right) = match expr {
        Expr::BinOp {
            left,
            op: BinOpKind::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return vec![],
    };
    if let Expr::PropAccess { var, prop } = left {
        if var.as_str() == node_var {
            return vec![prop.as_str()];
        }
    }
    if let Expr::PropAccess { var, prop } = right {
        if var.as_str() == node_var {
            return vec![prop.as_str()];
        }
    }
    vec![]
}

/// SPA-249 (lazy build): Extract all property names referenced in a WHERE-clause
/// range predicate (`n.prop > literal`, etc., or compound AND) so the caller
/// can pre-build the lazy index for those `(label_id, col_id)` pairs.
///
/// Returns an empty vec if the expression does not match the pattern.
fn where_clause_range_prop_names<'a>(expr: &'a Expr, node_var: &str) -> Vec<&'a str> {
    let is_range_op = |op: &BinOpKind| {
        matches!(
            op,
            BinOpKind::Gt | BinOpKind::Ge | BinOpKind::Lt | BinOpKind::Le
        )
    };

    // Simple range: `n.prop OP literal` or `literal OP n.prop`.
    if let Expr::BinOp { left, op, right } = expr {
        if is_range_op(op) {
            if let Expr::PropAccess { var, prop } = left.as_ref() {
                if var.as_str() == node_var {
                    return vec![prop.as_str()];
                }
            }
            if let Expr::PropAccess { var, prop } = right.as_ref() {
                if var.as_str() == node_var {
                    return vec![prop.as_str()];
                }
            }
            return vec![];
        }
    }

    // Compound AND: `lhs AND rhs` — collect from both sides.
    if let Expr::BinOp {
        left,
        op: BinOpKind::And,
        right,
    } = expr
    {
        let mut names: Vec<&'a str> = where_clause_range_prop_names(left, node_var);
        names.extend(where_clause_range_prop_names(right, node_var));
        return names;
    }

    vec![]
}

/// SPA-249 Phase 1b: Try to use the property equality index for a WHERE-clause
/// equality predicate of the form `n.prop = <literal>`.
///
/// Returns `Some(slots)` when:
/// 1. The WHERE expression is a `BinOp` with `Eq`, one side being
///    `PropAccess { var, prop }` where `var` == `node_var` and the other side
///    being an inline-encodable `Literal` (Int or String ≤ 7 bytes).
/// 2. The `(label_id, col_id)` pair is present in the index.
///
/// Returns `None` in all other cases so the caller falls back to a full scan.
fn try_where_eq_index_lookup(
    expr: &Expr,
    node_var: &str,
    label_id: u32,
    prop_index: &sparrowdb_storage::property_index::PropertyIndex,
) -> Option<Vec<u32>> {
    let (left, op, right) = match expr {
        Expr::BinOp { left, op, right } if matches!(op, BinOpKind::Eq) => {
            (left.as_ref(), op, right.as_ref())
        }
        _ => return None,
    };
    let _ = op;

    // Accept both `n.prop = literal` and `literal = n.prop`.
    let (prop_name, lit) = if let Expr::PropAccess { var, prop } = left {
        if var.as_str() == node_var {
            (prop.as_str(), right)
        } else {
            return None;
        }
    } else if let Expr::PropAccess { var, prop } = right {
        if var.as_str() == node_var {
            (prop.as_str(), left)
        } else {
            return None;
        }
    } else {
        return None;
    };

    let raw_value: u64 = match lit {
        Expr::Literal(Literal::Int(n)) => StoreValue::Int64(*n).to_u64(),
        Expr::Literal(Literal::String(s)) if s.len() <= 7 => {
            StoreValue::Bytes(s.as_bytes().to_vec()).to_u64()
        }
        _ => return None,
    };

    let col_id = prop_name_to_col_id(prop_name);
    if !prop_index.is_indexed(label_id, col_id) {
        return None;
    }
    Some(prop_index.lookup(label_id, col_id, raw_value).to_vec())
}

/// SPA-249 Phase 2: Try to use the property range index for WHERE-clause range
/// predicates (`>`, `>=`, `<`, `<=`) and compound AND range predicates.
///
/// Handles:
/// - Single bound: `n.age > 30`, `n.age >= 18`, `n.age < 100`, `n.age <= 65`.
/// - Compound AND with same prop and both bounds:
///   `n.age >= 18 AND n.age <= 65`.
///
/// Returns `Some(slots)` when a range can be resolved via the index.
/// Returns `None` to fall back to full scan.
fn try_where_range_index_lookup(
    expr: &Expr,
    node_var: &str,
    label_id: u32,
    prop_index: &sparrowdb_storage::property_index::PropertyIndex,
) -> Option<Vec<u32>> {
    use sparrowdb_storage::property_index::sort_key;

    /// Encode an integer literal to raw u64 (same as node_store).
    fn encode_int(n: i64) -> u64 {
        StoreValue::Int64(n).to_u64()
    }

    /// Extract a single (prop_name, lo, hi) range from a simple comparison.
    /// Returns None if not a recognised range pattern.
    #[allow(clippy::type_complexity)]
    fn extract_single_bound<'a>(
        expr: &'a Expr,
        node_var: &'a str,
    ) -> Option<(&'a str, Option<(u64, bool)>, Option<(u64, bool)>)> {
        let (left, op, right) = match expr {
            Expr::BinOp { left, op, right }
                if matches!(
                    op,
                    BinOpKind::Gt | BinOpKind::Ge | BinOpKind::Lt | BinOpKind::Le
                ) =>
            {
                (left.as_ref(), op, right.as_ref())
            }
            _ => return None,
        };

        // `n.prop OP literal`
        if let (Expr::PropAccess { var, prop }, Expr::Literal(Literal::Int(n))) = (left, right) {
            if var.as_str() != node_var {
                return None;
            }
            let sk = sort_key(encode_int(*n));
            let prop_name = prop.as_str();
            return match op {
                BinOpKind::Gt => Some((prop_name, Some((sk, false)), None)),
                BinOpKind::Ge => Some((prop_name, Some((sk, true)), None)),
                BinOpKind::Lt => Some((prop_name, None, Some((sk, false)))),
                BinOpKind::Le => Some((prop_name, None, Some((sk, true)))),
                _ => None,
            };
        }

        // `literal OP n.prop` — flip the operator direction.
        if let (Expr::Literal(Literal::Int(n)), Expr::PropAccess { var, prop }) = (left, right) {
            if var.as_str() != node_var {
                return None;
            }
            let sk = sort_key(encode_int(*n));
            let prop_name = prop.as_str();
            // `literal > n.prop` ↔ `n.prop < literal`
            return match op {
                BinOpKind::Gt => Some((prop_name, None, Some((sk, false)))),
                BinOpKind::Ge => Some((prop_name, None, Some((sk, true)))),
                BinOpKind::Lt => Some((prop_name, Some((sk, false)), None)),
                BinOpKind::Le => Some((prop_name, Some((sk, true)), None)),
                _ => None,
            };
        }

        None
    }

    // Try compound AND: `lhs AND rhs` where both sides are range predicates on
    // the same property.
    if let Expr::BinOp {
        left,
        op: BinOpKind::And,
        right,
    } = expr
    {
        if let (Some((lp, llo, lhi)), Some((rp, rlo, rhi))) = (
            extract_single_bound(left, node_var),
            extract_single_bound(right, node_var),
        ) {
            if lp == rp {
                let col_id = prop_name_to_col_id(lp);
                if !prop_index.is_indexed(label_id, col_id) {
                    return None;
                }
                // Merge the two half-open bounds: pick the most restrictive
                // (largest lower bound, smallest upper bound).  Plain `.or()`
                // is order-dependent and would silently accept a looser bound
                // when both sides specify the same direction (e.g. `age > 10
                // AND age > 20` must use `> 20`, not `> 10`).
                let lo: Option<(u64, bool)> = match (llo, rlo) {
                    (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                };
                let hi: Option<(u64, bool)> = match (lhi, rhi) {
                    (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                };
                // Validate: we need at least one bound.
                if lo.is_none() && hi.is_none() {
                    return None;
                }
                return Some(prop_index.lookup_range(label_id, col_id, lo, hi));
            }
        }
    }

    // Try single bound.
    if let Some((prop_name, lo, hi)) = extract_single_bound(expr, node_var) {
        let col_id = prop_name_to_col_id(prop_name);
        if !prop_index.is_indexed(label_id, col_id) {
            return None;
        }
        return Some(prop_index.lookup_range(label_id, col_id, lo, hi));
    }

    None
}

/// Map a property name to a col_id via the canonical FNV-1a hash.
///
/// All property names — including those that start with `col_` (e.g. `col_id`,
/// `col_name`, `col_0`) — are hashed with [`col_id_of`] so that the col_id
/// computed here always agrees with what the storage layer wrote to disk
/// (SPA-160).  The Cypher write path (`create_node_named`,
/// `execute_create_standalone`) consistently uses `col_id_of`, so the read
/// path must too.
///
/// ## SPA-165 bug fix
///
/// The previous implementation special-cased names matching `col_N`:
/// - If the suffix parsed as a `u32` the numeric value was returned directly.
/// - If it did not parse, `unwrap_or(0)` silently mapped to column 0.
///
/// Both behaviours were wrong for user-defined property names.  A name like
/// `col_id` resolved to column 0 (the tombstone sentinel), and even `col_0`
/// was inconsistent because `create_node_named` writes it at `col_id_of("col_0")`
/// while the old read path returned column 0.  The fix removes the `col_`
/// prefix shorthand entirely; every name goes through `col_id_of`.
fn prop_name_to_col_id(name: &str) -> u32 {
    col_id_of(name)
}

fn collect_col_ids_from_columns(column_names: &[String]) -> Vec<u32> {
    let mut ids = Vec::new();
    for name in column_names {
        // name could be "var.col_N" or "col_N"
        let prop = name.split('.').next_back().unwrap_or(name.as_str());
        let col_id = prop_name_to_col_id(prop);
        if !ids.contains(&col_id) {
            ids.push(col_id);
        }
    }
    ids
}

/// Collect the set of column IDs referenced by `var` in `column_names`.
///
/// `_label_id` is accepted to keep call sites consistent and is reserved for
/// future use (e.g. per-label schema lookups). It is intentionally unused in
/// the current implementation which derives column IDs purely from column names.
fn collect_col_ids_for_var(var: &str, column_names: &[String], _label_id: u32) -> Vec<u32> {
    let mut ids = Vec::new();
    for name in column_names {
        // name is either "var.col_N" or "col_N"
        if let Some((v, prop)) = name.split_once('.') {
            if v == var {
                let col_id = prop_name_to_col_id(prop);
                if !ids.contains(&col_id) {
                    ids.push(col_id);
                }
            }
        } else {
            // No dot — could be this var's column
            let col_id = prop_name_to_col_id(name.as_str());
            if !ids.contains(&col_id) {
                ids.push(col_id);
            }
        }
    }
    if ids.is_empty() {
        // Default: read col_0
        ids.push(0);
    }
    ids
}

/// Read node properties using the nullable store path (SPA-197).
///
/// Calls `get_node_raw_nullable` so that columns that were never written for
/// this node are returned as `None` (absent) rather than `0u64`.  The result
/// is a `Vec<(col_id, raw_u64)>` containing only the columns that have a real
/// stored value; callers that iterate over `col_ids` but don't find a column
/// in the result will receive `Value::Null` (e.g. via `project_row`).
///
/// This is the correct read path for any code that eventually projects
/// property values into query results.  Use `get_node_raw` only for
/// tombstone checks (col 0 == u64::MAX) where the raw sentinel is meaningful.
fn read_node_props(
    store: &NodeStore,
    node_id: NodeId,
    col_ids: &[u32],
) -> sparrowdb_common::Result<Vec<(u32, u64)>> {
    if col_ids.is_empty() {
        return Ok(vec![]);
    }
    let nullable = store.get_node_raw_nullable(node_id, col_ids)?;
    Ok(nullable
        .into_iter()
        .filter_map(|(col_id, opt): (u32, Option<u64>)| opt.map(|v| (col_id, v)))
        .collect())
}

/// Decode a raw `u64` column value (as returned by `get_node_raw`) into the
/// execution-layer `Value` type.
///
/// Uses `NodeStore::decode_raw_value` to honour the type tag embedded in the
/// top byte (SPA-169/SPA-212), reading from the overflow string heap when
/// necessary, then maps `StoreValue::Bytes` → `Value::String`.
fn decode_raw_val(raw: u64, store: &NodeStore) -> Value {
    match store.decode_raw_value(raw) {
        StoreValue::Int64(n) => Value::Int64(n),
        StoreValue::Bytes(b) => Value::String(String::from_utf8_lossy(&b).into_owned()),
        StoreValue::Float(f) => Value::Float64(f),
    }
}

fn build_row_vals(
    props: &[(u32, u64)],
    var_name: &str,
    _col_ids: &[u32],
    store: &NodeStore,
) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for &(col_id, raw) in props {
        let key = format!("{var_name}.col_{col_id}");
        map.insert(key, decode_raw_val(raw, store));
    }
    map
}

// ── Reserved label/type protection (SPA-208) ──────────────────────────────────

/// Returns `true` if `label` starts with the reserved `__SO_` prefix.
///
/// The `__SO_` namespace is reserved for internal SparrowDB system objects.
#[inline]
fn is_reserved_label(label: &str) -> bool {
    label.starts_with("__SO_")
}

/// Compare two `Value`s for equality, handling the mixed `Int64`/`String` case.
///
/// Properties are stored as raw `u64` and read back as `Value::Int64` by
/// `build_row_vals`, while a WHERE string literal evaluates to `Value::String`.
/// When one side is `Int64` and the other is `String`, encode the string using
/// the same inline-bytes encoding the storage layer uses and compare numerically
/// (SPA-161).
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // Normal same-type comparisons.
        (Value::Int64(x), Value::Int64(y)) => x == y,
        // SPA-212: overflow string storage ensures values are never truncated,
        // so a plain equality check is now correct and sufficient.  The former
        // 7-byte inline-encoding fallback (SPA-169) has been removed because it
        // caused two distinct strings sharing the same 7-byte prefix to compare
        // equal (e.g. "TypeScript" == "TypeScripx").
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Float64(x), Value::Float64(y)) => x == y,
        // Mixed: stored raw-int vs string literal — kept for backwards
        // compatibility; should not be triggered after SPA-169 since string
        // props are now decoded to Value::String by decode_raw_val.
        (Value::Int64(raw), Value::String(s)) => *raw as u64 == string_to_raw_u64(s),
        (Value::String(s), Value::Int64(raw)) => string_to_raw_u64(s) == *raw as u64,
        // Null is only equal to null.
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

fn eval_where(expr: &Expr, vals: &HashMap<String, Value>) -> bool {
    match expr {
        Expr::BinOp { left, op, right } => {
            let lv = eval_expr(left, vals);
            let rv = eval_expr(right, vals);
            match op {
                BinOpKind::Eq => values_equal(&lv, &rv),
                BinOpKind::Neq => !values_equal(&lv, &rv),
                BinOpKind::Contains => lv.contains(&rv),
                BinOpKind::StartsWith => {
                    matches!((&lv, &rv), (Value::String(l), Value::String(r)) if l.starts_with(r.as_str()))
                }
                BinOpKind::EndsWith => {
                    matches!((&lv, &rv), (Value::String(l), Value::String(r)) if l.ends_with(r.as_str()))
                }
                BinOpKind::Lt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a < b,
                    _ => false,
                },
                BinOpKind::Le => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a <= b,
                    _ => false,
                },
                BinOpKind::Gt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a > b,
                    _ => false,
                },
                BinOpKind::Ge => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a >= b,
                    _ => false,
                },
                _ => false,
            }
        }
        Expr::And(l, r) => eval_where(l, vals) && eval_where(r, vals),
        Expr::Or(l, r) => eval_where(l, vals) || eval_where(r, vals),
        Expr::Not(inner) => !eval_where(inner, vals),
        Expr::Literal(Literal::Bool(b)) => *b,
        Expr::Literal(_) => false,
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lv = eval_expr(expr, vals);
            let matched = list
                .iter()
                .any(|item| values_equal(&lv, &eval_expr(item, vals)));
            if *negated {
                !matched
            } else {
                matched
            }
        }
        Expr::ListPredicate { .. } => {
            // Delegate to eval_expr which handles ListPredicate and returns Value::Bool.
            match eval_expr(expr, vals) {
                Value::Bool(b) => b,
                _ => false,
            }
        }
        Expr::IsNull(inner) => matches!(eval_expr(inner, vals), Value::Null),
        Expr::IsNotNull(inner) => !matches!(eval_expr(inner, vals), Value::Null),
        // CASE WHEN — evaluate via eval_expr.
        Expr::CaseWhen { .. } => matches!(eval_expr(expr, vals), Value::Bool(true)),
        // EXISTS subquery and ShortestPath require graph access.
        // Engine::eval_where_graph handles them; standalone eval_where returns false.
        Expr::ExistsSubquery(_) | Expr::ShortestPath(_) | Expr::NotExists(_) | Expr::CountStar => {
            false
        }
        _ => false, // unsupported expression — reject row rather than silently pass
    }
}

fn eval_expr(expr: &Expr, vals: &HashMap<String, Value>) -> Value {
    match expr {
        Expr::PropAccess { var, prop } => {
            // First try the direct name key (e.g. "n.name").
            let key = format!("{var}.{prop}");
            if let Some(v) = vals.get(&key) {
                return v.clone();
            }
            // Fall back to the hashed col_id key (e.g. "n.col_12345").
            // build_row_vals stores values under this form because the storage
            // layer does not carry property names — only numeric col IDs.
            let col_id = prop_name_to_col_id(prop);
            let fallback_key = format!("{var}.col_{col_id}");
            vals.get(&fallback_key).cloned().unwrap_or(Value::Null)
        }
        Expr::Var(v) => vals.get(v.as_str()).cloned().unwrap_or(Value::Null),
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => Value::Int64(*n),
            Literal::Float(f) => Value::Float64(*f),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Param(p) => {
                // Runtime parameters are stored in `vals` with a `$` prefix key
                // (inserted by the engine before evaluation via `inject_params`).
                vals.get(&format!("${p}")).cloned().unwrap_or(Value::Null)
            }
            Literal::Null => Value::Null,
        },
        Expr::FnCall { name, args } => {
            // Special-case metadata functions that need direct row-map access.
            // type(r) and labels(n) look up pre-inserted metadata keys rather
            // than dispatching through the function library with evaluated args.
            let name_lc = name.to_lowercase();
            if name_lc == "type" {
                if let Some(Expr::Var(var_name)) = args.first() {
                    let meta_key = format!("{}.__type__", var_name);
                    return vals.get(&meta_key).cloned().unwrap_or(Value::Null);
                }
            }
            if name_lc == "labels" {
                if let Some(Expr::Var(var_name)) = args.first() {
                    let meta_key = format!("{}.__labels__", var_name);
                    return vals.get(&meta_key).cloned().unwrap_or(Value::Null);
                }
            }
            // SPA-213: id(n) must look up the NodeRef even when var n holds a Map.
            // Check __node_id__ first so it works with both NodeRef and Map values.
            if name_lc == "id" {
                if let Some(Expr::Var(var_name)) = args.first() {
                    // Prefer the explicit __node_id__ entry (present whenever eval path is used).
                    let id_key = format!("{}.__node_id__", var_name);
                    if let Some(Value::NodeRef(nid)) = vals.get(&id_key) {
                        return Value::Int64(nid.0 as i64);
                    }
                    // Fallback: var itself may be a NodeRef (old code path).
                    if let Some(Value::NodeRef(nid)) = vals.get(var_name.as_str()) {
                        return Value::Int64(nid.0 as i64);
                    }
                    return Value::Null;
                }
            }
            // Evaluate each argument recursively, then dispatch to the function library.
            let evaluated: Vec<Value> = args.iter().map(|a| eval_expr(a, vals)).collect();
            crate::functions::dispatch_function(name, evaluated).unwrap_or(Value::Null)
        }
        Expr::BinOp { left, op, right } => {
            // Evaluate binary operations for use in RETURN expressions.
            let lv = eval_expr(left, vals);
            let rv = eval_expr(right, vals);
            match op {
                BinOpKind::Eq => Value::Bool(lv == rv),
                BinOpKind::Neq => Value::Bool(lv != rv),
                BinOpKind::Lt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a < b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a < b),
                    _ => Value::Null,
                },
                BinOpKind::Le => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a <= b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a <= b),
                    _ => Value::Null,
                },
                BinOpKind::Gt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a > b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a > b),
                    _ => Value::Null,
                },
                BinOpKind::Ge => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a >= b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a >= b),
                    _ => Value::Null,
                },
                BinOpKind::Contains => match (&lv, &rv) {
                    (Value::String(l), Value::String(r)) => Value::Bool(l.contains(r.as_str())),
                    _ => Value::Null,
                },
                BinOpKind::StartsWith => match (&lv, &rv) {
                    (Value::String(l), Value::String(r)) => Value::Bool(l.starts_with(r.as_str())),
                    _ => Value::Null,
                },
                BinOpKind::EndsWith => match (&lv, &rv) {
                    (Value::String(l), Value::String(r)) => Value::Bool(l.ends_with(r.as_str())),
                    _ => Value::Null,
                },
                BinOpKind::And => match (&lv, &rv) {
                    (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a && *b),
                    _ => Value::Null,
                },
                BinOpKind::Or => match (&lv, &rv) {
                    (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a || *b),
                    _ => Value::Null,
                },
                BinOpKind::Add => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Int64(a + b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Float64(a + b),
                    (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 + b),
                    (Value::Float64(a), Value::Int64(b)) => Value::Float64(a + *b as f64),
                    (Value::String(a), Value::String(b)) => Value::String(format!("{a}{b}")),
                    _ => Value::Null,
                },
                BinOpKind::Sub => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Int64(a - b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Float64(a - b),
                    (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 - b),
                    (Value::Float64(a), Value::Int64(b)) => Value::Float64(a - *b as f64),
                    _ => Value::Null,
                },
                BinOpKind::Mul => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Int64(a * b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Float64(a * b),
                    (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 * b),
                    (Value::Float64(a), Value::Int64(b)) => Value::Float64(a * *b as f64),
                    _ => Value::Null,
                },
                BinOpKind::Div => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => {
                        if *b == 0 {
                            Value::Null
                        } else {
                            Value::Int64(a / b)
                        }
                    }
                    (Value::Float64(a), Value::Float64(b)) => Value::Float64(a / b),
                    (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 / b),
                    (Value::Float64(a), Value::Int64(b)) => Value::Float64(a / *b as f64),
                    _ => Value::Null,
                },
                BinOpKind::Mod => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => {
                        if *b == 0 {
                            Value::Null
                        } else {
                            Value::Int64(a % b)
                        }
                    }
                    _ => Value::Null,
                },
            }
        }
        Expr::Not(inner) => match eval_expr(inner, vals) {
            Value::Bool(b) => Value::Bool(!b),
            _ => Value::Null,
        },
        Expr::And(l, r) => match (eval_expr(l, vals), eval_expr(r, vals)) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
            _ => Value::Null,
        },
        Expr::Or(l, r) => match (eval_expr(l, vals), eval_expr(r, vals)) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),
            _ => Value::Null,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lv = eval_expr(expr, vals);
            let matched = list
                .iter()
                .any(|item| values_equal(&lv, &eval_expr(item, vals)));
            Value::Bool(if *negated { !matched } else { matched })
        }
        Expr::List(items) => {
            let evaluated: Vec<Value> = items.iter().map(|e| eval_expr(e, vals)).collect();
            Value::List(evaluated)
        }
        Expr::ListPredicate {
            kind,
            variable,
            list_expr,
            predicate,
        } => {
            let list_val = eval_expr(list_expr, vals);
            let items = match list_val {
                Value::List(v) => v,
                _ => return Value::Null,
            };
            let mut satisfied_count = 0usize;
            // Clone vals once and reuse the same scope map each iteration,
            // updating only the loop variable binding to avoid O(n * |scope|) clones.
            let mut scope = vals.clone();
            for item in &items {
                scope.insert(variable.clone(), item.clone());
                let result = eval_expr(predicate, &scope);
                if result == Value::Bool(true) {
                    satisfied_count += 1;
                }
            }
            let result = match kind {
                ListPredicateKind::Any => satisfied_count > 0,
                ListPredicateKind::All => satisfied_count == items.len(),
                ListPredicateKind::None => satisfied_count == 0,
                ListPredicateKind::Single => satisfied_count == 1,
            };
            Value::Bool(result)
        }
        Expr::IsNull(inner) => Value::Bool(matches!(eval_expr(inner, vals), Value::Null)),
        Expr::IsNotNull(inner) => Value::Bool(!matches!(eval_expr(inner, vals), Value::Null)),
        // CASE WHEN cond THEN val ... [ELSE val] END (SPA-138).
        Expr::CaseWhen {
            branches,
            else_expr,
        } => {
            for (cond, then_val) in branches {
                if let Value::Bool(true) = eval_expr(cond, vals) {
                    return eval_expr(then_val, vals);
                }
            }
            else_expr
                .as_ref()
                .map(|e| eval_expr(e, vals))
                .unwrap_or(Value::Null)
        }
        // Graph-dependent expressions — return Null without engine context.
        Expr::ExistsSubquery(_) | Expr::ShortestPath(_) | Expr::NotExists(_) | Expr::CountStar => {
            Value::Null
        }
    }
}

fn project_row(
    props: &[(u32, u64)],
    column_names: &[String],
    _col_ids: &[u32],
    // Variable name for the scanned node (e.g. "n"), used for labels(n) columns.
    var_name: &str,
    // Primary label for the scanned node, used for labels(n) columns.
    node_label: &str,
    store: &NodeStore,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            // Handle labels(var) column.
            if let Some(inner) = col_name
                .strip_prefix("labels(")
                .and_then(|s| s.strip_suffix(')'))
            {
                if inner == var_name && !node_label.is_empty() {
                    return Value::List(vec![Value::String(node_label.to_string())]);
                }
                return Value::Null;
            }
            let prop = col_name.split('.').next_back().unwrap_or(col_name.as_str());
            let col_id = prop_name_to_col_id(prop);
            props
                .iter()
                .find(|(c, _)| *c == col_id)
                .map(|(_, v)| decode_raw_val(*v, store))
                .unwrap_or(Value::Null)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn project_hop_row(
    src_props: &[(u32, u64)],
    dst_props: &[(u32, u64)],
    column_names: &[String],
    src_var: &str,
    _dst_var: &str,
    // Optional (rel_var, rel_type) for resolving `type(rel_var)` columns.
    rel_var_type: Option<(&str, &str)>,
    // Optional (src_var, src_label) for resolving `labels(src_var)` columns.
    src_label_meta: Option<(&str, &str)>,
    // Optional (dst_var, dst_label) for resolving `labels(dst_var)` columns.
    dst_label_meta: Option<(&str, &str)>,
    store: &NodeStore,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            // Handle metadata function calls: type(r) → "type(r)" column name.
            if let Some(inner) = col_name
                .strip_prefix("type(")
                .and_then(|s| s.strip_suffix(')'))
            {
                // inner is the variable name, e.g. "r"
                if let Some((rel_var, rel_type)) = rel_var_type {
                    if inner == rel_var {
                        return Value::String(rel_type.to_string());
                    }
                }
                return Value::Null;
            }
            // Handle labels(n) → "labels(n)" column name.
            if let Some(inner) = col_name
                .strip_prefix("labels(")
                .and_then(|s| s.strip_suffix(')'))
            {
                if let Some((meta_var, label)) = src_label_meta {
                    if inner == meta_var {
                        return Value::List(vec![Value::String(label.to_string())]);
                    }
                }
                if let Some((meta_var, label)) = dst_label_meta {
                    if inner == meta_var {
                        return Value::List(vec![Value::String(label.to_string())]);
                    }
                }
                return Value::Null;
            }
            if let Some((v, prop)) = col_name.split_once('.') {
                let col_id = prop_name_to_col_id(prop);
                let props = if v == src_var { src_props } else { dst_props };
                props
                    .iter()
                    .find(|(c, _)| *c == col_id)
                    .map(|(_, val)| decode_raw_val(*val, store))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        })
        .collect()
}

/// Project a single 2-hop result row.
///
/// For each return column of the form `var.prop`, looks up the property value
/// from `src_props` when `var == src_var`, and from `fof_props` otherwise.
/// This ensures that `RETURN a.name, c.name` correctly reads the source and
/// destination node properties independently (SPA-252).
fn project_fof_row(
    src_props: &[(u32, u64)],
    fof_props: &[(u32, u64)],
    column_names: &[String],
    src_var: &str,
    store: &NodeStore,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            if let Some((var, prop)) = col_name.split_once('.') {
                let col_id = prop_name_to_col_id(prop);
                let props = if !src_var.is_empty() && var == src_var {
                    src_props
                } else {
                    fof_props
                };
                props
                    .iter()
                    .find(|(c, _)| *c == col_id)
                    .map(|(_, v)| decode_raw_val(*v, store))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        })
        .collect()
}

fn deduplicate_rows(rows: &mut Vec<Vec<Value>>) {
    // Deduplicate using structural row equality to avoid false collisions from
    // string-key approaches (e.g. ["a|", "b"] vs ["a", "|b"] would hash equal).
    let mut unique: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        if !unique.iter().any(|existing| existing == &row) {
            unique.push(row);
        }
    }
    *rows = unique;
}

/// Maximum rows to sort in-memory before spilling to disk (SPA-100).
fn sort_spill_threshold() -> usize {
    std::env::var("SPARROWDB_SORT_SPILL_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(crate::sort_spill::DEFAULT_ROW_THRESHOLD)
}

/// Build a sort key from a single row and the ORDER BY spec.
fn make_sort_key(
    row: &[Value],
    order_by: &[(Expr, SortDir)],
    column_names: &[String],
) -> Vec<crate::sort_spill::SortKeyVal> {
    use crate::sort_spill::{OrdValue, SortKeyVal};
    order_by
        .iter()
        .map(|(expr, dir)| {
            let col_idx = match expr {
                Expr::PropAccess { var, prop } => {
                    let key = format!("{var}.{prop}");
                    column_names.iter().position(|c| c == &key)
                }
                Expr::Var(v) => column_names.iter().position(|c| c == v.as_str()),
                _ => None,
            };
            let val = col_idx
                .and_then(|i| row.get(i))
                .map(OrdValue::from_value)
                .unwrap_or(OrdValue::Null);
            match dir {
                SortDir::Asc => SortKeyVal::Asc(val),
                SortDir::Desc => SortKeyVal::Desc(std::cmp::Reverse(val)),
            }
        })
        .collect()
}

fn apply_order_by(rows: &mut Vec<Vec<Value>>, m: &MatchStatement, column_names: &[String]) {
    if m.order_by.is_empty() {
        return;
    }

    let threshold = sort_spill_threshold();

    if rows.len() <= threshold {
        rows.sort_by(|a, b| {
            for (expr, dir) in &m.order_by {
                let col_idx = match expr {
                    Expr::PropAccess { var, prop } => {
                        let key = format!("{var}.{prop}");
                        column_names.iter().position(|c| c == &key)
                    }
                    Expr::Var(v) => column_names.iter().position(|c| c == v.as_str()),
                    _ => None,
                };
                if let Some(idx) = col_idx {
                    if idx < a.len() && idx < b.len() {
                        let cmp = compare_values(&a[idx], &b[idx]);
                        let cmp = if *dir == SortDir::Desc {
                            cmp.reverse()
                        } else {
                            cmp
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
    } else {
        use crate::sort_spill::{SortableRow, SpillingSorter};
        let mut sorter: SpillingSorter<SortableRow> = SpillingSorter::new();
        for row in rows.drain(..) {
            let key = make_sort_key(&row, &m.order_by, column_names);
            if sorter.push(SortableRow { key, data: row }).is_err() {
                return;
            }
        }
        if let Ok(iter) = sorter.finish() {
            *rows = iter.map(|sr| sr.data).collect::<Vec<_>>();
        }
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Float64(x), Value::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

// ── aggregation (COUNT/SUM/AVG/MIN/MAX/collect) ───────────────────────────────

/// Returns `true` if `expr` is any aggregate call.
fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::FnCall { name, .. } => matches!(
            name.to_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect"
        ),
        // ANY/ALL/NONE/SINGLE(x IN collect(...) WHERE pred) is an aggregate.
        Expr::ListPredicate { list_expr, .. } => expr_has_collect(list_expr),
        _ => false,
    }
}

/// Returns `true` if the expression contains a `collect()` call (directly or nested).
fn expr_has_collect(expr: &Expr) -> bool {
    match expr {
        Expr::FnCall { name, .. } => name.to_lowercase() == "collect",
        Expr::ListPredicate { list_expr, .. } => expr_has_collect(list_expr),
        _ => false,
    }
}

/// Extract the `collect()` argument from an expression that contains `collect()`.
///
/// Handles two forms:
/// - Direct: `collect(expr)` → evaluates `expr` against `row_vals`
/// - Nested: `ANY(x IN collect(expr) WHERE pred)` → evaluates `expr` against `row_vals`
fn extract_collect_arg(expr: &Expr, row_vals: &HashMap<String, Value>) -> Value {
    match expr {
        Expr::FnCall { args, .. } if !args.is_empty() => eval_expr(&args[0], row_vals),
        Expr::ListPredicate { list_expr, .. } => extract_collect_arg(list_expr, row_vals),
        _ => Value::Null,
    }
}

/// Evaluate an aggregate expression given the already-accumulated list.
///
/// For a bare `collect(...)`, returns the list itself.
/// For `ANY/ALL/NONE/SINGLE(x IN collect(...) WHERE pred)`, substitutes the
/// accumulated list and evaluates the predicate.
fn evaluate_aggregate_expr(
    expr: &Expr,
    accumulated_list: &Value,
    outer_vals: &HashMap<String, Value>,
) -> Value {
    match expr {
        Expr::FnCall { name, .. } if name.to_lowercase() == "collect" => accumulated_list.clone(),
        Expr::ListPredicate {
            kind,
            variable,
            predicate,
            ..
        } => {
            let items = match accumulated_list {
                Value::List(v) => v,
                _ => return Value::Null,
            };
            let mut satisfied_count = 0usize;
            for item in items {
                let mut scope = outer_vals.clone();
                scope.insert(variable.clone(), item.clone());
                let result = eval_expr(predicate, &scope);
                if result == Value::Bool(true) {
                    satisfied_count += 1;
                }
            }
            let result = match kind {
                ListPredicateKind::Any => satisfied_count > 0,
                ListPredicateKind::All => satisfied_count == items.len(),
                ListPredicateKind::None => satisfied_count == 0,
                ListPredicateKind::Single => satisfied_count == 1,
            };
            Value::Bool(result)
        }
        _ => Value::Null,
    }
}

/// Returns `true` if any RETURN item is an aggregate expression.
fn has_aggregate_in_return(items: &[ReturnItem]) -> bool {
    items.iter().any(|item| is_aggregate_expr(&item.expr))
}

/// Returns `true` if any RETURN item requires a `NodeRef` / `EdgeRef` value to
/// be present in the row map in order to evaluate correctly.
///
/// This covers:
/// - `id(var)` — a scalar function that receives the whole node reference.
/// - Bare `var` — projecting a node variable as a property map (SPA-213).
///
/// When this returns `true`, the scan must use the eval path (which inserts
/// `Value::Map` / `Value::NodeRef` under the variable key) instead of the fast
/// `project_row` path (which only stores individual property columns).
fn needs_node_ref_in_return(items: &[ReturnItem]) -> bool {
    items.iter().any(|item| {
        matches!(&item.expr, Expr::FnCall { name, .. } if name.to_lowercase() == "id")
            || matches!(&item.expr, Expr::Var(_))
            || expr_needs_graph(&item.expr)
            || expr_needs_eval_path(&item.expr)
    })
}

/// Returns `true` when the expression contains a scalar `FnCall` that cannot
/// be resolved by the fast `project_row` column-name lookup.
///
/// `project_row` maps column names like `"n.name"` directly to stored property
/// values.  Any function call such as `coalesce(n.missing, n.name)`,
/// `toUpper(n.name)`, or `size(n.name)` produces a column name like
/// `"coalesce(n.missing, n.name)"` which has no matching stored property.
/// Those expressions must be evaluated via `eval_expr` on the full row map.
///
/// Aggregate functions (`count`, `sum`, etc.) are already handled via the
/// `use_agg` flag; we exclude them here to avoid double-counting.
fn expr_needs_eval_path(expr: &Expr) -> bool {
    match expr {
        Expr::FnCall { name, args } => {
            let name_lc = name.to_lowercase();
            // Aggregates are handled separately by use_agg.
            if matches!(
                name_lc.as_str(),
                "count" | "sum" | "avg" | "min" | "max" | "collect"
            ) {
                return false;
            }
            // Any other FnCall (coalesce, toUpper, size, labels, type, id, etc.)
            // needs the eval path.  We include id/labels/type here even though
            // they are special-cased in eval_expr, because the fast project_row
            // path cannot handle them at all.
            let _ = args; // args not needed for this check
            true
        }
        // Recurse into compound expressions that may contain FnCalls.
        Expr::BinOp { left, right, .. } => {
            expr_needs_eval_path(left) || expr_needs_eval_path(right)
        }
        Expr::And(l, r) | Expr::Or(l, r) => expr_needs_eval_path(l) || expr_needs_eval_path(r),
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_needs_eval_path(inner)
        }
        _ => false,
    }
}

/// Collect the variable names that appear as bare `Expr::Var` in a RETURN clause (SPA-213).
///
/// These variables must be projected as a `Value::Map` containing all node properties
/// rather than returning `Value::Null` or a raw `NodeRef`.
fn bare_var_names_in_return(items: &[ReturnItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| {
            if let Expr::Var(v) = &item.expr {
                Some(v.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Build a `Value::Map` from a raw property slice.
///
/// Keys are `"col_{col_id}"` strings; values are decoded via [`decode_raw_val`].
/// This is used to project a bare node variable (SPA-213).
fn build_node_map(props: &[(u32, u64)], store: &NodeStore) -> Value {
    let entries: Vec<(String, Value)> = props
        .iter()
        .map(|&(col_id, raw)| (format!("col_{col_id}"), decode_raw_val(raw, store)))
        .collect();
    Value::Map(entries)
}

/// The aggregation kind for a single RETURN item.
#[derive(Debug, Clone, PartialEq)]
enum AggKind {
    /// Non-aggregate — used as a grouping key.
    Key,
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

fn agg_kind(expr: &Expr) -> AggKind {
    match expr {
        Expr::CountStar => AggKind::CountStar,
        Expr::FnCall { name, .. } => match name.to_lowercase().as_str() {
            "count" => AggKind::Count,
            "sum" => AggKind::Sum,
            "avg" => AggKind::Avg,
            "min" => AggKind::Min,
            "max" => AggKind::Max,
            "collect" => AggKind::Collect,
            _ => AggKind::Key,
        },
        // ANY/ALL/NONE/SINGLE(x IN collect(...) WHERE pred) treated as Collect-kind aggregate.
        Expr::ListPredicate { list_expr, .. } if expr_has_collect(list_expr) => AggKind::Collect,
        _ => AggKind::Key,
    }
}

/// Aggregate a set of flat `HashMap<String, Value>` rows by evaluating RETURN
/// items that contain aggregate calls (COUNT(*), COUNT, SUM, AVG, MIN, MAX, collect).
///
/// Non-aggregate RETURN items become the group key.  Returns one output
/// `Vec<Value>` per unique key in the same column order as `return_items`.
/// Returns `true` if the expression contains a `CASE WHEN`, `shortestPath`,
/// or `EXISTS` sub-expression that requires the graph-aware eval path
/// (rather than the fast `project_row` column lookup).
fn expr_needs_graph(expr: &Expr) -> bool {
    match expr {
        Expr::ShortestPath(_) | Expr::ExistsSubquery(_) | Expr::CaseWhen { .. } => true,
        Expr::And(l, r) | Expr::Or(l, r) => expr_needs_graph(l) || expr_needs_graph(r),
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_needs_graph(inner),
        Expr::BinOp { left, right, .. } => expr_needs_graph(left) || expr_needs_graph(right),
        _ => false,
    }
}

fn aggregate_rows(rows: &[HashMap<String, Value>], return_items: &[ReturnItem]) -> Vec<Vec<Value>> {
    // Classify each return item.
    let kinds: Vec<AggKind> = return_items
        .iter()
        .map(|item| agg_kind(&item.expr))
        .collect();

    let key_indices: Vec<usize> = kinds
        .iter()
        .enumerate()
        .filter(|(_, k)| **k == AggKind::Key)
        .map(|(i, _)| i)
        .collect();

    let agg_indices: Vec<usize> = kinds
        .iter()
        .enumerate()
        .filter(|(_, k)| **k != AggKind::Key)
        .map(|(i, _)| i)
        .collect();

    // No aggregate items — fall through to plain projection.
    if agg_indices.is_empty() {
        return rows
            .iter()
            .map(|row_vals| {
                return_items
                    .iter()
                    .map(|item| eval_expr(&item.expr, row_vals))
                    .collect()
            })
            .collect();
    }

    // Build groups preserving insertion order.
    let mut group_keys: Vec<Vec<Value>> = Vec::new();
    // [group_idx][agg_col_pos] → accumulated raw values
    let mut group_accum: Vec<Vec<Vec<Value>>> = Vec::new();

    for row_vals in rows {
        let key: Vec<Value> = key_indices
            .iter()
            .map(|&i| eval_expr(&return_items[i].expr, row_vals))
            .collect();

        let group_idx = if let Some(pos) = group_keys.iter().position(|k| k == &key) {
            pos
        } else {
            group_keys.push(key);
            group_accum.push(vec![vec![]; agg_indices.len()]);
            group_keys.len() - 1
        };

        for (ai, &ri) in agg_indices.iter().enumerate() {
            match &kinds[ri] {
                AggKind::CountStar => {
                    // Sentinel: count the number of sentinels after grouping.
                    group_accum[group_idx][ai].push(Value::Int64(1));
                }
                AggKind::Count | AggKind::Sum | AggKind::Avg | AggKind::Min | AggKind::Max => {
                    let arg_val = match &return_items[ri].expr {
                        Expr::FnCall { args, .. } if !args.is_empty() => {
                            eval_expr(&args[0], row_vals)
                        }
                        _ => Value::Null,
                    };
                    // All aggregates ignore NULLs (standard Cypher semantics).
                    if !matches!(arg_val, Value::Null) {
                        group_accum[group_idx][ai].push(arg_val);
                    }
                }
                AggKind::Collect => {
                    // For collect() or ListPredicate(x IN collect(...) WHERE ...), extract the
                    // collect() argument (handles both direct and nested forms).
                    let arg_val = extract_collect_arg(&return_items[ri].expr, row_vals);
                    // Standard Cypher: collect() ignores nulls.
                    if !matches!(arg_val, Value::Null) {
                        group_accum[group_idx][ai].push(arg_val);
                    }
                }
                AggKind::Key => unreachable!(),
            }
        }
    }

    // No grouping keys and no rows → one result row of zero/empty aggregates.
    if group_keys.is_empty() && key_indices.is_empty() {
        let empty_vals: HashMap<String, Value> = HashMap::new();
        let row: Vec<Value> = return_items
            .iter()
            .zip(kinds.iter())
            .map(|(item, k)| match k {
                AggKind::CountStar | AggKind::Count | AggKind::Sum => Value::Int64(0),
                AggKind::Avg | AggKind::Min | AggKind::Max => Value::Null,
                AggKind::Collect => {
                    evaluate_aggregate_expr(&item.expr, &Value::List(vec![]), &empty_vals)
                }
                AggKind::Key => Value::Null,
            })
            .collect();
        return vec![row];
    }

    // There are grouping keys but no rows → no output rows.
    if group_keys.is_empty() {
        return vec![];
    }

    // Finalize and assemble output rows — one per group.
    let mut out: Vec<Vec<Value>> = Vec::with_capacity(group_keys.len());
    for (gi, key_vals) in group_keys.into_iter().enumerate() {
        let mut output_row: Vec<Value> = Vec::with_capacity(return_items.len());
        let mut ki = 0usize;
        let mut ai = 0usize;
        // Build outer scope from key columns for ListPredicate predicate evaluation.
        let outer_vals: HashMap<String, Value> = key_indices
            .iter()
            .enumerate()
            .map(|(pos, &i)| {
                let name = return_items[i]
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("_k{i}"));
                (name, key_vals[pos].clone())
            })
            .collect();
        for col_idx in 0..return_items.len() {
            if kinds[col_idx] == AggKind::Key {
                output_row.push(key_vals[ki].clone());
                ki += 1;
            } else {
                let accumulated = Value::List(group_accum[gi][ai].clone());
                let result = if kinds[col_idx] == AggKind::Collect {
                    evaluate_aggregate_expr(&return_items[col_idx].expr, &accumulated, &outer_vals)
                } else {
                    finalize_aggregate(&kinds[col_idx], &group_accum[gi][ai])
                };
                output_row.push(result);
                ai += 1;
            }
        }
        out.push(output_row);
    }
    out
}

/// Reduce accumulated values for a single aggregate column into a final `Value`.
fn finalize_aggregate(kind: &AggKind, vals: &[Value]) -> Value {
    match kind {
        AggKind::CountStar | AggKind::Count => Value::Int64(vals.len() as i64),
        AggKind::Sum => {
            let mut sum_i: i64 = 0;
            let mut sum_f: f64 = 0.0;
            let mut is_float = false;
            for v in vals {
                match v {
                    Value::Int64(n) => sum_i += n,
                    Value::Float64(f) => {
                        is_float = true;
                        sum_f += f;
                    }
                    _ => {}
                }
            }
            if is_float {
                Value::Float64(sum_f + sum_i as f64)
            } else {
                Value::Int64(sum_i)
            }
        }
        AggKind::Avg => {
            if vals.is_empty() {
                return Value::Null;
            }
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for v in vals {
                match v {
                    Value::Int64(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Value::Float64(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                Value::Null
            } else {
                Value::Float64(sum / count as f64)
            }
        }
        AggKind::Min => vals
            .iter()
            .fold(None::<Value>, |acc, v| match (acc, v) {
                (None, v) => Some(v.clone()),
                (Some(Value::Int64(a)), Value::Int64(b)) => Some(Value::Int64(a.min(*b))),
                (Some(Value::Float64(a)), Value::Float64(b)) => Some(Value::Float64(a.min(*b))),
                (Some(Value::String(a)), Value::String(b)) => {
                    Some(Value::String(if a <= *b { a } else { b.clone() }))
                }
                (Some(a), _) => Some(a),
            })
            .unwrap_or(Value::Null),
        AggKind::Max => vals
            .iter()
            .fold(None::<Value>, |acc, v| match (acc, v) {
                (None, v) => Some(v.clone()),
                (Some(Value::Int64(a)), Value::Int64(b)) => Some(Value::Int64(a.max(*b))),
                (Some(Value::Float64(a)), Value::Float64(b)) => Some(Value::Float64(a.max(*b))),
                (Some(Value::String(a)), Value::String(b)) => {
                    Some(Value::String(if a >= *b { a } else { b.clone() }))
                }
                (Some(a), _) => Some(a),
            })
            .unwrap_or(Value::Null),
        AggKind::Collect => Value::List(vals.to_vec()),
        AggKind::Key => Value::Null,
    }
}

// ── Storage-size helpers (SPA-171) ────────────────────────────────────────────

fn dir_size_bytes(dir: &std::path::Path) -> u64 {
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

// ── CALL helpers ─────────────────────────────────────────────────────────────

/// Evaluate an expression to a string value for use as a procedure argument.
///
/// Supports `Literal::String(s)` only for v1.  Parameter binding would require
/// a runtime `params` map that is not yet threaded through the CALL path.
fn eval_expr_to_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(Literal::String(s)) => Ok(s.clone()),
        Expr::Literal(Literal::Param(p)) => Err(sparrowdb_common::Error::InvalidArgument(format!(
            "parameter ${p} requires runtime binding; pass a literal string instead"
        ))),
        other => Err(sparrowdb_common::Error::InvalidArgument(format!(
            "procedure argument must be a string literal, got: {other:?}"
        ))),
    }
}

/// Derive a display column name from a return expression (used when no AS alias
/// is provided).
fn expr_to_col_name(expr: &Expr) -> String {
    match expr {
        Expr::PropAccess { var, prop } => format!("{var}.{prop}"),
        Expr::Var(v) => v.clone(),
        _ => "value".to_owned(),
    }
}

/// Evaluate a RETURN expression against a CALL row environment.
///
/// The environment maps YIELD column names → values (e.g. `"node"` →
/// `Value::NodeRef`).  For `PropAccess` on a NodeRef the property is looked up
/// from the node store.
fn eval_call_expr(expr: &Expr, env: &HashMap<String, Value>, store: &NodeStore) -> Value {
    match expr {
        Expr::Var(v) => env.get(v.as_str()).cloned().unwrap_or(Value::Null),
        Expr::PropAccess { var, prop } => match env.get(var.as_str()) {
            Some(Value::NodeRef(node_id)) => {
                let col_id = prop_name_to_col_id(prop);
                read_node_props(store, *node_id, &[col_id])
                    .ok()
                    .and_then(|pairs| pairs.into_iter().find(|(c, _)| *c == col_id))
                    .map(|(_, raw)| decode_raw_val(raw, store))
                    .unwrap_or(Value::Null)
            }
            Some(other) => other.clone(),
            None => Value::Null,
        },
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => Value::Int64(*n),
            Literal::Float(f) => Value::Float64(*f),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::String(s) => Value::String(s.clone()),
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}
