//! Query execution engine.
//!
//! Converts a bound Cypher AST into an operator tree and executes it,
//! returning a materialized `QueryResult`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Per-rel-table edge property cache: `rel_table_id → { (src_slot, dst_slot) → [(col_id, raw_value)] }`.
type EdgePropsCache = Arc<RwLock<HashMap<u32, HashMap<(u64, u64), Vec<(u32, u64)>>>>>;

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

// ── Delta index (SPA-283) ─────────────────────────────────────────────────────
//
// Instead of scanning the entire delta log O(n) for every neighbor lookup,
// pre-index records by `(src_label_id, src_slot)`.  Lookups become O(1)
// amortized, which turns multi-hop traversals from O(k × n) into O(k).

/// Pre-indexed delta log keyed by `(src_label_id, src_slot)`.
///
/// Each entry holds the list of `DeltaRecord`s whose source matches that key.
/// Built once per query from the flat `Vec<DeltaRecord>` and passed into
/// traversal code for O(1) neighbor lookups.
pub(crate) type DeltaIndex = HashMap<(u32, u64), Vec<DeltaRecord>>;

/// Decompose a raw `NodeId` value into `(label_id, slot)`.
///
/// The encoding is: high 32 bits = label_id, low 32 bits = slot.
#[inline]
pub(crate) fn node_id_parts(raw: u64) -> (u32, u64) {
    ((raw >> 32) as u32, raw & 0xFFFF_FFFF)
}

/// Build a [`DeltaIndex`] from a flat slice of delta records.
pub(crate) fn build_delta_index(records: &[DeltaRecord]) -> DeltaIndex {
    let mut idx: DeltaIndex = HashMap::with_capacity(records.len() / 4);
    for r in records {
        let (src_label, src_slot) = node_id_parts(r.src.0);
        idx.entry((src_label, src_slot)).or_default().push(*r);
    }
    idx
}

/// Look up delta neighbors for a given `(src_label_id, src_slot)` and return
/// their destination slots (lower 32 bits of `dst.0`).
pub(crate) fn delta_neighbors_from_index(
    index: &DeltaIndex,
    src_label_id: u32,
    src_slot: u64,
) -> Vec<u64> {
    index
        .get(&(src_label_id, src_slot))
        .map(|recs| recs.iter().map(|r| node_id_parts(r.dst.0).1).collect())
        .unwrap_or_default()
}

/// Look up delta neighbors for a given `(src_label_id, src_slot)` and return
/// `(dst_slot, dst_label_id)` pairs extracted from the full dst `NodeId`.
pub(crate) fn delta_neighbors_labeled_from_index(
    index: &DeltaIndex,
    src_label_id: u32,
    src_slot: u64,
) -> impl Iterator<Item = (u64, u32)> + '_ {
    index
        .get(&(src_label_id, src_slot))
        .into_iter()
        .flat_map(|recs| {
            recs.iter().map(|r| {
                let (dst_label, dst_slot) = node_id_parts(r.dst.0);
                (dst_slot, dst_label)
            })
        })
}

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
            let src_slot = node_id_parts(rec.src.0).1;
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
    /// Shared edge-property cache (SPA-261).
    edge_props_cache: EdgePropsCache,
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

    /// Return the cached edge-props map for `rel_table_id` (SPA-261).
    pub fn edge_props_for_rel(&self, rel_table_id: u32) -> HashMap<(u64, u64), Vec<(u32, u64)>> {
        {
            let cache = self
                .edge_props_cache
                .read()
                .expect("edge_props_cache poisoned");
            if let Some(cached) = cache.get(&rel_table_id) {
                return cached.clone();
            }
        }
        let raw: Vec<(u64, u64, u32, u64)> =
            EdgeStore::open(&self.db_root, RelTableId(rel_table_id))
                .and_then(|s| s.read_all_edge_props())
                .unwrap_or_default();
        let mut grouped: HashMap<(u64, u64), Vec<(u32, u64)>> = HashMap::new();
        for (src_s, dst_s, col_id, value) in raw {
            let entry = grouped.entry((src_s, dst_s)).or_default();
            if let Some(existing) = entry.iter_mut().find(|(c, _)| *c == col_id) {
                existing.1 = value;
            } else {
                entry.push((col_id, value));
            }
        }
        let mut cache = self
            .edge_props_cache
            .write()
            .expect("edge_props_cache poisoned");
        cache.insert(rel_table_id, grouped.clone());
        grouped
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
    /// Opt-in flag for the Phase 1 chunked vectorized pipeline (#299).
    ///
    /// When `true`, qualifying queries (currently: simple single-label scans with
    /// no hops) route through the chunked pipeline instead of the row-at-a-time
    /// engine. Defaults to `false` so all existing behaviour is unchanged.
    ///
    /// Activate with [`Engine::with_chunked_pipeline`].
    pub use_chunked_pipeline: bool,
    /// Per-query memory limit in bytes for Phase 3 BFS expansion (Phase 3+).
    ///
    /// When the accumulated frontier size exceeds this during 2-hop expansion,
    /// the engine returns `Error::QueryMemoryExceeded` instead of OOM-ing.
    /// Defaults to `usize::MAX` (unlimited).
    ///
    /// Set via `EngineBuilder::with_memory_limit`.
    pub memory_limit_bytes: usize,
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
    ///
    /// When `cached_row_counts` is `Some`, the pre-built label row-count map is
    /// used directly instead of re-reading each label's HWM from disk.  This
    /// eliminates O(n_labels) syscalls on every read query (SPA-190).
    pub fn new_with_cached_index(
        store: NodeStore,
        catalog: Catalog,
        csrs: HashMap<u32, CsrForward>,
        db_root: &Path,
        cached_index: Option<&std::sync::RwLock<PropertyIndex>>,
    ) -> Self {
        Self::new_with_all_caches(store, catalog, csrs, db_root, cached_index, None, None)
    }

    /// Like [`Engine::new_with_cached_index`] but also accepts a pre-built
    /// `label_row_counts` map to avoid the per-query HWM disk reads (SPA-190).
    pub fn new_with_all_caches(
        store: NodeStore,
        catalog: Catalog,
        csrs: HashMap<u32, CsrForward>,
        db_root: &Path,
        cached_index: Option<&std::sync::RwLock<PropertyIndex>>,
        cached_row_counts: Option<HashMap<LabelId, usize>>,
        shared_edge_props_cache: Option<EdgePropsCache>,
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
        // SPA-Q1-perf / SPA-190: use pre-built row counts when provided by the
        // caller (GraphDb passes cached values to skip per-label HWM disk reads).
        // Fall back to building from disk when no cache is available (Engine::new
        // or first call after a write invalidation).
        let label_row_counts: HashMap<LabelId, usize> = cached_row_counts.unwrap_or_else(|| {
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
        });

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
            edge_props_cache: shared_edge_props_cache
                .unwrap_or_else(|| std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()))),
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
            use_chunked_pipeline: false,
            memory_limit_bytes: usize::MAX,
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

    /// Enable the Phase 1 chunked vectorized pipeline (#299).
    ///
    /// When enabled, qualifying queries route through the pull-based chunked
    /// pipeline instead of the row-at-a-time engine.  The existing engine
    /// remains the default (`use_chunked_pipeline = false`) and all non-
    /// qualifying queries continue to use it unchanged.
    pub fn with_chunked_pipeline(mut self) -> Self {
        self.use_chunked_pipeline = true;
        self
    }

    /// Return the `chunk_capacity` field (used by Phase 2+ operators when
    /// building pipeline nodes).  Defaults to `CHUNK_CAPACITY` until
    /// `EngineBuilder::with_chunk_capacity` is wired into pipeline construction.
    pub fn chunk_capacity(&self) -> usize {
        crate::chunk::CHUNK_CAPACITY
    }

    /// Return the configured per-query memory limit in bytes.
    ///
    /// Defaults to `usize::MAX` (unlimited). Set via
    /// `EngineBuilder::with_memory_limit` to enforce a budget on Phase 3
    /// BFS expansion. When the frontier exceeds this, the engine returns
    /// `Error::QueryMemoryExceeded`.
    pub fn memory_limit_bytes(&self) -> usize {
        self.memory_limit_bytes
    }

    /// Merge the engine's lazily-populated property index into the shared cache
    /// so that future read queries can skip I/O for columns we already loaded.
    ///
    /// Uses union/merge semantics: only columns not yet present in the shared
    /// cache are added.  This prevents last-writer-wins races when multiple
    /// concurrent read queries write back to the shared cache simultaneously.
    ///
    /// Called from `GraphDb` read paths after `execute_statement`.
    /// Write lazily-loaded index columns back to the shared per-`GraphDb` cache.
    ///
    /// Only merges data back when the shared cache's `generation` matches the
    /// generation this engine's index was cloned from (SPA-242).  If a write
    /// transaction committed and cleared the shared cache while this engine was
    /// executing, the shared cache's generation will have advanced, indicating
    /// that this engine's index data is derived from a stale on-disk snapshot.
    /// In that case the write-back is skipped entirely so the next engine that
    /// runs will rebuild the index from the freshly committed column files.
    pub fn write_back_prop_index(&self, shared: &std::sync::RwLock<PropertyIndex>) {
        if let Ok(mut guard) = shared.write() {
            let engine_index = self.prop_index.borrow();
            if guard.generation == engine_index.generation {
                guard.merge_from(&engine_index);
            }
            // If generations differ a write committed since this engine was
            // created — its index is stale and must not pollute the shared cache.
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

    /// Return neighbor slots from the CSR, filtered to a specific set of
    /// relation-type IDs.  When `rel_ids` is empty, falls back to scanning
    /// all CSR tables (equivalent to [`csr_neighbors_all`]).
    ///
    /// This avoids the overhead of merging neighbors from irrelevant relation
    /// types on heterogeneous graphs where only one or a few types are needed.
    fn csr_neighbors_filtered(&self, src_slot: u64, rel_ids: &[u32]) -> Vec<u64> {
        if rel_ids.is_empty() {
            return self.csr_neighbors_all(src_slot);
        }
        let mut out: Vec<u64> = Vec::new();
        for &rid in rel_ids {
            if let Some(csr) = self.snapshot.csrs.get(&rid) {
                out.extend_from_slice(csr.neighbors(src_slot));
            }
        }
        out
    }

    /// Resolve all rel-table IDs whose type name matches `rel_type`.
    ///
    /// When `rel_type` is empty (no type constraint), returns an empty vec
    /// which signals callers to scan all types.
    fn resolve_rel_ids_for_type(&self, rel_type: &str) -> Vec<u32> {
        if rel_type.is_empty() {
            return vec![];
        }
        self.snapshot
            .catalog
            .list_rel_tables_with_ids()
            .into_iter()
            .filter(|(_, _, _, rt)| rt == rel_type)
            .map(|(id, _, _, _)| id as u32)
            .collect()
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
            Statement::CallSubquery {
                subquery,
                imports,
                return_clause,
                return_order_by,
                return_skip,
                return_limit,
                return_distinct,
            } => self.execute_call_subquery(
                &subquery,
                &imports,
                return_clause.as_ref(),
                &return_order_by,
                return_skip,
                return_limit,
                return_distinct,
            ),
        }
    }

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
            // Recursively check CALL { } subqueries so that a mutation inside
            // the braces is routed through the write-transaction path rather than
            // being silently dispatched via the read path and failing late.
            Statement::CallSubquery { subquery, .. } => Self::is_mutation(subquery),
            _ => false,
        }
    }
}

// ── EngineBuilder (Phase 2 configurability, SPA-299 §2.4) ─────────────────────

/// Builder for [`Engine`] with Phase 2+ configuration options.
///
/// `with_chunk_capacity` and `with_memory_limit` are no-ops until the relevant
/// pipeline operators use them — they are wired here so callers can set them now
/// and Phase 3/4 work can plug in without API changes.
pub struct EngineBuilder {
    store: NodeStore,
    catalog: Catalog,
    csrs: HashMap<u32, CsrForward>,
    db_root: std::path::PathBuf,
    chunked_pipeline: bool,
    /// Reserved for Phase 4: overrides `CHUNK_CAPACITY` at runtime.
    #[allow(dead_code)]
    chunk_capacity: usize,
    /// Per-query memory limit in bytes for Phase 3 BFS expansion.
    memory_limit: usize,
}

impl EngineBuilder {
    /// Start building an engine from storage primitives.
    pub fn new(
        store: NodeStore,
        catalog: Catalog,
        csrs: HashMap<u32, CsrForward>,
        db_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        EngineBuilder {
            store,
            catalog,
            csrs,
            db_root: db_root.into(),
            chunked_pipeline: false,
            chunk_capacity: crate::chunk::CHUNK_CAPACITY,
            memory_limit: usize::MAX,
        }
    }

    /// Enable the chunked vectorized pipeline (equivalent to
    /// `Engine::with_chunked_pipeline`).
    pub fn with_chunked_pipeline(mut self, enabled: bool) -> Self {
        self.chunked_pipeline = enabled;
        self
    }

    /// Override the chunk capacity used by pipeline operators (Phase 4).
    ///
    /// Currently a no-op — stored for future use when Phase 4 passes this
    /// value into `ScanByLabel` and other operators.
    pub fn with_chunk_capacity(mut self, n: usize) -> Self {
        self.chunk_capacity = n;
        self
    }

    /// Set the per-query memory limit in bytes (Phase 3).
    ///
    /// When the accumulated frontier during two-hop BFS expansion exceeds
    /// this budget, the engine returns `Error::QueryMemoryExceeded` instead
    /// of continuing and potentially running out of memory.
    ///
    /// Default: `usize::MAX` (unlimited).
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Construct the [`Engine`].
    pub fn build(self) -> Engine {
        let mut engine = Engine::new(self.store, self.catalog, self.csrs, &self.db_root);
        if self.chunked_pipeline {
            engine = engine.with_chunked_pipeline();
        }
        engine.memory_limit_bytes = self.memory_limit;
        engine
    }
}

// ── Submodules (split from the original monolithic engine.rs) ─────────────────
mod aggregate;
mod expr;
mod hop;
mod mutation;
mod path;
pub mod pipeline_exec;
mod procedure;
mod scan;
mod subquery;

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

/// Collect the set of column IDs referenced by `var` in a `ReturnItem` slice.
///
/// Unlike `collect_col_ids_for_var` (which operates on column name strings and
/// breaks when AS aliases are present), this function inspects `item.expr`
/// directly so aliased items like `RETURN n.name AS from` still contribute the
/// correct column ID for `n`.
///
/// This is the preferred call site for all hop/traversal projection paths (#369).
fn collect_col_ids_for_var_from_items(var: &str, items: &[ReturnItem]) -> Vec<u32> {
    let mut id_set = std::collections::HashSet::new();
    for item in items {
        // FnCall expressions (type, labels, id, etc.) don't reference stored
        // node property columns; they are handled in project_hop_row at
        // projection time.
        if let Expr::PropAccess { var: v, prop } = &item.expr {
            if v.as_str() == var {
                id_set.insert(prop_name_to_col_id(prop));
            }
        }
    }
    // Also include WHERE-clause referenced columns — callers extend the list
    // from the WHERE expression separately, so we don't duplicate that here.
    if id_set.is_empty() {
        // Default: read col_0 so tombstone / existence checks work.
        return vec![0];
    }
    id_set.into_iter().collect()
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
        // SPA-264: Bool↔Int64 coercion — booleans are stored as Int64(0/1)
        // because the storage layer has no Bool type tag.  When a WHERE clause
        // compares a stored Int64 against a boolean literal (or vice versa),
        // coerce so that true == 1 and false == 0.
        (Value::Bool(b), Value::Int64(n)) | (Value::Int64(n), Value::Bool(b)) => {
            *n == if *b { 1 } else { 0 }
        }
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

/// Compare an i64 and an f64 numerically, returning `None` when the i64 cannot
/// be represented exactly as f64 (i.e. `|i| > 2^53`).  Callers that receive
/// `None` should treat the values as incomparable.
fn cmp_i64_f64(i: i64, f: f64) -> Option<std::cmp::Ordering> {
    const MAX_EXACT: i64 = 1_i64 << 53;
    if i.unsigned_abs() > MAX_EXACT as u64 {
        return None; // precision loss: cannot compare faithfully
    }
    (i as f64).partial_cmp(&f)
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
                    (Value::Float64(a), Value::Float64(b)) => a < b,
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).is_some_and(|o| o.is_lt())
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).is_some_and(|o| o.is_gt())
                    }
                    _ => false,
                },
                BinOpKind::Le => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a <= b,
                    (Value::Float64(a), Value::Float64(b)) => a <= b,
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).is_some_and(|o| o.is_le())
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).is_some_and(|o| o.is_ge())
                    }
                    _ => false,
                },
                BinOpKind::Gt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a > b,
                    (Value::Float64(a), Value::Float64(b)) => a > b,
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).is_some_and(|o| o.is_gt())
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).is_some_and(|o| o.is_lt())
                    }
                    _ => false,
                },
                BinOpKind::Ge => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a >= b,
                    (Value::Float64(a), Value::Float64(b)) => a >= b,
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).is_some_and(|o| o.is_ge())
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).is_some_and(|o| o.is_le())
                    }
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
                // SPA-264: use values_equal for cross-type Bool↔Int64 coercion.
                BinOpKind::Eq => Value::Bool(values_equal(&lv, &rv)),
                BinOpKind::Neq => Value::Bool(!values_equal(&lv, &rv)),
                BinOpKind::Lt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a < b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a < b),
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_lt()))
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_gt()))
                    }
                    _ => Value::Null,
                },
                BinOpKind::Le => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a <= b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a <= b),
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_le()))
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_ge()))
                    }
                    _ => Value::Null,
                },
                BinOpKind::Gt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a > b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a > b),
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_gt()))
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_lt()))
                    }
                    _ => Value::Null,
                },
                BinOpKind::Ge => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => Value::Bool(a >= b),
                    (Value::Float64(a), Value::Float64(b)) => Value::Bool(a >= b),
                    (Value::Int64(a), Value::Float64(b)) => {
                        cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_ge()))
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_le()))
                    }
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
    // NodeId of the scanned node, used for id(var) columns.
    node_id: Option<NodeId>,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            // Handle id(var) column — returns the node's integer id.
            if let Some(inner) = col_name
                .strip_prefix("id(")
                .and_then(|s| s.strip_suffix(')'))
            {
                if inner == var_name {
                    if let Some(nid) = node_id {
                        return Value::Int64(nid.0 as i64);
                    }
                }
                return Value::Null;
            }
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
    return_items: &[ReturnItem],
    src_var: &str,
    _dst_var: &str,
    // Optional (rel_var, rel_type) for resolving `type(rel_var)` columns.
    rel_var_type: Option<(&str, &str)>,
    // Optional (src_var, src_label) for resolving `labels(src_var)` columns.
    src_label_meta: Option<(&str, &str)>,
    // Optional (dst_var, dst_label) for resolving `labels(dst_var)` columns.
    dst_label_meta: Option<(&str, &str)>,
    store: &NodeStore,
    // Edge properties for the matched relationship variable (SPA-178).
    // Keyed by rel_var name; the slice contains (col_id, raw_u64) pairs.
    edge_props: Option<(&str, &[(u32, u64)])>,
) -> Vec<Value> {
    return_items
        .iter()
        .map(|item| {
            // Dispatch on the AST expression, not on the (possibly aliased) column name.
            // This fixes #369: `RETURN n.name AS from` must resolve `n.name`, not `"from"`.
            match &item.expr {
                Expr::FnCall { name, args } => {
                    let name_lc = name.to_lowercase();
                    let arg_var = args.first().and_then(|a| {
                        if let Expr::Var(v) = a {
                            Some(v.as_str())
                        } else {
                            None
                        }
                    });
                    match name_lc.as_str() {
                        "type" => {
                            if let (Some(var), Some((rel_var, rel_type))) = (arg_var, rel_var_type)
                            {
                                if var == rel_var {
                                    return Value::String(rel_type.to_string());
                                }
                            }
                            Value::Null
                        }
                        "labels" => {
                            if let Some(var) = arg_var {
                                if let Some((meta_var, label)) = src_label_meta {
                                    if var == meta_var {
                                        return Value::List(vec![Value::String(label.to_string())]);
                                    }
                                }
                                if let Some((meta_var, label)) = dst_label_meta {
                                    if var == meta_var {
                                        return Value::List(vec![Value::String(label.to_string())]);
                                    }
                                }
                            }
                            Value::Null
                        }
                        _ => Value::Null,
                    }
                }
                Expr::PropAccess { var, prop } => {
                    let col_id = prop_name_to_col_id(prop);
                    // Check if this is a relationship variable property access (SPA-178).
                    if let Some((evar, eprops)) = edge_props {
                        if var.as_str() == evar {
                            return eprops
                                .iter()
                                .find(|(c, _)| *c == col_id)
                                .map(|(_, val)| decode_raw_val(*val, store))
                                .unwrap_or(Value::Null);
                        }
                    }
                    let props = if var.as_str() == src_var {
                        src_props
                    } else {
                        dst_props
                    };
                    props
                        .iter()
                        .find(|(c, _)| *c == col_id)
                        .map(|(_, val)| decode_raw_val(*val, store))
                        .unwrap_or(Value::Null)
                }
                _ => Value::Null,
            }
        })
        .collect()
}

/// Project a single 2-hop result row (src + fof only, no mid).
///
/// For each return column of the form `var.prop`, looks up the property value
/// from `src_props` when `var == src_var`, and from `fof_props` otherwise.
/// This ensures that `RETURN a.name, c.name` correctly reads the source and
/// destination node properties independently (SPA-252).
///
/// NOTE: SPA-241 replaced calls to this function in the forward-forward two-hop
/// path with `project_three_var_row`, which also handles the mid variable.
/// Retained for potential future use in simplified single-level projections.
#[allow(dead_code)]
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

/// SPA-201: Three-variable row projection for the incoming second-hop pattern
/// `(a)-[:R]->(m)<-[:R]-(b) RETURN m.name`.
///
/// Resolves column references to src (a), mid (m), or fof (b) props based on
/// variable name matching.  Any unrecognised variable falls back to fof_props.
///
/// Accepts `ReturnItem` slices so that AS aliases are handled correctly (#369):
/// the expression (`item.expr`) is used for value resolution, not the column
/// name string which reflects the alias.
fn project_three_var_row(
    src_props: &[(u32, u64)],
    mid_props: &[(u32, u64)],
    fof_props: &[(u32, u64)],
    return_items: &[ReturnItem],
    src_var: &str,
    mid_var: &str,
    store: &NodeStore,
) -> Vec<Value> {
    return_items
        .iter()
        .map(|item| {
            if let Expr::PropAccess { var, prop } = &item.expr {
                let col_id = prop_name_to_col_id(prop);
                let props: &[(u32, u64)] = if !src_var.is_empty() && var.as_str() == src_var {
                    src_props
                } else if !mid_var.is_empty() && var.as_str() == mid_var {
                    mid_props
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
    // O(N) deduplication: encode each row via bincode (type-tagged, deterministic)
    // and use a HashSet of byte keys.  The original O(N²) `any()` scan became a
    // bottleneck for DISTINCT queries on large result sets (SPA-299 Q4).
    //
    // Bincode avoids the false-collision risk that string concatenation carries
    // (e.g. ["a|", "b"] vs ["a", "|b"] would hash equal as strings).
    //
    // NaN preservation: bincode serializes Float64 bit-for-bit, which would
    // collapse multiple NaN rows into one (all NaN bit-patterns serialize
    // identically).  The original O(N²) path used PartialEq, where NaN != NaN
    // (IEEE 754), so every NaN row was always kept.  We preserve that semantic
    // here by bypassing the HashSet for any row that contains a NaN value.
    use std::collections::HashSet;
    let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(rows.len());
    rows.retain(|row| {
        let has_nan = row
            .iter()
            .any(|v| matches!(v, Value::Float64(f) if f.is_nan()));
        if has_nan {
            return true;
        }
        let key = bincode::serialize(row).expect("Value must be bincode-serializable");
        seen.insert(key)
    });
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

/// Apply DISTINCT, ORDER BY, SKIP, and LIMIT to a result row set.
///
/// This is the single authoritative post-processing step shared by every scan
/// path (single-label, multi-label, label-less, and multi-pattern).  It must
/// be called **after** aggregation / eval-path projection so that ORDER BY and
/// LIMIT see the final projected values.
fn apply_post_processing(rows: &mut Vec<Vec<Value>>, m: &MatchStatement, column_names: &[String]) {
    if m.distinct {
        deduplicate_rows(rows);
    }
    apply_order_by(rows, m, column_names);
    if let Some(skip) = m.skip {
        let skip = (skip as usize).min(rows.len());
        rows.drain(0..skip);
    }
    if let Some(lim) = m.limit {
        rows.truncate(lim as usize);
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
pub(crate) fn has_aggregate_in_return(items: &[ReturnItem]) -> bool {
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
