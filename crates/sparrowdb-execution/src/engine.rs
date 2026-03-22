//! Query execution engine.
//!
//! Converts a bound Cypher AST into an operator tree and executes it,
//! returning a materialized `QueryResult`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tracing::info_span;

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::{col_id_of, NodeId, Result};
use sparrowdb_cypher::ast::{
    BinOpKind, CallStatement, CreateStatement, Expr, ListPredicateKind, Literal,
    MatchCreateStatement, MatchMutateStatement, MatchOptionalMatchStatement, MatchStatement,
    MatchWithStatement, Mutation, OptionalMatchStatement, PathPattern, ReturnItem, SortDir,
    Statement, UnionStatement, UnwindStatement, WithClause,
};
use sparrowdb_cypher::{bind, parse};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{DeltaRecord, EdgeStore, RelTableId};
use sparrowdb_storage::fulltext_index::FulltextIndex;
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};

use crate::types::{QueryResult, Value};

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

/// The execution engine holds references to the storage layer.
pub struct Engine {
    pub store: NodeStore,
    pub catalog: Catalog,
    /// Per-relationship-type CSR forward files, keyed by `RelTableId` (u32).
    /// Replaces the old single `csr: CsrForward` field so that different
    /// relationship types use separate edge tables (SPA-185).
    pub csrs: HashMap<u32, CsrForward>,
    pub db_root: std::path::PathBuf,
    /// Runtime query parameters supplied by the caller (e.g. `$name` → Value).
    pub params: HashMap<String, Value>,
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
        Engine {
            store,
            catalog,
            csrs,
            db_root: db_root.to_path_buf(),
            params: HashMap::new(),
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
        EdgeStore::open(&self.db_root, RelTableId(rel_table_id))
            .and_then(|s| s.read_delta())
            .unwrap_or_default()
    }

    /// Read delta records across **all** registered rel types.
    ///
    /// Used by code paths that traverse edges without a type filter.
    fn read_delta_all(&self) -> Vec<sparrowdb_storage::edge_store::DeltaRecord> {
        let ids = self.catalog.list_rel_table_ids();
        if ids.is_empty() {
            // No rel types in catalog yet; fall back to table-id 0 (legacy).
            return EdgeStore::open(&self.db_root, RelTableId(0))
                .and_then(|s| s.read_delta())
                .unwrap_or_default();
        }
        ids.into_iter()
            .flat_map(|(id, _, _, _)| {
                EdgeStore::open(&self.db_root, RelTableId(id as u32))
                    .and_then(|s| s.read_delta())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Return neighbor slots from the CSR for a given src slot and rel table.
    fn csr_neighbors(&self, rel_table_id: u32, src_slot: u64) -> Vec<u64> {
        self.csrs
            .get(&rel_table_id)
            .map(|csr| csr.neighbors(src_slot).to_vec())
            .unwrap_or_default()
    }

    /// Return neighbor slots merged across **all** registered rel types.
    fn csr_neighbors_all(&self, src_slot: u64) -> Vec<u64> {
        let mut out: Vec<u64> = Vec::new();
        for csr in self.csrs.values() {
            out.extend_from_slice(csr.neighbors(src_slot));
        }
        out
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
            bind(stmt, &self.catalog)?
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
            Statement::Merge(_) | Statement::MatchMutate(_) | Statement::MatchCreate(_) => {
                Err(sparrowdb_common::Error::InvalidArgument(
                    "mutation statements must be executed via execute_mutation".into(),
                ))
            }
            Statement::OptionalMatch(om) => self.execute_optional_match(&om),
            Statement::MatchOptionalMatch(mom) => self.execute_match_optional_match(&mom),
            Statement::Union(u) => self.execute_union(u),
            Statement::Checkpoint | Statement::Optimize => Ok(QueryResult::empty(vec![])),
            Statement::Call(c) => self.execute_call(&c),
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
        let index = FulltextIndex::open(&self.db_root, &index_name)?;
        let node_ids = index.search(&query);

        // Determine which column names to project.
        // Default to ["node"] when no YIELD clause was specified.
        let yield_cols: Vec<String> = if c.yield_columns.is_empty() {
            vec!["node".to_owned()]
        } else {
            c.yield_columns.clone()
        };

        // Validate YIELD columns — only "node" is defined for this procedure.
        if let Some(bad_col) = yield_cols.iter().find(|c| c.as_str() != "node") {
            return Err(sparrowdb_common::Error::InvalidArgument(format!(
                "unsupported YIELD column for db.index.fulltext.queryNodes: {bad_col}"
            )));
        }

        // Build result rows: one per matching node.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for raw_id in node_ids {
            let node_id = sparrowdb_common::NodeId(raw_id);
            let row: Vec<Value> = yield_cols.iter().map(|_| Value::NodeRef(node_id)).collect();
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
                .map(|item| eval_call_expr(&item.expr, &env, &self.store))
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
            Statement::Merge(_) | Statement::MatchMutate(_) | Statement::MatchCreate(_) => true,
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

        let label_id = match self.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "unknown label: {label}"
                )))
            }
        };

        let hwm = self.store.hwm_for_label(label_id)?;

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
            let props = read_node_props(&self.store, node_id, &all_col_ids)?;

            if !matches_prop_filter_static(&props, &node_pat.props) {
                continue;
            }

            if let Some(ref where_expr) = mm.where_clause {
                let row_vals = build_row_vals(&props, var_name, &all_col_ids);
                if !eval_where(where_expr, &row_vals) {
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
    /// A read error is treated as "not tombstoned" so that missing-file errors
    /// during a fresh scan do not suppress valid nodes.
    fn is_node_tombstoned(&self, node_id: NodeId) -> bool {
        match self.store.get_node_raw(node_id, &[0u32]) {
            Ok(col0) => col0.iter().any(|&(c, v)| c == 0 && v == u64::MAX),
            Err(_) => false,
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
        match self.store.get_node_raw(node_id, filter_col_ids) {
            Ok(raw_props) => matches_prop_filter_static(&raw_props, props),
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
                let label_id: u32 = match self.catalog.get_label(&label)? {
                    Some(id) => id as u32,
                    None => {
                        // Label not found → no matching nodes for this variable.
                        var_candidates.insert(node_pat.var.clone(), vec![]);
                        continue;
                    }
                };

                let hwm = self.store.hwm_for_label(label_id)?;

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
                    match self.store.get_node_raw(node_id, &[0u32]) {
                        Ok(col0) if col0.iter().any(|&(c, v)| c == 0 && v == u64::MAX) => {
                            continue;
                        }
                        Ok(_) | Err(_) => {}
                    }

                    // Apply inline prop filter if any.
                    if !node_pat.props.is_empty() {
                        match self.store.get_node_raw(node_id, &filter_col_ids) {
                            Ok(props) => {
                                if !matches_prop_filter_static(&props, &node_pat.props) {
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

                    let label = node_pat.labels.first().cloned().unwrap_or_default();
                    let label_id: u32 = match self.catalog.get_label(&label)? {
                        Some(id) => id as u32,
                        None => {
                            // No nodes can match → entire MATCH yields nothing.
                            return Ok(vec![]);
                        }
                    };

                    let hwm = self.store.hwm_for_label(label_id)?;
                    let filter_col_ids: Vec<u32> = node_pat
                        .props
                        .iter()
                        .map(|p| prop_name_to_col_id(&p.key))
                        .collect();

                    let mut matching_ids: Vec<NodeId> = Vec::new();
                    for slot in 0..hwm {
                        let node_id = NodeId(((label_id as u64) << 32) | slot);

                        if self.is_node_tombstoned(node_id) {
                            continue;
                        }
                        if !self.node_matches_prop_filter(node_id, &filter_col_ids, &node_pat.props)
                        {
                            continue;
                        }

                        matching_ids.push(node_id);
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

                let src_label_id: u32 = match self.catalog.get_label(&src_label)? {
                    Some(id) => id as u32,
                    None => return Ok(vec![]),
                };
                let dst_label_id: u32 = match self.catalog.get_label(&dst_label)? {
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

                let hwm_src = self.store.hwm_for_label(src_label_id)?;

                // Pairs yielded by this pattern for cross-join below.
                let mut pattern_rows: Vec<HashMap<String, NodeId>> = Vec::new();

                for src_slot in 0..hwm_src {
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
            let label_id: u32 = match self.catalog.get_label(&label)? {
                Some(id) => id as u32,
                None => self.catalog.create_label(&label)? as u32,
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

            self.store.create_node(label_id, &props)?;
        }
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

        // Step 2 & 3: project through WITH + filter.
        let mut projected: Vec<HashMap<String, Value>> = Vec::new();
        for row_vals in &intermediate {
            let mut with_vals: HashMap<String, Value> = HashMap::new();
            for item in &m.with_clause.items {
                let val = eval_expr(&item.expr, row_vals);
                with_vals.insert(item.alias.clone(), val);
            }
            if let Some(ref where_expr) = m.with_clause.where_clause {
                if !eval_where(where_expr, &with_vals) {
                    continue;
                }
            }
            projected.push(with_vals);
        }

        // Step 4: project RETURN from the WITH-projected rows.
        let column_names = extract_return_column_names(&m.return_clause.items);
        let mut rows: Vec<Vec<Value>> = projected
            .iter()
            .map(|with_vals| {
                m.return_clause
                    .items
                    .iter()
                    .map(|item| eval_expr(&item.expr, with_vals))
                    .collect()
            })
            .collect();

        if m.distinct {
            deduplicate_rows(&mut rows);
        }
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(rows.len());
            rows.drain(0..skip);
        }
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
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
                .catalog
                .get_label(&label)?
                .ok_or(sparrowdb_common::Error::NotFound)?;
            let label_id_u32 = label_id as u32;
            let hwm = self.store.hwm_for_label(label_id_u32)?;

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
                let col0_check = self.store.get_node_raw(node_id, &[0u32])?;
                if col0_check.iter().any(|&(c, v)| c == 0 && v == u64::MAX) {
                    continue;
                }
                let props = read_node_props(&self.store, node_id, &all_col_ids)?;
                if !self.matches_prop_filter(&props, &node.props) {
                    continue;
                }
                let row_vals = build_row_vals(&props, var_name, &all_col_ids);
                if let Some(wexpr) = &where_clause {
                    if !eval_where(wexpr, &row_vals) {
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
        // Detect variable-length path: single pattern with exactly 1 rel that has min_hops set.
        let is_var_len = m.pattern.len() == 1
            && m.pattern[0].rels.len() == 1
            && m.pattern[0].rels[0].min_hops.is_some();

        let column_names = extract_return_column_names(&m.return_clause.items);

        if is_var_len {
            self.execute_variable_length(m, &column_names)
        } else if is_two_hop {
            self.execute_two_hop(m, &column_names)
        } else if is_one_hop {
            self.execute_one_hop(m, &column_names)
        } else if m.pattern[0].rels.is_empty() {
            self.execute_scan(m, &column_names)
        } else {
            // Multi-pattern or complex query — fallback to sequential execution.
            self.execute_scan(m, &column_names)
        }
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
        let lead_label_id = match self.catalog.get_label(&lead_label)? {
            Some(id) => id as u32,
            None => {
                let null_row = vec![Value::Null; column_names.len()];
                return Ok(QueryResult {
                    columns: column_names,
                    rows: vec![null_row],
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

        let lead_hwm = self.store.hwm_for_label(lead_label_id)?;
        let lead_var = lead_node_pat.var.as_str();

        // Collect lead rows as (slot, props) pairs.
        let mut lead_rows: Vec<(u64, Vec<(u32, u64)>)> = Vec::new();
        for slot in 0..lead_hwm {
            let node_id = NodeId(((lead_label_id as u64) << 32) | slot);
            let col0_check = self.store.get_node_raw(node_id, &[0u32])?;
            if col0_check.iter().any(|&(c, v)| c == 0 && v == u64::MAX) {
                continue;
            }
            let props = read_node_props(&self.store, node_id, &lead_all_col_ids)?;
            if !self.matches_prop_filter(&props, &lead_node_pat.props) {
                continue;
            }
            if let Some(ref wexpr) = mom.match_where {
                let row_vals = build_row_vals(&props, lead_var, &lead_all_col_ids);
                if !eval_where(wexpr, &row_vals) {
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
            let lead_row_vals = build_row_vals(lead_props, lead_var, &lead_all_col_ids);

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
                let opt_dst_label_id: Option<u32> = match self.catalog.get_label(&opt_dst_label) {
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
            let dst_props = read_node_props(&self.store, dst_node, &col_ids_dst)?;
            if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                continue;
            }
            let row_vals = build_row_vals(&dst_props, dst_var, &col_ids_dst);
            sub_rows.push(row_vals);
        }

        Ok(sub_rows)
    }

    // ── Node-only scan (no relationships) ─────────────────────────────────────

    fn execute_scan(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let node = &pat.nodes[0];

        // SPA-192/SPA-194: when no label is specified, scan ALL known labels and union
        // the results.  Delegate to the per-label helper for each label.
        if node.labels.is_empty() {
            return self.execute_scan_all_labels(m, column_names);
        }

        let label = node.labels.first().cloned().unwrap_or_default();
        let label_id = self
            .catalog
            .get_label(&label)?
            .ok_or(sparrowdb_common::Error::NotFound)?;
        let label_id_u32 = label_id as u32;

        let hwm = self.store.hwm_for_label(label_id_u32)?;
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
        let mut raw_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for slot in 0..hwm {
            let node_id = NodeId(((label_id_u32 as u64) << 32) | slot);
            if slot < 1024 || slot % 10_000 == 0 {
                tracing::trace!(slot = slot, node_id = node_id.0, "scan emit");
            }

            // SPA-164: skip tombstoned nodes.  delete_node writes u64::MAX into
            // col_0 as the deletion sentinel; nodes in that state must not
            // appear in scan results.
            let col0_check = self.store.get_node_raw(node_id, &[0u32])?;
            if col0_check.iter().any(|&(c, v)| c == 0 && v == u64::MAX) {
                continue;
            }

            // Use nullable reads so that absent columns (property never written
            // for this node) are omitted from the row map rather than surfacing
            // as Err(NotFound).  Absent columns will evaluate to Value::Null in
            // eval_expr, enabling correct IS NULL / IS NOT NULL semantics.
            let nullable_props = self.store.get_node_raw_nullable(node_id, &all_col_ids)?;
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
                let mut row_vals = build_row_vals(&props, var_name, &all_col_ids);
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
                if !eval_where(where_expr, &row_vals) {
                    continue;
                }
            }

            if use_eval_path {
                // Build eval_expr-compatible map for aggregation / id() path.
                let mut row_vals = build_row_vals(&props, var_name, &all_col_ids);
                // Inject label metadata for aggregation.
                if !var_name.is_empty() && !label.is_empty() {
                    row_vals.insert(
                        format!("{}.__labels__", var_name),
                        Value::List(vec![Value::String(label.clone())]),
                    );
                }
                if !var_name.is_empty() {
                    row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                }
                raw_rows.push(row_vals);
            } else {
                // Project RETURN columns directly (fast path).
                let row = project_row(&props, column_names, &all_col_ids, var_name, &label);
                rows.push(row);
            }
        }

        if use_eval_path {
            rows = aggregate_rows(&raw_rows, &m.return_clause.items);
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
        let all_labels = self.catalog.list_labels()?;
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
        if use_agg {
            for item in &m.return_clause.items {
                collect_col_ids_from_expr(&item.expr, &mut all_col_ids);
            }
        }

        let mut raw_rows: Vec<HashMap<String, Value>> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for (label_id, label_name) in &all_labels {
            let label_id_u32 = *label_id as u32;
            let hwm = self.store.hwm_for_label(label_id_u32)?;
            tracing::debug!(label = %label_name, hwm = hwm, "label-less scan: label slot");

            for slot in 0..hwm {
                let node_id = NodeId(((label_id_u32 as u64) << 32) | slot);

                // Skip tombstoned nodes (SPA-164).
                let col0_check = self.store.get_node_raw(node_id, &[0u32])?;
                if col0_check.iter().any(|&(c, v)| c == 0 && v == u64::MAX) {
                    continue;
                }

                let nullable_props = self.store.get_node_raw_nullable(node_id, &all_col_ids)?;
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
                    let mut row_vals = build_row_vals(&props, var_name, &all_col_ids);
                    if !var_name.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", var_name),
                            Value::List(vec![Value::String(label_name.clone())]),
                        );
                    }
                    if !eval_where(where_expr, &row_vals) {
                        continue;
                    }
                }

                if use_agg {
                    let mut row_vals = build_row_vals(&props, var_name, &all_col_ids);
                    if !var_name.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", var_name),
                            Value::List(vec![Value::String(label_name.clone())]),
                        );
                        row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                    }
                    raw_rows.push(row_vals);
                } else {
                    let row = project_row(&props, column_names, &all_col_ids, var_name, label_name);
                    rows.push(row);
                }
            }
        }

        if use_agg {
            rows = aggregate_rows(&raw_rows, &m.return_clause.items);
        } else {
            if m.distinct {
                deduplicate_rows(&mut rows);
            }
            apply_order_by(&mut rows, m, column_names);
            if let Some(lim) = m.limit {
                rows.truncate(lim as usize);
            }
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
            self.catalog.get_label(&src_label)?.map(|id| id as u32)
        };
        let dst_label_id_opt: Option<u32> = if dst_label.is_empty() {
            None
        } else {
            self.catalog.get_label(&dst_label)?.map(|id| id as u32)
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
        let all_rel_tables = self.catalog.list_rel_tables_with_ids();
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
            self.catalog.list_labels().unwrap_or_default()
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

            let hwm_src = match self.store.hwm_for_label(effective_src_label_id) {
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
                let edge_store = EdgeStore::open(&self.db_root, storage_rel_id);
                edge_store.and_then(|s| s.read_delta()).unwrap_or_default()
            };

            // Scan source nodes for this label.
            for src_slot in 0..hwm_src {
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
                    self.store.get_node_raw(src_node, &all_needed)?
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

                // Include CSR neighbors only when scanning rel-table 0 — the CSR
                // snapshot is built exclusively for that table.  Using it for any
                // other table would mix in neighbors from the wrong edge set.
                let csr_neighbors: &[u64] = if *catalog_rel_id == 0 {
                    self.csrs
                        .get(&0)
                        .map(|c| c.neighbors(src_slot))
                        .unwrap_or(&[])
                } else {
                    &[]
                };
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
                        self.store.get_node_raw(dst_node, &all_needed)?
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
                        let mut row_vals =
                            build_row_vals(&src_props, &src_node_pat.var, &col_ids_src);
                        row_vals.extend(build_row_vals(
                            &dst_props,
                            &dst_node_pat.var,
                            &col_ids_dst,
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
                        if !eval_where(where_expr, &row_vals) {
                            continue;
                        }
                    }

                    if use_agg {
                        let mut row_vals =
                            build_row_vals(&src_props, &src_node_pat.var, &col_ids_src);
                        row_vals.extend(build_row_vals(
                            &dst_props,
                            &dst_node_pat.var,
                            &col_ids_dst,
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

                let hwm_bwd = match self.store.hwm_for_label(bwd_scan_label_id) {
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

                // Read delta records for this rel table (edges in the forward
                // direction b→a are stored as src=b, dst=a in the delta log).
                let delta_records_bwd = EdgeStore::open(&self.db_root, storage_rel_id)
                    .and_then(|s| s.read_delta())
                    .unwrap_or_default();

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
                        self.store.get_node_raw(b_node, &all_needed)?
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

                    let mut seen_preds: HashSet<u64> = HashSet::new();
                    for a_slot in delta_predecessors {
                        if !seen_preds.insert(a_slot) {
                            continue;
                        }
                        // Skip pairs already emitted in the forward pass
                        // (forward emitted (a_slot, b_slot)).
                        if seen_undirected.contains(&(a_slot, b_slot)) {
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
                            self.store.get_node_raw(a_node, &all_needed)?
                        } else {
                            vec![]
                        };

                        if !self.matches_prop_filter(&a_props, &dst_node_pat.props) {
                            continue;
                        }

                        // Apply WHERE clause.
                        if let Some(ref where_expr) = m.where_clause {
                            let mut row_vals =
                                build_row_vals(&b_props, &src_node_pat.var, &col_ids_src);
                            row_vals.extend(build_row_vals(
                                &a_props,
                                &dst_node_pat.var,
                                &col_ids_dst,
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
                            if !eval_where(where_expr, &row_vals) {
                                continue;
                            }
                        }

                        if use_agg {
                            let mut row_vals =
                                build_row_vals(&b_props, &src_node_pat.var, &col_ids_src);
                            row_vals.extend(build_row_vals(
                                &a_props,
                                &dst_node_pat.var,
                                &col_ids_dst,
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
                            );
                            rows.push(row);
                        }
                    }
                }
            }
        }

        if use_agg {
            rows = aggregate_rows(&raw_rows, &m.return_clause.items);
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
            .catalog
            .get_label(&src_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;
        let fof_label_id = self
            .catalog
            .get_label(&fof_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;

        let hwm_src = self.store.hwm_for_label(src_label_id)?;
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

        // Collect col_ids for src that are referenced by the WHERE clause, scoped to the src
        // variable so that fof-only predicates do not cause spurious column fetches from src nodes.
        let col_ids_src_where: Vec<u32> = {
            let mut ids = Vec::new();
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
            let max_nodes = self.csrs.values().map(|c| c.n_nodes()).max().unwrap_or(0);
            let mut edges: Vec<(u64, u64)> = Vec::new();
            for csr in self.csrs.values() {
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

            let src_props = read_node_props(&self.store, src_node, &src_needed)?;

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
                let fof_props = read_node_props(&self.store, fof_node, &col_ids_fof)?;

                // Apply fof inline prop filter.
                if !self.matches_prop_filter(&fof_props, &fof_node_pat.props) {
                    continue;
                }

                // Apply WHERE clause predicate.
                if let Some(ref where_expr) = m.where_clause {
                    let mut row_vals =
                        build_row_vals(&src_props, &src_node_pat.var, &col_ids_src_where);
                    row_vals.extend(build_row_vals(&fof_props, &fof_node_pat.var, &col_ids_fof));
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
                    if !eval_where(where_expr, &row_vals) {
                        continue;
                    }
                }

                let row = project_fof_row(&fof_props, column_names, &fof_node_pat.var);
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

    // ── Variable-length path traversal: (a)-[:R*M..N]->(b) ──────────────────

    /// Collect all neighbor slot-ids reachable from `src_slot` via the delta
    /// log and CSR adjacency.  src_label_id is used to filter delta records.
    ///
    /// SPA-185: reads across all rel types (used by variable-length path
    /// traversal which does not currently filter on rel_type).
    fn get_node_neighbors_by_slot(&self, src_slot: u64, src_label_id: u32) -> Vec<u64> {
        let csr_neighbors: Vec<u64> = self.csr_neighbors_all(src_slot);
        let delta_neighbors: Vec<u64> = self
            .read_delta_all()
            .into_iter()
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

    /// BFS traversal for variable-length path patterns `(src)-[:R*min..max]->(dst)`.
    ///
    /// Returns the set of destination slot-ids reachable from `src_slot` in
    /// `[min_hops, max_hops]` hops.  Max is capped at 10 to prevent runaway
    /// traversals on dense graphs.
    fn execute_variable_hops(
        &self,
        src_slot: u64,
        src_label_id: u32,
        min_hops: u32,
        max_hops: u32,
    ) -> Vec<u64> {
        const SAFETY_CAP: u32 = 10;
        let max_hops = max_hops.min(SAFETY_CAP);

        // BFS: frontier = nodes at the current depth.
        // visited = all nodes ever enqueued (for cycle-avoidance).
        // results = nodes at depth >= min_hops.
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(src_slot);
        let mut frontier: Vec<u64> = vec![src_slot];
        let mut results: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for depth in 1..=max_hops {
            let mut next_frontier: Vec<u64> = Vec::new();
            for &node_slot in &frontier {
                let neighbors = self.get_node_neighbors_by_slot(node_slot, src_label_id);
                for nb in neighbors {
                    if visited.insert(nb) {
                        next_frontier.push(nb);
                    }
                    if depth >= min_hops {
                        results.insert(nb);
                    }
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }

        results.into_iter().collect()
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
            .catalog
            .get_label(&src_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;
        let dst_label_id = self
            .catalog
            .get_label(&dst_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;

        let hwm_src = self.store.hwm_for_label(src_label_id)?;

        let col_ids_src = collect_col_ids_for_var(&src_node_pat.var, column_names, src_label_id);
        let col_ids_dst = collect_col_ids_for_var(&dst_node_pat.var, column_names, dst_label_id);

        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut seen_pairs: std::collections::HashSet<(u64, u64)> =
            std::collections::HashSet::new();

        for src_slot in 0..hwm_src {
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
                v
            };
            let src_props = read_node_props(&self.store, src_node, &src_all_col_ids)?;

            if !self.matches_prop_filter(&src_props, &src_node_pat.props) {
                continue;
            }

            // BFS to find all reachable dst slots within [min_hops, max_hops].
            let dst_slots = self.execute_variable_hops(src_slot, src_label_id, min_hops, max_hops);

            for dst_slot in dst_slots {
                if !seen_pairs.insert((src_slot, dst_slot)) {
                    continue;
                }

                let dst_node = NodeId(((dst_label_id as u64) << 32) | dst_slot);
                let dst_props = read_node_props(&self.store, dst_node, &col_ids_dst)?;

                if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                    continue;
                }

                // Apply WHERE clause.
                if let Some(ref where_expr) = m.where_clause {
                    let mut row_vals = build_row_vals(&src_props, &src_node_pat.var, &col_ids_src);
                    row_vals.extend(build_row_vals(&dst_props, &dst_node_pat.var, &col_ids_dst));
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
                    if !dst_node_pat.var.is_empty() && !dst_label.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", dst_node_pat.var),
                            Value::List(vec![Value::String(dst_label.clone())]),
                        );
                    }
                    if !eval_where(where_expr, &row_vals) {
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
                let dst_label_meta = if !dst_node_pat.var.is_empty() && !dst_label.is_empty() {
                    Some((dst_node_pat.var.as_str(), dst_label.as_str()))
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
        matches_prop_filter_static(props, filters)
    }
}

// ── Free-standing prop-filter helper (usable without &self) ───────────────────

fn matches_prop_filter_static(
    props: &[(u32, u64)],
    filters: &[sparrowdb_cypher::ast::PropEntry],
) -> bool {
    for f in filters {
        let col_id = prop_name_to_col_id(&f.key);
        let stored_val = props.iter().find(|(c, _)| *c == col_id).map(|(_, v)| *v);

        // Evaluate the filter expression (supports literals and function calls).
        let empty_filter_bindings: HashMap<String, Value> = HashMap::new();
        let filter_val = eval_expr(&f.value, &empty_filter_bindings);
        let matches = match filter_val {
            Value::Int64(n) => {
                // Int64 values are stored with TAG_INT64 (0x00) in the top byte.
                // Use StoreValue::to_u64() for canonical encoding (SPA-169).
                stored_val == Some(StoreValue::Int64(n).to_u64())
            }
            Value::String(s) => {
                // Strings are stored with TAG_BYTES (0x01) in the top byte.
                // Encode the literal the same way and compare (SPA-161, SPA-169).
                stored_val == Some(string_to_raw_u64(&s))
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
        Literal::Float(f) => StoreValue::Int64(f64::to_bits(*f) as i64),
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
        Value::Float64(f) => StoreValue::Int64(f64::to_bits(f) as i64),
        Value::Bool(b) => StoreValue::Int64(if b { 1 } else { 0 }),
        Value::String(s) => StoreValue::Bytes(s.into_bytes()),
        Value::Null => StoreValue::Int64(0),
        Value::NodeRef(id) => StoreValue::Int64(id.0 as i64),
        Value::EdgeRef(id) => StoreValue::Int64(id.0 as i64),
        Value::List(_) => StoreValue::Int64(0),
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

/// Map a property name like "col_0" or "name" to a col_id.
///
/// Uses the canonical [`sparrowdb_common::col_id_of`] FNV-1a hash so that
/// this always agrees with what the storage layer wrote to disk (SPA-160).
fn prop_name_to_col_id(name: &str) -> u32 {
    if let Some(suffix) = name.strip_prefix("col_") {
        suffix.parse().unwrap_or(0)
    } else {
        col_id_of(name)
    }
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
/// Uses `StoreValue::from_u64` to honour the type tag embedded in the top
/// byte (SPA-169), then maps `StoreValue::Bytes` → `Value::String` so that
/// string properties are returned as strings, not garbage integers.
fn decode_raw_val(raw: u64) -> Value {
    match StoreValue::from_u64(raw) {
        StoreValue::Int64(n) => Value::Int64(n),
        StoreValue::Bytes(b) => Value::String(String::from_utf8_lossy(&b).into_owned()),
    }
}

fn build_row_vals(
    props: &[(u32, u64)],
    var_name: &str,
    _col_ids: &[u32],
) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for &(col_id, raw) in props {
        let key = format!("{var_name}.col_{col_id}");
        map.insert(key, decode_raw_val(raw));
    }
    map
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
        (Value::String(x), Value::String(y)) => {
            // First try exact match (short strings, or both full strings).
            if x == y {
                return true;
            }
            // If the stored value was decoded from the 7-byte inline encoding,
            // it is truncated.  Compare using the inline-encoded forms so that
            // a truncated stored value matches the corresponding full literal
            // (SPA-169).  Two distinct strings that share the same first 7
            // bytes will incorrectly compare equal — this is an accepted
            // limitation of the v1 inline encoding (overflow deferred).
            StoreValue::Bytes(x.as_bytes().to_vec()).to_u64()
                == StoreValue::Bytes(y.as_bytes().to_vec()).to_u64()
        }
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
            Literal::Param(_p) => Value::Null, // params not bound in engine
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
        Expr::NotExists(_) | Expr::CountStar => Value::Null,
        Expr::IsNull(inner) => Value::Bool(matches!(eval_expr(inner, vals), Value::Null)),
        Expr::IsNotNull(inner) => Value::Bool(!matches!(eval_expr(inner, vals), Value::Null)),
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
                .map(|(_, v)| decode_raw_val(*v))
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
                    .map(|(_, val)| decode_raw_val(*val))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        })
        .collect()
}

fn project_fof_row(
    fof_props: &[(u32, u64)],
    column_names: &[String],
    _fof_var: &str,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            let prop = if let Some((_, p)) = col_name.split_once('.') {
                p
            } else {
                col_name.as_str()
            };
            let col_id = prop_name_to_col_id(prop);
            fof_props
                .iter()
                .find(|(c, _)| *c == col_id)
                .map(|(_, v)| decode_raw_val(*v))
                .unwrap_or(Value::Null)
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

fn apply_order_by(rows: &mut [Vec<Value>], m: &MatchStatement, column_names: &[String]) {
    if m.order_by.is_empty() {
        return;
    }
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
/// Currently this covers `id(var)` — a scalar (non-aggregate) function that
/// receives the whole node/relationship reference rather than a scalar property.
/// When this returns `true`, the scan must use the eval path (which inserts
/// `Value::NodeRef` under the variable key) instead of the fast `project_row`
/// path (which only stores individual property columns).
fn needs_node_ref_in_return(items: &[ReturnItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(&item.expr, Expr::FnCall { name, .. } if name.to_lowercase() == "id"))
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
                    .map(|(_, raw)| decode_raw_val(raw))
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
