use sparrowdb_cypher::ast::{
    BinOpKind, BinaryOp, CallStatement, ClosingClause, CreateStatement, Expr, ExistsPattern, Expression, FunctionCall, ListPredicateKind, Literal,
    MatchCreateStatement, MatchMergeRelStatement, MatchMutateStatement, MatchOptionalMatchStatement, MatchStatement, MatchWithStatement, Mutation,
    OptionalMatchStatement, PathPattern, PipelineStage, PipelineStatement, ReturnItem, SortDir, Statement, UnionStatement, UnwindStatement, WithClause,
};
use sparrowdb_cypher::{bind, parse};
use sparrowdb_storage::csr::{CsrBackward, CsrForward};
use sparrowdb_storage::edge_store::{DeltaRecord, EdgeStore, RelTableId};
use sparrowdb_storage::fulltext_index::FulltextIndex;

use sparrowdb_storage::text_index::TextIndex;
use sparrowdb_storage::wal::WalReplayer;

use crate::types::{QueryResult, Value};
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
    /// In-memory B-tree property equality index (SPA-249).
    ///
    /// Built at `Engine::new` time by scanning all on-disk column files.
    /// Enables O(log n) equality-filter scans instead of O(n) full scans.
    pub prop_index: PropertyIndex,
    /// In-memory text search index for CONTAINS and STARTS WITH (SPA-251).
    ///
    /// Built at `Engine::new` alongside `prop_index`.  Stores sorted
    /// `(decoded_string, slot)` pairs per `(label_id, col_id)`.
    /// - CONTAINS: linear scan avoids per-slot property-decode overhead.
    /// - STARTS WITH: binary-search prefix range — O(log n + k).
    pub text_index: TextIndex,
    /// Optional per-query deadline (SPA-254).
    ///
    /// When `Some`, the engine checks this deadline at the top of each hot
    /// scan / traversal loop iteration.  If `Instant::now() >= deadline`,
    /// `Error::QueryTimeout` is returned immediately.  `None` means no
    /// deadline (backward-compatible default).
    pub deadline: Option<std::time::Instant>,
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
        // SPA-249: build property equality index from on-disk column files.
        // Degrades gracefully on error — queries still work, just O(n).
        let (prop_index, text_index) = match catalog.list_labels() {
            Ok(labels) => {
                // catalog returns (u16, String); index builders expect (u32, String).
                let labels_u32: Vec<(u32, String)> = labels
                    .into_iter()
                    .map(|(id, name)| (id as u32, name))
                    .collect();
                let pi = PropertyIndex::build(&store, &labels_u32);
                // SPA-251: build text search index alongside property index.
                let ti = TextIndex::build(&store, &labels_u32);
                (pi, ti)
            }
            Err(e) => {
                tracing::warn!(error = ?e, "SPA-249/SPA-251: index build failed; disabled");
                (PropertyIndex::new(), TextIndex::new())
            }
        };
        Engine {
            store,
            catalog,
            csrs,
            db_root: db_root.to_path_buf(),
            params: HashMap::new(),
            prop_index,
            text_index,
            deadline: None,
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
        let hits = index.search_with_scores(&query);

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
        // Each column emits the appropriate value: NodeRef for "node", Float64 for "score".
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for (raw_id, score) in hits {
            let node_id = sparrowdb_common::NodeId(raw_id);
            let row: Vec<Value> = yield_cols
                .iter()
                .map(|col| match col.as_str() {
                    "score" => Value::Float64(score),
                    _ => Value::NodeRef(node_id),
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
        let wal_dir = self.db_root.join("wal");
        let schema = WalReplayer::scan_schema(&wal_dir)?;

        let mut rows: Vec<Vec<Value>> = Vec::new();

        // Node labels — from catalog.
        let labels = self.catalog.list_labels()?;
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
        let rel_tables = self.catalog.list_rel_tables()?;
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

            // SPA-216: skip tombstoned nodes so that already-deleted nodes are
            // not re-deleted and are not matched by SET mutations either.
            if self.is_node_tombstoned(node_id) {
                continue;
            }

            let props = read_node_props(&self.store, node_id, &all_col_ids)?;

            if !matches_prop_filter_static(
                &props,
                &node_pat.props,
                &self.dollar_params(),
                &self.store,
            ) {
                continue;
            }

            if let Some(ref where_expr) = mm.where_clause {
                let mut row_vals = build_row_vals(&props, var_name, &all_col_ids, &self.store);
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
        match self.store.get_node_raw(node_id, &[0u32]) {
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
        match self.store.get_node_raw(node_id, filter_col_ids) {
            Ok(raw_props) => {
                matches_prop_filter_static(&raw_props, props, &self.dollar_params(), &self.store)
            }
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
                                if !matches_prop_filter_static(
                                    &props,
                                    &node_pat.props,
                                    &self.dollar_params(),
                                    &self.store,
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
                        self.catalog
                            .list_labels()?
                            .into_iter()
                            .map(|(id, _)| id as u32)
                            .collect()
                    } else {
                        let label = node_pat.labels.first().cloned().unwrap_or_default();
                        match self.catalog.get_label(&label)? {
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
                        let hwm = self.store.hwm_for_label(label_id)?;
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
    /// `scan_match_create_rows` but taking the MERGE form's match patterns.
    pub fn scan_match_merge_rel_rows(
        &self,
        mm: &MatchMergeRelStatement,
    ) -> Result<Vec<HashMap<String, NodeId>>> {
        // Reuse scan_match_create_rows by wrapping the MERGE patterns in a
        // MatchCreateStatement with an empty (no-op) CREATE body.
        let proxy = MatchCreateStatement {
