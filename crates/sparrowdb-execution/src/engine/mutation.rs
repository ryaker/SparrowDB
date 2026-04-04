//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

impl Engine {
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

        // Col_ids referenced by the WHERE clause (needed for WHERE evaluation
        // even after the index narrows candidates by inline prop filter).
        let mut where_col_ids: Vec<u32> = node_pat
            .props
            .iter()
            .map(|pe| prop_name_to_col_id(&pe.key))
            .collect();
        if let Some(ref where_expr) = mm.where_clause {
            collect_col_ids_from_expr(where_expr, &mut where_col_ids);
        }

        let var_name = node_pat.var.as_str();

        // Use the property index for O(1) equality lookups on inline prop
        // filters, falling back to full scan for overflow strings / params.
        let candidates = self.scan_nodes_for_label_with_index(label_id, &node_pat.props)?;

        let mut matching_ids = Vec::new();
        for node_id in candidates {
            // Re-read props needed for WHERE clause evaluation.
            if mm.where_clause.is_some() {
                let props = read_node_props(&self.snapshot.store, node_id, &where_col_ids)?;
                if let Some(ref where_expr) = mm.where_clause {
                    let mut row_vals =
                        build_row_vals(&props, var_name, &where_col_ids, &self.snapshot.store);
                    row_vals.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &row_vals) {
                        continue;
                    }
                }
            }
            matching_ids.push(node_id);
        }

        Ok(matching_ids)
    }

    /// Return the mutations carried by a `MatchMutate` statement, exposing them
    /// to the caller (GraphDb) so they can be applied inside a write transaction.
    pub fn mutations_from_match_mutate(mm: &MatchMutateStatement) -> &[Mutation] {
        &mm.mutations
    }

    /// Scan edges matching a MATCH pattern with exactly one hop and return
    /// `(src, dst, rel_type)` tuples for edge deletion.
    ///
    /// Supports `MATCH (a:Label)-[r:REL]->(b:Label) DELETE r` with optional
    /// inline property filters on source and destination node patterns.
    ///
    /// Includes both checkpointed (CSR) and uncheckpointed (delta) edges.
    pub fn scan_match_mutate_edges(
        &self,
        mm: &MatchMutateStatement,
    ) -> Result<Vec<(NodeId, NodeId, String)>> {
        if mm.match_patterns.len() != 1 {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "MATCH...DELETE edge: only single-path patterns are supported".into(),
            ));
        }
        let pat = &mm.match_patterns[0];
        if pat.rels.len() != 1 || pat.nodes.len() != 2 {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "MATCH...DELETE edge: pattern must have exactly one relationship hop".into(),
            ));
        }

        let src_node_pat = &pat.nodes[0];
        let dst_node_pat = &pat.nodes[1];
        let rel_pat = &pat.rels[0];

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();

        // Resolve optional label-id constraints.
        let src_label_id_opt: Option<u32> = if src_label.is_empty() {
            None
        } else {
            match self.snapshot.catalog.get_label(&src_label)? {
                Some(id) => Some(id as u32),
                None => return Ok(vec![]), // unknown label → no matches
            }
        };
        let dst_label_id_opt: Option<u32> = if dst_label.is_empty() {
            None
        } else {
            match self.snapshot.catalog.get_label(&dst_label)? {
                Some(id) => Some(id as u32),
                None => return Ok(vec![]), // unknown label → no matches
            }
        };

        // Filter registered rel tables by rel type and src/dst label.
        let rel_tables: Vec<(u64, u32, u32, String)> = self
            .snapshot
            .catalog
            .list_rel_tables_with_ids()
            .into_iter()
            .filter(|(_, sid, did, rt)| {
                let type_ok = rel_pat.rel_type.is_empty() || rt == &rel_pat.rel_type;
                let src_ok = src_label_id_opt.map(|id| id == *sid as u32).unwrap_or(true);
                let dst_ok = dst_label_id_opt.map(|id| id == *did as u32).unwrap_or(true);
                type_ok && src_ok && dst_ok
            })
            .map(|(cid, sid, did, rt)| (cid, sid as u32, did as u32, rt))
            .collect();

        // Pre-compute col_ids for inline prop filters (avoid re-computing per slot).
        let src_filter_col_ids: Vec<u32> = src_node_pat
            .props
            .iter()
            .map(|p| prop_name_to_col_id(&p.key))
            .collect();
        let dst_filter_col_ids: Vec<u32> = dst_node_pat
            .props
            .iter()
            .map(|p| prop_name_to_col_id(&p.key))
            .collect();

        let mut result: Vec<(NodeId, NodeId, String)> = Vec::new();

        for (catalog_rel_id, effective_src_lid, effective_dst_lid, rel_type) in &rel_tables {
            let catalog_rel_id_u32 =
                u32::try_from(*catalog_rel_id).expect("catalog_rel_id overflowed u32");

            // ── Checkpointed edges (CSR) ──────────────────────────────────────
            let hwm_src = match self.snapshot.store.hwm_for_label(*effective_src_lid) {
                Ok(hwm) => hwm,
                Err(_) => continue,
            };
            for src_slot in 0..hwm_src {
                let src_node = NodeId(((*effective_src_lid as u64) << 32) | src_slot);
                if self.is_node_tombstoned(src_node) {
                    continue;
                }
                if !self.node_matches_prop_filter(
                    src_node,
                    &src_filter_col_ids,
                    &src_node_pat.props,
                ) {
                    continue;
                }
                for dst_slot in self.csr_neighbors(catalog_rel_id_u32, src_slot) {
                    let dst_node = NodeId(((*effective_dst_lid as u64) << 32) | dst_slot);
                    if self.is_node_tombstoned(dst_node) {
                        continue;
                    }
                    if !self.node_matches_prop_filter(
                        dst_node,
                        &dst_filter_col_ids,
                        &dst_node_pat.props,
                    ) {
                        continue;
                    }
                    result.push((src_node, dst_node, rel_type.clone()));
                }
            }

            // ── Uncheckpointed edges (delta log) ──────────────────────────────
            for rec in self.read_delta_for(catalog_rel_id_u32) {
                let r_src_label = (rec.src.0 >> 32) as u32;
                let r_dst_label = (rec.dst.0 >> 32) as u32;
                if src_label_id_opt
                    .map(|id| id != r_src_label)
                    .unwrap_or(false)
                {
                    continue;
                }
                if dst_label_id_opt
                    .map(|id| id != r_dst_label)
                    .unwrap_or(false)
                {
                    continue;
                }
                if self.is_node_tombstoned(rec.src) || self.is_node_tombstoned(rec.dst) {
                    continue;
                }
                if !self.node_matches_prop_filter(rec.src, &src_filter_col_ids, &src_node_pat.props)
                {
                    continue;
                }
                if !self.node_matches_prop_filter(rec.dst, &dst_filter_col_ids, &dst_node_pat.props)
                {
                    continue;
                }
                result.push((rec.src, rec.dst, rel_type.clone()));
            }
        }

        Ok(result)
    }

    // ── Node-scan helpers (shared by scan_match_create and scan_match_create_rows) ──

    /// Returns `true` if the given node has been tombstoned (col 0 == u64::MAX).
    ///
    /// `NotFound` is expected for new/sparse nodes where col_0 has not been
    /// written yet and is treated as "not tombstoned".  All other errors are
    /// logged as warnings and also treated as "not tombstoned" so that
    /// transient storage issues do not suppress valid nodes during a scan.
    pub(crate) fn is_node_tombstoned(&self, node_id: NodeId) -> bool {
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
    pub(crate) fn node_matches_prop_filter(
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

    /// Return all live `NodeId`s for `label_id` whose inline prop predicates
    /// match, using the `PropertyIndex` for O(1) equality lookups when possible.
    ///
    /// ## Index path (O(log n) per unique value)
    ///
    /// When there is exactly one inline prop filter and the literal is directly
    /// encodable (integers and strings ≤ 7 bytes), the method:
    ///   1. Calls `build_for` lazily — reads the column file once and caches it.
    ///   2. Does a single `BTreeMap::get` to obtain the matching slot list.
    ///   3. Verifies tombstones on the (usually tiny) candidate set.
    ///
    /// ## Fallback (O(n) full scan)
    ///
    /// When the filter cannot use the index (overflow string, multiple props,
    /// parameter expressions, or `build_for` I/O error) the method falls back
    /// to iterating all `0..hwm` slots — the same behaviour as before this fix.
    ///
    /// ## Multi-label support (SPA-200)
    ///
    /// After the primary-label scan, nodes where `label_id` is a **secondary**
    /// label are also added from the catalog reverse index.  This ensures
    /// `MATCH (n:A)` finds nodes where `:A` is secondary (e.g. `CREATE (n:B:A)`).
    ///
    /// ## Integration
    ///
    /// This replaces the inline `for slot in 0..hwm` blocks in
    /// `scan_match_create`, `scan_match_create_rows`, and `scan_match_mutate`
    /// so that the index is used consistently across all write-side MATCH paths.
    pub(crate) fn scan_nodes_for_label_with_index(
        &self,
        label_id: u32,
        node_props: &[sparrowdb_cypher::ast::PropEntry],
    ) -> Result<Vec<NodeId>> {
        let hwm = self.snapshot.store.hwm_for_label(label_id)?;

        // Collect filter col_ids up-front (needed for the fallback path too).
        let filter_col_ids: Vec<u32> = node_props
            .iter()
            .map(|p| prop_name_to_col_id(&p.key))
            .collect();

        // ── Lazy index build ────────────────────────────────────────────────
        // Ensure the property index is loaded for every column referenced by
        // inline prop filters.  `build_for` is idempotent (cache-hit no-op
        // after the first call) and suppresses I/O errors internally.
        for &col_id in &filter_col_ids {
            let _ = self
                .prop_index
                .borrow_mut()
                .build_for(&self.snapshot.store, label_id, col_id);
        }

        // ── Index lookup (single-equality filter, literal value) ────────────
        let index_slots: Option<Vec<u32>> = {
            let prop_index_ref = self.prop_index.borrow();
            try_index_lookup_for_props(node_props, label_id, &prop_index_ref)
        };

        if let Some(candidate_slots) = index_slots {
            // O(k) verification over a small candidate set (typically 1 slot).
            let mut result = Vec::with_capacity(candidate_slots.len());
            for slot in candidate_slots {
                let node_id = NodeId(((label_id as u64) << 32) | slot as u64);
                if self.is_node_tombstoned(node_id) {
                    continue;
                }
                // For multi-prop filters the index only narrowed on one column;
                // verify the remaining filters here.
                if !self.node_matches_prop_filter(node_id, &filter_col_ids, node_props) {
                    continue;
                }
                result.push(node_id);
            }
            // SPA-200: also check secondary-label hits (not in the property index,
            // which is keyed on primary label only).
            self.append_secondary_label_hits(label_id, &filter_col_ids, node_props, &mut result);
            return Ok(result);
        }

        // ── Fallback: full O(N) scan ────────────────────────────────────────
        let mut result = Vec::new();
        for slot in 0..hwm {
            let node_id = NodeId(((label_id as u64) << 32) | slot);
            if self.is_node_tombstoned(node_id) {
                continue;
            }
            if !self.node_matches_prop_filter(node_id, &filter_col_ids, node_props) {
                continue;
            }
            result.push(node_id);
        }

        // SPA-200: union nodes where label_id is a *secondary* label.
        self.append_secondary_label_hits(label_id, &filter_col_ids, node_props, &mut result);

        Ok(result)
    }

    /// Append nodes that have `label_id` as a **secondary** label to `result`,
    /// applying property filters.  Used by `scan_nodes_for_label_with_index`
    /// to implement SPA-200 multi-label MATCH semantics.
    ///
    /// Nodes already in `result` (primary-label hits) are not duplicated.
    fn append_secondary_label_hits(
        &self,
        label_id: u32,
        filter_col_ids: &[u32],
        node_props: &[sparrowdb_cypher::ast::PropEntry],
        result: &mut Vec<NodeId>,
    ) {
        // Build a quick-lookup set of already-found nodes to avoid duplicates.
        let already_found: HashSet<NodeId> = result.iter().copied().collect();

        let lid = label_id as sparrowdb_catalog::LabelId;
        for node_id in self.snapshot.catalog.nodes_with_secondary_label(lid) {
            if already_found.contains(&node_id) {
                continue;
            }
            if self.is_node_tombstoned(node_id) {
                continue;
            }
            // For secondary-label nodes, properties are stored under the
            // primary-label directory (encoded in node_id).  The col_ids are
            // the same — we read the stored columns and apply the filter.
            if !self.node_matches_prop_filter(node_id, filter_col_ids, node_props) {
                continue;
            }
            result.push(node_id);
        }
    }

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

                // Use the property index for O(1) equality lookups when possible,
                // falling back to a full O(N) scan for overflow strings / params.
                let matching_ids =
                    self.scan_nodes_for_label_with_index(label_id, &node_pat.props)?;

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

                    // Use the property index for O(1) equality lookups when possible,
                    // falling back to a full O(N) scan for overflow strings / params.
                    let mut matching_ids: Vec<NodeId> = Vec::new();
                    for label_id in scan_label_ids {
                        let ids =
                            self.scan_nodes_for_label_with_index(label_id, &node_pat.props)?;
                        matching_ids.extend(ids);
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
                return_clause: None,
            },
        };
        self.scan_match_create_rows(&proxy)
    }

    // ── UNWIND ─────────────────────────────────────────────────────────────────

    pub(crate) fn execute_unwind(&self, u: &UnwindStatement) -> Result<QueryResult> {
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
    /// 1. Look up (or create) its **primary** label (first label in the
    ///    pattern) in the catalog.  The primary label determines the `NodeId`
    ///    encoding and storage directory.
    /// 2. Resolve all secondary labels (labels[1..]) and record them in the
    ///    catalog side table after the node is created (SPA-200).
    /// 3. Convert inline properties to `(col_id, StoreValue)` pairs using the
    ///    same FNV-1a hash used by `WriteTx::merge_node`.
    /// 4. Write the node to the node store.
    pub(crate) fn execute_create(&mut self, create: &CreateStatement) -> Result<QueryResult> {
        for node in &create.nodes {
            // Resolve the primary label, creating it if absent.
            let label = node.labels.first().cloned().unwrap_or_default();

            // SPA-208: reject reserved __SO_ label prefix.
            if is_reserved_label(&label) {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "invalid argument: label \"{label}\" is reserved — the __SO_ prefix is for internal use only"
                )));
            }

            // SPA-200: reject reserved __SO_ prefix on secondary labels too.
            for secondary_label_name in node.labels.iter().skip(1) {
                if is_reserved_label(secondary_label_name) {
                    return Err(sparrowdb_common::Error::InvalidArgument(format!(
                        "invalid argument: label \"{secondary_label_name}\" is reserved — the __SO_ prefix is for internal use only"
                    )));
                }
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
            // FTS auto-indexing: if a fulltext index is registered for any
            // (label, property) pair of this node, insert the string value
            // into the BM25 index so it is searchable immediately.
            {
                use sparrowdb_storage::fts_index::{FtsIndex, FtsRegistry};
                let registry = FtsRegistry::load(&self.snapshot.db_root);
                for entry in &node.props {
                    if registry.contains(&label, &entry.key) {
                        let val = eval_expr(&entry.value, &HashMap::new());
                        if let Value::String(text) = val {
                            if let Ok(mut idx) =
                                FtsIndex::open(&self.snapshot.db_root, &label, &entry.key)
                            {
                                idx.insert(node_id.0, &text);
                                if let Err(e) = idx.save() {
                                    tracing::warn!(
                                        "FTS index save failed for ({label}, {}): {e}",
                                        entry.key
                                    );
                                }
                            }
                        }
                    }
                }
            }
            // SPA-200: record secondary labels in the catalog side table.
            // The primary label is already encoded in `node_id`; secondary
            // labels are persisted here so that MATCH on secondary labels
            // (and labels(n)) return the full label set.
            if node.labels.len() > 1 {
                let mut secondary_label_ids: Vec<sparrowdb_catalog::LabelId> = Vec::new();
                for secondary_name in node.labels.iter().skip(1) {
                    let sid = match self.snapshot.catalog.get_label(secondary_name)? {
                        Some(id) => id,
                        None => self.snapshot.catalog.create_label(secondary_name)?,
                    };
                    secondary_label_ids.push(sid);
                }
                self.snapshot
                    .catalog
                    .record_secondary_labels(node_id, &secondary_label_ids)?;
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

    pub(crate) fn execute_create_index(
        &mut self,
        label: &str,
        property: &str,
    ) -> Result<QueryResult> {
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
    pub(crate) fn execute_create_constraint(
        &mut self,
        label: &str,
        property: &str,
    ) -> Result<QueryResult> {
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

    /// Execute `CREATE FULLTEXT INDEX [name] FOR (n:Label) ON (n.property)`.
    ///
    /// Creates (or overwrites) a BM25 full-text index on the given label+property
    /// and registers it in the FTS registry so that `full_text_search()` and
    /// `bm25_score()` can locate it.
    pub(crate) fn execute_create_fulltext_index(
        &mut self,
        _name: Option<&str>,
        label: &str,
        property: &str,
    ) -> Result<QueryResult> {
        use sparrowdb_storage::fts_index::{FtsIndex, FtsRegistry};
        // Create (or overwrite) the on-disk BM25 index.
        FtsIndex::create(&self.snapshot.db_root, label, property)?;
        // Register in the persistent registry.
        let mut registry = FtsRegistry::load(&self.snapshot.db_root);
        registry.register(&self.snapshot.db_root, label, property)?;
        Ok(QueryResult::empty(vec![]))
    }
}
