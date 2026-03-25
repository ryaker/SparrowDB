//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

impl Engine {
    /// Execute a multi-clause Cypher pipeline (SPA-134).
    ///
    /// Executes stages left-to-right, passing the intermediate row set from
    /// one stage to the next, then projects the final RETURN clause.
    pub(crate) fn execute_pipeline(&self, p: &PipelineStatement) -> Result<QueryResult> {
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
    pub(crate) fn collect_pipeline_match_rows(
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
    pub(crate) fn execute_pipeline_match_stage(
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
    pub(crate) fn execute_pipeline_match_hop(
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
    pub(crate) fn matches_prop_filter_with_binding(
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
    pub(crate) fn collect_match_rows_for_with(
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

    pub(crate) fn execute_match(&self, m: &MatchStatement) -> Result<QueryResult> {
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
            // ── Q6 COUNT label fast-path (SPA-197) ──────────────────────
            // MATCH (n:Label) RETURN COUNT(n) AS total  →  O(1) lookup
            if let Some(result) = self.try_count_label_fastpath(m, &column_names)? {
                return Ok(result);
            }

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

    // ── Q6 COUNT label fast-path (SPA-197) ─────────────────────────────────────
    //
    // Detects `MATCH (n:Label) RETURN COUNT(n) AS alias` (or COUNT(*)) and
    // answers it from the pre-populated `label_row_counts` HashMap in O(1)
    // instead of scanning every node slot.
    //
    // Qualifying conditions:
    //   1. Exactly one label on the node pattern.
    //   2. No WHERE clause.
    //   3. No inline prop filters on the node pattern.
    //   4. RETURN has exactly one item: COUNT(*) or COUNT(var) where var
    //      matches the node pattern variable.
    //   5. No ORDER BY, SKIP, or LIMIT (single scalar result).
    pub(crate) fn try_count_label_fastpath(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<Option<QueryResult>> {
        let pat = &m.pattern[0];
        let node = &pat.nodes[0];

        // Condition 1: exactly one label.
        let label = match &node.labels[..] {
            [l] => l.clone(),
            _ => return Ok(None),
        };

        // Condition 2: no WHERE clause.
        if m.where_clause.is_some() {
            return Ok(None);
        }

        // Condition 3: no inline prop filters.
        if !node.props.is_empty() {
            return Ok(None);
        }

        // Condition 4: exactly one RETURN item that is COUNT(*) or COUNT(var).
        if m.return_clause.items.len() != 1 {
            return Ok(None);
        }
        let item = &m.return_clause.items[0];
        let is_count = match &item.expr {
            Expr::CountStar => true,
            Expr::FnCall { name, args } => {
                name == "count"
                    && args.len() == 1
                    && matches!(&args[0], Expr::Var(v) if v == &node.var)
            }
            _ => false,
        };
        if !is_count {
            return Ok(None);
        }

        // Condition 5: no ORDER BY / SKIP / LIMIT.
        if !m.order_by.is_empty() || m.skip.is_some() || m.limit.is_some() {
            return Ok(None);
        }

        // All conditions met — resolve label → count from the cached map.
        let count = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => *self.snapshot.label_row_counts.get(&id).unwrap_or(&0),
            None => 0,
        };

        tracing::debug!(label = %label, count = count, "Q6 COUNT label fastpath hit");

        Ok(Some(QueryResult {
            columns: column_names.to_vec(),
            rows: vec![vec![Value::Int64(count as i64)]],
        }))
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
    pub(crate) fn try_degree_sort_fastpath(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<Option<QueryResult>> {
        use sparrowdb_cypher::ast::SortDir;

        let pat = &m.pattern[0];
        let node = &pat.nodes[0];

        // Condition 1: exactly one label.
        let label = match &node.labels[..] {
            [l] => l.clone(),
            _ => return Ok(None),
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

    // ── COUNT(f) + ORDER BY alias DESC LIMIT k fast-path (SPA-272 / Q7) ────────
    //
    // Detects 1-hop aggregation queries of the shape:
    //
    //   MATCH (n:Label)-[:TYPE]->(f:Label2)
    //   RETURN n.prop, COUNT(f) AS alias
    //   ORDER BY alias DESC LIMIT k
    //
    // and answers them directly from the pre-computed DegreeCache without
    // scanning edges or grouping rows.  Returns `None` when the pattern does
    // not qualify; the caller falls through to the normal execution path.
    //
    // Qualifying conditions:
    //   1. Single 1-hop pattern with outgoing direction, no WHERE clause,
    //      no inline prop filters on either node.
    //   2. Exactly 2 RETURN items: one property access `n.prop` (group key)
    //      and one `COUNT(var)` where `var` matches the destination variable.
    //   3. ORDER BY is `Expr::Var(alias)` DESC where alias == COUNT's alias.
    //   4. LIMIT is Some(k) with k > 0.
    pub(crate) fn try_count_agg_degree_fastpath(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<Option<QueryResult>> {
        use sparrowdb_cypher::ast::EdgeDir;

        let pat = &m.pattern[0];
        // Must be a 1-hop pattern.
        if pat.nodes.len() != 2 || pat.rels.len() != 1 {
            return Ok(None);
        }
        let src_node = &pat.nodes[0];
        let dst_node = &pat.nodes[1];
        let rel = &pat.rels[0];

        // Outgoing direction only.
        if rel.dir != EdgeDir::Outgoing {
            return Ok(None);
        }

        // No WHERE clause.
        if m.where_clause.is_some() {
            return Ok(None);
        }

        // No inline prop filters on either node.
        if !src_node.props.is_empty() || !dst_node.props.is_empty() {
            return Ok(None);
        }

        // Source must have a label.
        let src_label = match src_node.labels.first() {
            Some(l) if !l.is_empty() => l.clone(),
            _ => return Ok(None),
        };

        // Exactly 2 RETURN items.
        let items = &m.return_clause.items;
        if items.len() != 2 {
            return Ok(None);
        }

        // Identify which item is COUNT(dst_var) and which is the group key (n.prop).
        let dst_var = &dst_node.var;
        let src_var = &src_node.var;

        let (prop_col_name, count_alias) = {
            let mut prop_col: Option<String> = None;
            let mut count_al: Option<String> = None;

            for item in items {
                match &item.expr {
                    Expr::FnCall { name, args }
                        if name.to_lowercase() == "count" && args.len() == 1 =>
                    {
                        // COUNT(f) — arg must be the destination variable.
                        if let Some(Expr::Var(v)) = args.first() {
                            if v == dst_var {
                                count_al =
                                    item.alias.clone().or_else(|| Some(format!("COUNT({})", v)));
                            } else {
                                return Ok(None);
                            }
                        } else {
                            return Ok(None);
                        }
                    }
                    Expr::PropAccess { var, prop } => {
                        // n.prop — must reference the source variable.
                        if var == src_var {
                            prop_col = Some(prop.clone());
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                }
            }

            match (prop_col, count_al) {
                (Some(pc), Some(ca)) => (pc, ca),
                _ => return Ok(None),
            }
        };

        // ORDER BY must be a single Var matching the COUNT alias, DESC.
        if m.order_by.len() != 1 {
            return Ok(None);
        }
        let (sort_expr, sort_dir) = &m.order_by[0];
        if *sort_dir != SortDir::Desc {
            return Ok(None);
        }
        match sort_expr {
            Expr::Var(v) if *v == count_alias => {}
            _ => return Ok(None),
        }

        // LIMIT must be set and > 0.
        let k = match m.limit {
            Some(k) if k > 0 => k as usize,
            _ => return Ok(None),
        };

        // ── All conditions met — execute via DegreeCache. ──────────────────

        let label_id = match self.snapshot.catalog.get_label(&src_label)? {
            Some(id) => id as u32,
            None => {
                return Ok(Some(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                }));
            }
        };

        tracing::debug!(
            label = %src_label,
            k = k,
            count_alias = %count_alias,
            "SPA-272: COUNT-agg degree-cache fast-path activated (Q7 shape)"
        );

        let top_k = self.top_k_by_degree(label_id, k)?;

        // Apply SKIP if present.
        let skip = m.skip.unwrap_or(0) as usize;
        let top_k = if skip >= top_k.len() {
            &[][..]
        } else {
            &top_k[skip..]
        };

        // Resolve the property column ID for the group key.
        let prop_col_id = prop_name_to_col_id(&prop_col_name);

        // Build result rows. For each (slot, degree), look up n.prop and emit.
        // Skip degree-0 nodes: a 1-hop MATCH only produces rows for nodes with
        // at least one neighbor, so COUNT(f) is always >= 1 in the normal path.
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(top_k.len());
        for &(slot, degree) in top_k {
            if degree == 0 {
                continue;
            }

            let node_id = NodeId(((label_id as u64) << 32) | slot);

            // Skip tombstoned nodes.
            if self.is_node_tombstoned(node_id) {
                continue;
            }

            // Fetch the property for the group key (nullable path so missing
            // columns return NULL instead of a NotFound error).
            let prop_raw = read_node_props(&self.snapshot.store, node_id, &[prop_col_id])?;
            let prop_val = prop_raw
                .iter()
                .find(|(c, _)| *c == prop_col_id)
                .map(|(_, v)| decode_raw_val(*v, &self.snapshot.store))
                .unwrap_or(Value::Null);

            // Project in the same order as column_names.
            let row: Vec<Value> = column_names
                .iter()
                .map(|col| {
                    if col == &count_alias {
                        Value::Int64(degree as i64)
                    } else {
                        prop_val.clone()
                    }
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
    pub(crate) fn execute_optional_match(&self, om: &OptionalMatchStatement) -> Result<QueryResult> {
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
    pub(crate) fn execute_match_optional_match(
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
    pub(crate) fn optional_one_hop_sub_rows(
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
    pub(crate) fn execute_multi_pattern_scan(
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

    pub(crate) fn execute_scan(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
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

        // SPA-198: LIMIT pushdown — compute an early-exit cap so we can break
        // out of the scan loop once we have enough rows.  This is only safe
        // when there is no aggregation, no ORDER BY, and no DISTINCT (all of
        // which require the full result set before they can operate).
        let scan_cap: usize = if !use_eval_path && !m.distinct && m.order_by.is_empty() {
            match (m.skip, m.limit) {
                (Some(s), Some(l)) => (s as usize).saturating_add(l as usize),
                (None, Some(l)) => l as usize,
                _ => usize::MAX,
            }
        } else {
            usize::MAX
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
                // SPA-198: early exit when we have enough rows for SKIP+LIMIT.
                if rows.len() >= scan_cap {
                    break;
                }
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

    pub(crate) fn execute_scan_all_labels(
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

}
