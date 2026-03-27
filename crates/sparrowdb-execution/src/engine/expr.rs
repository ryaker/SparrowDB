//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

impl Engine {
    // ── Property filter helpers ───────────────────────────────────────────────

    pub(crate) fn matches_prop_filter(
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
    pub(crate) fn dollar_params(&self) -> HashMap<String, Value> {
        self.params
            .iter()
            .map(|(k, v)| (format!("${k}"), v.clone()))
            .collect()
    }

    // ── Graph-aware expression evaluation (SPA-136, SPA-137, SPA-138) ────────

    /// Evaluate an expression that may require graph access (EXISTS, ShortestPath).
    pub(crate) fn eval_expr_graph(&self, expr: &Expr, vals: &HashMap<String, Value>) -> Value {
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
    pub(crate) fn eval_where_graph(&self, expr: &Expr, vals: &HashMap<String, Value>) -> bool {
        match self.eval_expr_graph(expr, vals) {
            Value::Bool(b) => b,
            _ => eval_where(expr, vals),
        }
    }

    /// Evaluate `EXISTS { (n)-[:REL]->(:DstLabel) }` — SPA-137.
    pub(crate) fn eval_exists_subquery(
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
    pub(crate) fn resolve_node_id_from_var(
        &self,
        var: &str,
        vals: &HashMap<String, Value>,
    ) -> Option<NodeId> {
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
    pub(crate) fn eval_shortest_path_expr(
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
    pub(crate) fn find_node_by_props(
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
    /// Each frontier node carries its own `label_id` so that delta-log edge
    /// lookups use the correct `(label_id, slot)` key at every hop.  Without
    /// this, BFS through heterogeneous graphs would use the source label for
    /// all intermediate nodes, missing WAL edges on label-boundary crossings.
    pub(crate) fn bfs_shortest_path(
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
        // SPA-283: build HashMap index for O(1) per-node delta lookups.
        let delta_idx = build_delta_index(&delta_all);
        // Frontier carries (slot, label_id) so each hop uses the correct label
        // when probing the delta index.
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(src_slot);
        let mut frontier: Vec<(u64, u32)> = vec![(src_slot, src_label_id)];

        for depth in 1..=max_hops {
            let mut next_frontier: Vec<(u64, u32)> = Vec::new();
            for &(node_slot, node_label_id) in &frontier {
                let neighbors =
                    self.get_node_neighbors_by_slot(node_slot, node_label_id, &delta_idx, &[]);
                for nb_slot in neighbors {
                    if nb_slot == dst_slot {
                        return Some(depth);
                    }
                    if visited.insert(nb_slot) {
                        // Recover the neighbor's label from the delta index; fall
                        // back to node_label_id for CSR-only nodes in homogeneous
                        // graphs (the same conservative default used elsewhere).
                        let nb_label = delta_neighbors_labeled_from_index(
                            &delta_idx,
                            node_label_id,
                            node_slot,
                        )
                        .find(|&(s, _)| s == nb_slot)
                        .map(|(_, l)| l)
                        .unwrap_or(node_label_id);
                        next_frontier.push((nb_slot, nb_label));
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
    pub(crate) fn aggregate_rows_graph(
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
