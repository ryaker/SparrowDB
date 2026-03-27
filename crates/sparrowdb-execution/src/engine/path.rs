//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

impl Engine {
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
    pub(crate) fn get_node_neighbors_labeled(
        &self,
        src_slot: u64,
        src_label_id: u32,
        delta_idx: &DeltaIndex,
        node_label: &std::collections::HashSet<(u64, u32)>,
        all_label_ids: &[u32],
        out: &mut std::collections::HashSet<(u64, u32)>,
    ) {
        out.clear();

        // ── CSR neighbors (slot only; label recovered by scanning all label HWMs
        //    or falling back to src_label_id for homogeneous graphs) ────────────
        let csr_slots: Vec<u64> = self.csr_neighbors_all(src_slot);

        // ── Delta neighbors (full NodeId available) ───────────────────────────
        // SPA-283: O(1) indexed lookup instead of linear scan.
        if let Some(recs) = delta_idx.get(&(src_label_id, src_slot)) {
            for r in recs {
                let dst_slot = r.dst.0 & 0xFFFF_FFFF;
                let dst_label = (r.dst.0 >> 32) as u32;
                out.insert((dst_slot, dst_label));
            }
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
    pub(crate) fn execute_variable_hops(
        &self,
        src_slot: u64,
        src_label_id: u32,
        min_hops: u32,
        max_hops: u32,
        delta_idx: &DeltaIndex,
        node_label: &std::collections::HashSet<(u64, u32)>,
        all_label_ids: &[u32],
        neighbors_buf: &mut std::collections::HashSet<(u64, u32)>,
        use_reachability: bool,
        result_limit: usize,
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
            //
            // Early-exit: when `result_limit` is set (LIMIT clause with no ORDER BY /
            // SKIP), stop expanding the frontier once we have collected enough results.
            // Safe because DISTINCT + LIMIT with no ORDER BY has no defined ordering
            // — BFS order is as valid as any other.  (Issue #199.)
            let mut global_visited: std::collections::HashSet<(u64, u32)> =
                std::collections::HashSet::new();
            global_visited.insert((src_slot, src_label_id));

            let mut frontier: std::collections::VecDeque<(u64, u32, u32)> =
                std::collections::VecDeque::new();
            frontier.push_back((src_slot, src_label_id, 0));

            'bfs: while let Some((cur_slot, cur_label, depth)) = frontier.pop_front() {
                if depth >= max_hops {
                    continue;
                }
                self.get_node_neighbors_labeled(
                    cur_slot,
                    cur_label,
                    delta_idx,
                    node_label,
                    all_label_ids,
                    neighbors_buf,
                );
                for (nb_slot, nb_label) in neighbors_buf.iter().copied().collect::<Vec<_>>() {
                    if global_visited.insert((nb_slot, nb_label)) {
                        let nb_depth = depth + 1;
                        if nb_depth >= min_hops {
                            results.push((nb_slot, nb_label));
                            // Early-exit: stop the moment we have enough results.
                            // Only safe when result_limit reflects a LIMIT with no ORDER BY.
                            if results.len() >= result_limit {
                                break 'bfs;
                            }
                        }
                        frontier.push_back((nb_slot, nb_label, nb_depth));
                    }
                }
            }
        } else {
            // ── Enumerative DFS (full path semantics) ─────────────────────────────────
            //
            // Hard cap: min of the caller's result_limit and PATH_RESULT_CAP.
            // Prevents unbounded memory growth on highly-connected graphs.
            const PATH_RESULT_CAP: usize = 100_000;
            let effective_cap = result_limit.min(PATH_RESULT_CAP);

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
                delta_idx,
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
                            if results.len() >= effective_cap {
                                if effective_cap >= PATH_RESULT_CAP {
                                    eprintln!(
                                        "sparrowdb: variable-length path result cap \
                                         ({PATH_RESULT_CAP}) hit; truncating results.  \
                                         Consider RETURN DISTINCT or a tighter *M..N bound."
                                    );
                                }
                                return results;
                            }
                        }

                        // Recurse deeper if max_hops not yet reached.
                        if depth < max_hops {
                            path_visited.insert((nb_slot, nb_label));
                            self.get_node_neighbors_labeled(
                                nb_slot,
                                nb_label,
                                delta_idx,
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
    pub(crate) fn get_node_neighbors_by_slot(
        &self,
        src_slot: u64,
        src_label_id: u32,
        delta_idx: &DeltaIndex,
    ) -> Vec<u64> {
        let csr_neighbors: Vec<u64> = self.csr_neighbors_all(src_slot);
        // SPA-283: O(1) indexed lookup instead of linear scan.
        // Extend the dedup set directly from the index iterator — no intermediate Vec.
        let mut all: std::collections::HashSet<u64> = csr_neighbors.into_iter().collect();
        if let Some(recs) = delta_idx.get(&(src_label_id, src_slot)) {
            all.extend(recs.iter().map(|r| node_id_parts(r.dst.0).1));
        }
        all.into_iter().collect()
    }

    /// Execute a variable-length path query: `MATCH (a:L1)-[:R*M..N]->(b:L2) RETURN …`.
    pub(crate) fn execute_variable_length(
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
        // SPA-283: build HashMap index for O(1) per-node delta lookups.
        let delta_idx = build_delta_index(&delta_all);
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

        // Compute effective result limit: when no ORDER BY and no SKIP are present,
        // we can stop collecting rows once we reach LIMIT (early exit).
        // With ORDER BY or SKIP we must collect all rows before sorting/skipping.
        let has_order_by = !m.order_by.is_empty();
        let has_skip = m.skip.is_some();
        let row_limit: usize = if has_order_by || has_skip {
            usize::MAX
        } else {
            m.limit.map(|l| l as usize).unwrap_or(usize::MAX)
        };

        for src_slot in 0..hwm_src {
            // SPA-254: check per-query deadline at every slot boundary.
            self.check_deadline()?;

            // Early exit: already have enough rows for the LIMIT.
            if rows.len() >= row_limit {
                break;
            }

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
            // Pass remaining row budget into the BFS/DFS so it can stop early.
            let remaining = row_limit.saturating_sub(rows.len());
            let dst_nodes = self.execute_variable_hops(
                src_slot,
                src_label_id,
                min_hops,
                max_hops,
                &delta_idx,
                &node_label,
                &all_label_ids,
                &mut neighbors_buf,
                use_reachability,
                remaining,
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
                    None, // edge props not available in OPTIONAL MATCH path
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
}
