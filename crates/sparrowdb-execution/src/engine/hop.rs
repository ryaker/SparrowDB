//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

/// Precomputed neighbor entry for the `b`-slot in a mutual-friends (FoF)
/// hash-set intersection: `(b_slot, forward_neighbor_set, b_property_values)`.
type BNeighborEntry = (u64, HashSet<u64>, Vec<(u32, u64)>);

impl Engine {
    // ── 1-hop traversal: (a)-[:R]->(f) ───────────────────────────────────────

    pub(crate) fn execute_one_hop(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        // ── Q7 COUNT-agg degree-cache fast-path (SPA-272) ─────────────────────
        // Try to short-circuit `MATCH (n)-[:R]->(f) RETURN n.prop, COUNT(f) AS
        // alias ORDER BY alias DESC LIMIT k` via DegreeCache before falling
        // through to the full scan + aggregate path.
        if let Some(result) = self.try_count_agg_degree_fastpath(m, column_names)? {
            return Ok(result);
        }

        let pat = &m.pattern[0];
        let rel_pat = &pat.rels[0];
        let dir = &rel_pat.dir;
        use sparrowdb_cypher::ast::EdgeDir;

        // Incoming (`(a)<-[:R]-(x)`): the physical edge runs x -> a, so swap
        // which pattern node plays the "src" (traversal-forward) role before
        // any label resolution, table filtering, or CSR lookups happen below.
        // Every reference downstream goes through src_node_pat/dst_node_pat
        // (never pat.nodes[..] directly), so this single swap is sufficient —
        // it also naturally binds the correct variable name to each side when
        // rows are projected at the end of the function.
        // Both (undirected): handled by running forward + backward passes,
        // so it keeps the unswapped (nodes[0], nodes[1]) order here.
        let (src_node_pat, dst_node_pat) = if *dir == EdgeDir::Incoming {
            (&pat.nodes[1], &pat.nodes[0])
        } else {
            (&pat.nodes[0], &pat.nodes[1])
        };

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

        // use_raw_rows_path is true for aggregations OR for id(n)/NodeRef-dependent
        // RETURN items (#372): the raw-rows + aggregate_rows_graph path correctly
        // injects NodeRef into the row map, whereas project_hop_row does not handle
        // id().
        let use_raw_rows_path = has_aggregate_in_return(&m.return_clause.items)
            || needs_node_ref_in_return(&m.return_clause.items);
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
                collect_col_ids_for_var_from_items(&src_node_pat.var, &m.return_clause.items);
            let mut col_ids_dst =
                collect_col_ids_for_var_from_items(&dst_node_pat.var, &m.return_clause.items);
            if use_raw_rows_path {
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

            // SPA-283: build a HashMap index keyed by (src_label_id, src_slot)
            // so each per-node neighbor lookup is O(1) instead of O(n).
            let delta_index = build_delta_index(&delta_records_all);

            // SPA-240: Pre-read all edge props for this rel table if any edge
            // property access is needed (inline filter, projection, or WHERE).
            //
            // edge_props.bin is now keyed by (src_slot, dst_slot) rather than by
            // the transient delta-log edge_id.  This makes lookups correct for
            // both pre- and post-checkpoint databases; previously the lookup via
            // delta_edge_id_map always returned None after CHECKPOINT because the
            // delta log is truncated on checkpoint.
            //
            // Guard: skip the read entirely when the query has no inline edge
            // property filter AND the relationship variable (if any) is not
            // referenced by a property access in either RETURN or WHERE.  This
            // avoids opening edge_props.bin for every hop query (SPA-243 perf).
            let needs_edge_props = !rel_pat.props.is_empty()
                || (!rel_pat.var.is_empty() && {
                    // Check RETURN columns for rel_var.* references.
                    let in_return = column_names.iter().any(|c| {
                        c.split_once('.')
                            .is_some_and(|(v, _)| v == rel_pat.var.as_str())
                    });
                    // Check WHERE clause for rel_var.* property access.
                    let in_where = m.where_clause.as_ref().is_some_and(|wexpr| {
                        let mut tmp: Vec<u32> = Vec::new();
                        collect_col_ids_from_expr_for_var(wexpr, rel_pat.var.as_str(), &mut tmp);
                        !tmp.is_empty()
                    });
                    in_return || in_where
                });
            // SPA-261: use cached edge-props map from ReadSnapshot instead of
            // re-reading edge_props.bin on every query.  On first access per
            // rel table the file is read once and the grouped HashMap is cached.
            let edge_props_by_slots: std::collections::HashMap<(u64, u64), Vec<(u32, u64)>> =
                if needs_edge_props {
                    self.snapshot.edge_props_for_rel(storage_rel_id.0)
                } else {
                    std::collections::HashMap::new()
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

                // SPA-163 / SPA-195 / SPA-283: O(1) indexed delta lookup
                // instead of linear scan over all delta records.
                let delta_neighbors: Vec<u64> =
                    delta_neighbors_from_index(&delta_index, effective_src_label_id, src_slot);

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
                    .chain(delta_neighbors)
                    .collect();

                // ── SPA-200: batch-read dst properties — O(cols) fs::read() calls
                // instead of O(neighbors × cols). ─────────────────────────────────
                // Compute the full column-id list needed for dst (same for every
                // neighbor in this src → * traversal).
                let all_needed_dst: Vec<u32> =
                    if !col_ids_dst.is_empty() || !dst_node_pat.props.is_empty() {
                        let mut v = col_ids_dst.clone();
                        for p in &dst_node_pat.props {
                            let col_id = prop_name_to_col_id(&p.key);
                            if !v.contains(&col_id) {
                                v.push(col_id);
                            }
                        }
                        v
                    } else {
                        vec![]
                    };

                // Deduplicate neighbor slots for the batch read (same set we
                // visit in the inner loop; duplicates are skipped there anyway).
                let unique_dst_slots: Vec<u32> = {
                    let mut seen: HashSet<u64> = HashSet::new();
                    all_neighbors
                        .iter()
                        .filter_map(|&s| if seen.insert(s) { Some(s as u32) } else { None })
                        .collect()
                };

                // Batch-read: one fs::read() per column for all neighbors.
                // dst_batch[i] = raw column values for unique_dst_slots[i].
                let dst_batch: Vec<Vec<u64>> = if !all_needed_dst.is_empty() {
                    self.snapshot.store.batch_read_node_props(
                        effective_dst_label_id,
                        &unique_dst_slots,
                        &all_needed_dst,
                    )?
                } else {
                    vec![]
                };
                // Build a slot → batch-row index map for O(1) lookup.
                let dst_slot_to_idx: HashMap<u64, usize> = unique_dst_slots
                    .iter()
                    .enumerate()
                    .map(|(i, &s)| (s as u64, i))
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
                    // Use the batch-prefetched result; fall back to per-node
                    // read only when the slot was not in the batch (shouldn't
                    // happen, but keeps the code correct under all conditions).
                    let dst_props: Vec<(u32, u64)> = if !all_needed_dst.is_empty() {
                        if let Some(&idx) = dst_slot_to_idx.get(&dst_slot) {
                            all_needed_dst
                                .iter()
                                .copied()
                                .zip(dst_batch[idx].iter().copied())
                                .collect()
                        } else {
                            // Fallback: individual read (e.g. delta-only slot).
                            self.snapshot
                                .store
                                .get_node_raw(dst_node, &all_needed_dst)?
                        }
                    } else {
                        vec![]
                    };

                    // Apply dst inline prop filter.
                    if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                        continue;
                    }

                    // SPA-240: look up edge props for this (src_slot, dst_slot) pair.
                    // Works for both delta-only and checkpointed edges because
                    // edge_props.bin is now keyed by (src_slot, dst_slot).
                    let current_edge_props: Vec<(u32, u64)> = if needs_edge_props {
                        edge_props_by_slots
                            .get(&(src_slot, dst_slot))
                            .cloned()
                            .unwrap_or_default()
                    } else {
                        vec![]
                    };

                    // Apply inline edge prop filter from rel pattern: [r:TYPE {prop: val}].
                    if !rel_pat.props.is_empty()
                        && !self.matches_prop_filter(&current_edge_props, &rel_pat.props)
                    {
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
                        // Inject edge properties so r.prop references in WHERE resolve.
                        if !rel_pat.var.is_empty() && !current_edge_props.is_empty() {
                            for &(col_id, raw) in &current_edge_props {
                                let key = format!("{}.col_{}", rel_pat.var, col_id);
                                row_vals.insert(key, decode_raw_val(raw, &self.snapshot.store));
                            }
                        }
                        // SPA-200: inject full label set (primary + secondary).
                        if !src_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", src_node_pat.var),
                                self.labels_value_for_node(src_node),
                            );
                        }
                        if !dst_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", dst_node_pat.var),
                                self.labels_value_for_node(dst_node),
                            );
                        }
                        row_vals.extend(self.dollar_params());
                        if !self.eval_where_graph(where_expr, &row_vals) {
                            continue;
                        }
                    }

                    if use_raw_rows_path {
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
                        // SPA-200: inject full label set (primary + secondary).
                        if !src_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", src_node_pat.var),
                                self.labels_value_for_node(src_node),
                            );
                        }
                        if !dst_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", dst_node_pat.var),
                                self.labels_value_for_node(dst_node),
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
                        // SPA-178: build edge_props arg for project_hop_row.
                        let rel_edge_props_arg =
                            if !rel_pat.var.is_empty() && !current_edge_props.is_empty() {
                                Some((rel_pat.var.as_str(), current_edge_props.as_slice()))
                            } else {
                                None
                            };
                        let row = project_hop_row(
                            &src_props,
                            &dst_props,
                            &m.return_clause.items,
                            &src_node_pat.var,
                            &dst_node_pat.var,
                            rel_var_type,
                            src_label_meta,
                            dst_label_meta,
                            &self.snapshot.store,
                            rel_edge_props_arg,
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
                    collect_col_ids_for_var_from_items(&src_node_pat.var, &m.return_clause.items);
                let mut col_ids_dst =
                    collect_col_ids_for_var_from_items(&dst_node_pat.var, &m.return_clause.items);
                if use_raw_rows_path {
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
                        .chain(delta_predecessors)
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
                            // SPA-200: inject full label set (primary + secondary).
                            if !src_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", src_node_pat.var),
                                    self.labels_value_for_node(b_node),
                                );
                            }
                            if !dst_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", dst_node_pat.var),
                                    self.labels_value_for_node(a_node),
                                );
                            }
                            row_vals.extend(self.dollar_params());
                            if !self.eval_where_graph(where_expr, &row_vals) {
                                continue;
                            }
                        }

                        if use_raw_rows_path {
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
                            // SPA-200: inject full label set (primary + secondary).
                            if !src_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", src_node_pat.var),
                                    self.labels_value_for_node(b_node),
                                );
                            }
                            if !dst_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", dst_node_pat.var),
                                    self.labels_value_for_node(a_node),
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
                                &m.return_clause.items,
                                &src_node_pat.var,
                                &dst_node_pat.var,
                                rel_var_type,
                                src_label_meta,
                                dst_label_meta,
                                &self.snapshot.store,
                                None, // edge props not available in backward pass
                            );
                            rows.push(row);
                        }
                    }
                }
            }
        }

        if use_raw_rows_path {
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

    pub(crate) fn execute_two_hop(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        // nodes[1] is the mid node (may be named, e.g. `m` in Q8 mutual-friends)
        let mid_node_pat = &pat.nodes[1];
        // nodes[2] is the fof (friend-of-friend) / anchor-B in Q8
        let fof_node_pat = &pat.nodes[2];

        // If the terminal node is unlabeled, the generic N-hop executor already
        // recovers the actual label of every final hop from the stored edges.
        // Route there instead of requiring fof_label_id and returning a bare
        // "not found".
        if fof_node_pat.labels.is_empty() {
            return self.execute_n_hop(m, column_names);
        }

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
            let mut ids =
                collect_col_ids_for_var_from_items(&fof_node_pat.var, &m.return_clause.items);
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
        // SPA-252: projection columns must be included so that project_three_var_row
        // can resolve src-variable columns (e.g. `RETURN a.name` when src_var = "a").
        let col_ids_src_where: Vec<u32> = {
            let mut ids =
                collect_col_ids_for_var_from_items(&src_node_pat.var, &m.return_clause.items);
            if let Some(ref where_expr) = m.where_clause {
                collect_col_ids_from_expr_for_var(where_expr, &src_node_pat.var, &mut ids);
            }
            ids
        };

        // SPA-201: detect if the second relationship hop is Incoming FIRST,
        // because col_ids_mid is only populated for the incoming case.
        // For patterns like (a)-[:R]->(m)<-[:R]-(b), rels[1].dir == Incoming,
        // meaning we need the PREDECESSORS of mid (nodes that have an edge TO mid)
        // rather than the SUCCESSORS (forward neighbors of mid).
        let second_hop_incoming = pat
            .rels
            .get(1)
            .map(|r| r.dir == sparrowdb_cypher::ast::EdgeDir::Incoming)
            .unwrap_or(false);

        // SPA-201: collect col_ids for the mid node (nodes[1] = m in Q8).
        // For the Incoming second-hop case the mid is the projected "common neighbor"
        // (e.g. `RETURN m.uid`), so we must read its properties.
        let mid_label = mid_node_pat.labels.first().cloned().unwrap_or_default();
        let mid_label_id: u32 = if mid_label.is_empty() {
            src_label_id // fall back to src label when mid has no label annotation
        } else {
            self.snapshot
                .catalog
                .get_label(&mid_label)
                .ok()
                .flatten()
                .map(|id| id as u32)
                .unwrap_or(src_label_id)
        };
        // SPA-241: collect col_ids for the mid node for BOTH forward-forward
        // and incoming patterns.  Previously this was only populated for the
        // incoming case, leaving mid node properties unresolvable in the
        // forward-forward path (a)-[:R]->(m)-[:R]->(b).
        let col_ids_mid: Vec<u32> = if !mid_node_pat.var.is_empty() {
            let mut ids =
                collect_col_ids_for_var_from_items(&mid_node_pat.var, &m.return_clause.items);
            for p in &mid_node_pat.props {
                let col_id = prop_name_to_col_id(&p.key);
                if !ids.contains(&col_id) {
                    ids.push(col_id);
                }
            }
            if let Some(ref where_expr) = m.where_clause {
                collect_col_ids_from_expr_for_var(where_expr, &mid_node_pat.var, &mut ids);
            }
            ids
        } else {
            vec![]
        };

        // SPA-163 + SPA-185: build a slot-level adjacency map from all delta
        // logs so that edges written since the last checkpoint are visible for
        // 2-hop queries.  We aggregate across all rel types here because the
        // 2-hop executor does not currently filter on rel_type.
        // Map: src_slot → Vec<dst_slot> (only records whose src label matches).
        // SPA-263: per-hop CSRs and delta adjacency maps.
        let rel1 = &pat.rels[0];
        let rel2 = &pat.rels[1];
        // #487: the first hop's direction was never read anywhere in this
        // function — only the second hop's (`second_hop_incoming` below, the
        // SPA-201/#294 fix) was. A left-pointing first hop (`(a)<-[:R]-(m)`)
        // was silently treated as if it had been written `(a)-[:R]->(m)`,
        // the same failure mode #486 fixed in `execute_one_hop`.
        let first_hop_incoming = rel1.dir == sparrowdb_cypher::ast::EdgeDir::Incoming;
        let all_rel_tables_2hop = self.snapshot.catalog.list_rel_tables_with_ids();
        let hop1_rel_ids: Vec<u64> = all_rel_tables_2hop
            .iter()
            .filter(|(_, sid, did, rt)| {
                let type_ok = rel1.rel_type.is_empty() || rt == &rel1.rel_type;
                if first_hop_incoming {
                    // (src)<-[:R]-(mid): the physical edge runs mid -> src,
                    // so the catalog table is keyed (src=mid, dst=src).
                    let src_ok = *sid as u32 == mid_label_id;
                    let dst_ok = *did as u32 == src_label_id;
                    type_ok && src_ok && dst_ok
                } else {
                    let src_ok = *sid as u32 == src_label_id;
                    let dst_ok = *did as u32 == mid_label_id;
                    type_ok && src_ok && dst_ok
                }
            })
            .map(|(id, _, _, _)| *id)
            .collect();
        // #294: when the second hop is Incoming, the pattern is
        // (mid)<-[:R]-(fof), meaning edges are stored as (fof)-[:R]->(mid)
        // in the catalog (src=fof_label, dst=mid_label).  We must swap the
        // label filter so we actually find matching rel tables.
        let hop2_rel_ids: Vec<u64> = all_rel_tables_2hop
            .iter()
            .filter(|(_, sid, did, rt)| {
                let type_ok = rel2.rel_type.is_empty() || rt == &rel2.rel_type;
                if second_hop_incoming {
                    let src_ok = *sid as u32 == fof_label_id;
                    let dst_ok = *did as u32 == mid_label_id;
                    type_ok && src_ok && dst_ok
                } else {
                    let src_ok = *sid as u32 == mid_label_id;
                    let dst_ok = *did as u32 == fof_label_id;
                    type_ok && src_ok && dst_ok
                }
            })
            .map(|(id, _, _, _)| *id)
            .collect();
        let hop1_csr = {
            let mut max_n: u64 = 0;
            let mut edges: Vec<(u64, u64)> = Vec::new();
            for &rid in &hop1_rel_ids {
                if let Some(csr) = self.snapshot.csrs.get(&(rid as u32)) {
                    if csr.n_nodes() > max_n {
                        max_n = csr.n_nodes();
                    }
                    for s in 0..csr.n_nodes() {
                        for &d in csr.neighbors(s) {
                            edges.push((s, d));
                        }
                    }
                }
            }
            edges.sort_unstable();
            edges.dedup();
            CsrForward::build(max_n, &edges)
        };
        let hop2_csr = {
            let mut max_n: u64 = 0;
            let mut edges: Vec<(u64, u64)> = Vec::new();
            for &rid in &hop2_rel_ids {
                if let Some(csr) = self.snapshot.csrs.get(&(rid as u32)) {
                    if csr.n_nodes() > max_n {
                        max_n = csr.n_nodes();
                    }
                    for s in 0..csr.n_nodes() {
                        for &d in csr.neighbors(s) {
                            edges.push((s, d));
                        }
                    }
                }
            }
            edges.sort_unstable();
            edges.dedup();
            CsrForward::build(max_n, &edges)
        };
        let mut delta_adj_hop1: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut delta_adj_hop2: HashMap<u64, Vec<u64>> = HashMap::new();
        for &rid in &hop1_rel_ids {
            for r in self.read_delta_for(rid as u32) {
                let ss = r.src.0 & 0xFFFF_FFFF;
                let ds = r.dst.0 & 0xFFFF_FFFF;
                let e = delta_adj_hop1.entry(ss).or_default();
                if !e.contains(&ds) {
                    e.push(ds);
                }
            }
        }
        for &rid in &hop2_rel_ids {
            for r in self.read_delta_for(rid as u32) {
                let ss = r.src.0 & 0xFFFF_FFFF;
                let ds = r.dst.0 & 0xFFFF_FFFF;
                let e = delta_adj_hop2.entry(ss).or_default();
                if !e.contains(&ds) {
                    e.push(ds);
                }
            }
        }

        // SPA-185: build a merged CSR that union-combines edges from all
        // per-type CSRs so the 2-hop traversal sees paths through any rel type.
        // AspJoin requires a single &CsrForward; we construct a combined one
        // rather than using an arbitrary first entry.
        // (merged_csr removed by SPA-263)

        // SPA-201: build a merged backward CSR when the second hop is Incoming.
        // For (a)-[:R]->(m)<-[:R]-(b) we need predecessors(mid) to find b-nodes.
        // We derive this from the already-loaded forward CSRs (no extra disk I/O)
        // by building CsrBackward from the same .
        // CsrBackward::build takes (src, dst) forward edges and stores them reversed.
        let merged_bwd_csr: Option<CsrBackward> = if second_hop_incoming {
            let mut max_nodes: u64 = 0;
            //
            //
            //
            let mut fwd_edges: Vec<(u64, u64)> = Vec::new();
            for &rid in &hop2_rel_ids {
                if let Some(csr) = self.snapshot.csrs.get(&(rid as u32)) {
                    if csr.n_nodes() > max_nodes {
                        max_nodes = csr.n_nodes();
                    }
                    for src in 0..csr.n_nodes() {
                        for &dst in csr.neighbors(src) {
                            fwd_edges.push((src, dst));
                        }
                    }
                }
            }
            fwd_edges.sort_unstable();
            fwd_edges.dedup();
            if fwd_edges.is_empty() {
                None
            } else {
                Some(CsrBackward::build(max_nodes, &fwd_edges))
            }
        } else {
            None
        };

        // SPA-201: build a delta adjacency map for the backward (incoming) direction.
        // Maps dst_slot → Vec<src_slot> for edges in the delta log (written since checkpoint).
        let delta_adj_bwd: HashMap<u64, Vec<u64>> = if second_hop_incoming {
            let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
            for &rid in &hop2_rel_ids {
                for r in self.read_delta_for(rid as u32) {
                    let ds = r.dst.0 & 0xFFFF_FFFF;
                    let ss = r.src.0 & 0xFFFF_FFFF;
                    adj.entry(ds).or_default().push(ss);
                }
            }
            adj
        } else {
            HashMap::new()
        };

        // #487: mirror of `merged_bwd_csr` / `delta_adj_bwd` above, but for
        // the FIRST hop. For `(src)<-[:R]-(mid)` the physical edge runs
        // mid -> src, so finding mid-candidates for a given src_slot means
        // walking `hop1_csr` (built from the swapped `hop1_rel_ids` above,
        // so it already holds the mid -> src physical edges) *backward*.
        let hop1_bwd_csr: Option<CsrBackward> = if first_hop_incoming {
            let mut max_nodes: u64 = 0;
            let mut fwd_edges: Vec<(u64, u64)> = Vec::new();
            for &rid in &hop1_rel_ids {
                if let Some(csr) = self.snapshot.csrs.get(&(rid as u32)) {
                    if csr.n_nodes() > max_nodes {
                        max_nodes = csr.n_nodes();
                    }
                    for src in 0..csr.n_nodes() {
                        for &dst in csr.neighbors(src) {
                            fwd_edges.push((src, dst));
                        }
                    }
                }
            }
            fwd_edges.sort_unstable();
            fwd_edges.dedup();
            if fwd_edges.is_empty() {
                None
            } else {
                Some(CsrBackward::build(max_nodes, &fwd_edges))
            }
        } else {
            None
        };
        let delta_adj_hop1_bwd: HashMap<u64, Vec<u64>> = if first_hop_incoming {
            let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
            for &rid in &hop1_rel_ids {
                for r in self.read_delta_for(rid as u32) {
                    let ds = r.dst.0 & 0xFFFF_FFFF;
                    let ss = r.src.0 & 0xFFFF_FFFF;
                    adj.entry(ds).or_default().push(ss);
                }
            }
            adj
        } else {
            HashMap::new()
        };
        // Candidate mid-slots for a given src_slot, respecting hop1's
        // direction: forward CSR/delta neighbours when Outgoing, backward
        // CSR/delta predecessors when Incoming.
        let hop1_mid_candidates = |src_slot: u64| -> Vec<u64> {
            if first_hop_incoming {
                let mut v: Vec<u64> = hop1_bwd_csr
                    .as_ref()
                    .map(|b| b.predecessors(src_slot).to_vec())
                    .unwrap_or_default();
                if let Some(delta_preds) = delta_adj_hop1_bwd.get(&src_slot) {
                    for &mid in delta_preds {
                        if !v.contains(&mid) {
                            v.push(mid);
                        }
                    }
                }
                v
            } else {
                let mut v: Vec<u64> = hop1_csr.neighbors(src_slot).to_vec();
                if let Some(df) = delta_adj_hop1.get(&src_slot) {
                    for &mid in df {
                        if !v.contains(&mid) {
                            v.push(mid);
                        }
                    }
                }
                v
            }
        };

        let mut rows = Vec::new();
        // SPA-263: detect aggregates early so we can build proper HashMap rows
        // instead of projecting through project_three_var_row (which returns Null
        // for aggregate columns like COUNT(*)).
        let use_agg = has_aggregate_in_return(&m.return_clause.items);
        let mut raw_rows: Vec<HashMap<String, Value>> = Vec::new();

        // ── #287: HashSet intersection for mutual neighbor queries ────────────
        //
        // For the incoming second-hop pattern (a)-[:R]->(m)<-[:R]-(b), the naive
        // algorithm iterates all predecessors of each mid-node M, which is
        // O(|neighbors(a)| × avg_predecessor_degree).  When both endpoints have
        // inline property filters (the typical Q8 mutual-friends case), we can
        // pre-identify qualifying b-nodes and collect their forward neighbors
        // into HashSets.  Finding mutual mid-nodes then becomes a set
        // intersection in O(min(deg_a, deg_b)) per (a, b) pair.
        //
        // Pre-compute: for each qualifying b-slot, its forward neighbor set via
        // hop2 (since b→m means m ∈ hop2_fwd_neighbors(b)).
        let b_neighbor_sets: Option<Vec<BNeighborEntry>> =
            if second_hop_incoming && !fof_node_pat.props.is_empty() {
                let hwm_fof = self.snapshot.store.hwm_for_label(fof_label_id)?;
                let mut sets: Vec<BNeighborEntry> = Vec::new();
                for b_slot in 0..hwm_fof {
                    let b_node = NodeId(((fof_label_id as u64) << 32) | b_slot);
                    let b_props = read_node_props(&self.snapshot.store, b_node, &col_ids_fof)?;
                    if !self.matches_prop_filter(&b_props, &fof_node_pat.props) {
                        continue;
                    }
                    // Collect forward neighbors of b via hop2 rel tables.
                    let mut nbrs: HashSet<u64> = HashSet::new();
                    for &n in hop2_csr.neighbors(b_slot) {
                        nbrs.insert(n);
                    }
                    if let Some(delta) = delta_adj_hop2.get(&b_slot) {
                        for &n in delta {
                            nbrs.insert(n);
                        }
                    }
                    if !nbrs.is_empty() {
                        sets.push((b_slot, nbrs, b_props));
                    }
                }
                Some(sets)
            } else {
                None
            };

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

            if second_hop_incoming {
                // SPA-201: Incoming second hop — pattern (a)-[:R]->(m)<-[:R]-(b).
                //
                // Semantics: find all mid-nodes M such that (a→M) AND (b→M) where
                // b matches the fof_node_pat filter.  The result rows project M
                // (the common neighbor / mutual friend), not B.

                // Collect all candidate M slots from the first hop, respecting
                // its direction (#487 — this used to always assume Outgoing).
                let neighbors_a: HashSet<u64> = hop1_mid_candidates(src_slot).into_iter().collect();

                // ── #287 fast path: HashSet intersection ──────────────────────
                if let Some(ref b_sets) = b_neighbor_sets {
                    for (b_slot, neighbors_b, b_props) in b_sets {
                        // Intersect neighbors_a ∩ neighbors_b to find mutual
                        // mid-nodes.  Iterate the smaller set for O(min) time.
                        let mutual_mids: Vec<u64> = if neighbors_a.len() <= neighbors_b.len() {
                            neighbors_a
                                .iter()
                                .filter(|m| neighbors_b.contains(m))
                                .copied()
                                .collect()
                        } else {
                            neighbors_b
                                .iter()
                                .filter(|m| neighbors_a.contains(m))
                                .copied()
                                .collect()
                        };

                        let b_node = NodeId(((fof_label_id as u64) << 32) | *b_slot);

                        for mid_slot in mutual_mids {
                            let mid_node = NodeId(((mid_label_id as u64) << 32) | mid_slot);
                            let mid_props = if !col_ids_mid.is_empty() {
                                read_node_props(&self.snapshot.store, mid_node, &col_ids_mid)?
                            } else {
                                vec![]
                            };

                            // Apply mid inline prop filter.
                            if !self.matches_prop_filter(&mid_props, &mid_node_pat.props) {
                                continue;
                            }

                            // Apply WHERE clause.
                            if let Some(ref where_expr) = m.where_clause {
                                let mut row_vals = build_row_vals(
                                    &src_props,
                                    &src_node_pat.var,
                                    &col_ids_src_where,
                                    &self.snapshot.store,
                                );
                                row_vals.extend(build_row_vals(
                                    &mid_props,
                                    &mid_node_pat.var,
                                    &col_ids_mid,
                                    &self.snapshot.store,
                                ));
                                row_vals.extend(build_row_vals(
                                    b_props,
                                    &fof_node_pat.var,
                                    &col_ids_fof,
                                    &self.snapshot.store,
                                ));
                                // SPA-200: inject full label set (primary + secondary).
                                if !src_node_pat.var.is_empty() {
                                    row_vals.insert(
                                        format!("{}.__labels__", src_node_pat.var),
                                        self.labels_value_for_node(src_node),
                                    );
                                }
                                if !mid_node_pat.var.is_empty() {
                                    row_vals.insert(
                                        format!("{}.__labels__", mid_node_pat.var),
                                        self.labels_value_for_node(mid_node),
                                    );
                                }
                                if !fof_node_pat.var.is_empty() {
                                    row_vals.insert(
                                        format!("{}.__labels__", fof_node_pat.var),
                                        self.labels_value_for_node(b_node),
                                    );
                                }
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

                            if use_agg {
                                let mut row_vals = build_row_vals(
                                    &src_props,
                                    &src_node_pat.var,
                                    &col_ids_src_where,
                                    &self.snapshot.store,
                                );
                                row_vals.extend(build_row_vals(
                                    &mid_props,
                                    &mid_node_pat.var,
                                    &col_ids_mid,
                                    &self.snapshot.store,
                                ));
                                row_vals.extend(build_row_vals(
                                    b_props,
                                    &fof_node_pat.var,
                                    &col_ids_fof,
                                    &self.snapshot.store,
                                ));
                                if !src_node_pat.var.is_empty() {
                                    row_vals
                                        .insert(src_node_pat.var.clone(), Value::NodeRef(src_node));
                                }
                                if !mid_node_pat.var.is_empty() {
                                    row_vals
                                        .insert(mid_node_pat.var.clone(), Value::NodeRef(mid_node));
                                }
                                if !fof_node_pat.var.is_empty() {
                                    row_vals
                                        .insert(fof_node_pat.var.clone(), Value::NodeRef(b_node));
                                }
                                raw_rows.push(row_vals);
                            } else {
                                let row = project_three_var_row(
                                    &src_props,
                                    &mid_props,
                                    b_props,
                                    &m.return_clause.items,
                                    &src_node_pat.var,
                                    &mid_node_pat.var,
                                    &self.snapshot.store,
                                );
                                rows.push(row);
                            }
                        }
                    }
                    continue; // Skip old path for this src_slot.
                }

                // ── Fallback: original predecessor-scan path (no b-side filter) ──
                //
                // Algorithm:
                //   1. First-hop forward: candidate M slots = CSR neighbors of src + delta.
                //   2. For each M: collect B slots = predecessors of M (bwd CSR + delta).
                //   3. Read B props, apply fof_node_pat filter — if any B passes, M is valid.
                //   4. For valid M: read mid props, apply mid prop filter, build result row.

                let mid_slots: Vec<u64> = neighbors_a.into_iter().collect();

                for mid_slot in mid_slots {
                    // Read mid props for projection (and mid prop filter).
                    let mid_node = NodeId(((mid_label_id as u64) << 32) | mid_slot);
                    let mid_props = if !col_ids_mid.is_empty() {
                        read_node_props(&self.snapshot.store, mid_node, &col_ids_mid)?
                    } else {
                        vec![]
                    };

                    // Apply mid inline prop filter (e.g. `m:User`).
                    if !self.matches_prop_filter(&mid_props, &mid_node_pat.props) {
                        continue;
                    }

                    // Collect B slots = predecessors of M (bwd CSR + delta bwd).
                    let mut found_valid_fof = false;
                    let csr_preds: &[u64] = merged_bwd_csr
                        .as_ref()
                        .map(|bwd| bwd.predecessors(mid_slot))
                        .unwrap_or(&[]);
                    let delta_preds_opt = delta_adj_bwd.get(&mid_slot);

                    let all_b_slots: Vec<u64> = {
                        let mut v: Vec<u64> = csr_preds.to_vec();
                        if let Some(delta_preds) = delta_preds_opt {
                            for &b in delta_preds {
                                if !v.contains(&b) {
                                    v.push(b);
                                }
                            }
                        }
                        v
                    };

                    for b_slot in &all_b_slots {
                        let b_node = NodeId(((fof_label_id as u64) << 32) | *b_slot);
                        let b_props = read_node_props(&self.snapshot.store, b_node, &col_ids_fof)?;

                        // Apply fof (b) inline prop filter.
                        if !self.matches_prop_filter(&b_props, &fof_node_pat.props) {
                            continue;
                        }

                        // Apply WHERE clause for this (src=a, mid=m, fof=b) binding.
                        if let Some(ref where_expr) = m.where_clause {
                            let mut row_vals = build_row_vals(
                                &src_props,
                                &src_node_pat.var,
                                &col_ids_src_where,
                                &self.snapshot.store,
                            );
                            row_vals.extend(build_row_vals(
                                &mid_props,
                                &mid_node_pat.var,
                                &col_ids_mid,
                                &self.snapshot.store,
                            ));
                            row_vals.extend(build_row_vals(
                                &b_props,
                                &fof_node_pat.var,
                                &col_ids_fof,
                                &self.snapshot.store,
                            ));
                            // SPA-200: inject full label set (primary + secondary).
                            if !src_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", src_node_pat.var),
                                    self.labels_value_for_node(src_node),
                                );
                            }
                            if !mid_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", mid_node_pat.var),
                                    self.labels_value_for_node(mid_node),
                                );
                            }
                            if !fof_node_pat.var.is_empty() {
                                row_vals.insert(
                                    format!("{}.__labels__", fof_node_pat.var),
                                    self.labels_value_for_node(b_node),
                                );
                            }
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

                        // SPA-263: when aggregates are present, build a HashMap
                        // row with node refs (needed for COUNT(var), etc.) instead
                        // of projecting through project_three_var_row which returns
                        // Null for non-property columns.
                        if use_agg {
                            let mut row_vals = build_row_vals(
                                &src_props,
                                &src_node_pat.var,
                                &col_ids_src_where,
                                &self.snapshot.store,
                            );
                            row_vals.extend(build_row_vals(
                                &mid_props,
                                &mid_node_pat.var,
                                &col_ids_mid,
                                &self.snapshot.store,
                            ));
                            row_vals.extend(build_row_vals(
                                &b_props,
                                &fof_node_pat.var,
                                &col_ids_fof,
                                &self.snapshot.store,
                            ));
                            // Bind node refs so COUNT(var) resolves as non-null.
                            if !src_node_pat.var.is_empty() {
                                row_vals.insert(src_node_pat.var.clone(), Value::NodeRef(src_node));
                            }
                            if !mid_node_pat.var.is_empty() {
                                row_vals.insert(mid_node_pat.var.clone(), Value::NodeRef(mid_node));
                            }
                            if !fof_node_pat.var.is_empty() {
                                row_vals.insert(fof_node_pat.var.clone(), Value::NodeRef(b_node));
                            }
                            raw_rows.push(row_vals);
                        } else {
                            // Project a row: src (a) + mid (m) + fof (b) columns.
                            let row = project_three_var_row(
                                &src_props,
                                &mid_props,
                                &b_props,
                                &m.return_clause.items,
                                &src_node_pat.var,
                                &mid_node_pat.var,
                                &self.snapshot.store,
                            );
                            rows.push(row);
                        }
                        found_valid_fof = true;
                        // Continue — multiple b nodes may match (emit one row per match).
                    }
                    let _ = found_valid_fof; // suppress unused warning
                }
                // Skip the rest of the per-src-slot processing for the Incoming case.
                continue;
            }

            // ── src->mid (either direction) then mid->fof forward ─────────────
            // SPA-241: use factorized join to preserve mid_slot→fof_slots mapping.
            // The previous flat two_hop() call discarded which mid node connected
            // src to each fof, making it impossible to read or return mid properties.
            // #487: this branch runs whenever the second hop is Outgoing; the
            // first hop's direction is resolved via `hop1_mid_candidates`
            // rather than assumed Outgoing.
            let mut mid_fof_pairs: Vec<(u64, Vec<u64>)> = Vec::new();
            {
                let mid_slots: Vec<u64> = hop1_mid_candidates(src_slot);
                for mid_slot in mid_slots {
                    let mut fof_set: HashSet<u64> = HashSet::new();
                    for &f in hop2_csr.neighbors(mid_slot) {
                        fof_set.insert(f);
                    }
                    if let Some(d2) = delta_adj_hop2.get(&mid_slot) {
                        for &f in d2 {
                            fof_set.insert(f);
                        }
                    }
                    if !fof_set.is_empty() {
                        let mut fv: Vec<u64> = fof_set.into_iter().collect();
                        fv.sort_unstable();
                        mid_fof_pairs.push((mid_slot, fv));
                    }
                }
            }

            // Collect all unique fof slots for batch property reads (SPA-200).
            let all_fof_slots: Vec<u32> = {
                let mut seen: HashSet<u64> = HashSet::new();
                let mut v: Vec<u32> = Vec::new();
                for (_, fof_slots) in &mid_fof_pairs {
                    for &fof in fof_slots {
                        if seen.insert(fof) {
                            v.push(fof as u32);
                        }
                    }
                }
                v.sort_unstable();
                v
            };

            // Batch-read fof properties once per src_slot.
            let fof_batch: Vec<Vec<u64>> = if !col_ids_fof.is_empty() {
                self.snapshot.store.batch_read_node_props(
                    fof_label_id,
                    &all_fof_slots,
                    &col_ids_fof,
                )?
            } else {
                vec![]
            };
            let fof_slot_to_idx: HashMap<u64, usize> = all_fof_slots
                .iter()
                .enumerate()
                .map(|(i, &s)| (s as u64, i))
                .collect();

            for (mid_slot, fof_slots) in mid_fof_pairs {
                // SPA-241: read mid node properties for forward-forward path.
                let mid_node = NodeId(((mid_label_id as u64) << 32) | mid_slot);
                let mid_props: Vec<(u32, u64)> = if !col_ids_mid.is_empty() {
                    read_node_props(&self.snapshot.store, mid_node, &col_ids_mid)?
                } else {
                    vec![]
                };

                // Apply mid inline prop filter.
                if !self.matches_prop_filter(&mid_props, &mid_node_pat.props) {
                    continue;
                }

                for fof_slot in fof_slots {
                    let fof_node = NodeId(((fof_label_id as u64) << 32) | fof_slot);
                    // Build fof_props from batch or fallback individual read.
                    let fof_props: Vec<(u32, u64)> = if !col_ids_fof.is_empty() {
                        if let Some(&idx) = fof_slot_to_idx.get(&fof_slot) {
                            col_ids_fof
                                .iter()
                                .copied()
                                .zip(fof_batch[idx].iter().copied())
                                .filter(|&(_, v)| v != 0)
                                .collect()
                        } else {
                            // Fallback: individual read (delta-only slot not in batch).
                            read_node_props(&self.snapshot.store, fof_node, &col_ids_fof)?
                        }
                    } else {
                        vec![]
                    };

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
                            &mid_props,
                            &mid_node_pat.var,
                            &col_ids_mid,
                            &self.snapshot.store,
                        ));
                        row_vals.extend(build_row_vals(
                            &fof_props,
                            &fof_node_pat.var,
                            &col_ids_fof,
                            &self.snapshot.store,
                        ));
                        // SPA-200: inject full label set (primary + secondary).
                        if !src_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", src_node_pat.var),
                                self.labels_value_for_node(src_node),
                            );
                        }
                        if !mid_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", mid_node_pat.var),
                                self.labels_value_for_node(mid_node),
                            );
                        }
                        if !fof_node_pat.var.is_empty() {
                            row_vals.insert(
                                format!("{}.__labels__", fof_node_pat.var),
                                self.labels_value_for_node(fof_node),
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

                    // SPA-263: when aggregates are present, build a HashMap
                    // row with node refs instead of projecting.
                    if use_agg {
                        let mut row_vals = build_row_vals(
                            &src_props,
                            &src_node_pat.var,
                            &col_ids_src_where,
                            &self.snapshot.store,
                        );
                        row_vals.extend(build_row_vals(
                            &mid_props,
                            &mid_node_pat.var,
                            &col_ids_mid,
                            &self.snapshot.store,
                        ));
                        row_vals.extend(build_row_vals(
                            &fof_props,
                            &fof_node_pat.var,
                            &col_ids_fof,
                            &self.snapshot.store,
                        ));
                        // Bind node refs so COUNT(var) resolves as non-null.
                        if !src_node_pat.var.is_empty() {
                            row_vals.insert(src_node_pat.var.clone(), Value::NodeRef(src_node));
                        }
                        if !mid_node_pat.var.is_empty() {
                            row_vals.insert(mid_node_pat.var.clone(), Value::NodeRef(mid_node));
                        }
                        if !fof_node_pat.var.is_empty() {
                            row_vals.insert(fof_node_pat.var.clone(), Value::NodeRef(fof_node));
                        }
                        raw_rows.push(row_vals);
                    } else {
                        // SPA-241: use three-var projection so mid variable columns
                        // are resolved from mid_props rather than defaulting to fof_props.
                        let row = project_three_var_row(
                            &src_props,
                            &mid_props,
                            &fof_props,
                            &m.return_clause.items,
                            &src_node_pat.var,
                            &mid_node_pat.var,
                            &self.snapshot.store,
                        );
                        rows.push(row);
                    }
                }
            }
        }

        // SPA-263: apply aggregation using pre-built raw_rows (with node refs).
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
    ///
    /// # Variable-length hops (#421)
    ///
    /// Any hop in the chain may carry a variable-length quantifier
    /// (`-[:R*1..2]->`).  Such a hop expands the frontier with
    /// [`Engine::execute_variable_hops`] — the same DFS/BFS used by the
    /// standalone `execute_variable_length` path — instead of taking a single
    /// step, so `(a)-[:R*1..2]->(b)-[:S]->(c)` binds `b` to every node reachable
    /// from `a` in 1..2 hops and then joins each of those against the trailing
    /// `:S` hop.  Before this, `is_var_len` in `execute_match` only recognised a
    /// quantifier when the varlen relationship was the *only* relationship in
    /// the pattern, so a trailing hop silently demoted `*1..2` to a plain single
    /// hop and every depth >= 2 match was dropped without an error.
    pub(crate) fn execute_n_hop(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
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

        // ── #421: variable-length quantifiers anywhere in the chain ──────────
        //
        // `Some((min, max))` for a hop written `-[:R*min..max]->`.  An open
        // upper bound is capped at 10, matching `execute_variable_length`.
        let varlen_per_hop: Vec<Option<(u32, u32)>> = pat
            .rels
            .iter()
            .map(|r| {
                if r.min_hops.is_some() || r.max_hops.is_some() {
                    Some((r.min_hops.unwrap_or(1), r.max_hops.unwrap_or(10)))
                } else {
                    None
                }
            })
            .collect();
        let has_varlen = varlen_per_hop.iter().any(Option::is_some);

        // Pre-resolve per-hop rel-table IDs so the inner loop uses filtered
        // CSR lookups instead of scanning every relation type (SPA-284).
        //
        // #429: a relationship *type* can back several rel tables — LDBC's
        // `isLocatedIn` exists as both `(Person)->(Place)` and
        // `(Organisation)->(Place)`.  CSRs are keyed by rel-table id and indexed
        // by *source slot*, so unioning every table of a given type makes
        // `Person` slot 2 inherit `Organisation` slot 2's edges.  (Concretely:
        // Carol White, Person slot 2, appeared to live in Germany because
        // TechStart Inc is Organisation slot 2.)  When both endpoint labels of a
        // fixed hop are declared, narrow to the single rel table for that triple
        // — the same discrimination `resolve_rel_table_id` already gives the
        // one- and two-hop executors.
        //
        // Two cases stay exposed and are tracked in #429: hops whose endpoint
        // labels are not both declared, and variable-length hops, whose
        // intermediate nodes are not constrained by the endpoint labels and so
        // must keep the type-wide set.
        let rel_ids_per_hop: Vec<Vec<u32>> = (0..n_rels)
            .map(|i| {
                let type_wide = self.resolve_rel_ids_for_type(&pat.rels[i].rel_type);
                if varlen_per_hop[i].is_some() || type_wide.len() < 2 {
                    return type_wide;
                }
                match (label_ids_per_node[i], label_ids_per_node[i + 1]) {
                    (Some(pattern_src), Some(pattern_dst)) => {
                        // #487/#488: the catalog keys a rel table by the
                        // edge's PHYSICAL (src, dst), not by pattern
                        // position. For an Incoming hop (`<-`) the pattern's
                        // right-hand node is the physical source, so the
                        // lookup must be swapped or it silently resolves to
                        // no table whenever the two labels differ and more
                        // than one table shares this rel type (#429).
                        let incoming = pat.rels[i].dir == sparrowdb_cypher::ast::EdgeDir::Incoming;
                        let (phys_src, phys_dst) = if incoming {
                            (pattern_dst, pattern_src)
                        } else {
                            (pattern_src, pattern_dst)
                        };
                        self.snapshot
                            .catalog
                            .get_rel_table(phys_src as u16, phys_dst as u16, &pat.rels[i].rel_type)
                            .ok()
                            .flatten()
                            .map(|id| vec![id as u32])
                            // No table for this (src, dst, type) triple: the hop
                            // cannot match anything.
                            .unwrap_or_default()
                    }
                    _ => type_wide,
                }
            })
            .collect();
        // If any hop specifies a rel type that doesn't exist in the catalog, no
        // traversal can produce results — return empty immediately.
        for (i, rel_ids) in rel_ids_per_hop.iter().enumerate() {
            if !pat.rels[i].rel_type.is_empty() && rel_ids.is_empty() {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        }

        // `execute_variable_hops` only walks outgoing edges.  Reject rather than
        // quietly answering an `<-[:R*1..2]-` pattern with the wrong direction.
        for (i, v) in varlen_per_hop.iter().enumerate() {
            if v.is_some() && pat.rels[i].dir != sparrowdb_cypher::ast::EdgeDir::Outgoing {
                return Err(sparrowdb_common::Error::Unimplemented);
            }
        }

        // Delta index used by the variable-length expansion. Built once (not
        // per source node) and only when a varlen hop exists.
        //
        // #431: neighbour labels for the varlen expansion are no longer read
        // from a registry built here — `execute_variable_hops` resolves each
        // neighbour's label straight from the catalog/CSR or the delta
        // record's own NodeId (`get_node_neighbors_labeled`), so there is
        // nothing left for this function to precompute or hand it.
        let delta_idx = if has_varlen {
            build_delta_index(&delta_all)
        } else {
            DeltaIndex::new()
        };
        // Reusable neighbor buffer for the variable-length traversal.
        let mut neighbors_buf: HashSet<(u64, u32)> = HashSet::new();

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

            // `frontier` holds (slot, label_id, accumulated_vals) triples for the
            // current boundary of the traversal.  Each entry represents one
            // in-progress path; cloning ensures bindings are isolated across
            // branches.
            //
            // #429: the label is carried, not re-derived.  It used to be taken
            // from the *pattern* (`label_ids_per_node[hop_idx]`, falling back to
            // the source's label when the position was unlabeled), so an
            // unlabeled intermediate node was addressed as label 0 and every hop
            // probed CSRs with a bare slot.  With `isLocatedIn` registered for
            // both (Person)->(Place) and (Organisation)->(Place), that let
            // Person slot 2 inherit Organisation slot 2's edges.
            let mut frontier: Vec<(u64, u32, HashMap<String, Value>)> =
                vec![(src_slot, src_label_id, row_vals)];

            for hop_idx in 0..n_rels {
                let next_node_pat = &pat.nodes[hop_idx + 1];
                let next_label_id_opt = label_ids_per_node[hop_idx + 1];
                let next_col_ids = &col_ids_per_node[hop_idx + 1];

                let mut next_frontier: Vec<(u64, u32, HashMap<String, Value>)> = Vec::new();

                for (cur_slot, cur_label_id, cur_vals) in frontier {
                    let hop_rel_ids = &rel_ids_per_hop[hop_idx];

                    // `(slot, resolved_label_id)` candidates for this hop, each
                    // carrying the neighbour's *actual* label — read from the
                    // catalog/CSR or the stored NodeId, never guessed from the
                    // pattern or assumed from the source's label. A bare slot
                    // is not a safe node identity: see #427, #429, #431.
                    let all_nb: Vec<(u64, u32)> = match varlen_per_hop[hop_idx] {
                        // ── #421: variable-length hop ────────────────────────
                        Some((min_hops, max_hops)) => {
                            // Per-path enumeration is only collapsed to
                            // reachability when the query is DISTINCT and no
                            // relationship variable is bound — same rule as
                            // `execute_variable_length` (issue #165).
                            let use_reachability = m.distinct && pat.rels[hop_idx].var.is_empty();
                            let reached = self.execute_variable_hops(
                                cur_slot,
                                cur_label_id,
                                min_hops,
                                max_hops,
                                &delta_idx,
                                &mut neighbors_buf,
                                use_reachability,
                                usize::MAX,
                                hop_rel_ids,
                            );
                            reached
                                .into_iter()
                                .filter_map(|(slot, discovered_label)| {
                                    match next_label_id_opt {
                                        // A label on the pattern is a filter:
                                        // drop nodes whose recovered label differs.
                                        Some(required) if discovered_label != required => None,
                                        Some(required) => Some((slot, required)),
                                        None => Some((slot, discovered_label)),
                                    }
                                })
                                .collect()
                        }
                        // ── fixed single hop ─────────────────────────────────
                        None => {
                            // Gather neighbours from CSR + delta for this hop.
                            // Both sources report the neighbour's own label:
                            // the catalog's label for checkpointed edges, the
                            // stored NodeId for delta edges.
                            // SPA-284: `rel_ids` restricts the lookup to the requested types.
                            // `seen` deduplicates on the full identity
                            // `(slot, label)`; `nb` keeps insertion order so
                            // row order stays deterministic for queries with
                            // no ORDER BY.
                            //
                            // #487/#488: a left-pointing hop (`<-`) means
                            // `cur_slot` is the PHYSICAL destination, not the
                            // source — walking the forward CSR/delta from it
                            // silently answered the outbound question
                            // instead of the inbound one asked. Mirror
                            // `execute_one_hop`'s fix by branching on the
                            // hop's direction rather than assuming Outgoing.
                            let incoming =
                                pat.rels[hop_idx].dir == sparrowdb_cypher::ast::EdgeDir::Incoming;
                            let mut seen: HashSet<(u64, u32)> = HashSet::new();
                            let mut nb: Vec<(u64, u32)> = if incoming {
                                self.csr_predecessors_labeled(cur_slot, cur_label_id, hop_rel_ids)
                            } else {
                                self.csr_neighbors_labeled(cur_slot, cur_label_id, hop_rel_ids)
                            }
                            .into_iter()
                            .filter(|nb| seen.insert(*nb))
                            .collect();
                            for r in delta_all.iter() {
                                // Filter by relation-table IDs when a type constraint exists.
                                if !hop_rel_ids.is_empty() && !hop_rel_ids.contains(&r.rel_id.0) {
                                    continue;
                                }
                                if incoming {
                                    // cur is the edge's destination; the
                                    // neighbour is its physical source.
                                    let (r_dst_label, r_dst_slot) = node_id_parts(r.dst.0);
                                    if r_dst_label != cur_label_id || r_dst_slot != cur_slot {
                                        continue;
                                    }
                                    let (r_src_label, r_src_slot) = node_id_parts(r.src.0);
                                    if seen.insert((r_src_slot, r_src_label)) {
                                        nb.push((r_src_slot, r_src_label));
                                    }
                                } else {
                                    let (r_src_label, r_src_slot) = node_id_parts(r.src.0);
                                    if r_src_label != cur_label_id || r_src_slot != cur_slot {
                                        continue;
                                    }
                                    let (r_dst_label, r_dst_slot) = node_id_parts(r.dst.0);
                                    if seen.insert((r_dst_slot, r_dst_label)) {
                                        nb.push((r_dst_slot, r_dst_label));
                                    }
                                }
                            }
                            nb
                        }
                    };

                    for (next_slot, next_label_id) in all_nb {
                        // When this position carries a label, the neighbour must
                        // actually have it — matching on the slot alone would
                        // admit the same-numbered node of any other label.
                        if let Some(required) = next_label_id_opt {
                            if next_label_id != required {
                                continue;
                            }
                        }
                        let next_node_id = NodeId(((next_label_id as u64) << 32) | next_slot);

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

                        next_frontier.push((next_slot, next_label_id, new_vals));
                    }
                }

                frontier = next_frontier;
            }

            // `frontier` now contains complete paths.  Project result rows.
            for (_final_slot, _final_label_id, path_vals) in frontier {
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
}
