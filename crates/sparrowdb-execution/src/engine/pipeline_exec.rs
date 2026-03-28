//! Opt-in chunked pipeline execution entry points (Phase 1 + Phase 2, #299).
//!
//! This module wires the Phase 1 and Phase 2 pipeline data structures into the
//! existing engine without modifying any row-at-a-time code paths.
//!
//! When `Engine::use_chunked_pipeline` is `true` AND the query shape qualifies,
//! these methods are called instead of the row-at-a-time equivalents.
//!
//! # Phase 1 supported shape
//!
//! Single-label scan with no hops and no aggregation:
//! `MATCH (n:Label) [WHERE n.prop op val] RETURN n.prop1, n.prop2`
//!
//! # Phase 2 supported shape
//!
//! Single-label, single-hop, directed (outgoing or incoming):
//! `MATCH (a:SrcLabel)-[:R]->(b:DstLabel) [WHERE ...] RETURN a.p, b.q [LIMIT n]`
//!
//! All other shapes fall back to the row-at-a-time engine.

use std::sync::Arc;

use sparrowdb_common::NodeId;
use sparrowdb_storage::edge_store::EdgeStore;

use super::*;
use crate::chunk::{DataChunk, COL_ID_DST_SLOT, COL_ID_SRC_SLOT};
use crate::pipeline::{ChunkPredicate, GetNeighbors, PipelineOperator, ReadNodeProps, ScanByLabel};

impl Engine {
    /// Return `true` when `m` qualifies for Phase 1 chunked execution.
    ///
    /// Eligibility:
    /// - `use_chunked_pipeline` flag is set.
    /// - Single node pattern with no relationship hops.
    /// - No aggregation in RETURN (aggregation is Phase 4).
    /// - No ORDER BY / SKIP / LIMIT (trivially added in Phase 2).
    /// - At least one label is specified (unlabeled scans fall back).
    pub(crate) fn can_use_chunked_pipeline(&self, m: &MatchStatement) -> bool {
        if !self.use_chunked_pipeline {
            return false;
        }
        if m.pattern.len() != 1 || !m.pattern[0].rels.is_empty() {
            return false;
        }
        if has_aggregate_in_return(&m.return_clause.items) {
            return false;
        }
        if !m.order_by.is_empty() || m.skip.is_some() || m.limit.is_some() {
            return false;
        }
        !m.pattern[0].nodes[0].labels.is_empty()
    }

    /// Return `true` when `m` qualifies for Phase 2 one-hop chunked execution.
    ///
    /// Eligibility (spec §3.6):
    /// - `use_chunked_pipeline` flag is set.
    /// - Exactly 2 nodes, 1 relationship (single hop).
    /// - Both nodes have exactly one label.
    /// - Directed (Outgoing or Incoming); undirected deferred to Phase 3.
    /// - No `OPTIONAL MATCH`, no `UNION`, no subquery in `WHERE`.
    /// - No aggregate, no `ORDER BY`.
    /// - `LIMIT` allowed when no `DISTINCT`.
    /// - Planner resolves exactly one relationship table.
    /// - No edge-property references in RETURN or WHERE.
    pub(crate) fn can_use_one_hop_chunked(&self, m: &MatchStatement) -> bool {
        use sparrowdb_cypher::ast::EdgeDir;

        if !self.use_chunked_pipeline {
            return false;
        }
        // Exactly 1 path pattern, 2 nodes, 1 rel.
        if m.pattern.len() != 1 {
            return false;
        }
        let pat = &m.pattern[0];
        if pat.rels.len() != 1 || pat.nodes.len() != 2 {
            return false;
        }
        // Both nodes must have exactly one label.
        if pat.nodes[0].labels.len() != 1 || pat.nodes[1].labels.len() != 1 {
            return false;
        }
        // Only directed (Outgoing or Incoming) supported in Phase 2.
        let dir = &pat.rels[0].dir;
        if *dir != EdgeDir::Outgoing && *dir != EdgeDir::Incoming {
            return false;
        }
        // No aggregation.
        if has_aggregate_in_return(&m.return_clause.items) {
            return false;
        }
        // No DISTINCT — chunked materializer has no dedup.
        if m.distinct {
            return false;
        }
        // No ORDER BY.
        if !m.order_by.is_empty() {
            return false;
        }
        // No variable-length hops.
        if pat.rels[0].min_hops.is_some() {
            return false;
        }
        // No edge-property references (Phase 2 spec §3.7 — no edge prop reads).
        // Guard both RETURN items and WHERE clause: a `WHERE r.weight > 5` with
        // no `r.*` in RETURN would silently return 0 rows because the chunked
        // materializer does not populate edge-property row_vals.
        let rel_var = &pat.rels[0].var;
        if !rel_var.is_empty() {
            let ref_in_return = m.return_clause.items.iter().any(|item| {
                column_name_for_item(item)
                    .split_once('.')
                    .is_some_and(|(v, _)| v == rel_var.as_str())
            });
            if ref_in_return {
                return false;
            }
            // Also reject if the WHERE clause accesses rel-variable properties.
            if let Some(ref wexpr) = m.where_clause {
                if expr_references_var(wexpr, rel_var.as_str()) {
                    return false;
                }
            }
        }
        // Only simple WHERE predicates supported (no CONTAINS, no subquery).
        if let Some(ref wexpr) = m.where_clause {
            if !is_simple_where_for_chunked(wexpr) {
                return false;
            }
        }
        // Resolve to exactly one rel table.
        let src_label = pat.nodes[0].labels.first().cloned().unwrap_or_default();
        let dst_label = pat.nodes[1].labels.first().cloned().unwrap_or_default();
        let rel_type = pat.rels[0].rel_type.clone();
        let n_tables = self
            .snapshot
            .catalog
            .list_rel_tables_with_ids()
            .into_iter()
            .filter(|(_, sid, did, rt)| {
                let type_ok = rel_type.is_empty() || rt == &rel_type;
                let src_ok = self
                    .snapshot
                    .catalog
                    .get_label(&src_label)
                    .ok()
                    .flatten()
                    .map(|id| id as u32 == *sid as u32)
                    .unwrap_or(false);
                let dst_ok = self
                    .snapshot
                    .catalog
                    .get_label(&dst_label)
                    .ok()
                    .flatten()
                    .map(|id| id as u32 == *did as u32)
                    .unwrap_or(false);
                type_ok && src_ok && dst_ok
            })
            .count();
        n_tables == 1
    }

    /// Execute a 1-hop query using the Phase 2 chunked pipeline.
    ///
    /// Pipeline shape (spec §3.6):
    /// ```text
    /// MaterializeRows(limit?)
    ///   <- optional Filter(ChunkPredicate, dst)
    ///   <- ReadNodeProps(dst)      [only if dst props referenced]
    ///   <- GetNeighbors(rel_type_id, src_label_id)
    ///   <- optional Filter(ChunkPredicate, src)
    ///   <- ReadNodeProps(src)      [only if src props referenced]
    ///   <- ScanByLabel(hwm)
    /// ```
    ///
    /// Terminal projection uses existing `project_hop_row` helpers at the
    /// materializer sink so we never duplicate projection semantics.
    pub(crate) fn execute_one_hop_chunked(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        use sparrowdb_cypher::ast::EdgeDir;

        let pat = &m.pattern[0];
        let rel_pat = &pat.rels[0];
        let dir = &rel_pat.dir;

        // For Incoming, swap the logical src/dst so the pipeline always runs
        // in the outgoing direction and we swap back at projection time.
        let (src_node_pat, dst_node_pat, swapped) = if *dir == EdgeDir::Incoming {
            (&pat.nodes[1], &pat.nodes[0], true)
        } else {
            (&pat.nodes[0], &pat.nodes[1], false)
        };

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();
        let rel_type = rel_pat.rel_type.clone();

        // Resolve label IDs — both must exist for this to reach us.
        let src_label_id = match self.snapshot.catalog.get_label(&src_label)? {
            Some(id) => id as u32,
            None => {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        };
        let dst_label_id = match self.snapshot.catalog.get_label(&dst_label)? {
            Some(id) => id as u32,
            None => {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        };

        // Resolve rel table ID.
        let (catalog_rel_id, _) = self
            .snapshot
            .catalog
            .list_rel_tables_with_ids()
            .into_iter()
            .find(|(_, sid, did, rt)| {
                let type_ok = rel_type.is_empty() || rt == &rel_type;
                let src_ok = *sid as u32 == src_label_id;
                let dst_ok = *did as u32 == dst_label_id;
                type_ok && src_ok && dst_ok
            })
            .map(|(cid, sid, did, rt)| (cid as u32, (sid, did, rt)))
            .ok_or_else(|| {
                sparrowdb_common::Error::InvalidArgument(
                    "no matching relationship table found".into(),
                )
            })?;

        let hwm_src = self.snapshot.store.hwm_for_label(src_label_id).unwrap_or(0);
        tracing::debug!(
            engine = "chunked",
            src_label = %src_label,
            dst_label = %dst_label,
            rel_type = %rel_type,
            hwm_src,
            "executing via chunked pipeline (1-hop)"
        );

        // Determine which property col_ids are needed for src and dst,
        // taking into account both RETURN and WHERE references.
        let src_var = src_node_pat.var.as_str();
        let dst_var = dst_node_pat.var.as_str();

        // For column name collection: when swapped, actual query vars are
        // swapped vs. the src/dst in the pipeline. Use the original query vars.
        let (query_src_var, query_dst_var) = if swapped {
            (dst_node_pat.var.as_str(), src_node_pat.var.as_str())
        } else {
            (src_var, dst_var)
        };

        let mut col_ids_src = collect_col_ids_for_var(query_src_var, column_names, src_label_id);
        let mut col_ids_dst = collect_col_ids_for_var(query_dst_var, column_names, dst_label_id);

        // Ensure WHERE-referenced columns are fetched.
        if let Some(ref wexpr) = m.where_clause {
            collect_col_ids_from_expr_for_var(wexpr, query_src_var, &mut col_ids_src);
            collect_col_ids_from_expr_for_var(wexpr, query_dst_var, &mut col_ids_dst);
        }
        // Ensure inline prop filter columns are fetched.
        for p in &src_node_pat.props {
            let cid = col_id_of(&p.key);
            if !col_ids_src.contains(&cid) {
                col_ids_src.push(cid);
            }
        }
        for p in &dst_node_pat.props {
            let cid = col_id_of(&p.key);
            if !col_ids_dst.contains(&cid) {
                col_ids_dst.push(cid);
            }
        }

        // Build delta index for this rel table.
        let delta_records = {
            let edge_store = EdgeStore::open(
                &self.snapshot.db_root,
                sparrowdb_storage::edge_store::RelTableId(catalog_rel_id),
            );
            edge_store.and_then(|s| s.read_delta()).unwrap_or_default()
        };

        // Get CSR for this rel table.
        let csr = self
            .snapshot
            .csrs
            .get(&catalog_rel_id)
            .cloned()
            .unwrap_or_else(|| sparrowdb_storage::csr::CsrForward::build(0, &[]));

        // Degree hint from stats.
        let avg_degree_hint = self
            .snapshot
            .rel_degree_stats()
            .get(&catalog_rel_id)
            .map(|s| s.mean().ceil() as usize)
            .unwrap_or(8);

        // Build WHERE predicates for src and dst (or use closure fallback).
        let src_pred_opt = m
            .where_clause
            .as_ref()
            .and_then(|wexpr| try_compile_predicate(wexpr, query_src_var, &col_ids_src));
        let dst_pred_opt = m
            .where_clause
            .as_ref()
            .and_then(|wexpr| try_compile_predicate(wexpr, query_dst_var, &col_ids_dst));

        let store_arc = Arc::new(NodeStore::open(self.snapshot.store.root_path())?);

        // ── Build the pipeline ────────────────────────────────────────────────
        //
        // We box each layer into a type-erased enum so we can build the pipeline
        // dynamically depending on which operators are needed.

        let limit = m.limit.map(|l| l as usize);
        let mut rows: Vec<Vec<Value>> = Vec::new();

        // Use a macro-free trait-object approach: build as a flat loop with
        // explicit operator invocations to avoid complex generic nesting.
        // This keeps Phase 2 simple and Phase 4 can refactor to a proper
        // operator tree if needed.

        let mut scan = ScanByLabel::new(hwm_src);

        'outer: while let Some(scan_chunk) = scan.next_chunk()? {
            // Tombstone check happens in the slot loop below.

            // ── ReadNodeProps(src) ────────────────────────────────────────────
            let src_chunk = if !col_ids_src.is_empty() {
                let mut rnp = ReadNodeProps::new(
                    SingleChunkSource::new(scan_chunk),
                    Arc::clone(&store_arc),
                    src_label_id,
                    crate::chunk::COL_ID_SLOT,
                    col_ids_src.clone(),
                );
                match rnp.next_chunk()? {
                    Some(c) => c,
                    None => continue,
                }
            } else {
                scan_chunk
            };

            // ── Filter(src) ───────────────────────────────────────────────────
            let src_chunk = if let Some(ref pred) = src_pred_opt {
                let pred = pred.clone();
                let keep: Vec<bool> = {
                    (0..src_chunk.len())
                        .map(|i| pred.eval(&src_chunk, i))
                        .collect()
                };
                let mut c = src_chunk;
                c.filter_sel(|i| keep[i]);
                if c.live_len() == 0 {
                    continue;
                }
                c
            } else {
                src_chunk
            };

            // ── GetNeighbors ──────────────────────────────────────────────────
            let mut gn = GetNeighbors::new(
                SingleChunkSource::new(src_chunk.clone()),
                csr.clone(),
                &delta_records,
                src_label_id,
                avg_degree_hint,
            );

            while let Some(hop_chunk) = gn.next_chunk()? {
                // hop_chunk has COL_ID_SRC_SLOT and COL_ID_DST_SLOT columns.

                // ── ReadNodeProps(dst) ────────────────────────────────────────
                let dst_chunk = if !col_ids_dst.is_empty() {
                    let mut rnp = ReadNodeProps::new(
                        SingleChunkSource::new(hop_chunk),
                        Arc::clone(&store_arc),
                        dst_label_id,
                        COL_ID_DST_SLOT,
                        col_ids_dst.clone(),
                    );
                    match rnp.next_chunk()? {
                        Some(c) => c,
                        None => continue,
                    }
                } else {
                    hop_chunk
                };

                // ── Filter(dst) ───────────────────────────────────────────────
                let dst_chunk = if let Some(ref pred) = dst_pred_opt {
                    let pred = pred.clone();
                    let keep: Vec<bool> = (0..dst_chunk.len())
                        .map(|i| pred.eval(&dst_chunk, i))
                        .collect();
                    let mut c = dst_chunk;
                    c.filter_sel(|i| keep[i]);
                    if c.live_len() == 0 {
                        continue;
                    }
                    c
                } else {
                    dst_chunk
                };

                // ── MaterializeRows ───────────────────────────────────────────
                let src_slot_col = src_chunk.find_column(crate::chunk::COL_ID_SLOT);
                let dst_slot_col = dst_chunk.find_column(COL_ID_DST_SLOT);
                let hop_src_col = dst_chunk.find_column(COL_ID_SRC_SLOT);

                for row_idx in dst_chunk.live_rows() {
                    let dst_slot = dst_slot_col.map(|c| c.data[row_idx]).unwrap_or(0);
                    let hop_src_slot = hop_src_col.map(|c| c.data[row_idx]).unwrap_or(0);

                    // Tombstone checks.
                    let src_node = NodeId(((src_label_id as u64) << 32) | hop_src_slot);
                    let dst_node = NodeId(((dst_label_id as u64) << 32) | dst_slot);
                    if self.is_node_tombstoned(src_node) || self.is_node_tombstoned(dst_node) {
                        continue;
                    }

                    // Build src_props from the src_chunk (find the src slot row).
                    // The src_chunk row index = the physical index in the scan
                    // chunk that produced this hop. We locate it by matching
                    // hop_src_slot with the slot column.
                    let src_props = if let Some(sc) = src_slot_col {
                        // Find the src row by slot value.
                        let src_row = (0..sc.data.len()).find(|&i| sc.data[i] == hop_src_slot);
                        if let Some(src_ri) = src_row {
                            build_props_from_chunk(&src_chunk, src_ri, &col_ids_src)
                        } else {
                            // Fallback: read from store.
                            let nullable = self
                                .snapshot
                                .store
                                .get_node_raw_nullable(src_node, &col_ids_src)?;
                            nullable
                                .into_iter()
                                .filter_map(|(cid, opt)| opt.map(|v| (cid, v)))
                                .collect()
                        }
                    } else {
                        vec![]
                    };

                    let dst_props = build_props_from_chunk(&dst_chunk, row_idx, &col_ids_dst);

                    // Apply WHERE clause if present (covers complex predicates
                    // that couldn't be compiled into ChunkPredicate).
                    if let Some(ref where_expr) = m.where_clause {
                        // Determine actual src/dst variable names for row_vals.
                        let (actual_src_var, actual_dst_var) = if swapped {
                            (dst_node_pat.var.as_str(), src_node_pat.var.as_str())
                        } else {
                            (src_node_pat.var.as_str(), dst_node_pat.var.as_str())
                        };
                        let (actual_src_props, actual_dst_props) = if swapped {
                            (&dst_props, &src_props)
                        } else {
                            (&src_props, &dst_props)
                        };
                        let mut row_vals = build_row_vals(
                            actual_src_props,
                            actual_src_var,
                            &col_ids_src,
                            &self.snapshot.store,
                        );
                        row_vals.extend(build_row_vals(
                            actual_dst_props,
                            actual_dst_var,
                            &col_ids_dst,
                            &self.snapshot.store,
                        ));
                        row_vals.extend(self.dollar_params());
                        if !self.eval_where_graph(where_expr, &row_vals) {
                            continue;
                        }
                    }

                    // Project output row using existing hop-row helper.
                    let (proj_src_props, proj_dst_props) = if swapped {
                        (&dst_props as &[(u32, u64)], &src_props as &[(u32, u64)])
                    } else {
                        (&src_props as &[(u32, u64)], &dst_props as &[(u32, u64)])
                    };
                    let (proj_src_var, proj_dst_var, proj_src_label, proj_dst_label) = if swapped {
                        (
                            dst_node_pat.var.as_str(),
                            src_node_pat.var.as_str(),
                            dst_label.as_str(),
                            src_label.as_str(),
                        )
                    } else {
                        (
                            src_node_pat.var.as_str(),
                            dst_node_pat.var.as_str(),
                            src_label.as_str(),
                            dst_label.as_str(),
                        )
                    };

                    let row = project_hop_row(
                        proj_src_props,
                        proj_dst_props,
                        column_names,
                        proj_src_var,
                        proj_dst_var,
                        None, // no rel_var_type for Phase 2
                        Some((proj_src_var, proj_src_label)),
                        Some((proj_dst_var, proj_dst_label)),
                        &self.snapshot.store,
                        None, // no edge_props
                    );
                    rows.push(row);

                    // LIMIT short-circuit.
                    if let Some(lim) = limit {
                        if rows.len() >= lim {
                            break 'outer;
                        }
                    }
                }
            }
        }

        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    /// Execute a simple label scan using the Phase 1 chunked pipeline.
    ///
    /// The pipeline emits slot numbers in `CHUNK_CAPACITY`-sized batches via
    /// `ScanByLabel`. For each chunk we apply inline-prop filters and the WHERE
    /// clause row-at-a-time (same semantics as the row-at-a-time engine) and
    /// batch-read the RETURN properties.
    ///
    /// Phase 2 will replace the per-row property reads with bulk columnar reads
    /// and evaluate the WHERE clause column-at-a-time.
    pub(crate) fn execute_scan_chunked(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        use crate::pipeline::PipelineOperator;

        let pat = &m.pattern[0];
        let node = &pat.nodes[0];
        let label = node.labels.first().cloned().unwrap_or_default();

        // Unknown label → 0 rows (standard Cypher semantics, matches row-at-a-time).
        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        };

        let hwm = self.snapshot.store.hwm_for_label(label_id)?;
        tracing::debug!(label = %label, hwm = hwm, "chunked pipeline: label scan");

        // Collect all col_ids needed (RETURN + WHERE + inline prop filters).
        let mut all_col_ids: Vec<u32> = collect_col_ids_from_columns(column_names);
        if let Some(ref wexpr) = m.where_clause {
            collect_col_ids_from_expr(wexpr, &mut all_col_ids);
        }
        for p in &node.props {
            let cid = col_id_of(&p.key);
            if !all_col_ids.contains(&cid) {
                all_col_ids.push(cid);
            }
        }

        let var_name = node.var.as_str();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        // ── Drive the ScanByLabel pipeline ───────────────────────────────────
        //
        // Phase 1: the scan is purely over slot indices (0..hwm). Property
        // reads happen per live-slot inside this loop. Phase 2 will push the
        // reads into the pipeline operators themselves.

        let mut scan = ScanByLabel::new(hwm);

        while let Some(chunk) = scan.next_chunk()? {
            // Process each slot in this chunk.
            for row_idx in chunk.live_rows() {
                let slot = chunk.column(0).data[row_idx];
                let node_id = NodeId(((label_id as u64) << 32) | slot);

                // Skip tombstoned nodes (same as row-at-a-time engine).
                if self.is_node_tombstoned(node_id) {
                    continue;
                }

                // Read properties needed for filter and projection.
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
                    if !var_name.is_empty() && !label.is_empty() {
                        row_vals.insert(
                            format!("{}.__labels__", var_name),
                            Value::List(vec![Value::String(label.clone())]),
                        );
                    }
                    if !var_name.is_empty() {
                        row_vals.insert(var_name.to_string(), Value::NodeRef(node_id));
                    }
                    row_vals.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &row_vals) {
                        continue;
                    }
                }

                // Project RETURN columns.
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

        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }
}

// ── SingleChunkSource ─────────────────────────────────────────────────────────

/// A one-shot `PipelineOperator` that yields a single pre-built `DataChunk`.
///
/// Used to wrap an existing chunk so it can be passed to operators that expect
/// a child `PipelineOperator`.  After the chunk is consumed, returns `None`.
struct SingleChunkSource {
    chunk: Option<DataChunk>,
}

impl SingleChunkSource {
    fn new(chunk: DataChunk) -> Self {
        SingleChunkSource { chunk: Some(chunk) }
    }
}

impl PipelineOperator for SingleChunkSource {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        Ok(self.chunk.take())
    }
}

// ── Free helpers used by execute_one_hop_chunked ──────────────────────────────

/// Extract the column name for a `ReturnItem`.
fn column_name_for_item(item: &ReturnItem) -> String {
    if let Some(ref alias) = item.alias {
        return alias.clone();
    }
    // Fallback: render the expr as a rough string.
    match &item.expr {
        Expr::PropAccess { var, prop } => format!("{}.{}", var, prop),
        Expr::Var(v) => v.clone(),
        _ => String::new(),
    }
}

/// Returns `true` when the WHERE expression can be fully handled by the chunked
/// pipeline (either compiled into `ChunkPredicate` or evaluated via the fallback
/// row-vals path — which covers all simple property predicates).
///
/// Returns `false` for CONTAINS/STARTS WITH/EXISTS/subquery shapes that would
/// require the full row-engine evaluator in a way that the chunked path can't
/// trivially support at the sink.
/// Returns `true` if `expr` contains any `var.prop` access for the given variable name.
///
/// Used to guard the chunked path against edge-property predicates in WHERE:
/// `WHERE r.weight > 5` must fall back to the row engine because the chunked
/// materializer does not populate edge-property row_vals, which would silently
/// return zero results rather than the correct filtered set.
fn expr_references_var(expr: &Expr, var_name: &str) -> bool {
    match expr {
        Expr::PropAccess { var, .. } => var.as_str() == var_name,
        Expr::BinOp { left, right, .. } => {
            expr_references_var(left, var_name) || expr_references_var(right, var_name)
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr_references_var(a, var_name) || expr_references_var(b, var_name)
        }
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_references_var(inner, var_name)
        }
        _ => false,
    }
}

fn is_simple_where_for_chunked(expr: &Expr) -> bool {
    match expr {
        Expr::BinOp { left, op, right } => {
            match op {
                // These require text-index support or are unsafe to fallback.
                BinOpKind::Contains | BinOpKind::StartsWith | BinOpKind::EndsWith => false,
                _ => is_simple_where_for_chunked(left) && is_simple_where_for_chunked(right),
            }
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            is_simple_where_for_chunked(a) && is_simple_where_for_chunked(b)
        }
        Expr::Not(inner) => is_simple_where_for_chunked(inner),
        Expr::IsNull(_) | Expr::IsNotNull(_) => true,
        Expr::PropAccess { .. } | Expr::Var(_) | Expr::Literal(_) => true,
        // Subqueries, EXISTS, function calls → fall back to row engine.
        Expr::ExistsSubquery(_) | Expr::NotExists(_) | Expr::FnCall { .. } => false,
        _ => true,
    }
}

/// Try to compile a simple WHERE expression for `var_name` into a `ChunkPredicate`.
///
/// Only handles `var.prop op literal` patterns.  Returns `None` when the
/// expression references multiple variables or uses unsupported operators,
/// in which case the row-vals fallback path in the materializer handles it.
fn try_compile_predicate(expr: &Expr, var_name: &str, _col_ids: &[u32]) -> Option<ChunkPredicate> {
    match expr {
        Expr::BinOp { left, op, right } => {
            // Only handle `var.prop op literal` or `literal op var.prop`.
            let (prop_expr, lit_expr, swapped) = if matches!(right.as_ref(), Expr::Literal(_)) {
                (left.as_ref(), right.as_ref(), false)
            } else if matches!(left.as_ref(), Expr::Literal(_)) {
                (right.as_ref(), left.as_ref(), true)
            } else {
                return None;
            };

            let (v, key) = match prop_expr {
                Expr::PropAccess { var, prop } => (var.as_str(), prop.as_str()),
                _ => return None,
            };
            if v != var_name {
                return None;
            }
            let col_id = col_id_of(key);

            let rhs_raw = match lit_expr {
                Expr::Literal(lit) => literal_to_raw_u64(lit)?,
                _ => return None,
            };

            // Swap operators if literal is on the left.
            let effective_op = if swapped {
                match op {
                    BinOpKind::Lt => BinOpKind::Gt,
                    BinOpKind::Le => BinOpKind::Ge,
                    BinOpKind::Gt => BinOpKind::Lt,
                    BinOpKind::Ge => BinOpKind::Le,
                    other => other.clone(),
                }
            } else {
                op.clone()
            };

            match effective_op {
                BinOpKind::Eq => Some(ChunkPredicate::Eq { col_id, rhs_raw }),
                BinOpKind::Neq => Some(ChunkPredicate::Ne { col_id, rhs_raw }),
                BinOpKind::Gt => Some(ChunkPredicate::Gt { col_id, rhs_raw }),
                BinOpKind::Ge => Some(ChunkPredicate::Ge { col_id, rhs_raw }),
                BinOpKind::Lt => Some(ChunkPredicate::Lt { col_id, rhs_raw }),
                BinOpKind::Le => Some(ChunkPredicate::Le { col_id, rhs_raw }),
                _ => None,
            }
        }
        Expr::IsNull(inner) => {
            if let Expr::PropAccess { var, prop } = inner.as_ref() {
                if var.as_str() == var_name {
                    return Some(ChunkPredicate::IsNull {
                        col_id: col_id_of(prop),
                    });
                }
            }
            None
        }
        Expr::IsNotNull(inner) => {
            if let Expr::PropAccess { var, prop } = inner.as_ref() {
                if var.as_str() == var_name {
                    return Some(ChunkPredicate::IsNotNull {
                        col_id: col_id_of(prop),
                    });
                }
            }
            None
        }
        Expr::And(a, b) => {
            let ca = try_compile_predicate(a, var_name, _col_ids);
            let cb = try_compile_predicate(b, var_name, _col_ids);
            match (ca, cb) {
                (Some(pa), Some(pb)) => Some(ChunkPredicate::And(vec![pa, pb])),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Encode a literal as a raw `u64` for `ChunkPredicate` comparison.
///
/// Returns `None` for string/float literals that cannot be compared using
/// simple raw-u64 equality (those fall back to the row-vals path).
fn literal_to_raw_u64(lit: &Literal) -> Option<u64> {
    use sparrowdb_storage::node_store::Value as StoreValue;
    match lit {
        Literal::Int(n) => Some(StoreValue::Int64(*n).to_u64()),
        Literal::Bool(b) => Some(StoreValue::Int64(if *b { 1 } else { 0 }).to_u64()),
        // Strings and floats: leave to the row-vals fallback.
        Literal::String(_) | Literal::Float(_) | Literal::Null | Literal::Param(_) => None,
    }
}

/// Build a `Vec<(col_id, raw_value)>` from a chunk at a given physical row index.
///
/// Only returns columns that are NOT null (null-bitmap bit is clear) and whose
/// col_id is in `col_ids`.
fn build_props_from_chunk(chunk: &DataChunk, row_idx: usize, col_ids: &[u32]) -> Vec<(u32, u64)> {
    col_ids
        .iter()
        .filter_map(|&cid| {
            let col = chunk.find_column(cid)?;
            if col.nulls.is_null(row_idx) {
                None
            } else {
                Some((cid, col.data[row_idx]))
            }
        })
        .collect()
}
