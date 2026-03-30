//! Opt-in chunked pipeline execution entry points (Phase 1 + Phase 2 + Phase 3, #299).
//!
//! This module wires the Phase 1, Phase 2, and Phase 3 pipeline data structures
//! into the existing engine without modifying any row-at-a-time code paths.
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
//! # Phase 3 supported shape
//!
//! Single-label, two-hop same-rel, both hops outgoing:
//! `MATCH (a:L)-[:R]->(b:L)-[:R]->(c:L) [WHERE ...] RETURN ... [LIMIT n]`
//!
//! All other shapes fall back to the row-at-a-time engine.

use std::sync::Arc;

use sparrowdb_common::NodeId;
use sparrowdb_storage::edge_store::EdgeStore;

use super::*;
use crate::chunk::{DataChunk, COL_ID_DST_SLOT, COL_ID_SLOT, COL_ID_SRC_SLOT};
use crate::pipeline::{
    BfsArena, ChunkPredicate, GetNeighbors, PipelineOperator, ReadNodeProps, ScanByLabel,
    SlotIntersect,
};

// ── ChunkedPlan ───────────────────────────────────────────────────────────────

/// Shape selector for the chunked vectorized pipeline (Phase 4, spec §2.3).
///
/// Replaces the cascade of `can_use_*` boolean guards with a typed plan enum.
/// `Engine::try_plan_chunked_match` returns one of these variants (or `None`
/// to indicate the row engine should be used), and dispatch is a `match` with
/// no further `if can_use_*` calls.
///
/// Each variant may carry shape-specific parameters in future phases.  For now
/// all resolution happens in the `execute_*_chunked` methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkedPlan {
    /// Single-label scan only — no relationship hops.
    Scan,
    /// Single-hop directed traversal.
    OneHop,
    /// Two-hop same-rel-type directed traversal.
    TwoHop,
    /// Mutual-neighbors: `(a)-[:R]->(x)<-[:R]-(b)` with both a and b bound.
    MutualNeighbors,
}

impl std::fmt::Display for ChunkedPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkedPlan::Scan => write!(f, "Scan"),
            ChunkedPlan::OneHop => write!(f, "OneHop"),
            ChunkedPlan::TwoHop => write!(f, "TwoHop"),
            ChunkedPlan::MutualNeighbors => write!(f, "MutualNeighbors"),
        }
    }
}

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
        // DISTINCT deduplication is not implemented in the chunked scan path —
        // fall back to the row engine which applies deduplicate_rows.
        if m.distinct {
            return false;
        }
        // Inline prop filters on the node pattern are not evaluated by the
        // chunked scan path — fall back to the row engine so they are applied.
        // (Tracked as #362 for native support in the chunked path.)
        if !m.pattern[0].nodes[0].props.is_empty() {
            return false;
        }
        // Bare variable projection (RETURN n) requires the row engine eval path
        // to build a full property map (SPA-213). project_row returns Null for
        // bare vars. Fall back until the chunked path implements SPA-213.
        if m.return_clause
            .items
            .iter()
            .any(|item| matches!(&item.expr, Expr::Var(_)))
        {
            return false;
        }
        // id(n) and other NodeRef-dependent functions require the row engine (#372).
        if return_requires_row_engine(&m.return_clause.items) {
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
        // Inline prop filters on node patterns are not evaluated by the chunked
        // one-hop path — fall back to the row engine.  (See #362.)
        if pat.nodes.iter().any(|n| !n.props.is_empty()) {
            return false;
        }
        // Inline prop filters on the relationship pattern (e.g. [r:KNOWS {since:2020}])
        // are not evaluated by the chunked one-hop path — it has no edge-props read
        // stage.  Fall back to the row engine so filters are applied correctly (#367).
        if !pat.rels[0].props.is_empty() {
            return false;
        }
        // id(n) and other NodeRef-dependent functions require the row engine (#372).
        if return_requires_row_engine(&m.return_clause.items) {
            return false;
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

        let mut col_ids_src =
            collect_col_ids_for_var_from_items(query_src_var, &m.return_clause.items);
        let mut col_ids_dst =
            collect_col_ids_for_var_from_items(query_dst_var, &m.return_clause.items);

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

                    // Supply rel_var_type so that `type(r) AS alias` resolves correctly.
                    let rel_var_type_arg = if !rel_pat.var.is_empty() && !rel_type.is_empty() {
                        Some((rel_pat.var.as_str(), rel_type.as_str()))
                    } else {
                        None
                    };
                    let row = project_hop_row(
                        proj_src_props,
                        proj_dst_props,
                        &m.return_clause.items,
                        proj_src_var,
                        proj_dst_var,
                        rel_var_type_arg,
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

    /// Select a `ChunkedPlan` for the given `MatchStatement` (Phase 4, spec §2.3).
    ///
    /// Returns `Some(plan)` when the query shape maps to a known chunked fast-path,
    /// or `None` when the row engine should handle it.  The caller dispatches via
    /// `match` — no further `can_use_*` calls are made after this returns.
    ///
    /// # Precedence
    ///
    /// MutualNeighbors is checked before TwoHop because it is a more specific
    /// pattern (both endpoints bound) that would otherwise fall into TwoHop.
    pub fn try_plan_chunked_match(&self, m: &MatchStatement) -> Option<ChunkedPlan> {
        // MutualNeighbors is a specialised 2-hop shape — check first.
        if self.can_use_mutual_neighbors_chunked(m) {
            return Some(ChunkedPlan::MutualNeighbors);
        }
        if self.can_use_two_hop_chunked(m) {
            return Some(ChunkedPlan::TwoHop);
        }
        if self.can_use_one_hop_chunked(m) {
            return Some(ChunkedPlan::OneHop);
        }
        if self.can_use_chunked_pipeline(m) {
            return Some(ChunkedPlan::Scan);
        }
        None
    }

    /// Return `true` when `m` qualifies for Phase 4 mutual-neighbors chunked execution.
    ///
    /// The mutual-neighbors pattern is:
    /// ```cypher
    /// MATCH (a:L)-[:R]->(x:L)<-[:R]-(b:L)
    /// WHERE id(a) = $x AND id(b) = $y
    /// RETURN x
    /// ```
    ///
    /// # Guard (spec §5.2 hard gate)
    ///
    /// Must be strict:
    /// - Exactly 2 nodes in each of 2 path patterns, OR exactly 3 nodes + 2 rels
    ///   with the middle node shared and direction fork (first hop Outgoing, second
    ///   hop Incoming or vice versa).
    /// - Actually: we look for exactly 1 path pattern with 3 nodes + 2 rels where
    ///   hops are directed but in *opposite* directions (fork pattern).
    /// - Both endpoint nodes must have exactly one bound-param `id()` filter.
    /// - Same rel-type for both hops.
    /// - Same label on all three nodes.
    /// - No edge-property references.
    /// - No aggregation, no ORDER BY, no DISTINCT.
    pub(crate) fn can_use_mutual_neighbors_chunked(&self, m: &MatchStatement) -> bool {
        use sparrowdb_cypher::ast::EdgeDir;

        if !self.use_chunked_pipeline {
            return false;
        }
        // Exactly 1 path pattern, 3 nodes, 2 rels.
        if m.pattern.len() != 1 {
            return false;
        }
        let pat = &m.pattern[0];
        if pat.rels.len() != 2 || pat.nodes.len() != 3 {
            return false;
        }
        // Fork pattern: first hop Outgoing, second hop Incoming (a→x←b).
        if pat.rels[0].dir != EdgeDir::Outgoing || pat.rels[1].dir != EdgeDir::Incoming {
            return false;
        }
        // No variable-length hops.
        if pat.rels[0].min_hops.is_some() || pat.rels[1].min_hops.is_some() {
            return false;
        }
        // Same rel-type for both hops (including both empty).
        if pat.rels[0].rel_type != pat.rels[1].rel_type {
            return false;
        }
        // All three nodes must have the same single label.
        if pat.nodes[0].labels.len() != 1
            || pat.nodes[1].labels.len() != 1
            || pat.nodes[2].labels.len() != 1
        {
            return false;
        }
        if pat.nodes[0].labels[0] != pat.nodes[1].labels[0]
            || pat.nodes[1].labels[0] != pat.nodes[2].labels[0]
        {
            return false;
        }
        // No aggregation.
        if has_aggregate_in_return(&m.return_clause.items) {
            return false;
        }
        // No DISTINCT.
        if m.distinct {
            return false;
        }
        // No ORDER BY.
        if !m.order_by.is_empty() {
            return false;
        }
        // No edge-property references.
        for rel in &pat.rels {
            if !rel.var.is_empty() {
                let ref_in_return = m.return_clause.items.iter().any(|item| {
                    column_name_for_item(item)
                        .split_once('.')
                        .is_some_and(|(v, _)| v == rel.var.as_str())
                });
                if ref_in_return {
                    return false;
                }
                if let Some(ref wexpr) = m.where_clause {
                    if expr_references_var(wexpr, rel.var.as_str()) {
                        return false;
                    }
                }
            }
        }
        // Inline prop filters on relationship patterns are not evaluated by the
        // chunked mutual-neighbors path — fall back to the row engine.  (See #367.)
        if pat.rels.iter().any(|r| !r.props.is_empty()) {
            return false;
        }
        // Endpoint binding: either WHERE id(a)=$x AND id(b)=$y, or both
        // endpoint nodes carry exactly one inline prop filter (e.g. {uid: 0}).
        // The inline-prop form is the shape used by the Facebook benchmark Q8:
        //   MATCH (a:User {uid: X})-[:R]->(m)<-[:R]-(b:User {uid: Y}) RETURN m.uid
        let a_var = pat.nodes[0].var.as_str();
        let b_var = pat.nodes[2].var.as_str();
        match m.where_clause.as_ref() {
            None => {
                // Accept inline-prop binding: each endpoint must carry exactly
                // one prop filter so execute can scan for the matching slot.
                let a_bound = pat.nodes[0].props.len() == 1;
                let b_bound = pat.nodes[2].props.len() == 1;
                if !a_bound || !b_bound {
                    return false;
                }
            }
            Some(wexpr) => {
                if !where_is_only_id_param_conjuncts(wexpr, a_var, b_var) {
                    return false;
                }
            }
        }
        // id(n) and other NodeRef-dependent functions require the row engine (#372).
        if return_requires_row_engine(&m.return_clause.items) {
            return false;
        }
        // Rel table must exist.
        let label = pat.nodes[0].labels[0].clone();
        let rel_type = &pat.rels[0].rel_type;
        let catalog = &self.snapshot.catalog;
        let tables = catalog.list_rel_tables_with_ids();
        let label_id_opt = catalog.get_label(&label).ok().flatten();
        let label_id = match label_id_opt {
            Some(id) => id as u32,
            None => return false,
        };
        let has_table = tables.iter().any(|(_, sid, did, rt)| {
            let type_ok = rel_type.is_empty() || rt == rel_type;
            let endpoint_ok = *sid as u32 == label_id && *did as u32 == label_id;
            type_ok && endpoint_ok
        });
        has_table
    }

    /// Execute the mutual-neighbors fast-path for the chunked pipeline (Phase 4).
    ///
    /// Pattern: `MATCH (a:L)-[:R]->(x:L)<-[:R]-(b:L) WHERE id(a)=$x AND id(b)=$y RETURN x`
    ///
    /// Algorithm:
    /// 1. Resolve bound slot for `a` from `id(a) = $x` param.
    /// 2. Resolve bound slot for `b` from `id(b) = $y` param.
    /// 3. Expand outgoing neighbors of `a` into set A.
    /// 4. Expand outgoing neighbors of `b` into set B.
    /// 5. Intersect A ∩ B via `SlotIntersect` — produces sorted common neighbors.
    /// 6. Materialise output rows from common neighbor slots.
    pub(crate) fn execute_mutual_neighbors_chunked(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let a_node_pat = &pat.nodes[0];
        let x_node_pat = &pat.nodes[1];
        let b_node_pat = &pat.nodes[2];

        let label = a_node_pat.labels[0].clone();
        let rel_type = pat.rels[0].rel_type.clone();

        let label_id = match self.snapshot.catalog.get_label(&label)? {
            Some(id) => id as u32,
            None => {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        };

        // Resolve rel table ID.
        let catalog_rel_id = self
            .snapshot
            .catalog
            .list_rel_tables_with_ids()
            .into_iter()
            .find(|(_, sid, did, rt)| {
                let type_ok = rel_type.is_empty() || rt == &rel_type;
                let endpoint_ok = *sid as u32 == label_id && *did as u32 == label_id;
                type_ok && endpoint_ok
            })
            .map(|(cid, _, _, _)| cid as u32)
            .ok_or_else(|| {
                sparrowdb_common::Error::InvalidArgument(
                    "no matching relationship table for mutual-neighbors".into(),
                )
            })?;

        // Extract bound slots for a and b.
        // Two supported forms:
        //   1. WHERE id(a) = $x AND id(b) = $y  — param-bound NodeId
        //   2. Inline props on endpoint nodes    — scan label for matching slot
        let a_var = a_node_pat.var.as_str();
        let b_var = b_node_pat.var.as_str();
        let (a_slot_opt, b_slot_opt) = if m.where_clause.is_some() {
            // Form 1: id() params.
            (
                extract_id_param_slot(m.where_clause.as_ref(), a_var, &self.params, label_id),
                extract_id_param_slot(m.where_clause.as_ref(), b_var, &self.params, label_id),
            )
        } else {
            // Form 2: inline props — scan the label to find matching slots.
            let hwm = self.snapshot.store.hwm_for_label(label_id).unwrap_or(0);
            let dollar_params = self.dollar_params();
            let prop_idx = self.prop_index.borrow();
            (
                find_slot_by_props(
                    &self.snapshot.store,
                    label_id,
                    hwm,
                    &a_node_pat.props,
                    &dollar_params,
                    &prop_idx,
                ),
                find_slot_by_props(
                    &self.snapshot.store,
                    label_id,
                    hwm,
                    &b_node_pat.props,
                    &dollar_params,
                    &prop_idx,
                ),
            )
        };

        let (a_slot, b_slot) = match (a_slot_opt, b_slot_opt) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                // Endpoint not resolved — return empty.
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        };

        // Cypher requires distinct node bindings; a node cannot be its own mutual
        // neighbor.  When both id() params resolve to the same slot the intersection
        // would include `a` itself, which is semantically wrong — return empty.
        if a_slot == b_slot {
            return Ok(QueryResult {
                columns: column_names.to_vec(),
                rows: vec![],
            });
        }

        tracing::debug!(
            engine = "chunked",
            plan = %ChunkedPlan::MutualNeighbors,
            label = %label,
            rel_type = %rel_type,
            a_slot,
            b_slot,
            "executing via chunked pipeline"
        );

        let csr = self
            .snapshot
            .csrs
            .get(&catalog_rel_id)
            .cloned()
            .unwrap_or_else(|| sparrowdb_storage::csr::CsrForward::build(0, &[]));

        let delta_records = {
            let edge_store = sparrowdb_storage::edge_store::EdgeStore::open(
                &self.snapshot.db_root,
                sparrowdb_storage::edge_store::RelTableId(catalog_rel_id),
            );
            edge_store.and_then(|s| s.read_delta()).unwrap_or_default()
        };

        // Build neighbor sets via GetNeighbors on single-slot sources.
        let a_scan = ScanByLabel::from_slots(vec![a_slot]);
        let a_neighbors = GetNeighbors::new(a_scan, csr.clone(), &delta_records, label_id, 8);

        let b_scan = ScanByLabel::from_slots(vec![b_slot]);
        let b_neighbors = GetNeighbors::new(b_scan, csr, &delta_records, label_id, 8);

        // GetNeighbors emits (src_slot, dst_slot) pairs. We need dst_slot column.
        // Wrap in an adaptor that projects COL_ID_DST_SLOT → COL_ID_SLOT.
        let a_proj = DstSlotProjector::new(a_neighbors);
        let b_proj = DstSlotProjector::new(b_neighbors);

        // Intersect.
        let spill_threshold = 64 * 1024; // 64 K entries before spill warning
        let mut intersect =
            SlotIntersect::new(a_proj, b_proj, COL_ID_SLOT, COL_ID_SLOT, spill_threshold);

        // Collect common neighbor slots.
        let mut common_slots: Vec<u64> = Vec::new();
        while let Some(chunk) = intersect.next_chunk()? {
            if let Some(col) = chunk.find_column(COL_ID_SLOT) {
                for row_idx in chunk.live_rows() {
                    common_slots.push(col.data[row_idx]);
                }
            }
        }

        // Materialise output rows.
        let x_var = x_node_pat.var.as_str();
        let mut col_ids_x = collect_col_ids_for_var(x_var, column_names, label_id);
        if let Some(ref wexpr) = m.where_clause {
            collect_col_ids_from_expr_for_var(wexpr, x_var, &mut col_ids_x);
        }
        for p in &x_node_pat.props {
            let cid = col_id_of(&p.key);
            if !col_ids_x.contains(&cid) {
                col_ids_x.push(cid);
            }
        }

        let store_arc = Arc::new(sparrowdb_storage::node_store::NodeStore::open(
            self.snapshot.store.root_path(),
        )?);

        let limit = m.limit.map(|l| l as usize);
        let mut rows: Vec<Vec<Value>> = Vec::new();

        'outer: for x_slot in common_slots {
            let x_node_id = NodeId(((label_id as u64) << 32) | x_slot);

            // Skip tombstoned common neighbors.
            if self.is_node_tombstoned(x_node_id) {
                continue;
            }

            // Read x properties.
            let x_props: Vec<(u32, u64)> = if !col_ids_x.is_empty() {
                let nullable = store_arc.batch_read_node_props_nullable(
                    label_id,
                    &[x_slot as u32],
                    &col_ids_x,
                )?;
                if nullable.is_empty() {
                    vec![]
                } else {
                    col_ids_x
                        .iter()
                        .enumerate()
                        .filter_map(|(i, &cid)| nullable[0][i].map(|v| (cid, v)))
                        .collect()
                }
            } else {
                vec![]
            };

            // Apply remaining WHERE predicates (e.g. x.prop filters).
            if let Some(ref where_expr) = m.where_clause {
                let mut row_vals =
                    build_row_vals(&x_props, x_var, &col_ids_x, &self.snapshot.store);
                // Also inject a and b NodeRef for id() evaluation.
                if !a_var.is_empty() {
                    let a_node_id = NodeId(((label_id as u64) << 32) | a_slot);
                    row_vals.insert(a_var.to_string(), Value::NodeRef(a_node_id));
                }
                if !b_var.is_empty() {
                    let b_node_id = NodeId(((label_id as u64) << 32) | b_slot);
                    row_vals.insert(b_var.to_string(), Value::NodeRef(b_node_id));
                }
                row_vals.extend(self.dollar_params());
                if !self.eval_where_graph(where_expr, &row_vals) {
                    continue;
                }
            }

            // Project output row.
            let row = project_row(
                &x_props,
                column_names,
                &col_ids_x,
                x_var,
                &label,
                &self.snapshot.store,
                Some(x_node_id),
            );
            rows.push(row);

            if let Some(lim) = limit {
                if rows.len() >= lim {
                    break 'outer;
                }
            }
        }

        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    /// Return `true` when `m` qualifies for Phase 3 two-hop chunked execution.
    ///
    /// Eligibility (spec §4.3):
    /// - `use_chunked_pipeline` flag is set.
    /// - Exactly 3 nodes, 2 relationships (two hops).
    /// - Both hops resolve to the **same relationship table**.
    /// - Both hops same direction (both Outgoing).
    /// - No `OPTIONAL MATCH`, no subquery in `WHERE`.
    /// - No aggregate, no `ORDER BY`, no `DISTINCT`.
    /// - No edge-property references in RETURN or WHERE.
    /// - No variable-length hops.
    pub(crate) fn can_use_two_hop_chunked(&self, m: &MatchStatement) -> bool {
        use sparrowdb_cypher::ast::EdgeDir;

        if !self.use_chunked_pipeline {
            return false;
        }
        // Exactly 1 path pattern with 3 nodes and 2 rels.
        if m.pattern.len() != 1 {
            return false;
        }
        let pat = &m.pattern[0];
        if pat.rels.len() != 2 || pat.nodes.len() != 3 {
            return false;
        }
        // Both hops must be directed Outgoing (Phase 3 constraint).
        if pat.rels[0].dir != EdgeDir::Outgoing || pat.rels[1].dir != EdgeDir::Outgoing {
            return false;
        }
        // No variable-length hops.
        if pat.rels[0].min_hops.is_some() || pat.rels[1].min_hops.is_some() {
            return false;
        }
        // No aggregation.
        if has_aggregate_in_return(&m.return_clause.items) {
            return false;
        }
        // No DISTINCT.
        if m.distinct {
            return false;
        }
        // No ORDER BY.
        if !m.order_by.is_empty() {
            return false;
        }
        // No edge-property references.
        for rel in &pat.rels {
            if !rel.var.is_empty() {
                let ref_in_return = m.return_clause.items.iter().any(|item| {
                    column_name_for_item(item)
                        .split_once('.')
                        .is_some_and(|(v, _)| v == rel.var.as_str())
                });
                if ref_in_return {
                    return false;
                }
                if let Some(ref wexpr) = m.where_clause {
                    if expr_references_var(wexpr, rel.var.as_str()) {
                        return false;
                    }
                }
            }
        }
        // Only simple WHERE predicates.
        if let Some(ref wexpr) = m.where_clause {
            if !is_simple_where_for_chunked(wexpr) {
                return false;
            }
        }
        // Inline prop filters on node patterns are not evaluated by the chunked
        // two-hop path — fall back to the row engine.  (See #362.)
        if pat.nodes.iter().any(|n| !n.props.is_empty()) {
            return false;
        }
        // Inline prop filters on relationship patterns are not evaluated by the
        // chunked two-hop path — fall back to the row engine.  (See #367.)
        if pat.rels.iter().any(|r| !r.props.is_empty()) {
            return false;
        }
        // id(n) and other NodeRef-dependent functions require the row engine (#372).
        if return_requires_row_engine(&m.return_clause.items) {
            return false;
        }
        // Both hops must resolve to the same relationship table.
        let src_label = pat.nodes[0].labels.first().cloned().unwrap_or_default();
        let mid_label = pat.nodes[1].labels.first().cloned().unwrap_or_default();
        let dst_label = pat.nodes[2].labels.first().cloned().unwrap_or_default();
        let rel_type1 = &pat.rels[0].rel_type;
        let rel_type2 = &pat.rels[1].rel_type;

        // Both rel types must be identical (including both-empty).
        // Allowing one empty + one non-empty would silently ignore the typed hop
        // in execute_two_hop_chunked which only uses rels[0].rel_type.
        if rel_type1 != rel_type2 {
            return false;
        }

        // Resolve the shared rel table: src→mid and mid→dst must map to same table.
        let catalog = &self.snapshot.catalog;
        let tables = catalog.list_rel_tables_with_ids();

        let hop1_matches: Vec<_> = tables
            .iter()
            .filter(|(_, sid, did, rt)| {
                let type_ok = rel_type1.is_empty() || rt == rel_type1;
                let src_ok = catalog
                    .get_label(&src_label)
                    .ok()
                    .flatten()
                    .map(|id| id as u32 == *sid as u32)
                    .unwrap_or(false);
                let mid_ok = catalog
                    .get_label(&mid_label)
                    .ok()
                    .flatten()
                    .map(|id| id as u32 == *did as u32)
                    .unwrap_or(false);
                type_ok && src_ok && mid_ok
            })
            .collect();

        // Only enter chunked path if there is exactly one matching rel table.
        let n_tables = hop1_matches.len();
        if n_tables != 1 {
            return false;
        }

        let hop2_id = tables.iter().find(|(_, sid, did, rt)| {
            let type_ok = rel_type2.is_empty() || rt == rel_type2;
            let mid_ok = catalog
                .get_label(&mid_label)
                .ok()
                .flatten()
                .map(|id| id as u32 == *sid as u32)
                .unwrap_or(false);
            let dst_ok = catalog
                .get_label(&dst_label)
                .ok()
                .flatten()
                .map(|id| id as u32 == *did as u32)
                .unwrap_or(false);
            type_ok && mid_ok && dst_ok
        });

        // Both hops must resolve, and to the same table.
        match (hop1_matches.first(), hop2_id) {
            (Some((id1, _, _, _)), Some((id2, _, _, _))) => id1 == id2,
            _ => false,
        }
    }

    /// Execute a 2-hop query using the Phase 3 chunked pipeline.
    ///
    /// Pipeline shape (spec §4.3, same-rel 2-hop):
    /// ```text
    /// MaterializeRows(limit?)
    ///   <- optional Filter(ChunkPredicate, dst)
    ///   <- ReadNodeProps(dst)             [only if dst props referenced]
    ///   <- GetNeighbors(hop2, mid_label)  [second hop]
    ///   <- optional Filter(ChunkPredicate, mid)   [intermediate predicates]
    ///   <- ReadNodeProps(mid)             [only if mid props referenced in WHERE]
    ///   <- GetNeighbors(hop1, src_label)  [first hop]
    ///   <- optional Filter(ChunkPredicate, src)
    ///   <- ReadNodeProps(src)             [only if src props referenced]
    ///   <- ScanByLabel(hwm)
    /// ```
    ///
    /// Memory-limit enforcement: if the accumulated output row count in bytes
    /// exceeds `self.memory_limit_bytes`, returns `Error::QueryMemoryExceeded`.
    ///
    /// Path multiplicity: duplicate destination slots from distinct source paths
    /// are emitted as distinct output rows (no implicit dedup — spec §4.1).
    pub(crate) fn execute_two_hop_chunked(
        &self,
        m: &MatchStatement,
        column_names: &[String],
    ) -> Result<QueryResult> {
        use sparrowdb_common::Error as DbError;

        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        let mid_node_pat = &pat.nodes[1];
        let dst_node_pat = &pat.nodes[2];

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let mid_label = mid_node_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();
        let rel_type = pat.rels[0].rel_type.clone();

        // Resolve label IDs.
        let src_label_id = match self.snapshot.catalog.get_label(&src_label)? {
            Some(id) => id as u32,
            None => {
                return Ok(QueryResult {
                    columns: column_names.to_vec(),
                    rows: vec![],
                });
            }
        };
        let mid_label_id = if mid_label.is_empty() {
            src_label_id
        } else {
            match self.snapshot.catalog.get_label(&mid_label)? {
                Some(id) => id as u32,
                None => {
                    return Ok(QueryResult {
                        columns: column_names.to_vec(),
                        rows: vec![],
                    });
                }
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

        // Resolve the shared rel table ID.
        let catalog_rel_id = self
            .snapshot
            .catalog
            .list_rel_tables_with_ids()
            .into_iter()
            .find(|(_, sid, did, rt)| {
                let type_ok = rel_type.is_empty() || rt == &rel_type;
                let src_ok = *sid as u32 == src_label_id;
                let mid_ok = *did as u32 == mid_label_id;
                type_ok && src_ok && mid_ok
            })
            .map(|(cid, _, _, _)| cid as u32)
            .ok_or_else(|| {
                sparrowdb_common::Error::InvalidArgument(
                    "no matching relationship table found for 2-hop".into(),
                )
            })?;

        let hwm_src = self.snapshot.store.hwm_for_label(src_label_id).unwrap_or(0);
        let hwm_dst = self.snapshot.store.hwm_for_label(dst_label_id).unwrap_or(0);
        tracing::debug!(
            engine = "chunked",
            src_label = %src_label,
            mid_label = %mid_label,
            dst_label = %dst_label,
            rel_type = %rel_type,
            hwm_src,
            hwm_dst,
            "executing via chunked pipeline (2-hop)"
        );

        // Variable names from the query.
        let src_var = src_node_pat.var.as_str();
        let mid_var = mid_node_pat.var.as_str();
        let dst_var = dst_node_pat.var.as_str();

        // Collect property col_ids needed for each node.
        // Late materialization: only read what WHERE or RETURN references.
        let mut col_ids_src = collect_col_ids_for_var_from_items(src_var, &m.return_clause.items);
        let mut col_ids_dst = collect_col_ids_for_var_from_items(dst_var, &m.return_clause.items);

        // Mid node properties: only needed if WHERE references them.
        let mut col_ids_mid: Vec<u32> = vec![];

        if let Some(ref wexpr) = m.where_clause {
            collect_col_ids_from_expr_for_var(wexpr, src_var, &mut col_ids_src);
            collect_col_ids_from_expr_for_var(wexpr, dst_var, &mut col_ids_dst);
            collect_col_ids_from_expr_for_var(wexpr, mid_var, &mut col_ids_mid);
        }
        // Inline prop filters.
        for p in &src_node_pat.props {
            let cid = sparrowdb_common::col_id_of(&p.key);
            if !col_ids_src.contains(&cid) {
                col_ids_src.push(cid);
            }
        }
        for p in &mid_node_pat.props {
            let cid = sparrowdb_common::col_id_of(&p.key);
            if !col_ids_mid.contains(&cid) {
                col_ids_mid.push(cid);
            }
        }
        for p in &dst_node_pat.props {
            let cid = sparrowdb_common::col_id_of(&p.key);
            if !col_ids_dst.contains(&cid) {
                col_ids_dst.push(cid);
            }
        }
        // If mid var is referenced in RETURN, read those props too.
        if !mid_var.is_empty() {
            let mid_return_ids =
                collect_col_ids_for_var_from_items(mid_var, &m.return_clause.items);
            for cid in mid_return_ids {
                if !col_ids_mid.contains(&cid) {
                    col_ids_mid.push(cid);
                }
            }
        }

        // Build delta index for this rel table.
        let delta_records = {
            let edge_store = sparrowdb_storage::edge_store::EdgeStore::open(
                &self.snapshot.db_root,
                sparrowdb_storage::edge_store::RelTableId(catalog_rel_id),
            );
            edge_store.and_then(|s| s.read_delta()).unwrap_or_default()
        };

        // Get CSR for the shared rel table.
        let csr = self
            .snapshot
            .csrs
            .get(&catalog_rel_id)
            .cloned()
            .unwrap_or_else(|| sparrowdb_storage::csr::CsrForward::build(0, &[]));

        let avg_degree_hint = self
            .snapshot
            .rel_degree_stats()
            .get(&catalog_rel_id)
            .map(|s| s.mean().ceil() as usize)
            .unwrap_or(8);

        // Compile WHERE predicates.
        let src_pred_opt = m
            .where_clause
            .as_ref()
            .and_then(|wexpr| try_compile_predicate(wexpr, src_var, &col_ids_src));
        let mid_pred_opt = m
            .where_clause
            .as_ref()
            .and_then(|wexpr| try_compile_predicate(wexpr, mid_var, &col_ids_mid));
        let dst_pred_opt = m
            .where_clause
            .as_ref()
            .and_then(|wexpr| try_compile_predicate(wexpr, dst_var, &col_ids_dst));

        let store_arc = Arc::new(sparrowdb_storage::node_store::NodeStore::open(
            self.snapshot.store.root_path(),
        )?);

        let limit = m.limit.map(|l| l as usize);
        let memory_limit = self.memory_limit_bytes;
        let mut rows: Vec<Vec<Value>> = Vec::new();

        // ── BfsArena: reused across both hops ────────────────────────────────
        //
        // BfsArena replaces the old FrontierScratch + per-chunk HashSet dedup
        // pattern. It pairs a double-buffer frontier with a flat bitvector for
        // O(1) visited-set membership testing — no per-chunk HashSet allocation.
        // arena.clear() only zeroes modified bitvector words (O(dirty)), not
        // the full pre-allocated bitvector.
        let node_capacity = (hwm_src.max(hwm_dst) as usize).max(64);
        let mut frontier = BfsArena::new(
            avg_degree_hint * (crate::chunk::CHUNK_CAPACITY / 2),
            node_capacity,
        );

        // ── Memory-limit tracking ─────────────────────────────────────────────
        // We track accumulated output rows as a proxy for memory usage.
        // Each output row is estimated as column_names.len() * 16 bytes.
        let row_size_estimate = column_names.len().max(1) * 16;

        let mut scan = ScanByLabel::new(hwm_src);

        'outer: while let Some(scan_chunk) = scan.next_chunk()? {
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
                let keep: Vec<bool> = (0..src_chunk.len())
                    .map(|i| pred.eval(&src_chunk, i))
                    .collect();
                let mut c = src_chunk;
                c.filter_sel(|i| keep[i]);
                if c.live_len() == 0 {
                    continue;
                }
                c
            } else {
                src_chunk
            };

            // ── Hop 1: GetNeighbors(src → mid) ────────────────────────────────
            let mut gn1 = GetNeighbors::new(
                SingleChunkSource::new(src_chunk.clone()),
                csr.clone(),
                &delta_records,
                src_label_id,
                avg_degree_hint,
            );

            // For each hop-1 output chunk: (src_slot, mid_slot) pairs.
            while let Some(hop1_chunk) = gn1.next_chunk()? {
                // Reset the BfsArena for this hop-1 chunk. clear() is O(1)
                // amortized — no allocations, just length resets + bitmap clear.
                // Must happen BEFORE the memory-limit check so frontier.bytes_used()
                // reflects the cleared (zero) state rather than the previous iteration.
                frontier.clear();

                // Memory-limit check: check after each hop-1 chunk.
                // frontier.bytes_used() is 0 after clear(), so this measures row
                // accumulation only.
                let accum_bytes = rows.len() * row_size_estimate + frontier.bytes_used();
                if accum_bytes > memory_limit {
                    return Err(DbError::QueryMemoryExceeded);
                }

                // ── ReadNodeProps(mid) — only if WHERE references mid ─────────
                let mid_chunk = if !col_ids_mid.is_empty() {
                    let mut rnp = ReadNodeProps::new(
                        SingleChunkSource::new(hop1_chunk),
                        Arc::clone(&store_arc),
                        mid_label_id,
                        COL_ID_DST_SLOT,
                        col_ids_mid.clone(),
                    );
                    match rnp.next_chunk()? {
                        Some(c) => c,
                        None => continue,
                    }
                } else {
                    hop1_chunk
                };

                // ── Filter(mid) — intermediate hop predicate ─────────────────
                let mid_chunk = if let Some(ref pred) = mid_pred_opt {
                    let pred = pred.clone();
                    let keep: Vec<bool> = (0..mid_chunk.len())
                        .map(|i| pred.eval(&mid_chunk, i))
                        .collect();
                    let mut c = mid_chunk;
                    c.filter_sel(|i| keep[i]);
                    if c.live_len() == 0 {
                        continue;
                    }
                    c
                } else {
                    mid_chunk
                };

                // ── Hop 2: GetNeighbors(mid → dst) ────────────────────────────
                let mid_slot_col = mid_chunk.find_column(COL_ID_DST_SLOT);
                let hop1_src_col = mid_chunk.find_column(COL_ID_SRC_SLOT);

                // Collect (src_slot, mid_slot) pairs for live mid rows.
                let live_pairs: Vec<(u64, u64)> = mid_chunk
                    .live_rows()
                    .map(|row_idx| {
                        let mid_slot = mid_slot_col.map(|c| c.data[row_idx]).unwrap_or(0);
                        let src_slot = hop1_src_col.map(|c| c.data[row_idx]).unwrap_or(0);
                        (src_slot, mid_slot)
                    })
                    .collect();

                // Populate BfsArena.current with DEDUPLICATED mid slots for hop-2.
                // Deduplication prevents GetNeighbors from expanding the same mid
                // node multiple times (once per source path through it), which would
                // produce N^2 output rows instead of N.
                // Path multiplicity is preserved by iterating ALL live_pairs at
                // materialization time — we emit one row per distinct (src, mid, dst)
                // triple, which is the correct semantics.
                //
                // arena.visit() uses a RoaringBitmap for O(1) membership checks,
                // eliminating the per-chunk HashSet allocation of the old approach.
                for &(_, mid_slot) in &live_pairs {
                    if frontier.visit(mid_slot) {
                        frontier.current_mut().push(mid_slot);
                    }
                }

                // Use BfsArena.current as input to GetNeighbors.
                // Build a ScanByLabel-equivalent from deduplicated mid slots.
                let mid_slots_chunk = {
                    let data: Vec<u64> = frontier.current().to_vec();
                    let col =
                        crate::chunk::ColumnVector::from_data(crate::chunk::COL_ID_SLOT, data);
                    DataChunk::from_columns(vec![col])
                };

                let mut gn2 = GetNeighbors::new(
                    SingleChunkSource::new(mid_slots_chunk),
                    csr.clone(),
                    &delta_records,
                    mid_label_id,
                    avg_degree_hint,
                );

                while let Some(hop2_chunk) = gn2.next_chunk()? {
                    // hop2_chunk: (mid_slot=COL_ID_SRC_SLOT, dst_slot=COL_ID_DST_SLOT)

                    // ── ReadNodeProps(dst) ────────────────────────────────────
                    let dst_chunk = if !col_ids_dst.is_empty() {
                        let mut rnp = ReadNodeProps::new(
                            SingleChunkSource::new(hop2_chunk),
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
                        hop2_chunk
                    };

                    // ── Filter(dst) ───────────────────────────────────────────
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

                    // ── MaterializeRows ───────────────────────────────────────
                    // For each live (mid_slot, dst_slot) pair, walk backwards
                    // through live_pairs to find all (src_slot, mid_slot) pairs,
                    // emitting one row per (src, mid, dst) path.
                    let hop2_src_col = dst_chunk.find_column(COL_ID_SRC_SLOT); // mid_slot
                    let dst_slot_col = dst_chunk.find_column(COL_ID_DST_SLOT);

                    let src_slot_col_in_scan = src_chunk.find_column(crate::chunk::COL_ID_SLOT);

                    // Build slot→row-index maps once before the triple loop to
                    // avoid O(N) linear scans per output row (WARNING 2).
                    let src_index: std::collections::HashMap<u64, usize> = src_slot_col_in_scan
                        .map(|sc| (0..sc.data.len()).map(|i| (sc.data[i], i)).collect())
                        .unwrap_or_default();

                    let mid_index: std::collections::HashMap<u64, usize> = {
                        let mid_slot_col_in_mid = mid_chunk.find_column(COL_ID_DST_SLOT);
                        mid_slot_col_in_mid
                            .map(|mc| (0..mc.data.len()).map(|i| (mc.data[i], i)).collect())
                            .unwrap_or_default()
                    };

                    for row_idx in dst_chunk.live_rows() {
                        let dst_slot = dst_slot_col.map(|c| c.data[row_idx]).unwrap_or(0);
                        let via_mid_slot = hop2_src_col.map(|c| c.data[row_idx]).unwrap_or(0);

                        // Find all (src, mid) pairs whose mid == via_mid_slot.
                        for &(src_slot, mid_slot) in &live_pairs {
                            if mid_slot != via_mid_slot {
                                continue;
                            }

                            // Path multiplicity: each (src, mid, dst) triple is
                            // a distinct path — emit as a distinct row (no dedup).
                            let src_node = NodeId(((src_label_id as u64) << 32) | src_slot);
                            let mid_node = NodeId(((mid_label_id as u64) << 32) | mid_slot);
                            let dst_node = NodeId(((dst_label_id as u64) << 32) | dst_slot);

                            // Tombstone checks.
                            if self.is_node_tombstoned(src_node)
                                || self.is_node_tombstoned(mid_node)
                                || self.is_node_tombstoned(dst_node)
                            {
                                continue;
                            }

                            // Read src props (from scan chunk, using pre-built index).
                            let src_props = if src_slot_col_in_scan.is_some() {
                                if let Some(&src_ri) = src_index.get(&src_slot) {
                                    build_props_from_chunk(&src_chunk, src_ri, &col_ids_src)
                                } else {
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

                            // Read mid props (from mid_chunk, using pre-built index).
                            let mid_props: Vec<(u32, u64)> = if !col_ids_mid.is_empty() {
                                if let Some(&mid_ri) = mid_index.get(&mid_slot) {
                                    build_props_from_chunk(&mid_chunk, mid_ri, &col_ids_mid)
                                } else {
                                    let nullable = self
                                        .snapshot
                                        .store
                                        .get_node_raw_nullable(mid_node, &col_ids_mid)?;
                                    nullable
                                        .into_iter()
                                        .filter_map(|(cid, opt)| opt.map(|v| (cid, v)))
                                        .collect()
                                }
                            } else {
                                vec![]
                            };

                            // Read dst props (from dst_chunk).
                            let dst_props =
                                build_props_from_chunk(&dst_chunk, row_idx, &col_ids_dst);

                            // Apply WHERE clause (fallback for complex predicates).
                            if let Some(ref where_expr) = m.where_clause {
                                let mut row_vals = build_row_vals(
                                    &src_props,
                                    src_var,
                                    &col_ids_src,
                                    &self.snapshot.store,
                                );
                                row_vals.extend(build_row_vals(
                                    &mid_props,
                                    mid_var,
                                    &col_ids_mid,
                                    &self.snapshot.store,
                                ));
                                row_vals.extend(build_row_vals(
                                    &dst_props,
                                    dst_var,
                                    &col_ids_dst,
                                    &self.snapshot.store,
                                ));
                                row_vals.extend(self.dollar_params());
                                if !self.eval_where_graph(where_expr, &row_vals) {
                                    continue;
                                }
                            }

                            // Project output row using existing three-var helper.
                            let row = project_three_var_row(
                                &src_props,
                                &mid_props,
                                &dst_props,
                                &m.return_clause.items,
                                src_var,
                                mid_var,
                                &self.snapshot.store,
                            );
                            rows.push(row);

                            // Memory-limit check on accumulated output.
                            if rows.len() * row_size_estimate > memory_limit {
                                return Err(DbError::QueryMemoryExceeded);
                            }

                            // LIMIT short-circuit.
                            if let Some(lim) = limit {
                                if rows.len() >= lim {
                                    break 'outer;
                                }
                            }
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
                    Some(node_id),
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

// ── DstSlotProjector ──────────────────────────────────────────────────────────

/// Projects `COL_ID_DST_SLOT` from a `GetNeighbors` output chunk to `COL_ID_SLOT`.
///
/// `GetNeighbors` emits `(COL_ID_SRC_SLOT, COL_ID_DST_SLOT)` pairs.
/// `SlotIntersect` operates on `COL_ID_SLOT` columns.  This thin adaptor
/// renames the `COL_ID_DST_SLOT` column to `COL_ID_SLOT` so that
/// `SlotIntersect` can be wired directly to `GetNeighbors` output.
struct DstSlotProjector<C: PipelineOperator> {
    child: C,
}

impl<C: PipelineOperator> DstSlotProjector<C> {
    fn new(child: C) -> Self {
        DstSlotProjector { child }
    }
}

impl<C: PipelineOperator> PipelineOperator for DstSlotProjector<C> {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        use crate::chunk::ColumnVector;

        loop {
            let chunk = match self.child.next_chunk()? {
                Some(c) => c,
                None => return Ok(None),
            };

            if chunk.is_empty() {
                continue;
            }

            // Extract dst slots from live rows and build a new COL_ID_SLOT chunk.
            let dst_col = match chunk.find_column(COL_ID_DST_SLOT) {
                Some(c) => c,
                None => continue,
            };

            let data: Vec<u64> = chunk.live_rows().map(|i| dst_col.data[i]).collect();
            if data.is_empty() {
                continue;
            }
            let col = ColumnVector::from_data(crate::chunk::COL_ID_SLOT, data);
            return Ok(Some(DataChunk::from_columns(vec![col])));
        }
    }
}

// ── MutualNeighbors helpers ───────────────────────────────────────────────────

/// Return `true` if `expr` is `id(var_name)`.
fn is_id_call(expr: &Expr, var_name: &str) -> bool {
    match expr {
        Expr::FnCall { name, args } => {
            name.eq_ignore_ascii_case("id")
                && args.len() == 1
                && matches!(&args[0], Expr::Var(v) if v.as_str() == var_name)
        }
        _ => false,
    }
}

/// Return `true` if `expr` is a `$param` literal.
fn is_param_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Literal::Param(_)))
}

/// Return `true` ONLY if `expr` is a pure conjunction of `id(var)=$param`
/// equalities for the two given variable names.
///
/// Any OR, property access, function call other than `id()`, or other expression
/// shape returns `false` — this is the strict purity check that prevents
/// `WHERE id(a)=$aid OR id(b)=$bid` from incorrectly passing the fast-path guard.
fn where_is_only_id_param_conjuncts(expr: &Expr, a_var: &str, b_var: &str) -> bool {
    match expr {
        Expr::And(left, right) => {
            where_is_only_id_param_conjuncts(left, a_var, b_var)
                && where_is_only_id_param_conjuncts(right, a_var, b_var)
        }
        Expr::BinOp {
            left,
            op: BinOpKind::Eq,
            right,
        } => {
            // Must be id(a_var)=$param, id(b_var)=$param, or either commuted.
            (is_id_call(left, a_var) || is_id_call(left, b_var)) && is_param_literal(right)
                || is_param_literal(left) && (is_id_call(right, a_var) || is_id_call(right, b_var))
        }
        _ => false,
    }
}

/// Extract the slot number for `var_name` from `id(var_name) = $param` in WHERE.
///
/// Looks up the parameter value in `params`, then decodes the slot from the
/// NodeId encoding: `slot = node_id & 0xFFFF_FFFF`.
///
/// Returns `None` when the param is not found or the label doesn't match.
fn extract_id_param_slot(
    where_clause: Option<&Expr>,
    var_name: &str,
    params: &std::collections::HashMap<String, crate::types::Value>,
    expected_label_id: u32,
) -> Option<u64> {
    let wexpr = where_clause?;
    let param_name = find_id_param_name(wexpr, var_name)?;
    let val = params.get(&param_name)?;

    // The param value is expected to be a NodeId (Int64 or NodeRef).
    let raw_node_id: u64 = match val {
        crate::types::Value::Int64(n) => *n as u64,
        crate::types::Value::NodeRef(nid) => nid.0,
        _ => return None,
    };

    let (label_id, slot) = super::node_id_parts(raw_node_id);
    if label_id != expected_label_id {
        return None;
    }
    Some(slot)
}

/// Find the parameter name in `id(var_name) = $param` expressions.
fn find_id_param_name(expr: &Expr, var_name: &str) -> Option<String> {
    match expr {
        Expr::BinOp { left, op, right } => {
            if *op == BinOpKind::Eq {
                if is_id_call(left, var_name) {
                    if let Expr::Literal(Literal::Param(p)) = right.as_ref() {
                        return Some(p.clone());
                    }
                }
                if is_id_call(right, var_name) {
                    if let Expr::Literal(Literal::Param(p)) = left.as_ref() {
                        return Some(p.clone());
                    }
                }
            }
            find_id_param_name(left, var_name).or_else(|| find_id_param_name(right, var_name))
        }
        Expr::And(a, b) => {
            find_id_param_name(a, var_name).or_else(|| find_id_param_name(b, var_name))
        }
        _ => None,
    }
}

/// Scan a label's slots to find the first node that matches all `props` filters.
///
/// Used by `execute_mutual_neighbors_chunked` when endpoints are bound via
/// inline props (`{uid: 0}`) rather than `WHERE id(a) = $param`.
///
/// # Performance
///
/// 1. Property index (O(1)) — checked first when an index exists for `(label_id, prop)`.
/// 2. Single-column bulk read — reads the column file **once**, scans in memory.
///    O(N) in memory instead of O(N) × `fs::read` calls (the per-slot path
///    re-reads the entire column file on every slot, causing 4000+ disk reads
///    for a typical social-graph dataset).
/// 3. Per-slot fallback — used only for complex/multi-prop filters.
fn find_slot_by_props(
    store: &NodeStore,
    label_id: u32,
    hwm: u64,
    props: &[sparrowdb_cypher::ast::PropEntry],
    params: &std::collections::HashMap<String, crate::types::Value>,
    prop_index: &PropertyIndex,
) -> Option<u64> {
    if props.is_empty() || hwm == 0 {
        return None;
    }

    // Fast path: property index (O(1) when an index exists for this label+prop).
    if let Some(slots) = try_index_lookup_for_props(props, label_id, prop_index) {
        return slots.into_iter().next().map(|s| s as u64);
    }

    // Single-prop bulk-read path: read the column file once, scan in memory.
    // This replaces O(N) per-slot `fs::read` calls (each re-reads the whole file)
    // with O(1) file reads + O(N) in-memory iteration.
    if props.len() == 1 {
        let filter = &props[0];
        let col_id = prop_name_to_col_id(&filter.key);

        // Encode the filter value to its raw u64 storage representation.
        let target_raw_opt: Option<u64> = match &filter.value {
            Expr::Literal(Literal::Int(n)) => Some(StoreValue::Int64(*n).to_u64()),
            Expr::Literal(Literal::String(s)) if s.len() <= 7 => {
                Some(StoreValue::Bytes(s.as_bytes().to_vec()).to_u64())
            }
            // Params, floats, long strings: fall through to per-slot path.
            _ => None,
        };

        if let Some(target_raw) = target_raw_opt {
            let col_data = match store.read_col_all(label_id, col_id) {
                Ok(d) => d,
                Err(_) => return None,
            };
            let null_bitmap = store.read_null_bitmap_all(label_id, col_id).ok().flatten();

            for (slot, &raw) in col_data.iter().enumerate().take(hwm as usize) {
                // Check presence before equality: in pre-SPA-207 data, raw == 0
                // means absent (not the integer zero), so a search for uid:0
                // must not match an absent slot.
                let is_present = match &null_bitmap {
                    // No bitmap (pre-SPA-207 data): use `raw != 0` sentinel.
                    None => raw != 0,
                    // Bitmap present: check the explicit null bit.
                    Some(bits) => bits.get(slot).copied().unwrap_or(false),
                };
                if !is_present {
                    continue;
                }
                if raw != target_raw {
                    continue;
                }
                return Some(slot as u64);
            }
            return None;
        }
    }

    // Fallback: per-slot read for complex/multi-prop filters.
    let col_ids: Vec<u32> = props.iter().map(|p| prop_name_to_col_id(&p.key)).collect();
    for slot in 0..hwm {
        let node_id = NodeId(((label_id as u64) << 32) | slot);
        let Ok(raw_props) = store.get_node_raw_nullable(node_id, &col_ids) else {
            continue;
        };
        let stored: Vec<(u32, u64)> = raw_props
            .into_iter()
            .filter_map(|(c, opt)| opt.map(|v| (c, v)))
            .collect();
        if matches_prop_filter_static(&stored, props, params, store) {
            return Some(slot);
        }
    }
    None
}

/// Returns `true` when the RETURN clause contains expressions that the chunked
/// pipeline paths cannot handle and the query must fall back to the row engine.
///
/// Specifically, `id(n)` and other `NodeRef`-dependent functions require
/// `Value::NodeRef` to be injected into the row map by the eval path.  The
/// chunked `project_row` / `project_hop_row` paths match by column-name string;
/// when an AS alias is present the column name is the alias rather than
/// `"id(n)"`, so those paths return Null.  The row engine's eval path resolves
/// `id(n)` correctly via `eval_expr` (#372).
fn return_requires_row_engine(items: &[ReturnItem]) -> bool {
    needs_node_ref_in_return(items)
}
