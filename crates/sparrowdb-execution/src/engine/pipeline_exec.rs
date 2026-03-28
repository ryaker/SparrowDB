//! Opt-in chunked pipeline execution entry points (Phase 1, #299).
//!
//! This module wires the Phase 1 pipeline data structures into the existing
//! engine without modifying any row-at-a-time code paths.
//!
//! When `Engine::use_chunked_pipeline` is `true` AND the query shape qualifies,
//! these methods are called instead of the row-at-a-time equivalents.
//!
//! # Phase 1 supported shape
//!
//! Single-label scan with no hops and no aggregation:
//! `MATCH (n:Label) [WHERE n.prop op val] RETURN n.prop1, n.prop2`
//!
//! All other shapes fall back to the row-at-a-time engine.

use super::*;
use crate::pipeline::ScanByLabel;

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
