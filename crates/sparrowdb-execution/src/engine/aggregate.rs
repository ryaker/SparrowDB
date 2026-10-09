//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

impl Engine {
    // ── UNION ─────────────────────────────────────────────────────────────────

    /// Execute `stmt1 UNION [ALL] stmt2`.
    ///
    /// Concatenates the row sets from both sides.  When `!all`, duplicate rows
    /// are eliminated using the same `deduplicate_rows` logic used by DISTINCT.
    /// Both sides must produce the same number of columns; column names are taken
    /// from the left side.
    pub(crate) fn execute_union(&mut self, u: UnionStatement) -> Result<QueryResult> {
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
    pub(crate) fn execute_match_with(&self, m: &MatchWithStatement) -> Result<QueryResult> {
        // Step 1: collect intermediate rows from MATCH scan.
        let intermediate = self.collect_match_rows_for_with(
            &m.match_patterns,
            m.match_where.as_ref(),
            &m.with_clause,
        )?;

        // Step 2: check if WITH clause has aggregate expressions.
        // If so, we aggregate the intermediate rows first, producing one output row
        // per unique grouping key.
        let has_agg = m
            .with_clause
            .items
            .iter()
            .any(|item| is_aggregate_expr(&item.expr));

        let projected: Vec<HashMap<String, Value>> = if has_agg {
            // Aggregate the intermediate rows into a set of projected rows.
            let agg_rows = self.aggregate_with_items(&intermediate, &m.with_clause.items);
            // Apply WHERE filter on the aggregated rows.
            agg_rows
                .into_iter()
                .filter(|with_vals| {
                    if let Some(ref where_expr) = m.with_clause.where_clause {
                        let mut with_vals_p = with_vals.clone();
                        with_vals_p.extend(self.dollar_params());
                        self.eval_where_graph(where_expr, &with_vals_p)
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
            // Non-aggregate path: project each row through the WITH items.
            let mut projected: Vec<HashMap<String, Value>> = Vec::new();
            for row_vals in &intermediate {
                let mut with_vals: HashMap<String, Value> = HashMap::new();
                for item in &m.with_clause.items {
                    let val = self.eval_expr_graph(&item.expr, row_vals);
                    with_vals.insert(item.alias.clone(), val);
                    // SPA-134: if the WITH item is a bare Var (e.g. `n AS person`),
                    // also inject the NodeRef under the alias so that EXISTS subqueries
                    // in a subsequent WHERE clause can resolve the source node.
                    if let sparrowdb_cypher::ast::Expr::Var(ref src_var) = item.expr {
                        if let Some(node_ref) = row_vals.get(src_var) {
                            if matches!(node_ref, Value::NodeRef(_)) {
                                with_vals.insert(item.alias.clone(), node_ref.clone());
                                with_vals.insert(
                                    format!("{}.__node_id__", item.alias),
                                    node_ref.clone(),
                                );
                            }
                        }
                        // Also check __node_id__ key.
                        let nid_key = format!("{src_var}.__node_id__");
                        if let Some(node_ref) = row_vals.get(&nid_key) {
                            with_vals
                                .insert(format!("{}.__node_id__", item.alias), node_ref.clone());
                        }
                    }
                }
                if let Some(ref where_expr) = m.with_clause.where_clause {
                    let mut with_vals_p = with_vals.clone();
                    with_vals_p.extend(self.dollar_params());
                    if !self.eval_where_graph(where_expr, &with_vals_p) {
                        continue;
                    }
                }
                // Merge dollar_params into the projected row so that downstream
                // RETURN/ORDER-BY/SKIP/LIMIT expressions can resolve $param references.
                with_vals.extend(self.dollar_params());
                projected.push(with_vals);
            }
            projected
        };

        // Step 3: project RETURN from the WITH-projected rows.
        let column_names = extract_return_column_names(&m.return_clause.items);

        // WITH-level ORDER BY / SKIP / LIMIT shape the WITH output, so they run
        // before RETURN sees the rows (and before a RETURN aggregate groups them).
        // Evaluated graph-aware (#477) so `ORDER BY bm25_score(n.text, 'q')`
        // resolves even when the score was not also WITH-aliased.
        let mut ordered_projected = projected;
        self.order_skip_limit_projected(
            &mut ordered_projected,
            &m.with_order_by,
            m.with_skip,
            m.with_limit,
        );

        // #558: an aggregate in the RETURN after a non-aggregating WITH must
        // aggregate the WITH output (one group per distinct key), not be
        // evaluated per row. RETURN-level ORDER BY / SKIP / LIMIT then apply to
        // the aggregated rows, exactly as on a plain `MATCH … RETURN agg`.
        if has_aggregate_in_return(&m.return_clause.items) {
            let mut rows = super::aggregate_rows(self, &ordered_projected, &m.return_clause.items);
            if m.distinct {
                deduplicate_rows(&mut rows);
            }
            super::sort_rows_by(
                &mut rows,
                &m.order_by,
                &m.return_clause.items,
                &column_names,
            );
            if let Some(skip) = m.skip {
                rows.drain(0..(skip as usize).min(rows.len()));
            }
            if let Some(lim) = m.limit {
                rows.truncate(lim as usize);
            }
            return Ok(QueryResult {
                columns: column_names,
                rows,
            });
        }

        // Non-aggregate RETURN: ORDER BY runs on the projected rows (which still
        // have all WITH aliases) before projecting down to RETURN columns — this
        // allows ORDER BY on columns that are not in the RETURN clause (e.g.
        // ORDER BY age when only name is returned).
        self.order_skip_limit_projected(&mut ordered_projected, &m.order_by, m.skip, m.limit);

        let mut rows: Vec<Vec<Value>> = ordered_projected
            .iter()
            .map(|with_vals| {
                m.return_clause
                    .items
                    .iter()
                    .map(|item| self.eval_expr_graph(&item.expr, with_vals))
                    .collect()
            })
            .collect();

        if m.distinct {
            deduplicate_rows(&mut rows);
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
    }

    /// Sort `rows` by `order_by` (graph-aware), then apply SKIP and LIMIT.
    fn order_skip_limit_projected(
        &self,
        rows: &mut Vec<HashMap<String, Value>>,
        order_by: &[(Expr, SortDir)],
        skip: Option<u64>,
        limit: Option<u64>,
    ) {
        if !order_by.is_empty() {
            rows.sort_by(|a, b| {
                for (expr, dir) in order_by {
                    let cmp = compare_values(
                        &self.eval_expr_graph(expr, a),
                        &self.eval_expr_graph(expr, b),
                    );
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
        if let Some(skip) = skip {
            rows.drain(0..(skip as usize).min(rows.len()));
        }
        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }
    }

    /// Aggregate a set of raw scan rows through a list of WITH items that
    /// include aggregate expressions (COUNT(*), collect(), etc.).
    ///
    /// Returns one `HashMap<String, Value>` per unique grouping key.
    pub(crate) fn aggregate_with_items(
        &self,
        rows: &[HashMap<String, Value>],
        items: &[sparrowdb_cypher::ast::WithItem],
    ) -> Vec<HashMap<String, Value>> {
        // A WITH that aggregates groups exactly like a RETURN that aggregates:
        // one implementation (`aggregate_rows`) owns grouping, NULL-skipping,
        // DISTINCT, and the per-function finalisation (SUM int/float, AVG,
        // MIN/MAX, empty-input defaults). Reusing it keeps the two in lockstep.
        let return_items: Vec<ReturnItem> = items
            .iter()
            .map(|item| ReturnItem {
                expr: item.expr.clone(),
                alias: Some(item.alias.clone()),
            })
            .collect();
        super::aggregate_rows(self, rows, &return_items)
            .into_iter()
            .map(|vals| {
                items
                    .iter()
                    .map(|item| item.alias.clone())
                    .zip(vals)
                    .collect()
            })
            .collect()
    }
}
