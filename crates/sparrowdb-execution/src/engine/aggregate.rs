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

        // Apply ORDER BY on the projected rows (which still have all WITH aliases)
        // before projecting down to RETURN columns — this allows ORDER BY on columns
        // that are not in the RETURN clause (e.g. ORDER BY age when only name is returned).
        let mut ordered_projected = projected;
        if !m.order_by.is_empty() {
            ordered_projected.sort_by(|a, b| {
                for (expr, dir) in &m.order_by {
                    let val_a = eval_expr(expr, a);
                    let val_b = eval_expr(expr, b);
                    let cmp = compare_values(&val_a, &val_b);
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

        // Apply SKIP / LIMIT before final projection.
        if let Some(skip) = m.skip {
            let skip = (skip as usize).min(ordered_projected.len());
            ordered_projected.drain(0..skip);
        }
        if let Some(lim) = m.limit {
            ordered_projected.truncate(lim as usize);
        }

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

    /// Aggregate a set of raw scan rows through a list of WITH items that
    /// include aggregate expressions (COUNT(*), collect(), etc.).
    ///
    /// Returns one `HashMap<String, Value>` per unique grouping key.
    pub(crate) fn aggregate_with_items(
        &self,
        rows: &[HashMap<String, Value>],
        items: &[sparrowdb_cypher::ast::WithItem],
    ) -> Vec<HashMap<String, Value>> {
        // Classify each WITH item as key or aggregate.
        let key_indices: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, item)| !is_aggregate_expr(&item.expr))
            .map(|(i, _)| i)
            .collect();
        let agg_indices: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, item)| is_aggregate_expr(&item.expr))
            .map(|(i, _)| i)
            .collect();

        // Build groups.
        let mut group_keys: Vec<Vec<Value>> = Vec::new();
        let mut group_accum: Vec<Vec<Vec<Value>>> = Vec::new(); // [group][agg_pos] → values

        for row_vals in rows {
            let key: Vec<Value> = key_indices
                .iter()
                .map(|&i| eval_expr(&items[i].expr, row_vals))
                .collect();
            let group_idx = if let Some(pos) = group_keys.iter().position(|k| k == &key) {
                pos
            } else {
                group_keys.push(key);
                group_accum.push(vec![vec![]; agg_indices.len()]);
                group_keys.len() - 1
            };
            for (ai, &ri) in agg_indices.iter().enumerate() {
                match &items[ri].expr {
                    sparrowdb_cypher::ast::Expr::CountStar => {
                        group_accum[group_idx][ai].push(Value::Int64(1));
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, args }
                        if name.to_lowercase() == "collect" =>
                    {
                        let val = if !args.is_empty() {
                            eval_expr(&args[0], row_vals)
                        } else {
                            Value::Null
                        };
                        if !matches!(val, Value::Null) {
                            group_accum[group_idx][ai].push(val);
                        }
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, args }
                        if matches!(
                            name.to_lowercase().as_str(),
                            "count" | "sum" | "avg" | "min" | "max"
                        ) =>
                    {
                        let val = if !args.is_empty() {
                            eval_expr(&args[0], row_vals)
                        } else {
                            Value::Null
                        };
                        if !matches!(val, Value::Null) {
                            group_accum[group_idx][ai].push(val);
                        }
                    }
                    _ => {}
                }
            }
        }

        // If no rows were seen, still produce one output row for global aggregates
        // (e.g. COUNT(*) over an empty scan returns 0).
        if rows.is_empty() && key_indices.is_empty() {
            let mut out_row: HashMap<String, Value> = HashMap::new();
            for &ri in &agg_indices {
                let val = match &items[ri].expr {
                    sparrowdb_cypher::ast::Expr::CountStar => Value::Int64(0),
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "collect" =>
                    {
                        Value::List(vec![])
                    }
                    _ => Value::Int64(0),
                };
                out_row.insert(items[ri].alias.clone(), val);
            }
            return vec![out_row];
        }

        // Finalize each group.
        let mut result: Vec<HashMap<String, Value>> = Vec::new();
        for (gi, key_vals) in group_keys.iter().enumerate() {
            let mut out_row: HashMap<String, Value> = HashMap::new();
            // Insert key values.
            for (ki, &ri) in key_indices.iter().enumerate() {
                out_row.insert(items[ri].alias.clone(), key_vals[ki].clone());
            }
            // Finalize aggregates.
            for (ai, &ri) in agg_indices.iter().enumerate() {
                let accum = &group_accum[gi][ai];
                let val = match &items[ri].expr {
                    sparrowdb_cypher::ast::Expr::CountStar => Value::Int64(accum.len() as i64),
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "collect" =>
                    {
                        Value::List(accum.clone())
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "count" =>
                    {
                        Value::Int64(accum.len() as i64)
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "sum" =>
                    {
                        let sum: i64 = accum
                            .iter()
                            .filter_map(|v| {
                                if let Value::Int64(n) = v {
                                    Some(*n)
                                } else {
                                    None
                                }
                            })
                            .sum();
                        Value::Int64(sum)
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "min" =>
                    {
                        accum
                            .iter()
                            .min_by(|a, b| compare_values(a, b))
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                    sparrowdb_cypher::ast::Expr::FnCall { name, .. }
                        if name.to_lowercase() == "max" =>
                    {
                        accum
                            .iter()
                            .max_by(|a, b| compare_values(a, b))
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                };
                out_row.insert(items[ri].alias.clone(), val);
            }
            result.push(out_row);
        }
        result
    }
}
