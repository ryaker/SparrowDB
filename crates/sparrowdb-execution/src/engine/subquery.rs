//! CALL { } subquery execution (issue #290).
//!
//! Handles both unit subqueries (no outer variable imports) and correlated
//! subqueries (imports via `WITH` inside the braces).

use std::collections::HashMap;

use sparrowdb_cypher::ast::{
    PipelineStage, PipelineStatement, ReturnClause, Statement, WithClause,
};

use super::*;
use crate::types::{QueryResult, Value};

impl Engine {
    // ── Standalone CALL { } RETURN … ─────────────────────────────────────────

    /// Execute a standalone `CALL { … } [RETURN …]` statement.
    ///
    /// For a unit subquery (`imports` is empty) the inner statement is executed
    /// independently and its result rows are extended with the optional outer
    /// `RETURN` clause.
    ///
    /// Note: correlated standalone form (`imports` non-empty without an outer
    /// MATCH) is not meaningful because there is no outer scope to import from.
    /// In that case the imports are silently ignored and the subquery runs as a
    /// unit subquery.
    pub(crate) fn execute_call_subquery(
        &self,
        subquery: &Statement,
        _imports: &[String],
        return_clause: Option<&ReturnClause>,
    ) -> Result<QueryResult> {
        // Execute the inner subquery statement.
        let inner = self.execute_read_stmt(subquery)?;

        if let Some(ret) = return_clause {
            // Project the outer RETURN clause over the subquery's result rows.
            // Build a name → column-index map from the inner result.
            let col_idx: HashMap<&str, usize> = inner
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.as_str(), i))
                .collect();

            let out_cols: Vec<String> = ret
                .items
                .iter()
                .map(|item| {
                    item.alias
                        .clone()
                        .unwrap_or_else(|| expr_to_col_name(&item.expr))
                })
                .collect();

            let out_rows: Vec<Vec<Value>> = inner
                .rows
                .iter()
                .map(|row| {
                    // Build an env map from the inner row.
                    let env: HashMap<String, Value> = inner
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    ret.items
                        .iter()
                        .map(|item| {
                            // Try direct column lookup first, then full expression eval.
                            match &item.expr {
                                sparrowdb_cypher::ast::Expr::Var(name) => col_idx
                                    .get(name.as_str())
                                    .and_then(|&i| row.get(i))
                                    .cloned()
                                    .unwrap_or(Value::Null),
                                other => eval_expr(other, &env),
                            }
                        })
                        .collect()
                })
                .collect();

            Ok(QueryResult {
                columns: out_cols,
                rows: out_rows,
            })
        } else {
            Ok(inner)
        }
    }

    // ── Pipeline stage: PipelineStage::CallSubquery ───────────────────────────

    /// Execute a `CALL { }` pipeline stage against a set of outer rows.
    ///
    /// For each outer row in `current_rows`:
    ///   - Unit subquery (`imports` empty): run the inner statement once and
    ///     cross-join its output with every outer row.
    ///   - Correlated subquery (`imports` non-empty): inject imported variable
    ///     bindings from the outer row into the inner statement's execution
    ///     context, then append the inner result columns to the outer row.
    ///
    /// In both cases, if the inner subquery produces zero rows for a given outer
    /// row, that outer row is **dropped** (inner-join semantics, consistent with
    /// Neo4j's CALL { } behaviour).
    pub(crate) fn execute_pipeline_call_subquery_stage(
        &self,
        subquery: &Statement,
        imports: &[String],
        current_rows: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut next_rows: Vec<HashMap<String, Value>> = Vec::new();

        if imports.is_empty() {
            // Unit subquery: execute once, cross-join with every outer row.
            let inner = self.execute_read_stmt(subquery)?;
            let inner_col_names: Vec<&str> = inner.columns.iter().map(|s| s.as_str()).collect();

            for outer_row in &current_rows {
                for inner_row in &inner.rows {
                    let mut merged = outer_row.clone();
                    for (col, val) in inner_col_names.iter().zip(inner_row.iter()) {
                        merged.insert(col.to_string(), val.clone());
                    }
                    next_rows.push(merged);
                }
            }
        } else {
            // Correlated subquery: re-execute for each outer row with imported bindings.
            for outer_row in &current_rows {
                let inner = self.execute_read_stmt_with_bindings(subquery, imports, outer_row)?;
                let inner_col_names: Vec<&str> = inner.columns.iter().map(|s| s.as_str()).collect();

                for inner_row in &inner.rows {
                    let mut merged = outer_row.clone();
                    for (col, val) in inner_col_names.iter().zip(inner_row.iter()) {
                        merged.insert(col.to_string(), val.clone());
                    }
                    next_rows.push(merged);
                }
            }
        }

        Ok(next_rows)
    }

    // ── Read-only statement dispatch ─────────────────────────────────────────

    /// Execute a read-only `Statement` from `&self`.
    ///
    /// Only non-mutating statement variants are supported.  Mutation statements
    /// (`CREATE`, `MERGE`, etc.) return an error — they must go through
    /// `GraphDb::execute` which owns a write transaction.
    pub(crate) fn execute_read_stmt(&self, stmt: &Statement) -> Result<QueryResult> {
        self.execute_read_stmt_with_bindings(stmt, &[], &HashMap::new())
    }

    /// Execute a read-only statement with optional imported variable bindings.
    ///
    /// `imports` names the subset of `outer_row` keys to inject as parameters
    /// (unused for unit subqueries — pass an empty slice and empty map).
    ///
    /// For correlated execution the approach is to rewrite the subquery as a
    /// single-stage pipeline that starts from the imported bindings.  This works
    /// for the common case:
    ///
    /// ```cypher
    /// CALL { WITH n MATCH (n)-[:KNOWS]->(f) RETURN count(f) AS fc }
    /// ```
    ///
    /// where `n` is a `NodeRef` in `outer_row`.  We inject the outer binding as
    /// the leading row-set and feed it into `execute_pipeline_match_stage`.
    pub(crate) fn execute_read_stmt_with_bindings(
        &self,
        stmt: &Statement,
        imports: &[String],
        outer_row: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        // Build the outer binding map (empty for unit subqueries).
        let outer_binding: HashMap<String, Value> = if imports.is_empty() {
            HashMap::new()
        } else {
            let mut b: HashMap<String, Value> = imports
                .iter()
                .filter_map(|k| outer_row.get(k).map(|v| (k.clone(), v.clone())))
                .collect();
            // Carry __node_id__ sentinel entries used by the pipeline match stage.
            for key in imports {
                let nid_key = format!("{key}.__node_id__");
                if let Some(val) = outer_row.get(&nid_key) {
                    b.insert(nid_key, val.clone());
                }
                if let Some(nr @ Value::NodeRef(_)) = outer_row.get(key) {
                    b.insert(key.clone(), nr.clone());
                    b.insert(format!("{key}.__node_id__"), nr.clone());
                }
            }
            b
        };

        let use_seeded = !outer_binding.is_empty();

        match stmt {
            Statement::Match(m) => {
                if use_seeded {
                    // Correlated: wrap in a one-stage pipeline seeded with outer binding.
                    let p = PipelineStatement {
                        leading_match: Some(m.pattern.clone()),
                        leading_where: m.where_clause.clone(),
                        leading_unwind: None,
                        stages: vec![],
                        return_clause: m.return_clause.clone(),
                        return_order_by: m.order_by.clone(),
                        return_skip: m.skip,
                        return_limit: m.limit,
                        distinct: m.distinct,
                    };
                    self.execute_pipeline_seeded(&p, vec![outer_binding])
                } else {
                    self.execute_match(m)
                }
            }
            Statement::Pipeline(p) => {
                if use_seeded {
                    self.execute_pipeline_seeded(p, vec![outer_binding])
                } else {
                    self.execute_pipeline(p)
                }
            }
            Statement::MatchWith(mw) => self.execute_match_with(mw),
            Statement::OptionalMatch(om) => self.execute_optional_match(om),
            Statement::MatchOptionalMatch(mom) => self.execute_match_optional_match(mom),
            Statement::Unwind(u) => self.execute_unwind(u),
            Statement::Call(c) => self.execute_call(c),
            Statement::Checkpoint | Statement::Optimize => Ok(QueryResult::empty(vec![])),
            Statement::CreateIndex { .. } | Statement::CreateConstraint { .. } => {
                Ok(QueryResult::empty(vec![]))
            }
            Statement::CallSubquery {
                subquery,
                imports: sub_imports,
                return_clause,
            } => self.execute_call_subquery(subquery, sub_imports, return_clause.as_ref()),
            other => Err(sparrowdb_common::Error::InvalidArgument(format!(
                "CALL {{ }}: unsupported or mutating statement kind in subquery: {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }

    /// Execute a `PipelineStatement` seeded with an initial set of rows instead
    /// of scanning from the leading MATCH/UNWIND clause.
    ///
    /// Used by correlated `CALL { WITH imports … }` to feed the outer row
    /// binding directly into the pipeline's first MATCH stage.
    fn execute_pipeline_seeded(
        &self,
        p: &PipelineStatement,
        seed_rows: Vec<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        // Start from the seed rows rather than scanning the leading MATCH.
        let mut current_rows = if let Some(ref patterns) = p.leading_match {
            // Re-traverse graph for each seeded binding using the pipeline MATCH runner.
            let where_clause = p.leading_where.as_ref();
            let mut rows = Vec::new();
            for binding in &seed_rows {
                let new_rows =
                    self.execute_pipeline_match_stage(patterns, where_clause, binding)?;
                rows.extend(new_rows);
            }
            rows
        } else {
            seed_rows
        };

        // Execute subsequent pipeline stages (reuse existing logic).
        for stage in &p.stages {
            match stage {
                PipelineStage::With {
                    clause,
                    order_by,
                    skip,
                    limit,
                } => {
                    current_rows =
                        self.apply_with_stage(current_rows, clause, order_by, skip, limit)?;
                }
                PipelineStage::Match {
                    patterns,
                    where_clause,
                } => {
                    let mut next = Vec::new();
                    for binding in &current_rows {
                        let new_rows = self.execute_pipeline_match_stage(
                            patterns,
                            where_clause.as_ref(),
                            binding,
                        )?;
                        next.extend(new_rows);
                    }
                    current_rows = next;
                }
                PipelineStage::Unwind { alias, new_alias } => {
                    let mut next = Vec::new();
                    for row in &current_rows {
                        let list_val = row.get(alias.as_str()).cloned().unwrap_or(Value::Null);
                        let items = match list_val {
                            Value::List(v) => v,
                            other => vec![other],
                        };
                        for item in items {
                            let mut new_row = row.clone();
                            new_row.insert(new_alias.clone(), item);
                            next.push(new_row);
                        }
                    }
                    current_rows = next;
                }
                PipelineStage::CallSubquery { subquery, imports } => {
                    current_rows =
                        self.execute_pipeline_call_subquery_stage(subquery, imports, current_rows)?;
                }
            }
        }

        // Project the RETURN clause.
        let column_names = extract_return_column_names(&p.return_clause.items);
        let has_agg = has_aggregate_in_return(&p.return_clause.items);
        let mut rows: Vec<Vec<Value>> = if has_agg {
            // Aggregate RETURN items (COUNT, SUM, collect, etc.) need grouping.
            self.aggregate_rows_graph(&current_rows, &p.return_clause.items)
        } else {
            current_rows
                .iter()
                .map(|row_vals| {
                    p.return_clause
                        .items
                        .iter()
                        .map(|item| self.eval_expr_graph(&item.expr, row_vals))
                        .collect()
                })
                .collect()
        };

        if p.distinct {
            deduplicate_rows(&mut rows);
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
    }

    // ── WITH stage helper (extracted for reuse in seeded pipeline) ────────────

    /// Apply a `PipelineStage::With` to a set of current rows.
    fn apply_with_stage(
        &self,
        mut current_rows: Vec<HashMap<String, Value>>,
        clause: &WithClause,
        order_by: &[(sparrowdb_cypher::ast::Expr, SortDir)],
        skip: &Option<u64>,
        limit: &Option<u64>,
    ) -> Result<Vec<HashMap<String, Value>>> {
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

        let has_agg = clause
            .items
            .iter()
            .any(|item| is_aggregate_expr(&item.expr));
        let next_rows = if has_agg {
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
            let mut next = Vec::new();
            for row_vals in &current_rows {
                let mut with_vals: HashMap<String, Value> = HashMap::new();
                for item in &clause.items {
                    let val = self.eval_expr_graph(&item.expr, row_vals);
                    with_vals.insert(item.alias.clone(), val);
                    if let sparrowdb_cypher::ast::Expr::Var(ref src_var) = item.expr {
                        if let Some(nr @ Value::NodeRef(_)) = row_vals.get(src_var) {
                            with_vals.insert(item.alias.clone(), nr.clone());
                            with_vals.insert(format!("{}.__node_id__", item.alias), nr.clone());
                        }
                        let nid_key = format!("{src_var}.__node_id__");
                        if let Some(nr) = row_vals.get(&nid_key) {
                            with_vals.insert(format!("{}.__node_id__", item.alias), nr.clone());
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
                next.push(with_vals);
            }
            next
        };
        Ok(next_rows)
    }
}
