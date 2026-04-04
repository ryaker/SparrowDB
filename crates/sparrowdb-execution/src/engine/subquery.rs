//! CALL { } subquery execution (issue #290).
//!
//! Handles both unit subqueries (no outer variable imports) and correlated
//! subqueries (imports via `WITH` inside the braces).

use std::collections::HashMap;

use sparrowdb_cypher::ast::{PipelineStatement, ReturnClause, Statement};

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

            // Only build the env map if at least one RETURN item needs expression eval
            // (non-Var expressions).  For the common case of bare variable projections
            // we skip the per-row allocation entirely.
            let needs_env = ret
                .items
                .iter()
                .any(|item| !matches!(item.expr, sparrowdb_cypher::ast::Expr::Var(_)));

            let out_rows: Vec<Vec<Value>> = inner
                .rows
                .iter()
                .map(|row| {
                    let env: Option<HashMap<String, Value>> = if needs_env {
                        Some(
                            inner
                                .columns
                                .iter()
                                .zip(row.iter())
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        )
                    } else {
                        None
                    };
                    ret.items
                        .iter()
                        .map(|item| match &item.expr {
                            sparrowdb_cypher::ast::Expr::Var(name) => col_idx
                                .get(name.as_str())
                                .and_then(|&i| row.get(i))
                                .cloned()
                                .unwrap_or(Value::Null),
                            other => eval_expr(other, env.as_ref().expect("env present")),
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
            let inner_col_count = inner_col_names.len();

            for outer_row in &current_rows {
                for inner_row in &inner.rows {
                    // Pre-allocate to avoid repeated rehashing during inserts.
                    let mut merged = HashMap::with_capacity(outer_row.len() + inner_col_count);
                    merged.extend(outer_row.iter().map(|(k, v)| (k.clone(), v.clone())));
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
                let inner_col_count = inner_col_names.len();

                for inner_row in &inner.rows {
                    let mut merged = HashMap::with_capacity(outer_row.len() + inner_col_count);
                    merged.extend(outer_row.iter().map(|(k, v)| (k.clone(), v.clone())));
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
                    b.insert(nid_key.clone(), val.clone());
                }
                if let Some(nr @ Value::NodeRef(_)) = outer_row.get(key) {
                    b.insert(key.clone(), nr.clone());
                    b.insert(nid_key, nr.clone());
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
            // For these statement variants, correlated import injection is not
            // supported (they have no leading MATCH we can seed).  If the caller
            // supplied imports, return a clear error rather than silently ignoring
            // the bindings and producing wrong results.
            Statement::MatchWith(_)
            | Statement::OptionalMatch(_)
            | Statement::MatchOptionalMatch(_)
            | Statement::Unwind(_)
                if use_seeded =>
            {
                Err(sparrowdb_common::Error::InvalidArgument(
                    "CALL { WITH … }: correlated imports are not supported for this subquery form; \
                     use MATCH … RETURN … or a Pipeline inside CALL { }"
                        .to_string(),
                ))
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
}
// NOTE: `execute_pipeline_seeded` lives in `scan.rs` as part of the shared pipeline
// implementation.  Correlated subquery execution delegates to that method to avoid
// duplicating the pipeline stage loop.
