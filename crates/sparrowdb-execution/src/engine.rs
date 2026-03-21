//! Query execution engine.
//!
//! Converts a bound Cypher AST into an operator tree and executes it,
//! returning a materialized `QueryResult`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tracing::info_span;

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::{col_id_of, NodeId, Result};
use sparrowdb_cypher::ast::{
    BinOpKind, Expr, Literal, MatchStatement, ReturnItem, SortDir, Statement, UnwindStatement,
};
use sparrowdb_cypher::{bind, parse};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::node_store::NodeStore;

use crate::types::{QueryResult, Value};

/// The execution engine holds references to the storage layer.
pub struct Engine {
    pub store: NodeStore,
    pub catalog: Catalog,
    pub csr: CsrForward,
    pub db_root: std::path::PathBuf,
}

impl Engine {
    pub fn new(store: NodeStore, catalog: Catalog, csr: CsrForward, db_root: &Path) -> Self {
        Engine {
            store,
            catalog,
            csr,
            db_root: db_root.to_path_buf(),
        }
    }

    /// Parse, bind, plan, and execute a Cypher query.
    pub fn execute(&self, cypher: &str) -> Result<QueryResult> {
        let stmt = {
            let _parse_span = info_span!("sparrowdb.parse", cypher = cypher).entered();
            parse(cypher)?
        };

        let bound = {
            let _bind_span = info_span!("sparrowdb.bind").entered();
            bind(stmt, &self.catalog)?
        };

        {
            let _plan_span = info_span!("sparrowdb.plan_execute").entered();
            self.execute_bound(bound.inner)
        }
    }

    /// Execute an already-bound [`Statement`] directly.
    ///
    /// Useful for callers (e.g. `WriteTx`) that have already parsed and bound
    /// the statement and want to dispatch CHECKPOINT/OPTIMIZE themselves.
    pub fn execute_statement(&self, stmt: Statement) -> Result<QueryResult> {
        self.execute_bound(stmt)
    }

    fn execute_bound(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Match(m) => self.execute_match(&m),
            Statement::Unwind(u) => self.execute_unwind(&u),
            Statement::Create(_) => {
                // CREATE is a write — not handled in read-only engine stub.
                Ok(QueryResult::empty(vec![]))
            }
            Statement::MatchCreate(_) => Ok(QueryResult::empty(vec![])),
            Statement::Checkpoint | Statement::Optimize => Ok(QueryResult::empty(vec![])),
        }
    }

    // ── UNWIND ─────────────────────────────────────────────────────────────────

    fn execute_unwind(&self, u: &UnwindStatement) -> Result<QueryResult> {
        use crate::operators::{Operator, UnwindOperator};

        // Evaluate the list expression to a Vec<Value>.
        let values = eval_list_expr(&u.expr)?;

        // Determine the output column name from the RETURN clause.
        let column_names = extract_return_column_names(&u.return_clause.items);

        if values.is_empty() {
            return Ok(QueryResult::empty(column_names));
        }

        let mut op = UnwindOperator::new(u.alias.clone(), values);
        let chunks = op.collect_all()?;

        // Materialize: for each chunk/group/row, project the RETURN columns.
        //
        // Only fall back to the UNWIND alias value when the output column
        // actually corresponds to the alias variable.  Returning a value for
        // an unrelated variable (e.g. `RETURN y` when alias is `x`) would
        // silently produce wrong results instead of NULL.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for chunk in &chunks {
            for group in &chunk.groups {
                let n = group.len();
                for row_idx in 0..n {
                    let row = u
                        .return_clause
                        .items
                        .iter()
                        .map(|item| {
                            // Determine whether this RETURN item refers to the
                            // alias variable produced by UNWIND.
                            let is_alias = match &item.expr {
                                Expr::Var(name) => name == &u.alias,
                                _ => false,
                            };
                            if is_alias {
                                group.get_value(&u.alias, row_idx).unwrap_or(Value::Null)
                            } else {
                                // Variable is not in scope for this UNWIND —
                                // return NULL rather than leaking the alias value.
                                Value::Null
                            }
                        })
                        .collect();
                    rows.push(row);
                }
            }
        }

        Ok(QueryResult {
            columns: column_names,
            rows,
        })
    }

    fn execute_match(&self, m: &MatchStatement) -> Result<QueryResult> {
        if m.pattern.is_empty() {
            return Ok(QueryResult::empty(vec![]));
        }

        // Determine if this is a 2-hop query.
        let is_two_hop = m.pattern.len() == 1 && m.pattern[0].rels.len() == 2;
        let is_one_hop = m.pattern.len() == 1 && m.pattern[0].rels.len() == 1;

        let column_names = extract_return_column_names(&m.return_clause.items);

        if is_two_hop {
            self.execute_two_hop(m, &column_names)
        } else if is_one_hop {
            self.execute_one_hop(m, &column_names)
        } else if m.pattern[0].rels.is_empty() {
            self.execute_scan(m, &column_names)
        } else {
            // Multi-pattern or complex query — fallback to sequential execution.
            self.execute_scan(m, &column_names)
        }
    }

    // ── Node-only scan (no relationships) ─────────────────────────────────────

    fn execute_scan(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let node = &pat.nodes[0];

        let label = node.labels.first().cloned().unwrap_or_default();
        let label_id = self
            .catalog
            .get_label(&label)?
            .ok_or(sparrowdb_common::Error::NotFound)?;
        let label_id_u32 = label_id as u32;

        let hwm = self.store.hwm_for_label(label_id_u32)?;
        tracing::debug!(label = %label, hwm = hwm, "node scan start");

        // Collect all col_ids we need.
        let col_ids = collect_col_ids_from_columns(column_names);
        let all_col_ids: Vec<u32> = col_ids.clone();

        let mut rows = Vec::new();

        for slot in 0..hwm {
            let node_id = NodeId(((label_id_u32 as u64) << 32) | slot);
            if slot < 1024 || slot % 10_000 == 0 {
                tracing::trace!(slot = slot, node_id = node_id.0, "scan emit");
            }

            // SPA-164: skip tombstoned nodes.  delete_node writes u64::MAX into
            // col_0 as the deletion sentinel; nodes in that state must not
            // appear in scan results.
            let col0_check = self.store.get_node_raw(node_id, &[0u32])?;
            if col0_check.iter().any(|&(c, v)| c == 0 && v == u64::MAX) {
                continue;
            }

            let props = self.store.get_node_raw(node_id, &all_col_ids)?;

            // Apply inline prop filter from the pattern.
            if !self.matches_prop_filter(&props, &node.props) {
                continue;
            }

            // Apply WHERE clause.
            if let Some(ref where_expr) = m.where_clause {
                let var_name = node.var.as_str();
                let row_vals = build_row_vals(&props, var_name, &all_col_ids);
                if !eval_where(where_expr, &row_vals) {
                    continue;
                }
            }

            // Project RETURN columns.
            let row = project_row(&props, column_names, &all_col_ids);
            rows.push(row);
        }

        // ORDER BY
        apply_order_by(&mut rows, m, column_names);

        // LIMIT
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(rows = rows.len(), "node scan complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── 1-hop traversal: (a)-[:R]->(f) ───────────────────────────────────────

    fn execute_one_hop(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        let dst_node_pat = &pat.nodes[1];
        let rel_pat = &pat.rels[0];
        if rel_pat.dir != sparrowdb_cypher::ast::EdgeDir::Outgoing {
            return Err(sparrowdb_common::Error::Unimplemented);
        }

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let dst_label = dst_node_pat.labels.first().cloned().unwrap_or_default();
        let src_label_id = self
            .catalog
            .get_label(&src_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;
        let dst_label_id = self
            .catalog
            .get_label(&dst_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;

        let hwm_src = self.store.hwm_for_label(src_label_id)?;
        tracing::debug!(src_label = %src_label, dst_label = %dst_label, hwm_src = hwm_src, "one-hop traversal start");

        let col_ids_src = collect_col_ids_for_var(&src_node_pat.var, column_names, src_label_id);
        let col_ids_dst = collect_col_ids_for_var(&dst_node_pat.var, column_names, dst_label_id);

        let mut rows = Vec::new();

        // Scan source nodes.
        for src_slot in 0..hwm_src {
            let src_node = NodeId(((src_label_id as u64) << 32) | src_slot);
            let src_props = if !col_ids_src.is_empty() || !src_node_pat.props.is_empty() {
                let all_needed: Vec<u32> = {
                    let mut v = col_ids_src.clone();
                    // Add prop filter cols
                    for p in &src_node_pat.props {
                        let col_id = prop_name_to_col_id(&p.key);
                        if !v.contains(&col_id) {
                            v.push(col_id);
                        }
                    }
                    v
                };
                self.store.get_node_raw(src_node, &all_needed)?
            } else {
                vec![]
            };

            // Apply src inline prop filter.
            if !self.matches_prop_filter(&src_props, &src_node_pat.props) {
                continue;
            }

            // SPA-163: read delta log edges for this source node and merge
            // with CSR neighbors so edges are visible before a checkpoint.
            let delta_neighbors: Vec<u64> = {
                let edge_store = EdgeStore::open(&self.db_root, RelTableId(0));
                match edge_store.and_then(|s| s.read_delta()) {
                    Ok(records) => records
                        .into_iter()
                        .filter(|r| {
                            let r_src_label = (r.src.0 >> 32) as u32;
                            let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                            r_src_label == src_label_id && r_src_slot == src_slot
                        })
                        .map(|r| r.dst.0 & 0xFFFF_FFFF)
                        .collect(),
                    Err(_) => vec![],
                }
            };

            // Traverse CSR.
            let csr_neighbors = self.csr.neighbors(src_slot);
            let all_neighbors: Vec<u64> = csr_neighbors
                .iter()
                .copied()
                .chain(delta_neighbors.into_iter())
                .collect();
            let mut seen_neighbors: HashSet<u64> = HashSet::new();
            for &dst_slot in &all_neighbors {
                if !seen_neighbors.insert(dst_slot) {
                    continue;
                }
                let dst_node = NodeId(((dst_label_id as u64) << 32) | dst_slot);
                let dst_props = if !col_ids_dst.is_empty() {
                    self.store.get_node_raw(dst_node, &col_ids_dst)?
                } else {
                    vec![]
                };

                // Apply dst inline prop filter.
                if !self.matches_prop_filter(&dst_props, &dst_node_pat.props) {
                    continue;
                }

                // Apply WHERE clause.
                if let Some(ref where_expr) = m.where_clause {
                    let mut row_vals = build_row_vals(&src_props, &src_node_pat.var, &col_ids_src);
                    row_vals.extend(build_row_vals(&dst_props, &dst_node_pat.var, &col_ids_dst));
                    if !eval_where(where_expr, &row_vals) {
                        continue;
                    }
                }

                // Build result row.
                let row = project_hop_row(
                    &src_props,
                    &dst_props,
                    column_names,
                    &src_node_pat.var,
                    &dst_node_pat.var,
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

        // LIMIT
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(rows = rows.len(), "one-hop traversal complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── 2-hop traversal: (a)-[:R]->()-[:R]->(fof) ────────────────────────────

    fn execute_two_hop(&self, m: &MatchStatement, column_names: &[String]) -> Result<QueryResult> {
        use crate::join::AspJoin;

        let pat = &m.pattern[0];
        let src_node_pat = &pat.nodes[0];
        // nodes[1] is the anonymous mid node
        let fof_node_pat = &pat.nodes[2];

        let src_label = src_node_pat.labels.first().cloned().unwrap_or_default();
        let fof_label = fof_node_pat.labels.first().cloned().unwrap_or_default();
        let src_label_id = self
            .catalog
            .get_label(&src_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;
        let fof_label_id = self
            .catalog
            .get_label(&fof_label)?
            .ok_or(sparrowdb_common::Error::NotFound)? as u32;

        let hwm_src = self.store.hwm_for_label(src_label_id)?;
        tracing::debug!(src_label = %src_label, fof_label = %fof_label, hwm_src = hwm_src, "two-hop traversal start");

        // Collect col_ids for fof: projected columns plus any columns referenced by prop filters.
        let col_ids_fof = {
            let mut ids = collect_col_ids_for_var(&fof_node_pat.var, column_names, fof_label_id);
            for p in &fof_node_pat.props {
                let col_id = prop_name_to_col_id(&p.key);
                if !ids.contains(&col_id) {
                    ids.push(col_id);
                }
            }
            ids
        };

        // SPA-163: build a slot-level adjacency map from the delta log so that
        // edges written since the last checkpoint are visible for 2-hop queries.
        // Map: src_slot → Vec<dst_slot> (only records whose src label matches).
        let delta_adj: HashMap<u64, Vec<u64>> = {
            let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
            if let Ok(store) = EdgeStore::open(&self.db_root, RelTableId(0)) {
                if let Ok(records) = store.read_delta() {
                    for r in records {
                        let r_src_label = (r.src.0 >> 32) as u32;
                        let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                        if r_src_label == src_label_id {
                            adj.entry(r_src_slot)
                                .or_default()
                                .push(r.dst.0 & 0xFFFF_FFFF);
                        }
                    }
                }
            }
            adj
        };

        let join = AspJoin::new(&self.csr);
        let mut rows = Vec::new();

        // Scan source nodes.
        for src_slot in 0..hwm_src {
            let src_node = NodeId(((src_label_id as u64) << 32) | src_slot);
            let src_needed: Vec<u32> = {
                let mut v = vec![];
                for p in &src_node_pat.props {
                    let col_id = prop_name_to_col_id(&p.key);
                    if !v.contains(&col_id) {
                        v.push(col_id);
                    }
                }
                v
            };

            let src_props = if !src_needed.is_empty() {
                self.store.get_node_raw(src_node, &src_needed)?
            } else {
                vec![]
            };

            // Apply src inline prop filter.
            if !self.matches_prop_filter(&src_props, &src_node_pat.props) {
                continue;
            }

            // Use ASP-Join to get 2-hop fof from CSR.
            let mut fof_slots = join.two_hop(src_slot)?;

            // SPA-163: extend with delta-log 2-hop paths.
            // First-hop delta neighbors of src_slot:
            let first_hop_delta = delta_adj
                .get(&src_slot)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            if !first_hop_delta.is_empty() {
                let mut delta_fof: HashSet<u64> = HashSet::new();
                for &mid_slot in first_hop_delta {
                    // CSR second hop from mid:
                    for &fof in self.csr.neighbors(mid_slot) {
                        delta_fof.insert(fof);
                    }
                    // Delta second hop from mid:
                    if let Some(mid_neighbors) = delta_adj.get(&mid_slot) {
                        for &fof in mid_neighbors {
                            delta_fof.insert(fof);
                        }
                    }
                }
                fof_slots.extend(delta_fof);
                // Re-deduplicate the combined set.
                let unique: HashSet<u64> = fof_slots.into_iter().collect();
                fof_slots = unique.into_iter().collect();
                fof_slots.sort_unstable();
            }

            for fof_slot in fof_slots {
                let fof_node = NodeId(((fof_label_id as u64) << 32) | fof_slot);
                let fof_props = if !col_ids_fof.is_empty() {
                    self.store.get_node_raw(fof_node, &col_ids_fof)?
                } else {
                    vec![]
                };

                // Apply fof inline prop filter.
                if !self.matches_prop_filter(&fof_props, &fof_node_pat.props) {
                    continue;
                }

                let row = project_fof_row(&fof_props, column_names, &fof_node_pat.var);
                rows.push(row);
            }
        }

        // DISTINCT
        if m.distinct {
            deduplicate_rows(&mut rows);
        }

        // ORDER BY
        apply_order_by(&mut rows, m, column_names);

        // LIMIT
        if let Some(lim) = m.limit {
            rows.truncate(lim as usize);
        }

        tracing::debug!(rows = rows.len(), "two-hop traversal complete");
        Ok(QueryResult {
            columns: column_names.to_vec(),
            rows,
        })
    }

    // ── Property filter helpers ───────────────────────────────────────────────

    fn matches_prop_filter(
        &self,
        props: &[(u32, u64)],
        filters: &[sparrowdb_cypher::ast::PropEntry],
    ) -> bool {
        for f in filters {
            let col_id = prop_name_to_col_id(&f.key);
            let stored_val = props.iter().find(|(c, _)| *c == col_id).map(|(_, v)| *v);

            let matches = match &f.value {
                Literal::Int(n) => stored_val == Some(*n as u64),
                Literal::String(_) => {
                    // Strings are stored as i64 hash — for test simplicity we
                    // compare using string-to-i64 lookup table.
                    // In production this would use the overflow store.
                    false
                }
                Literal::Param(_) => true, // params always pass in current impl
                _ => false,
            };
            if !matches {
                return false;
            }
        }
        true
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Evaluate an UNWIND list expression to a concrete `Vec<Value>`.
///
/// Supports:
/// - `Expr::List([...])` — list literal
/// - `Expr::Literal(Param(_))` — parameter (returns empty list; callers supply params separately)
///
/// `range()` function support is a future TODO.
fn eval_list_expr(expr: &Expr) -> Result<Vec<Value>> {
    match expr {
        Expr::List(elems) => {
            let mut values = Vec::with_capacity(elems.len());
            for elem in elems {
                values.push(eval_scalar_expr(elem));
            }
            Ok(values)
        }
        Expr::Literal(Literal::Param(_)) => {
            // Parameters are not resolved by the read-only engine stub.
            // Callers that need param support should bind params before calling execute().
            Ok(vec![])
        }
        other => Err(sparrowdb_common::Error::InvalidArgument(format!(
            "UNWIND expression is not a list: {:?}",
            other
        ))),
    }
}

/// Evaluate a scalar expression to a `Value` (no row context needed).
fn eval_scalar_expr(expr: &Expr) -> Value {
    match expr {
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => Value::Int64(*n),
            Literal::Float(f) => Value::Float64(*f),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Null => Value::Null,
            Literal::Param(_) => Value::Null,
        },
        _ => Value::Null,
    }
}

fn extract_return_column_names(items: &[ReturnItem]) -> Vec<String> {
    items
        .iter()
        .map(|item| match &item.alias {
            Some(alias) => alias.clone(),
            None => match &item.expr {
                Expr::PropAccess { var, prop } => format!("{var}.{prop}"),
                Expr::Var(v) => v.clone(),
                _ => "?".to_string(),
            },
        })
        .collect()
}

/// Map a property name like "col_0" or "name" to a col_id.
///
/// Uses the canonical [`sparrowdb_common::col_id_of`] FNV-1a hash so that
/// this always agrees with what the storage layer wrote to disk (SPA-160).
fn prop_name_to_col_id(name: &str) -> u32 {
    if let Some(suffix) = name.strip_prefix("col_") {
        suffix.parse().unwrap_or(0)
    } else {
        col_id_of(name)
    }
}

fn collect_col_ids_from_columns(column_names: &[String]) -> Vec<u32> {
    let mut ids = Vec::new();
    for name in column_names {
        // name could be "var.col_N" or "col_N"
        let prop = name.split('.').next_back().unwrap_or(name.as_str());
        let col_id = prop_name_to_col_id(prop);
        if !ids.contains(&col_id) {
            ids.push(col_id);
        }
    }
    ids
}

fn collect_col_ids_for_var(var: &str, column_names: &[String], _label_id: u32) -> Vec<u32> {
    let mut ids = Vec::new();
    for name in column_names {
        // name is either "var.col_N" or "col_N"
        if let Some((v, prop)) = name.split_once('.') {
            if v == var {
                let col_id = prop_name_to_col_id(prop);
                if !ids.contains(&col_id) {
                    ids.push(col_id);
                }
            }
        } else {
            // No dot — could be this var's column
            let col_id = prop_name_to_col_id(name.as_str());
            if !ids.contains(&col_id) {
                ids.push(col_id);
            }
        }
    }
    if ids.is_empty() {
        // Default: read col_0
        ids.push(0);
    }
    ids
}

fn build_row_vals(
    props: &[(u32, u64)],
    var_name: &str,
    _col_ids: &[u32],
) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for &(col_id, raw) in props {
        let key = format!("{var_name}.col_{col_id}");
        map.insert(key, Value::Int64(raw as i64));
    }
    map
}

fn eval_where(expr: &Expr, vals: &HashMap<String, Value>) -> bool {
    match expr {
        Expr::BinOp { left, op, right } => {
            let lv = eval_expr(left, vals);
            let rv = eval_expr(right, vals);
            match op {
                BinOpKind::Eq => lv == rv,
                BinOpKind::Neq => lv != rv,
                BinOpKind::Contains => lv.contains(&rv),
                BinOpKind::StartsWith => {
                    matches!((&lv, &rv), (Value::String(l), Value::String(r)) if l.starts_with(r.as_str()))
                }
                BinOpKind::EndsWith => {
                    matches!((&lv, &rv), (Value::String(l), Value::String(r)) if l.ends_with(r.as_str()))
                }
                BinOpKind::Lt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a < b,
                    _ => false,
                },
                BinOpKind::Le => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a <= b,
                    _ => false,
                },
                BinOpKind::Gt => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a > b,
                    _ => false,
                },
                BinOpKind::Ge => match (&lv, &rv) {
                    (Value::Int64(a), Value::Int64(b)) => a >= b,
                    _ => false,
                },
                _ => false,
            }
        }
        Expr::And(l, r) => eval_where(l, vals) && eval_where(r, vals),
        Expr::Or(l, r) => eval_where(l, vals) || eval_where(r, vals),
        Expr::Not(inner) => !eval_where(inner, vals),
        Expr::Literal(Literal::Bool(b)) => *b,
        Expr::Literal(_) => false,
        _ => false, // unsupported expression — reject row rather than silently pass
    }
}

fn eval_expr(expr: &Expr, vals: &HashMap<String, Value>) -> Value {
    match expr {
        Expr::PropAccess { var, prop } => {
            let key = format!("{var}.{prop}");
            vals.get(&key).cloned().unwrap_or(Value::Null)
        }
        Expr::Var(v) => vals.get(v.as_str()).cloned().unwrap_or(Value::Null),
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => Value::Int64(*n),
            Literal::Float(f) => Value::Float64(*f),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Param(_p) => Value::Null, // params not bound in engine
            Literal::Null => Value::Null,
        },
        _ => Value::Null,
    }
}

fn project_row(props: &[(u32, u64)], column_names: &[String], _col_ids: &[u32]) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            let prop = col_name.split('.').next_back().unwrap_or(col_name.as_str());
            let col_id = prop_name_to_col_id(prop);
            props
                .iter()
                .find(|(c, _)| *c == col_id)
                .map(|(_, v)| Value::Int64(*v as i64))
                .unwrap_or(Value::Null)
        })
        .collect()
}

fn project_hop_row(
    src_props: &[(u32, u64)],
    dst_props: &[(u32, u64)],
    column_names: &[String],
    src_var: &str,
    _dst_var: &str,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            if let Some((v, prop)) = col_name.split_once('.') {
                let col_id = prop_name_to_col_id(prop);
                let props = if v == src_var { src_props } else { dst_props };
                props
                    .iter()
                    .find(|(c, _)| *c == col_id)
                    .map(|(_, val)| Value::Int64(*val as i64))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        })
        .collect()
}

fn project_fof_row(
    fof_props: &[(u32, u64)],
    column_names: &[String],
    _fof_var: &str,
) -> Vec<Value> {
    column_names
        .iter()
        .map(|col_name| {
            let prop = if let Some((_, p)) = col_name.split_once('.') {
                p
            } else {
                col_name.as_str()
            };
            let col_id = prop_name_to_col_id(prop);
            fof_props
                .iter()
                .find(|(c, _)| *c == col_id)
                .map(|(_, v)| Value::Int64(*v as i64))
                .unwrap_or(Value::Null)
        })
        .collect()
}

fn deduplicate_rows(rows: &mut Vec<Vec<Value>>) {
    // Deduplicate by converting to a string key.
    let mut seen: HashSet<String> = HashSet::new();
    rows.retain(|row| {
        let key: String = row
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("|");
        seen.insert(key)
    });
}

fn apply_order_by(rows: &mut [Vec<Value>], m: &MatchStatement, column_names: &[String]) {
    if m.order_by.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for (expr, dir) in &m.order_by {
            let col_idx = match expr {
                Expr::PropAccess { var, prop } => {
                    let key = format!("{var}.{prop}");
                    column_names.iter().position(|c| c == &key)
                }
                Expr::Var(v) => column_names.iter().position(|c| c == v.as_str()),
                _ => None,
            };
            if let Some(idx) = col_idx {
                if idx < a.len() && idx < b.len() {
                    let cmp = compare_values(&a[idx], &b[idx]);
                    let cmp = if *dir == SortDir::Desc {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Float64(x), Value::Float64(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}
