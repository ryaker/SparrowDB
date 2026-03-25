//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

impl Engine {
    // ── CALL procedure dispatch ──────────────────────────────────────────────

    /// Dispatch a `CALL` statement to the appropriate built-in procedure.
    ///
    /// Currently implemented procedures:
    /// - `db.index.fulltext.queryNodes(indexName, query)` — full-text search
    pub(crate) fn execute_call(&self, c: &CallStatement) -> Result<QueryResult> {
        match c.procedure.as_str() {
            "db.index.fulltext.queryNodes" => self.call_fulltext_query_nodes(c),
            "db.schema" => self.call_db_schema(c),
            "db.stats" => self.call_db_stats(c),
            other => Err(sparrowdb_common::Error::InvalidArgument(format!(
                "unknown procedure: {other}"
            ))),
        }
    }

    /// Implementation of `CALL db.index.fulltext.queryNodes(indexName, query)`.
    ///
    /// Args:
    ///   0 — index name (string literal or param)
    ///   1 — query string (string literal or param)
    ///
    /// Returns one row per matching node with columns declared in YIELD
    /// (typically `node`).  Each `node` value is a `NodeRef`.
    pub(crate) fn call_fulltext_query_nodes(&self, c: &CallStatement) -> Result<QueryResult> {
        // Validate argument count — must be exactly 2.
        if c.args.len() != 2 {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "db.index.fulltext.queryNodes requires exactly 2 arguments: (indexName, query)"
                    .into(),
            ));
        }

        // Evaluate arg 0 → index name.
        let index_name = eval_expr_to_string(&c.args[0])?;
        // Evaluate arg 1 → query string.
        let query = eval_expr_to_string(&c.args[1])?;

        // Open the fulltext index (read-only; no flush on this path).
        // `FulltextIndex::open` validates the name for path traversal.
        let index = FulltextIndex::open(&self.snapshot.db_root, &index_name)?;

        // Determine which column names to project.
        // Default to ["node"] when no YIELD clause was specified.
        let yield_cols: Vec<String> = if c.yield_columns.is_empty() {
            vec!["node".to_owned()]
        } else {
            c.yield_columns.clone()
        };

        // Validate YIELD columns — "node" and "score" are supported.
        if let Some(bad_col) = yield_cols
            .iter()
            .find(|c| c.as_str() != "node" && c.as_str() != "score")
        {
            return Err(sparrowdb_common::Error::InvalidArgument(format!(
                "unsupported YIELD column for db.index.fulltext.queryNodes: {bad_col}"
            )));
        }

        // Build result rows: one per matching node.
        // Use search_with_scores so we can populate the `score` YIELD column.
        let node_ids_with_scores = index.search_with_scores(&query);
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for (raw_id, score) in node_ids_with_scores {
            let node_id = sparrowdb_common::NodeId(raw_id);
            let row: Vec<Value> = yield_cols
                .iter()
                .map(|col| match col.as_str() {
                    "node" => Value::NodeRef(node_id),
                    "score" => Value::Float64(score),
                    _ => Value::Null,
                })
                .collect();
            rows.push(row);
        }

        // If a RETURN clause follows, project its items over the YIELD rows.
        let (columns, rows) = if let Some(ref ret) = c.return_clause {
            self.project_call_return(ret, &yield_cols, rows)?
        } else {
            (yield_cols, rows)
        };

        Ok(QueryResult { columns, rows })
    }

    /// Implementation of `CALL db.schema()`.
    ///
    /// Returns one row per node label and one row per relationship type with
    /// columns `["type", "name", "properties"]` where:
    ///   - `type` is `"node"` or `"relationship"`
    ///   - `name` is the label or rel-type string
    ///   - `properties` is a `List` of property name strings (sorted, may be empty)
    ///
    /// Property names are collected by scanning committed WAL records so the
    /// caller does not need to have created any nodes yet for labels to appear.
    pub(crate) fn call_db_schema(&self, c: &CallStatement) -> Result<QueryResult> {
        if !c.args.is_empty() {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "db.schema requires exactly 0 arguments".into(),
            ));
        }
        let columns = vec![
            "type".to_owned(),
            "name".to_owned(),
            "properties".to_owned(),
        ];

        // Collect property names per label_id and rel_type from the WAL.
        let wal_dir = self.snapshot.db_root.join("wal");
        let schema = WalReplayer::scan_schema(&wal_dir)?;

        let mut rows: Vec<Vec<Value>> = Vec::new();

        // Node labels — from catalog.
        let labels = self.snapshot.catalog.list_labels()?;
        for (label_id, label_name) in &labels {
            let mut prop_names: Vec<String> = schema
                .node_props
                .get(&(*label_id as u32))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            prop_names.sort();
            let props_value = Value::List(prop_names.into_iter().map(Value::String).collect());
            rows.push(vec![
                Value::String("node".to_owned()),
                Value::String(label_name.clone()),
                props_value,
            ]);
        }

        // Relationship types — from catalog.
        let rel_tables = self.snapshot.catalog.list_rel_tables()?;
        // Deduplicate by rel_type name since the same type can appear across multiple src/dst pairs.
        let mut seen_rel_types: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (_, _, rel_type) in &rel_tables {
            if seen_rel_types.insert(rel_type.clone()) {
                let mut prop_names: Vec<String> = schema
                    .rel_props
                    .get(rel_type)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                prop_names.sort();
                let props_value = Value::List(prop_names.into_iter().map(Value::String).collect());
                rows.push(vec![
                    Value::String("relationship".to_owned()),
                    Value::String(rel_type.clone()),
                    props_value,
                ]);
            }
        }

        Ok(QueryResult { columns, rows })
    }

    /// Implementation of `CALL db.stats()` (SPA-171).
    ///
    /// Returns metric/value rows for `total_bytes`, `wal_bytes`, `edge_count`,
    /// and per-label `nodes.<Label>` and `label_bytes.<Label>`.
    ///
    /// `nodes.<Label>` reports the per-label high-water mark (HWM), not the
    /// live-node count.  Tombstoned slots are included until compaction runs.
    ///
    /// Filesystem entries that cannot be read are silently skipped; values may
    /// be lower bounds when the database directory is partially inaccessible.
    pub(crate) fn call_db_stats(&self, c: &CallStatement) -> Result<QueryResult> {
        if !c.args.is_empty() {
            return Err(sparrowdb_common::Error::InvalidArgument(
                "db.stats requires exactly 0 arguments".into(),
            ));
        }
        let db_root = &self.snapshot.db_root;
        let mut rows: Vec<Vec<Value>> = Vec::new();

        rows.push(vec![
            Value::String("total_bytes".to_owned()),
            Value::Int64(dir_size_bytes(db_root) as i64),
        ]);

        let mut wal_bytes: u64 = 0;
        if let Ok(es) = std::fs::read_dir(db_root.join("wal")) {
            for e in es.flatten() {
                let n = e.file_name();
                let ns = n.to_string_lossy();
                if ns.starts_with("segment-") && ns.ends_with(".wal") {
                    if let Ok(m) = e.metadata() {
                        wal_bytes += m.len();
                    }
                }
            }
        }
        rows.push(vec![
            Value::String("wal_bytes".to_owned()),
            Value::Int64(wal_bytes as i64),
        ]);

        const DR: u64 = 20; // DeltaRecord: src(8) + dst(8) + rel_id(4) = 20 bytes
        let mut edge_count: u64 = 0;
        if let Ok(ts) = std::fs::read_dir(db_root.join("edges")) {
            for t in ts.flatten() {
                if !t.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }
                let rd = t.path();
                if let Ok(m) = std::fs::metadata(rd.join("delta.log")) {
                    edge_count += m.len().checked_div(DR).unwrap_or(0);
                }
                let fp = rd.join("base.fwd.csr");
                if fp.exists() {
                    if let Ok(b) = std::fs::read(&fp) {
                        if let Ok(csr) = sparrowdb_storage::csr::CsrForward::decode(&b) {
                            edge_count += csr.n_edges();
                        }
                    }
                }
            }
        }
        rows.push(vec![
            Value::String("edge_count".to_owned()),
            Value::Int64(edge_count as i64),
        ]);

        for (label_id, label_name) in self.snapshot.catalog.list_labels()? {
            let lid = label_id as u32;
            let hwm = self.snapshot.store.hwm_for_label(lid).unwrap_or(0);
            rows.push(vec![
                Value::String(format!("nodes.{label_name}")),
                Value::Int64(hwm as i64),
            ]);
            let mut lb: u64 = 0;
            if let Ok(es) = std::fs::read_dir(db_root.join("nodes").join(lid.to_string())) {
                for e in es.flatten() {
                    if let Ok(m) = e.metadata() {
                        lb += m.len();
                    }
                }
            }
            rows.push(vec![
                Value::String(format!("label_bytes.{label_name}")),
                Value::Int64(lb as i64),
            ]);
        }

        let columns = vec!["metric".to_owned(), "value".to_owned()];
        let yield_cols: Vec<String> = if c.yield_columns.is_empty() {
            columns.clone()
        } else {
            c.yield_columns.clone()
        };
        for col in &yield_cols {
            if col != "metric" && col != "value" {
                return Err(sparrowdb_common::Error::InvalidArgument(format!(
                    "unsupported YIELD column for db.stats: {col}"
                )));
            }
        }
        let idxs: Vec<usize> = yield_cols
            .iter()
            .map(|c| if c == "metric" { 0 } else { 1 })
            .collect();
        let projected: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|r| idxs.iter().map(|&i| r[i].clone()).collect())
            .collect();
        let (fc, fr) = if let Some(ref ret) = c.return_clause {
            self.project_call_return(ret, &yield_cols, projected)?
        } else {
            (yield_cols, projected)
        };
        Ok(QueryResult {
            columns: fc,
            rows: fr,
        })
    }

    /// Project a RETURN clause over rows produced by a CALL statement.
    ///
    /// The YIELD columns from the CALL become the row environment.  Each
    /// return item is evaluated against those columns:
    ///   - `Var(name)` — returns the raw yield-column value
    ///   - `PropAccess { var, prop }` — reads a property from the NodeRef
    ///
    /// This covers the primary KMS pattern:
    /// `CALL … YIELD node RETURN node.content, node.title`
    pub(crate) fn project_call_return(
        &self,
        ret: &sparrowdb_cypher::ast::ReturnClause,
        yield_cols: &[String],
        rows: Vec<Vec<Value>>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        // Column names from return items.
        let out_cols: Vec<String> = ret
            .items
            .iter()
            .map(|item| {
                item.alias
                    .clone()
                    .unwrap_or_else(|| expr_to_col_name(&item.expr))
            })
            .collect();

        let mut out_rows = Vec::new();
        for row in rows {
            // Build a name → Value map for this row.
            let env: HashMap<String, Value> = yield_cols
                .iter()
                .zip(row.iter())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let projected: Vec<Value> = ret
                .items
                .iter()
                .map(|item| eval_call_expr(&item.expr, &env, &self.snapshot.store))
                .collect();
            out_rows.push(projected);
        }
        Ok((out_cols, out_rows))
    }
}
