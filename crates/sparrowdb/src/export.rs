//! Graph export / import tooling for cross-version migration (SPA-XXX).
//!
//! Provides [`GraphDump`], [`NodeDump`], and [`EdgeDump`] for serialising the
//! full contents of a [`super::GraphDb`] to JSON, and re-importing that JSON
//! into a (possibly new / freshly-opened) database.
//!
//! ## Workflow
//!
//! ```text
//! // Old version
//! let json = old_db.export_json()?;
//!
//! // New version (fresh directory)
//! let new_db = GraphDb::open(new_path)?;
//! new_db.import_json(&json)?;
//! ```

use std::collections::HashMap;

use sparrowdb_common::col_id_of;

use crate::{GraphDb, Result};

// ── Public dump types ─────────────────────────────────────────────────────────

/// A single node extracted from the database during export.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeDump {
    /// The node's internal ID in the *source* database.
    pub node_id: u64,
    /// The node's label (single label per node in SparrowDB).
    pub label: String,
    /// All stored properties, keyed by property name.
    pub properties: HashMap<String, serde_json::Value>,
}

/// A single directed edge extracted from the database during export.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeDump {
    /// Internal node ID of the source node (from the *source* database).
    pub src_id: u64,
    /// Internal node ID of the destination node (from the *source* database).
    pub dst_id: u64,
    /// Relationship type name (e.g. `"KNOWS"`).
    pub rel_type: String,
    /// Edge properties (currently empty; reserved for future use).
    pub properties: HashMap<String, serde_json::Value>,
}

/// A complete serialisable snapshot of a SparrowDB graph.
///
/// Produced by [`GraphDb::export`] and consumed by [`GraphDb::import`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GraphDump {
    /// The SparrowDB crate version that produced this dump.
    pub sparrowdb_version: String,
    /// Unix timestamp (seconds since epoch) when the dump was created.
    pub exported_at: u64,
    /// All nodes in the graph.
    pub nodes: Vec<NodeDump>,
    /// All directed edges in the graph.
    pub edges: Vec<EdgeDump>,
}

// ── Export ────────────────────────────────────────────────────────────────────

impl GraphDb {
    /// Export all nodes and edges to a [`GraphDump`].
    ///
    /// The dump is a point-in-time snapshot: it captures all data committed
    /// at or before the moment of the call.  Concurrent writers may cause the
    /// node and edge sets to be slightly inconsistent; for a fully consistent
    /// snapshot call [`GraphDb::checkpoint`] first.
    pub fn export(&self) -> Result<GraphDump> {
        // ── 1. Collect schema: label_name → [prop_names] ──────────────────
        let schema_result = self.execute("CALL db.schema()")?;

        // Build: label_name → Vec<prop_name>
        let mut label_props: HashMap<String, Vec<String>> = HashMap::new();
        for row in &schema_result.rows {
            use sparrowdb_execution::types::Value;
            let kind = match &row[0] {
                Value::String(s) => s.as_str(),
                _ => continue,
            };
            if kind != "node" {
                continue;
            }
            let label = match &row[1] {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            let props: Vec<String> = match &row[2] {
                Value::List(items) => items
                    .iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                _ => vec![],
            };
            label_props.insert(label, props);
        }

        // Build reverse lookup: col_id (u32) → prop_name, per label.
        // Since col_id = FNV(prop_name), we compute the mapping once globally
        // (collisions across labels are theoretically possible but vanishingly
        // rare in practice — the FNV hash space is 2^32).
        let mut col_id_to_name: HashMap<u32, String> = HashMap::new();
        for props in label_props.values() {
            for prop_name in props {
                let cid = col_id_of(prop_name);
                col_id_to_name.insert(cid, prop_name.clone());
            }
        }

        // ── 2. Export all labels listed in the catalog ────────────────────
        let catalog = self.catalog_snapshot();
        let all_labels = catalog.list_labels()?;

        let mut nodes: Vec<NodeDump> = Vec::new();

        for (_label_id, label_name) in &all_labels {
            // Skip the internal __SparrowSid migration label if present.
            if label_name.starts_with("__Sparrow") {
                continue;
            }

            let query = format!("MATCH (n:{label_name}) RETURN id(n), n");
            let result = match self.execute(&query) {
                Ok(r) => r,
                Err(_) => continue, // skip labels with no nodes
            };

            for row in &result.rows {
                use sparrowdb_execution::types::Value;

                let node_id = match &row[0] {
                    Value::Int64(i) => *i as u64,
                    _ => continue,
                };

                // row[1] is a Value::Map with "col_{col_id}" keys.
                let props_map = match &row[1] {
                    Value::Map(entries) => entries,
                    _ => {
                        // Node with no properties — still export it.
                        nodes.push(NodeDump {
                            node_id,
                            label: label_name.clone(),
                            properties: HashMap::new(),
                        });
                        continue;
                    }
                };

                let mut properties: HashMap<String, serde_json::Value> = HashMap::new();
                for (col_key, val) in props_map {
                    // col_key is "col_{col_id}", e.g. "col_3735928559"
                    let prop_name = if let Some(suffix) = col_key.strip_prefix("col_") {
                        if let Ok(cid) = suffix.parse::<u32>() {
                            col_id_to_name
                                .get(&cid)
                                .cloned()
                                .unwrap_or_else(|| col_key.clone())
                        } else {
                            col_key.clone()
                        }
                    } else {
                        col_key.clone()
                    };

                    let json_val = execution_value_to_json(val);
                    if json_val != serde_json::Value::Null {
                        properties.insert(prop_name, json_val);
                    }
                }

                nodes.push(NodeDump {
                    node_id,
                    label: label_name.clone(),
                    properties,
                });
            }
        }

        // ── 3. Export edges from storage (delta log + CSR) ────────────────
        // This mirrors the approach used in `to_dot` to avoid Cypher engine
        // limitations around id(a) / id(b) in hop patterns.
        let path = &self.inner.path;
        let rel_tables = catalog.list_rel_tables_with_ids();
        let mut edges: Vec<EdgeDump> = Vec::new();
        // Deduplicate: delta log edges may also appear in CSR after checkpoint.
        let mut seen: std::collections::HashSet<(u64, u64, String)> =
            std::collections::HashSet::new();

        for (catalog_id, src_label_id, dst_label_id, rel_type) in &rel_tables {
            let storage_rel_id = sparrowdb_storage::edge_store::RelTableId(*catalog_id as u32);

            if let Ok(store) = sparrowdb_storage::edge_store::EdgeStore::open(path, storage_rel_id)
            {
                // Delta log — stores full NodeId pairs (label_id << 32 | slot).
                if let Ok(records) = store.read_delta() {
                    for rec in records {
                        let key = (rec.src.0, rec.dst.0, rel_type.clone());
                        if seen.insert(key) {
                            edges.push(EdgeDump {
                                src_id: rec.src.0,
                                dst_id: rec.dst.0,
                                rel_type: rel_type.clone(),
                                properties: HashMap::new(),
                            });
                        }
                    }
                }

                // CSR — (src_slot, dst_slot) relative to their label IDs.
                if let Ok(csr) = store.open_fwd() {
                    let n_nodes = csr.n_nodes();
                    for src_slot in 0..n_nodes {
                        let src_id = (*src_label_id as u64) << 32 | src_slot;
                        for &dst_slot in csr.neighbors(src_slot) {
                            let dst_id = (*dst_label_id as u64) << 32 | dst_slot;
                            let key = (src_id, dst_id, rel_type.clone());
                            if seen.insert(key) {
                                edges.push(EdgeDump {
                                    src_id,
                                    dst_id,
                                    rel_type: rel_type.clone(),
                                    properties: HashMap::new(),
                                });
                            }
                        }
                    }
                }
            }
        }

        let exported_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(GraphDump {
            sparrowdb_version: env!("CARGO_PKG_VERSION").to_owned(),
            exported_at,
            nodes,
            edges,
        })
    }

    /// Serialize the full graph to a pretty-printed JSON string.
    ///
    /// Convenience wrapper around [`export`](Self::export) +
    /// [`serde_json::to_string_pretty`].
    pub fn export_json(&self) -> Result<String> {
        let dump = self.export()?;
        serde_json::to_string_pretty(&dump)
            .map_err(|e| sparrowdb_common::Error::InvalidArgument(e.to_string()))
    }

    /// Re-create all nodes and edges from a [`GraphDump`].
    ///
    /// This is the inverse of [`export`](Self::export).  It is safe to call on
    /// a non-empty database — imported nodes will be **added** to any existing
    /// data (no deduplication is performed).
    ///
    /// ## Node identity during import
    ///
    /// Because SparrowDB auto-assigns node IDs, the original `node_id` values
    /// from the dump cannot be preserved.  A temporary property
    /// `_sparrow_export_id` is written to each node during import to allow
    /// edges to be wired correctly, and is **left in place** after import.
    /// Callers may remove it at the application layer if desired (e.g. with a
    /// future `REMOVE` Cypher clause once that is supported).
    ///
    /// ## Performance (SPA-223)
    ///
    /// Node creation and edge wiring each use [`execute_batch`](Self::execute_batch)
    /// so that all mutations in each phase commit with a **single WAL fsync**
    /// rather than one fsync per node/edge.  A 100-node import therefore
    /// requires 2 fsyncs (one for the node batch, one for the edge batch)
    /// instead of O(N) fsyncs.
    pub fn import(&self, dump: &GraphDump) -> Result<()> {
        // ── 1. Build a label lookup from the node list ────────────────────
        // Maps original node_id → label name (needed when wiring edges).
        let mut node_label: HashMap<u64, String> = HashMap::new();
        for n in &dump.nodes {
            node_label.insert(n.node_id, n.label.clone());
        }

        // ── 2. Create all nodes in a single batch (one WAL fsync) ─────────
        // SPA-223: build all CREATE queries up front and submit them via
        // execute_batch so the entire node set is committed with one fsync.
        if !dump.nodes.is_empty() {
            let node_queries: Vec<String> = dump
                .nodes
                .iter()
                .map(|node| {
                    let props_cypher = build_props_cypher(&node.properties, Some(node.node_id));
                    let label = cypher_escape_label(&node.label);
                    format!("CREATE (n:{label} {{{props_cypher}}})")
                })
                .collect();
            let node_refs: Vec<&str> = node_queries.iter().map(String::as_str).collect();
            self.execute_batch(&node_refs)?;
        }

        // ── 3. Wire edges in a single batch (one WAL fsync) ───────────────
        // Look up source and destination nodes by the temporary `_sparrow_export_id`
        // property we stamped during node creation.
        // SPA-223: collect all valid edge queries and submit them via
        // execute_batch so the entire edge set commits with one fsync.
        let edge_queries: Vec<String> = dump
            .edges
            .iter()
            .filter_map(|edge| {
                let src_label = cypher_escape_label(node_label.get(&edge.src_id)?);
                let dst_label = cypher_escape_label(node_label.get(&edge.dst_id)?);
                let rel_type = cypher_escape_label(&edge.rel_type);
                let src_sid = edge.src_id;
                let dst_sid = edge.dst_id;
                Some(format!(
                    "MATCH (a:{src_label} {{_sparrow_export_id: '{src_sid}'}}), \
                     (b:{dst_label} {{_sparrow_export_id: '{dst_sid}'}}) \
                     CREATE (a)-[:{rel_type}]->(b)"
                ))
            })
            .collect();
        if !edge_queries.is_empty() {
            let edge_refs: Vec<&str> = edge_queries.iter().map(String::as_str).collect();
            self.execute_batch(&edge_refs)?;
        }

        Ok(())
    }

    /// Deserialise a [`GraphDump`] from JSON and import it.
    ///
    /// Convenience wrapper around [`import`](Self::import) +
    /// [`serde_json::from_str`].
    pub fn import_json(&self, json: &str) -> Result<()> {
        let dump: GraphDump = serde_json::from_str(json)
            .map_err(|e| sparrowdb_common::Error::InvalidArgument(e.to_string()))?;
        self.import(&dump)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Convert an execution-layer [`Value`] to a [`serde_json::Value`].
fn execution_value_to_json(val: &sparrowdb_execution::types::Value) -> serde_json::Value {
    use sparrowdb_execution::types::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Int64(i) => serde_json::json!(*i),
        Value::Float64(f) => serde_json::json!(*f),
        Value::Bool(b) => serde_json::json!(*b),
        Value::String(s) => serde_json::json!(s),
        Value::NodeRef(nid) => serde_json::json!(nid.0),
        Value::EdgeRef(eid) => serde_json::json!(eid.0),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(execution_value_to_json).collect())
        }
        Value::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), execution_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Build the inline properties string for a Cypher `CREATE` statement.
///
/// Always injects `_sparrow_export_id` set to the original node_id so that
/// edges can be wired during import.
fn build_props_cypher(props: &HashMap<String, serde_json::Value>, sid: Option<u64>) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(id) = sid {
        parts.push(format!("_sparrow_export_id: '{id}'"));
    }

    for (key, val) in props {
        // Skip the migration key if the source already had it.
        if key == "_sparrow_export_id" {
            continue;
        }
        let esc_key = cypher_escape_identifier(key);
        let val_str = json_val_to_cypher_literal(val);
        parts.push(format!("{esc_key}: {val_str}"));
    }

    parts.join(", ")
}

/// Render a JSON value as a Cypher inline literal.
fn json_val_to_cypher_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            // Escape single quotes inside the string.
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // Nested collections: serialise as a JSON string and store as string.
            let s = val.to_string();
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        }
    }
}

/// Escape a label or relationship type name for use in a Cypher query.
/// Backtick-quotes the name if it contains special characters.
fn cypher_escape_label(name: &str) -> String {
    if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        name.to_owned()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

/// Escape a property key identifier.
fn cypher_escape_identifier(name: &str) -> String {
    if name.chars().all(|c| c.is_alphanumeric() || c == '_') && !name.is_empty() {
        name.to_owned()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}
