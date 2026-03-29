//! Graph export / import tooling for cross-version migration (SPA-XXX).
//!
//! Provides [`GraphDump`], [`NodeDump`], and [`EdgeDump`] for serialising the
//! full contents of a [`super::GraphDb`] to JSON, and re-importing that JSON
//! into a (possibly new / freshly-opened) database.

use std::collections::HashMap;

use sparrowdb_common::{col_id_of, NodeId};
use sparrowdb_storage::node_store::Value as StoreValue;

use crate::{GraphDb, Result};

/// A single node extracted from the database during export.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeDump {
    pub node_id: u64,
    pub label: String,
    pub properties: HashMap<String, serde_json::Value>,
}

/// A single directed edge extracted from the database during export.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeDump {
    pub src_id: u64,
    pub dst_id: u64,
    pub rel_type: String,
    pub properties: HashMap<String, serde_json::Value>,
}

/// A complete serialisable snapshot of a SparrowDB graph.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GraphDump {
    pub sparrowdb_version: String,
    pub exported_at: u64,
    pub nodes: Vec<NodeDump>,
    pub edges: Vec<EdgeDump>,
}

impl GraphDb {
    pub fn export(&self) -> Result<GraphDump> {
        let schema_result = self.execute("CALL db.schema()")?;
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
        let mut col_id_to_name: HashMap<u32, String> = HashMap::new();
        for props in label_props.values() {
            for prop_name in props {
                col_id_to_name.insert(col_id_of(prop_name), prop_name.clone());
            }
        }
        let catalog = self.catalog_snapshot();
        let all_labels = catalog.list_labels()?;
        let mut nodes: Vec<NodeDump> = Vec::new();
        for (_label_id, label_name) in &all_labels {
            if label_name.starts_with("__Sparrow") {
                continue;
            }
            let query = format!("MATCH (n:{label_name}) RETURN id(n), n");
            let result = match self.execute(&query) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for row in &result.rows {
                use sparrowdb_execution::types::Value;
                let node_id = match &row[0] {
                    Value::Int64(i) => *i as u64,
                    _ => continue,
                };
                let props_map = match &row[1] {
                    Value::Map(entries) => entries,
                    _ => {
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
        let path = &self.inner.path;
        let rel_tables = catalog.list_rel_tables_with_ids();
        let mut edges: Vec<EdgeDump> = Vec::new();
        for (catalog_id, src_label_id, dst_label_id, rel_type) in &rel_tables {
            let storage_rel_id = sparrowdb_storage::edge_store::RelTableId(*catalog_id as u32);
            if let Ok(store) = sparrowdb_storage::edge_store::EdgeStore::open(path, storage_rel_id)
            {
                let mut seen: std::collections::HashSet<(u64, u64)> =
                    std::collections::HashSet::new();
                if let Ok(records) = store.read_delta() {
                    for rec in records {
                        if seen.insert((rec.src.0, rec.dst.0)) {
                            edges.push(EdgeDump {
                                src_id: rec.src.0,
                                dst_id: rec.dst.0,
                                rel_type: rel_type.clone(),
                                properties: HashMap::new(),
                            });
                        }
                    }
                }
                if let Ok(csr) = store.open_fwd() {
                    let n_nodes = csr.n_nodes();
                    for src_slot in 0..n_nodes {
                        let src_id = (*src_label_id as u64) << 32 | src_slot;
                        for &dst_slot in csr.neighbors(src_slot) {
                            let dst_id = (*dst_label_id as u64) << 32 | dst_slot;
                            if seen.insert((src_id, dst_id)) {
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

    pub fn export_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&self.export()?)
            .map_err(|e| sparrowdb_common::Error::InvalidArgument(e.to_string()))
    }

    /// Re-create all nodes and edges from a [`GraphDump`].
    ///
    /// Uses an in-memory side-table to map original export IDs to new NodeIds.
    /// The mapping is never persisted, so no synthetic `_sparrow_export_id`
    /// property is written to imported nodes (closes #313).
    pub fn import(&self, dump: &GraphDump) -> Result<()> {
        if dump.nodes.is_empty() && dump.edges.is_empty() {
            return Ok(());
        }
        let mut tx = self.begin_write()?;
        let mut id_map: HashMap<u64, NodeId> = HashMap::with_capacity(dump.nodes.len());
        for node in &dump.nodes {
            let label_id = tx.get_or_create_label_id(&node.label)?;
            let named_props: Vec<(String, StoreValue)> = node
                .properties
                .iter()
                .filter_map(|(key, json_val)| {
                    json_val_to_store_value(json_val).map(|v| (key.clone(), v))
                })
                .collect();
            let new_node_id = tx.create_node_named(label_id, &named_props)?;
            id_map.insert(node.node_id, new_node_id);
        }
        for edge in &dump.edges {
            let src = match id_map.get(&edge.src_id) {
                Some(&id) => id,
                None => continue,
            };
            let dst = match id_map.get(&edge.dst_id) {
                Some(&id) => id,
                None => continue,
            };
            tx.create_edge(src, dst, &edge.rel_type, HashMap::new())?;
        }
        tx.commit()?;
        self.refresh_caches();
        Ok(())
    }

    pub fn import_json(&self, json: &str) -> Result<()> {
        let dump: GraphDump = serde_json::from_str(json)
            .map_err(|e| sparrowdb_common::Error::InvalidArgument(e.to_string()))?;
        self.import(&dump)
    }
}

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

fn json_val_to_store_value(val: &serde_json::Value) -> Option<StoreValue> {
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(StoreValue::Int64(if *b { 1 } else { 0 })),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(StoreValue::Int64(i))
            } else {
                n.as_f64().map(StoreValue::Float)
            }
        }
        serde_json::Value::String(s) => Some(StoreValue::Bytes(s.as_bytes().to_vec())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Some(StoreValue::Bytes(val.to_string().into_bytes()))
        }
    }
}
