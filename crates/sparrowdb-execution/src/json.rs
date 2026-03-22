//! JSON serialization helpers for execution types.
//!
//! Shared by `sparrowdb-cli` (NDJSON serve mode) and `sparrowdb-node`
//! (napi-rs bindings) to avoid duplicating the same conversion logic.

use crate::types::{QueryResult, Value};

/// Convert a scalar [`Value`] to a [`serde_json::Value`].
///
/// `NodeRef` and `EdgeRef` are serialized as tagged objects with string ids:
/// `{"$type": "node", "id": "123"}` / `{"$type": "edge", "id": "456"}`.
/// IDs are strings (not numbers) to preserve u64 precision across the JS boundary.
///
/// `Int64` values within `±(2^53-1)` are plain JSON numbers; values outside
/// that range are serialized as decimal strings to avoid silent precision loss.
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        // JavaScript's Number can only represent integers exactly up to 2^53-1.
        // Serialize Int64 values outside the safe range as strings so callers
        // don't silently receive wrong values.
        Value::Int64(i) => {
            const MAX_SAFE: i64 = (1_i64 << 53) - 1;
            const MIN_SAFE: i64 = -MAX_SAFE;
            if *i >= MIN_SAFE && *i <= MAX_SAFE {
                serde_json::json!(i)
            } else {
                serde_json::json!(i.to_string())
            }
        }
        Value::Float64(f) => serde_json::json!(f),
        Value::Bool(b) => serde_json::json!(b),
        Value::String(s) => serde_json::json!(s),
        // IDs are u64, which exceeds JavaScript's safe integer range (2^53-1).
        // Serialize as strings so precision is preserved across the JSON boundary.
        Value::NodeRef(n) => serde_json::json!({"$type": "node", "id": n.0.to_string()}),
        Value::EdgeRef(e) => serde_json::json!({"$type": "edge", "id": e.0.to_string()}),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Materialize a [`QueryResult`] into a JSON object:
///
/// ```json
/// {
///   "columns": ["n.name", "n.age"],
///   "rows": [{"n.name": "Alice", "n.age": 30}, ...]
/// }
/// ```
pub fn query_result_to_json(result: &QueryResult) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = result
                .columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.clone(), value_to_json(val)))
                .collect();
            serde_json::Value::Object(obj)
        })
        .collect();

    serde_json::json!({
        "columns": result.columns,
        "rows": rows,
    })
}
