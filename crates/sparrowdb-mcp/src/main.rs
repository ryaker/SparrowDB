use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

#[derive(Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
}

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.is_empty() {
            continue;
        }

        // Detect notifications before full parse: JSON-RPC notifications have no "id".
        // We still parse fully so we can identify the method for proper dispatch.
        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => handle_request(req),
            Err(e) => {
                // Parse error — only respond if the raw JSON had a non-null "id" field,
                // otherwise it looks like a notification and MCP SDK rejects null-id errors.
                let id = serde_json::from_str::<Value>(&line)
                    .ok()
                    .and_then(|v| v.get("id").cloned())
                    .filter(|v| !v.is_null());
                if id.is_none() {
                    continue; // Malformed notification or request with null id, discard silently.
                }
                Some(JsonRpcResponse {
                    jsonrpc: "2.0".into(),
                    id,
                    result: None,
                    error: Some(json!({"code": -32700, "message": e.to_string()})),
                })
            }
        };

        let response = match response {
            Some(r) => r,
            None => continue, // Notification — no response.
        };

        let resp_str = match serde_json::to_string(&response) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if writeln!(out, "{resp_str}").is_err() {
            break;
        }
        if out.flush().is_err() {
            break;
        }
    }
}

/// Returns `None` for notifications (no response required by MCP spec).
fn handle_request(req: JsonRpcRequest) -> Option<JsonRpcResponse> {
    // MCP spec: messages without an "id" are notifications — must receive no response.
    // Keying off id absence (not method prefix) handles all notifications correctly,
    // including future notification methods beyond "notifications/*".
    req.id.as_ref()?;

    if req.jsonrpc != "2.0" {
        return Some(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: None,
            error: Some(
                json!({"code": -32600, "message": "Invalid Request: jsonrpc must be \"2.0\""}),
            ),
        });
    }

    let result = match req.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "sparrowdb-mcp", "version": "0.1.0"}
        })),
        "tools/list" => Ok(json!({
            "tools": [
                {
                    "name": "execute_cypher",
                    "description": "Execute a Cypher query against SparrowDB",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "db_path": {"type": "string"}
                        },
                        "required": ["query", "db_path"]
                    }
                },
                {
                    "name": "create_entity",
                    "description": "Create a node (entity) in SparrowDB with the given label and properties. Equivalent to CREATE (n:ClassName {props}) but with a structured interface that surfaces descriptive errors (SPA-243).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_path": {"type": "string"},
                            "class_name": {
                                "type": "string",
                                "description": "The node label / class name (e.g. \"Person\")"
                            },
                            "properties": {
                                "type": "object",
                                "description": "Key-value pairs: string, number, or boolean values only.",
                                "additionalProperties": true
                            }
                        },
                        "required": ["db_path", "class_name"]
                    }
                },
                {
                    "name": "merge_node_by_property",
                    "description": "Find-or-create a node by a unique property (upsert). If a node with label and match_key=match_value exists, update it with the given properties; otherwise create a new node. Returns whether the node was created or found. (SPA-247)",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_path": {"type": "string"},
                            "label": {
                                "type": "string",
                                "description": "Node label (e.g. \"Person\")"
                            },
                            "match_key": {
                                "type": "string",
                                "description": "Property name used for matching (e.g. \"id\")"
                            },
                            "match_value": {
                                "description": "Property value to match on (string, number, or boolean)"
                            },
                            "properties": {
                                "type": "object",
                                "description": "Properties to set/update on the node. Scalar values only.",
                                "additionalProperties": true
                            }
                        },
                        "required": ["db_path", "label", "match_key", "match_value"]
                    }
                },
                {
                    "name": "add_property",
                    "description": "Add or update a property on existing nodes matched by label and a filter property. Equivalent to MATCH (n:Label {match_prop: 'match_val'}) SET n.set_prop = set_val RETURN count(n) AS updated (SPA-229).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_path": {"type": "string"},
                            "label": {
                                "type": "string",
                                "description": "Node label to match (e.g. \"Person\")"
                            },
                            "match_prop": {
                                "type": "string",
                                "description": "Property name to match on (e.g. \"id\")"
                            },
                            "match_val": {
                                "type": "string",
                                "description": "String value to match (e.g. \"alice\")"
                            },
                            "set_prop": {
                                "type": "string",
                                "description": "Property name to set (e.g. \"email\")"
                            },
                            "set_val": {
                                "description": "New scalar value to assign (string, number, or boolean)"
                            }
                        },
                        "required": ["db_path", "label", "match_prop", "match_val", "set_prop", "set_val"]
                    }
                },
                {
                    "name": "checkpoint",
                    "description": "Flush WAL and compact the database",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"db_path": {"type": "string"}},
                        "required": ["db_path"]
                    }
                },
                {
                    "name": "info",
                    "description": "Get database metadata",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"db_path": {"type": "string"}},
                        "required": ["db_path"]
                    }
                }
            ]
        })),
        "tools/call" => Ok(handle_tool_call(req.params)),
        _ => Err(json!({"code": -32601, "message": format!("Method not found: {}", req.method)})),
    };

    Some(match result {
        Ok(r) => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(r),
            error: None,
        },
        Err(e) => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: None,
            error: Some(e),
        },
    })
}

/// All tool calls return a successful JSON-RPC result.
/// Errors are expressed as `isError: true` in the content, per MCP spec.
/// (MCP SDK clients reject `{ error }` responses from tool calls.)
fn handle_tool_call(params: Option<Value>) -> Value {
    match handle_tool_call_inner(params) {
        Ok(v) => v,
        Err(msg) => json!({
            "content": [{"type": "text", "text": msg}],
            "isError": true
        }),
    }
}

fn handle_tool_call_inner(params: Option<Value>) -> Result<Value, String> {
    let params = params.ok_or("Missing params")?;
    let tool_name = params["name"].as_str().ok_or("Missing tool name")?;
    let args = &params["arguments"];

    match tool_name {
        "add_property" => {
            let db_path = args["db_path"].as_str().ok_or("Missing db_path")?;
            let label = args["label"].as_str().ok_or("Missing label")?;
            let match_prop = args["match_prop"].as_str().ok_or("Missing match_prop")?;
            let match_val = args["match_val"].as_str().ok_or("Missing match_val")?;
            let set_prop = args["set_prop"].as_str().ok_or("Missing set_prop")?;
            let set_val = &args["set_val"];

            validate_cypher_identifier(label, "add_property: label")?;
            validate_cypher_identifier(match_prop, "add_property: match_prop")?;
            validate_cypher_identifier(set_prop, "add_property: set_prop")?;
            let cypher_set_val = json_scalar_to_cypher(set_val).ok_or_else(|| match set_val {
                Value::Null => {
                    "add_property: set_val is null; use a concrete scalar value".to_string()
                }
                Value::Array(_) => {
                    "add_property: set_val is an array; only scalar values are supported"
                        .to_string()
                }
                Value::Object(_) => {
                    "add_property: set_val is a nested object; only scalar values are supported"
                        .to_string()
                }
                _ => "add_property: unsupported set_val type".to_string(),
            })?;
            let set_query = format!(
                "MATCH (n:{label} {{{match_prop}: '{match_val_escaped}'}}) SET n.{set_prop} = {cypher_set_val}",
                label = label,
                match_prop = match_prop,
                match_val_escaped = escape_cypher_string(match_val),
                set_prop = set_prop,
                cypher_set_val = cypher_set_val,
            );

            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path)).map_err(|e| {
                format!(
                    "add_property: failed to open database at '{}': {}",
                    db_path, e
                )
            })?;

            let count_query = format!(
                "MATCH (n:{label} {{{match_prop}: '{match_val_escaped}'}}) RETURN count(n) AS cnt",
                label = label,
                match_prop = match_prop,
                match_val_escaped = escape_cypher_string(match_val),
            );
            let count_result = db.execute(&count_query).map_err(|e| {
                format!(
                    "add_property: count query failed for label '{}': {}",
                    label, e
                )
            })?;

            let matched: i64 = count_result
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(|v| match v {
                    sparrowdb_execution::types::Value::Int64(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(0);

            if matched == 0 {
                return Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": format!(
                            "add_property: 0 nodes matched (label: '{}', {}='{}'); property not set.",
                            label, match_prop, match_val
                        )
                    }],
                    "updated": 0
                }));
            }

            db.execute(&set_query)
                .map_err(|e| format!("add_property: SET failed for label '{}': {}", label, e))?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "add_property: set '{}.{}' on {} node(s) matching {}='{}'.",
                        label, set_prop, matched, match_prop, match_val
                    )
                }],
                "updated": matched
            }))
        }
        "checkpoint" => {
            let db_path = args["db_path"].as_str().ok_or("Missing db_path")?;
            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path))
                .map_err(|e| e.to_string())?;
            db.checkpoint().map_err(|e| e.to_string())?;
            Ok(json!({"content": [{"type": "text", "text": "Checkpoint complete"}]}))
        }
        "create_entity" => {
            let db_path = args["db_path"].as_str().ok_or("Missing db_path")?;
            let class_name = args["class_name"].as_str().ok_or("Missing class_name")?;
            let props = &args["properties"];

            validate_cypher_identifier(class_name, "create_entity: class_name")?;
            let mut prop_parts: Vec<String> = Vec::new();
            if let Some(obj) = props.as_object() {
                for (key, val) in obj {
                    validate_cypher_identifier(key, "create_entity: property key")?;
                    let cypher_val = json_scalar_to_cypher(val).ok_or_else(|| match val {
                        Value::Null => format!("create_entity: property '{}' is null; use a concrete value", key),
                        Value::Array(_) => format!("create_entity: property '{}' is an array; only scalar values are supported", key),
                        Value::Object(_) => format!("create_entity: property '{}' is a nested object; only scalar values are supported", key),
                        _ => format!("create_entity: unsupported value for property '{}'", key),
                    })?;
                    prop_parts.push(format!("{}: {}", key, cypher_val));
                }
            }
            let props_clause = if prop_parts.is_empty() {
                String::new()
            } else {
                format!(" {{{}}}", prop_parts.join(", "))
            };
            let query = format!("CREATE (n:{}{})", class_name, props_clause);

            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path)).map_err(|e| {
                format!(
                    "create_entity: failed to open database at '{}': {}",
                    db_path, e
                )
            })?;

            db.execute(&query).map_err(|e| {
                format!(
                    "create_entity: write failed for class '{}': {}",
                    class_name, e
                )
            })?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!("Entity created successfully (class: '{}')", class_name)
                }]
            }))
        }
        "execute_cypher" => {
            let db_path = args["db_path"].as_str().ok_or("Missing db_path")?;
            let query = args["query"].as_str().ok_or("Missing query")?;
            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path))
                .map_err(|e| e.to_string())?;
            let result = db.execute(query).map_err(|e| e.to_string())?;
            Ok(json!({
                "content": [{"type": "text", "text": format!("{result:?}")}]
            }))
        }
        "info" => {
            let db_path = args["db_path"].as_str().ok_or("Missing db_path")?;
            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path))
                .map_err(|e| e.to_string())?;
            let rx = db.begin_read().map_err(|e| e.to_string())?;
            let txn_id = rx.snapshot_txn_id;
            let meta = json!({
                "db_path": db_path,
                "txn_id": txn_id,
                "node_count": null,
                "edge_count": null,
                "note": "node_count/edge_count will be populated once the catalog API is promoted"
            });
            Ok(json!({"content": [{"type": "text", "text": meta.to_string()}]}))
        }
        "merge_node_by_property" => {
            let db_path = args["db_path"].as_str().ok_or("Missing db_path")?;
            let label = args["label"].as_str().ok_or("Missing label")?;
            let match_key = args["match_key"].as_str().ok_or("Missing match_key")?;
            let match_value = &args["match_value"];
            let properties = &args["properties"];

            validate_cypher_identifier(label, "merge_node_by_property: label")?;
            validate_cypher_identifier(match_key, "merge_node_by_property: match_key")?;

            let cypher_match_val = json_scalar_to_cypher(match_value).ok_or_else(|| {
                format!(
                    "merge_node_by_property: match_value must be a scalar, got {:?}",
                    match_value
                )
            })?;

            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path)).map_err(|e| {
                format!(
                    "merge_node_by_property: failed to open database at '{}': {}",
                    db_path, e
                )
            })?;

            // Build SET and CREATE property fragments from `properties` in a single pass,
            // excluding the match key (handled separately in both paths).
            // set_parts  → "n.k = v"   used in the MATCH+SET path
            // prop_parts → "k: v"      used in the CREATE path
            let mut set_parts: Vec<String> = Vec::new();
            let mut prop_parts: Vec<String> = Vec::new();
            if let Some(obj) = properties.as_object() {
                for (k, v) in obj {
                    if k == match_key {
                        continue; // match key handled separately
                    }
                    validate_cypher_identifier(k, "merge_node_by_property: property key")?;
                    let cypher_v = json_scalar_to_cypher(v).ok_or_else(|| {
                        format!("merge_node_by_property: property '{}' must be a scalar", k)
                    })?;
                    set_parts.push(format!("n.{k} = {cypher_v}"));
                    prop_parts.push(format!("{k}: {cypher_v}"));
                }
            }

            // Check whether the node already exists before the upsert so we can accurately
            // report whether it was created or matched.
            let count_query =
                format!("MATCH (n:{label} {{{match_key}: {cypher_match_val}}}) RETURN count(n)",);
            let count_result = db
                .execute(&count_query)
                .map_err(|e| format!("merge_node_by_property: pre-merge count failed: {}", e))?;
            let pre_existing = count_result
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(|v| match v {
                    sparrowdb_execution::types::Value::Int64(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(0);

            // Use two-path logic (MATCH+SET or CREATE) since MERGE is not yet
            // supported by the Cypher parser.
            let result = if pre_existing > 0 {
                // Node exists — match it and optionally set extra properties.
                let query = if set_parts.is_empty() {
                    format!("MATCH (n:{label} {{{match_key}: {cypher_match_val}}})",)
                } else {
                    format!(
                        "MATCH (n:{label} {{{match_key}: {cypher_match_val}}}) SET {set_clause_inner}",
                        set_clause_inner = set_parts.join(", "),
                    )
                };
                db.execute(&query)
                    .map_err(|e| format!("merge_node_by_property: MATCH failed: {}", e))?
            } else {
                // Node does not exist — create it with all properties.
                let mut all_props: Vec<String> = vec![format!("{match_key}: {cypher_match_val}")];
                all_props.extend(prop_parts);
                let props_str = all_props.join(", ");
                let query = format!("CREATE (n:{label} {{{props_str}}})",);
                db.execute(&query)
                    .map_err(|e| format!("merge_node_by_property: CREATE failed: {}", e))?
            };

            let created = pre_existing == 0;
            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "merge_node_by_property: node with {}={} on label '{}' {} ({} row(s) returned).",
                        match_key, match_value, label,
                        if created { "created" } else { "found (existing)" },
                        result.rows.len()
                    )
                }],
                "rows": result.rows.len(),
                "created": created
            }))
        }
        _ => Err(format!("Unknown tool: {tool_name}")),
    }
}

/// Validate a Cypher identifier (label or property key).
/// Accepts only `[A-Za-z_][A-Za-z0-9_]*` — no spaces, backticks, or special chars.
/// Returns `Err` with a descriptive message when the identifier is unsafe.
fn validate_cypher_identifier(ident: &str, context: &str) -> Result<(), String> {
    if ident.is_empty() {
        return Err(format!("{context}: identifier must not be empty"));
    }
    let mut chars = ident.chars();
    let first_ok = chars
        .next()
        .map(|c| c.is_ascii_alphabetic() || c == '_')
        .unwrap_or(false);
    if !first_ok || chars.any(|c| !c.is_ascii_alphanumeric() && c != '_') {
        return Err(format!(
            "{context}: identifier '{ident}' contains unsafe characters; \
             only [A-Za-z_][A-Za-z0-9_]* is allowed"
        ));
    }
    Ok(())
}

/// Escape a string for safe Cypher single-quoted literal interpolation.
fn escape_cypher_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Convert a JSON scalar to a Cypher literal string. Returns None for
/// non-scalar values (null, array, object).
fn json_scalar_to_cypher(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(format!("'{}'", escape_cypher_string(s))),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notifications_return_none() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: None,
            method: "notifications/initialized".into(),
            params: None,
        };
        assert!(handle_request(req).is_none());
    }

    #[test]
    fn test_notifications_generic_return_none() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: None,
            method: "notifications/something_else".into(),
            params: None,
        };
        assert!(handle_request(req).is_none());
    }

    #[test]
    fn test_tool_error_is_is_error_not_rpc_error() {
        let result = handle_tool_call(None);
        assert!(result["isError"].as_bool().unwrap_or(false));
        assert!(result["content"].is_array());
    }

    #[test]
    fn test_json_scalar_to_cypher() {
        assert_eq!(
            json_scalar_to_cypher(&json!("hello")),
            Some("'hello'".into())
        );
        assert_eq!(json_scalar_to_cypher(&json!(42)), Some("42".into()));
        assert_eq!(json_scalar_to_cypher(&json!(true)), Some("true".into()));
        assert!(json_scalar_to_cypher(&json!(null)).is_none());
        assert!(json_scalar_to_cypher(&json!([])).is_none());
    }
}
