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

        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => handle_request(req),
            Err(e) => JsonRpcResponse {
                jsonrpc: "2.0".into(),
                id: None,
                result: None,
                error: Some(json!({"code": -32700, "message": e.to_string()})),
            },
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

fn handle_request(req: JsonRpcRequest) -> JsonRpcResponse {
    if req.jsonrpc != "2.0" {
        return JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: None,
            error: Some(
                json!({"code": -32600, "message": "Invalid Request: jsonrpc must be \"2.0\""}),
            ),
        };
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
        "tools/call" => handle_tool_call(req.params),
        _ => Err(json!({"code": -32601, "message": "Method not found"})),
    };

    match result {
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
    }
}

/// Escape a string for safe Cypher single-quoted literal interpolation.
fn escape_cypher_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Build a Cypher CREATE query from a class name and a JSON properties object.
///
/// Returns Err with a descriptive message when class_name is empty or a
/// property value is null, an array, or a nested object.
fn build_create_query(class_name: &str, props: &Value) -> Result<String, String> {
    if class_name.is_empty() {
        return Err("create_entity: class_name must not be empty".into());
    }

    let mut prop_parts: Vec<String> = Vec::new();

    if let Some(obj) = props.as_object() {
        for (key, val) in obj {
            if key.is_empty() {
                return Err("create_entity: property key must not be empty".into());
            }
            let cypher_val = match val {
                Value::String(s) => format!("'{}'", escape_cypher_string(s)),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => {
                    return Err(format!(
                        "create_entity: property '{}' is null; use a concrete value",
                        key
                    ))
                }
                Value::Array(_) => {
                    return Err(format!(
                        "create_entity: property '{}' is an array; only scalar values are supported",
                        key
                    ))
                }
                Value::Object(_) => {
                    return Err(format!(
                        "create_entity: property '{}' is a nested object; only scalar values are supported",
                        key
                    ))
                }
            };
            prop_parts.push(format!("{}: {}", key, cypher_val));
        }
    }

    let props_clause = if prop_parts.is_empty() {
        String::new()
    } else {
        format!(" {{{}}}", prop_parts.join(", "))
    };

    Ok(format!("CREATE (n:{}{})", class_name, props_clause))
}

/// Build a Cypher MATCH … SET query for the add_property tool.
///
/// Returns Err with a descriptive message when any argument is invalid
/// (empty label, empty property names, unsupported value type).
fn build_add_property_query(
    label: &str,
    match_prop: &str,
    match_val: &str,
    set_prop: &str,
    set_val: &Value,
) -> Result<String, String> {
    if label.is_empty() {
        return Err("add_property: label must not be empty".into());
    }
    if match_prop.is_empty() {
        return Err("add_property: match_prop must not be empty".into());
    }
    if set_prop.is_empty() {
        return Err("add_property: set_prop must not be empty".into());
    }

    let cypher_set_val = match set_val {
        Value::String(s) => format!("'{}'", escape_cypher_string(s)),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => {
            return Err(
                "add_property: set_val is null; use a concrete scalar value".into(),
            )
        }
        Value::Array(_) => {
            return Err(
                "add_property: set_val is an array; only scalar values are supported".into(),
            )
        }
        Value::Object(_) => {
            return Err(
                "add_property: set_val is a nested object; only scalar values are supported"
                    .into(),
            )
        }
    };

    Ok(format!(
        "MATCH (n:{label} {{{match_prop}: '{match_val_escaped}'}}) SET n.{set_prop} = {cypher_set_val}",
        label = label,
        match_prop = match_prop,
        match_val_escaped = escape_cypher_string(match_val),
        set_prop = set_prop,
        cypher_set_val = cypher_set_val,
    ))
}

/// Build a Cypher MATCH count query to check how many nodes match.
fn build_count_query(label: &str, match_prop: &str, match_val: &str) -> String {
    format!(
        "MATCH (n:{label} {{{match_prop}: '{match_val_escaped}'}}) RETURN count(n) AS cnt",
        label = label,
        match_prop = match_prop,
        match_val_escaped = escape_cypher_string(match_val),
    )
}

fn handle_tool_call(params: Option<Value>) -> Result<Value, Value> {
    let params = params.ok_or_else(|| json!({"code": -32602, "message": "Missing params"}))?;
    let tool_name = params["name"]
        .as_str()
        .ok_or_else(|| json!({"code": -32602, "message": "Missing tool name"}))?;
    let args = &params["arguments"];

    match tool_name {
        "add_property" => {
            let db_path = args["db_path"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing db_path"}))?;
            let label = args["label"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing label"}))?;
            let match_prop = args["match_prop"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing match_prop"}))?;
            let match_val = args["match_val"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing match_val"}))?;
            let set_prop = args["set_prop"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing set_prop"}))?;
            let set_val = &args["set_val"];

            // Validate and build the SET query.
            let set_query =
                build_add_property_query(label, match_prop, match_val, set_prop, set_val)
                    .map_err(|msg| json!({"code": -32602, "message": msg}))?;

            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path)).map_err(|e| {
                json!({
                    "code": -32000,
                    "message": format!(
                        "add_property: failed to open database at '{}': {}",
                        db_path, e
                    )
                })
            })?;

            // Count how many nodes match before applying SET, so we can report
            // zero-match without the caller having to inspect empty rows.
            let count_query = build_count_query(label, match_prop, match_val);
            let count_result = db.execute(&count_query).map_err(|e| {
                json!({
                    "code": -32000,
                    "message": format!(
                        "add_property: count query failed for label '{}': {}",
                        label, e
                    )
                })
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

            // Apply the SET.
            db.execute(&set_query).map_err(|e| {
                json!({
                    "code": -32000,
                    "message": format!(
                        "add_property: SET failed for label '{}': {}",
                        label, e
                    )
                })
            })?;

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
            let db_path = args["db_path"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing db_path"}))?;
            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path))
                .map_err(|e| json!({"code": -32000, "message": e.to_string()}))?;
            db.checkpoint()
                .map_err(|e| json!({"code": -32000, "message": e.to_string()}))?;
            Ok(json!({"content": [{"type": "text", "text": "Checkpoint complete"}]}))
        }
        "create_entity" => {
            let db_path = args["db_path"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing db_path"}))?;
            let class_name = args["class_name"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing class_name"}))?;
            let props = &args["properties"];

            // Build the Cypher CREATE statement, surfacing argument errors as
            // -32602 (invalid params) not -32000 (generic execution error).
            // SPA-243 root cause: the tool was absent; callers received the
            // generic "Unknown tool: create_entity" error.
            let query = build_create_query(class_name, props)
                .map_err(|msg| json!({"code": -32602, "message": msg}))?;

            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path)).map_err(|e| {
                json!({
                    "code": -32000,
                    "message": format!(
                        "create_entity: failed to open database at '{}': {}",
                        db_path, e
                    )
                })
            })?;

            db.execute(&query).map_err(|e| {
                json!({
                    "code": -32000,
                    "message": format!(
                        "create_entity: write failed for class '{}': {}",
                        class_name, e
                    )
                })
            })?;

            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": format!("Entity created successfully (class: '{}')", class_name)
                }]
            }))
        }
        "execute_cypher" => {
            let db_path = args["db_path"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing db_path"}))?;
            let query = args["query"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing query"}))?;
            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path))
                .map_err(|e| json!({"code": -32000, "message": e.to_string()}))?;
            match db.execute(query) {
                Ok(result) => Ok(json!({
                    "content": [{"type": "text", "text": format!("{result:?}")}]
                })),
                Err(e) => Err(json!({"code": -32000, "message": e.to_string()})),
            }
        }
        "info" => {
            let db_path = args["db_path"]
                .as_str()
                .ok_or_else(|| json!({"code": -32602, "message": "Missing db_path"}))?;
            let db = sparrowdb::GraphDb::open(std::path::Path::new(db_path))
                .map_err(|e| json!({"code": -32000, "message": e.to_string()}))?;
            let rx = db
                .begin_read()
                .map_err(|e| json!({"code": -32000, "message": e.to_string()}))?;
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
        _ => Err(json!({"code": -32602, "message": format!("Unknown tool: {tool_name}")})),
    }
}
