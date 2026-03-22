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
        // Stdin EOF or read error → exit gracefully
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
        // Stdout write error (broken pipe) → exit gracefully
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

fn handle_tool_call(params: Option<Value>) -> Result<Value, Value> {
    let params = params.ok_or_else(|| json!({"code": -32602, "message": "Missing params"}))?;
    let tool_name = params["name"]
        .as_str()
        .ok_or_else(|| json!({"code": -32602, "message": "Missing tool name"}))?;
    let args = &params["arguments"];

    match tool_name {
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
