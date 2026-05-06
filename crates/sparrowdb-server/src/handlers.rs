//! HTTP route handlers.
//!
//! Each handler returns a [`tiny_http::Response`] with `Content-Type:
//! application/json` and a permissive CORS header set.  Response bodies are
//! always JSON (an object, never a bare value) so clients can rely on a
//! consistent shape.

use crate::auth::AuthConfig;
use crate::VERSION;
use sparrowdb::GraphDb;
use sparrowdb_execution::{json::query_result_to_json, Value};
use std::collections::HashMap;
use std::io::Read;
use tiny_http::{Header, Method, Request, Response};

/// Maximum accepted request body length (1 MiB) — protects against runaway POSTs.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Top-level dispatcher.  Routes by `(method, path)`.
pub fn dispatch(mut req: Request, db: &GraphDb, auth: &AuthConfig) {
    let method = req.method().clone();
    // Strip query string — Phase A has no query parameters.
    let path = req.url().split('?').next().unwrap_or("");

    tracing::debug!(?method, %path, "incoming request");

    let response = match (&method, path) {
        // CORS preflight — answer any path.
        (Method::Options, _) => cors_preflight(),

        // Public routes.
        (Method::Get, "/health") | (Method::Get, "/health/") => handle_health(),

        // Authenticated routes.
        (Method::Get, "/info") | (Method::Get, "/info/") => {
            if !check_auth(&req, auth) {
                unauthorized()
            } else {
                handle_info(db)
            }
        }
        (Method::Post, "/cypher") | (Method::Post, "/cypher/") => {
            if !check_auth(&req, auth) {
                unauthorized()
            } else {
                handle_cypher(&mut req, db)
            }
        }

        // Anything else.
        _ => not_found(),
    };

    if let Err(e) = req.respond(response) {
        tracing::warn!("response write error: {e}");
    }
}

// ── Auth helpers ────────────────────────────────────────────────────

fn check_auth(req: &Request, auth: &AuthConfig) -> bool {
    let header = req
        .headers()
        .iter()
        .find(|h| h.field.equiv("Authorization"))
        .map(|h| h.value.as_str());
    auth.check_header(header)
}

// ── Route handlers ──────────────────────────────────────────────────

fn handle_health() -> Response<std::io::Cursor<Vec<u8>>> {
    let body = serde_json::json!({
        "status": "ok",
        "version": VERSION,
    });
    json_response(200, &body)
}

fn handle_info(db: &GraphDb) -> Response<std::io::Cursor<Vec<u8>>> {
    let labels = match db.labels() {
        Ok(v) => v,
        Err(e) => return engine_error(&e.to_string()),
    };
    let rel_types = match db.relationship_types() {
        Ok(v) => v,
        Err(e) => return engine_error(&e.to_string()),
    };
    let (nodes, edges) = match db.db_counts() {
        Ok(v) => v,
        Err(e) => return engine_error(&e.to_string()),
    };

    let body = serde_json::json!({
        "version": VERSION,
        "labels": labels,
        "relationship_types": rel_types,
        "counts": {
            "nodes": nodes,
            "edges": edges,
        },
    });
    json_response(200, &body)
}

fn handle_cypher(req: &mut Request, db: &GraphDb) -> Response<std::io::Cursor<Vec<u8>>> {
    // Read body up to MAX_BODY_BYTES.
    let body = match read_body(req) {
        Ok(b) => b,
        Err(e) => return bad_request(&format!("failed to read request body: {e}")),
    };

    // Parse the JSON envelope.
    #[derive(serde::Deserialize)]
    struct CypherRequest {
        query: String,
        #[serde(default)]
        params: serde_json::Value, // optional; may be Null or {}
    }

    let parsed: CypherRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("invalid JSON body: {e}")),
    };

    if parsed.query.trim().is_empty() {
        return bad_request("query string is empty");
    }

    // Convert the optional `params` JSON object into Value map.
    let params = match parsed.params {
        serde_json::Value::Null => HashMap::new(),
        serde_json::Value::Object(map) => {
            let mut out = HashMap::with_capacity(map.len());
            for (k, v) in map {
                match json_to_value(v) {
                    Ok(val) => {
                        out.insert(k, val);
                    }
                    Err(e) => {
                        return bad_request(&format!(
                            "param `{k}` could not be coerced to a SparrowDB value: {e}"
                        ));
                    }
                }
            }
            out
        }
        _ => return bad_request("`params` must be a JSON object or null"),
    };

    // Execute against the engine.
    let result = if params.is_empty() {
        db.execute(&parsed.query)
    } else {
        db.execute_with_params(&parsed.query, params)
    };

    match result {
        Ok(r) => json_response(200, &query_result_to_json(&r)),
        Err(e) => engine_error(&e.to_string()),
    }
}

// ── Error responses ─────────────────────────────────────────────────

fn unauthorized() -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(
        401,
        &serde_json::json!({
            "error": "unauthorized: missing or invalid bearer token",
        }),
    )
}

fn bad_request(msg: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(400, &serde_json::json!({ "error": msg }))
}

fn engine_error(msg: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(500, &serde_json::json!({ "error": msg }))
}

fn not_found() -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(
        404,
        &serde_json::json!({
            "error": "not found",
        }),
    )
}

fn cors_preflight() -> Response<std::io::Cursor<Vec<u8>>> {
    // Empty 204 with CORS headers.
    let mut response = Response::from_data(Vec::<u8>::new()).with_status_code(204);
    for h in cors_headers() {
        response.add_header(h);
    }
    response
}

// ── Response builder ────────────────────────────────────────────────

fn json_response(status: u16, body: &serde_json::Value) -> Response<std::io::Cursor<Vec<u8>>> {
    let bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::from_data(bytes).with_status_code(status);
    response.add_header(
        "Content-Type: application/json; charset=utf-8"
            .parse::<Header>()
            .expect("static header parses"),
    );
    for h in cors_headers() {
        response.add_header(h);
    }
    response
}

/// Permissive Phase-1 CORS — `*` origin, POST + GET, common headers.
fn cors_headers() -> Vec<Header> {
    vec![
        "Access-Control-Allow-Origin: *"
            .parse()
            .expect("static header parses"),
        "Access-Control-Allow-Methods: POST, GET, OPTIONS"
            .parse()
            .expect("static header parses"),
        "Access-Control-Allow-Headers: Authorization, Content-Type"
            .parse()
            .expect("static header parses"),
    ]
}

// ── Body reading ────────────────────────────────────────────────────

fn read_body(req: &mut Request) -> std::io::Result<Vec<u8>> {
    // If a Content-Length header was supplied, refuse early on huge bodies.
    if let Some(len) = req.body_length() {
        if len > MAX_BODY_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("body too large: {len} > {MAX_BODY_BYTES}"),
            ));
        }
    }
    let mut buf = Vec::new();
    let n = req
        .as_reader()
        .take((MAX_BODY_BYTES as u64) + 1)
        .read_to_end(&mut buf)?;
    if n > MAX_BODY_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("body too large: > {MAX_BODY_BYTES}"),
        ));
    }
    Ok(buf)
}

// ── JSON ⇒ Value ────────────────────────────────────────────────────

/// Best-effort conversion from a JSON value into a SparrowDB [`Value`].
///
/// Maps:
/// - `null` → [`Value::Null`]
/// - `true` / `false` → [`Value::Bool`]
/// - numbers without a fractional part that fit in `i64` → [`Value::Int64`]
/// - any other number → [`Value::Float64`]
/// - strings → [`Value::String`]
/// - arrays → [`Value::List`] (recursively)
/// - objects → [`Value::Map`] with stable key ordering (recursively)
///
/// Returns `Err` if a number is not representable as `i64` *or* `f64`
/// (effectively never — `serde_json::Number` only carries those — but the
/// signature leaves room for stricter types later).
fn json_to_value(v: serde_json::Value) -> Result<Value, String> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float64(f))
            } else {
                Err(format!("number `{n}` not representable as i64 or f64"))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s)),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(json_to_value(item)?);
            }
            Ok(Value::List(out))
        }
        serde_json::Value::Object(map) => {
            let mut out = Vec::with_capacity(map.len());
            for (k, v) in map {
                out.push((k, json_to_value(v)?));
            }
            Ok(Value::Map(out))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_int_to_value() {
        assert_eq!(
            json_to_value(serde_json::json!(42)).unwrap(),
            Value::Int64(42)
        );
    }

    #[test]
    fn json_float_to_value() {
        match json_to_value(serde_json::json!(2.5)).unwrap() {
            Value::Float64(f) => assert!((f - 2.5).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn json_array_to_list() {
        let v = json_to_value(serde_json::json!([1, "two", true])).unwrap();
        let Value::List(items) = v else {
            panic!("expected list")
        };
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn json_object_to_map() {
        let v = json_to_value(serde_json::json!({"a": 1, "b": "two"})).unwrap();
        let Value::Map(entries) = v else {
            panic!("expected map")
        };
        assert_eq!(entries.len(), 2);
    }
}
