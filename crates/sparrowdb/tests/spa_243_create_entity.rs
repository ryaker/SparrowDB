//! Regression tests for SPA-243: create_entity MCP tool write path.
//!
//! The create_entity tool was absent from the MCP layer — callers received a
//! generic "Error occurred during tool execution" with no diagnostic text.
//!
//! These tests verify that:
//! 1. Successful entity creation via GraphDb::execute with a CREATE statement
//!    returns Ok and the entity is retrievable via MATCH.
//! 2. The MCP create_entity tool (subprocess) returns a success-shaped
//!    response when the write path is exercised end-to-end.
//! 3. Descriptive errors are returned for bad inputs (empty class name, null
//!    property values) — not a generic swallowed message.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// A valid CREATE statement must return Ok.
#[test]
fn spa243_create_entity_succeeds() {
    let (_dir, db) = make_db();
    let result = db.execute("CREATE (n:Person {name: 'Rich Yaker'})");
    assert!(result.is_ok(), "CREATE must succeed; got: {:?}", result.err());
}

/// A node created via execute must be retrievable via a subsequent MATCH.
#[test]
fn spa243_created_entity_is_retrievable() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    let result = db
        .execute("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name")
        .expect("MATCH must succeed");
    assert_eq!(result.rows.len(), 1, "expected exactly 1 row, got {:?}", result.rows);
}

/// Multiple entities with different classes round-trip correctly.
#[test]
fn spa243_multiple_entities_retrievable() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Company {name: 'Acme'})").unwrap();

    let people = db.execute("MATCH (n:Person) RETURN n.name").expect("MATCH Person must succeed");
    assert_eq!(people.rows.len(), 2, "expected 2 Person nodes, got {:?}", people.rows);

    let companies = db.execute("MATCH (n:Company) RETURN n.name").expect("MATCH Company must succeed");
    assert_eq!(companies.rows.len(), 1, "expected 1 Company node, got {:?}", companies.rows);
}

/// An invalid class name must produce a descriptive error, not a generic one.
#[test]
fn spa243_empty_class_name_returns_descriptive_error() {
    let (_dir, db) = make_db();
    let err = db.execute("CREATE (n: {name: 'test'})").expect_err("CREATE with no label should fail");
    let msg = err.to_string();
    assert!(!msg.is_empty(), "error message must not be empty");
    assert_ne!(msg, "Error occurred during tool execution", "must not return generic error string");
}

/// A CREATE with a null property value must return a descriptive error.
#[test]
fn spa243_null_property_value_returns_descriptive_error() {
    let (_dir, db) = make_db();
    let err = db.execute("CREATE (n:Person {name: null})").expect_err("CREATE with null property must fail");
    let msg = err.to_string();
    assert!(!msg.is_empty(), "error message must not be empty");
    assert!(
        msg.contains("null") || msg.contains("invalid") || msg.contains("concrete"),
        "error '{msg}' should mention null or invalid value"
    );
}

/// Spawn the sparrowdb-mcp binary and verify create_entity succeeds end-to-end.
/// Skipped if the binary is not yet built.
#[test]
fn spa243_mcp_create_entity_succeeds_end_to_end() {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let binary = {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir.parent().and_then(|p| p.parent()).and_then(|p| p.parent()).expect("workspace root");
        let debug_bin = workspace_root.join("target").join("debug").join("sparrowdb-mcp");
        let release_bin = workspace_root.join("target").join("release").join("sparrowdb-mcp");
        if debug_bin.exists() { debug_bin } else if release_bin.exists() { release_bin } else {
            eprintln!("sparrowdb-mcp binary not found; skipping");
            return;
        }
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().to_str().expect("utf8 path");
    { let _db = open(dir.path()).expect("pre-create db"); }

    let initialize = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}});
    let create_call = serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_entity","arguments":{"db_path":db_path,"class_name":"Person","properties":{"name":"Rich Yaker"}}}});
    let read_call = serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"execute_cypher","arguments":{"db_path":db_path,"query":"MATCH (n:Person) WHERE n.name = 'Rich Yaker' RETURN n.name"}}});

    let input = format!("{}\n{}\n{}\n",
        serde_json::to_string(&initialize).unwrap(),
        serde_json::to_string(&create_call).unwrap(),
        serde_json::to_string(&read_call).unwrap());

    let mut child = Command::new(&binary).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().expect("spawn");
    child.stdin.take().expect("stdin").write_all(input.as_bytes()).expect("write");
    let output = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut create_resp: Option<serde_json::Value> = None;
    let mut read_resp: Option<serde_json::Value> = None;
    for line in stdout.lines() {
        if line.is_empty() { continue; }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            if v["id"] == serde_json::json!(2) { create_resp = Some(v); }
            else if v["id"] == serde_json::json!(3) { read_resp = Some(v); }
        }
    }

    let cr = create_resp.expect("no create_entity response");
    assert!(cr["error"].is_null(), "create_entity must not error; got: {cr}");
    assert!(!cr["result"].is_null(), "create_entity must return result; got: {cr}");

    let rr = read_resp.expect("no read-back response");
    assert!(rr["error"].is_null(), "read-back must not error; got: {rr}");
    let text = rr["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("Rich Yaker") || text.contains("row"), "read-back should contain entity; got: {text}");
}

/// The MCP create_entity tool must return a descriptive error for a reserved label.
/// Skipped if the binary is not yet built.
#[test]
fn spa243_mcp_create_entity_reserved_label_returns_descriptive_error() {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let binary = {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir.parent().and_then(|p| p.parent()).and_then(|p| p.parent()).expect("workspace root");
        let debug_bin = workspace_root.join("target").join("debug").join("sparrowdb-mcp");
        let release_bin = workspace_root.join("target").join("release").join("sparrowdb-mcp");
        if debug_bin.exists() { debug_bin } else if release_bin.exists() { release_bin } else {
            eprintln!("sparrowdb-mcp binary not found; skipping");
            return;
        }
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().to_str().expect("utf8 path");
    { let _db = open(dir.path()).expect("pre-create db"); }

    let initialize = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}});
    let create_call = serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_entity","arguments":{"db_path":db_path,"class_name":"__SO_Forbidden","properties":{"name":"attempt"}}}});

    let input = format!("{}\n{}\n",
        serde_json::to_string(&initialize).unwrap(),
        serde_json::to_string(&create_call).unwrap());

    let mut child = Command::new(&binary).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().expect("spawn");
    child.stdin.take().expect("stdin").write_all(input.as_bytes()).expect("write");
    let output = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut resp: Option<serde_json::Value> = None;
    for line in stdout.lines() {
        if line.is_empty() { continue; }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            if v["id"] == serde_json::json!(2) { resp = Some(v); break; }
        }
    }

    let resp = resp.expect("no response");
    assert!(!resp["error"].is_null(), "reserved-label create must error; got: {resp}");
    let msg = resp["error"]["message"].as_str().expect("error.message must be string");
    assert!(!msg.is_empty(), "error.message must not be empty; got: {resp}");
    assert_ne!(msg, "Error occurred during tool execution", "must not return generic error");
    assert!(
        msg.contains("__SO_") || msg.contains("reserved") || msg.contains("create_entity"),
        "error '{msg}' should mention reserved label or create_entity"
    );
}
