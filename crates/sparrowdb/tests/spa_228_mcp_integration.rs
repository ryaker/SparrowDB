//! Integration tests for SPA-228: Claude Desktop MCP server end-to-end verification.
//!
//! Verifies the `sparrowdb-mcp` binary works correctly as a JSON-RPC 2.0
//! MCP server over stdio:
//!
//! a. `tools/list` returns all expected tools (execute_cypher, checkpoint,
//!    info, create_entity, add_property).
//! b. `execute_cypher` with a valid CREATE then MATCH returns correct rows.
//! c. `execute_cypher` with invalid Cypher returns a JSON-RPC error with a
//!    real, non-generic message.
//! d. `create_entity` creates a node that is retrievable via execute_cypher.
//! e. `add_property` updates a node's property, verified via execute_cypher.
//!
//! All subprocess tests skip gracefully when the binary has not been built,
//! making them safe to run in CI before the artifact is present.

use serde_json::{json, Value};
use std::io::Write as _;
use std::process::{Command, Stdio};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Locate the `sparrowdb-mcp` binary.  Returns `None` when not built yet.
fn find_binary() -> Option<std::path::PathBuf> {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent() // crates/sparrowdb
        .and_then(|p| p.parent()) // crates
        .and_then(|p| p.parent()) // workspace root
        .expect("could not locate workspace root");

    let debug_bin = workspace_root
        .join("target")
        .join("debug")
        .join("sparrowdb-mcp");
    let release_bin = workspace_root
        .join("target")
        .join("release")
        .join("sparrowdb-mcp");

    if debug_bin.exists() {
        Some(debug_bin)
    } else if release_bin.exists() {
        Some(release_bin)
    } else {
        None
    }
}

/// Create an empty SparrowDB at a temp directory so the MCP server can open it.
fn make_temp_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().to_str().expect("utf8 path").to_owned();
    {
        let _db = sparrowdb::open(dir.path()).expect("pre-create db");
    }
    (dir, db_path)
}

/// Prepare a JSON-RPC session payload: an `initialize` handshake followed by
/// caller-supplied request lines.  Returns the concatenated newline-delimited
/// bytes ready for stdin.
fn build_session(requests: &[Value]) -> Vec<u8> {
    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "spa228-test", "version": "0"}
        }
    });

    let mut buf = String::new();
    buf.push_str(&serde_json::to_string(&initialize).unwrap());
    buf.push('\n');
    for req in requests {
        buf.push_str(&serde_json::to_string(req).unwrap());
        buf.push('\n');
    }
    buf.into_bytes()
}

/// Spawn the binary, feed `input` to stdin, collect stdout lines, and return
/// them parsed as JSON values indexed by their `id` field.
fn run_session(binary: &std::path::Path, input: Vec<u8>) -> std::collections::HashMap<i64, Value> {
    let mut child = Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn sparrowdb-mcp");

    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(&input)
        .expect("write stdin");

    let output = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut map = std::collections::HashMap::new();
    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<Value>(line) {
            if let Some(id) = v["id"].as_i64() {
                map.insert(id, v);
            }
        }
    }
    map
}

// ── Test a: tools/list ────────────────────────────────────────────────────────

/// `tools/list` must return all five expected tools.
#[test]
fn spa228_tools_list_contains_all_expected_tools() {
    let binary = match find_binary() {
        Some(b) => b,
        None => {
            eprintln!("sparrowdb-mcp binary not found; skipping spa228 tools/list test");
            return;
        }
    };

    let list_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": null
    });

    let input = build_session(&[list_req]);
    let responses = run_session(&binary, input);

    let resp = responses
        .get(&1)
        .expect("did not receive a response for id=1 (tools/list)");

    assert!(
        resp["error"].is_null(),
        "tools/list must not return an error; got: {resp}"
    );
    assert!(
        !resp["result"].is_null(),
        "tools/list must return a result; got: {resp}"
    );

    let tools = resp["result"]["tools"]
        .as_array()
        .expect("tools/list result.tools must be an array");

    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    let expected = [
        "execute_cypher",
        "checkpoint",
        "info",
        "create_entity",
        "add_property",
    ];

    for name in &expected {
        assert!(
            tool_names.contains(name),
            "tools/list missing expected tool '{name}'; got: {tool_names:?}"
        );
    }

    assert_eq!(
        tools.len(),
        expected.len(),
        "tools/list returned {} tools, expected {}; got: {tool_names:?}",
        tools.len(),
        expected.len()
    );
}

// ── Test b: execute_cypher CREATE then MATCH ──────────────────────────────────

/// A valid CREATE followed by a MATCH must return the created node's property.
#[test]
fn spa228_execute_cypher_create_then_match_returns_correct_rows() {
    let binary = match find_binary() {
        Some(b) => b,
        None => {
            eprintln!("sparrowdb-mcp binary not found; skipping spa228 create+match test");
            return;
        }
    };

    let (_dir, db_path) = make_temp_db();

    let create_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "CREATE (n:Person {name: 'SPA228'})"
            }
        }
    });
    let match_req = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "MATCH (n:Person) WHERE n.name = 'SPA228' RETURN n.name"
            }
        }
    });

    let input = build_session(&[create_req, match_req]);
    let responses = run_session(&binary, input);

    // CREATE must succeed (result, not error).
    let create_resp = responses.get(&1).expect("no response for id=1 (CREATE)");
    assert!(
        create_resp["error"].is_null(),
        "CREATE must not return a JSON-RPC error; got: {create_resp}"
    );

    // MATCH must succeed and the content text must mention the node's name.
    let match_resp = responses.get(&2).expect("no response for id=2 (MATCH)");
    assert!(
        match_resp["error"].is_null(),
        "MATCH must not return a JSON-RPC error; got: {match_resp}"
    );

    let content_text = match_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("result.content[0].text must be a string");

    assert!(
        content_text.contains("SPA228"),
        "MATCH response must contain the created node's name 'SPA228'; got: '{content_text}'"
    );
}

// ── Test c: execute_cypher invalid Cypher returns real error ──────────────────

/// An invalid Cypher query must produce a JSON-RPC `error` field with a
/// non-empty, non-generic message — not a success-shaped response.
#[test]
fn spa228_execute_cypher_invalid_query_returns_real_error() {
    let binary = match find_binary() {
        Some(b) => b,
        None => {
            eprintln!("sparrowdb-mcp binary not found; skipping spa228 invalid-cypher test");
            return;
        }
    };

    let (_dir, db_path) = make_temp_db();

    let bad_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "THIS IS NOT VALID CYPHER @@@@"
            }
        }
    });

    let input = build_session(&[bad_req]);
    let responses = run_session(&binary, input);

    let resp = responses
        .get(&1)
        .expect("no response for id=1 (invalid Cypher)");

    // Must have an error, not a result.
    assert!(
        !resp["error"].is_null(),
        "invalid Cypher must return a JSON-RPC error field; got: {resp}"
    );
    assert!(
        resp["result"].is_null(),
        "invalid Cypher must NOT return a result field; got: {resp}"
    );

    let error_msg = resp["error"]["message"]
        .as_str()
        .expect("error.message must be a string");

    // Message must be non-empty.
    assert!(
        !error_msg.is_empty(),
        "error.message must not be empty for invalid Cypher"
    );

    // Must not be an old generic swallowed placeholder.
    assert_ne!(
        error_msg, "Cypher execution not yet implemented",
        "error.message must not be the old swallowed generic message"
    );
    assert_ne!(
        error_msg, "something went wrong",
        "error.message must not be a generic placeholder"
    );
}

// ── Test d: create_entity creates a retrievable node ─────────────────────────

/// `create_entity` with a class name and properties must create a node that
/// is subsequently retrievable via `execute_cypher` MATCH.
#[test]
fn spa228_create_entity_creates_retrievable_node() {
    let binary = match find_binary() {
        Some(b) => b,
        None => {
            eprintln!("sparrowdb-mcp binary not found; skipping spa228 create_entity test");
            return;
        }
    };

    let (_dir, db_path) = make_temp_db();

    let create_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "create_entity",
            "arguments": {
                "db_path": db_path,
                "class_name": "Company",
                "properties": {
                    "name": "SparrowCo",
                    "founded": 2024
                }
            }
        }
    });
    let match_req = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "MATCH (n:Company {name: 'SparrowCo'}) RETURN n.name"
            }
        }
    });

    let input = build_session(&[create_req, match_req]);
    let responses = run_session(&binary, input);

    // create_entity must succeed.
    let create_resp = responses
        .get(&1)
        .expect("no response for id=1 (create_entity)");
    assert!(
        create_resp["error"].is_null(),
        "create_entity must not return a JSON-RPC error; got: {create_resp}"
    );

    let create_text = create_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("create_entity result.content[0].text must be a string");
    assert!(
        create_text.contains("Company") || create_text.contains("created"),
        "create_entity success message should mention the class or 'created'; got: '{create_text}'"
    );

    // MATCH must find the entity created above.
    let match_resp = responses
        .get(&2)
        .expect("no response for id=2 (MATCH after create_entity)");
    assert!(
        match_resp["error"].is_null(),
        "MATCH after create_entity must not return an error; got: {match_resp}"
    );

    let match_text = match_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("MATCH result.content[0].text must be a string");
    assert!(
        match_text.contains("SparrowCo"),
        "MATCH must find the node created by create_entity; got: '{match_text}'"
    );
}

// ── Test e: add_property updates a node's property ───────────────────────────

/// `add_property` must update a property on an existing node; a subsequent
/// `execute_cypher` MATCH must observe the new value.
#[test]
fn spa228_add_property_updates_node_property() {
    let binary = match find_binary() {
        Some(b) => b,
        None => {
            eprintln!("sparrowdb-mcp binary not found; skipping spa228 add_property test");
            return;
        }
    };

    let (_dir, db_path) = make_temp_db();

    // Step 1: create the node.
    let create_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "CREATE (n:User {id: 'u1', name: 'TestUser'})"
            }
        }
    });

    // Step 2: add a property via add_property.
    let add_prop_req = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "add_property",
            "arguments": {
                "db_path": db_path,
                "label": "User",
                "match_prop": "id",
                "match_val": "u1",
                "set_prop": "email",
                "set_val": "testuser@example.com"
            }
        }
    });

    // Step 3: verify the property is now present.
    let verify_req = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "MATCH (n:User {id: 'u1'}) RETURN n.email"
            }
        }
    });

    let input = build_session(&[create_req, add_prop_req, verify_req]);
    let responses = run_session(&binary, input);

    // CREATE must succeed.
    let create_resp = responses
        .get(&1)
        .expect("no response for id=1 (CREATE User)");
    assert!(
        create_resp["error"].is_null(),
        "CREATE User must not return an error; got: {create_resp}"
    );

    // add_property must succeed and report ≥1 updated node.
    let add_resp = responses
        .get(&2)
        .expect("no response for id=2 (add_property)");
    assert!(
        add_resp["error"].is_null(),
        "add_property must not return a JSON-RPC error; got: {add_resp}"
    );

    // The updated count in the result must be ≥ 1.
    let updated = add_resp["result"]["updated"].as_i64().unwrap_or(0);
    assert!(
        updated >= 1,
        "add_property must report at least 1 updated node; got updated={updated}, resp: {add_resp}"
    );

    // Verify the email property is readable.
    let verify_resp = responses
        .get(&3)
        .expect("no response for id=3 (verify email)");
    assert!(
        verify_resp["error"].is_null(),
        "MATCH after add_property must not return an error; got: {verify_resp}"
    );

    let verify_text = verify_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("verify result.content[0].text must be a string");
    assert!(
        verify_text.contains("testuser@example.com"),
        "email property must be readable after add_property; got: '{verify_text}'"
    );
}
