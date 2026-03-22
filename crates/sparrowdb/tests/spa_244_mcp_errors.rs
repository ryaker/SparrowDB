//! Regression tests for SPA-244: MCP layer must propagate real error messages.
//!
//! Previously, the MCP `execute_cypher` tool caught `Error::Unimplemented` and
//! returned a success-shaped content response with the generic text "Cypher
//! execution not yet implemented", hiding the real cause from callers.
//!
//! These tests verify that:
//! 1. `GraphDb::execute` surfaces meaningful error messages for bad queries.
//! 2. The MCP JSON-RPC server returns a proper `error` field (not a `result`)
//!    when `execute_cypher` fails, and that the error message contains useful
//!    diagnostic text — not an empty string or a generic placeholder.

use sparrowdb::open;

// ── Helper ────────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Unit-level: GraphDb::execute error propagation ────────────────────────────

/// An empty Cypher string must return an `Err` whose message is non-empty
/// and contains meaningful diagnostic text (e.g., "empty input").
#[test]
fn spa244_empty_query_returns_meaningful_error() {
    let (_dir, db) = make_db();

    let err = db.execute("").expect_err("empty query must fail");
    let msg = err.to_string();

    assert!(
        !msg.is_empty(),
        "error message must not be empty for an empty query"
    );
    assert!(
        msg.contains("empty") || msg.contains("invalid") || msg.contains("parse"),
        "error message '{msg}' should describe why the empty query is invalid"
    );
}

/// A syntactically invalid Cypher string must return an `Err` with a message
/// that is non-empty and contains actionable diagnostic text.
#[test]
fn spa244_syntax_error_returns_meaningful_error() {
    let (_dir, db) = make_db();

    let err = db
        .execute("THIS IS NOT CYPHER @@@@")
        .expect_err("invalid Cypher must fail");
    let msg = err.to_string();

    assert!(
        !msg.is_empty(),
        "error message must not be empty for a syntax-error query"
    );
    // Must NOT be the old generic swallowed message.
    assert_ne!(
        msg, "Cypher execution not yet implemented",
        "must not return the old swallowed generic message"
    );
}

/// `Error::Unimplemented` must propagate with its real display string
/// "not yet implemented", not be hidden behind a generic success response.
///
/// We exercise this via a MATCH…CREATE with an incoming edge direction, which
/// the execution engine does not yet support.
#[test]
fn spa244_unimplemented_feature_returns_real_error() {
    let (_dir, db) = make_db();

    // Seed a node so the MATCH has something to bind to.
    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();

    // MATCH…CREATE with an *incoming* direction is `Error::Unimplemented`.
    let result =
        db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)<-[:KNOWS]-(b)");

    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.is_empty(),
                "Unimplemented error message must not be empty"
            );
            // The real message is "not yet implemented" — it must reach the caller.
            assert!(
                msg.contains("not yet implemented") || msg.contains("unimplemented"),
                "expected 'not yet implemented' in error message, got: '{msg}'"
            );
            // Must NOT be the old swallowed generic message that was emitted as
            // a success content response.
            assert_ne!(
                msg, "Cypher execution not yet implemented",
                "old swallowed message must never reach callers"
            );
        }
        Ok(_) => {
            // The operation may have been implemented since this test was written.
            // That is fine — the test only guards against silent error swallowing.
        }
    }
}

// ── Integration: MCP JSON-RPC subprocess ─────────────────────────────────────

/// Spawn the `sparrowdb-mcp` binary, send a `tools/call` for `execute_cypher`
/// with an invalid query, and verify the JSON-RPC response has an `error`
/// field (not a `result`) whose `message` is non-empty and meaningful.
///
/// This test is skipped (passes vacuously) if the binary is not yet built,
/// so it is safe to run in CI before the binary artifact is present.
#[test]
fn spa244_mcp_binary_propagates_real_error() {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    // Find the binary in the workspace target directory.
    let binary = {
        // Try debug build first (fast), then release.
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
            debug_bin
        } else if release_bin.exists() {
            release_bin
        } else {
            // Binary not built yet — skip gracefully.
            eprintln!("sparrowdb-mcp binary not found; skipping MCP subprocess test");
            return;
        }
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().to_str().expect("utf8 path");

    // We need the DB directory to exist and be a valid SparrowDB so that the
    // MCP server can open it. Create it first via GraphDb::open.
    {
        let _db = open(dir.path()).expect("pre-create db");
    }

    // Build a minimal JSON-RPC session: initialize + tools/call.
    let initialize = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0"}
        }
    });
    let tool_call = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "execute_cypher",
            "arguments": {
                "db_path": db_path,
                "query": "THIS IS NOT VALID CYPHER @@@@"
            }
        }
    });

    let input = format!(
        "{}\n{}\n",
        serde_json::to_string(&initialize).unwrap(),
        serde_json::to_string(&tool_call).unwrap()
    );

    let mut child = Command::new(&binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn sparrowdb-mcp");

    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");

    let output = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse response lines (one JSON object per line).
    let mut tool_call_response: Option<serde_json::Value> = None;
    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v["id"] == serde_json::json!(2) {
            tool_call_response = Some(v);
            break;
        }
    }

    let response = tool_call_response.expect("did not receive a response for id=2");

    // The response MUST have an `error` field, not a `result`, because the
    // query is invalid Cypher.
    assert!(
        !response["error"].is_null(),
        "MCP response for invalid Cypher must contain an `error` field, got: {response}"
    );
    assert!(
        response["result"].is_null(),
        "MCP response for invalid Cypher must NOT contain a `result` field, got: {response}"
    );

    // The error message must be non-empty and must NOT be the old generic text.
    let error_msg = response["error"]["message"]
        .as_str()
        .expect("error.message must be a string");

    assert!(
        !error_msg.is_empty(),
        "error.message must not be empty, got: {response}"
    );
    assert_ne!(
        error_msg, "Cypher execution not yet implemented",
        "must not return old swallowed generic message"
    );
    assert_ne!(
        error_msg, "something went wrong",
        "must not return another generic placeholder"
    );
}
