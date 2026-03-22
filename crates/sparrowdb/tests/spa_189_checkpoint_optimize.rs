//! Integration tests for SPA-189: CHECKPOINT and OPTIMIZE Cypher commands
//! are wired to the real maintenance engine, and SPA-218: cypher_escape_string
//! utility and parameterized query handling of special characters.

use sparrowdb::{cypher_escape_string, GraphDb};

// ── SPA-189: CHECKPOINT ───────────────────────────────────────────────────────

/// Executing `CHECKPOINT` via the Cypher API must not return an error,
/// even on an empty database.
#[test]
fn checkpoint_command_runs_without_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    // Empty DB: checkpoint should succeed.
    db.execute("CHECKPOINT")
        .expect("CHECKPOINT on empty DB must not error");
}

/// Executing `CHECKPOINT` after writing data should fold the delta log and
/// return successfully.
#[test]
fn checkpoint_command_runs_after_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("create Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("create Bob");

    db.execute("CHECKPOINT")
        .expect("CHECKPOINT after writes must not error");

    // Data must still be readable after a checkpoint.
    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH after CHECKPOINT");
    assert_eq!(result.rows.len(), 2, "both nodes must survive CHECKPOINT");
}

// ── SPA-189: OPTIMIZE ─────────────────────────────────────────────────────────

/// Executing `OPTIMIZE` via the Cypher API must not return an error,
/// even on an empty database.
#[test]
fn optimize_command_runs_without_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("OPTIMIZE")
        .expect("OPTIMIZE on empty DB must not error");
}

/// Executing `OPTIMIZE` after writing data should compact and sort neighbor
/// lists and return successfully.
#[test]
fn optimize_command_runs_after_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("create Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("create Bob");

    db.execute("OPTIMIZE")
        .expect("OPTIMIZE after writes must not error");

    // Data must still be readable after an optimize.
    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH after OPTIMIZE");
    assert_eq!(result.rows.len(), 2, "both nodes must survive OPTIMIZE");
}

// ── SPA-218: cypher_escape_string ─────────────────────────────────────────────

/// Single quotes in a string are escaped to `\'`.
#[test]
fn escape_single_quote() {
    assert_eq!(cypher_escape_string("O'Reilly"), "O\\'Reilly");
}

/// Backslashes in a string are escaped to `\\`.
#[test]
fn escape_backslash() {
    assert_eq!(cypher_escape_string("C:\\Users"), "C:\\\\Users");
}

/// A string with both backslash and single quote is handled in the correct
/// order (backslash first, then apostrophe) so that inserted backslashes are
/// not double-escaped.
#[test]
fn escape_backslash_and_quote() {
    // Input: foo\'bar  (backslash then apostrophe)
    // Expected: foo\\\\'bar  i.e. \\ (escaped backslash) + \' (escaped quote)
    let input = "foo\\'bar";
    let escaped = cypher_escape_string(input);
    assert_eq!(escaped, "foo\\\\\\'bar");
}

/// A string with no special characters is returned unchanged.
#[test]
fn escape_no_special_chars() {
    assert_eq!(cypher_escape_string("hello world"), "hello world");
}

/// An empty string is returned as an empty string.
#[test]
fn escape_empty_string() {
    assert_eq!(cypher_escape_string(""), "");
}

// ── SPA-218: parameterized query handles apostrophes ─────────────────────────

/// A parameterized query (`$name`) correctly stores and retrieves a node whose
/// `name` property contains a single quote (apostrophe).
#[test]
fn parameterized_query_handles_apostrophe() {
    use sparrowdb_execution::types::Value;
    use std::collections::HashMap;

    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    // Insert via direct Cypher — use cypher_escape_string for safe interpolation.
    let name = "O'Reilly";
    let escaped = cypher_escape_string(name);
    let create_cypher = format!("CREATE (n:Author {{name: '{escaped}'}})");
    db.execute(&create_cypher)
        .expect("CREATE with escaped apostrophe");

    // Query back using a parameterized query so the apostrophe never touches
    // the Cypher string interpolation path.
    let mut params = HashMap::new();
    params.insert("name".to_string(), Value::String("O'Reilly".to_string()));

    let result = db
        .execute_with_params("MATCH (n:Author {name: $name}) RETURN n.name", params)
        .expect("MATCH with $name param containing apostrophe");

    assert_eq!(result.rows.len(), 1, "must find exactly one Author node");
    let row = &result.rows[0];
    let returned_name = match &row[0] {
        Value::String(s) => s.clone(),
        other => panic!("expected String, got {other:?}"),
    };
    assert_eq!(returned_name, "O'Reilly");
}
