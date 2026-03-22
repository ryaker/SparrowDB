//! Integration tests for MERGE…RETURN support (SPA-215).
//!
//! Verifies that an optional RETURN clause after MERGE correctly projects
//! properties of the merged (created or matched) node.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// First call: node does not exist — MERGE creates it and RETURN projects name.
#[test]
fn merge_return_creates_node_and_returns_name() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MERGE (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("MERGE...RETURN must not fail");

    assert_eq!(result.columns, vec!["n.name"]);
    assert_eq!(result.rows.len(), 1, "should return exactly one row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "projected name should be Alice"
    );
}

/// Second call: node already exists — MERGE is an upsert, RETURN still works.
#[test]
fn merge_return_upsert_returns_name() {
    let (_dir, db) = make_db();

    // First MERGE — creates the node.
    db.execute("MERGE (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("first MERGE must succeed");

    // Second MERGE — node exists, should match and still project the name.
    let result = db
        .execute("MERGE (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("second MERGE...RETURN must not fail");

    assert_eq!(result.columns, vec!["n.name"]);
    assert_eq!(result.rows.len(), 1, "upsert should return exactly one row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "projected name should be Alice after upsert"
    );
}

/// MERGE without RETURN should still work (backward-compatibility).
#[test]
fn merge_without_return_still_works() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MERGE (n:Person {name: 'Bob'})")
        .expect("MERGE without RETURN must not fail");

    assert_eq!(result.rows.len(), 0, "no RETURN means empty result set");
}
