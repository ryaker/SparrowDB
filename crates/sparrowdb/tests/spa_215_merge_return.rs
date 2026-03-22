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

/// Bug regression (CodeAnt / SPA-215): when a node already exists with extra
/// properties that are NOT part of the MERGE pattern, RETURN must reflect the
/// actual on-disk node state rather than only the pattern props.
#[test]
fn merge_return_reflects_extra_stored_properties() {
    let (_dir, db) = make_db();

    // Step 1: create the node via MERGE.
    db.execute("MERGE (n:Person {name: 'Alice'})")
        .expect("initial MERGE must succeed");

    // Step 2: add an extra property via MATCH … SET that is NOT in the merge pattern.
    db.execute("MATCH (n:Person) SET n.score = 99")
        .expect("MATCH SET must succeed");

    // Step 3: MERGE on the same pattern (finds existing node), RETURN both props.
    // Without the fix, n.score would be Null because the old code only projected
    // from the input pattern props (which only contained `name`).
    let result = db
        .execute("MERGE (n:Person {name: 'Alice'}) RETURN n.name, n.score")
        .expect("MERGE...RETURN with extra props must not fail");

    assert_eq!(result.columns, vec!["n.name", "n.score"]);
    assert_eq!(result.rows.len(), 1, "should return exactly one row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "n.name must be Alice"
    );
    assert_eq!(
        result.rows[0][1],
        Value::Int64(99),
        "n.score must reflect the on-disk value set via MATCH SET, not Null"
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
