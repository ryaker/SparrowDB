//! SPA-178 — Edge properties: write + read, inline filter, projection.
//!
//! Tests:
//!  1. Basic edge property write + read: CREATE with {since:2020}, RETURN r.since
//!  2. Edge property filter (match): [r:KNOWS {since:2020}] returns matching node
//!  3. Edge property filter (no match): [r:KNOWS {since:1999}] returns nothing
//!  4. Multiple edge properties: {weight: 5, active: 1}, RETURN r.weight, r.active

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Test 1: basic edge property write + read.
///
/// CREATE (a:Person {name:"Alice"})-[:KNOWS {since:2020}]->(b:Person {name:"Bob"})
/// MATCH (a)-[r:KNOWS]->(b) RETURN r.since  →  [[Int(2020)]]
#[test]
fn test_edge_prop_basic_write_read() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")
        .expect("match");

    assert_eq!(result.columns, vec!["r.since"]);
    assert_eq!(result.rows.len(), 1, "expected one KNOWS edge");
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(2020)],
        "r.since should be 2020"
    );
}

/// Test 2: edge property inline filter — match case.
///
/// MATCH (a)-[r:KNOWS {since:2020}]->(b) RETURN b.name  →  [["Bob"]]
#[test]
fn test_edge_prop_filter_match() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS {since:2020}]->(b:Person) RETURN b.name")
        .expect("match");

    assert_eq!(result.rows.len(), 1, "should match one edge with since=2020");
    assert_eq!(
        result.rows[0],
        vec![Value::String("Bob".to_string())],
        "destination should be Bob"
    );
}

/// Test 3: edge property inline filter — no-match case.
///
/// MATCH (a)-[r:KNOWS {since:1999}]->(b) RETURN b.name  →  []
#[test]
fn test_edge_prop_filter_no_match() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS {since:1999}]->(b:Person) RETURN b.name")
        .expect("match");

    assert_eq!(
        result.rows.len(),
        0,
        "no edge with since=1999 should exist"
    );
}

/// Test 4: multiple edge properties.
///
/// CREATE (a:Item {id:1})-[:LINK {weight:5, active:1}]->(b:Item {id:2})
/// MATCH (a)-[r:LINK]->(b) RETURN r.weight, r.active  →  [[5, 1]]
#[test]
fn test_edge_prop_multiple_props() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Item {id:1})-[:LINK {weight:5, active:1}]->(b:Item {id:2})")
        .expect("create");

    let result = db
        .execute("MATCH (a:Item)-[r:LINK]->(b:Item) RETURN r.weight, r.active")
        .expect("match");

    assert_eq!(result.columns, vec!["r.weight", "r.active"]);
    assert_eq!(result.rows.len(), 1, "expected one LINK edge");
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(5), Value::Int64(1)],
        "r.weight=5, r.active=1"
    );
}
