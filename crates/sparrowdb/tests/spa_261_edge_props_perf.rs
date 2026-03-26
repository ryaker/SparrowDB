//! SPA-261 — Edge property cache: skip edge_props.bin re-read across queries.
//!
//! Tests:
//!  1. Queries WITHOUT edge prop references work correctly (no edge_props I/O).
//!  2. Queries WITH edge prop references return correct values.
//!  3. Running the same edge-prop query twice returns correct results (cache hit path).
//!  4. Edge prop cache is invalidated after a write — new data is visible.

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Test 1: hop query without edge prop references returns correct results
/// and does not need to read edge_props.bin.
#[test]
fn test_hop_without_edge_props() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create");

    // Query that does NOT reference any edge property — should skip edge_props.bin.
    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
        .expect("match");

    assert_eq!(result.columns, vec!["b.name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![Value::String("Bob".to_string())]);
}

/// Test 2: hop query WITH edge prop references returns correct values.
#[test]
fn test_hop_with_edge_props() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020, weight:5}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since, r.weight")
        .expect("match");

    assert_eq!(result.columns, vec!["r.since", "r.weight"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![Value::Int64(2020), Value::Int64(5)]);
}

/// Test 3: running the same edge-prop query twice hits the cache on the
/// second call and still returns correct results.
#[test]
fn test_edge_props_cache_hit() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create");

    let query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since";

    // First query: populates cache.
    let r1 = db.execute(query).expect("first query");
    assert_eq!(r1.rows.len(), 1);
    assert_eq!(r1.rows[0], vec![Value::Int64(2020)]);

    // Second query: should hit cache and still return correct data.
    let r2 = db.execute(query).expect("second query");
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(r2.rows[0], vec![Value::Int64(2020)]);
}

/// Test 4: cache is invalidated after a write — new edge props are visible.
#[test]
fn test_edge_props_cache_invalidation() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:Person {name:\"Bob\"})",
    )
    .expect("create first edge");

    // Prime the cache.
    let r1 = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")
        .expect("first query");
    assert_eq!(r1.rows.len(), 1);

    // Write a second edge with different properties.
    db.execute(
        "CREATE (c:Person {name:\"Carol\"})-[:KNOWS {since:2024}]->(d:Person {name:\"Dave\"})",
    )
    .expect("create second edge");

    // After the write, the cache should be invalidated and show both edges.
    let r2 = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since ORDER BY r.since")
        .expect("second query");
    assert_eq!(r2.rows.len(), 2, "should see both edges after write");
    assert_eq!(r2.rows[0], vec![Value::Int64(2020)]);
    assert_eq!(r2.rows[1], vec![Value::Int64(2024)]);
}
