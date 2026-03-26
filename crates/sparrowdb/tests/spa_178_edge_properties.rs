//! SPA-178 — Edge properties: write + read, inline filter, projection.
//!
//! Tests:
//!  1. Basic edge property write + read: CREATE with {since:2020}, RETURN r.since
//!  2. Edge property filter (match): [r:KNOWS {since:2020}] returns matching node
//!  3. Edge property filter (no match): [r:KNOWS {since:1999}] returns nothing
//!  4. Multiple edge properties: {weight: 5, active: 1}, RETURN r.weight, r.active
//!  5. (SPA-240) Edge props survive CHECKPOINT — int property
//!  6. (SPA-240) Edge props survive CHECKPOINT — float property
//!  7. (SPA-240) Edge props survive CHECKPOINT — string property
//!  8. (SPA-240) Edge prop filter works after CHECKPOINT

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

    assert_eq!(
        result.rows.len(),
        1,
        "should match one edge with since=2020"
    );
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

    assert_eq!(result.rows.len(), 0, "no edge with since=1999 should exist");
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

// ── SPA-240 regression tests: edge props must survive CHECKPOINT ──────────────

/// Test 5 (SPA-240): integer edge property is readable after CHECKPOINT.
///
/// Before the fix, CHECKPOINT truncated the delta log which caused the
/// (src_slot, dst_slot) → edge_id map in the hop engine to be empty, so all
/// edge property reads returned null post-checkpoint.
#[test]
fn test_edge_prop_int_survives_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:P {n:1})-[:K {score:42}]->(b:P {n:2})")
        .expect("create");
    db.checkpoint().expect("checkpoint");

    let result = db
        .execute("MATCH (a:P)-[r:K]->(b:P) RETURN r.score")
        .expect("match after checkpoint");

    assert_eq!(result.rows.len(), 1, "expected one edge after checkpoint");
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(42)],
        "r.score must be 42 after checkpoint (SPA-240)"
    );
}

/// Test 6 (SPA-240): float edge property is readable after CHECKPOINT.
#[test]
fn test_edge_prop_float_survives_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:P {n:1})-[:K {rating:3.14}]->(b:P {n:2})")
        .expect("create");
    db.checkpoint().expect("checkpoint");

    let result = db
        .execute("MATCH (a:P)-[r:K]->(b:P) RETURN r.rating")
        .expect("match after checkpoint");

    assert_eq!(result.rows.len(), 1, "expected one edge after checkpoint");
    match &result.rows[0][0] {
        Value::Float64(v) => assert!(
            (*v - 3.14_f64).abs() < 1e-9,
            "r.rating must be ~3.14 after checkpoint, got {v}"
        ),
        other => panic!("expected Float64, got {other:?}"),
    }
}

/// Test 7 (SPA-240): string edge property is readable after CHECKPOINT.
#[test]
fn test_edge_prop_string_survives_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:P {n:1})-[:K {label:\"hello\"}]->(b:P {n:2})")
        .expect("create");
    db.checkpoint().expect("checkpoint");

    let result = db
        .execute("MATCH (a:P)-[r:K]->(b:P) RETURN r.label")
        .expect("match after checkpoint");

    assert_eq!(result.rows.len(), 1, "expected one edge after checkpoint");
    assert_eq!(
        result.rows[0],
        vec![Value::String("hello".to_string())],
        "r.label must be 'hello' after checkpoint (SPA-240)"
    );
}

/// Test 9: edge property WHERE clause filter — match case (perf guard regression).
///
/// Verifies that `WHERE r.since > 2019` correctly filters edges when the
/// edge variable `r` is referenced only in the WHERE clause (not in RETURN).
/// This also exercises the perf guard: needs_edge_props must be true when
/// the edge var appears in WHERE, ensuring edge_props.bin is read.
#[test]
fn test_edge_prop_where_filter_match() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:User {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:User {name:\"Bob\"})")
        .expect("create");
    db.execute("CREATE (c:User {name:\"Carol\"})-[:KNOWS {since:2018}]->(d:User {name:\"Dave\"})")
        .expect("create");

    // Only edges with since > 2019 should be returned.
    let result = db
        .execute("MATCH (a:User)-[r:KNOWS]->(b:User) WHERE r.since > 2019 RETURN b.name")
        .expect("match with WHERE edge prop");

    assert_eq!(result.rows.len(), 1, "only one KNOWS edge has since > 2019");
    assert_eq!(
        result.rows[0],
        vec![Value::String("Bob".to_string())],
        "destination should be Bob (since=2020 > 2019)"
    );
}

/// Test 10: no edge variable — edge_props.bin read must be skipped (perf guard).
///
/// Verifies that `MATCH (a:User)-[:KNOWS]->(b:User) RETURN b.name` completes
/// correctly without reading edge properties when the rel pattern has no
/// variable and no inline prop filter.  This is the regression case from #243.
#[test]
fn test_no_edge_var_no_read() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:User {name:\"Alice\"})-[:KNOWS {since:2020}]->(b:User {name:\"Bob\"})")
        .expect("create");

    // No [r] variable, no inline edge props — edge_props.bin must not be read.
    let result = db
        .execute("MATCH (a:User)-[:KNOWS]->(b:User) RETURN b.name")
        .expect("match without edge var");

    assert_eq!(result.rows.len(), 1, "should return one row");
    assert_eq!(
        result.rows[0],
        vec![Value::String("Bob".to_string())],
        "destination should be Bob"
    );
}

/// Test 8 (SPA-240): edge prop inline filter works after CHECKPOINT.
///
/// MATCH (a)-[r:K {score:42}]->(b) must return the edge; MATCH with
/// score:99 must return nothing.
#[test]
fn test_edge_prop_filter_survives_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:P {n:1})-[:K {score:42}]->(b:P {n:2})")
        .expect("create");
    db.checkpoint().expect("checkpoint");

    // Matching filter — should return one row.
    let hit = db
        .execute("MATCH (a:P)-[r:K {score:42}]->(b:P) RETURN b.n")
        .expect("match hit after checkpoint");
    assert_eq!(
        hit.rows.len(),
        1,
        "filter score=42 must match after checkpoint"
    );

    // Non-matching filter — should return no rows.
    let miss = db
        .execute("MATCH (a:P)-[r:K {score:99}]->(b:P) RETURN b.n")
        .expect("match miss after checkpoint");
    assert_eq!(
        miss.rows.len(),
        0,
        "filter score=99 must not match after checkpoint"
    );
}
