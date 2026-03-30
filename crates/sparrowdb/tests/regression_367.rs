//! Regression test for issue #367:
//! Relationship property values return null on MATCH traversal.
//!
//! `MATCH (a)-[r:KNOWS {weight: 0.9}]->(b) RETURN r.weight` returned Null
//! instead of 0.9.  Root cause: inline edge property filter combined with
//! property projection did not correctly pass edge_props to the projection
//! function when `needs_edge_props` relied solely on the return-column-name
//! scan rather than the AST expression.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use tempfile::tempdir;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    (dir, db)
}

/// Test 1: inline filter on rel prop AND project that same prop.
///
/// `MATCH (a)-[r:KNOWS {weight: 0.9}]->(b) RETURN r.weight` must return 0.9.
#[test]
fn rel_prop_filter_and_project_returns_value() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name: 'alice'})-[:KNOWS {weight: 0.9}]->(b:Person {name: 'bob'})",
    )
    .unwrap();

    let r = db
        .execute("MATCH (a:Person)-[r:KNOWS {weight: 0.9}]->(b:Person) RETURN r.weight")
        .unwrap();

    assert_eq!(r.rows.len(), 1, "should match exactly one edge");
    match &r.rows[0][0] {
        Value::Float64(f) => {
            assert!(
                (f - 0.9_f64).abs() < 1e-10,
                "r.weight should be 0.9, got {f}"
            );
        }
        other => panic!("Expected Float64(0.9) for r.weight, got {:?}", other),
    }
}

/// Test 2: inline filter must EXCLUDE non-matching edges.
///
/// `MATCH (a)-[r:KNOWS {weight: 0.9}]->(b) RETURN r.weight` must return
/// no rows when the stored weight is 0.5.
#[test]
fn rel_prop_filter_excludes_non_matching() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name: 'alice'})-[:KNOWS {weight: 0.5}]->(b:Person {name: 'bob'})",
    )
    .unwrap();

    let r = db
        .execute("MATCH (a:Person)-[r:KNOWS {weight: 0.9}]->(b:Person) RETURN r.weight")
        .unwrap();

    assert_eq!(r.rows.len(), 0, "should NOT match edge with weight=0.5");
}

/// Test 3: integer inline filter on rel prop plus projection.
///
/// `MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN r.since` must return 2020,
/// and a filter for `since: 1999` must return no rows.
#[test]
fn rel_prop_int_filter_and_project() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE (a:Person {name: 'alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'bob'})",
    )
    .unwrap();

    // Matching filter
    let hit = db
        .execute("MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) RETURN r.since")
        .unwrap();

    assert_eq!(hit.rows.len(), 1, "should match edge with since=2020");
    assert_eq!(hit.rows[0][0], Value::Int64(2020), "r.since should be 2020");

    // Non-matching filter
    let miss = db
        .execute("MATCH (a:Person)-[r:KNOWS {since: 1999}]->(b:Person) RETURN r.since")
        .unwrap();

    assert_eq!(miss.rows.len(), 0, "should NOT match edge with since=1999");
}

/// Test 4: rel prop projection after checkpoint (post-SPA-240 scenario).
///
/// Edge properties must survive CHECKPOINT and inline filters must still
/// correctly exclude non-matching edges.
#[test]
fn rel_prop_filter_and_project_after_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Node {id: 1})-[:LINK {score: 42}]->(b:Node {id: 2})")
        .unwrap();
    db.checkpoint().unwrap();

    // Matching filter + projection
    let hit = db
        .execute("MATCH (a:Node)-[r:LINK {score: 42}]->(b:Node) RETURN r.score")
        .unwrap();
    assert_eq!(hit.rows.len(), 1, "should match after checkpoint");
    assert_eq!(hit.rows[0][0], Value::Int64(42), "r.score should be 42");

    // Non-matching filter must return nothing
    let miss = db
        .execute("MATCH (a:Node)-[r:LINK {score: 99}]->(b:Node) RETURN r.score")
        .unwrap();
    assert_eq!(
        miss.rows.len(),
        0,
        "non-matching filter must return 0 rows after checkpoint"
    );
}
