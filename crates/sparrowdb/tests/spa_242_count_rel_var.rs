//! Regression tests for SPA-242: COUNT(r) on a relationship variable returns 0.
//!
//! When `MATCH (a)-[r:REL]->(b)` is used with `COUNT(r)`, the relationship
//! variable `r` must be bound to a non-null value in the row so the aggregation
//! correctly counts matched edges instead of returning 0.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// `COUNT(r)` must equal the number of matched relationship edges.
#[test]
fn count_rel_var_returns_edge_count() {
    let (_dir, db) = make_db();

    // Create 3 Person nodes
    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Carol'})").unwrap();

    // Create 2 KNOWS relationships: Alice→Bob, Alice→Carol
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(c)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS cnt")
        .expect("COUNT(r) on relationship variable must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(r) must return exactly one aggregated row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(2),
        "COUNT(r) must equal the number of matched edges (2), got {:?}",
        result.rows[0][0]
    );
    assert_eq!(result.columns[0], "cnt", "alias 'cnt' must be preserved");
}

/// `COUNT(r)` must still work when a WHERE filter reduces the matched rows.
#[test]
fn count_rel_var_with_filter() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (b:Person {name: 'Bob', age: 20})")
        .unwrap();
    db.execute("CREATE (c:Person {name: 'Carol', age: 25})")
        .unwrap();

    // Alice→Bob, Alice→Carol
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(c)",
    )
    .unwrap();

    // Only count relationships where the destination age > 22
    // Carol (25) qualifies; Bob (20) does not.
    let result = db
        .execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             WHERE b.age > 22 \
             RETURN count(r) AS cnt",
        )
        .expect("COUNT(r) with WHERE filter must not error");

    assert_eq!(result.rows.len(), 1, "must return one aggregated row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(1),
        "only 1 edge survives the WHERE filter (Carol, age 25), got {:?}",
        result.rows[0][0]
    );
}

/// `COUNT(r)` must return 0 when no edges survive the WHERE predicate.
///
/// We create KNOWS edges but apply a WHERE that eliminates all of them,
/// so the aggregation sees no rows and must return 0 (not an error).
#[test]
fn count_rel_var_zero_when_no_edges_match_filter() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (b:Person {name: 'Bob', age: 20})")
        .unwrap();

    // Create one KNOWS edge: Alice→Bob
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // WHERE age > 50 eliminates all edges — count must be 0.
    let result = db
        .execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             WHERE b.age > 50 \
             RETURN count(r) AS cnt",
        )
        .expect("COUNT(r) with all-eliminating WHERE must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "must return one aggregated row (the zero-row case)"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(0),
        "COUNT(r) with no surviving edges must return 0, got {:?}",
        result.rows[0][0]
    );
}
