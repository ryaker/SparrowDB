//! Regression tests for SPA-263: 2-hop MATCH across different labels injects
//! spurious null rows — COUNT(*) returns null instead of aggregate.
//!
//! Root cause: the merged CSR conflated slot numbers across different labels,
//! producing Cartesian-product paths through unrelated edges.  The fix builds
//! separate per-hop CSRs filtered by relationship type and label endpoints.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Core regression: 2-hop across 3 different labels returns only valid rows.
///
/// Graph:
///   Alice -[:WORKS_FOR]-> Acme -[:LOCATED_AT]-> NYC
///   Bob   -[:WORKS_FOR]-> Acme -[:LOCATED_AT]-> NYC
///   Carol -[:WORKS_FOR]-> Beta
///
/// Query:
///   MATCH (p:Person)-[:WORKS_FOR]->(o:Organization)-[:LOCATED_AT]->(l:Location)
///   RETURN p.name, o.name, l.name
///
/// Expected: 2 rows (Alice+Acme+NYC, Bob+Acme+NYC). No null rows.
#[test]
fn two_hop_cross_label_no_null_rows() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Organization {name: 'Acme'})").unwrap();
    db.execute("CREATE (:Organization {name: 'Beta'})").unwrap();
    db.execute("CREATE (:Location {name: 'NYC'})").unwrap();

    db.execute(
        "MATCH (p:Person {name: 'Alice'}), (o:Organization {name: 'Acme'}) \
         CREATE (p)-[:WORKS_FOR]->(o)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person {name: 'Bob'}), (o:Organization {name: 'Acme'}) \
         CREATE (p)-[:WORKS_FOR]->(o)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person {name: 'Carol'}), (o:Organization {name: 'Beta'}) \
         CREATE (p)-[:WORKS_FOR]->(o)",
    )
    .unwrap();
    db.execute(
        "MATCH (o:Organization {name: 'Acme'}), (l:Location {name: 'NYC'}) \
         CREATE (o)-[:LOCATED_AT]->(l)",
    )
    .unwrap();

    // 2-hop RETURN props — should be exactly 2 valid rows, no nulls.
    let result = db
        .execute(
            "MATCH (p:Person)-[:WORKS_FOR]->(o:Organization)-[:LOCATED_AT]->(l:Location) \
             RETURN p.name, o.name, l.name",
        )
        .expect("2-hop cross-label query");

    assert_eq!(
        result.rows.len(),
        2,
        "SPA-263: expected 2 rows, got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    // No row should have a null l.name.
    for row in &result.rows {
        assert!(
            !matches!(row[2], Value::Null),
            "SPA-263: l.name must not be null, got row: {:?}",
            row
        );
    }
}

/// COUNT(*) over a 2-hop cross-label path must return a single integer row.
#[test]
fn two_hop_cross_label_count_star() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Organization {name: 'Acme'})").unwrap();
    db.execute("CREATE (:Location {name: 'NYC'})").unwrap();

    db.execute(
        "MATCH (p:Person {name: 'Alice'}), (o:Organization {name: 'Acme'}) \
         CREATE (p)-[:WORKS_FOR]->(o)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person {name: 'Bob'}), (o:Organization {name: 'Acme'}) \
         CREATE (p)-[:WORKS_FOR]->(o)",
    )
    .unwrap();
    db.execute(
        "MATCH (o:Organization {name: 'Acme'}), (l:Location {name: 'NYC'}) \
         CREATE (o)-[:LOCATED_AT]->(l)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (p:Person)-[:WORKS_FOR]->(o:Organization)-[:LOCATED_AT]->(l:Location) \
             RETURN COUNT(*) AS n",
        )
        .expect("2-hop COUNT(*)");

    assert_eq!(
        result.rows.len(),
        1,
        "SPA-263: COUNT(*) must return exactly 1 row, got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    assert_eq!(
        result.rows[0][0],
        Value::Int64(2),
        "SPA-263: COUNT(*) should be 2, got {:?}",
        result.rows[0][0]
    );
}

/// Same-label 2-hop still works correctly (regression guard).
#[test]
fn two_hop_same_label_still_works() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Charlie'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) \
         CREATE (b)-[:KNOWS]->(c)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN a.name, c.name",
        )
        .expect("same-label 2-hop");

    assert_eq!(
        result.rows.len(),
        1,
        "Same-label 2-hop should return 1 row, got {}: {:?}",
        result.rows.len(),
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::String("Charlie".to_string()));
}
