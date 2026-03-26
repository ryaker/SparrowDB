//! Regression tests for SPA-263: 2-hop MATCH with aggregates (COUNT, etc.)
//! returns null instead of proper aggregate values.
//!
//! The root cause: `project_three_var_row` returns `Value::Null` for aggregate
//! columns like `COUNT(*) AS n` because they don't match any `var.prop` pattern.
//! The fix makes the 2-hop path use `build_row_vals` + `aggregate_rows_graph`
//! when aggregates are present (same approach as the 1-hop path).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Same-label 2-hop COUNT(*) must return a single integer row, not null rows.
///
/// Graph:
///   Alice -[:KNOWS]-> Bob -[:WORKS_AT]-> Acme
///   Carol -[:KNOWS]-> Dave -[:WORKS_AT]-> BigCo
#[test]
fn two_hop_same_rel_count_star() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Person {name: 'Dave'})").unwrap();
    db.execute("CREATE (:Company {name: 'Acme'})").unwrap();
    db.execute("CREATE (:Company {name: 'BigCo'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) \
         CREATE (b)-[:WORKS_AT]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Carol'}), (b:Person {name: 'Dave'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Dave'}), (c:Company {name: 'BigCo'}) \
         CREATE (b)-[:WORKS_AT]->(c)",
    )
    .unwrap();

    // This should return [[Int64(2)]], NOT [[Null], [Null]]
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) \
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

/// Same-label 2-hop with COUNT(var) on a specific variable.
#[test]
fn two_hop_count_variable() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Company {name: 'Acme'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) \
         CREATE (b)-[:WORKS_AT]->(c)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) \
             RETURN COUNT(c) AS company_count",
        )
        .expect("2-hop COUNT(c)");

    assert_eq!(
        result.rows.len(),
        1,
        "SPA-263: COUNT(c) must return exactly 1 row, got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    assert_eq!(
        result.rows[0][0],
        Value::Int64(1),
        "SPA-263: COUNT(c) should be 1, got {:?}",
        result.rows[0][0]
    );
}

/// Non-aggregate 2-hop queries must still work correctly after the fix.
#[test]
fn two_hop_non_aggregate_still_works() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Company {name: 'Acme'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) \
         CREATE (b)-[:WORKS_AT]->(c)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) \
             RETURN a.name, b.name, c.name",
        )
        .expect("2-hop non-aggregate");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::String("Bob".to_string()));
    assert_eq!(result.rows[0][2], Value::String("Acme".to_string()));
}
