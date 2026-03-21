//! E2E tests for grouped aggregation: COUNT(*), COUNT, SUM, AVG, MIN, MAX.
//!
//! All tests use a real tempdir-backed GraphDb with no mocks.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: count_star ────────────────────────────────────────────────────────

/// `MATCH (n:Person) RETURN COUNT(*) AS total` → total node count.
#[test]
fn count_star() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})").expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Charlie'})").expect("CREATE Charlie");

    let result = db
        .execute("MATCH (n:Person) RETURN COUNT(*) AS total")
        .expect("COUNT(*) must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(*) with no grouping key must return 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "COUNT(*) must return 3; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 2: count_grouped ─────────────────────────────────────────────────────

/// Per-person friend count via `MATCH (n:Person)-[:KNOWS]->(f) RETURN n.name, COUNT(f) AS cnt`.
#[test]
fn count_grouped() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})").expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol'})").expect("CREATE Carol");

    // Alice knows Bob and Carol; Bob knows Carol.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("Alice->Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .expect("Alice->Carol");
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .expect("Bob->Carol");

    let result = db
        .execute("MATCH (n:Person)-[:KNOWS]->(f:Person) RETURN n.name, COUNT(f.name) AS cnt")
        .expect("COUNT grouped must succeed");

    // Alice: 2 friends, Bob: 1 friend.
    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 groups (Alice, Bob); got: {:?}",
        result.rows
    );

    // Sort rows by name for deterministic assertion.
    let mut rows = result.rows.clone();
    rows.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });

    // Alice has 2 friends.
    assert_eq!(rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(rows[0][1], Value::Int64(2), "Alice should have 2 friends");

    // Bob has 1 friend.
    assert_eq!(rows[1][0], Value::String("Bob".to_string()));
    assert_eq!(rows[1][1], Value::Int64(1), "Bob should have 1 friend");
}

// ── Test 3: sum_property ──────────────────────────────────────────────────────

/// `MATCH (n:Person) RETURN SUM(n.age) AS total_age`.
#[test]
fn sum_property() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {age: 30})").expect("age 30");
    db.execute("CREATE (n:Person {age: 25})").expect("age 25");
    db.execute("CREATE (n:Person {age: 45})").expect("age 45");

    let result = db
        .execute("MATCH (n:Person) RETURN SUM(n.age) AS total_age")
        .expect("SUM must succeed");

    assert_eq!(result.rows.len(), 1, "SUM with no grouping key must return 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(100),
        "SUM(age) must be 100; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 4: avg_property ──────────────────────────────────────────────────────

/// `MATCH (n:Person) RETURN AVG(n.age) AS avg`.
#[test]
fn avg_property() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {age: 10})").expect("age 10");
    db.execute("CREATE (n:Person {age: 20})").expect("age 20");
    db.execute("CREATE (n:Person {age: 30})").expect("age 30");

    let result = db
        .execute("MATCH (n:Person) RETURN AVG(n.age) AS avg")
        .expect("AVG must succeed");

    assert_eq!(result.rows.len(), 1, "AVG with no grouping key must return 1 row");
    match &result.rows[0][0] {
        Value::Float64(f) => {
            let diff = (f - 20.0_f64).abs();
            assert!(
                diff < 1e-9,
                "AVG(age) should be 20.0; got: {f}"
            );
        }
        other => panic!("expected Float64 for AVG, got {:?}", other),
    }
}

// ── Test 5: min_max ───────────────────────────────────────────────────────────

/// `MATCH (n:Person) RETURN MIN(n.age), MAX(n.age)`.
#[test]
fn min_max() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {age: 42})").expect("age 42");
    db.execute("CREATE (n:Person {age: 7})").expect("age 7");
    db.execute("CREATE (n:Person {age: 99})").expect("age 99");
    db.execute("CREATE (n:Person {age: 23})").expect("age 23");

    let result = db
        .execute("MATCH (n:Person) RETURN MIN(n.age), MAX(n.age)")
        .expect("MIN/MAX must succeed");

    assert_eq!(result.rows.len(), 1, "MIN/MAX must return 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(7),
        "MIN(age) must be 7; got: {:?}",
        result.rows[0][0]
    );
    assert_eq!(
        result.rows[0][1],
        Value::Int64(99),
        "MAX(age) must be 99; got: {:?}",
        result.rows[0][1]
    );
}

// ── Test 6: count_star_no_rows ────────────────────────────────────────────────

/// When the label exists but no rows match, COUNT(*) must return 0, not an error.
#[test]
fn count_star_no_rows() {
    let (_dir, db) = make_db();

    // Register the label by creating one node that won't match the filter.
    db.execute("CREATE (n:Person {name: 'Sentinel'})").expect("CREATE sentinel");

    // Use a filter that matches nobody — label exists but zero rows match.
    let result = db
        .execute("MATCH (n:Person {name: 'NOBODY'}) RETURN COUNT(*) AS total")
        .expect("COUNT(*) over zero-matching-nodes must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(*) must still return 1 row when zero nodes match"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(0),
        "COUNT(*) over zero rows must be 0; got: {:?}",
        result.rows[0][0]
    );
}
