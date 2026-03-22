//! Regression tests for COUNT aggregation and DISTINCT deduplication (SPA-172).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Bug 1: COUNT(n) parse error ───────────────────────────────────────────────

/// COUNT(variable) must parse and return the same scalar count as COUNT(*).
#[test]
fn count_variable_parses_and_returns_total() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob',   age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Carol',  age: 35})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN COUNT(n)")
        .expect("COUNT(n) must not raise a parse error");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(n) should return exactly one row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "COUNT(n) should equal the number of matched nodes"
    );
}

// ── Bug 2: COUNT(*) returns wrong results ────────────────────────────────────

/// COUNT(*) must return a single scalar with the total row count, not one row per node.
#[test]
fn count_star_returns_scalar_total() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Dave'})").unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN COUNT(*)")
        .expect("COUNT(*) must succeed");

    // Must return exactly one row, not one row per node.
    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(*) must aggregate into a single row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(4),
        "COUNT(*) must equal total number of matched nodes"
    );

    // Column must be labelled count(*).
    assert_eq!(result.columns[0], "count(*)");
}

// ── Bug 3: DISTINCT not deduplicating ────────────────────────────────────────

/// RETURN DISTINCT must eliminate duplicate values in the simple node-scan path.
#[test]
fn distinct_deduplicates_node_scan() {
    let (_dir, db) = make_db();

    // Insert 5 nodes: two share age=30.
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Alice2', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob',   age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Carol',  age: 35})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Dave',   age: 40})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN DISTINCT n.age")
        .expect("DISTINCT must succeed");

    // 4 unique ages: 30, 25, 35, 40.
    assert_eq!(
        result.rows.len(),
        4,
        "DISTINCT should return 4 unique ages, got {:?}",
        result.rows
    );

    // Validate returned values are typed Int64 and contain exactly the 4 unique ages.
    let mut ages: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64 age value, got {:?}", other),
        })
        .collect();
    ages.sort_unstable();
    ages.dedup();
    assert_eq!(
        ages,
        vec![25, 30, 35, 40],
        "DISTINCT must return exactly the 4 unique ages"
    );
}

/// COUNT(*) on an empty result set must return a single row with value 0.
#[test]
fn count_star_empty_result_returns_zero() {
    let (_dir, db) = make_db();

    // Insert a node so the label is registered, then match with a WHERE that matches nothing.
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) WHERE n.age > 9999 RETURN COUNT(*)")
        .expect("COUNT(*) on empty set must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(*) should return one row even for empty input"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(0),
        "COUNT(*) of empty set should be 0"
    );
}
