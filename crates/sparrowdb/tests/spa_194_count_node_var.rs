//! Regression tests for SPA-194: count(node_variable) aggregation.
//!
//! `count(n)` where `n` is a node variable must route through the aggregation
//! path (AggKind::Count) and not fall into scalar function dispatch.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── count(n) with label filter ────────────────────────────────────────────────

/// `MATCH (n:Person) RETURN count(n) as cnt` must return the total node count.
#[test]
fn count_node_var_lowercase_with_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Carol', age: 35})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN count(n) as cnt")
        .expect("count(n) with label must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "count(n) must return exactly one aggregated row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "count(n) must equal the number of matched nodes"
    );
    assert_eq!(
        result.columns[0], "cnt",
        "alias 'cnt' must be used as column name"
    );
}

// ── count(n) without label — full scan ───────────────────────────────────────

/// `MATCH (n) RETURN count(n) as cnt` must count all nodes across all labels.
#[test]
fn count_node_var_no_label_full_scan() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Movie {title: 'Matrix'})").unwrap();

    let result = db
        .execute("MATCH (n) RETURN count(n) as cnt")
        .expect("count(n) without label must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "count(n) without label must return one aggregated row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "count(n) without label must count nodes across all labels"
    );
}

// ── Grouped count ─────────────────────────────────────────────────────────────

/// `MATCH (n:Person) RETURN n.name, count(n)` must produce one row per name
/// with each count equal to 1 (since names are unique).
#[test]
fn count_node_var_grouped_by_property() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Alice', age: 22})")
        .unwrap(); // duplicate name

    let result = db
        .execute("MATCH (n:Person) RETURN n.name, count(n)")
        .expect("grouped count(n) must not error");

    // Two distinct names: Alice (×2) and Bob (×1).
    assert_eq!(
        result.rows.len(),
        2,
        "grouped count(n) must return one row per distinct group, got {:?}",
        result.rows
    );

    // Build a map of name → count for order-independent checking.
    let mut name_counts: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for row in &result.rows {
        let name = match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected string name, got {:?}", other),
        };
        let cnt = match &row[1] {
            Value::Int64(n) => *n,
            other => panic!("expected Int64 count, got {:?}", other),
        };
        name_counts.insert(name, cnt);
    }

    assert_eq!(
        name_counts.get("Alice").copied(),
        Some(2),
        "Alice appears twice — count must be 2"
    );
    assert_eq!(
        name_counts.get("Bob").copied(),
        Some(1),
        "Bob appears once — count must be 1"
    );
}
