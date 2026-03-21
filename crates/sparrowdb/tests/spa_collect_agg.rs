//! E2E tests for collect() aggregation.
//!
//! collect() is a Cypher aggregate function that collects values into a list:
//!   MATCH (p:Person) RETURN collect(p.name)
//!   MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, collect(f.name) AS friends
//!
//! The implementation (added in feature/collect-aggregation) groups rows by
//! non-aggregate RETURN expressions and accumulates collected values into a
//! Value::List per group.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: collect_basic ─────────────────────────────────────────────────────

/// MATCH (p:Person) RETURN collect(p.name) AS names
///
/// When there are no grouping keys, a single row is returned with a list
/// containing all matched values.
#[test]
fn collect_basic() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})").expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Charlie'})").expect("CREATE Charlie");

    let result = db
        .execute("MATCH (p:Person) RETURN collect(p.name) AS names")
        .expect("collect() must succeed");

    // One output row containing all names.
    assert_eq!(
        result.rows.len(),
        1,
        "collect() with no grouping key must return exactly 1 row; got: {:?}",
        result.rows
    );

    let collected = &result.rows[0][0];
    match collected {
        Value::List(items) => {
            assert_eq!(
                items.len(),
                3,
                "collected list must have 3 items; got: {:?}",
                items
            );
            // The list should contain Alice, Bob, Charlie (any order).
            let names: Vec<String> = items
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert!(names.contains(&"Alice".to_string()), "missing Alice");
            assert!(names.contains(&"Bob".to_string()), "missing Bob");
            assert!(names.contains(&"Charlie".to_string()), "missing Charlie");
        }
        other => panic!("expected Value::List, got {:?}", other),
    }
}

// ── Test 2: collect_grouped ───────────────────────────────────────────────────

/// MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, collect(f.name) AS friends
///
/// Groups by p.name, collecting friend names into a list per group.
/// Uses MATCH…CREATE to establish edges (requires SPA-168).
#[test]
fn collect_grouped() {
    let (_dir, db) = make_db();

    // Create three Person nodes.
    db.execute("CREATE (n:Person {name: 'Alice'})").expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})").expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol'})").expect("CREATE Carol");

    // Alice knows Bob and Carol.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("Alice KNOWS Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .expect("Alice KNOWS Carol");

    let result = db
        .execute("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, collect(f.name) AS friends")
        .expect("grouped collect() must succeed");

    // Alice should appear once with friends [Bob, Carol].
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 group (Alice); got: {:?}",
        result.rows
    );

    let row = &result.rows[0];
    assert_eq!(row.len(), 2, "expected 2 columns per row");

    // First column: p.name = "Alice"
    assert_eq!(row[0], Value::String("Alice".to_string()), "group key mismatch");

    // Second column: collect(f.name) = [Bob, Carol]
    match &row[1] {
        Value::List(items) => {
            assert_eq!(
                items.len(),
                2,
                "Alice should have 2 friends; got: {:?}",
                items
            );
            let friends: Vec<String> = items
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert!(friends.contains(&"Bob".to_string()), "missing Bob");
            assert!(friends.contains(&"Carol".to_string()), "missing Carol");
        }
        other => panic!("expected Value::List for collect(f.name), got {:?}", other),
    }
}

// ── Test 3: collect_empty ─────────────────────────────────────────────────────

/// When no rows match, collect() with no grouping keys returns one row with
/// an empty list (not an error and not zero rows).
///
/// Two sub-cases:
/// 1. Label exists but has no nodes → should return one row with an empty list.
/// 2. Label does not exist → the engine errors with "unknown label"; we handle
///    this the same as OPTIONAL MATCH (the query returns no results without
///    panicking).
#[test]
fn collect_empty() {
    let (_dir, db) = make_db();

    // Sub-case 1: label exists, zero nodes.
    // Create and then verify the label exists by creating a dummy node with a
    // different name, then scanning Person (which will have 0 nodes).
    // Actually the simplest approach: create one node then delete the label
    // from the result via WHERE false - but we don't have that.
    // Instead, test the case where the label exists with zero matching nodes
    // by using an inline prop filter that matches nothing.
    db.execute("CREATE (n:Person {name: 'ZZZ'})").expect("CREATE Person");
    let result = db
        .execute("MATCH (p:Person {name: 'NOBODY'}) RETURN collect(p.name) AS names")
        .expect("collect() over zero-matching-nodes must succeed");

    // With no rows matching, aggregate_rows emits one row with an empty list.
    assert_eq!(result.rows.len(), 1, "must return 1 row even with zero matches");
    match &result.rows[0][0] {
        Value::List(items) => assert!(
            items.is_empty(),
            "collect over zero nodes should return empty list; got: {:?}",
            items
        ),
        other => panic!("expected Value::List([]), got {:?}", other),
    }
}

// ── Test 4: collect_integers ──────────────────────────────────────────────────

/// collect(p.age) collects integer properties into a list.
#[test]
fn collect_integers() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {age: 30})").expect("CREATE age 30");
    db.execute("CREATE (n:Person {age: 25})").expect("CREATE age 25");
    db.execute("CREATE (n:Person {age: 40})").expect("CREATE age 40");

    let result = db
        .execute("MATCH (p:Person) RETURN collect(p.age) AS ages")
        .expect("collect(integer) must succeed");

    assert_eq!(result.rows.len(), 1, "must return 1 row");

    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 3, "should collect 3 ages; got: {:?}", items);
            let ages: Vec<i64> = items
                .iter()
                .filter_map(|v| match v {
                    Value::Int64(n) => Some(*n),
                    _ => None,
                })
                .collect();
            assert!(ages.contains(&30), "missing age 30");
            assert!(ages.contains(&25), "missing age 25");
            assert!(ages.contains(&40), "missing age 40");
        }
        other => panic!("expected Value::List for collect(p.age), got {:?}", other),
    }
}
