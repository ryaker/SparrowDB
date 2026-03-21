//! E2E tests for `type(r)` and `labels(n)` metadata functions.
//!
//! `type(r)` returns the relationship type name as a string:
//!   MATCH (a)-[r:KNOWS]->(b) RETURN type(r)  -- returns "KNOWS"
//!
//! `labels(n)` returns the node label(s) as a list:
//!   MATCH (n:Person) RETURN labels(n)         -- returns ["Person"]
//!
//! Both functions require the engine to inject metadata into the row map
//! during traversal so that eval_expr can resolve them without a catalog
//! round-trip per row.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: type(r) returns the relationship type name ───────────────────────

/// MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r)
/// must return the string "KNOWS" for each matched edge.
#[test]
fn type_fn_returns_rel_type() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE edge");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r)")
        .expect("MATCH with type(r) must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("KNOWS".to_string()),
        "type(r) must return 'KNOWS'; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 2: type(r) in WHERE filters correctly ───────────────────────────────

/// WHERE type(r) = 'KNOWS' should pass for KNOWS edges and filter out others.
#[test]
fn type_fn_in_where() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE KNOWS edge");

    // Filter that matches — should return 1 row.
    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE type(r) = 'KNOWS' RETURN a.name")
        .expect("MATCH with WHERE type(r) must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "WHERE type(r) = 'KNOWS' must return 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "a.name must be 'Alice'; got: {:?}",
        result.rows[0][0]
    );

    // Filter that does NOT match — should return 0 rows.
    let result_no_match = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE type(r) = 'FOLLOWS' RETURN a.name")
        .expect("MATCH with WHERE type(r) = 'FOLLOWS' must succeed");

    assert_eq!(
        result_no_match.rows.len(),
        0,
        "WHERE type(r) = 'FOLLOWS' must return 0 rows; got: {:?}",
        result_no_match.rows
    );
}

// ── Test 3: labels(n) returns the node label ─────────────────────────────────

/// MATCH (n:Person) RETURN labels(n)
/// must return a list containing "Person" for each matched node.
#[test]
fn labels_fn_returns_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = db
        .execute("MATCH (n:Person) RETURN labels(n)")
        .expect("MATCH with labels(n) must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows; got: {:?}",
        result.rows
    );

    for row in &result.rows {
        match &row[0] {
            Value::List(items) => {
                assert!(
                    items.contains(&Value::String("Person".to_string())),
                    "labels(n) must contain 'Person'; got: {:?}",
                    items
                );
            }
            other => panic!("expected Value::List from labels(n), got: {:?}", other),
        }
    }
}

// ── Test 4: type(r) works on variable-length paths ───────────────────────────

/// MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN type(r)
/// should return "KNOWS" for each path found within 1..2 hops.
#[test]
fn type_fn_variable_path() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");

    // Alice→Bob, Bob→Charlie
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE Alice→Bob");
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE Bob→Charlie");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN type(r)")
        .expect("variable-length MATCH with type(r) must succeed");

    // Should return at least 1 row (direct 1-hop paths).
    assert!(
        !result.rows.is_empty(),
        "variable-length path must return rows; got: {:?}",
        result.rows
    );

    // Every row should have type "KNOWS".
    for row in &result.rows {
        assert_eq!(
            row[0],
            Value::String("KNOWS".to_string()),
            "type(r) must be 'KNOWS' for variable-length path; got: {:?}",
            row[0]
        );
    }
}
