//! SPA-195: `type(r)` on a relationship variable must return the relationship
//! type name string (e.g. "KNOWS") rather than erroring or returning empty/null.
//!
//! ## Bug scenario
//!
//! ```js
//! db.execute('MATCH (a:Person {name:"Alice"}),(b:Person {name:"Bob"}) CREATE (a)-[:KNOWS]->(b)');
//! db.execute('MATCH (a)-[r]->(b) RETURN type(r)');
//! // → Error: unknown relationship type  (before fix)
//! ```
//!
//! ## Root causes fixed
//!
//! 1. Parser: `[r]` (variable-only, no type) was mis-parsed — `r` was treated
//!    as the relationship type name instead of as a variable. The binder then
//!    rejected it as "unknown relationship type: r".
//! 2. Engine (`execute_one_hop`): required labeled src/dst nodes; unlabeled
//!    patterns like `(a)-[r]->(b)` failed with `NotFound`.
//! 3. Engine: delta log was always read from `RelTableId(0)`, so edges written
//!    to other rel tables were invisible and type names were lost.

use sparrowdb::open;
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: basic type(r) with unlabeled pattern ──────────────────────────────

/// `MATCH (a)-[r]->(b) RETURN type(r)` must return "KNOWS".
#[test]
fn type_r_unlabeled_pattern_returns_type_name() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE KNOWS");

    let result = db
        .execute("MATCH (a)-[r]->(b) RETURN type(r)")
        .expect("MATCH (a)-[r]->(b) RETURN type(r) must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("KNOWS".into()),
        "type(r) must return 'KNOWS', got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 2: typed pattern [r:KNOWS] still returns correct type ────────────────

/// `MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r)` must return "KNOWS".
#[test]
fn type_r_typed_pattern_returns_type_name() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE KNOWS");

    let result = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r)")
        .expect("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r)");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("KNOWS".into()),
        "type(r) for typed pattern must return 'KNOWS'"
    );
}

// ── Test 3: multiple rel types — type(r) returns distinct type names ──────────

/// With KNOWS and LIKES edges, `MATCH (a)-[r]->(b) RETURN type(r)` must
/// return both "KNOWS" and "LIKES" (in any order).
#[test]
fn type_r_multiple_rel_types_returns_correct_names() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol'})")
        .expect("CREATE Carol");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE KNOWS");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:LIKES]->(c)",
    )
    .expect("CREATE LIKES");

    let result = db
        .execute("MATCH (a)-[r]->(b) RETURN type(r)")
        .expect("MATCH (a)-[r]->(b) RETURN type(r) with multiple types");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows (one per edge); got: {:?}",
        result.rows
    );

    let mut types: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();
    types.sort();

    assert_eq!(types, vec!["KNOWS".to_string(), "LIKES".to_string()]);
}
