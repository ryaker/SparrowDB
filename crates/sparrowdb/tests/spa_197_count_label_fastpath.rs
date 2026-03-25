//! Tests for SPA-197: O(1) COUNT label fast-path via `label_row_counts`.
//!
//! `MATCH (n:Label) RETURN COUNT(n) AS total` (and COUNT(*)) should be answered
//! from the pre-populated `label_row_counts` HashMap without scanning nodes.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── COUNT(n) with label ─────────────────────────────────────────────────────

#[test]
fn count_label_fastpath_count_var() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'Alice'})").unwrap();
    db.execute("CREATE (:User {name: 'Bob'})").unwrap();
    db.execute("CREATE (:User {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Product {name: 'Widget'})").unwrap();

    let result = db
        .execute("MATCH (n:User) RETURN COUNT(n) AS total")
        .expect("COUNT(n) with label");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(3));
    assert_eq!(result.columns[0], "total");
}

// ── COUNT(*) with label ─────────────────────────────────────────────────────

#[test]
fn count_label_fastpath_count_star() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'Alice'})").unwrap();
    db.execute("CREATE (:User {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n:User) RETURN COUNT(*) AS total")
        .expect("COUNT(*) with label");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(2));
    assert_eq!(result.columns[0], "total");
}

// ── Unknown label returns 0 ─────────────────────────────────────────────────

#[test]
fn count_label_fastpath_unknown_label() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MATCH (n:NonExistent) RETURN COUNT(n) AS total")
        .expect("COUNT on unknown label");

    // Standard Cypher: unknown label → single row with count 0.
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(0));
}

// ── COUNT with WHERE falls through to full scan ─────────────────────────────

#[test]
fn count_with_where_falls_through() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:User {name: 'Bob', age: 25})").unwrap();
    db.execute("CREATE (:User {name: 'Carol', age: 35})")
        .unwrap();

    let result = db
        .execute("MATCH (n:User) WHERE n.age > 28 RETURN COUNT(n) AS total")
        .expect("COUNT with WHERE");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(2));
}

// ── COUNT without label falls through ───────────────────────────────────────

#[test]
fn count_no_label_falls_through() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Product {name: 'Widget'})").unwrap();

    let result = db
        .execute("MATCH (n) RETURN COUNT(n) AS total")
        .expect("COUNT without label");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(2));
}
