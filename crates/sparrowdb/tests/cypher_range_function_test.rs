//! Tests for the `range(start, end[, step])` Cypher list function.

use sparrowdb::open;
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Basic range ─────────────────────────────────────────────────────────────

#[test]
fn test_range_basic() {
    let (_dir, db) = make_db();
    let result = db.execute("RETURN range(1, 5)").unwrap();
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);
    let expected = Value::List(vec![
        Value::Int64(1),
        Value::Int64(2),
        Value::Int64(3),
        Value::Int64(4),
        Value::Int64(5),
    ]);
    assert_eq!(result.rows[0][0], expected);
}

// ── Range with step ─────────────────────────────────────────────────────────

#[test]
fn test_range_with_step() {
    let (_dir, db) = make_db();
    let result = db.execute("RETURN range(0, 10, 2)").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected = Value::List(vec![
        Value::Int64(0),
        Value::Int64(2),
        Value::Int64(4),
        Value::Int64(6),
        Value::Int64(8),
        Value::Int64(10),
    ]);
    assert_eq!(result.rows[0][0], expected);
}

// ── Range with negative step ─────────────────────────────────────────────────

#[test]
fn test_range_with_negative_step() {
    let (_dir, db) = make_db();
    let result = db.execute("RETURN range(5, 1, -1)").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected = Value::List(vec![
        Value::Int64(5),
        Value::Int64(4),
        Value::Int64(3),
        Value::Int64(2),
        Value::Int64(1),
    ]);
    assert_eq!(result.rows[0][0], expected);
}

// ── Range in UNWIND ─────────────────────────────────────────────────────────

#[test]
fn test_range_in_unwind() {
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND range(1, 3) AS i RETURN i").unwrap();
    assert_eq!(result.columns, vec!["i"]);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0], vec![Value::Int64(1)]);
    assert_eq!(result.rows[1], vec![Value::Int64(2)]);
    assert_eq!(result.rows[2], vec![Value::Int64(3)]);
}
