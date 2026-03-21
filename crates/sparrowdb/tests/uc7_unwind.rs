//! UC-7 / SPA-133: UNWIND clause integration tests.
//!
//! Verifies that UNWIND takes a list expression and produces one row per
//! element, with the element bound to the declared alias variable.
//!
//! Test cases:
//!   - Integer list literal `[1, 2, 3]`
//!   - String list literal `['hello', 'world']`
//!   - Empty list `[]`
//!   - Float list `[1.5, 2.5, 3.5]`
//!   - Parameter reference `$items` (empty — params not resolved in stub)

use sparrowdb::open;
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Integer list ──────────────────────────────────────────────────────────────

#[test]
fn unwind_integer_list_returns_one_row_per_element() {
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND [1, 2, 3] AS x RETURN x").unwrap();

    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 3, "expected 3 rows");
    assert_eq!(result.rows[0], vec![Value::Int64(1)]);
    assert_eq!(result.rows[1], vec![Value::Int64(2)]);
    assert_eq!(result.rows[2], vec![Value::Int64(3)]);
}

// ── String list ───────────────────────────────────────────────────────────────

#[test]
fn unwind_string_list_returns_one_row_per_element() {
    let (_dir, db) = make_db();
    let result = db
        .execute("UNWIND ['hello', 'world'] AS s RETURN s")
        .unwrap();

    assert_eq!(result.columns, vec!["s"]);
    assert_eq!(result.rows.len(), 2, "expected 2 rows");
    assert_eq!(result.rows[0], vec![Value::String("hello".into())]);
    assert_eq!(result.rows[1], vec![Value::String("world".into())]);
}

// ── Empty list ────────────────────────────────────────────────────────────────

#[test]
fn unwind_empty_list_returns_no_rows() {
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND [] AS x RETURN x").unwrap();

    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 0, "empty list must produce 0 rows");
}

// ── Parameter reference ───────────────────────────────────────────────────────

#[test]
fn unwind_param_returns_empty_without_binding() {
    // The read-only engine stub does not resolve parameters — it returns an
    // empty list for `$items`.  This test documents the current behaviour;
    // when parameter binding is implemented the expectation can be updated.
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND $items AS item RETURN item").unwrap();

    assert_eq!(result.columns, vec!["item"]);
    assert_eq!(
        result.rows.len(),
        0,
        "unbound param produces 0 rows in current stub"
    );
}

// ── Single-element list ───────────────────────────────────────────────────────

#[test]
fn unwind_single_element_list() {
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND [42] AS n RETURN n").unwrap();

    assert_eq!(result.columns, vec!["n"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![Value::Int64(42)]);
}

// ── Wrong RETURN variable does NOT leak alias value ────────────────────────────
//
// Regression guard for the projection bug where `or_else` caused any missing
// column to fall back to the alias column, making `RETURN y` silently return
// the `x` values instead of NULL.

#[test]
fn unwind_return_wrong_variable_yields_null() {
    let (_dir, db) = make_db();
    // `y` is not in scope; each row should be NULL, not the `x` value.
    let result = db.execute("UNWIND [1, 2, 3] AS x RETURN y").unwrap();

    assert_eq!(result.columns, vec!["y"]);
    assert_eq!(result.rows.len(), 3, "still 3 rows");
    for row in &result.rows {
        assert_eq!(
            row,
            &vec![Value::Null],
            "out-of-scope variable must be NULL, not the alias value"
        );
    }
}

// ── Float list ────────────────────────────────────────────────────────────────

#[test]
fn unwind_float_list() {
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND [1.5, 2.5, 3.5] AS f RETURN f").unwrap();

    assert_eq!(result.columns, vec!["f"]);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0], vec![Value::Float64(1.5)]);
    assert_eq!(result.rows[1], vec![Value::Float64(2.5)]);
    assert_eq!(result.rows[2], vec![Value::Float64(3.5)]);
}
