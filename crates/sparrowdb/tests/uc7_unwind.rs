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
//!   - Parameter reference `$items` (empty — unbound params produce 0 rows)
//!   - Parameter reference `$names` with real binding (SPA-190)

use std::collections::HashMap;

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

// ── Parameter reference (unbound) ─────────────────────────────────────────────

#[test]
fn unwind_param_returns_empty_without_binding() {
    // When no params are supplied (using the plain `execute` API without a
    // params map), an unresolved `$items` parameter produces an empty list and
    // therefore 0 rows.  This is the correct fallback behaviour.
    let (_dir, db) = make_db();
    let result = db.execute("UNWIND $items AS item RETURN item").unwrap();

    assert_eq!(result.columns, vec!["item"]);
    assert_eq!(
        result.rows.len(),
        0,
        "unbound param produces 0 rows when no params map is provided"
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

// ── SPA-190: Parameter binding — $param resolves from execute_with_params ─────

#[test]
fn spa190_unwind_string_param_resolves_list() {
    // Verify that `UNWIND $names AS name` correctly expands when the `names`
    // parameter is supplied as a `Value::List` via `execute_with_params`.
    let (_dir, db) = make_db();

    let mut params = HashMap::new();
    params.insert(
        "names".into(),
        Value::List(vec![
            Value::String("Alice".into()),
            Value::String("Bob".into()),
            Value::String("Charlie".into()),
        ]),
    );

    let result = db
        .execute_with_params("UNWIND $names AS name RETURN name", params)
        .unwrap();

    assert_eq!(result.columns, vec!["name"]);
    assert_eq!(result.rows.len(), 3, "expected one row per element");
    assert_eq!(result.rows[0], vec![Value::String("Alice".into())]);
    assert_eq!(result.rows[1], vec![Value::String("Bob".into())]);
    assert_eq!(result.rows[2], vec![Value::String("Charlie".into())]);
}

#[test]
fn spa190_unwind_integer_param_resolves_list() {
    // Verify integer list parameter binding.
    let (_dir, db) = make_db();

    let mut params = HashMap::new();
    params.insert(
        "nums".into(),
        Value::List(vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)]),
    );

    let result = db
        .execute_with_params("UNWIND $nums AS n RETURN n", params)
        .unwrap();

    assert_eq!(result.columns, vec!["n"]);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0], vec![Value::Int64(10)]);
    assert_eq!(result.rows[1], vec![Value::Int64(20)]);
    assert_eq!(result.rows[2], vec![Value::Int64(30)]);
}

#[test]
fn spa190_unwind_empty_param_list_produces_no_rows() {
    // An explicitly supplied but empty list parameter must produce 0 rows.
    let (_dir, db) = make_db();

    let mut params = HashMap::new();
    params.insert("items".into(), Value::List(vec![]));

    let result = db
        .execute_with_params("UNWIND $items AS item RETURN item", params)
        .unwrap();

    assert_eq!(result.columns, vec!["item"]);
    assert_eq!(result.rows.len(), 0, "empty list param must produce 0 rows");
}

#[test]
fn spa190_unwind_scalar_param_wraps_as_single_row() {
    // When a scalar (non-list) value is supplied as a parameter to UNWIND,
    // it should be treated as a single-element list (matches Neo4j behaviour).
    let (_dir, db) = make_db();

    let mut params = HashMap::new();
    params.insert("val".into(), Value::Int64(42));

    let result = db
        .execute_with_params("UNWIND $val AS x RETURN x", params)
        .unwrap();

    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 1, "scalar param wraps as single element");
    assert_eq!(result.rows[0], vec![Value::Int64(42)]);
}

#[test]
fn spa190_unwind_missing_param_produces_no_rows() {
    // If execute_with_params is called but the referenced param is not in the
    // map, we get 0 rows (same as unbound — not an error).
    let (_dir, db) = make_db();

    // Supply a different key — `$items` is not in the map.
    let mut params = HashMap::new();
    params.insert("other".into(), Value::Int64(1));

    let result = db
        .execute_with_params("UNWIND $items AS item RETURN item", params)
        .unwrap();

    assert_eq!(result.columns, vec!["item"]);
    assert_eq!(
        result.rows.len(),
        0,
        "missing param key produces 0 rows, not an error"
    );
}
