//! SPA-155: UNWIND $param resolves to empty list — wire parameter map.
//!
//! These tests verify that `UNWIND $param AS alias RETURN alias` correctly
//! expands the runtime parameter supplied via `execute_with_params()`.
//!
//! Test cases:
//!   - `unwind_param_list`           — integer list `$items = [1, 2, 3]` → 3 rows
//!   - `unwind_param_string_list`    — string list `$names = [...]` → 3 rows
//!   - `unwind_param_empty_list`     — empty list `$items = []` → 0 rows, no error
//!   - `unwind_param_with_match`     — UNWIND IDs, then MATCH each node; verifies
//!     that UNWIND param results can drive subsequent
//!     MATCH queries (two-query pattern)

use sparrowdb::open;
use sparrowdb_execution::Value;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ── unwind_param_list ─────────────────────────────────────────────────────────

/// `UNWIND $items AS x RETURN x` with `items = [1, 2, 3]` must return 3 rows.
#[test]
fn unwind_param_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute_with_params(
            "UNWIND $items AS x RETURN x",
            params(&[(
                "items",
                Value::List(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]),
            )]),
        )
        .expect("UNWIND $items must not error");

    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 3, "expected one row per list element");
    assert_eq!(result.rows[0], vec![Value::Int64(1)]);
    assert_eq!(result.rows[1], vec![Value::Int64(2)]);
    assert_eq!(result.rows[2], vec![Value::Int64(3)]);
}

// ── unwind_param_string_list ─────────────────────────────────────────────────

/// `UNWIND $names AS name RETURN name` with a string list must return one row
/// per element with the correct string value.
#[test]
fn unwind_param_string_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute_with_params(
            "UNWIND $names AS name RETURN name",
            params(&[(
                "names",
                Value::List(vec![
                    Value::String("Alice".into()),
                    Value::String("Bob".into()),
                    Value::String("Charlie".into()),
                ]),
            )]),
        )
        .expect("UNWIND $names must not error");

    assert_eq!(result.columns, vec!["name"]);
    assert_eq!(result.rows.len(), 3, "expected one row per string element");
    assert_eq!(result.rows[0], vec![Value::String("Alice".into())]);
    assert_eq!(result.rows[1], vec![Value::String("Bob".into())]);
    assert_eq!(result.rows[2], vec![Value::String("Charlie".into())]);
}

// ── unwind_param_empty_list ───────────────────────────────────────────────────

/// `UNWIND $items AS x RETURN x` with an empty list must produce 0 rows and
/// no error.
#[test]
fn unwind_param_empty_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute_with_params(
            "UNWIND $items AS x RETURN x",
            params(&[("items", Value::List(vec![]))]),
        )
        .expect("UNWIND with empty list must not error");

    assert_eq!(result.columns, vec!["x"]);
    assert_eq!(result.rows.len(), 0, "empty param list must produce 0 rows");
}

// ── unwind_param_with_match ───────────────────────────────────────────────────

/// Verifies the two-query pattern: UNWIND $ids → extract ids → MATCH each node.
///
/// Because SparrowDB's UNWIND currently requires an immediate RETURN clause
/// (UNWIND…MATCH in a single statement is not yet supported), this test uses
/// the natural two-step approach that KMS and other callers would use:
///
///   1. `UNWIND $ids AS id RETURN id`   — unwrap the parameter list
///   2. `MATCH (n {name: $name}) RETURN n.name` — look up each node by id value
///
/// This proves that the UNWIND param binding works end-to-end and that the
/// values it produces can drive subsequent parameterized MATCH queries.
#[test]
fn unwind_param_with_match() {
    let (_dir, db) = make_db();

    // Seed the graph.
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Charlie'})").unwrap();

    // Step 1: UNWIND the $ids parameter to get a list of name values.
    let ids = vec![Value::String("Alice".into()), Value::String("Bob".into())];
    let unwind_result = db
        .execute_with_params(
            "UNWIND $ids AS id RETURN id",
            params(&[("ids", Value::List(ids))]),
        )
        .expect("UNWIND $ids must not error");

    assert_eq!(unwind_result.rows.len(), 2, "UNWIND must produce 2 rows");

    // Step 2: For each unwound id, issue a MATCH query.
    let mut matched_names: Vec<String> = Vec::new();
    for row in &unwind_result.rows {
        if let Some(Value::String(name_val)) = row.first() {
            let match_result = db
                .execute_with_params(
                    "MATCH (n:Person {name: $name}) RETURN n.name",
                    params(&[("name", Value::String(name_val.clone()))]),
                )
                .expect("MATCH by param must not error");
            for mrow in &match_result.rows {
                if let Some(Value::String(n)) = mrow.first() {
                    matched_names.push(n.clone());
                }
            }
        }
    }

    assert_eq!(matched_names.len(), 2, "should match exactly 2 nodes");
    assert!(matched_names.contains(&"Alice".to_string()));
    assert!(matched_names.contains(&"Bob".to_string()));
}
