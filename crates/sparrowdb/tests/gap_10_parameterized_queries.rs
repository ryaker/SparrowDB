//! Integration tests for GAP-10: parameterized query support.
//!
//! Covers `$param` syntax in:
//!   - Inline property filters: `MATCH (n:Person {name: $name})`
//!   - WHERE clauses: `MATCH (n:Person) WHERE n.name = $name`
//!   - UNWIND (pre-existing, regression check): `UNWIND $names AS x`
//!
//! Also verifies that plain `execute()` still works (no regression).

use sparrowdb::open;
use sparrowdb_execution::types::Value;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Helper ────────────────────────────────────────────────────────────────────

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ── GAP-10: inline property filter with $param ────────────────────────────────

/// `MATCH (n:Person {name: $name}) RETURN n.name` with a matching param
/// must return exactly the matching node.
#[test]
fn param_in_inline_prop_filter_returns_matching_row() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = db
        .execute_with_params(
            "MATCH (n:Person {name: $name}) RETURN n.name",
            params(&[("name", Value::String("Alice".into()))]),
        )
        .expect("execute_with_params must not error");

    assert_eq!(result.rows.len(), 1, "should return exactly one row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".into()),
        "returned name must be Alice"
    );
}

/// `execute_with_params` with a non-matching param value must return 0 rows.
#[test]
fn param_in_inline_prop_filter_non_match_returns_zero_rows() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute_with_params(
            "MATCH (n:Person {name: $name}) RETURN n.name",
            params(&[("name", Value::String("Charlie".into()))]),
        )
        .expect("execute_with_params must not error even when no rows match");

    assert_eq!(
        result.rows.len(),
        0,
        "non-matching param should produce 0 rows"
    );
}

/// `MATCH (n:Person {name: $name}) RETURN n.name` scoped to one label among many.
#[test]
fn param_filter_only_returns_nodes_of_correct_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Robot {name: 'Alice'})").unwrap(); // same name, different label

    let result = db
        .execute_with_params(
            "MATCH (n:Person {name: $name}) RETURN n.name",
            params(&[("name", Value::String("Alice".into()))]),
        )
        .expect("execute_with_params");

    assert_eq!(
        result.rows.len(),
        1,
        "should only match Person nodes, not Robot"
    );
}

// ── GAP-10: $param in WHERE clause ────────────────────────────────────────────

/// `MATCH (n:Person) WHERE n.name = $name RETURN n.name` must resolve the param.
#[test]
fn param_in_where_clause_returns_matching_row() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute_with_params(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
            params(&[("name", Value::String("Bob".into()))]),
        )
        .expect("execute_with_params with WHERE $param must not error");

    assert_eq!(result.rows.len(), 1, "should return exactly one row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Bob".into()),
        "returned name must be Bob"
    );
}

// ── GAP-10: multiple params ────────────────────────────────────────────────────

/// Multiple params in the same query must all be resolved.
/// Uses both $name and $age in the WHERE clause so that both params are
/// actually exercised (not just one with an unused extra key).
#[test]
fn multiple_params_in_prop_filter() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .unwrap();

    // Both $name and $age must be substituted — only the Alice node with
    // age 30 should survive the combined WHERE filter.
    let result = db
        .execute_with_params(
            "MATCH (n:Person) WHERE n.name = $name AND n.age = $age RETURN n.name",
            params(&[
                ("name", Value::String("Alice".into())),
                ("age", Value::Int64(30)),
            ]),
        )
        .expect("multi-param query");

    assert_eq!(result.rows.len(), 1, "only one Alice node has age 30");
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
}

// ── Regression: plain execute() still works ───────────────────────────────────

/// `execute()` (no params) must still work correctly after the GAP-10 changes.
#[test]
fn plain_execute_still_works() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("plain execute must still work");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
}

// ── Regression: UNWIND $param (pre-existing) ──────────────────────────────────

/// `UNWIND $names AS name RETURN name` must still work (pre-existing behaviour).
#[test]
fn unwind_param_regression() {
    let (_dir, db) = make_db();

    let result = db
        .execute_with_params(
            "UNWIND $names AS name RETURN name",
            params(&[(
                "names",
                Value::List(vec![
                    Value::String("Alice".into()),
                    Value::String("Bob".into()),
                ]),
            )]),
        )
        .expect("UNWIND $param must still work");

    assert_eq!(result.rows.len(), 2);
    let names: Vec<_> = result.rows.iter().map(|r| r[0].clone()).collect();
    assert!(names.contains(&Value::String("Alice".into())));
    assert!(names.contains(&Value::String("Bob".into())));
}


#[test]
fn parameterized_merge_creates_node() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "MERGE (n:Person {name: $name})",
        params(&[("name", Value::String("Alice".into()))]),
    ).expect("parameterized MERGE must succeed");
    let result = db.execute("MATCH (n:Person) RETURN n.name").expect("MATCH");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
}

#[test]
fn parameterized_merge_is_idempotent() {
    let (_dir, db) = make_db();
    let p = params(&[("name", Value::String("Alice".into()))]);
    db.execute_with_params("MERGE (n:Person {name: $name})", p.clone()).expect("first");
    db.execute_with_params("MERGE (n:Person {name: $name})", p).expect("second");
    let result = db.execute("MATCH (n:Person) RETURN n.name").expect("MATCH");
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn parameterized_merge_with_integer_param() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "MERGE (n:Counter {value: $val})",
        params(&[("val", Value::Int64(42))]),
    ).expect("MERGE with integer param");
    let result = db.execute("MATCH (n:Counter) RETURN n.value").expect("MATCH");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(42));
}

#[test]
fn parameterized_merge_missing_param_returns_error() {
    let (_dir, db) = make_db();
    let result = db.execute_with_params("MERGE (n:Person {name: $name})", HashMap::new());
    assert!(result.is_err(), "missing param must error");
}

#[test]
fn parameterized_set_updates_property() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice', age: 0})").unwrap();
    db.execute_with_params(
        "MATCH (n:Person {name: $name}) SET n.age = $age",
        params(&[("name", Value::String("Alice".into())), ("age", Value::Int64(30))]),
    ).expect("parameterized SET");
    let result = db.execute("MATCH (n:Person {name: 'Alice'}) RETURN n.age").expect("MATCH");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(30));
}
