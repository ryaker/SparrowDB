//! Regression tests for issue #364:
//! `MATCH (n:Label) RETURN id(n), labels(n), n` returns `Value::Null` for the
//! `n` column under the chunked pipeline.
//!
//! Root cause: `can_use_chunked_pipeline` did not guard against bare variable
//! returns (`Expr::Var`). Queries with `RETURN n` were routed to
//! `execute_scan_chunked` → `project_row`, which hashes the string "n" as a
//! col_id and finds no matching column, returning `Value::Null`.
//!
//! Fix: `can_use_chunked_pipeline` now returns `false` whenever any RETURN item
//! is a bare variable, routing those queries to the row engine that implements
//! SPA-213 full-property-map projection.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db_with_person() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Person");
    (dir, db)
}

/// Core regression: `RETURN n` (bare var) must yield `Value::Map`, not `Value::Null`.
///
/// Before the fix the chunked pipeline routed this query and `project_row`
/// returned `Value::Null` for the `n` column.
#[test]
fn regression_364_bare_var_return_yields_map_not_null() {
    let (_dir, db) = make_db_with_person();

    let result = db
        .execute("MATCH (n:Person) RETURN id(n), labels(n), n")
        .expect("MATCH (n:Person) RETURN id(n), labels(n), n must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row, got {}.\nRows: {:?}",
        result.rows.len(),
        result.rows
    );

    assert_eq!(
        result.columns,
        vec!["id(n)", "labels(n)", "n"],
        "column names must match"
    );

    let row = &result.rows[0];
    assert_eq!(row.len(), 3);

    // id(n) must be a non-null integer.
    assert!(
        matches!(&row[0], Value::Int64(_)),
        "id(n) must be Value::Int64, got {:?}",
        row[0]
    );

    // labels(n) must be a list containing "Person".
    match &row[1] {
        Value::List(labels) => {
            assert!(
                labels
                    .iter()
                    .any(|v| matches!(v, Value::String(s) if s == "Person")),
                "labels(n) must contain 'Person', got {:?}",
                labels
            );
        }
        other => panic!("labels(n) must be Value::List, got {:?}", other),
    }

    // n must be a Value::Map — NOT Value::Null (the regression).
    match &row[2] {
        Value::Map(props) => {
            // The map must carry name and age.
            // Value::Map is Vec<(String, Value)> — iterate over tuples.
            let has_name = props
                .iter()
                .any(|(_, v)| matches!(v, Value::String(s) if s == "Alice"));
            let has_age = props.iter().any(|(_, v)| matches!(v, Value::Int64(30)));
            assert!(has_name, "map must contain name='Alice', got {:?}", props);
            assert!(has_age, "map must contain age=30, got {:?}", props);
        }
        Value::Null => panic!(
            "regression #364: n column returned Value::Null — bare variable not projected correctly"
        ),
        other => panic!("n must be Value::Map, got {:?}", other),
    }
}

/// Variant with WHERE clause: `RETURN id(n), labels(n), n` after a property filter.
///
/// Exercises that the row-engine fallback also handles WHERE + bare var correctly.
#[test]
fn regression_364_bare_var_return_with_where_yields_map() {
    let (_dir, db) = make_db_with_person();

    let result = db
        .execute("MATCH (n:Person) WHERE n.name = 'Alice' RETURN id(n), labels(n), n")
        .expect("MATCH (n:Person) WHERE n.name='Alice' RETURN id(n), labels(n), n must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row, got {}.\nRows: {:?}",
        result.rows.len(),
        result.rows
    );

    let row = &result.rows[0];
    assert_eq!(row.len(), 3);

    // n must still be a Value::Map.
    assert!(
        matches!(&row[2], Value::Map(_)),
        "n column must be Value::Map even after WHERE filter, got {:?}",
        row[2]
    );
}
