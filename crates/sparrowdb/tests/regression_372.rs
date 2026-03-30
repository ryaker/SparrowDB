//! Regression tests for #372 — `id(n)` in RETURN clause returns Null instead
//! of the node's internal ID.
//!
//! Root cause: `build_read_engine` enables the chunked pipeline
//! (`with_chunked_pipeline`), and `can_use_chunked_pipeline` did not guard
//! against `id()` in RETURN items.  When an alias is present (`RETURN id(n) AS
//! nid`) the column name becomes `"nid"`, which `project_row`'s string-pattern
//! check (`col_name.strip_prefix("id(")`) cannot match.  The row engine's eval
//! path (enabled by `needs_node_ref_in_return`) correctly injects
//! `NodeRef` into the row map and resolves `id(n)` via `eval_expr`.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// `id(n)` must return a non-null Int64 when combined with an AS alias.
///
/// This is the exact query shape from issue #372:
/// `MATCH (n:Person {name: "Alice"}) RETURN id(n), n.name`
#[test]
fn id_function_with_alias_returns_integer() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN id(n), n.name")
        .expect("query must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    let row = &result.rows[0];
    assert!(
        matches!(row[0], Value::Int64(_)),
        "id(n) must return Int64, got {:?}",
        row[0]
    );
    assert_ne!(row[0], Value::Null, "id(n) must not be Null");
    assert_eq!(
        row[1],
        Value::String("Alice".into()),
        "n.name must be Alice"
    );
}

/// `RETURN id(n) AS nid` — aliased id() must return the correct node ID.
#[test]
fn id_function_alias_returns_integer() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Widget {color: 'blue'})").unwrap();

    let result = db
        .execute("MATCH (n:Widget) RETURN id(n) AS nid, n.color AS color")
        .expect("query must succeed");

    assert_eq!(result.columns, vec!["nid", "color"]);
    assert_eq!(result.rows.len(), 1);
    assert!(
        matches!(result.rows[0][0], Value::Int64(_)),
        "nid column must be Int64, got {:?}",
        result.rows[0][0]
    );
    assert_ne!(result.rows[0][0], Value::Null, "nid must not be Null");
    assert_eq!(
        result.rows[0][1],
        Value::String("blue".into()),
        "color must be blue"
    );
}

/// `id(n)` values must be unique across distinct nodes — validates that the ID
/// returned is the actual per-node slot-based ID, not a constant.
#[test]
fn id_function_unique_ids() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Item {val: 1})").unwrap();
    db.execute("CREATE (n:Item {val: 2})").unwrap();
    db.execute("CREATE (n:Item {val: 3})").unwrap();

    let result = db
        .execute("MATCH (n:Item) RETURN id(n) AS nid")
        .expect("query must succeed");

    assert_eq!(result.rows.len(), 3, "must return 3 rows");

    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int64(v) => v,
            ref other => panic!("expected Int64, got {:?}", other),
        })
        .collect();

    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        3,
        "all node IDs must be distinct, got: {:?}",
        ids
    );
}

/// `WHERE id(n) = <known_id>` must filter to exactly one node.
///
/// Uses inline prop filter `{name: ...}` on the MATCH to match the issue
/// description exactly.
#[test]
fn id_function_where_filter_with_inline_prop() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    // Get Alice's node ID.
    let all = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN id(n) AS nid")
        .expect("must succeed");
    assert_eq!(all.rows.len(), 1, "should match exactly Alice");
    let alice_id = match all.rows[0][0] {
        Value::Int64(id) => id,
        ref other => panic!("expected Int64, got {:?}", other),
    };

    // Filter by ID.
    let query = format!("MATCH (n:Person) WHERE id(n) = {alice_id} RETURN n.name");
    let result = db.execute(&query).expect("WHERE id(n) = X must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "WHERE id(n) = alice_id must return 1 row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".into()),
        "must return Alice"
    );
}
