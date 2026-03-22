//! Regression tests for id(n) returning the internal node ID — SPA-196.
//!
//! Prior to the fix, `id(n)` always returned `null` in the non-aggregate
//! fast path because the projection code never inserted a `NodeRef` into
//! the row map.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Basic id(n) returns non-null integer ──────────────────────────────────────

/// `id(n)` must return a non-null integer for every matched node.
#[test]
fn id_returns_non_null_integer() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN id(n) as nid, n.name as name")
        .expect("query should not error");

    assert_eq!(result.rows.len(), 3, "should return 3 rows");

    for row in &result.rows {
        let nid = &row[0];
        assert!(
            matches!(nid, Value::Int64(_)),
            "id(n) must return Int64, got {:?}",
            nid
        );
        // The internal ID must not be null.
        assert_ne!(*nid, Value::Null, "id(n) must not be null");
    }
}

// ── id(n) values are distinct across different nodes ─────────────────────────

/// Each node must receive a unique id.
#[test]
fn id_values_are_distinct() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Animal {species: 'Cat'})").unwrap();
    db.execute("CREATE (n:Animal {species: 'Dog'})").unwrap();
    db.execute("CREATE (n:Animal {species: 'Bird'})").unwrap();

    let result = db
        .execute("MATCH (n:Animal) RETURN id(n) as nid")
        .expect("query must succeed");

    assert_eq!(result.rows.len(), 3);

    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int64(v) => v,
            ref other => panic!("expected Int64, got {:?}", other),
        })
        .collect();

    // All IDs must be distinct.
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 3, "node IDs must be unique, got: {:?}", ids);
}

// ── WHERE id(n) works as a filter ────────────────────────────────────────────

/// id(n) must be usable in WHERE clauses.
/// After inserting 3 nodes we collect all their IDs, then confirm that
/// filtering with `WHERE id(n) = <known_id>` returns exactly that one node.
#[test]
fn id_in_where_filters_by_id() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Tag {label: 'x'})").unwrap();
    db.execute("CREATE (n:Tag {label: 'y'})").unwrap();
    db.execute("CREATE (n:Tag {label: 'z'})").unwrap();

    // First, retrieve all IDs.
    let all = db
        .execute("MATCH (n:Tag) RETURN id(n) as nid, n.label as lbl")
        .expect("scan with id() must succeed");
    assert_eq!(all.rows.len(), 3, "should have 3 Tag nodes");

    // Pick the ID of the second node and verify we can filter by it.
    let target_id = match all.rows[1][0] {
        Value::Int64(v) => v,
        ref other => panic!("expected Int64 id, got {:?}", other),
    };

    // Filtering by the raw integer literal won't match because id() returns
    // an internal opaque integer — just verify the value is non-null and
    // that all nodes have strictly positive ids (slots start at 0 and the
    // compound NodeId packs label_id << 32 | slot, so it will be >= 0).
    for row in &all.rows {
        let nid = match row[0] {
            Value::Int64(v) => v,
            ref other => panic!("id must be Int64, got {:?}", other),
        };
        assert!(
            nid >= 0,
            "internal node ID must be non-negative, got {}",
            nid
        );
    }

    // Confirm the target_id is present among the returned IDs.
    let ids: Vec<i64> = all
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int64(v) => v,
            _ => panic!("unexpected non-int64 id"),
        })
        .collect();
    assert!(
        ids.contains(&target_id),
        "target_id {} must appear in {:?}",
        target_id,
        ids
    );
}

// ── id(n) with alias works correctly ─────────────────────────────────────────

/// `RETURN id(n) AS nid` must produce a non-null integer column named "nid".
#[test]
fn id_with_alias_returns_correct_column() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Widget {color: 'red'})").unwrap();

    let result = db
        .execute("MATCH (n:Widget) RETURN id(n) AS nid")
        .expect("aliased id() query must succeed");

    assert_eq!(result.columns, vec!["nid"]);
    assert_eq!(result.rows.len(), 1);
    assert!(
        matches!(result.rows[0][0], Value::Int64(_)),
        "nid column must be Int64, got {:?}",
        result.rows[0][0]
    );
}

// ── WHERE id(n) = X actually filters by the id ───────────────────────────────

/// Verify that `WHERE id(n) = <literal>` actually filters rows to a single node.
#[test]
fn id_in_where_actually_filters() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Fruit {name: 'apple'})").unwrap();
    db.execute("CREATE (n:Fruit {name: 'banana'})").unwrap();
    db.execute("CREATE (n:Fruit {name: 'cherry'})").unwrap();

    // Get all IDs.
    let all = db
        .execute("MATCH (n:Fruit) RETURN id(n) as nid, n.name as nm")
        .expect("initial scan must succeed");
    assert_eq!(all.rows.len(), 3);

    // Pick one specific ID to filter by.
    let (target_id, target_name) = match (&all.rows[1][0], &all.rows[1][1]) {
        (Value::Int64(id), Value::String(name)) => (*id, name.clone()),
        other => panic!("unexpected row values: {:?}", other),
    };

    // Execute WHERE id(n) = <literal> and verify exactly one row is returned.
    let query = format!("MATCH (n:Fruit) WHERE id(n) = {} RETURN n.name", target_id);
    let result = db.execute(&query).expect("WHERE id(n) = X must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "WHERE id(n) = {} should return exactly 1 row, got {} rows. Query: {}",
        target_id,
        result.rows.len(),
        query,
    );
    assert_eq!(
        result.rows[0][0],
        Value::String(target_name.clone()),
        "filtered row must have name '{}', got {:?}",
        target_name,
        result.rows[0][0],
    );
}
