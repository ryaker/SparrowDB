//! Regression tests for SPA-165: property names starting with `col_` must not
//! collide with the internal column-file naming scheme.
//!
//! The storage engine persists each property column as `col_{col_id}.bin` where
//! `col_id` is the FNV-1a hash of the property name.  A previous bug in the
//! execution engine's `prop_name_to_col_id` helper caused any property name
//! that started with `col_` but whose suffix did not parse as a `u32` (e.g.
//! `col_id`, `col_name`) to silently resolve to column 0 instead of the
//! correct FNV-1a hash, resulting in the wrong value being read back.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: `col_id` property round-trip ─────────────────────────────────────

/// A property literally named `col_id` must store and retrieve its value
/// correctly.  Before SPA-165 was fixed, reading `n.col_id` would silently
/// resolve to column 0 (the tombstone sentinel column) and return `Null`.
#[test]
fn col_prefixed_prop_stores_and_retrieves() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Thing {col_id: 42})").unwrap();

    let result = db
        .execute("MATCH (n:Thing) RETURN n.col_id")
        .expect("MATCH with col_id property must not error");

    assert_eq!(result.rows.len(), 1, "expected one matching node");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(42),
        "n.col_id must return 42, not Null or 0"
    );
}

// ── Test 2: `col_0` property round-trip ──────────────────────────────────────

/// A property literally named `col_0` must be stored via its FNV-1a hash and
/// retrieved correctly.  The name `col_0` looks like an internal column
/// reference but is a valid user property key with its own hash distinct from
/// column 0.
#[test]
fn col_zero_prop_roundtrip() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Slot {col_0: 99})").unwrap();

    let result = db
        .execute("MATCH (n:Slot) RETURN n.col_0")
        .expect("MATCH with col_0 property must not error");

    assert_eq!(result.rows.len(), 1, "expected one matching node");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(99),
        "n.col_0 must return 99"
    );
}

// ── Test 3: multiple `col_`-prefixed properties ───────────────────────────────

/// Multiple properties with `col_`-prefixed names must all be independently
/// stored and retrieved without cross-contamination.
#[test]
fn multiple_col_props() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Widget {col_id: 1, col_name: 'test'})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Widget) RETURN n.col_id, n.col_name")
        .expect("MATCH with multiple col_ properties must not error");

    assert_eq!(result.rows.len(), 1, "expected one matching node");
    assert_eq!(result.rows[0][0], Value::Int64(1), "n.col_id must return 1");
    assert_eq!(
        result.rows[0][1],
        Value::String("test".to_string()),
        "n.col_name must return 'test'"
    );
}

// ── Test 4: `col_`-prefixed name in WHERE clause ──────────────────────────────

/// A WHERE predicate on a `col_`-prefixed property must filter correctly.
#[test]
fn col_prefixed_prop_in_where() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Item {col_id: 10})").unwrap();
    db.execute("CREATE (n:Item {col_id: 20})").unwrap();

    let result = db
        .execute("MATCH (n:Item) WHERE n.col_id = 10 RETURN n.col_id")
        .expect("WHERE on col_id must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "WHERE n.col_id = 10 must match exactly one node"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(10),
        "returned n.col_id must be 10"
    );
}
