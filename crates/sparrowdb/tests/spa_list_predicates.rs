//! E2E tests for ANY/ALL/NONE/SINGLE list predicate functions.
//!
//! These predicates iterate over a list value and apply a filter predicate,
//! returning a boolean result:
//!
//!   ANY(x IN list WHERE predicate)    — true if at least one element matches
//!   ALL(x IN list WHERE predicate)    — true if every element matches
//!   NONE(x IN list WHERE predicate)   — true if no element matches
//!   SINGLE(x IN list WHERE predicate) — true if exactly one element matches
//!
//! Tests exercise two patterns:
//!   1. Inline list literals: RETURN ANY(x IN ['a','b','c'] WHERE x = 'a')
//!   2. collect() aggregation: MATCH … RETURN ANY(x IN collect(t.name) WHERE x = 'graph')

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: any_matches — collect() ───────────────────────────────────────────

/// ANY wrapping collect() returns true when at least one element satisfies the predicate.
#[test]
fn any_matches() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Tag {name: 'graph'})")
        .expect("CREATE graph");
    db.execute("CREATE (n:Tag {name: 'database'})")
        .expect("CREATE database");
    db.execute("CREATE (n:Tag {name: 'rust'})")
        .expect("CREATE rust");

    let result = db
        .execute("MATCH (t:Tag) RETURN ANY(x IN collect(t.name) WHERE x = 'graph') AS has_graph")
        .expect("ANY predicate must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "ANY(x IN collect(t.name) WHERE x = 'graph') must be true; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 1b: any_inline_list — inline list literal ────────────────────────────

/// ANY on an inline list literal returns true when at least one element matches.
#[test]
fn any_inline_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute("RETURN ANY(x IN ['a', 'b', 'c'] WHERE x = 'b') AS found")
        .expect("ANY with inline list must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "ANY(x IN ['a','b','c'] WHERE x = 'b') must be true; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 1c: all_inline_list — inline list literal ────────────────────────────

/// ALL on an inline list literal returns true when every element satisfies predicate.
#[test]
fn all_inline_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute("RETURN ALL(x IN [2, 4, 6] WHERE x > 1) AS all_positive")
        .expect("ALL with inline list must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "ALL(x IN [2,4,6] WHERE x > 1) must be true; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 1d: none_inline_list — inline list literal ───────────────────────────

/// NONE on an inline list literal returns true when no element matches.
#[test]
fn none_inline_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute("RETURN NONE(x IN [1, 2, 3] WHERE x = 99) AS none_99")
        .expect("NONE with inline list must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "NONE(x IN [1,2,3] WHERE x = 99) must be true; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 1e: single_inline_list — inline list literal ─────────────────────────

/// SINGLE on an inline list literal returns true when exactly one element matches.
#[test]
fn single_inline_list() {
    let (_dir, db) = make_db();

    let result = db
        .execute("RETURN SINGLE(x IN [1, 2, 3] WHERE x = 2) AS exactly_one")
        .expect("SINGLE with inline list must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "SINGLE(x IN [1,2,3] WHERE x = 2) must be true; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 2: any_no_match — collect() ──────────────────────────────────────────

/// ANY returns false when no element satisfies the predicate.
#[test]
fn any_no_match() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Tag {name: 'graph'})")
        .expect("CREATE graph");
    db.execute("CREATE (n:Tag {name: 'database'})")
        .expect("CREATE database");

    let result = db
        .execute("MATCH (t:Tag) RETURN ANY(x IN collect(t.name) WHERE x = 'missing') AS found")
        .expect("ANY predicate (no match) must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(false),
        "ANY on no-match must be false; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 3: all_matches — collect() ───────────────────────────────────────────

/// ALL returns true when every element satisfies the predicate.
#[test]
fn all_matches() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Score {value: 90})")
        .expect("CREATE 90");
    db.execute("CREATE (n:Score {value: 85})")
        .expect("CREATE 85");
    db.execute("CREATE (n:Score {value: 95})")
        .expect("CREATE 95");

    let result = db
        .execute("MATCH (s:Score) RETURN ALL(x IN collect(s.value) WHERE x > 80) AS all_above_80")
        .expect("ALL predicate must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "ALL(x IN scores WHERE x > 80) must be true; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 4: all_fails — collect() ─────────────────────────────────────────────

/// ALL returns false when at least one element does not satisfy the predicate.
#[test]
fn all_fails() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Score {value: 90})")
        .expect("CREATE 90");
    db.execute("CREATE (n:Score {value: 70})")
        .expect("CREATE 70 (below threshold)");
    db.execute("CREATE (n:Score {value: 95})")
        .expect("CREATE 95");

    let result = db
        .execute("MATCH (s:Score) RETURN ALL(x IN collect(s.value) WHERE x > 80) AS all_above_80")
        .expect("ALL predicate (fail case) must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(false),
        "ALL(x > 80) must be false when 70 is in list; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 5: none_matches — collect() ─────────────────────────────────────────

/// NONE returns true when no element satisfies the predicate.
#[test]
fn none_matches() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Item {name: 'alpha'})")
        .expect("CREATE alpha");
    db.execute("CREATE (n:Item {name: 'beta'})")
        .expect("CREATE beta");
    db.execute("CREATE (n:Item {name: 'gamma'})")
        .expect("CREATE gamma");

    let result = db
        .execute(
            "MATCH (i:Item) RETURN NONE(x IN collect(i.name) WHERE x = 'deleted') AS none_deleted",
        )
        .expect("NONE predicate must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "NONE(x = 'deleted') must be true when no item is named 'deleted'; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 6: none_fails — collect() ───────────────────────────────────────────

/// NONE returns false when at least one element satisfies the predicate.
#[test]
fn none_fails() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Item {name: 'alpha'})")
        .expect("CREATE alpha");
    db.execute("CREATE (n:Item {name: 'deleted'})")
        .expect("CREATE deleted");

    let result = db
        .execute(
            "MATCH (i:Item) RETURN NONE(x IN collect(i.name) WHERE x = 'deleted') AS none_deleted",
        )
        .expect("NONE predicate (fail case) must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(false),
        "NONE(x = 'deleted') must be false when 'deleted' is in list; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 7: any_on_empty_list ─────────────────────────────────────────────────

/// ANY on an empty list returns false (no element can satisfy the predicate).
///
/// Uses a prop filter that matches nothing so collect() produces an empty list.
#[test]
fn any_on_empty_list() {
    let (_dir, db) = make_db();

    // Create a node so the label exists, but use a prop filter that matches nothing.
    db.execute("CREATE (n:Empty {val: 999})")
        .expect("CREATE dummy");

    // Prop filter {val: 0} matches nothing → collect() produces [] → ANY = false.
    let result = db
        .execute("MATCH (e:Empty {val: 0}) RETURN ANY(x IN collect(e.val) WHERE x = 0) AS found")
        .expect("ANY on empty list must succeed");

    // With no rows matching the prop filter, aggregate_rows returns one row with empty list.
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(false),
        "ANY on empty list must be false; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 8: all_on_empty_list ─────────────────────────────────────────────────

/// ALL on an empty list returns true (vacuous truth: no element fails the predicate).
#[test]
fn all_on_empty_list() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Empty {val: 999})")
        .expect("CREATE dummy");

    let result = db
        .execute(
            "MATCH (e:Empty {val: 0}) RETURN ALL(x IN collect(e.val) WHERE x > 100) AS all_big",
        )
        .expect("ALL on empty list must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "ALL on empty list must be true (vacuous truth); got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 9: single_matches — collect() ───────────────────────────────────────

/// SINGLE returns true when exactly one element satisfies the predicate.
#[test]
fn single_matches() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");

    let result = db
        .execute(
            "MATCH (p:Person) RETURN SINGLE(x IN collect(p.name) WHERE x = 'Alice') AS one_alice",
        )
        .expect("SINGLE predicate must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "SINGLE(x = 'Alice') must be true when exactly one Alice exists; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 10: single_fails_multiple ────────────────────────────────────────────

/// SINGLE returns false when more than one element satisfies the predicate.
#[test]
fn single_fails_multiple() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice 1");
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice 2");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = db
        .execute(
            "MATCH (p:Person) RETURN SINGLE(x IN collect(p.name) WHERE x = 'Alice') AS one_alice",
        )
        .expect("SINGLE predicate (multiple matches) must succeed");

    assert_eq!(result.rows.len(), 1, "expected 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Bool(false),
        "SINGLE(x = 'Alice') must be false when two Alices exist; got: {:?}",
        result.rows[0][0]
    );
}
