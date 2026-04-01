//! Regression tests for issue #380 — ON CREATE SET / ON MATCH SET in MERGE.
//!
//! Verifies that:
//!   - `ON CREATE SET` fires when the node is newly created.
//!   - `ON MATCH SET` fires when the node already exists.
//!   - Neither clause fires on the wrong branch.
//!
//! Note: the storage layer has no Bool type; `true` is stored and returned as
//! `Value::Int64(1)` and `false` as `Value::Int64(0)`.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

/// Shorthand: the execution-layer representation of Cypher `true`.
const TRUE: Value = Value::Int64(1);

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── ON CREATE SET fires on a new node ────────────────────────────────────────

#[test]
fn on_create_set_fires_when_node_is_new() {
    let (_dir, db) = make_db();

    db.execute("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true")
        .expect("MERGE ON CREATE SET must not fail");

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.created")
        .expect("MATCH must not fail");

    assert_eq!(result.rows.len(), 1, "exactly one Person node should exist");
    assert_eq!(
        result.rows[0][0], TRUE,
        "n.created should be true after ON CREATE SET"
    );
}

// ── ON MATCH SET fires when the node already exists ───────────────────────────

#[test]
fn on_match_set_fires_when_node_exists() {
    let (_dir, db) = make_db();

    // First MERGE — creates the node.
    db.execute("MERGE (n:Person {name: 'Alice'})")
        .expect("first MERGE must succeed");

    // Second MERGE — node exists, ON MATCH SET should fire.
    db.execute("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.seen = true")
        .expect("MERGE ON MATCH SET must not fail");

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.seen")
        .expect("MATCH must not fail");

    assert_eq!(result.rows.len(), 1, "exactly one Person node should exist");
    assert_eq!(
        result.rows[0][0], TRUE,
        "n.seen should be true after ON MATCH SET"
    );
}

// ── ON CREATE SET does NOT fire on an existing node ──────────────────────────

#[test]
fn on_create_set_does_not_fire_on_existing_node() {
    let (_dir, db) = make_db();

    // Create the node first.
    db.execute("MERGE (n:Person {name: 'Bob'})")
        .expect("initial MERGE must succeed");

    // Second MERGE — node exists; ON CREATE SET must NOT set n.created.
    db.execute("MERGE (n:Person {name: 'Bob'}) ON CREATE SET n.created = true")
        .expect("MERGE ON CREATE SET on existing node must not fail");

    let result = db
        .execute("MATCH (n:Person {name: 'Bob'}) RETURN n.created")
        .expect("MATCH must not fail");

    assert_eq!(result.rows.len(), 1, "exactly one node should exist");
    // n.created was never set, so it should be Null.
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "n.created must remain Null when ON CREATE SET is skipped"
    );
}

// ── ON MATCH SET does NOT fire on a newly created node ───────────────────────

#[test]
fn on_match_set_does_not_fire_on_new_node() {
    let (_dir, db) = make_db();

    // Node does not exist — ON MATCH SET must NOT fire.
    db.execute("MERGE (n:Person {name: 'Carol'}) ON MATCH SET n.seen = true")
        .expect("MERGE ON MATCH SET on new node must not fail");

    let result = db
        .execute("MATCH (n:Person {name: 'Carol'}) RETURN n.seen")
        .expect("MATCH must not fail");

    assert_eq!(result.rows.len(), 1, "exactly one node should exist");
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "n.seen must remain Null when ON MATCH SET is skipped for new node"
    );
}

// ── Both ON CREATE SET and ON MATCH SET in the same statement ────────────────

#[test]
fn both_on_clauses_first_call_fires_create() {
    let (_dir, db) = make_db();

    db.execute(
        "MERGE (n:Person {name: 'Dave'}) \
         ON CREATE SET n.created = true \
         ON MATCH SET n.seen = true",
    )
    .expect("MERGE with both ON clauses must not fail");

    let result = db
        .execute("MATCH (n:Person {name: 'Dave'}) RETURN n.created, n.seen")
        .expect("MATCH must not fail");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0], TRUE,
        "n.created should be true on first (create) call"
    );
    assert_eq!(
        result.rows[0][1],
        Value::Null,
        "n.seen should be Null on first (create) call"
    );
}

#[test]
fn both_on_clauses_second_call_fires_match() {
    let (_dir, db) = make_db();

    // First call — creates the node.
    db.execute(
        "MERGE (n:Person {name: 'Eve'}) \
         ON CREATE SET n.created = true \
         ON MATCH SET n.seen = true",
    )
    .expect("first MERGE must not fail");

    // Second call — matches the existing node.
    db.execute(
        "MERGE (n:Person {name: 'Eve'}) \
         ON CREATE SET n.created = true \
         ON MATCH SET n.seen = true",
    )
    .expect("second MERGE must not fail");

    let result = db
        .execute("MATCH (n:Person {name: 'Eve'}) RETURN n.seen")
        .expect("MATCH must not fail");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0], TRUE,
        "n.seen should be true on second (match) call"
    );
}
