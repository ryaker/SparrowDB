//! Regression tests for issue #379: `DETACH DELETE` parse error.
//!
//! `MATCH (n:Person {name: 'Alice'}) DETACH DELETE n` previously caused a
//! parse error because `Token::Detach` was lexed but never consumed by the
//! parser.  This suite verifies:
//!
//! 1. `DETACH DELETE` parses and executes without error.
//! 2. The deleted node is gone after `DETACH DELETE`.
//! 3. All incident edges are removed as part of `DETACH DELETE`.
//! 4. Plain `DELETE` on a node with edges still returns `NodeHasEdges`.

use sparrowdb::{open, GraphDb};

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn count_matches(db: &GraphDb, cypher: &str) -> usize {
    db.execute(cypher).expect("execute").rows.len()
}

// ── Parser: DETACH DELETE is no longer a parse error ─────────────────────────

/// `MATCH (n:Person {name: 'Alice'}) DETACH DELETE n` must parse and execute
/// without returning an error (the exact query from the bug report).
#[test]
fn detach_delete_parses_without_error() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    let result = db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n");
    assert!(
        result.is_ok(),
        "DETACH DELETE must not return a parse error — issue #379: {result:?}"
    );
}

// ── Node removal ──────────────────────────────────────────────────────────────

/// After `DETACH DELETE`, the node must no longer be returned by MATCH.
#[test]
fn detach_delete_removes_node() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Alice'}) RETURN n.name"),
        1,
        "Alice must exist before DETACH DELETE"
    );

    db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .expect("DETACH DELETE must succeed");

    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Alice'}) RETURN n.name"),
        0,
        "Alice must not exist after DETACH DELETE"
    );
}

// ── Edge removal ──────────────────────────────────────────────────────────────

/// `DETACH DELETE` on a node that has an outgoing edge must also remove that
/// edge so that the edge is no longer visible via MATCH.
#[test]
fn detach_delete_removes_outgoing_edge() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        1,
        "KNOWS edge must exist before DETACH DELETE"
    );

    db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .expect("DETACH DELETE must succeed even when node has edges");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        0,
        "KNOWS edge must be gone after DETACH DELETE of the source node"
    );

    // Bob must still be present.
    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Bob'}) RETURN n.name"),
        1,
        "Bob must still exist after Alice is DETACH DELETEd"
    );
}

/// `DETACH DELETE` on a node that has an incoming edge must also remove that
/// edge so that it is no longer visible via MATCH.
#[test]
fn detach_delete_removes_incoming_edge() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // Delete the destination node (Bob).
    db.execute("MATCH (n:Person {name: 'Bob'}) DETACH DELETE n")
        .expect("DETACH DELETE on the destination node must succeed");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        0,
        "KNOWS edge must be gone after DETACH DELETE of the destination node"
    );

    // Alice must still be present.
    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Alice'}) RETURN n.name"),
        1,
        "Alice must still exist after Bob is DETACH DELETEd"
    );
}

/// `DETACH DELETE` removes all edges regardless of direction when the node
/// participates in multiple relationships.
#[test]
fn detach_delete_removes_multiple_edges() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Carol'})").unwrap();

    // Alice → Bob and Carol → Alice.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Person {name: 'Carol'}), (a:Person {name: 'Alice'}) CREATE (c)-[:KNOWS]->(a)",
    )
    .unwrap();

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        2,
        "two KNOWS edges must exist before DETACH DELETE"
    );

    db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .expect("DETACH DELETE must succeed when node has both incoming and outgoing edges");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        0,
        "all KNOWS edges incident to Alice must be gone after DETACH DELETE"
    );
}

// ── WHERE predicate variant ───────────────────────────────────────────────────

/// `MATCH (n:Person) WHERE n.name = 'Alice' DETACH DELETE n` — DETACH DELETE
/// with a WHERE clause must also work.
#[test]
fn detach_delete_with_where_clause() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    db.execute("MATCH (n:Person) WHERE n.name = 'Alice' DETACH DELETE n")
        .expect("DETACH DELETE with WHERE clause must succeed");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        0,
        "edge must be gone after DETACH DELETE with WHERE clause"
    );
    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Alice'}) RETURN n.name"),
        0,
        "Alice must be gone after DETACH DELETE with WHERE clause"
    );
    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Bob'}) RETURN n.name"),
        1,
        "Bob must still exist"
    );
}

// ── Post-CHECKPOINT CSR code path ────────────────────────────────────────────

/// `DETACH DELETE` must also work after CHECKPOINT when edges are in CSR files
/// rather than the delta log.
#[test]
fn detach_delete_after_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // Move edges from delta log to CSR files.
    db.execute("CHECKPOINT").unwrap();

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        1,
        "KNOWS edge must exist after CHECKPOINT"
    );

    db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .expect("DETACH DELETE must succeed on checkpointed edges");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        0,
        "KNOWS edge must be gone after DETACH DELETE of checkpointed source"
    );

    // Bob must still be present.
    assert_eq!(
        count_matches(&db, "MATCH (n:Person {name: 'Bob'}) RETURN n.name"),
        1,
        "Bob must still exist after Alice is DETACH DELETEd"
    );
}

// ── Documented behavior: plain DELETE fails on node with edges ────────────────

/// Plain `DELETE` on a node that still has edges must return an error.
///
/// This documents the existing behaviour: users must use `DETACH DELETE` to
/// remove a connected node, or manually delete the edges first.
#[test]
fn plain_delete_on_node_with_edges_returns_error() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db.execute("MATCH (n:Person {name: 'Alice'}) DELETE n");
    assert!(
        result.is_err(),
        "plain DELETE on a node with edges must return NodeHasEdges error"
    );
}
