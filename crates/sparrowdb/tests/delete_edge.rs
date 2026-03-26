//! `delete_edge` API acceptance tests.
//!
//! Verifies that `WriteTx::delete_edge` and `GraphDb::delete_edge` correctly
//! remove a specific directed edge and that subsequent MATCH queries no longer
//! return it.
//!
//! Unblocks `SparrowOntology::init(force=true)` which needs to remove edges
//! before re-seeding the ontology graph.

use sparrowdb::{open, GraphDb};
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Helpers: count how many rows a Cypher MATCH returns.
fn count_matches(db: &GraphDb, cypher: &str) -> usize {
    db.execute(cypher).expect("execute").rows.len()
}

// ── WriteTx::delete_edge ──────────────────────────────────────────────────────

/// Create two nodes and an edge, delete the edge, verify it is gone.
#[test]
fn delete_edge_removes_from_delta() {
    let (dir, db) = make_db();

    // label_id=0 keeps packed NodeIds equal to their slot numbers.
    let (a, b) = {
        let mut tx = db.begin_write().unwrap();
        let a = tx.create_node(0, &[]).unwrap();
        let b = tx.create_node(0, &[]).unwrap();
        tx.commit().unwrap();
        (a, b)
    };

    // Create edge a → b with type "KNOWS".
    {
        let mut tx = db.begin_write().unwrap();
        tx.create_edge(a, b, "KNOWS", HashMap::new())
            .expect("create_edge");
        tx.commit().unwrap();
    }

    // Verify the edge is visible via Cypher.
    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        1,
        "edge should exist before deletion"
    );

    // Delete the edge via WriteTx.
    {
        let mut tx = db.begin_write().unwrap();
        tx.delete_edge(a, b, "KNOWS").expect("delete_edge");
        tx.commit().unwrap();
    }

    // The edge must no longer be returned by MATCH.
    assert_eq!(
        count_matches(&db, "MATCH (x)-[:KNOWS]->(y) RETURN x, y"),
        0,
        "edge must be gone after delete_edge"
    );

    drop(dir);
}

// ── GraphDb::delete_edge (convenience wrapper) ────────────────────────────────

/// Verify the one-shot GraphDb::delete_edge wrapper works end-to-end.
#[test]
fn graphdb_delete_edge_wrapper() {
    let (dir, db) = make_db();

    let (a, b) = {
        let mut tx = db.begin_write().unwrap();
        let a = tx.create_node(0, &[]).unwrap();
        let b = tx.create_node(0, &[]).unwrap();
        tx.commit().unwrap();
        (a, b)
    };

    {
        let mut tx = db.begin_write().unwrap();
        tx.create_edge(a, b, "LINKED", HashMap::new())
            .expect("create_edge");
        tx.commit().unwrap();
    }

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:LINKED]->(y) RETURN x, y"),
        1,
        "edge should exist before deletion"
    );

    // Use the convenience wrapper.
    db.delete_edge(a, b, "LINKED")
        .expect("GraphDb::delete_edge");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:LINKED]->(y) RETURN x, y"),
        0,
        "edge must be gone after GraphDb::delete_edge"
    );

    drop(dir);
}

// ── Multiple edges — only the targeted one is removed ─────────────────────────

/// Create three edges and delete only the middle one.
#[test]
fn delete_edge_leaves_others_intact() {
    let (dir, db) = make_db();

    let (a, b, c) = {
        let mut tx = db.begin_write().unwrap();
        let a = tx.create_node(0, &[]).unwrap();
        let b = tx.create_node(0, &[]).unwrap();
        let c = tx.create_node(0, &[]).unwrap();
        tx.commit().unwrap();
        (a, b, c)
    };

    // Create a→b and b→c with "FOLLOWS".
    {
        let mut tx = db.begin_write().unwrap();
        tx.create_edge(a, b, "FOLLOWS", HashMap::new())
            .expect("a→b");
        tx.create_edge(b, c, "FOLLOWS", HashMap::new())
            .expect("b→c");
        tx.commit().unwrap();
    }

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:FOLLOWS]->(y) RETURN x, y"),
        2,
        "both edges should exist"
    );

    // Delete only a→b.
    db.delete_edge(a, b, "FOLLOWS").expect("delete a→b");

    assert_eq!(
        count_matches(&db, "MATCH (x)-[:FOLLOWS]->(y) RETURN x, y"),
        1,
        "only b→c should remain"
    );

    drop(dir);
}

// ── Error cases ───────────────────────────────────────────────────────────────

/// Deleting a non-existent rel type returns an error.
#[test]
fn delete_edge_unknown_rel_type_errors() {
    let (dir, db) = make_db();

    let (a, b) = {
        let mut tx = db.begin_write().unwrap();
        let a = tx.create_node(0, &[]).unwrap();
        let b = tx.create_node(0, &[]).unwrap();
        tx.commit().unwrap();
        (a, b)
    };

    // No edge created — rel type never registered.
    let mut tx = db.begin_write().unwrap();
    let result = tx.delete_edge(a, b, "NONEXISTENT");
    assert!(
        result.is_err(),
        "expected error for unknown rel type, got Ok"
    );

    drop(dir);
}

// ── Cypher MATCH...DELETE r ───────────────────────────────────────────────────

/// `MATCH (a:P)-[r:KNOWS]->(b:P) DELETE r` removes the edge.
#[test]
fn cypher_match_delete_rel_removes_edge() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name:\"Alice\"})-[:KNOWS]->(b:Person {name:\"Bob\"})")
        .expect("create");

    assert_eq!(
        count_matches(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"),
        1,
        "edge should exist before Cypher DELETE"
    );

    db.execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r")
        .expect("delete");

    assert_eq!(
        count_matches(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"),
        0,
        "edge must be gone after Cypher DELETE r"
    );
}

/// `MATCH (a)-[r:REL]->(b) DELETE r` with inline prop filter on src node.
#[test]
fn cypher_match_delete_rel_with_src_prop_filter() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Item {id:1})-[:LINKED]->(b:Item {id:2})")
        .expect("create 1->2");
    db.execute("CREATE (a:Item {id:3})-[:LINKED]->(b:Item {id:4})")
        .expect("create 3->4");

    assert_eq!(
        count_matches(&db, "MATCH (:Item)-[:LINKED]->(:Item) RETURN 1"),
        2,
        "two edges before delete"
    );

    // Only delete the edge starting from Item {id:1}.
    db.execute("MATCH (a:Item {id:1})-[r:LINKED]->(b:Item) DELETE r")
        .expect("delete with src filter");

    assert_eq!(
        count_matches(&db, "MATCH (:Item)-[:LINKED]->(:Item) RETURN 1"),
        1,
        "only one edge should remain after targeted delete"
    );
}

/// Cypher edge delete survives CHECKPOINT.
#[test]
fn cypher_match_delete_rel_after_checkpoint() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:N {x:1})-[:E]->(b:N {x:2})")
        .expect("create");
    db.checkpoint().expect("checkpoint");

    assert_eq!(
        count_matches(&db, "MATCH (:N)-[:E]->(:N) RETURN 1"),
        1,
        "edge visible after checkpoint"
    );

    db.execute("MATCH (a:N)-[r:E]->(b:N) DELETE r")
        .expect("delete after checkpoint");

    assert_eq!(
        count_matches(&db, "MATCH (:N)-[:E]->(:N) RETURN 1"),
        0,
        "edge gone after Cypher DELETE r post-checkpoint"
    );
}
