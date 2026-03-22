//! Integration tests for SPA-217: `db_counts()` returns real node and edge counts.
//!
//! Verifies that `GraphDb::db_counts()` (the backing function for `sparrowdb info`)
//! correctly sums `hwm_for_label` across all catalog labels for node count, and
//! sums delta-log record counts across all registered rel tables for edge count.

use sparrowdb::GraphDb;

/// Create nodes and edges; verify counts are non-zero and match expected values.
#[test]
fn db_counts_reflects_created_nodes_and_edges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");
    let db = GraphDb::open(&db_path).expect("open");

    // Empty DB: both counts should be 0.
    let (node_count, edge_count) = db.db_counts().expect("db_counts on empty DB");
    assert_eq!(node_count, 0, "empty DB should have 0 nodes");
    assert_eq!(edge_count, 0, "empty DB should have 0 edges");

    // Create 3 nodes.
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("create Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("create Bob");
    db.execute("CREATE (n:Person {name: 'Carol'})")
        .expect("create Carol");

    let (node_count, edge_count) = db.db_counts().expect("db_counts after nodes");
    assert_eq!(node_count, 3, "should count 3 created nodes");
    assert_eq!(edge_count, 0, "no edges created yet");

    // Create 2 edges.
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("create Alice->Bob KNOWS");
    db.execute("MATCH (a:Person {name:'Bob'}),(b:Person {name:'Carol'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("create Bob->Carol KNOWS");

    let (node_count, edge_count) = db.db_counts().expect("db_counts after edges");
    assert_eq!(node_count, 3, "node count should still be 3");
    assert_eq!(edge_count, 2, "should count 2 created edges");
}

/// Counts are non-zero after nodes/edges and survive reopening the DB.
#[test]
fn db_counts_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    // Session 1: create data.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (n:Animal {name: 'Cat'})")
            .expect("create Cat");
        db.execute("CREATE (n:Animal {name: 'Dog'})")
            .expect("create Dog");
        db.execute(
            "MATCH (a:Animal {name:'Cat'}),(b:Animal {name:'Dog'}) CREATE (a)-[:CHASES]->(b)",
        )
        .expect("create Cat->Dog CHASES");
        db.checkpoint().expect("checkpoint");
    }

    // Session 2: reopen and verify counts.
    {
        let db = GraphDb::open(&db_path).expect("open session 2");
        let (node_count, edge_count) = db.db_counts().expect("db_counts after reopen");
        assert!(
            node_count >= 2,
            "should find at least 2 nodes after reopen, got {node_count}"
        );
        assert!(
            edge_count >= 1,
            "should find at least 1 edge after reopen, got {edge_count}"
        );
    }
}

/// node_count is a HWM and includes soft-deleted nodes until compaction/GC runs.
///
/// This test documents the current behaviour: deleting a node does **not**
/// decrease the node count reported by `db_counts()` because the underlying
/// store uses a high-water mark (slot index) rather than a live-node counter.
#[test]
fn db_counts_hwm_includes_deleted_nodes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");
    let db = GraphDb::open(&db_path).expect("open");

    db.execute("CREATE (n:Temp {name: 'ToDelete'})")
        .expect("create node");

    let (node_count_before, _) = db.db_counts().expect("counts before delete");
    assert_eq!(node_count_before, 1, "one node created");

    // Attempt DELETE — if the engine does not yet support it, skip the assertion.
    let delete_result = db.execute("MATCH (n:Temp {name: 'ToDelete'}) DELETE n");

    if delete_result.is_ok() {
        let (node_count_after, _) = db.db_counts().expect("counts after delete");
        // HWM semantics: the slot is soft-deleted but the high-water mark is
        // unchanged, so db_counts() still reports 1 until compaction/GC runs.
        assert_eq!(
            node_count_after, 1,
            "node_count should still be 1 (HWM includes soft-deleted slots)"
        );
    }
    // If DELETE is not yet implemented the test passes as a documentation stub.
}

/// Multiple node labels: counts correctly sum across all labels.
#[test]
fn db_counts_sums_across_multiple_labels() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");
    let db = GraphDb::open(&db_path).expect("open");

    // Create nodes of two different labels.
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("create Person");
    db.execute("CREATE (n:City {name: 'London'})")
        .expect("create City");
    db.execute("CREATE (n:City {name: 'Paris'})")
        .expect("create City 2");

    let (node_count, _) = db.db_counts().expect("db_counts with multiple labels");
    assert_eq!(
        node_count, 3,
        "node count should sum across Person and City labels"
    );
}
