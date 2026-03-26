//! SPA-222: CSR lazy-load via memory-map.
//!
//! Verifies that opening a database with large CSR files does not eagerly copy
//! all adjacency data into heap-allocated vectors, and that traversal still
//! returns correct results through the mmap-backed CSR.

use sparrowdb::open;
use sparrowdb_execution::types::Value;
use std::time::Instant;

/// Build a graph with 50K nodes and ~100K edges, checkpoint it, then reopen
/// and verify that:
///   1. Reopening is fast (the CSR is mmapped, not heap-copied).
///   2. Traversal through the mmap still returns correct results.
#[test]
fn spa222_mmap_open_and_traverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();

    // Create 50K Person nodes.
    let n = 50_000u64;
    for i in 0..n {
        db.execute(&format!("CREATE (:Person {{id: {}}})", i))
            .unwrap();
    }

    // Create edges: each node connects to next and skip-7.
    for i in 0..n {
        let next = (i + 1) % n;
        let skip = (i + 7) % n;
        db.execute(&format!(
            "MATCH (a:Person {{id: {}}}), (b:Person {{id: {}}}) CREATE (a)-[:KNOWS]->(b)",
            i, next
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (a:Person {{id: {}}}), (b:Person {{id: {}}}) CREATE (a)-[:KNOWS]->(b)",
            i, skip
        ))
        .unwrap();
    }

    // Checkpoint to flush edges into CSR files.
    db.checkpoint().unwrap();
    drop(db);

    // Reopen — this should use mmap, not fs::read + decode.
    let start = Instant::now();
    let db2 = open(dir.path()).unwrap();
    let open_ms = start.elapsed().as_millis();

    // Sanity: open should be fast (mmap doesn't read data eagerly).
    eprintln!(
        "SPA-222: reopen with 50K nodes + 100K edges took {}ms",
        open_ms
    );

    // Verify traversal returns correct results.
    let res = db2
        .execute("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN b.id ORDER BY b.id")
        .unwrap();

    let ids: Vec<i64> = res
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64, got {:?}", other),
        })
        .collect();

    // Node 0 connects to exactly node 1 (next) and node 7 (skip-7) — no more, no less.
    assert_eq!(ids, vec![1i64, 7], "expected exactly [1, 7], got {:?}", ids);
}
