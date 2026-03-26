//! Performance regression test for the O(1) MATCH property filter (issue #230).
//!
//! Before the fix, `MATCH (a:User {uid: X}), (b:User {uid: Y}) CREATE (a)-[:FRIENDS]->(b)`
//! called `read_col_all()` for every node in the label on every lookup, producing
//! O(N) work per edge insert.  With 4 039 nodes this regressed to ~9 edges/sec.
//!
//! After the fix, `scan_nodes_for_label_with_index` uses the `PropertyIndex`
//! (lazy BTree per `(label_id, col_id)`) so each point-lookup is O(log N).
//!
//! This file validates two things:
//!   1. **Correctness**: MATCH by uid returns exactly the right node.
//!   2. **Performance**: 1 000 MATCH-then-CREATE cycles over 10 000 nodes
//!      complete in < 5 000 ms on any reasonable machine (the O(N) version
//!      would take ~55 s on the benchmark hardware).

use sparrowdb::open;
use std::time::Instant;

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: correctness — MATCH by uid finds exactly the right node ─────────

#[test]
fn match_by_uid_returns_correct_node() {
    let (_dir, mut db) = make_db();

    // Insert three User nodes with distinct uid values.
    db.execute("CREATE (:User {uid: 10, name: 'Alice'})").unwrap();
    db.execute("CREATE (:User {uid: 20, name: 'Bob'})").unwrap();
    db.execute("CREATE (:User {uid: 30, name: 'Carol'})").unwrap();

    // MATCH each by uid and verify the name property.
    let r = db
        .execute("MATCH (n:User {uid: 10}) RETURN n.name")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "expected exactly one node with uid 10");
    assert_eq!(
        r.rows[0][0],
        sparrowdb_execution::types::Value::String("Alice".to_string()),
        "uid 10 should be Alice"
    );

    let r2 = db
        .execute("MATCH (n:User {uid: 20}) RETURN n.name")
        .unwrap();
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(
        r2.rows[0][0],
        sparrowdb_execution::types::Value::String("Bob".to_string()),
    );

    // uid 99 does not exist → 0 rows.
    let r3 = db
        .execute("MATCH (n:User {uid: 99}) RETURN n.name")
        .unwrap();
    assert!(r3.rows.is_empty(), "uid 99 should not exist");
}

// ── Test 2: correctness — MATCH … CREATE edge connects the right nodes ───────

#[test]
fn match_create_edge_connects_correct_nodes() {
    let (_dir, mut db) = make_db();

    db.execute("CREATE (:User {uid: 1})").unwrap();
    db.execute("CREATE (:User {uid: 2})").unwrap();
    db.execute("CREATE (:User {uid: 3})").unwrap();

    // Connect uid:1 → uid:2
    db.execute(
        "MATCH (a:User {uid: 1}), (b:User {uid: 2}) CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();

    // Verify the edge exists: uid:1 should have one outgoing FOLLOWS edge.
    let r = db
        .execute("MATCH (a:User {uid: 1})-[:FOLLOWS]->(b:User) RETURN b.uid")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "uid:1 should follow exactly one node");
    assert_eq!(
        r.rows[0][0],
        sparrowdb_execution::types::Value::Int64(2),
        "uid:1 should follow uid:2"
    );

    // uid:1 must NOT follow uid:3 (no edge was created).
    let r2 = db
        .execute("MATCH (a:User {uid: 1})-[:FOLLOWS]->(b:User {uid: 3}) RETURN b.uid")
        .unwrap();
    assert!(r2.rows.is_empty(), "uid:1 should not follow uid:3");
}

// ── Test 3: performance — 1 000 MATCH…CREATE cycles over 10 000 nodes ───────
//
// The O(N) implementation would take ~ 10_000 * 1_000 * 2 property reads each
// traversing a full column file → effectively 20 M disk reads.  On the reported
// benchmark machine this is ~55 s.  The indexed path reduces this to ~1 000
// BTree lookups.  We assert < 5 000 ms which is generous even for CI machines.

#[test]
fn match_create_edge_oi_performance() {
    let (_dir, mut db) = make_db();

    const N_NODES: i64 = 10_000;
    const N_EDGES: usize = 1_000;

    // Insert N_NODES User nodes each with a unique uid (1-based to avoid the
    // Int64(0) == ABSENT encoding sentinel that prevents uid=0 from being indexed).
    for uid in 1..=N_NODES {
        db.execute(&format!("CREATE (:User {{uid: {uid}}})",))
            .unwrap();
    }

    let start = Instant::now();

    // Create N_EDGES edges using MATCH with inline property filter.
    // Each edge connects uid:i → uid:(i+1); both values are >= 1.
    for i in 1..=(N_EDGES as i64) {
        let src = i;
        let dst = if i < N_NODES { i + 1 } else { 1 };
        db.execute(&format!(
            "MATCH (a:User {{uid: {src}}}), (b:User {{uid: {dst}}}) CREATE (a)-[:EDGE]->(b)"
        ))
        .unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "match_create_edge_oi_performance: {} edges in {:?} ({:.1} edges/sec)",
        N_EDGES,
        elapsed,
        N_EDGES as f64 / elapsed.as_secs_f64()
    );

    // Correctness spot-check: uid:1 should have exactly one outgoing EDGE.
    let r = db
        .execute("MATCH (a:User {uid: 1})-[:EDGE]->(b) RETURN b.uid")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "uid:1 should have exactly one EDGE");

    // Performance assertion: release builds must be fast; debug builds get a
    // generous ceiling just to catch catastrophic O(N²) regressions.
    //
    // macOS note: each WriteTx commit includes a WAL fsync (~13 ms on macOS
    // SSDs due to stricter flush guarantees).  With 1 000 edge-create commits
    // the fsync overhead alone totals ~13 s, regardless of how fast the index
    // lookup is.  The O(N) scan baseline on macOS is ~55 s+, so 30 s is
    // comfortably between "indexed O(log N) + fsync" and "O(N) scan".
    // On Linux CI, fsync takes ~1 ms, so the 5 s limit remains appropriate.
    let limit_ms: u128 = if cfg!(debug_assertions) {
        60_000
    } else if cfg!(target_os = "macos") {
        30_000
    } else {
        5_000
    };
    assert!(
        elapsed.as_millis() < limit_ms,
        "Performance regression: {N_EDGES} MATCH…CREATE cycles over {N_NODES} nodes took {:?} — expected < {}ms. O(N) scan may have regressed.",
        elapsed,
        limit_ms
    );
}
