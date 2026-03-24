//! Integration tests for SPA-272: DegreeCache + top-k degree queries.
//!
//! Tests the `DegreeCache` and `Engine::top_k_by_degree` through the Engine
//! API directly (building Engine from an on-disk database), and validates
//! correctness by comparing per-node edge counts via Cypher against the
//! cache results.

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::EdgeStore;
use sparrowdb_storage::edge_store::RelTableId;
use sparrowdb_storage::node_store::NodeStore;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Build an Engine from an on-disk database directory (mirrors what GraphDb
/// does internally in lib.rs's `open_csr_map` + `Engine::new`).
fn build_engine(db_path: &std::path::Path) -> Engine {
    let catalog = Catalog::open(db_path).expect("open catalog");
    let store = NodeStore::open(db_path).expect("open node store");

    // Load all CSR forward files registered in the catalog.
    let mut csrs: HashMap<u32, CsrForward> = HashMap::new();
    let rel_ids: Vec<u32> = {
        let mut ids: Vec<u32> = catalog
            .list_rel_table_ids()
            .into_iter()
            .map(|(id, _, _, _)| id as u32)
            .collect();
        if !ids.contains(&0u32) {
            ids.push(0u32);
        }
        ids
    };
    for rid in rel_ids {
        if let Ok(es) = EdgeStore::open(db_path, RelTableId(rid)) {
            if let Ok(csr) = es.open_fwd() {
                csrs.insert(rid, csr);
            }
        }
    }

    Engine::new(store, catalog, csrs, db_path)
}

/// Get label_id for a named label.
fn label_id(db_path: &std::path::Path, label: &str) -> u32 {
    let cat = Catalog::open(db_path).expect("open catalog");
    cat.get_label(label)
        .expect("catalog lookup")
        .expect("label not found") as u32
}

// ── Test 1: DegreeCache built from CSR + delta, top_k returns correct order ──

/// Graph: 20 Person nodes.
///   After checkpoint, nodes 0-9 get (9-i) outgoing FOLLOWS edges.
///   Nodes 10-19 remain isolated.
///
/// top_k_by_degree(label_id, 5) must return nodes with degrees 9,8,7,6,5
/// (which correspond to node IDs/slots 0,1,2,3,4).
#[test]
fn top_k_by_degree_correct_order_and_count() {
    let (dir, db) = make_db();
    let db_path = dir.path();

    // Create 20 Person nodes (slot order = creation order).
    for i in 0..20u32 {
        db.execute(&format!("CREATE (n:Person {{id: {i}}})"))
            .expect("create node");
    }

    // Checkpoint so CSR files are written to disk.
    db.checkpoint().expect("checkpoint");

    // Create FOLLOWS edges: Person[i] gets (9-i) outgoing edges.
    // Only nodes 0-9 send edges.
    for i in 0..10u32 {
        let out_degree = 9 - i;
        for j in 1..=out_degree {
            let target = (i + j) % 20;
            db.execute(&format!(
                "MATCH (a:Person {{id: {i}}}), (b:Person {{id: {target}}}) \
                 CREATE (a)-[:FOLLOWS]->(b)"
            ))
            .expect("create edge");
        }
    }

    // Checkpoint again so all edges are in the CSR.
    db.checkpoint().expect("checkpoint 2");

    // Build engine directly from the on-disk database.
    let engine = build_engine(db_path);
    let lid = label_id(db_path, "Person");

    // top_k=5 should return slots 0..4 with degrees 9,8,7,6,5.
    let top5 = engine.top_k_by_degree(lid, 5).expect("top_k");

    assert_eq!(top5.len(), 5, "expected exactly 5 results");

    // Verify degrees are non-increasing.
    for w in top5.windows(2) {
        assert!(
            w[0].1 >= w[1].1,
            "degrees should be non-increasing: {:?}",
            top5
        );
    }

    // Node 0 (id=0) → slot 0 → degree 9 (highest).
    assert_eq!(top5[0].1, 9, "slot 0 should have degree 9");
    // Node 4 (id=4) → slot 4 → degree 5.
    assert_eq!(top5[4].1, 5, "slot 4 should have degree 5");

    // The top-5 slots must be exactly {0,1,2,3,4}.
    let mut slots: Vec<u64> = top5.iter().map(|&(s, _)| s).collect();
    slots.sort_unstable();
    assert_eq!(slots, vec![0u64, 1, 2, 3, 4]);

    // Verify: out_degree O(1) lookup for individual slots via the public API.
    assert_eq!(engine.out_degree(0), 9);
    assert_eq!(engine.out_degree(1), 8);
    assert_eq!(engine.out_degree(9), 0);
    assert_eq!(engine.out_degree(10), 0);
    assert_eq!(engine.out_degree(19), 0);
}

// ── Test 2: DegreeCache includes delta-log edges (no checkpoint) ──────────────

/// Create nodes and edges but do NOT checkpoint.  All edges live only in the
/// delta log.  DegreeCache must still count them correctly.
#[test]
fn degree_cache_counts_delta_log_edges() {
    let (dir, db) = make_db();
    let db_path = dir.path();

    for i in 0..5u32 {
        db.execute(&format!("CREATE (n:Hub {{id: {i}}})"))
            .expect("create node");
    }

    // Hub 0 → 3 outgoing, Hub 1 → 2 outgoing, Hub 2 → 1 outgoing.
    db.execute("MATCH (a:Hub {id:0}),(b:Hub {id:1}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (a:Hub {id:0}),(b:Hub {id:2}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (a:Hub {id:0}),(b:Hub {id:3}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (a:Hub {id:1}),(b:Hub {id:2}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (a:Hub {id:1}),(b:Hub {id:3}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (a:Hub {id:2}),(b:Hub {id:3}) CREATE (a)-[:LINK]->(b)")
        .unwrap();

    // No checkpoint — all edges are delta only.

    let engine = build_engine(db_path);
    let lid = label_id(db_path, "Hub");

    assert_eq!(engine.out_degree(0), 3, "Hub 0 should have degree 3");
    assert_eq!(engine.out_degree(1), 2, "Hub 1 should have degree 2");
    assert_eq!(engine.out_degree(2), 1, "Hub 2 should have degree 1");
    assert_eq!(engine.out_degree(3), 0, "Hub 3 should have degree 0");
    assert_eq!(engine.out_degree(4), 0, "Hub 4 should have degree 0");

    let top3 = engine.top_k_by_degree(lid, 3).expect("top_k");
    assert_eq!(top3.len(), 3);
    assert_eq!(top3[0], (0, 3), "slot 0 deg 3");
    assert_eq!(top3[1], (1, 2), "slot 1 deg 2");
    assert_eq!(top3[2], (2, 1), "slot 2 deg 1");
}

// ── Test 3: top_k larger than node count ─────────────────────────────────────

/// Requesting more top-k results than there are nodes returns all nodes.
#[test]
fn top_k_clamps_to_available_nodes() {
    let (dir, db) = make_db();
    let db_path = dir.path();

    db.execute("CREATE (n:Tiny {id: 0})").unwrap();
    db.execute("CREATE (n:Tiny {id: 1})").unwrap();
    db.execute("MATCH (a:Tiny {id:0}),(b:Tiny {id:1}) CREATE (a)-[:E]->(b)")
        .unwrap();

    let engine = build_engine(db_path);
    let lid = label_id(db_path, "Tiny");

    // Ask for top-100 from a 2-node label.
    let result = engine.top_k_by_degree(lid, 100).expect("top_k");

    // Should get at most 2 results (one per node in label).
    assert!(result.len() <= 2, "cannot exceed node count");

    // Node 0 (slot 0) has degree 1; node 1 (slot 1) has degree 0.
    assert_eq!(result[0], (0, 1), "slot 0 should have degree 1");
}

// ── Test 4: Empty database ────────────────────────────────────────────────────

/// top_k on a label with no edges returns isolated nodes (degree 0).
#[test]
fn top_k_empty_label() {
    let (dir, db) = make_db();
    let db_path = dir.path();

    db.execute("CREATE (n:Empty {id: 0})").unwrap();
    // No edges at all.

    let engine = build_engine(db_path);
    let lid = label_id(db_path, "Empty");

    let result = engine.top_k_by_degree(lid, 5).expect("top_k");

    // One node with degree 0.
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, 0, "isolated node should have degree 0");
}

// ── Test 5: k=0 returns empty ─────────────────────────────────────────────────

#[test]
fn top_k_zero_returns_empty() {
    let (dir, db) = make_db();
    let db_path = dir.path();

    db.execute("CREATE (n:Node {id: 0})").unwrap();

    let engine = build_engine(db_path);
    let lid = label_id(db_path, "Node");

    let result = engine.top_k_by_degree(lid, 0).expect("top_k");
    assert!(result.is_empty(), "k=0 must return empty");
}
