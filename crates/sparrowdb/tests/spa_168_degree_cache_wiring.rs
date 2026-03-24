//! Integration tests for SPA-168: wiring DegreeCache to GraphDb::top_degree_nodes.
//!
//! Validates that `GraphDb::top_degree_nodes(label, limit)` correctly delegates
//! to `Engine::top_k_by_degree` through the degree cache — no full edge scan at
//! query time.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: Known degree distribution ─────────────────────────────────────────
//
// Node A has 3 edges, B has 2, C has 1, D has 0.
// top_degree_nodes("Person", 2) → [(A, 3), (B, 2)]

#[test]
fn top_degree_nodes_returns_correct_top_k() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'A'})").unwrap();
    db.execute("CREATE (n:Person {name: 'B'})").unwrap();
    db.execute("CREATE (n:Person {name: 'C'})").unwrap();
    db.execute("CREATE (n:Person {name: 'D'})").unwrap();

    // A → B, A → C, A → D  (degree 3)
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'B'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'C'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'D'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    // B → C, B → D  (degree 2)
    db.execute("MATCH (a:Person {name:'B'}),(b:Person {name:'C'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'B'}),(b:Person {name:'D'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    // C → D  (degree 1)
    db.execute("MATCH (a:Person {name:'C'}),(b:Person {name:'D'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    // D — no outgoing edges (degree 0)

    let top2 = db.top_degree_nodes("Person", 2).unwrap();

    assert_eq!(top2.len(), 2, "should return exactly 2 results");
    // Degrees must be non-increasing.
    assert!(
        top2[0].1 >= top2[1].1,
        "results must be sorted descending by degree"
    );
    // Top node must have degree 3.
    assert_eq!(top2[0].1, 3, "highest-degree node should have degree 3");
    // Second node must have degree 2.
    assert_eq!(top2[1].1, 2, "second node should have degree 2");
}

// ── Test 2: limit=0 returns empty ─────────────────────────────────────────────

#[test]
fn top_degree_nodes_limit_zero_returns_empty() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Widget {id: 0})").unwrap();
    let result = db.top_degree_nodes("Widget", 0).unwrap();
    assert!(result.is_empty(), "limit=0 must return empty vec");
}

// ── Test 3: Unknown label returns empty ───────────────────────────────────────

#[test]
fn top_degree_nodes_unknown_label_returns_empty() {
    let (_dir, db) = make_db();
    // No nodes at all.
    let result = db.top_degree_nodes("NonExistentLabel", 10).unwrap();
    assert!(result.is_empty(), "unknown label must return empty vec");
}

// ── Test 4: Limit larger than node count returns all nodes ────────────────────

#[test]
fn top_degree_nodes_limit_exceeds_node_count() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Tiny {id: 0})").unwrap();
    db.execute("CREATE (n:Tiny {id: 1})").unwrap();
    db.execute("MATCH (a:Tiny {id:0}),(b:Tiny {id:1}) CREATE (a)-[:E]->(b)")
        .unwrap();

    // Ask for 100 but only 2 nodes exist.
    let result = db.top_degree_nodes("Tiny", 100).unwrap();
    assert!(
        result.len() <= 2,
        "cannot return more results than there are nodes"
    );
    // Node with slot 0 must be first with degree 1.
    assert_eq!(result[0].1, 1, "top node should have degree 1");
}

// ── Test 5: Post-checkpoint CSR + delta-log edges both counted ─────────────────

#[test]
fn top_degree_nodes_post_checkpoint_and_delta() {
    let (_dir, db) = make_db();

    // Create 3 nodes and checkpoint so they appear in CSR.
    for i in 0..3u32 {
        db.execute(&format!("CREATE (n:Star {{id: {i}}})")).unwrap();
    }
    db.checkpoint().unwrap();

    // Add edges in delta (no second checkpoint).
    // Star 0 → 1, Star 0 → 2  (degree 2 in delta only)
    db.execute("MATCH (a:Star {id:0}),(b:Star {id:1}) CREATE (a)-[:RAY]->(b)")
        .unwrap();
    db.execute("MATCH (a:Star {id:0}),(b:Star {id:2}) CREATE (a)-[:RAY]->(b)")
        .unwrap();

    let top1 = db.top_degree_nodes("Star", 1).unwrap();
    assert_eq!(top1.len(), 1);
    assert_eq!(top1[0].1, 2, "Star 0 should have degree 2 from delta log");
}
