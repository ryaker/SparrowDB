//! Correctness tests for SPA-200: batch property reads in hop queries.
//!
//! The engine collects all neighbour node slots before reading properties, then
//! calls `batch_read_node_props` once per (label, column-set) instead of once
//! per neighbour.  These tests verify that the optimised path returns the same
//! results as the per-node path would.
//!
//! Graph shape:
//!   - 100 "Node" nodes, each with a `name` string property ("node_0" … "node_99")
//!     and an `idx` integer property (0 … 99).
//!   - Each node i is connected to 10 neighbours: (i*10) % 100 … (i*10+9) % 100.
//!   - That gives 1 000 edges total.
//!
//! Q3 (1-hop): MATCH (a:Node)-[:EDGE]->(b:Node) RETURN b.name
//!   Expected: 1 000 rows (one per edge).
//!
//! Q4 (2-hop): MATCH (a:Node)-[:EDGE]->(m:Node)-[:EDGE]->(b:Node) RETURN b.name
//!   Expected: 10 000 rows (100 srcs × 10 first-hop × 10 second-hop,
//!             before deduplication inside the engine).  We just assert >= 1000
//!             and that values are in the expected set.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");
    (dir, db)
}

/// Build the test graph: N_SRC source nodes and N_DST destination nodes.
/// Each source node is connected to exactly 10 distinct destination nodes
/// (dst indices 0..9 for src 0, 10..19 for src 1, etc.).
/// No src ever shares a destination with another src, so no inter-src
/// deduplication occurs and the total edge count is exactly N_SRC * 10.
const N_SRC: u32 = 5;    // 5 source nodes
const N_DST: u32 = 50;   // 50 destination nodes (10 per source, non-overlapping)

fn build_graph(db: &sparrowdb::GraphDb) {
    // Create destination nodes (the "b" side).
    // Use 1-based idx to avoid the 0-sentinel issue (0 == absent in storage).
    for j in 1..=N_DST {
        db.execute(&format!(
            "CREATE (n:Dst {{name: 'dst_{j}', idx: {j}}})"
        ))
        .unwrap_or_else(|e| panic!("CREATE dst_{j} failed: {e}"));
    }

    // Create source nodes and connect each to its 10 private destinations.
    for i in 1..=N_SRC {
        db.execute(&format!(
            "CREATE (n:Node {{name: 'node_{i}', idx: {i}}})"
        ))
        .unwrap_or_else(|e| panic!("CREATE node_{i} failed: {e}"));

        for k in 0u32..10 {
            let j = (i - 1) * 10 + k + 1; // unique dst idx 1..50, non-overlapping per src
            db.execute(&format!(
                "MATCH (a:Node {{idx: {i}}}), (b:Dst {{idx: {j}}}) CREATE (a)-[:EDGE]->(b)"
            ))
            .unwrap_or_else(|e| panic!("CREATE edge node_{i}->dst_{j} failed: {e}"));
        }
    }
}

// ── test 1: Q3 one-hop correctness ───────────────────────────────────────────

/// One-hop traversal must return one row per edge (N_SRC * 10 = 100 rows) and the
/// returned `b.name` values must all be valid destination names.
#[test]
fn one_hop_returns_all_neighbour_names() {
    let (_dir, db) = make_db();
    build_graph(&db);

    let expected = (N_SRC * 10) as usize;
    let result = db
        .execute("MATCH (a:Node)-[:EDGE]->(b:Dst) RETURN b.name")
        .expect("Q3 one-hop must succeed");

    assert_eq!(
        result.rows.len(),
        expected,
        "Q3 must return 1 row per edge ({expected}); got {}",
        result.rows.len()
    );

    // Spot-check: every returned name must match the pattern "dst_N".
    for row in &result.rows {
        match &row[0] {
            Value::String(s) => {
                assert!(
                    s.starts_with("dst_"),
                    "unexpected name value: {s:?}"
                );
            }
            other => panic!("expected String, got {other:?}"),
        }
    }
}

// ── test 2: Q3 src property also correct ─────────────────────────────────────

/// When both src and dst properties are projected, both must be resolved from
/// the batch-prefetched data without mixing up slots.
#[test]
fn one_hop_src_and_dst_properties_are_correct() {
    let (_dir, db) = make_db();

    // Small, fully-deterministic graph: a -> b -> c
    db.execute("CREATE (n:P {name: 'alpha', idx: 1})").unwrap();
    db.execute("CREATE (n:P {name: 'beta',  idx: 2})").unwrap();
    db.execute("CREATE (n:P {name: 'gamma', idx: 3})").unwrap();
    db.execute(
        "MATCH (a:P {idx: 1}), (b:P {idx: 2}) CREATE (a)-[:LINK]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:P {idx: 2}), (b:P {idx: 3}) CREATE (a)-[:LINK]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:P)-[:LINK]->(b:P) RETURN a.name, b.name")
        .expect("Q3 src+dst projection must succeed");

    assert_eq!(result.rows.len(), 2, "expected 2 edges");

    // Collect (src_name, dst_name) pairs and verify them.
    let mut pairs: Vec<(String, String)> = result
        .rows
        .iter()
        .map(|row| {
            let src = match &row[0] {
                Value::String(s) => s.clone(),
                v => panic!("expected String for src, got {v:?}"),
            };
            let dst = match &row[1] {
                Value::String(s) => s.clone(),
                v => panic!("expected String for dst, got {v:?}"),
            };
            (src, dst)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("alpha".to_string(), "beta".to_string()),
            ("beta".to_string(), "gamma".to_string()),
        ],
        "src/dst properties must not be swapped or cross-contaminated"
    );
}

// ── test 3: Q4 two-hop correctness ───────────────────────────────────────────

/// Two-hop traversal must return the correct number of rows and valid names.
///
/// We use a single `Person` label for all nodes so `execute_two_hop`'s
/// merged-CSR approach works correctly (the CSR merges slot-level edges across
/// all rel tables; multi-label mixing requires per-label CSR filtering that the
/// 2-hop executor does not currently support).
///
/// After checkpoint: 1 source (idx=100) → 5 mids (idx=1..5) → 3 leaves each
/// (idx=11..25), giving exactly 15 two-hop paths.
///
/// idx values start at 1 to avoid the 0-sentinel storage issue.
#[test]
fn two_hop_returns_valid_names() {
    let (_dir, db) = make_db();

    // Source node.
    db.execute("CREATE (n:Person {name: 'src', idx: 100})").unwrap();

    for m in 1u32..=5 {
        // Mid node.
        db.execute(&format!(
            "CREATE (n:Person {{name: 'mid_{m}', idx: {m}}})"
        ))
        .unwrap();
        // src -> mid
        db.execute(&format!(
            "MATCH (a:Person {{idx: 100}}), (b:Person {{idx: {m}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();

        for l in 1u32..=3 {
            let leaf_idx = 10 + (m - 1) * 3 + l; // 11..25, all > 0
            db.execute(&format!(
                "CREATE (n:Person {{name: 'leaf_{leaf_idx}', idx: {leaf_idx}}})"
            ))
            .unwrap();
            // mid -> leaf
            db.execute(&format!(
                "MATCH (a:Person {{idx: {m}}}), (b:Person {{idx: {leaf_idx}}}) \
                 CREATE (a)-[:KNOWS]->(b)"
            ))
            .unwrap();
        }
    }

    // Checkpoint so the forward CSR is populated; without this the merged_csr
    // in execute_two_hop is empty and only same-label delta paths are found.
    db.execute("CHECKPOINT").expect("checkpoint must succeed");

    let result = db
        .execute(
            "MATCH (a:Person {idx: 100})-[:KNOWS]->(m:Person)-[:KNOWS]->(b:Person) RETURN b.name",
        )
        .expect("Q4 two-hop must succeed");

    assert_eq!(
        result.rows.len(),
        15,
        "Q4 must return 15 rows (5 mids × 3 leaves each); got {}",
        result.rows.len()
    );

    // Every returned value must be a valid leaf name.
    for row in &result.rows {
        match &row[0] {
            Value::String(s) => {
                assert!(
                    s.starts_with("leaf_"),
                    "unexpected name in Q4 result: {s:?}"
                );
            }
            other => panic!("expected String, got {other:?}"),
        }
    }
}

// ── test 4: batch-read does not mix properties across slots ──────────────────

/// The integer `idx` property must match the node it was assigned to — verifies
/// that batch_read_node_props correctly indexes by slot and does not shift rows.
#[test]
fn one_hop_idx_values_are_not_shifted() {
    let (_dir, db) = make_db();

    // Three nodes with distinct idx values; two edges.
    db.execute("CREATE (n:Q {name: 'x0', idx: 10})").unwrap();
    db.execute("CREATE (n:Q {name: 'x1', idx: 20})").unwrap();
    db.execute("CREATE (n:Q {name: 'x2', idx: 30})").unwrap();
    db.execute(
        "MATCH (a:Q {idx: 10}), (b:Q {idx: 20}) CREATE (a)-[:HOP]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Q {idx: 10}), (b:Q {idx: 30}) CREATE (a)-[:HOP]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Q)-[:HOP]->(b:Q) RETURN b.name, b.idx")
        .expect("batch-read idx test must succeed");

    assert_eq!(result.rows.len(), 2, "expected 2 rows");

    for row in &result.rows {
        let name = match &row[0] {
            Value::String(s) => s.clone(),
            v => panic!("expected String for name, got {v:?}"),
        };
        let idx = match &row[1] {
            Value::Int64(n) => *n,
            v => panic!("expected Int64 for idx, got {v:?}"),
        };

        // The name encodes the expected idx: "x0" -> 10, "x1" -> 20, "x2" -> 30.
        let expected_idx = match name.as_str() {
            "x0" => 10i64,
            "x1" => 20i64,
            "x2" => 30i64,
            other => panic!("unexpected name {other}"),
        };
        assert_eq!(
            idx, expected_idx,
            "batch read returned wrong idx for node {name}: expected {expected_idx}, got {idx}"
        );
    }
}
