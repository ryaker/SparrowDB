//! Integration tests for the MLM referral-tree generator and query suite — SPA-206.
//!
//! Verifies that the generator produces the expected graph topology and that
//! each of the five MLM benchmark queries returns meaningful results.

use sparrowdb_bench::mlm::{self, MlmConfig};
use sparrowdb_execution::types::Value;

/// Helper: create a small MLM database for testing.
fn setup_mlm() -> (tempfile::TempDir, sparrowdb::GraphDb, mlm::MlmStats) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let cfg = MlmConfig {
        members: 100,
        max_depth: 5,
        avg_fanout: 3,
        seed: 42,
    };

    let stats = mlm::generate(&db, &cfg).expect("generate MLM tree");
    db.checkpoint().expect("checkpoint");
    (dir, db, stats)
}

#[test]
fn generator_creates_expected_nodes_and_edges() {
    let (_dir, db, stats) = setup_mlm();

    assert!(
        stats.nodes_created >= 50,
        "should create a substantial number of members, got {}",
        stats.nodes_created
    );
    assert!(
        stats.edges_created >= 49,
        "should create RECRUITED_BY edges (nodes-1 at minimum), got {}",
        stats.edges_created
    );
    assert!(
        stats.actual_depth >= 2,
        "tree should have at least depth 2, got {}",
        stats.actual_depth
    );

    // Verify via Cypher count.
    let result = db
        .execute("MATCH (m:Member) RETURN COUNT(m)")
        .expect("count members");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(
        count, stats.nodes_created as i64,
        "Cypher node count should match stats"
    );
}

#[test]
fn q_mlm1_downline_volume_returns_nonzero() {
    let (_dir, db, _stats) = setup_mlm();

    // Root (uid=1) should have downline.
    let (count, volume) = mlm::q_mlm1_downline_volume(&db, 1).expect("q_mlm1");
    assert!(count > 0, "root should have downline, count={count}");
    assert!(
        volume > 0,
        "total volume should be positive, volume={volume}"
    );
}

#[test]
fn q_mlm2_level3_downline() {
    let (_dir, db, stats) = setup_mlm();

    if stats.actual_depth >= 3 {
        let members = mlm::q_mlm2_level3_downline(&db, 1).expect("q_mlm2");
        // With a tree of depth >= 3 and avg_fanout 3, root should have some
        // level-3 descendants.
        assert!(
            !members.is_empty(),
            "root should have level-3 downline when depth >= 3"
        );
        for (uid, vol) in &members {
            assert!(*uid > 0, "uid should be positive");
            assert!(*vol >= 0, "volume should be non-negative, got {vol}");
        }
    }
}

#[test]
fn q_mlm3_upline_path_from_leaf() {
    let (_dir, db, stats) = setup_mlm();

    // Pick a high uid (likely a leaf or deep node).
    let leaf_uid = stats.nodes_created as usize;
    let ancestors = mlm::q_mlm3_upline_path(&db, leaf_uid).expect("q_mlm3");

    // Every non-root member should have at least one ancestor.
    assert!(
        !ancestors.is_empty(),
        "leaf uid={leaf_uid} should have at least one ancestor"
    );
    // The last ancestor in the chain should be the root (uid=1) or close to it.
    assert!(
        ancestors.contains(&1),
        "upline path should include the root (uid=1), got {:?}",
        ancestors
    );
}

#[test]
fn q_mlm4_subtree_volume_active_only() {
    let (_dir, db, _stats) = setup_mlm();

    let volume = mlm::q_mlm4_subtree_volume(&db, 1).expect("q_mlm4");
    // With 85% active rate and non-zero volumes, sum should be positive.
    assert!(
        volume > 0,
        "subtree volume of active members should be positive, got {volume}"
    );
}

#[test]
fn q_mlm5_top_recruiters() {
    let (_dir, db, _stats) = setup_mlm();

    let top = mlm::q_mlm5_top_recruiters(&db).expect("q_mlm5");
    assert!(
        !top.is_empty(),
        "should find at least one recruiter with downline"
    );
    assert!(top.len() <= 10, "should return at most 10 recruiters");

    // Results should be ordered descending by recruit count.
    for window in top.windows(2) {
        assert!(
            window[0].1 >= window[1].1,
            "top recruiters should be sorted DESC, got {:?}",
            top
        );
    }
}

#[test]
fn deterministic_generation() {
    // Two runs with the same seed should produce identical stats.
    let cfg = MlmConfig {
        members: 50,
        max_depth: 4,
        avg_fanout: 3,
        seed: 99,
    };

    let dir1 = tempfile::tempdir().expect("tempdir");
    let db1 = sparrowdb::open(dir1.path()).expect("open db1");
    let stats1 = mlm::generate(&db1, &cfg).expect("generate 1");

    let dir2 = tempfile::tempdir().expect("tempdir");
    let db2 = sparrowdb::open(dir2.path()).expect("open db2");
    let stats2 = mlm::generate(&db2, &cfg).expect("generate 2");

    assert_eq!(
        stats1.nodes_created, stats2.nodes_created,
        "deterministic: node count should match"
    );
    assert_eq!(
        stats1.edges_created, stats2.edges_created,
        "deterministic: edge count should match"
    );
    assert_eq!(
        stats1.actual_depth, stats2.actual_depth,
        "deterministic: depth should match"
    );
}
