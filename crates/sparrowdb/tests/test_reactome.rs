//! Integration tests for the Reactome biological-pathway benchmark dataset
//! — issue #301 (TuringDB comparison datasets).
//!
//! Verifies that the generator loads the correct graph shape and that
//! Q3/Q4/Q8-equivalent queries return meaningful results.

use sparrowdb_bench::realworld::{self, ReactomeConfig};
use sparrowdb_execution::types::Value;

/// Helper: build a small Reactome database for testing.
///
/// Uses reduced counts for fast test runs while preserving the structural
/// properties needed to exercise each query type.
fn setup_reactome() -> (
    tempfile::TempDir,
    sparrowdb::GraphDb,
    realworld::RealWorldStats,
    u64, // first pathway pid
    u64, // first sub-pathway pid
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let cfg = ReactomeConfig {
        pathways: 5,
        reactions_per_pathway: 4,
        entities: 20,
        seed: 42,
    };

    let stats = realworld::load_reactome(&db, &cfg).expect("load reactome");
    db.checkpoint().expect("checkpoint");

    // Entities are created first (nids 1..=entities).
    // First Pathway is at entities+1, first SubPathway is at entities+2.
    let first_pid = cfg.entities as u64 + 1;
    let first_sub_pid = cfg.entities as u64 + 2;

    (dir, db, stats, first_pid, first_sub_pid)
}

#[test]
fn reactome_loads_expected_node_counts() {
    let (_dir, db, stats, _, _) = setup_reactome();

    // 20 entities + 5 pathways + 5 sub-pathways + (5 * 4) reactions = 50 nodes.
    assert_eq!(
        stats.nodes_created, 50,
        "expected 50 nodes, got {}",
        stats.nodes_created
    );
    assert!(
        stats.edges_created > 0,
        "should create edges, got {}",
        stats.edges_created
    );

    // Verify via Cypher.
    let result = db
        .execute("MATCH (e:PhysicalEntity) RETURN COUNT(e)")
        .expect("count PhysicalEntity");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(count, 20, "PhysicalEntity count should be 20, got {count}");

    let result = db
        .execute("MATCH (p:Pathway) RETURN COUNT(p)")
        .expect("count Pathway");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // 5 parent pathways + 5 sub-pathways = 10
    assert_eq!(count, 10, "Pathway count should be 10, got {count}");

    let result = db
        .execute("MATCH (r:Reaction) RETURN COUNT(r)")
        .expect("count Reaction");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(count, 20, "Reaction count should be 20, got {count}");
}

#[test]
fn reactome_q3_1hop_returns_components() {
    let (_dir, db, _, first_pid, _) = setup_reactome();

    let components = realworld::q3_pathway_components(&db, first_pid).expect("q3");
    // Each parent pathway has exactly 1 HAS_COMPONENT -> sub-pathway edge.
    assert_eq!(
        components.len(),
        1,
        "parent pathway should have 1 direct component (sub-pathway), got {:?}",
        components
    );
}

#[test]
fn reactome_q4_2hop_finds_reactions() {
    let (_dir, db, _, _, first_sub_pid) = setup_reactome();

    // Q4 traverses sub-pathway -> reactions (4 reactions per sub-pathway).
    let count = realworld::q4_pathway_reactions_2hop(&db, first_sub_pid).expect("q4");
    assert_eq!(
        count, 4,
        "sub-pathway should have 4 direct reaction components, got {count}"
    );
}

#[test]
fn reactome_q8_shared_catalysts_returns_results() {
    let (_dir, db, _, _, first_sub_pid) = setup_reactome();

    // Q8 looks for PhysicalEntities catalysing reactions in the sub-pathway.
    // With 4 reactions each having 1-3 catalysis edges, there should be results.
    let count = realworld::q8_shared_catalysts(&db, first_sub_pid).expect("q8");
    assert!(
        count > 0,
        "should find at least one catalyst in the sub-pathway, got {count}"
    );
}

#[test]
fn reactome_has_component_edges_present() {
    let (_dir, db, _, _, _) = setup_reactome();

    let result = db
        .execute("MATCH ()-[r:HAS_COMPONENT]->() RETURN COUNT(r)")
        .expect("count HAS_COMPONENT");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // 5 parent->sub + 5*4 sub->reaction = 25 HAS_COMPONENT edges
    assert_eq!(count, 25, "should have 25 HAS_COMPONENT edges, got {count}");
}

#[test]
fn reactome_next_step_chain_present() {
    let (_dir, db, _, _, _) = setup_reactome();

    let result = db
        .execute("MATCH ()-[r:NEXT_STEP]->() RETURN COUNT(r)")
        .expect("count NEXT_STEP");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // Each pathway contributes reactions_per_pathway-1 NEXT_STEP edges = 5*3 = 15
    assert_eq!(count, 15, "should have 15 NEXT_STEP edges, got {count}");
}
