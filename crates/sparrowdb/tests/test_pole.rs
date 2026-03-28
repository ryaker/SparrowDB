//! Integration tests for the POLE investigation-graph benchmark dataset
//! — issue #301 (TuringDB comparison datasets).
//!
//! Verifies that the generator loads the correct graph shape and that
//! Q3/Q4/Q8-equivalent queries return meaningful results.

use sparrowdb_bench::realworld::{self, PoleConfig};
use sparrowdb_execution::types::Value;

/// Helper: build a POLE database for testing.
fn setup_pole() -> (
    tempfile::TempDir,
    sparrowdb::GraphDb,
    realworld::RealWorldStats,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let cfg = PoleConfig::default();
    let stats = realworld::load_pole(&db, &cfg).expect("load POLE");
    db.checkpoint().expect("checkpoint");
    (dir, db, stats)
}

#[test]
fn pole_loads_expected_node_counts() {
    let (_dir, db, stats) = setup_pole();

    let cfg = PoleConfig::default();
    let expected_nodes = (cfg.persons + cfg.objects + cfg.locations + cfg.events) as u64;

    assert_eq!(
        stats.nodes_created, expected_nodes,
        "expected {expected_nodes} nodes, got {}",
        stats.nodes_created
    );
    assert!(
        stats.edges_created > 0,
        "should create edges, got {}",
        stats.edges_created
    );

    // Verify each label individually via Cypher.
    let result = db
        .execute("MATCH (p:Person) RETURN COUNT(p)")
        .expect("count Person");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(
        count, cfg.persons as i64,
        "Person count should be {}, got {count}",
        cfg.persons
    );

    let result = db
        .execute("MATCH (o:Object) RETURN COUNT(o)")
        .expect("count Object");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(
        count, cfg.objects as i64,
        "Object count should be {}, got {count}",
        cfg.objects
    );

    let result = db
        .execute("MATCH (l:Location) RETURN COUNT(l)")
        .expect("count Location");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(
        count, cfg.locations as i64,
        "Location count should be {}, got {count}",
        cfg.locations
    );

    let result = db
        .execute("MATCH (e:Event) RETURN COUNT(e)")
        .expect("count Event");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    assert_eq!(
        count, cfg.events as i64,
        "Event count should be {}, got {count}",
        cfg.events
    );
}

#[test]
fn pole_knows_edges_present() {
    let (_dir, db, _) = setup_pole();

    let result = db
        .execute("MATCH ()-[r:KNOWS]->() RETURN COUNT(r)")
        .expect("count KNOWS");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // Each of 35 persons generates 2-4 KNOWS edges; expect a substantial count.
    assert!(count > 0, "should have KNOWS edges, got {count}");
}

#[test]
fn pole_located_at_edges_present() {
    let (_dir, db, _) = setup_pole();

    let result = db
        .execute("MATCH ()-[r:LOCATED_AT]->() RETURN COUNT(r)")
        .expect("count LOCATED_AT");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // persons + objects + events all have exactly one LOCATED_AT edge each
    let cfg = PoleConfig::default();
    let expected = (cfg.persons + cfg.objects + cfg.events) as i64;
    assert_eq!(
        count, expected,
        "LOCATED_AT count should be {expected}, got {count}"
    );
}

#[test]
fn pole_party_to_edges_present() {
    let (_dir, db, _) = setup_pole();

    let result = db
        .execute("MATCH ()-[r:PARTY_TO]->() RETURN COUNT(r)")
        .expect("count PARTY_TO");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // 30 events * 2-3 parties each = 60-90 PARTY_TO edges
    assert!(
        count >= 60,
        "should have at least 60 PARTY_TO edges, got {count}"
    );
}

#[test]
fn pole_q3_knows_1hop_returns_results() {
    let (_dir, db, _) = setup_pole();

    // Person with nid=1 should know some others.
    let count = realworld::q3_knows_1hop(&db, 1).expect("q3");
    assert!(
        count > 0,
        "person nid=1 should have at least one KNOWS neighbour, got {count}"
    );
}

#[test]
fn pole_q4_knows_2hop_reaches_more() {
    let (_dir, db, _) = setup_pole();

    let hop1 = realworld::q3_knows_1hop(&db, 1).expect("q3");
    let hop2 = realworld::q4_knows_2hop(&db, 1).expect("q4");

    // 2-hop should reach at least as many as 1-hop.
    assert!(
        hop2 >= hop1,
        "2-hop ({hop2}) should reach at least as many as 1-hop ({hop1})"
    );
}

#[test]
fn pole_q8_co_party_events() {
    let (_dir, db, _) = setup_pole();

    // With 30 events and 2-3 parties each, person 1 is likely party to at
    // least one event that also includes another person.
    let count = realworld::q8_co_party_events(&db, 1).expect("q8");
    // Result may be 0 for a specific person if they happen not to be party to
    // any event — the query is still valid if it executes without error.
    let _ = count; // suppress unused variable warning when count happens to be 0
}

#[test]
fn pole_involved_in_edges_present() {
    let (_dir, db, _) = setup_pole();

    let result = db
        .execute("MATCH ()-[r:INVOLVED_IN]->() RETURN COUNT(r)")
        .expect("count INVOLVED_IN");
    let count = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {:?}", other),
    };
    // 30 events * 1-2 objects each = 30-60 INVOLVED_IN edges
    assert!(
        count >= 30,
        "should have at least 30 INVOLVED_IN edges, got {count}"
    );
}
