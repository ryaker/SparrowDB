//! Integration tests for the LDBC SNB data loader (SPA-145).
//!
//! These tests use the synthetic mini-fixture dataset under
//! `fixtures/ldbc/mini/` to validate that:
//!
//! 1. Node CSV files are loaded and their count matches the CSV row count.
//! 2. Edge CSV files produce correctly wired relationships.
//! 3. The ID-map correctly resolves cross-file relationships.
//! 4. Unit helpers (label_for, classify_stem) work correctly.
//! 5. An end-to-end load of the full mini fixture completes without errors.

use sparrowdb::GraphDb;
use sparrowdb_bench::*;
use std::path::PathBuf;

/// Path to the mini fixture bundled inside the crate source tree.
fn fixture_dir() -> PathBuf {
    // When cargo runs tests the cwd is the crate root.
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("fixtures/ldbc/mini");
    p
}

/// Open a fresh SparrowDB in a temp directory.
fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open GraphDb");
    (dir, db)
}

/// Count nodes with a given label via Cypher.
fn count_label(db: &GraphDb, label: &str) -> u64 {
    let q = format!("MATCH (n:{label}) RETURN count(n)");
    let res = db.execute(&q).expect("count query");
    if res.rows.is_empty() {
        return 0;
    }
    match &res.rows[0][0] {
        sparrowdb_execution::Value::Int64(n) => *n as u64,
        sparrowdb_execution::Value::Float64(f) => *f as u64,
        _ => 0,
    }
}

// ── Unit tests: helpers ───────────────────────────────────────────────────────

#[test]
fn label_for_all_ldbc_entity_types() {
    assert_eq!(label_for("person"), "Person");
    assert_eq!(label_for("PERSON"), "Person");
    assert_eq!(label_for("post"), "Post");
    assert_eq!(label_for("comment"), "Comment");
    assert_eq!(label_for("forum"), "Forum");
    assert_eq!(label_for("organisation"), "Organisation");
    assert_eq!(label_for("organization"), "Organisation");
    assert_eq!(label_for("place"), "Place");
    assert_eq!(label_for("tag"), "Tag");
    assert_eq!(label_for("tagclass"), "TagClass");
}

#[test]
fn id_map_roundtrip() {
    let mut map = IdMap::default();
    let nid = sparrowdb::NodeId(0x0001_0000_0000_0005);
    map.insert("Person", "42", nid);

    assert_eq!(map.get("Person", "42"), Some(nid));
    // Different label — not found.
    assert_eq!(map.get("Forum", "42"), None);
    // Different id — not found.
    assert_eq!(map.get("Person", "99"), None);
}

// ── Integration test: load person node file ───────────────────────────────────

#[test]
fn load_person_nodes_from_fixture() {
    let (_dir, db) = fresh_db();
    let mut stats = LoadStats::default();
    let mut id_map = IdMap::default();

    let person_csv = fixture_dir().join("dynamic/person_0_0.csv");
    assert!(
        person_csv.exists(),
        "fixture not found: {}",
        person_csv.display()
    );

    load_node_file(&db, &person_csv, "Person", &mut id_map, &mut stats).expect("load person nodes");

    // The CSV has 10 persons.
    assert_eq!(stats.nodes_loaded, 10, "expected 10 Person nodes");

    // All 10 should be queryable.
    let count = count_label(&db, "Person");
    assert_eq!(count, 10, "10 Person nodes must be visible after load");

    // ID map should resolve person 1..=10.
    for i in 1u64..=10 {
        let id_str = i.to_string();
        assert!(
            id_map.get("Person", &id_str).is_some(),
            "IdMap should contain Person {i}"
        );
    }
}

// ── Integration test: load place node file ────────────────────────────────────

#[test]
fn load_place_nodes_from_fixture() {
    let (_dir, db) = fresh_db();
    let mut stats = LoadStats::default();
    let mut id_map = IdMap::default();

    let place_csv = fixture_dir().join("static/place_0_0.csv");
    assert!(
        place_csv.exists(),
        "fixture not found: {}",
        place_csv.display()
    );

    load_node_file(&db, &place_csv, "Place", &mut id_map, &mut stats).expect("load place nodes");

    assert_eq!(stats.nodes_loaded, 5, "expected 5 Place nodes");
    let count = count_label(&db, "Place");
    assert_eq!(count, 5, "5 Place nodes must be visible");
}

// ── Integration test: load KNOWS edges ───────────────────────────────────────

#[test]
fn load_knows_edges_from_fixture() {
    let (_dir, db) = fresh_db();
    let mut stats = LoadStats::default();
    let mut id_map = IdMap::default();

    // Load persons first so the ID map is populated.
    let person_csv = fixture_dir().join("dynamic/person_0_0.csv");
    load_node_file(&db, &person_csv, "Person", &mut id_map, &mut stats).expect("load persons");

    let knows_csv = fixture_dir().join("dynamic/person_knows_person_0_0.csv");
    assert!(
        knows_csv.exists(),
        "fixture not found: {}",
        knows_csv.display()
    );

    load_edge_file(
        &db, &knows_csv, "Person", "knows", "Person", &id_map, &mut stats,
    )
    .expect("load knows edges");

    // The CSV has 13 KNOWS relationships.
    assert_eq!(stats.edges_loaded, 13, "expected 13 KNOWS edges");

    // Checkpoint so edges are queryable via CSR.
    db.checkpoint().expect("checkpoint");

    // Verify with a Cypher MATCH — person 1 (Alice) knows persons 2, 3, 6.
    let res = db
        .execute("MATCH (a:Person {ldbc_id: 1})-[:knows]->(b:Person) RETURN count(b)")
        .expect("count Alice's friends");
    let alice_friends = match &res.rows[0][0] {
        sparrowdb_execution::Value::Int64(n) => *n,
        sparrowdb_execution::Value::Float64(f) => *f as i64,
        _ => -1,
    };
    assert_eq!(alice_friends, 3, "Alice (id=1) should have 3 KNOWS edges");
}

// ── Integration test: full end-to-end mini fixture load ──────────────────────

#[test]
fn end_to_end_mini_fixture_load() {
    let (_dir, db) = fresh_db();

    let stats = load(&db, &fixture_dir()).expect("load mini fixture");

    // We expect: 10 persons + 3 forums + 5 places + 4 tags = 22 nodes.
    assert!(
        stats.nodes_loaded >= 22,
        "expected at least 22 nodes (got {})",
        stats.nodes_loaded
    );

    // We expect: 13 knows + 7 hasMember = 20 edges.
    assert!(
        stats.edges_loaded >= 20,
        "expected at least 20 edges (got {})",
        stats.edges_loaded
    );

    // No load errors.
    assert_eq!(
        stats.errors, 0,
        "expected 0 load errors (got {})",
        stats.errors
    );

    // Checkpoint so edges land in CSR for queries.
    db.checkpoint().expect("checkpoint");

    // Verify node counts.
    assert_eq!(count_label(&db, "Person"), 10);
    assert_eq!(count_label(&db, "Forum"), 3);
    assert_eq!(count_label(&db, "Place"), 5);
    assert_eq!(count_label(&db, "Tag"), 4);
}
