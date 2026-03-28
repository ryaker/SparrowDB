//! SPA-296: BulkLoader — CSV/edge-list ingestion with batched WriteTx.
//!
//! ## What this tests
//!
//! * **T1**: Load 100 nodes from CSV; verify COUNT(*) equals 100.
//! * **T2**: Load 50 edges between previously loaded nodes; verify COUNT(*) = 50.
//! * **T3**: Verify a specific property round-trips — the `name` of the node
//!   with `id = 42`.

use sparrowdb::bulk::{BulkLoader, BulkOptions};
use sparrowdb::GraphDb;
use std::io::Write;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open GraphDb");
    (dir, db)
}

/// Count nodes matching `MATCH (n:Label) RETURN count(n)`.
fn count_nodes(db: &GraphDb, label: &str) -> u64 {
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

/// Count edges of `rel_type` via `MATCH ()-[r:REL]->() RETURN count(r)`.
fn count_edges(db: &GraphDb, rel_type: &str) -> u64 {
    let q = format!("MATCH ()-[r:{rel_type}]->() RETURN count(r)");
    let res = db.execute(&q).expect("count edges query");
    if res.rows.is_empty() {
        return 0;
    }
    match &res.rows[0][0] {
        sparrowdb_execution::Value::Int64(n) => *n as u64,
        sparrowdb_execution::Value::Float64(f) => *f as u64,
        _ => 0,
    }
}

/// Write a string to a named temp file inside `dir`, return the path.
fn write_temp_csv(dir: &tempfile::TempDir, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let mut f = std::fs::File::create(&path).expect("create csv");
    f.write_all(content.as_bytes()).expect("write csv");
    path
}

// ── T1: Load 100 nodes, verify COUNT(*) ───────────────────────────────────────

#[test]
fn t1_load_100_nodes() {
    let (dir, db) = fresh_db();

    // Build CSV: header + 100 rows with id and name columns.
    let mut csv = String::from("id,name\n");
    for i in 0..100u64 {
        csv.push_str(&format!("{i},person_{i}\n"));
    }
    let csv_path = write_temp_csv(&dir, "nodes.csv", &csv);

    let loader = BulkLoader::new(
        &db,
        BulkOptions {
            batch_size: 25,
            ..Default::default()
        },
    );
    let n = loader.load_nodes(&csv_path, "Person").expect("load_nodes");

    assert_eq!(n, 100, "load_nodes should return 100");
    let count = count_nodes(&db, "Person");
    assert_eq!(
        count, 100,
        "MATCH should find 100 Person nodes (got {count})"
    );
}

// ── T2: Load 50 edges, verify COUNT(*) ───────────────────────────────────────

#[test]
fn t2_load_50_edges() {
    let (dir, db) = fresh_db();

    // First load 100 nodes (ids 0..99).
    let mut node_csv = String::from("id,name\n");
    for i in 0..100u64 {
        node_csv.push_str(&format!("{i},person_{i}\n"));
    }
    let node_path = write_temp_csv(&dir, "nodes2.csv", &node_csv);

    let loader = BulkLoader::new(&db, BulkOptions::default());
    loader.load_nodes(&node_path, "Member").expect("load_nodes");

    // Now load 50 edges: node i → node i+50 (no duplicates).
    let mut edge_csv = String::from("src_id,dst_id\n");
    for i in 0..50u64 {
        edge_csv.push_str(&format!("{i},{}\n", i + 50));
    }
    let edge_path = write_temp_csv(&dir, "edges2.csv", &edge_csv);

    let e = loader
        .load_edges(&edge_path, "FOLLOWS", "Member", "Member")
        .expect("load_edges");

    assert_eq!(e, 50, "load_edges should return 50");
    let count = count_edges(&db, "FOLLOWS");
    assert_eq!(
        count, 50,
        "MATCH should find 50 FOLLOWS edges (got {count})"
    );
}

// ── T3: Property round-trip for id=42 ────────────────────────────────────────

#[test]
fn t3_property_roundtrip_id42() {
    let (dir, db) = fresh_db();

    let mut csv = String::from("id,name\n");
    for i in 0..100u64 {
        csv.push_str(&format!("{i},person_{i}\n"));
    }
    let csv_path = write_temp_csv(&dir, "nodes3.csv", &csv);

    let loader = BulkLoader::new(&db, BulkOptions::default());
    loader.load_nodes(&csv_path, "Actor").expect("load_nodes");

    // Look up the name property of the node with id=42.
    let res = db
        .execute("MATCH (n:Actor {id: 42}) RETURN n.name")
        .expect("query");

    assert!(!res.rows.is_empty(), "expected at least one row for id=42");
    let name_val = &res.rows[0][0];
    let name_str = match name_val {
        sparrowdb_execution::Value::String(s) => s.clone(),
        other => panic!("unexpected value type: {other:?}"),
    };
    assert_eq!(
        name_str, "person_42",
        "name property should round-trip as 'person_42', got '{name_str}'"
    );
}
