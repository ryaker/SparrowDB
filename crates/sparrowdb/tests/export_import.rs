//! Integration tests for GraphDb::export_json / import_json — SPA-XXX.
//!
//! Verifies that a graph snapshot can be serialised to JSON and re-imported
//! into a fresh database with full fidelity for nodes and edges.

use sparrowdb::{open, GraphDump};
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Export produces valid JSON with correct structure
// ─────────────────────────────────────────────────────────────────────────────

/// `export_json()` must return valid JSON that deserialises into a `GraphDump`
/// with the right node/edge counts and metadata fields.
#[test]
fn export_json_produces_valid_structure() {
    let (_dir, db) = make_db();

    // Create 2 Person nodes and 1 Company node.
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Company {name: 'Acme'})")
        .expect("CREATE Acme");

    // Create 2 directed edges.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE KNOWS");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) \
         CREATE (a)-[:WORKS_AT]->(c)",
    )
    .expect("CREATE WORKS_AT");

    // Export.
    let json = db.export_json().expect("export_json must succeed");
    assert!(!json.is_empty(), "JSON must not be empty");

    // Deserialise and validate structure.
    let dump: GraphDump = serde_json::from_str(&json).expect("must deserialise to GraphDump");

    assert_eq!(
        dump.sparrowdb_version,
        env!("CARGO_PKG_VERSION"),
        "version must match current crate version"
    );
    assert!(
        dump.exported_at > 0,
        "exported_at must be a non-zero timestamp"
    );
    assert_eq!(
        dump.nodes.len(),
        3,
        "must export 3 nodes; got: {:?}",
        dump.nodes.len()
    );
    assert_eq!(
        dump.edges.len(),
        2,
        "must export 2 edges; got: {:?}",
        dump.edges.len()
    );

    // Check node properties are present.
    let alice = dump
        .nodes
        .iter()
        .find(|n| {
            n.label == "Person" && n.properties.get("name") == Some(&serde_json::json!("Alice"))
        })
        .expect("Alice node must be in dump");
    assert_eq!(
        alice.properties.get("age"),
        Some(&serde_json::json!(30i64)),
        "Alice age must be 30"
    );

    // Check edge types are present.
    let edge_types: Vec<&str> = dump.edges.iter().map(|e| e.rel_type.as_str()).collect();
    assert!(
        edge_types.contains(&"KNOWS"),
        "KNOWS edge must be present; edges: {:?}",
        dump.edges
    );
    assert!(
        edge_types.contains(&"WORKS_AT"),
        "WORKS_AT edge must be present; edges: {:?}",
        dump.edges
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Full round-trip — export then import into fresh DB
// ─────────────────────────────────────────────────────────────────────────────

/// After `import_json(export_json())`, the new database must contain the same
/// 3 nodes (with labels and properties) and 2 edges (with rel types).
#[test]
fn round_trip_export_import() {
    // ── Source database ───────────────────────────────────────────────────
    let (_src_dir, src_db) = make_db();

    src_db
        .execute("CREATE (n:Person {name: 'Alice', score: 42})")
        .expect("CREATE Alice");
    src_db
        .execute("CREATE (n:Person {name: 'Bob', score: 7})")
        .expect("CREATE Bob");
    src_db
        .execute("CREATE (n:Company {name: 'Sparrow Inc'})")
        .expect("CREATE Sparrow Inc");

    src_db
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("CREATE KNOWS");
    src_db
        .execute(
            "MATCH (a:Person {name: 'Bob'}), (c:Company {name: 'Sparrow Inc'}) \
             CREATE (a)-[:WORKS_AT]->(c)",
        )
        .expect("CREATE WORKS_AT");

    let json = src_db.export_json().expect("export_json");

    // ── Destination database ──────────────────────────────────────────────
    let (_dst_dir, dst_db) = make_db();
    dst_db.import_json(&json).expect("import_json must succeed");

    // ── Verify node count per label ───────────────────────────────────────
    let persons = dst_db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query persons");
    assert_eq!(
        persons.rows.len(),
        2,
        "must have 2 Person nodes; got: {:?}",
        persons.rows
    );

    let companies = dst_db
        .execute("MATCH (n:Company) RETURN n.name")
        .expect("query companies");
    assert_eq!(
        companies.rows.len(),
        1,
        "must have 1 Company node; got: {:?}",
        companies.rows
    );

    // ── Verify a specific property value ──────────────────────────────────
    let alice_result = dst_db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.score")
        .expect("query Alice score");
    assert_eq!(
        alice_result.rows.len(),
        1,
        "Alice must exist in imported DB"
    );
    assert_eq!(
        alice_result.rows[0][0],
        Value::Int64(42),
        "Alice score must be 42; got: {:?}",
        alice_result.rows[0][0]
    );

    // ── Verify edge count and types ───────────────────────────────────────
    let knows_edges = dst_db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("query KNOWS edges");
    assert_eq!(
        knows_edges.rows.len(),
        1,
        "must have 1 KNOWS edge; got: {:?}",
        knows_edges.rows
    );

    let works_edges = dst_db
        .execute("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a.name, b.name")
        .expect("query WORKS_AT edges");
    assert_eq!(
        works_edges.rows.len(),
        1,
        "must have 1 WORKS_AT edge; got: {:?}",
        works_edges.rows
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Empty database exports and imports cleanly
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn empty_db_round_trip() {
    let (_dir, db) = make_db();

    let json = db
        .export_json()
        .expect("export_json on empty DB must succeed");
    let dump: GraphDump = serde_json::from_str(&json).expect("empty dump must deserialise");

    assert_eq!(dump.nodes.len(), 0, "empty DB must have 0 nodes");
    assert_eq!(dump.edges.len(), 0, "empty DB must have 0 edges");

    // Import into a fresh DB — must succeed without errors.
    let (_dst_dir, dst_db) = make_db();
    dst_db
        .import_json(&json)
        .expect("import of empty dump must succeed");
}
