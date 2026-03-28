//! Integration tests for GraphDb::export_json / import_json.

use sparrowdb::{open, GraphDump};
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

#[test]
fn export_json_produces_valid_structure() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Company {name: 'Acme'})")
        .expect("CREATE Acme");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE KNOWS");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(c)",
    )
    .expect("CREATE WORKS_AT");
    let json = db.export_json().expect("export_json must succeed");
    assert!(!json.is_empty());
    let dump: GraphDump = serde_json::from_str(&json).expect("must deserialise");
    assert_eq!(dump.sparrowdb_version, env!("CARGO_PKG_VERSION"));
    assert!(dump.exported_at > 0);
    assert_eq!(dump.nodes.len(), 3, "must export 3 nodes");
    assert_eq!(dump.edges.len(), 2, "must export 2 edges");
    let alice = dump
        .nodes
        .iter()
        .find(|n| {
            n.label == "Person" && n.properties.get("name") == Some(&serde_json::json!("Alice"))
        })
        .expect("Alice must be in dump");
    assert_eq!(alice.properties.get("age"), Some(&serde_json::json!(30i64)));
    let edge_types: Vec<&str> = dump.edges.iter().map(|e| e.rel_type.as_str()).collect();
    assert!(edge_types.contains(&"KNOWS"));
    assert!(edge_types.contains(&"WORKS_AT"));
}

#[test]
fn round_trip_export_import() {
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
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("CREATE KNOWS");
    src_db.execute("MATCH (a:Person {name: 'Bob'}), (c:Company {name: 'Sparrow Inc'}) CREATE (a)-[:WORKS_AT]->(c)").expect("CREATE WORKS_AT");
    let json = src_db.export_json().expect("export_json");
    let (_dst_dir, dst_db) = make_db();
    dst_db.import_json(&json).expect("import_json must succeed");
    let persons = dst_db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query persons");
    assert_eq!(persons.rows.len(), 2, "must have 2 Person nodes");
    let companies = dst_db
        .execute("MATCH (n:Company) RETURN n.name")
        .expect("query companies");
    assert_eq!(companies.rows.len(), 1, "must have 1 Company node");
    let alice_result = dst_db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.score")
        .expect("query Alice score");
    assert_eq!(alice_result.rows.len(), 1, "Alice must exist");
    assert_eq!(
        alice_result.rows[0][0],
        Value::Int64(42),
        "Alice score must be 42"
    );
    let knows_edges = dst_db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("query KNOWS");
    assert_eq!(knows_edges.rows.len(), 1, "must have 1 KNOWS edge");
    let works_edges = dst_db
        .execute("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a.name, b.name")
        .expect("query WORKS_AT");
    assert_eq!(works_edges.rows.len(), 1, "must have 1 WORKS_AT edge");
}

/// After a round-trip export -> import, no node must carry a
/// `_sparrow_export_id` property (closes #313).
#[test]
fn import_does_not_pollute_nodes_with_export_id() {
    let (_src_dir, src_db) = make_db();
    src_db
        .execute("CREATE (n:Widget {name: 'cog', weight: 5})")
        .expect("CREATE cog");
    src_db
        .execute("CREATE (n:Widget {name: 'gear', weight: 3})")
        .expect("CREATE gear");
    src_db.execute("MATCH (a:Widget {name: 'cog'}), (b:Widget {name: 'gear'}) CREATE (a)-[:MESHES_WITH]->(b)").expect("CREATE MESHES_WITH");
    let json = src_db.export_json().expect("export_json");
    let (_dst_dir, dst_db) = make_db();
    dst_db.import_json(&json).expect("import_json");
    let result = dst_db
        .execute("MATCH (n:Widget) RETURN n")
        .expect("query Widget nodes");
    assert_eq!(
        result.rows.len(),
        2,
        "must have 2 Widget nodes after import"
    );
    for row in &result.rows {
        if let Value::Map(props) = &row[0] {
            for (key, _) in props.iter() {
                assert_ne!(
                    key,
                    "_sparrow_export_id",
                    "imported node must NOT have _sparrow_export_id; got keys: {:?}",
                    props.iter().map(|(k, _)| k).collect::<Vec<_>>()
                );
            }
        }
    }
    let re_exported = dst_db.export_json().expect("re-export");
    assert!(
        !re_exported.contains("_sparrow_export_id"),
        "_sparrow_export_id must not appear in re-exported JSON:\n{re_exported}"
    );
}

#[test]
fn empty_db_round_trip() {
    let (_dir, db) = make_db();
    let json = db
        .export_json()
        .expect("export_json on empty DB must succeed");
    let dump: GraphDump = serde_json::from_str(&json).expect("empty dump must deserialise");
    assert_eq!(dump.nodes.len(), 0);
    assert_eq!(dump.edges.len(), 0);
    let (_dst_dir, dst_db) = make_db();
    dst_db
        .import_json(&json)
        .expect("import of empty dump must succeed");
}
