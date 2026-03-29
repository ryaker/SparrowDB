//! Regression/characterization tests for #368 — multi-clause CREATE patterns.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use tempfile::tempdir;

/// CREATE (a:Person), (b:Person) in one statement — two nodes
#[test]
fn create_two_nodes_in_one_statement() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (:Person {name: \"Alice\", age: 30}), (:Person {name: \"Bob\", age: 25})")
        .expect("CREATE two nodes should succeed");

    let result = db
        .execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2, "expected 2 Person nodes");
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    assert_eq!(result.rows[1][0], Value::String("Bob".into()));
}

/// CREATE inline path — both nodes created, edge connecting them
#[test]
fn create_inline_path_with_edge() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute(
        "CREATE (a:Person {name: \"Alice\"})-[r:KNOWS {since: 2020}]->(b:Person {name: \"Bob\"})",
    )
    .expect("CREATE inline path should succeed");

    // Both nodes exist
    let nodes = db
        .execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        .unwrap();
    assert_eq!(nodes.rows.len(), 2, "expected 2 Person nodes");

    // Edge exists with property
    let edges = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r.since")
        .unwrap();
    assert_eq!(edges.rows.len(), 1, "expected 1 KNOWS edge");
    assert_eq!(edges.rows[0][2], Value::Int64(2020));
}

/// CREATE inline path without edge properties
#[test]
fn create_inline_path_without_edge_props() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:Person {name: \"Carol\"})-[:KNOWS]->(b:Person {name: \"Dave\"})")
        .expect("CREATE inline path without edge props should succeed");

    let edges = db
        .execute("MATCH (a)-[r:KNOWS]->(b) RETURN a.name, b.name")
        .unwrap();
    assert_eq!(edges.rows.len(), 1);
}

/// CREATE three nodes in one statement
#[test]
fn create_three_nodes_one_statement() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (:Person {name: \"A\"}), (:Person {name: \"B\"}), (:Person {name: \"C\"})")
        .expect("CREATE three nodes should succeed");

    let result = db.execute("MATCH (n:Person) RETURN COUNT(*)").unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(3));
}
