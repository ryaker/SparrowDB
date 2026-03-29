//! Regression test for #366 — CREATE ... RETURN clause.
//!
//! The Cypher parser previously treated CREATE as a terminal clause, rejecting
//! any RETURN clause that followed.  This test verifies that CREATE ... RETURN
//! parses and executes correctly, returning the newly created node's properties.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use tempfile::tempdir;

#[test]
fn create_with_return_scalar_props() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let result = db
        .execute("CREATE (n:Person {name: \"Alice\", age: 30}) RETURN n.name, n.age")
        .unwrap();

    assert_eq!(result.columns, vec!["n.name", "n.age"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    assert_eq!(result.rows[0][1], Value::Int64(30));
}

#[test]
fn create_with_return_id() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let result = db
        .execute("CREATE (n:Person {name: \"Bob\"}) RETURN id(n), n.name")
        .unwrap();

    assert_eq!(result.columns, vec!["id(n)", "n.name"]);
    assert_eq!(result.rows.len(), 1);
    // id(n) is a u64-packed NodeId — just verify it's a non-null Int64
    assert!(matches!(result.rows[0][0], Value::Int64(_)));
    assert_eq!(result.rows[0][1], Value::String("Bob".into()));
}

#[test]
fn create_with_return_whole_node() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let result = db
        .execute("CREATE (n:Person {name: \"Carol\", active: true}) RETURN n")
        .unwrap();

    assert_eq!(result.columns, vec!["n"]);
    assert_eq!(result.rows.len(), 1);
    // Returning a bare variable should produce a Map value
    assert!(matches!(result.rows[0][0], Value::Map(_)));
    if let Value::Map(ref m) = result.rows[0][0] {
        let name_val = m.iter().find(|(k, _)| k == "name").map(|(_, v)| v);
        assert_eq!(name_val, Some(&Value::String("Carol".into())));
    }
}

#[test]
fn create_multiple_nodes_with_return() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    // CREATE two nodes, RETURN both
    db.execute("CREATE (:Person {name: \"Alice\"})").unwrap();
    db.execute("CREATE (:Person {name: \"Bob\"})").unwrap();

    // Now verify they exist
    let result = db
        .execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}
