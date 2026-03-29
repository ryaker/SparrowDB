//! Regression tests for #369 — hop queries with AS aliases return Null values.
//!
//! `MATCH (n)-[r]->(m) RETURN n.name AS from, type(r) AS rel, m.name AS to`
//! was returning `[[Null, Null, Null], ...]` because `project_hop_row` matched
//! on the column name string (e.g. `"from"`) instead of the underlying expr
//! (`n.name`), so the dot-split check never matched.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use tempfile::tempdir;

fn setup_graph() -> (GraphDb, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute(
        "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})",
    )
    .expect("CREATE should succeed");
    (db, dir)
}

/// One-hop: RETURN with AS aliases must produce correct values (not Null).
#[test]
fn one_hop_return_aliases_not_null() {
    let (db, _dir) = setup_graph();
    let result = db
        .execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS from_name, type(r) AS rel_type, b.name AS to_name",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1, "expected 1 row");
    let row = &result.rows[0];
    assert_eq!(
        row[0],
        Value::String("Alice".into()),
        "from_name alias should resolve to Alice, got {:?}",
        row[0]
    );
    assert_eq!(
        row[1],
        Value::String("KNOWS".into()),
        "rel_type alias should resolve to KNOWS, got {:?}",
        row[1]
    );
    assert_eq!(
        row[2],
        Value::String("Bob".into()),
        "to_name alias should resolve to Bob, got {:?}",
        row[2]
    );
}

/// One-hop: column headers must use the alias names.
#[test]
fn one_hop_alias_column_names() {
    let (db, _dir) = setup_graph();
    let result = db
        .execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS from_name, b.name AS to_name",
        )
        .unwrap();
    assert_eq!(result.columns, vec!["from_name", "to_name"]);
}

/// One-hop unlabeled: generic `MATCH (n)-[r]->(m) RETURN n.name AS from, type(r) AS rel, m.name AS to`.
#[test]
fn one_hop_unlabeled_return_aliases() {
    let (db, _dir) = setup_graph();
    let result = db
        .execute("MATCH (n)-[r]->(m) RETURN n.name AS from, type(r) AS rel, m.name AS to")
        .unwrap();
    assert_eq!(result.rows.len(), 1, "expected 1 row");
    let row = &result.rows[0];
    assert_eq!(
        row[0],
        Value::String("Alice".into()),
        "from alias must be Alice, got {:?}",
        row[0]
    );
    assert_ne!(row[0], Value::Null, "from must not be Null");
    assert_ne!(row[1], Value::Null, "rel must not be Null");
    assert_ne!(row[2], Value::Null, "to must not be Null");
}
