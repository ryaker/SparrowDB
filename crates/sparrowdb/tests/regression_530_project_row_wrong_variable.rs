//! Regression for #530 — a `PropAccess` on a variable other than the one
//! being scanned reached `project_row` and silently became `Null`.
//!
//! Fixture (derived by hand): Alice -KNOWS-> Carol <-KNOWS- Bob.
//! The only common neighbour of Alice and Bob is Carol, so the mutual-
//! neighbours query yields exactly one row: a.name = "Alice", x.name = "Carol".

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

fn setup() -> (GraphDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    for q in [
        "CREATE (n:Person {name: 'Alice'})",
        "CREATE (n:Person {name: 'Bob'})",
        "CREATE (n:Person {name: 'Carol'})",
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)",
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
    ] {
        db.execute(q).unwrap();
    }
    (db, dir)
}

#[test]
fn mutual_neighbours_return_of_endpoint_property_is_not_null() {
    let (db, _d) = setup();
    let r = db
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(x:Person)<-[:KNOWS]-(b:Person {name: 'Bob'}) \
             RETURN a.name, x.name",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1, "exactly one common neighbour");
    assert_eq!(r.rows[0][1], Value::String("Carol".into()), "x.name");
    assert_eq!(
        r.rows[0][0],
        Value::String("Alice".into()),
        "a.name must not be Null"
    );
}

#[test]
fn mutual_neighbours_return_of_second_endpoint_property_is_not_null() {
    let (db, _d) = setup();
    let r = db
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(x:Person)<-[:KNOWS]-(b:Person {name: 'Bob'}) \
             RETURN b.name",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0][0],
        Value::String("Bob".into()),
        "b.name must not be Null"
    );
}
