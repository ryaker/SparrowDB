//! End-to-end tests for shortestPath() BFS query (SPA-136).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// shortestPath returns 1 when two nodes share a direct edge.
#[test]
fn shortest_path_direct() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             RETURN shortestPath((a)-[:KNOWS*]->(b))",
        )
        .expect("shortestPath query must not error");

    assert_eq!(result.rows.len(), 1, "expected one result row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(1),
        "direct edge should have path length 1"
    );
}

/// shortestPath returns 2 when nodes are connected through an intermediate node.
#[test]
fn shortest_path_2_hops() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Charlie'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Charlie'}), (c:Person {name: 'Bob'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             RETURN shortestPath((a)-[:KNOWS*]->(b))",
        )
        .expect("shortestPath 2-hop query must not error");

    assert_eq!(result.rows.len(), 1, "expected one result row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(2),
        "2-hop path should have length 2"
    );
}

/// shortestPath returns NULL when no path exists between the two nodes.
#[test]
fn shortest_path_no_path() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    // No edge between Alice and Bob.

    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             RETURN shortestPath((a)-[:KNOWS*]->(b))",
        )
        .expect("shortestPath no-path query must not error");

    assert_eq!(result.rows.len(), 1, "expected one result row");
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "NULL expected when no path exists"
    );
}
