//! Regression tests for undirected relationship pattern matching (SPA-193).
//!
//! `(a)-[r]-(b)` must match edges in **both** directions, returning one row
//! per traversal direction.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Basic undirected match: Alice→Bob should produce two rows:
/// (Alice, Bob) for the forward direction and (Bob, Alice) for the
/// backward direction.
#[test]
fn undirected_returns_both_directions() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[r]-(b:Person) RETURN a.name, b.name")
        .expect("undirected pattern must not error");

    // Should produce exactly 2 rows: (Alice, Bob) and (Bob, Alice).
    assert_eq!(result.rows.len(), 2, "expected 2 rows (one per direction)");

    let mut pairs: Vec<(String, String)> = result
        .rows
        .iter()
        .map(|row| {
            let a = match &row[0] {
                Value::String(s) => s.clone(),
                other => panic!("expected string, got {:?}", other),
            };
            let b = match &row[1] {
                Value::String(s) => s.clone(),
                other => panic!("expected string, got {:?}", other),
            };
            (a, b)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Bob".to_string(), "Alice".to_string()),
        ],
        "rows must include both (Alice,Bob) and (Bob,Alice)"
    );
}

/// Undirected match with a source-side prop filter:
/// `MATCH (a:Person {name:'Alice'})-[r]-(b) RETURN b.name`
/// should return only Bob.
#[test]
fn undirected_with_src_prop_filter() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (a:Person {name:'Alice'})-[r]-(b:Person) RETURN b.name")
        .expect("undirected pattern with filter must not error");

    assert_eq!(result.rows.len(), 1, "expected exactly 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Bob".to_string()),
        "expected Bob as the neighbor"
    );
}

/// Undirected match from the *destination* side:
/// `MATCH (a:Person {name:'Bob'})-[r]-(b) RETURN b.name`
/// should return Alice (following the edge backward).
#[test]
fn undirected_from_destination_side() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (a:Person {name:'Bob'})-[r]-(b:Person) RETURN b.name")
        .expect("undirected pattern from dst side must not error");

    assert_eq!(result.rows.len(), 1, "expected exactly 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "expected Alice as the neighbor of Bob via backward edge"
    );
}
