//! Regression tests for issue #355: MATCH…WITH…MATCH property lookup returns 0 rows.
//!
//! When a node variable is passed through WITH and then used as the source node
//! in a second MATCH with a relationship hop, the second MATCH returns 0 rows.
//!
//! Example query:
//!   MATCH (a:Person {name: "Alice"})
//!   WITH a
//!   MATCH (a)-[:KNOWS]->(b:Person)
//!   RETURN b.name

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_social_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");

    // Create nodes
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol', age: 35})")
        .expect("CREATE Carol");

    // Create relationships: Alice knows Bob and Carol; Bob knows Carol
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("Alice KNOWS Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .expect("Alice KNOWS Carol");
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .expect("Bob KNOWS Carol");

    (dir, db)
}

/// Core regression: MATCH (a) WITH a MATCH (a)-[:REL]->(b) RETURN b.name
/// The second MATCH must use the NodeRef from WITH to constrain the src node.
#[test]
fn regression_355_match_with_node_var_match_hop_returns_results() {
    let (_dir, db) = make_social_db();

    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}) \
             WITH a \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN b.name",
        )
        .expect("MATCH…WITH a…MATCH hop must not error");

    assert_eq!(result.columns, vec!["b.name"]);
    assert_eq!(
        result.rows.len(),
        2,
        "Alice knows Bob and Carol — expected 2 rows, got {}.\nRows: {:?}",
        result.rows.len(),
        result.rows
    );

    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String for b.name, got {:?}", other),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["Bob".to_string(), "Carol".to_string()],
        "expected Alice's two KNOWS neighbors"
    );
}

/// Variant: WITH a AS person — node alias, then hop from person.
#[test]
fn regression_355_with_node_alias_then_hop() {
    let (_dir, db) = make_social_db();

    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}) \
             WITH a AS person \
             MATCH (person)-[:KNOWS]->(b:Person) \
             RETURN b.name",
        )
        .expect("MATCH…WITH a AS person…MATCH hop must not error");

    assert_eq!(result.columns, vec!["b.name"]);
    assert_eq!(
        result.rows.len(),
        2,
        "Alice (as 'person') knows Bob and Carol — expected 2 rows, got {}.\nRows: {:?}",
        result.rows.len(),
        result.rows
    );
}

/// Variant: filter in first MATCH, pass node var, hop in second MATCH.
/// `MATCH (a:Person) WHERE a.age > 25 WITH a MATCH (a)-[:KNOWS]->(b:Person) RETURN b.name`
/// Only Alice (30) and Carol (35) survive the WHERE. But only Alice has outgoing KNOWS edges.
#[test]
fn regression_355_match_where_with_node_var_match_hop() {
    let (_dir, db) = make_social_db();

    let result = db
        .execute(
            "MATCH (a:Person) \
             WHERE a.age > 25 \
             WITH a \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN b.name",
        )
        .expect("MATCH WHERE…WITH a…MATCH hop must not error");

    // Alice (30) has 2 KNOWS edges (Bob, Carol).
    // Carol (35) has 0 KNOWS edges.
    assert_eq!(result.columns, vec!["b.name"]);
    assert_eq!(
        result.rows.len(),
        2,
        "Alice knows 2 people; Carol has no outgoing KNOWS — expected 2 rows, got {}.\nRows: {:?}",
        result.rows.len(),
        result.rows
    );

    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(names, vec!["Bob".to_string(), "Carol".to_string()]);
}
