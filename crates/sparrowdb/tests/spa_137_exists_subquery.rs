//! End-to-end tests for EXISTS { } subquery predicate (SPA-137).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// EXISTS { } returns true when the relationship exists.
#[test]
fn exists_true_when_rel_exists() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person {name: 'Alice'}) WHERE EXISTS { (n)-[:KNOWS]->(:Person) } \
             RETURN n.name",
        )
        .expect("EXISTS subquery must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "Alice should match the EXISTS predicate"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "should return Alice's name"
    );
}

/// EXISTS { } returns false when no matching relationship exists.
#[test]
fn exists_false_when_no_rel() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    // No edge — Alice has no KNOWS relationships.

    let result = db
        .execute(
            "MATCH (n:Person {name: 'Alice'}) WHERE EXISTS { (n)-[:KNOWS]->(:Person) } \
             RETURN n.name",
        )
        .expect("EXISTS subquery no-match must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "Alice should NOT match the EXISTS predicate when no edge exists"
    );
}

/// EXISTS { } correctly filters: nodes with relationships pass, others do not.
#[test]
fn exists_in_where_filters_correctly() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Carol'})").unwrap();
    // Only Alice knows Bob.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->(:Person) } RETURN n.name")
        .expect("EXISTS subquery filter must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "only Alice has outgoing KNOWS relationships"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "Alice should be the only result"
    );
}
