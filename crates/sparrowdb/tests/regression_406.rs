//! Regression test for issue #406: MATCH ()-[r]->() RETURN count(r) throws "not found".
//!
//! Anonymous-endpoint relationship patterns (no label on either end) must succeed
//! on 0.1.22+ just as they did on 0.1.21.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// MATCH ()-[r]->() RETURN count(r) AS cnt must return 1 when 1 edge exists.
#[test]
fn anonymous_rel_count_directed() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH ()-[r]->() RETURN count(r) AS cnt")
        .expect("MATCH ()-[r]->() RETURN count(r) must not throw 'not found'");

    assert_eq!(result.rows.len(), 1, "must return one aggregated row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(1),
        "COUNT(r) must equal 1, got {:?}",
        result.rows[0][0]
    );
}

/// MATCH ()-[r]-() RETURN count(r) AS cnt (undirected) must also work.
#[test]
fn anonymous_rel_count_undirected() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH ()-[r]-() RETURN count(r) AS cnt")
        .expect("MATCH ()-[r]-() RETURN count(r) must not throw 'not found'");

    assert_eq!(result.rows.len(), 1, "must return one aggregated row");
    // Undirected: both (Alice->Bob) and (Bob<-Alice) are emitted, so count=2.
    // The key test: it must NOT throw.
    let cnt = match result.rows[0][0] {
        Value::Int64(n) => n,
        ref other => panic!("expected Int64, got {:?}", other),
    };
    assert!(
        cnt >= 1,
        "COUNT(r) for undirected must be >= 1, got {}",
        cnt
    );
}

/// Broader probes: various anonymous+mixed patterns must not error.
#[test]
fn mixed_anonymous_rel_patterns() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Knowledge {id: 'k1', title: 'GraphDB'})")
        .unwrap();
    db.execute("CREATE (b:Knowledge {id: 'k2', title: 'ML'})")
        .unwrap();
    db.execute(
        "MATCH (a:Knowledge {id: 'k1'}), (b:Knowledge {id: 'k2'}) CREATE (a)-[:RELATED_TO]->(b)",
    )
    .unwrap();

    // (a)-[r]->(b) bound endpoints
    let r1 = db
        .execute("MATCH (a)-[r]->(b) RETURN count(r) AS cnt")
        .expect("bound-endpoint anon-label must work");
    assert_eq!(r1.rows[0][0], Value::Int64(1));

    // ()-[r:RELATED_TO]->() typed rel with anon endpoints
    let r2 = db
        .execute("MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS cnt")
        .expect("typed rel with anon endpoints must work");
    assert_eq!(r2.rows[0][0], Value::Int64(1));

    // (n:Knowledge)-[r]->() mixed
    let r3 = db
        .execute("MATCH (n:Knowledge)-[r]->() RETURN count(r) AS cnt")
        .expect("labeled src + anon dst must work");
    assert_eq!(r3.rows[0][0], Value::Int64(1));
}
