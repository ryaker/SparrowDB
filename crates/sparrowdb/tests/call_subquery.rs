//! End-to-end tests for CALL { } subquery support (issue #290).
//!
//! Covers:
//! - Unit subquery: `CALL { MATCH … RETURN … } RETURN …`
//! - Correlated subquery: `MATCH (p) CALL { WITH p MATCH (p)-[:R]->(f) RETURN count(f) AS fc } RETURN p.name, fc`

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Unit subquery ─────────────────────────────────────────────────────────────

/// Basic unit subquery: `CALL { MATCH (n:Person) RETURN n.name AS name } RETURN name`
#[test]
fn unit_subquery_returns_all_rows() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();

    let r = db
        .execute("CALL { MATCH (n:Person) RETURN n.name AS name } RETURN name")
        .expect("unit subquery must execute without error");

    assert_eq!(r.rows.len(), 2, "should return two rows (Alice and Bob)");

    let mut names: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob"]);
}

/// Unit subquery with no matching rows returns empty result.
#[test]
fn unit_subquery_empty_when_no_match() {
    let (_dir, db) = make_db();
    // No nodes created — subquery should produce zero rows.

    let r = db
        .execute("CALL { MATCH (n:Person) RETURN n.name AS name } RETURN name")
        .expect("unit subquery on empty graph must not error");

    assert_eq!(r.rows.len(), 0, "empty graph → empty subquery result");
}

/// Unit subquery with LIMIT inside the subquery.
#[test]
fn unit_subquery_with_limit() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Carol'})").unwrap();

    let r = db
        .execute("CALL { MATCH (n:Person) RETURN n.name AS name LIMIT 2 } RETURN name")
        .expect("unit subquery with LIMIT must execute");

    assert_eq!(
        r.rows.len(),
        2,
        "LIMIT 2 inside subquery should produce 2 rows"
    );
}

// ── Correlated subquery ───────────────────────────────────────────────────────

/// Correlated subquery: count friends for each person.
///
/// Alice knows Bob → friendCount = 1
/// Bob has no outgoing KNOWS → friendCount = 0 (inner join drops Bob's row if
/// no friends; to get 0 we use a match that uses count — but the test for zero
/// rows is that Bob's row is not in the result when there are no friends).
///
/// This test verifies the row with Alice has fc = 1.
#[test]
fn correlated_subquery_counts_friends() {
    let (_dir, db) = make_db();

    // Create Alice and Bob.
    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    // Alice knows Bob.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // Correlated: for each person, count how many they know.
    // The inner RETURN uses count(f), which is an aggregate.  Even when the
    // inner MATCH produces zero rows for a given outer row (Bob has no
    // outgoing KNOWS), the aggregate collapses to fc=0 and that outer row is
    // still included (standard Cypher/Neo4j aggregate semantics).
    let r = db
        .execute(
            "MATCH (p:Person) \
             CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS fc } \
             RETURN p.name, fc",
        )
        .expect("correlated subquery must execute without error");

    // Alice has 1 friend → fc=1.  Bob has 0 friends → fc=0 (aggregate over
    // empty set).  Both outer rows survive because count() always produces
    // exactly one output row.
    assert_eq!(
        r.rows.len(),
        2,
        "both Alice and Bob should appear (fc=1 and fc=0)"
    );

    // Find Alice's row and Bob's row by name.
    let alice_row = r
        .rows
        .iter()
        .find(|row| row[0] == Value::String("Alice".to_string()))
        .expect("Alice's row must be present");
    let bob_row = r
        .rows
        .iter()
        .find(|row| row[0] == Value::String("Bob".to_string()))
        .expect("Bob's row must be present");

    assert_eq!(alice_row[1], Value::Int64(1), "Alice knows 1 person");
    assert_eq!(bob_row[1], Value::Int64(0), "Bob knows 0 people");
}

/// Correlated subquery: collect friend names per person.
#[test]
fn correlated_subquery_collects_friend_names() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Carol'})").unwrap();
    // Alice → Bob, Alice → Carol
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .unwrap();

    let r = db
        .execute(
            "MATCH (p:Person {name: 'Alice'}) \
             CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fname } \
             RETURN p.name, fname",
        )
        .expect("correlated subquery collect must execute");

    // 2 rows: (Alice, Bob) and (Alice, Carol).
    assert_eq!(r.rows.len(), 2, "Alice knows 2 people → 2 rows");
    let mut friend_names: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();
    friend_names.sort();
    assert_eq!(friend_names, vec!["Bob", "Carol"]);
}

// ── Parser tests ──────────────────────────────────────────────────────────────

/// Verify that procedure CALL syntax is still parsed correctly.
#[test]
fn procedure_call_still_works() {
    use sparrowdb_cypher::parser::parse;

    let stmt = parse("CALL db.schema() YIELD type, name RETURN type, name").unwrap();
    assert!(
        matches!(stmt, sparrowdb_cypher::ast::Statement::Call(_)),
        "procedure CALL must still parse as Statement::Call"
    );
}

/// Verify that `CALL { }` is parsed as `Statement::CallSubquery`.
#[test]
fn call_subquery_parses_as_call_subquery_variant() {
    use sparrowdb_cypher::parser::parse;

    let stmt = parse("CALL { MATCH (n:Person) RETURN n.name AS name } RETURN name").unwrap();
    assert!(
        matches!(stmt, sparrowdb_cypher::ast::Statement::CallSubquery { .. }),
        "CALL {{ }} must parse as Statement::CallSubquery, got {:?}",
        std::mem::discriminant(&stmt)
    );
}

/// Verify that the correlated form `CALL { WITH n ... }` parses correctly.
#[test]
fn correlated_call_subquery_parses() {
    use sparrowdb_cypher::parser::parse;

    let stmt = parse(
        "MATCH (p:Person) \
         CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS fc } \
         RETURN p.name, fc",
    )
    .unwrap();
    assert!(
        matches!(stmt, sparrowdb_cypher::ast::Statement::Pipeline(_)),
        "MATCH … CALL {{ }} … RETURN must parse as a Pipeline"
    );
}
