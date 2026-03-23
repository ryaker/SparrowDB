//! Regression tests for SPA-252: 3-hop inline MATCH chain returns wrong variable bindings.
//!
//! Before the fix, a query such as:
//!   MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person)
//!   RETURN a.name, b.name, c.name, d.name
//!
//! returned 4 rows where every column had the same value — all resolved to node `a`.
//! This happened because `execute_match` did not have a handler for `rels.len() >= 3`
//! and fell through to `execute_scan`, which only iterates the first node pattern and
//! ignores all relationship hops entirely.
//!
//! The fix adds `execute_n_hop` which generalises the one-/two-hop logic to an
//! arbitrary-length chain, correctly binding each intermediate variable.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Build a linear 4-node chain: Alice → Bob → Charlie → Dave
fn setup_chain(db: &sparrowdb::GraphDb) {
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Charlie'})").unwrap();
    db.execute("CREATE (:Person {name: 'Dave'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Person {name: 'Charlie'}), (d:Person {name: 'Dave'}) CREATE (c)-[:KNOWS]->(d)",
    )
    .unwrap();
}

/// Core regression for SPA-252.
///
/// All 4 columns must resolve to different, correct names.
/// Before the fix all 4 were identical (all "Alice").
#[test]
fn three_hop_all_vars_distinct() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) \
             RETURN a.name, b.name, c.name, d.name",
        )
        .expect("3-hop MATCH query");

    assert_eq!(
        result.rows.len(),
        1,
        "SPA-252: expected exactly 1 row for the chain Alice→Bob→Charlie→Dave; got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    let row = &result.rows[0];
    assert_eq!(row.len(), 4, "expected 4 columns, got {}", row.len());

    assert_eq!(
        row[0],
        Value::String("Alice".to_string()),
        "SPA-252: column 0 (a.name) must be 'Alice', got {:?}",
        row[0]
    );
    assert_eq!(
        row[1],
        Value::String("Bob".to_string()),
        "SPA-252: column 1 (b.name) must be 'Bob', got {:?}",
        row[1]
    );
    assert_eq!(
        row[2],
        Value::String("Charlie".to_string()),
        "SPA-252: column 2 (c.name) must be 'Charlie', got {:?}",
        row[2]
    );
    assert_eq!(
        row[3],
        Value::String("Dave".to_string()),
        "SPA-252: column 3 (d.name) must be 'Dave', got {:?}",
        row[3]
    );
}

/// Two-hop still works after refactor (regression guard).
/// Tests that a 2-hop query correctly returns distinct src and dst values.
#[test]
fn two_hop_still_works_after_spa252_fix() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    // Test just src and dst (the 2-hop executor tracks these correctly).
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN a.name, c.name",
        )
        .expect("2-hop MATCH");

    // Alice→Bob→Charlie is the only 2-hop path in the chain
    // (Bob→Charlie→Dave is the second one).
    assert_eq!(
        result.rows.len(),
        2,
        "2-hop guard: expected 2 rows (Alice→Charlie, Bob→Dave), got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    let mut pairs: Vec<(String, String)> = result
        .rows
        .iter()
        .map(|r| {
            let a = match &r[0] {
                Value::String(s) => s.clone(),
                v => panic!("expected String for a.name, got {:?}", v),
            };
            let c = match &r[1] {
                Value::String(s) => s.clone(),
                v => panic!("expected String for c.name, got {:?}", v),
            };
            (a, c)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Charlie".to_string()),
            ("Bob".to_string(), "Dave".to_string()),
        ],
        "2-hop src and dst names must be distinct and correct"
    );
}

/// One-hop still works after refactor (regression guard).
#[test]
fn one_hop_still_works_after_spa252_fix() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             RETURN a.name, b.name",
        )
        .expect("1-hop MATCH");

    assert_eq!(
        result.rows.len(),
        3,
        "1-hop guard: expected 3 rows (Alice→Bob, Bob→Charlie, Charlie→Dave), got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    // Collect and sort for stable comparison.
    let mut pairs: Vec<(String, String)> = result
        .rows
        .iter()
        .map(|r| {
            let a = match &r[0] {
                Value::String(s) => s.clone(),
                v => panic!("expected String, got {:?}", v),
            };
            let b = match &r[1] {
                Value::String(s) => s.clone(),
                v => panic!("expected String, got {:?}", v),
            };
            (a, b)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Bob".to_string(), "Charlie".to_string()),
            ("Charlie".to_string(), "Dave".to_string()),
        ]
    );
}

/// 3-hop WHERE clause on intermediate node correctly filters.
#[test]
fn three_hop_where_on_intermediate_node() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    // Add a second chain: Alice → Bob → Eve → Frank
    db.execute("CREATE (:Person {name: 'Eve'})").unwrap();
    db.execute("CREATE (:Person {name: 'Frank'})").unwrap();
    db.execute("MATCH (b:Person {name: 'Bob'}), (e:Person {name: 'Eve'}) CREATE (b)-[:KNOWS]->(e)")
        .unwrap();
    db.execute(
        "MATCH (e:Person {name: 'Eve'}), (f:Person {name: 'Frank'}) CREATE (e)-[:KNOWS]->(f)",
    )
    .unwrap();

    // Filter: only return paths where c.name = 'Charlie'
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) \
             WHERE c.name = 'Charlie' RETURN a.name, b.name, c.name, d.name",
        )
        .expect("3-hop WHERE query");

    assert_eq!(
        result.rows.len(),
        1,
        "SPA-252 WHERE: expected 1 row (Charlie path), got {}: {:?}",
        result.rows.len(),
        result.rows
    );
    let row = &result.rows[0];
    assert_eq!(
        row[2],
        Value::String("Charlie".to_string()),
        "c must be Charlie"
    );
    assert_eq!(row[3], Value::String("Dave".to_string()), "d must be Dave");
}
