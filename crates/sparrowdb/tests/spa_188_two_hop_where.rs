//! Regression tests for SPA-188: WHERE predicate ignored in 2-hop path matching.
//!
//! Before the fix, `execute_two_hop` evaluated and collected all candidate paths
//! but never applied `m.where_clause` to filter them.  Every path was returned
//! regardless of the WHERE condition.
//!
//! The fix adds proper `eval_where` evaluation after resolving both the source
//! and friend-of-friend nodes, building a combined binding map first.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Core regression test for SPA-188.
///
/// Graph:
///   Alice -[:KNOWS]-> Bob -[:KNOWS]-> Charlie
///   Alice -[:KNOWS]-> Bob -[:KNOWS]-> Dave
///
/// Query:
///   MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
///   WHERE c.name = 'Charlie' RETURN c.name
///
/// Expected: exactly one row containing String("Charlie").
/// Before fix: two rows (both Charlie and Dave returned).
#[test]
fn two_hop_where_filters_fof_by_name() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");
    db.execute("CREATE (:Person {name: 'Dave'})")
        .expect("CREATE Dave");

    // Alice -> Bob
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("edge Alice->Bob");

    // Bob -> Charlie
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .expect("edge Bob->Charlie");

    // Bob -> Dave
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (d:Person {name: 'Dave'}) CREATE (b)-[:KNOWS]->(d)",
    )
    .expect("edge Bob->Dave");

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE c.name = 'Charlie' RETURN c.name",
        )
        .expect("two-hop WHERE query");

    assert_eq!(
        result.rows.len(),
        1,
        "SPA-188: WHERE c.name = 'Charlie' must return exactly 1 row; got {} row(s): {:?}",
        result.rows.len(),
        result.rows
    );

    assert_eq!(
        result.rows[0][0],
        Value::String("Charlie".to_string()),
        "SPA-188: returned value must be String('Charlie'), got {:?}",
        result.rows[0][0]
    );
}

/// Complementary test: WHERE on a value that matches no node returns no rows.
#[test]
fn two_hop_where_no_match_returns_empty() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("edge Alice->Bob");
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .expect("edge Bob->Charlie");

    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE c.name = 'Zoe' RETURN c.name",
        )
        .expect("two-hop WHERE no-match query");

    assert!(
        result.rows.is_empty(),
        "SPA-188: WHERE c.name = 'Zoe' should return 0 rows; got {} row(s): {:?}",
        result.rows.len(),
        result.rows
    );
}

/// No WHERE clause still returns all 2-hop paths (regression guard).
#[test]
fn two_hop_without_where_returns_all_paths() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");
    db.execute("CREATE (:Person {name: 'Dave'})")
        .expect("CREATE Dave");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("edge Alice->Bob");
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .expect("edge Bob->Charlie");
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (d:Person {name: 'Dave'}) CREATE (b)-[:KNOWS]->(d)",
    )
    .expect("edge Bob->Dave");

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name")
        .expect("two-hop no-WHERE query");

    assert_eq!(
        result.rows.len(),
        2,
        "Two-hop without WHERE should return 2 paths (Charlie and Dave); got {} row(s): {:?}",
        result.rows.len(),
        result.rows
    );

    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Charlie", "Dave"]);
}
