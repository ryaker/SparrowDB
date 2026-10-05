//! Issue #476: `DELETE n RETURN count(n)` when one node is matched by several rows.
//!
//! Semantic ruling: Cypher counts ROWS, not distinct nodes. `count(n)` after
//! `DELETE n` counts every row that reached the DELETE, so a node matched by two
//! rows contributes 2. (openCypher/Neo4j plan an Eager between MATCH and DELETE,
//! so every row is matched against the pre-statement state; deleting an
//! already-deleted node is a no-op.) The graph state is the deduplicated set of
//! deletions. These tests pin that; expected values are derived by hand.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;
use tempfile::tempdir;

fn open_db() -> (GraphDb, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    (db, dir)
}

fn id_row(id: &str) -> Value {
    Value::Map(vec![("id".to_string(), Value::String(id.into()))])
}

/// Fixture: Items a, b, c. UNWIND rows: a, a, b  => 3 rows reach DELETE
/// (a twice, b once). Distinct nodes deleted = 2; rows = 3. count(n) = 3.
/// Remaining graph = {c}.
#[test]
fn unwind_duplicate_row_counts_rows_and_deletes_node_once() {
    let (db, _dir) = open_db();
    for id in ["a", "b", "c"] {
        db.execute(&format!("CREATE (n:Item {{id: '{id}'}})"))
            .unwrap();
    }
    let p = HashMap::from([(
        "ids".to_string(),
        Value::List(vec![id_row("a"), id_row("a"), id_row("b")]),
    )]);
    let r = db
        .execute_with_params(
            "UNWIND $ids AS row MATCH (n:Item {id: row.id}) DELETE n RETURN count(n)",
            p,
        )
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int64(3)]], "3 rows reached DELETE");
    let rem = db.execute("MATCH (n:Item) RETURN n.id").unwrap();
    assert_eq!(rem.rows, vec![vec![Value::String("c".into())]]);
}

/// Plain (non-UNWIND) path. `scan_match_mutate` accepts exactly one node
/// pattern, so a node can be scanned at most once and rows == distinct nodes.
/// Fixture: Items a, b, c, all matched. count(n) = 3, graph left empty of Items.
#[test]
fn plain_single_pattern_counts_each_node_once() {
    let (db, _dir) = open_db();
    for id in ["a", "b", "c"] {
        db.execute(&format!("CREATE (n:Item {{id: '{id}'}})"))
            .unwrap();
    }
    let r = db
        .execute("MATCH (n:Item) DELETE n RETURN count(n)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int64(3)]]);
    assert!(db
        .execute("MATCH (n:Item) RETURN n.id")
        .unwrap()
        .rows
        .is_empty());
}

/// The only plain shapes that could produce overlapping matches (a cross
/// product `(s), (n)` or a hop `(s)-[]->(n)` where two sources share a target)
/// are rejected with an error rather than counted, so no over-count is
/// reachable. Fixture: s1, s2, x; nothing may be deleted by the rejected query.
#[test]
fn plain_overlapping_shapes_are_rejected_not_miscounted() {
    let (db, _dir) = open_db();
    db.execute("CREATE (s:Src {id: 's1'})").unwrap();
    db.execute("CREATE (s:Src {id: 's2'})").unwrap();
    db.execute("CREATE (n:Item {id: 'x'})").unwrap();
    assert!(db
        .execute("MATCH (s:Src), (n:Item) DELETE n RETURN count(n)")
        .is_err());
    assert!(db
        .execute("MATCH (s:Src)-[:KNOWS]->(n:Item) DELETE n RETURN count(n)")
        .is_err());
    assert_eq!(
        db.execute("MATCH (n:Item) RETURN n.id").unwrap().rows.len(),
        1
    );
}
