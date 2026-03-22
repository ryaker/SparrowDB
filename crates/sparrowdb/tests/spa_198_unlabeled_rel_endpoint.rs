//! Regression tests for SPA-198: unlabeled relationship endpoint should match any node.
//!
//! When one endpoint in a MATCH pattern has a label and the other doesn't,
//! the unlabeled endpoint must match any node reachable via the edge,
//! regardless of its label.  Currently supported: unlabeled destination `(b)`.
//! Unlabeled source traversal (scanning all labels) is tracked as a follow-up.

use sparrowdb::open;
use sparrowdb_execution::types::Value as ExecValue;

fn db_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

/// `MATCH (a:Person)-[r]->(b) RETURN b.content` — unlabeled destination.
///
/// Creates a Person→Knowledge edge and expects the Knowledge node's `content`
/// property to be returned when the destination is unlabeled.
#[test]
fn spa198_unlabeled_dst_matches_any_node() {
    let dir = db_dir();
    let db = open(dir.path()).expect("open db");

    // Create nodes and edge via Cypher.
    db.execute("CREATE (a:Person {name:\"Alice\"})")
        .expect("CREATE Person");
    // Use a short string (≤7 bytes) to fit within the inline storage limit.
    db.execute("CREATE (b:Knowledge {content:\"factA\"})")
        .expect("CREATE Knowledge");
    db.execute("MATCH (a:Person {name:\"Alice\"}),(b:Knowledge) CREATE (a)-[:KNOWS]->(b)")
        .expect("CREATE edge");

    // Query with unlabeled destination — should return the Knowledge node.
    let result = db
        .execute("MATCH (a:Person)-[r]->(b) RETURN b.content")
        .expect("execute");

    assert!(
        !result.rows.is_empty(),
        "SPA-198: unlabeled dst endpoint should match Knowledge node; got 0 rows"
    );

    let contents: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(ExecValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        contents.iter().any(|s| s == "factA"),
        "SPA-198: expected \"factA\" in results; got {contents:?}"
    );
}

/// `MATCH (a:Person)-[r]->(b:Knowledge) RETURN a.name` — both endpoints labeled.
///
/// Creates a Person→Knowledge edge and verifies that the fix does not regress
/// the basic labeled-src + labeled-dst traversal path.
#[test]
fn spa198_labeled_src_and_dst_still_works() {
    let dir = db_dir();
    let db = open(dir.path()).expect("open db");

    db.execute("CREATE (a:Person {name:\"Alice\"})")
        .expect("CREATE Person");
    db.execute("CREATE (b:Knowledge {content:\"factA\"})")
        .expect("CREATE Knowledge");
    db.execute("MATCH (a:Person {name:\"Alice\"}),(b:Knowledge) CREATE (a)-[:KNOWS]->(b)")
        .expect("CREATE edge");

    // Query with both endpoints labeled — regression guard for the labeled path.
    let result = db
        .execute("MATCH (a:Person)-[r]->(b:Knowledge) RETURN a.name")
        .expect("execute");

    assert!(
        !result.rows.is_empty(),
        "SPA-198: labeled src+dst should return Person name; got 0 rows"
    );

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(ExecValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        names.iter().any(|s| s == "Alice"),
        "SPA-198: expected \"Alice\" in results; got {names:?}"
    );
}
