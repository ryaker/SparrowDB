/// SPA-237: UNWIND list AS x MATCH (n {id: x}) RETURN … pipeline.
///
/// Validates that values produced by an UNWIND clause can be used as
/// variable references inside a subsequent MATCH clause's inline property
/// filters, enabling bulk-lookup patterns used by LangChain and Graphiti.
use sparrowdb::GraphDb;
use tempfile::tempdir;

fn open_db() -> (GraphDb, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    (db, dir)
}

/// Basic: UNWIND list, MATCH on id property, RETURN name.
/// Only nodes whose id appears in the list should be returned.
#[test]
fn unwind_list_then_match_filters_by_id() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', name: 'Alpha'})")
        .unwrap();
    db.execute("CREATE (n:Item {id: 'b', name: 'Beta'})")
        .unwrap();
    db.execute("CREATE (n:Item {id: 'c', name: 'Gamma'})")
        .unwrap();

    let r = db
        .execute("UNWIND ['a', 'b'] AS x MATCH (n:Item {id: x}) RETURN n.name")
        .unwrap();

    assert_eq!(
        r.rows.len(),
        2,
        "Only items with id 'a' or 'b' should be returned"
    );

    let names: Vec<String> = r.rows.iter().map(|row| row[0].to_string()).collect();
    assert!(
        names.contains(&"\"Alpha\"".to_string()) || names.contains(&"Alpha".to_string()),
        "Expected Alpha in results, got {:?}",
        names
    );
    assert!(
        names.contains(&"\"Beta\"".to_string()) || names.contains(&"Beta".to_string()),
        "Expected Beta in results, got {:?}",
        names
    );
}

/// Two-element list, two matching nodes, verify row count equals 2.
#[test]
fn unwind_match_returns_correct_row_count() {
    let (db, _dir) = open_db();
    db.execute("CREATE (k:Knowledge {id: 'k1', title: 'Rust'})")
        .unwrap();
    db.execute("CREATE (k:Knowledge {id: 'k2', title: 'Go'})")
        .unwrap();
    db.execute("CREATE (k:Knowledge {id: 'k3', title: 'Python'})")
        .unwrap();

    let r = db
        .execute("UNWIND ['k1', 'k2'] AS kid MATCH (k:Knowledge {id: kid}) RETURN k.title")
        .unwrap();

    assert_eq!(
        r.rows.len(),
        2,
        "Expected exactly 2 rows, got {}",
        r.rows.len()
    );
}

/// Empty list: UNWIND produces no rows → no MATCH → empty result.
#[test]
fn unwind_empty_list_yields_no_rows() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', name: 'Alpha'})")
        .unwrap();

    let r = db
        .execute("UNWIND [] AS x MATCH (n:Item {id: x}) RETURN n.name")
        .unwrap();

    assert_eq!(r.rows.len(), 0, "Empty UNWIND list should produce 0 rows");
}

/// List element with no matching node → that element contributes 0 rows.
#[test]
fn unwind_list_element_with_no_match_is_skipped() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Node {id: 'exists', val: 1})")
        .unwrap();

    let r = db
        .execute("UNWIND ['exists', 'missing'] AS x MATCH (n:Node {id: x}) RETURN n.val")
        .unwrap();

    assert_eq!(r.rows.len(), 1, "Only the existing node should be returned");
}

/// Single-element list matches one node.
#[test]
fn unwind_single_element_list_matches_one_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (p:Person {uid: 'p1', name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (p:Person {uid: 'p2', name: 'Bob'})")
        .unwrap();

    let r = db
        .execute("UNWIND ['p1'] AS pid MATCH (p:Person {uid: pid}) RETURN p.name")
        .unwrap();

    assert_eq!(r.rows.len(), 1);
}

/// UNWIND followed by MATCH then WITH then RETURN (multi-stage pipeline).
#[test]
fn unwind_match_with_return_pipeline() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Doc {id: 'd1', score: 10})").unwrap();
    db.execute("CREATE (n:Doc {id: 'd2', score: 20})").unwrap();
    db.execute("CREATE (n:Doc {id: 'd3', score: 5})").unwrap();

    let r = db
        .execute(
            "UNWIND ['d1', 'd2'] AS x \
             MATCH (n:Doc {id: x}) \
             WITH n.score AS s \
             RETURN s",
        )
        .unwrap();

    assert_eq!(r.rows.len(), 2, "Expected 2 rows from d1 and d2");
}
