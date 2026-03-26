/// SPA-264: Boolean node properties stored as integers — WHERE = true returns 0 rows.
///
/// Booleans from Cypher CREATE are stored as Int64(0/1) because the storage
/// layer has no Bool type. The WHERE evaluator must coerce Bool↔Int64 so that
/// `WHERE n.active = true` matches a stored Int64(1).
use sparrowdb::GraphDb;
use tempfile::tempdir;

#[test]
fn where_bool_true_matches_stored_int() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Review {verified: true})").unwrap();
    db.execute("CREATE (n:Review {verified: false})").unwrap();

    let r = db
        .execute("MATCH (n:Review) WHERE n.verified = true RETURN COUNT(n) AS cnt")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], sparrowdb_execution::Value::Int64(1));
}

#[test]
fn where_bool_false_matches_stored_int() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Review {verified: true})").unwrap();
    db.execute("CREATE (n:Review {verified: false})").unwrap();

    let r = db
        .execute("MATCH (n:Review) WHERE n.verified = false RETURN COUNT(n) AS cnt")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], sparrowdb_execution::Value::Int64(1));
}

#[test]
fn inline_prop_filter_bool_true() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Review {verified: true, name: 'A'})")
        .unwrap();
    db.execute("CREATE (n:Review {verified: false, name: 'B'})")
        .unwrap();

    // Inline property filter: MATCH (n:Review {verified: true})
    let r = db
        .execute("MATCH (n:Review {verified: true}) RETURN n.name")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0][0],
        sparrowdb_execution::Value::String("A".to_string())
    );
}

#[test]
fn bool_neq_comparison() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Review {verified: true})").unwrap();
    db.execute("CREATE (n:Review {verified: false})").unwrap();

    let r = db
        .execute("MATCH (n:Review) WHERE n.verified <> false RETURN COUNT(n) AS cnt")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], sparrowdb_execution::Value::Int64(1));
}

#[test]
fn return_bool_prop_as_integer() {
    // Until the storage layer gains a native Bool type, booleans round-trip as
    // Int64(0/1). This test documents the current behavior.
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Review {verified: true})").unwrap();

    let r = db.execute("MATCH (n:Review) RETURN n.verified").unwrap();
    assert_eq!(r.rows.len(), 1);
    // Stored as Int64(1) since no Bool type in storage layer.
    assert_eq!(r.rows[0][0], sparrowdb_execution::Value::Int64(1));
}
