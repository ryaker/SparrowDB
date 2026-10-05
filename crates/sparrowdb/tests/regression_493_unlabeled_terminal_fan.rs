//! Regression tests for #493: two-hop MATCH (fan shape) with an unlabeled terminal node.
//!
//! Fixture: a1 -[:R]-> b1, a1 -[:S]-> s1.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    db.execute("CREATE (:A {id: 'a1'})").unwrap();
    db.execute("CREATE (:B {id: 'b1'})").unwrap();
    db.execute("CREATE (:S {id: 's1'})").unwrap();
    db.execute("MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .unwrap();
    db.execute("MATCH (a:A {id:'a1'}), (s:S {id:'s1'}) CREATE (a)-[:S]->(s)")
        .unwrap();
    (dir, db)
}

fn strs(db: &sparrowdb::GraphDb, q: &str) -> Result<Vec<Vec<Value>>, String> {
    db.execute(q).map(|r| r.rows).map_err(|e| e.to_string())
}

// Hand-derived: s1 <-S- a1 -R-> b1  => x = b1.
#[test]
fn fan_outbound_unlabeled_terminal() {
    let (_d, db) = make_db();
    let r = strs(&db, "MATCH (s:S)<-[:S]-(a:A)-[:R]->(x) RETURN x.id");
    assert_eq!(r, Ok(vec![vec![Value::String("b1".into())]]));
}

// Hand-derived: no node has an inbound :R into a1, so no rows.
#[test]
fn fan_inbound_unlabeled_terminal_no_match() {
    let (_d, db) = make_db();
    let r = strs(&db, "MATCH (s:S)<-[:S]-(a:A)<-[:R]-(x) RETURN x.id");
    assert_eq!(r, Ok(vec![]));
}

#[test]
fn fan_inbound_labeled_terminal_no_match() {
    let (_d, db) = make_db();
    let r = strs(&db, "MATCH (s:S)<-[:S]-(a:A)<-[:R]-(x:B) RETURN x.id");
    assert_eq!(r, Ok(vec![]));
}

// One hop, unlabeled tail: b1 <-R- a1.
#[test]
fn one_hop_unlabeled_tail() {
    let (_d, db) = make_db();
    let r = strs(&db, "MATCH (b:B)<-[:R]-(x) RETURN x.id");
    assert_eq!(r, Ok(vec![vec![Value::String("a1".into())]]));
}

// Same as the outbound fan, but served from the checkpointed (CSR) state.
#[test]
fn fan_outbound_unlabeled_terminal_after_checkpoint() {
    let (_d, db) = make_db();
    db.execute("CHECKPOINT").expect("checkpoint");
    let r = strs(&db, "MATCH (s:S)<-[:S]-(a:A)-[:R]->(x) RETURN x.id");
    assert_eq!(r, Ok(vec![vec![Value::String("b1".into())]]));
    let r = strs(&db, "MATCH (s:S)<-[:S]-(a:A)<-[:R]-(x) RETURN x.id");
    assert_eq!(r, Ok(vec![]));
}
