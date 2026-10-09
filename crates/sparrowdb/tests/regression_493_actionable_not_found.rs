//! #493: queries that fail must say WHAT was missing and WHERE, never a bare
//! `not found`. Only the existing error paths are made actionable; which
//! queries error (vs. return zero rows) is unchanged.

use sparrowdb::open;

fn db_with_edge() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    db.execute("CREATE (a:A {id: 1})").expect("create A");
    db.execute("CREATE (b:B {id: 2})").expect("create B");
    db.execute("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)")
        .expect("create edge");
    (dir, db)
}

fn err_of(db: &sparrowdb::GraphDb, q: &str) -> String {
    match db.execute(q) {
        Ok(r) => panic!("expected an error for `{q}`, got {} rows", r.rows.len()),
        Err(e) => e.to_string(),
    }
}

fn assert_actionable(msg: &str, needles: &[&str]) {
    assert_ne!(msg, "not found", "bare 'not found' leaked");
    for n in needles {
        assert!(msg.contains(n), "message {msg:?} should contain {n:?}");
    }
}

#[test]
fn with_after_match_unknown_label_names_label_and_clause() {
    let (_d, db) = db_with_edge();
    let m = err_of(&db, "MATCH (a:NoSuch) WITH a RETURN a");
    assert_actionable(&m, &["NoSuch", "label", "WITH"]);
}

#[test]
fn with_after_match_unlabeled_node_names_requirement() {
    let (_d, db) = db_with_edge();
    let m = err_of(&db, "MATCH (n) WITH n RETURN n");
    assert_actionable(&m, &["label", "WITH"]);
}

#[test]
fn two_hop_unknown_last_label_names_label_and_position() {
    let (_d, db) = db_with_edge();
    let m = err_of(&db, "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:NoSuch) RETURN c");
    assert_actionable(&m, &["NoSuch", "label", "last node", "2-hop"]);
}

#[test]
fn two_hop_unknown_first_label_names_label_and_position() {
    let (_d, db) = db_with_edge();
    let m = err_of(&db, "MATCH (a:NoSuch)-[:R]->(b:B)-[:R]->(c:B) RETURN c");
    assert_actionable(&m, &["NoSuch", "label", "first node"]);
}

#[test]
fn two_hop_unlabeled_head_says_label_required() {
    let (_d, db) = db_with_edge();
    let m = err_of(&db, "MATCH (a)-[:R]->(b:B)-[:R]->(c:A) RETURN a");
    assert_actionable(&m, &["first node", "requires a label"]);
}

#[test]
fn varlen_unknown_destination_label_names_label() {
    let (_d, db) = db_with_edge();
    let m = err_of(&db, "MATCH (a:A)-[:R*1..2]->(b:NoSuch) RETURN b");
    assert_actionable(&m, &["NoSuch", "label", "variable-length", "destination"]);
}

/// Semantically-empty cases stay empty (not turned into errors).
#[test]
fn unknown_label_in_plain_match_still_returns_zero_rows() {
    let (_d, db) = db_with_edge();
    let r = db.execute("MATCH (a:NoSuch) RETURN a").expect("ok");
    assert!(r.rows.is_empty());
}
