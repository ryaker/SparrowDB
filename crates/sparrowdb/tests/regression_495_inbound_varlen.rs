//! Issue #495: variable-length inbound traversal (`<-[:R*m..n]-`).
//!
//! Fixtures are hand-derived. Chain: n1 -R-> n2 -R-> n3 -R-> n4, plus a
//! side edge n5 -R-> n3 and an unrelated edge n1 -S-> n4 (wrong type).
//!
//! Predecessors of n3 by distance (type R only):
//!   1 hop: n2, n5     2 hops: n1 (via n2)     3 hops: none

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn fixture() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    for i in 1..=5 {
        db.execute(&format!("CREATE (:N {{id: {i}}})"))
            .expect("node");
    }
    for (a, b, t) in [
        (1, 2, "R"),
        (2, 3, "R"),
        (3, 4, "R"),
        (5, 3, "R"),
        (1, 4, "S"),
    ] {
        db.execute(&format!(
            "MATCH (a:N {{id:{a}}}), (b:N {{id:{b}}}) CREATE (a)-[:{t}]->(b)"
        ))
        .expect("edge");
    }
    (dir, db)
}

fn ids(db: &sparrowdb::GraphDb, q: &str) -> Vec<i64> {
    let r = db.execute(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let mut v: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(i) => *i,
            o => panic!("unexpected {o:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn issue_495_issue_table() {
    let (_d, db) = fixture();
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*2..2]-(x) RETURN x.id"),
        vec![1]
    );
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*1..2]-(x) RETURN x.id"),
        vec![1, 2, 5]
    );
}

#[test]
fn inbound_varlen_bounds() {
    let (_d, db) = fixture();
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*1..1]-(x) RETURN x.id"),
        vec![2, 5]
    );
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*3..3]-(x) RETURN x.id"),
        Vec::<i64>::new()
    );
    // unbounded: n4's R-ancestors: n3 (1), n2,n5 (2), n1 (3)
    assert_eq!(
        ids(&db, "MATCH (c:N {id:4})<-[:R*]-(x) RETURN x.id"),
        vec![1, 2, 3, 5]
    );
    assert_eq!(
        ids(&db, "MATCH (c:N {id:4})<-[:R*2..]-(x) RETURN x.id"),
        vec![1, 2, 5]
    );
    assert_eq!(
        ids(&db, "MATCH (c:N {id:4})<-[:R*..2]-(x) RETURN x.id"),
        vec![2, 3, 5]
    );
}

#[test]
fn inbound_varlen_zero_lower_bound_includes_self() {
    let (_d, db) = fixture();
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*0..1]-(x) RETURN x.id"),
        vec![2, 3, 5]
    );
}

#[test]
fn inbound_varlen_labeled_terminal_and_unlabeled_start() {
    let (_d, db) = fixture();
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*1..2]-(x:N) RETURN x.id"),
        vec![1, 2, 5]
    );
    // all (c,x) with x reaching c in 2 hops exactly: (3<-1) and (4<-2),(4<-5)
    assert_eq!(
        ids(&db, "MATCH (c)<-[:R*2..2]-(x) RETURN x.id"),
        vec![1, 2, 5]
    );
}

#[test]
fn inbound_matches_reversed_outbound() {
    let (_d, db) = fixture();
    assert_eq!(
        ids(&db, "MATCH (c:N {id:4})<-[:R*1..3]-(x) RETURN x.id"),
        ids(&db, "MATCH (x:N)-[:R*1..3]->(c:N {id:4}) RETURN x.id")
    );
}

#[test]
fn inbound_varlen_after_checkpoint_and_mixed_with_delta() {
    let (_d, db) = fixture();
    db.checkpoint().expect("checkpoint");
    // all edges now live in CSR
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*1..2]-(x) RETURN x.id"),
        vec![1, 2, 5]
    );
    // add n4 -R-> n5 after the checkpoint (delta only): n5's ancestors via
    // 4<-3<-{2,5}, so n3's inbound set is unchanged but n5 gains n4, n3, n2, n1
    db.execute("MATCH (a:N {id:4}), (b:N {id:5}) CREATE (a)-[:R]->(b)")
        .expect("edge");
    assert_eq!(
        ids(&db, "MATCH (c:N {id:5})<-[:R*1..2]-(x) RETURN x.id"),
        vec![3, 4]
    );
    // n5 -> n3 -> n4 -> n5 is a cycle: simple-path rule excludes the start.
    assert_eq!(
        ids(&db, "MATCH (c:N {id:5})<-[:R*]-(x) RETURN DISTINCT x.id"),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn inbound_varlen_heterogeneous_labels_same_slot() {
    // A, B, C each hold slot 0; slot numbers alias across labels.
    // a0 -R-> b0 -R-> c0.  Inbound from c0 must find b0 then a0, by label.
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    db.execute("CREATE (:A {id: 'a'})").expect("a");
    db.execute("CREATE (:B {id: 'b'})").expect("b");
    db.execute("CREATE (:C {id: 'c'})").expect("c");
    db.execute("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)")
        .expect("ab");
    db.execute("MATCH (b:B), (c:C) CREATE (b)-[:R]->(c)")
        .expect("bc");
    for checkpointed in [false, true] {
        if checkpointed {
            db.checkpoint().expect("checkpoint");
        }
        let r = db
            .execute("MATCH (c:C)<-[:R*1..2]-(x) RETURN x.id")
            .expect("query");
        let mut got: Vec<String> = r
            .rows
            .iter()
            .map(|row| match &row[0] {
                Value::String(s) => s.clone(),
                o => panic!("{o:?}"),
            })
            .collect();
        got.sort();
        assert_eq!(got, vec!["a", "b"], "checkpointed={checkpointed}");
        let r = db
            .execute("MATCH (c:C)<-[:R*1..2]-(x:A) RETURN x.id")
            .expect("query");
        assert_eq!(r.rows.len(), 1, "label filter, checkpointed={checkpointed}");
    }
}
