//! #559: the COUNT(dst) + ORDER BY alias DESC LIMIT k degree-cache fast path
//! keyed degrees by source slot alone, summing edges of every relationship
//! type and every label that happens to share that slot.

use sparrowdb::open;
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn run(db: &sparrowdb::GraphDb, q: &str) -> Vec<Vec<Value>> {
    db.execute(q).expect(q).rows
}

/// S and A are both slot 0 of their labels. S-[:R]->A x1, A-[:R]->W x3.
fn issue_fixture(db: &sparrowdb::GraphDb) {
    for q in [
        "CREATE (:W {id:1})",
        "CREATE (:W {id:2})",
        "CREATE (:W {id:3})",
        "CREATE (:A {name:'a'})",
        "CREATE (:S {name:'s'})",
        "MATCH (s:S),(a:A) CREATE (s)-[:R]->(a)",
        "MATCH (a:A),(w:W {id:1}) CREATE (a)-[:R]->(w)",
        "MATCH (a:A),(w:W {id:2}) CREATE (a)-[:R]->(w)",
        "MATCH (a:A),(w:W {id:3}) CREATE (a)-[:R]->(w)",
    ] {
        db.execute(q).expect(q);
    }
}

#[test]
fn shared_slot_other_label_not_counted() {
    let (_d, db) = make_db();
    issue_fixture(&db);
    // Hand-derived: A has 3 edges to W.
    assert_eq!(
        run(
            &db,
            "MATCH (a:A)-[:R]->(w:W) RETURN a.name, COUNT(w) AS c ORDER BY c DESC LIMIT 3"
        ),
        vec![vec![Value::String("a".into()), Value::Int64(3)]]
    );
    // Hand-derived: S has exactly 1 edge to A.
    assert_eq!(
        run(
            &db,
            "MATCH (s:S)-[:R]->(a:A) RETURN s.name, COUNT(a) AS c ORDER BY c DESC LIMIT 3"
        ),
        vec![vec![Value::String("s".into()), Value::Int64(1)]]
    );
}

#[test]
fn other_rel_type_and_other_dst_label_not_counted() {
    let (_d, db) = make_db();
    for q in [
        "CREATE (:P {name:'p'})",
        "CREATE (:Q {k:1})",
        "CREATE (:Q {k:2})",
        "CREATE (:T {k:1})",
        "MATCH (p:P),(q:Q {k:1}) CREATE (p)-[:R]->(q)",
        "MATCH (p:P),(q:Q {k:2}) CREATE (p)-[:R]->(q)",
        "MATCH (p:P),(t:T) CREATE (p)-[:R]->(t)",
        "MATCH (p:P),(q:Q {k:1}) CREATE (p)-[:OTHER]->(q)",
    ] {
        db.execute(q).expect(q);
    }
    // P-[:R]->Q: 2 edges (the :R edge to T and the :OTHER edge excluded).
    assert_eq!(
        run(
            &db,
            "MATCH (p:P)-[:R]->(q:Q) RETURN p.name, COUNT(q) AS c ORDER BY c DESC LIMIT 5"
        ),
        vec![vec![Value::String("p".into()), Value::Int64(2)]]
    );
    // P-[:R]->T: 1 edge.
    assert_eq!(
        run(
            &db,
            "MATCH (p:P)-[:R]->(t:T) RETURN p.name, COUNT(t) AS c ORDER BY c DESC LIMIT 5"
        ),
        vec![vec![Value::String("p".into()), Value::Int64(1)]]
    );
    // P-[:OTHER]->Q: 1 edge.
    assert_eq!(
        run(
            &db,
            "MATCH (p:P)-[:OTHER]->(q:Q) RETURN p.name, COUNT(q) AS c ORDER BY c DESC LIMIT 5"
        ),
        vec![vec![Value::String("p".into()), Value::Int64(1)]]
    );
}

#[test]
fn single_rel_table_shape_still_correct() {
    let (_d, db) = make_db();
    for q in [
        "CREATE (:N {name:'x'})",
        "CREATE (:N {name:'y'})",
        "CREATE (:N {name:'z'})",
        "MATCH (a:N {name:'x'}),(b:N {name:'y'}) CREATE (a)-[:K]->(b)",
        "MATCH (a:N {name:'x'}),(b:N {name:'z'}) CREATE (a)-[:K]->(b)",
        "MATCH (a:N {name:'y'}),(b:N {name:'z'}) CREATE (a)-[:K]->(b)",
    ] {
        db.execute(q).expect(q);
    }
    assert_eq!(
        run(
            &db,
            "MATCH (a:N)-[:K]->(b:N) RETURN a.name, COUNT(b) AS c ORDER BY c DESC LIMIT 2"
        ),
        vec![
            vec![Value::String("x".into()), Value::Int64(2)],
            vec![Value::String("y".into()), Value::Int64(1)],
        ]
    );
}
