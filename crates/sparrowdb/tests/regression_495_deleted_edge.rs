use sparrowdb::open;
use sparrowdb_execution::types::Value;
fn ids(db: &sparrowdb::GraphDb, q: &str) -> Vec<i64> {
    let r = db.execute(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let mut v: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect();
    v.sort();
    v
}
#[test]
fn deleted_edge_is_not_traversed_inbound() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for i in 1..=5 {
        db.execute(&format!("CREATE (:N {{id: {i}}})")).unwrap();
    }
    for (a, b) in [(1, 2), (2, 3), (3, 4), (5, 3)] {
        db.execute(&format!(
            "MATCH (a:N {{id:{a}}}), (b:N {{id:{b}}}) CREATE (a)-[:R]->(b)"
        ))
        .unwrap();
    }
    db.execute("MATCH (a:N {id:2})-[r:R]->(b:N {id:3}) DELETE r")
        .unwrap();
    // hand-derived: preds of n3 after removing 2->3 is only n5; n5 has none.
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*1..2]-(x) RETURN x.id"),
        vec![5],
        "pre-checkpoint"
    );
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})-[:R*1..2]->(x) RETURN x.id"),
        vec![4],
        "outbound control"
    );
    db.execute("CHECKPOINT").unwrap();
    assert_eq!(
        ids(&db, "MATCH (c:N {id:3})<-[:R*1..2]-(x) RETURN x.id"),
        vec![5],
        "post-checkpoint"
    );
}
