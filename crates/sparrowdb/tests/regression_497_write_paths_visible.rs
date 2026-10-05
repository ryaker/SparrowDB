use sparrowdb::open;
use sparrowdb_execution::types::Value;
fn count(db: &sparrowdb::GraphDb) -> i64 {
    let r = db.execute("MATCH (n) RETURN count(n)").unwrap();
    match &r.rows[0][0] {
        Value::Int64(i) => *i,
        o => panic!("{o:?}"),
    }
}
#[test]
fn match_all_sees_every_write_path_after_labels_emptied() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for i in 0..20 {
        db.execute(&format!("CREATE (:Empty{i} {{x:1}})")).unwrap();
    }
    db.execute("MATCH (n) DELETE n").unwrap(); // 20 labels now empty
    assert_eq!(count(&db), 0);
    db.execute("MERGE (:Empty3 {x: 7})").unwrap(); // MERGE into an emptied label
    assert_eq!(count(&db), 1, "after MERGE");
    for v in 1..=3 {
        db.execute(&format!("CREATE (:Empty5 {{x: {v}}})")).unwrap();
    }
    assert_eq!(count(&db), 4, "after CREATE into emptied label");
    db.execute("MERGE (:Brand {x: 1})").unwrap(); // brand-new label via MERGE
    assert_eq!(count(&db), 5, "after MERGE new label");
    let mut tx = db.begin_write().unwrap();
    let l = tx.get_or_create_label_id("ViaApi").unwrap();
    tx.create_node(l, &[]).unwrap();
    tx.commit().unwrap();
    assert_eq!(count(&db), 6, "after WriteTx API on new label");
    db.execute("CHECKPOINT").unwrap();
    assert_eq!(count(&db), 6, "after checkpoint");
    drop(db);
    let db = open(dir.path()).unwrap();
    assert_eq!(count(&db), 6, "after reopen");
}
