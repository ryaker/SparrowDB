//! Regression tests for #497: `MATCH (n)` did a per-label high-water-mark
//! disk load (`exists()` + `read()`) for EVERY label in the catalog, including
//! labels that hold no live nodes.
//!
//! The I/O test counts `NodeStore` HWM disk loads on the calling thread (a
//! deterministic proxy for filesystem operations) instead of timing, so it is
//! immune to machine load.  The equivalence tests pin that skipping empty
//! labels does not change any result.  They pass before AND after the fix
//! (the old code simply scanned zero slots for those labels); they are here
//! because the fix replaces "scan every label" with "scan labels the cached
//! live count says are non-empty", and a wrong skip would silently drop rows.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;
use sparrowdb_storage::node_store::hwm_disk_load_count;

fn names(db: &GraphDb, q: &str) -> Vec<String> {
    let r = db.execute(q).unwrap();
    let mut v: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {other:?}"),
        })
        .collect();
    v.sort();
    v
}

fn count_all(db: &GraphDb) -> i64 {
    let r = db.execute("MATCH (n) RETURN count(n)").unwrap();
    match r.rows[0][0] {
        Value::Int64(n) => n,
        ref o => panic!("expected Int64, got {o:?}"),
    }
}

/// 200 labels in the catalog: 97 never given a node (hwm 0), 100 whose only
/// node was deleted (hwm 1, live 0), 3 populated.  Hand-derived:
///   A: 3 created, 1 deleted -> 2 live;  B: 1;  C: 1  => count(n) = 4.
/// Only the 3 populated labels may cost an HWM disk load: 3.
/// Pre-fix this is 200 (one per catalog label).
#[test]
fn regression_497_match_all_skips_empty_labels_io() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let mut tx = db.begin_write().unwrap();
    for i in 0..97 {
        tx.create_label(&format!("Empty{i:03}")).unwrap();
    }
    tx.commit().unwrap();

    for i in 0..100 {
        db.execute(&format!("CREATE (:Gone{i:03} {{name: 'g{i}'}})"))
            .unwrap();
        db.execute(&format!("MATCH (n:Gone{i:03}) DELETE n"))
            .unwrap();
    }
    db.execute("CREATE (:A {name: 'a1'})").unwrap();
    db.execute("CREATE (:A {name: 'a2'})").unwrap();
    db.execute("CREATE (:A {name: 'a3'})").unwrap();
    db.execute("CREATE (:B {name: 'b1'})").unwrap();
    db.execute("CREATE (:C {name: 'c1'})").unwrap();
    db.execute("MATCH (n:A {name: 'a2'}) DELETE n").unwrap();

    let before = hwm_disk_load_count();
    let n = count_all(&db);
    let loads = hwm_disk_load_count() - before;

    assert_eq!(n, 4, "A(2 live) + B(1) + C(1)");
    assert_eq!(
        loads, 3,
        "one HWM disk load per populated label, none for the 197 empty ones"
    );
}

/// Result equivalence across tombstones, checkpoint and reopen.
#[test]
fn regression_497_match_all_equivalence_tombstone_checkpoint_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.sparrow");
    {
        let db = GraphDb::open(&path).unwrap();
        let mut tx = db.begin_write().unwrap();
        tx.create_label("NeverUsed").unwrap();
        tx.commit().unwrap();
        db.execute("CREATE (:X {name: 'x1'})").unwrap();
        db.execute("CREATE (:X {name: 'x2'})").unwrap();
        db.execute("CREATE (:Y {name: 'y1'})").unwrap();
        db.execute("CREATE (:Z {name: 'z1'})").unwrap();
        // Z fully deleted; X partially deleted.
        db.execute("MATCH (n:Z) DELETE n").unwrap();
        db.execute("MATCH (n:X {name: 'x1'}) DELETE n").unwrap();
        assert_eq!(names(&db, "MATCH (n) RETURN n.name"), vec!["x2", "y1"]);
        assert_eq!(count_all(&db), 2);

        db.checkpoint().unwrap();
        assert_eq!(names(&db, "MATCH (n) RETURN n.name"), vec!["x2", "y1"]);

        // A previously deleted-out label gets a node again: must reappear.
        db.execute("CREATE (:Z {name: 'z2'})").unwrap();
        assert_eq!(
            names(&db, "MATCH (n) RETURN n.name"),
            vec!["x2", "y1", "z2"]
        );
        // And a never-used label that gets its first node.
        db.execute("CREATE (:NeverUsed {name: 'n1'})").unwrap();
        assert_eq!(count_all(&db), 4);
    }
    let db = GraphDb::open(&path).unwrap();
    assert_eq!(
        names(&db, "MATCH (n) RETURN n.name"),
        vec!["n1", "x2", "y1", "z2"]
    );
    assert_eq!(count_all(&db), 4);
}

/// Via the WriteTx API (not Cypher CREATE) and with a WHERE filter.
#[test]
fn regression_497_match_all_equivalence_writetx_and_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (:Old {name: 'o1'})").unwrap();
    db.execute("MATCH (n:Old) DELETE n").unwrap();

    let mut tx = db.begin_write().unwrap();
    let lid = tx.get_or_create_label_id("Fresh").unwrap();
    let col = sparrowdb_common::col_id_of("name");
    tx.create_node(lid, &[(col, sparrowdb::Value::Bytes(b"f1".to_vec()))])
        .unwrap();
    tx.commit().unwrap();

    assert_eq!(names(&db, "MATCH (n) RETURN n.name"), vec!["f1"]);
    assert_eq!(
        names(&db, "MATCH (n) WHERE n.name = 'f1' RETURN n.name"),
        vec!["f1"]
    );
    assert_eq!(count_all(&db), 1);
}

/// Secondary labels: a node `(:P:Q)` lives in primary label P only, so
/// `MATCH (n)` returns it exactly once, and Q (no primary nodes) is skipped
/// without losing it.
#[test]
fn regression_497_match_all_equivalence_secondary_labels() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (:P:Q {name: 'pq'})").unwrap();
    db.execute("CREATE (:R {name: 'r1'})").unwrap();
    assert_eq!(names(&db, "MATCH (n) RETURN n.name"), vec!["pq", "r1"]);
    assert_eq!(count_all(&db), 2);
    assert_eq!(names(&db, "MATCH (n:Q) RETURN n.name"), vec!["pq"]);
}

/// Nothing but empty labels: zero rows, count 0.
#[test]
fn regression_497_match_all_only_empty_labels() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let mut tx = db.begin_write().unwrap();
    tx.create_label("E1").unwrap();
    tx.create_label("E2").unwrap();
    tx.commit().unwrap();
    db.execute("CREATE (:T {name: 't'})").unwrap();
    db.execute("MATCH (n:T) DELETE n").unwrap();
    assert!(db
        .execute("MATCH (n) RETURN n.name")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(count_all(&db), 0);
}
