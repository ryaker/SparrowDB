//! Regression test for #491: `GraphDb::db_counts()` (backing the `sparrowdb
//! info` CLI) shares #485's stale-count defect through a separate code path.
//!
//! Root cause: `db_counts()` (crates/sparrowdb/src/db.rs) summed
//! `NodeStore::hwm_for_label` across every catalog label — the same
//! high-water-mark value that made Cypher `COUNT(n)` stale in #485, and for
//! the same reason: `delete_node`/`detach_delete_node` tombstone a slot in
//! place rather than freeing it, so the HWM never shrinks. There is no
//! compaction/GC step that reconciles this later, and the staleness survives
//! `checkpoint()` + reopen because `NodeStore::open` re-derives the HWM from
//! disk on every call.
//!
//! Fix: `db_counts()` now calls `NodeStore::live_count_for_label` (added for
//! #485), which subtracts tombstoned slots from the HWM.
//!
//! Every expected value below is derived by hand from the CREATE/DELETE
//! statements in each test, never from observed program output.

use sparrowdb::GraphDb;

/// Delete one of three nodes -> node_count must drop from 3 to 2. Edge count
/// is untouched by this test and must stay 0.
#[test]
fn regression_491_db_counts_after_partial_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'a'})").unwrap();
    db.execute("CREATE (:U {id: 'b'})").unwrap();
    db.execute("CREATE (:U {id: 'c'})").unwrap();
    let (before, _) = db.db_counts().expect("counts before delete");
    assert_eq!(before, 3, "sanity: 3 created -> node_count 3");

    db.execute("MATCH (n:U {id: 'a'}) DELETE n").unwrap();

    // Expected by hand: 3 created - 1 deleted = 2 live nodes.
    let (after, edges) = db.db_counts().expect("counts after delete");
    assert_eq!(after, 2);
    assert_eq!(edges, 0, "no edges were ever created");
}

/// Delete every node of a label -> node_count must be exactly 0.
#[test]
fn regression_491_db_counts_after_delete_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'x'})").unwrap();
    db.execute("CREATE (:U {id: 'y'})").unwrap();
    db.execute("MATCH (n:U) DELETE n").unwrap();

    // Expected by hand: 2 created - 2 deleted = 0.
    let (node_count, _) = db.db_counts().expect("counts after delete-all");
    assert_eq!(node_count, 0);
}

/// The #485-equivalent reproduction for `db_counts()`: delete-all,
/// checkpoint, drop the handle, reopen — node_count must still be 0. This is
/// the case that proves the fix isn't an in-memory-only patch;
/// `NodeStore::open` re-derives the HWM (and now the tombstone count) from
/// disk on every call, including a fresh session.
#[test]
fn regression_491_db_counts_survives_checkpoint_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (:U {id: 'x'})").unwrap();
        db.execute("CREATE (:U {id: 'y'})").unwrap();
        let (before, _) = db.db_counts().expect("counts before any delete");
        assert_eq!(before, 2, "sanity before any delete");

        db.execute("MATCH (n:U) DELETE n").unwrap();
        // Expected by hand: 2 - 2 = 0.
        let (after_delete, _) = db.db_counts().expect("counts after delete-all");
        assert_eq!(after_delete, 0, "node_count right after delete-all");

        db.checkpoint().expect("checkpoint");
        let (after_checkpoint, _) = db.db_counts().expect("counts after checkpoint");
        assert_eq!(
            after_checkpoint, 0,
            "node_count immediately after checkpoint"
        );
        // `db` dropped here — simulates process exit.
    }

    {
        let db2 = GraphDb::open(&db_path).expect("open session 2");
        // Expected by hand: still 0 — nothing was created since the delete-all.
        let (node_count, _) = db2.db_counts().expect("counts after reopen");
        assert_eq!(
            node_count, 0,
            "node_count must be 0 after checkpoint + reopen, not the stale pre-delete HWM"
        );
    }
}

/// A label that was never touched by any delete must be unaffected by the
/// fix — its contribution to node_count is still just its HWM (no
/// tombstones exist), and it must not be double-counted or dropped when
/// summed alongside a label that WAS deleted from.
#[test]
fn regression_491_untouched_label_unaffected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'x'})").unwrap();
    db.execute("CREATE (:Control {id: '1'})").unwrap();
    db.execute("CREATE (:Control {id: '2'})").unwrap();
    db.execute("CREATE (:Control {id: '3'})").unwrap();

    db.execute("MATCH (n:U) DELETE n").unwrap();

    // Expected by hand: Control (3, untouched) + U (0, all deleted) = 3.
    let (node_count, _) = db.db_counts().expect("counts across labels");
    assert_eq!(node_count, 3);
}
