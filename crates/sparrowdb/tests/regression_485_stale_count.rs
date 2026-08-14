//! Regression test for #485: `COUNT(n)` ignores deletions and returns a
//! stale total that survives checkpoint and reopen.
//!
//! Root cause: `build_label_row_counts_from_disk` (crates/sparrowdb/src/helpers.rs)
//! populated the `label_row_counts` cache from `NodeStore::hwm_for_label`, which
//! is a high-water mark of slots ever *allocated* for a label. `delete_node` /
//! `detach_delete_node` tombstone the slot in place (write `u64::MAX` into
//! `col_0`) rather than freeing it, so the HWM never shrinks. SPA-197's O(1)
//! `COUNT(n)` / `COUNT(*)` fast-path reads straight from this cache, so any
//! delete left the reported count permanently too high — including across
//! `checkpoint()` and a fresh `GraphDb::open()`, since the same function seeds
//! the cache on open.
//!
//! Fix: `NodeStore::live_count_for_label` subtracts tombstoned slots (col_0 ==
//! u64::MAX) from the HWM, and `build_label_row_counts_from_disk` now calls it
//! instead of `hwm_for_label` directly.
//!
//! Every expected value below is derived by hand from the CREATE/DELETE
//! statements in each test, never from observed program output.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn count_of(db: &GraphDb, label: &str) -> i64 {
    let result = db
        .execute(&format!("MATCH (n:{label}) RETURN COUNT(n) AS total"))
        .expect("COUNT(n) query");
    assert_eq!(result.rows.len(), 1);
    match result.rows[0][0] {
        Value::Int64(n) => n,
        ref other => panic!("expected Int64, got {other:?}"),
    }
}

/// Delete one of three nodes -> count must drop from 3 to 2.
#[test]
fn regression_485_count_after_partial_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'a'})").unwrap();
    db.execute("CREATE (:U {id: 'b'})").unwrap();
    db.execute("CREATE (:U {id: 'c'})").unwrap();
    assert_eq!(count_of(&db, "U"), 3, "sanity: 3 created -> count 3");

    db.execute("MATCH (n:U {id: 'a'}) DELETE n").unwrap();

    // Expected by hand: 3 created - 1 deleted = 2 live nodes.
    assert_eq!(count_of(&db, "U"), 2);

    // Row projection must agree (this was already correct pre-fix).
    let result = db.execute("MATCH (n:U) RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::String("b".into()));
    assert_eq!(result.rows[1][0], Value::String("c".into()));
}

/// Delete every node of a label -> count must be exactly 0, not the stale HWM.
#[test]
fn regression_485_count_after_delete_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'x'})").unwrap();
    db.execute("CREATE (:U {id: 'y'})").unwrap();
    assert_eq!(count_of(&db, "U"), 2);

    db.execute("MATCH (n:U) DELETE n").unwrap();

    // Expected by hand: 2 created - 2 deleted = 0.
    assert_eq!(count_of(&db, "U"), 0);

    let result = db.execute("MATCH (n:U) RETURN n.id").unwrap();
    assert_eq!(result.rows.len(), 0);
}

/// The exact reproduction from the issue: delete-all, checkpoint, drop the
/// handle, reopen — count must still be 0. This is the case that proves the
/// fix isn't just an in-memory cache patch; `GraphDb::open()` reseeds
/// `label_row_counts` from the same disk-reading function.
#[test]
fn regression_485_count_survives_checkpoint_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (:U {id: 'x'})").unwrap();
        db.execute("CREATE (:U {id: 'y'})").unwrap();
        assert_eq!(count_of(&db, "U"), 2, "sanity before any delete");

        db.execute("MATCH (n:U {id: 'x'}) DELETE n").unwrap();
        // Expected by hand: 2 - 1 = 1.
        assert_eq!(count_of(&db, "U"), 1, "count right after delete");

        db.execute("MATCH (n:U) DELETE n").unwrap();
        // Expected by hand: 1 - 1 = 0 (the remaining node 'y' is gone too).
        assert_eq!(count_of(&db, "U"), 0, "count after deleting the rest");

        db.checkpoint().expect("checkpoint");
        assert_eq!(count_of(&db, "U"), 0, "count immediately after checkpoint");
        // `db` dropped here — simulates process exit.
    }

    {
        let db2 = GraphDb::open(&db_path).expect("open session 2");
        // Expected by hand: still 0 — nothing was created since the delete-all.
        assert_eq!(
            count_of(&db2, "U"),
            0,
            "count must be 0 after checkpoint + reopen, not the stale pre-delete HWM"
        );
        let result = db2.execute("MATCH (n:U) RETURN n.id").unwrap();
        assert_eq!(result.rows.len(), 0, "row projection must agree with count");
    }
}

/// Delete then create a new node of the same label. `create_node` is
/// append-only (never reuses a tombstoned slot — SPA-187 zero-pads instead),
/// so this exercises HWM continuing to grow past a tombstoned slot.
#[test]
fn regression_485_count_after_delete_then_create() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'a'})").unwrap();
    db.execute("MATCH (n:U {id: 'a'}) DELETE n").unwrap();
    assert_eq!(count_of(&db, "U"), 0, "sanity: sole node deleted");

    db.execute("CREATE (:U {id: 'b'})").unwrap();

    // Expected by hand: HWM after both creates is 2 (slots 0 and 1, no slot
    // reuse), 1 tombstoned (slot 0) -> live count 2 - 1 = 1.
    assert_eq!(count_of(&db, "U"), 1);

    let result = db.execute("MATCH (n:U) RETURN n.id").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("b".into()));
}

/// `COUNT(*)` shares the same SPA-197 fast-path code path as `COUNT(n)`
/// (both read `label_row_counts`), so it was equally stale pre-fix.
#[test]
fn regression_485_count_star_after_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'a'})").unwrap();
    db.execute("CREATE (:U {id: 'b'})").unwrap();
    db.execute("MATCH (n:U {id: 'a'}) DELETE n").unwrap();

    let result = db
        .execute("MATCH (n:U) RETURN COUNT(*) AS total")
        .expect("COUNT(*) query");
    assert_eq!(result.rows.len(), 1);
    // Expected by hand: 2 created - 1 deleted = 1.
    assert_eq!(result.rows[0][0], Value::Int64(1));
}

/// A label that was never touched by any delete must be unaffected by the
/// fix — its count is still just its HWM (no tombstones exist).
#[test]
fn regression_485_untouched_label_unaffected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    db.execute("CREATE (:U {id: 'x'})").unwrap();
    db.execute("CREATE (:Control {id: '1'})").unwrap();
    db.execute("CREATE (:Control {id: '2'})").unwrap();
    db.execute("CREATE (:Control {id: '3'})").unwrap();

    db.execute("MATCH (n:U) DELETE n").unwrap();

    // Expected by hand: Control was never deleted from -> still 3.
    assert_eq!(count_of(&db, "Control"), 3);
    assert_eq!(count_of(&db, "U"), 0);
}
