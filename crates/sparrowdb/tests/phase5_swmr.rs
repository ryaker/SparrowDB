//! Phase 5 SWMR integration tests — acceptance checks for snapshot isolation
//! and writer serialisation.
//!
//! Ticket references:
//!   P5-1  — snapshot pinning at ReadTx open
//!   P5-2  — writer serialisation (WriterBusy on concurrent write)
//!   P5-9  — `snapshot_reader_survives_writer_commit` (acceptance #11)

use sparrowdb::{open, GraphDb};
use sparrowdb_common::Error;
use sparrowdb_storage::node_store::Value;

// col_id constants (mirrors uc1_social_graph schema)
const COL_NAME: u32 = 0;
const COL_AGE: u32 = 1;

/// P5-9: Acceptance check #11 — snapshot reader survives writer commit.
///
/// Steps:
/// 1. Open DB, insert Alice (Person, age=30), commit  → txn_id = 1
/// 2. Open ReadTx  (pins snapshot at txn_id = 1)
/// 3. Open WriteTx, SET alice.age = 31, commit        → txn_id = 2
/// 4. Assert: the open ReadTx still sees alice.age == 30 (snapshot isolation)
/// 5. Open new ReadTx, assert: sees alice.age == 31 (new snapshot)
/// 6. Assert: begin_write() while a writer is active returns Err(WriterBusy)
#[test]
fn snapshot_reader_survives_writer_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    // ── Step 1: insert Alice with age = 30 ───────────────────────────────────
    let label_person: u32 = 1;
    let alice_node;
    {
        let mut tx1 = db.begin_write().expect("begin_write tx1");
        alice_node = tx1
            .create_node(
                label_person,
                &[
                    (COL_NAME, Value::Int64(1)), // name token = 1 (Alice)
                    (COL_AGE, Value::Int64(30)),
                ],
            )
            .expect("create Alice");
        let committed = tx1.commit().expect("commit tx1");
        assert_eq!(committed.0, 1, "first commit should be txn_id = 1");
    }

    // ── Step 2: open a ReadTx pinned at txn_id = 1 ───────────────────────────
    let read_old = db.begin_read().expect("begin_read (snapshot 1)");
    assert_eq!(
        read_old.snapshot_txn_id, 1,
        "ReadTx should be pinned at txn_id = 1"
    );

    // Sanity-check: old reader sees age = 30 before any write.
    {
        let props = read_old
            .get_node(alice_node, &[COL_AGE])
            .expect("get_node snapshot 1 (before write)");
        assert_eq!(
            props[0].1,
            Value::Int64(30),
            "snapshot reader should see age = 30 before writer commits"
        );
    }

    // ── Step 3: update alice.age = 31 and commit ─────────────────────────────
    {
        let mut tx2 = db.begin_write().expect("begin_write tx2");
        tx2.set_node_col(alice_node, COL_AGE, Value::Int64(31));
        let committed = tx2.commit().expect("commit tx2");
        assert_eq!(committed.0, 2, "second commit should be txn_id = 2");
    }

    // ── Step 4: old ReadTx still sees age = 30 ───────────────────────────────
    {
        let props = read_old
            .get_node(alice_node, &[COL_AGE])
            .expect("get_node snapshot 1 (after writer committed)");
        assert_eq!(
            props[0].1,
            Value::Int64(30),
            "snapshot reader pinned at txn_id=1 must still see age=30 after writer commits"
        );
    }

    // ── Step 5: new ReadTx sees age = 31 ─────────────────────────────────────
    {
        let read_new = db.begin_read().expect("begin_read (snapshot 2)");
        assert_eq!(
            read_new.snapshot_txn_id, 2,
            "new ReadTx should be pinned at txn_id = 2"
        );
        let props = read_new
            .get_node(alice_node, &[COL_AGE])
            .expect("get_node snapshot 2");
        assert_eq!(
            props[0].1,
            Value::Int64(31),
            "new ReadTx pinned at txn_id=2 must see age=31"
        );
    }

    // ── Step 6: begin_write while writer active returns WriterBusy ───────────
    {
        let _active_writer = db.begin_write().expect("open active writer");
        let err = db
            .begin_write()
            .err()
            .expect("second begin_write should fail");
        assert!(
            matches!(err, Error::WriterBusy),
            "expected WriterBusy, got: {err}"
        );
    }
    // After _active_writer drops, the lock is released.
    let tx_after = db.begin_write().expect("begin_write after lock released");
    tx_after.commit().expect("commit after lock released");
}

/// P5-1: Snapshot is pinned at the instant begin_read is called.
#[test]
fn snapshot_pinned_at_begin_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    // No writes yet → snapshot = 0.
    let rx0 = db.begin_read().expect("read before any writes");
    assert_eq!(rx0.snapshot_txn_id, 0);

    // Commit a write.
    let tx = db.begin_write().expect("write");
    tx.commit().expect("commit");

    // Old reader still at 0.
    assert_eq!(rx0.snapshot_txn_id, 0);

    // New reader at 1.
    let rx1 = db.begin_read().expect("read after write");
    assert_eq!(rx1.snapshot_txn_id, 1);
}

/// P5-2: Writer serialisation — WriterBusy on concurrent write.
#[test]
fn writer_busy_on_concurrent_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open");

    let _w1 = db.begin_write().expect("first writer");

    // Second writer must fail immediately.
    let result = db.begin_write();
    assert!(
        matches!(result, Err(Error::WriterBusy)),
        "expected WriterBusy"
    );

    // Dropping the first writer releases the lock.
    drop(_w1);

    let w2 = db.begin_write().expect("writer after first released");
    w2.commit().expect("commit");
}
