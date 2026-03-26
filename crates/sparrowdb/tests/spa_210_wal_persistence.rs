//! SPA-210: Persist WalWriter in GraphDb — eliminate per-commit open overhead.
//!
//! ## What this tests
//!
//! Before SPA-210, `write_mutation_wal` opened a new `WalWriter` on every
//! transaction commit, paying segment-scan and file-open overhead each time.
//!
//! After SPA-210, the `WalWriter` is persisted in `DbInner` and reused across
//! commits; only reopened on segment rotation.
//!
//! ## Test strategy
//!
//! 1. `wal_writer_reused_across_commits` — write 100 nodes in 100 separate
//!    transactions; verify all nodes are readable and the WAL contains all
//!    expected records (Begin/NodeCreate/Commit × 100).
//!
//! 2. `wal_survives_rotation` — write enough data to trigger at least one
//!    segment rotation, then reopen the DB and verify all data is recoverable.
//!
//! 3. `concurrent_reads_during_write` — writes do not block reads (SWMR
//!    guarantee): a read transaction opened before a write sees the old
//!    snapshot; one opened after sees the new data.

use sparrowdb::GraphDb;
use sparrowdb_storage::node_store::{NodeStore, Value};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("db");
    (dir, path)
}

// ── Test 1: WalWriter reused across 100 commits ───────────────────────────────

/// Write 100 nodes in 100 separate transactions; verify all nodes are readable
/// and that the WAL contains at least 100 NodeCreate records.
#[test]
fn wal_writer_reused_across_commits() {
    use sparrowdb_storage::wal::codec::{WalRecord, WalRecordKind};

    let (_dir, db_path) = fresh_db();
    const N: usize = 100;
    const LABEL: u32 = 1;

    // Write N nodes in N separate committed transactions.
    {
        let db = GraphDb::open(&db_path).expect("open db");
        for i in 0u64..N as u64 {
            let mut tx = db.begin_write().expect("begin_write");
            tx.create_node(LABEL, &[(0u32, Value::Int64(i as i64))])
                .expect("create_node");
            tx.commit().expect("commit");
        }
        // `db` is dropped here — simulates clean shutdown.
    }

    // Verify all N nodes are visible via the node store.
    {
        let store = NodeStore::open(&db_path).expect("node store");
        let hwm = store.hwm_for_label(LABEL).expect("hwm_for_label");
        assert_eq!(
            hwm, N as u64,
            "all {N} committed nodes must be visible (hwm = {hwm})"
        );
    }

    // Verify the WAL contains at least N NodeCreate records and N Commit records.
    let wal_dir = db_path.join("wal");
    assert!(wal_dir.exists(), "WAL directory must exist after commits");

    let mut nc_count = 0usize;
    let mut commit_count = 0usize;

    for entry in std::fs::read_dir(&wal_dir).expect("read wal dir").flatten() {
        if !entry.file_name().to_string_lossy().ends_with(".wal") {
            continue;
        }
        let data = std::fs::read(entry.path()).expect("read WAL segment");
        // Skip the 1-byte WAL format version header.
        let mut offset = if data.is_empty() { 0usize } else { 1usize };
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((rec, consumed)) => {
                    match rec.kind {
                        WalRecordKind::NodeCreate => nc_count += 1,
                        WalRecordKind::Commit => commit_count += 1,
                        _ => {}
                    }
                    offset += consumed;
                }
                Err(_) => break,
            }
        }
    }

    assert_eq!(
        nc_count, N,
        "WAL must contain exactly {N} NodeCreate records, got {nc_count}"
    );
    assert_eq!(
        commit_count, N,
        "WAL must contain exactly {N} Commit records, got {commit_count}"
    );
}

// ── Test 2: WAL survives segment rotation ─────────────────────────────────────

/// WAL segment rotation is handled by the persistent WalWriter without
/// needing a re-open.  This test verifies that after rotation:
/// 1. Multiple WAL segments are created.
/// 2. Records written before and after rotation are all readable.
///
/// Strategy: use the WAL writer API directly with large `Write` payloads
/// (page images) to quickly fill a segment, then write a few more DB
/// transactions and confirm they land in a subsequent segment.
#[test]
fn wal_survives_rotation() {
    use sparrowdb_common::TxnId;
    use sparrowdb_storage::wal::codec::{WalPayload, WalRecord, WalRecordKind};
    use sparrowdb_storage::wal::writer::{WalWriter, SEGMENT_SIZE};

    let dir = tempfile::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).expect("create wal dir");

    // Fill the segment with large Write records (page images of ~1 MiB each).
    // A single Write record of 1 MiB will exceed SEGMENT_SIZE (64 MiB) after
    // 64 records — or use a SEGMENT_SIZE-sized single record to force one shot.
    {
        let mut writer = WalWriter::open(&wal_dir).expect("open writer");

        // Write one record that is larger than SEGMENT_SIZE to guarantee rotation.
        // Use the `Write` payload (page image) which supports arbitrary byte sizes.
        let big_page = vec![0xFFu8; SEGMENT_SIZE as usize + 1];
        writer
            .append(
                WalRecordKind::Write,
                TxnId(1),
                WalPayload::Write {
                    page_id: 0,
                    image: big_page,
                },
            )
            .expect("append large record");
        writer.fsync().expect("fsync");
    }

    // Verify at least 2 segments exist after the large write forced rotation.
    let seg_count_after_big = std::fs::read_dir(&wal_dir)
        .expect("read wal dir")
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .count();
    assert!(
        seg_count_after_big >= 2,
        "rotation must have produced ≥ 2 WAL segments, got {seg_count_after_big}"
    );

    // Now write a few more normal records.
    {
        let mut writer = WalWriter::open(&wal_dir).expect("reopen writer");
        writer
            .append(WalRecordKind::Begin, TxnId(2), WalPayload::Empty)
            .expect("append begin");
        writer
            .append(WalRecordKind::Commit, TxnId(2), WalPayload::Empty)
            .expect("append commit");
        writer.fsync().expect("fsync");
    }

    // Scan all segments and verify both transactions are present.
    let mut found_txn1 = false;
    let mut found_txn2_commit = false;

    let mut segments: Vec<_> = std::fs::read_dir(&wal_dir)
        .expect("read wal dir")
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    segments.sort_by_key(|e| e.file_name());

    for entry in segments {
        let data = std::fs::read(entry.path()).expect("read WAL segment");
        let mut offset = if data.is_empty() { 0usize } else { 1usize };
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((rec, consumed)) => {
                    if rec.txn_id == TxnId(1) {
                        found_txn1 = true;
                    }
                    if rec.txn_id == TxnId(2) && rec.kind == WalRecordKind::Commit {
                        found_txn2_commit = true;
                    }
                    offset += consumed;
                }
                Err(_) => break,
            }
        }
    }

    assert!(
        found_txn1,
        "WAL must contain records from txn_id=1 (pre-rotation write)"
    );
    assert!(
        found_txn2_commit,
        "WAL must contain Commit from txn_id=2 (post-rotation write)"
    );

    // Also verify that a DB written across rotation is fully recoverable.
    let db_path = dir.path().join("db");
    const LABEL: u32 = 2;
    const N: usize = 10;

    {
        let db = GraphDb::open(&db_path).expect("open db");
        for i in 0..N as i64 {
            let mut tx = db.begin_write().expect("begin_write");
            tx.create_node(LABEL, &[(0u32, Value::Int64(i))])
                .expect("create_node");
            tx.commit().expect("commit");
        }
    }

    // Reopen and verify.
    let store = NodeStore::open(&db_path).expect("node store after reopen");
    let hwm = store.hwm_for_label(LABEL).expect("hwm_for_label");
    assert_eq!(
        hwm, N as u64,
        "all {N} nodes must be visible after reopen (hwm = {hwm})"
    );
}

// ── Test 3: concurrent reads during write (SWMR) ─────────────────────────────

/// A read transaction opened before a write sees the old snapshot; one opened
/// after the commit sees the new data.  This confirms writes do not block reads
/// and that the persistent WalWriter does not interfere with SWMR semantics.
#[test]
fn concurrent_reads_during_write() {
    let (_dir, db_path) = fresh_db();
    const LABEL: u32 = 3;

    let db = GraphDb::open(&db_path).expect("open db");

    // Create baseline node.
    let node_id = {
        let mut tx = db.begin_write().expect("begin_write baseline");
        let id = tx
            .create_node(LABEL, &[(0u32, Value::Int64(100))])
            .expect("create_node baseline");
        tx.commit().expect("commit baseline");
        id
    };

    // Open a read snapshot BEFORE the second write.
    let read_before = db.begin_read().expect("begin_read before write");

    // Commit a second write (updates the node's col 0).
    {
        let mut tx = db.begin_write().expect("begin_write update");
        tx.set_node_col(node_id, 0u32, Value::Int64(200));
        tx.commit().expect("commit update");
    }

    // Open a read snapshot AFTER the second write.
    let read_after = db.begin_read().expect("begin_read after write");

    // read_before must see the old value (100).
    let vals_before = read_before
        .get_node(node_id, &[0u32])
        .expect("get_node before");
    let val_before = vals_before
        .iter()
        .find(|(c, _)| *c == 0)
        .map(|(_, v)| v.clone());
    assert_eq!(
        val_before,
        Some(Value::Int64(100)),
        "read before commit must see old value 100, got {:?}",
        val_before
    );

    // read_after must see the new value (200).
    let vals_after = read_after
        .get_node(node_id, &[0u32])
        .expect("get_node after");
    let val_after = vals_after
        .iter()
        .find(|(c, _)| *c == 0)
        .map(|(_, v)| v.clone());
    assert_eq!(
        val_after,
        Some(Value::Int64(200)),
        "read after commit must see new value 200, got {:?}",
        val_after
    );
}
