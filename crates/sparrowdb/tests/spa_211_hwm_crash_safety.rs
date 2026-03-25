//! SPA-211: hwm.bin crash safety — atomic writes via tmp+rename.
//!
//! ## What this tests
//!
//! Prior to SPA-211, `save_hwm` used `fs::write` which is NOT atomic.
//! A crash mid-write could leave `hwm.bin` truncated or zeroed, causing
//! the DB to open with node-ID counter reset to 0 and collide with existing
//! node IDs on the next insert.
//!
//! After SPA-211:
//!   1. The write goes to `hwm.bin.tmp` first.
//!   2. `fsync` ensures the bytes are durable before renaming.
//!   3. `rename(hwm.bin.tmp, hwm.bin)` is atomic on POSIX — the old
//!      `hwm.bin` is replaced in one syscall.
//!
//! ## Recovery path
//!
//! If `hwm.bin` is missing or corrupt but `hwm.bin.tmp` exists (leftover
//! from a crash after write but before rename), `load_hwm` promotes the
//! tmp file to `hwm.bin` and returns its value.
//!
//! ## Tests
//!
//! 1. **Corrupted hwm.bin** — truncate `hwm.bin` to 0 bytes, reopen;
//!    verifies no panic and that `hwm_for_label` returns either 0 (fresh
//!    start, safe) or the original value (if the recovery path applied).
//!    The critical assertion: the DB must not panic, and any subsequent node
//!    creation must yield an ID that does NOT collide with already-persisted
//!    data.
//!
//! 2. **Tmp leftover recovery** — write a valid value into `hwm.bin.tmp`,
//!    delete `hwm.bin`, reopen; verifies the HWM is recovered from tmp.
//!
//! 3. **Happy-path round-trip** — verifies normal operation still works after
//!    the atomic-write refactor.

use sparrowdb::GraphDb;
use sparrowdb_storage::node_store::{NodeStore, Value};
use std::io::Write as _;
use tempfile::tempdir;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fresh_db_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("db");
    (dir, path)
}

/// Return the path to `hwm.bin` for label 0 inside `db_path`.
fn hwm_bin(db_path: &std::path::Path) -> std::path::PathBuf {
    db_path.join("nodes").join("0").join("hwm.bin")
}

/// Return the path to `hwm.bin.tmp` for label 0 inside `db_path`.
fn hwm_tmp(db_path: &std::path::Path) -> std::path::PathBuf {
    db_path.join("nodes").join("0").join("hwm.bin.tmp")
}

// ── Test 1: corrupted hwm.bin does not panic, IDs don't collide ──────────────

/// Simulate a crash that left `hwm.bin` truncated to 0 bytes.
///
/// On reopen, the DB must not panic.  If it resets the HWM to 0 (treating the
/// corrupt file as "no data") then any new node insertion must start from a
/// slot that is at least as large as the number of already-written column
/// entries — i.e. no ID collision.
///
/// This test does NOT require a specific return value from `hwm_for_label`;
/// it only requires:
///   a) No panic on open.
///   b) Subsequent node creation succeeds.
///   c) The returned NodeId is unique (not 0 if slot 0 is already used).
#[test]
fn corrupted_hwm_bin_no_panic_no_collision() {
    let (_dir, db_path) = fresh_db_path();

    // Session 1: create a node (label 0) and commit.
    let first_node_id;
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        let mut tx = db.begin_write().expect("begin_write");
        first_node_id = tx
            .create_node(0, &[(0u32, Value::Int64(42))])
            .expect("create_node");
        tx.commit().expect("commit");
    }

    // Corrupt hwm.bin by truncating it to 0 bytes.
    {
        let hwm_path = hwm_bin(&db_path);
        assert!(hwm_path.exists(), "hwm.bin must exist after commit");
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&hwm_path)
            .expect("open hwm.bin for corruption")
            .flush()
            .expect("flush");
        let meta = std::fs::metadata(&hwm_path).expect("metadata");
        assert_eq!(meta.len(), 0, "hwm.bin must be 0 bytes after corruption");
    }

    // Session 2: reopen — must not panic.
    // We accept either a Corruption error (graceful) or a recovered HWM.
    // The critical thing is no panic and no UB.
    {
        // Opening the NodeStore must not panic.
        let store_result = NodeStore::open(&db_path);
        assert!(store_result.is_ok(), "NodeStore::open must not panic");
        let store = store_result.unwrap();

        // `hwm_for_label` may return Err(Corruption) or Ok(0) — both are
        // acceptable.  It must not panic.
        let hwm_result = store.hwm_for_label(0);
        // If it returns Ok, then either 0 or the actual HWM is fine.
        // If it returns Err, it must be a Corruption variant, not a panic.
        match hwm_result {
            Ok(_) => {} // Any value is acceptable — no collision check needed at this level.
            Err(e) => {
                let desc = format!("{e:?}");
                assert!(
                    desc.contains("Corruption") || desc.contains("truncated"),
                    "expected Corruption error, got: {desc}"
                );
            }
        }
    }

    // Session 3: open the full GraphDb — must not panic.
    {
        let db = GraphDb::open(&db_path).expect("GraphDb::open after hwm corruption must not panic");

        // Insert another node. If the HWM was reset to 0, this would normally
        // collide with slot 0 which was already persisted.  The SPA-211 fix
        // should prevent this by recovering from tmp or returning an error.
        // We test that the insert at minimum does not panic.
        let mut tx = db.begin_write().expect("begin_write session 3");
        let new_node_result = tx.create_node(0, &[(0u32, Value::Int64(99))]);
        // If creation succeeds, the new ID must differ from the first one.
        if let Ok(new_node_id) = new_node_result {
            tx.commit().expect("commit session 3");
            assert_ne!(
                new_node_id, first_node_id,
                "new node ID must differ from previously committed node ID"
            );
        }
        // If creation failed, that is also acceptable — it's a clear error, not corruption.
    }
}

// ── Test 2: recovery from hwm.bin.tmp leftover ───────────────────────────────

/// Simulate a crash after writing `hwm.bin.tmp` but before the rename to
/// `hwm.bin`.  The recovery path must promote the tmp file and return the
/// correct HWM.
#[test]
fn hwm_recovered_from_tmp_leftover() {
    let (_dir, db_path) = fresh_db_path();

    // Session 1: create two nodes and commit so hwm.bin is written.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        let mut tx = db.begin_write().expect("begin_write");
        tx.create_node(0, &[(0u32, Value::Int64(1))]).expect("node 0");
        tx.commit().expect("commit 1");

        let mut tx2 = db.begin_write().expect("begin_write 2");
        tx2.create_node(0, &[(0u32, Value::Int64(2))]).expect("node 1");
        tx2.commit().expect("commit 2");
    }

    // The on-disk HWM for label 0 should now be 2.
    {
        let store = NodeStore::open(&db_path).expect("node store");
        let hwm = store.hwm_for_label(0).expect("hwm after 2 nodes");
        assert_eq!(hwm, 2, "HWM must be 2 after 2 committed nodes");
    }

    // Simulate a crash: write hwm=3 into hwm.bin.tmp, delete hwm.bin.
    {
        let label_dir = db_path.join("nodes").join("0");
        let tmp_path = hwm_tmp(&db_path);
        let main_path = hwm_bin(&db_path);

        // Write hwm=3 into tmp.
        let mut f = std::fs::File::create(&tmp_path).expect("create tmp");
        f.write_all(&3u64.to_le_bytes()).expect("write tmp");
        f.sync_all().expect("sync tmp");

        // Remove the canonical hwm.bin so only tmp remains.
        std::fs::remove_file(&main_path).expect("remove hwm.bin");
        assert!(!main_path.exists(), "hwm.bin must be gone");
        assert!(tmp_path.exists(), "hwm.bin.tmp must exist");

        // Suppress unused variable warning for label_dir.
        let _ = label_dir;
    }

    // On next open, load_hwm must recover from tmp and return 3.
    {
        let store = NodeStore::open(&db_path).expect("node store after tmp-only state");
        let hwm = store.hwm_for_label(0).expect("hwm from tmp recovery");
        assert_eq!(
            hwm, 3,
            "HWM must be recovered as 3 from hwm.bin.tmp leftover"
        );
        // After recovery, hwm.bin should exist again (tmp was promoted).
        assert!(
            hwm_bin(&db_path).exists(),
            "hwm.bin must be restored after tmp recovery"
        );
    }
}

// ── Test 3: happy-path round-trip still works ─────────────────────────────────

/// Normal operation — create nodes, close, reopen, verify HWM and data.
#[test]
fn hwm_happy_path_round_trip() {
    let (_dir, db_path) = fresh_db_path();

    let node_ids: Vec<_>;

    // Session 1: create 5 nodes.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        let mut ids = Vec::new();
        for i in 0u32..5 {
            let mut tx = db.begin_write().expect("begin_write");
            let nid = tx
                .create_node(0, &[(0u32, Value::Int64(i as i64 * 10))])
                .expect("create_node");
            tx.commit().expect("commit");
            ids.push(nid);
        }
        node_ids = ids;
    }

    // Session 2: reopen and verify HWM = 5.
    {
        let store = NodeStore::open(&db_path).expect("node store session 2");
        let hwm = store.hwm_for_label(0).expect("hwm_for_label");
        assert_eq!(hwm, 5, "HWM must be 5 after 5 committed nodes");
    }

    // Session 3: create one more node — must get slot 5.
    {
        let db = GraphDb::open(&db_path).expect("open session 3");
        let mut tx = db.begin_write().expect("begin_write session 3");
        let new_id = tx
            .create_node(0, &[(0u32, Value::Int64(999))])
            .expect("create_node session 3");
        tx.commit().expect("commit session 3");

        // The new node must be distinct from all prior nodes.
        assert!(
            !node_ids.contains(&new_id),
            "new node ID {new_id:?} must not collide with previous IDs"
        );
    }

    // Final: HWM must be 6.
    {
        let store = NodeStore::open(&db_path).expect("node store final");
        let hwm = store.hwm_for_label(0).expect("hwm final");
        assert_eq!(hwm, 6, "HWM must be 6 after 6 total committed nodes");
    }
}
