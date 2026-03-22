//! SPA-184: WAL-first durability — mutations must be recorded to WAL before
//! any data page is touched.
//!
//! ## What this tests
//!
//! Before SPA-184 the commit sequence was:
//!   1. apply pending_ops to disk   ← data pages written
//!   2. apply property updates      ← data pages written
//!   3. write WAL records + fsync   ← WAL written AFTER data
//!
//! A crash between steps 1 and 3 would leave data pages mutated with no WAL
//! record — the mutation could not be replayed or rolled back.
//!
//! After SPA-184 the sequence is:
//!   1. write WAL records + fsync   ← WAL written FIRST
//!   2. apply pending_ops to disk
//!   3. apply property updates
//!
//! ## Test strategy
//!
//! We simulate a "crash" by:
//! - Opening a DB and starting a WriteTx.
//! - Creating nodes/edges inside the WriteTx.
//! - **Not** calling commit() — just dropping the WriteTx (uncommitted).
//! - Reopening the DB (simulating process restart).
//! - Asserting the node/edge is NOT visible (uncommitted → absent).
//!
//! And the happy path:
//! - Create nodes/edges, call commit().
//! - Drop the GraphDb (simulating process exit).
//! - Reopen the DB.
//! - Assert the node/edge IS visible (committed → durable).

use sparrowdb::GraphDb;
use sparrowdb_storage::node_store::{NodeStore, Value};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fresh_db_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("db");
    (dir, path)
}

// ── Test 1: uncommitted node NOT visible on reopen ───────────────────────────

/// After a crash (WriteTx dropped without commit), no data should be visible.
///
/// This verifies the WAL-first guarantee: if no COMMIT record was written to
/// the WAL, no mutation survives a restart.
#[test]
fn uncommitted_node_not_visible_on_reopen() {
    let (_dir, db_path) = fresh_db_path();

    // Session 1: open DB, create a node but drop the WriteTx without commit.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        let mut tx = db.begin_write().expect("begin_write");
        // Stage a node but do NOT commit — drop tx to discard.
        tx.create_node(1, &[(0u32, Value::Int64(0xDEAD))]).ok();
        // `tx` is dropped here without commit — simulates crash after
        // pending_ops were buffered but before commit() was called.
    }

    // Session 2: reopen and verify the node is absent.
    {
        let _db2 = GraphDb::open(&db_path).expect("open session 2");
        // Inspect the node store directly — no nodes for label_id=1 should
        // exist because the WriteTx was never committed.
        let store = NodeStore::open(&db_path).expect("node store");
        let hwm = store.hwm_for_label(1).expect("hwm_for_label");
        assert_eq!(
            hwm, 0,
            "uncommitted node must NOT appear on reopen: hwm for label 1 must be 0"
        );
    }
}

// ── Test 2: committed node IS visible on reopen ───────────────────────────────

/// After a clean commit, the node must be visible after DB reopen.
#[test]
fn committed_node_visible_on_reopen() {
    let (_dir, db_path) = fresh_db_path();
    let node_id;

    // Session 1: create and commit a node.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        let mut tx = db.begin_write().expect("begin_write");
        node_id = tx
            .create_node(2, &[(0u32, Value::Int64(42))])
            .expect("create_node");
        tx.commit().expect("commit");
        // `db` dropped — simulates clean process exit.
    }

    // Session 2: reopen and verify the node is present and has correct data.
    {
        let store = NodeStore::open(&db_path).expect("node store after reopen");
        let hwm = store.hwm_for_label(2).expect("hwm_for_label");
        assert_eq!(
            hwm, 1,
            "committed node must be visible on reopen: hwm for label 2 must be 1"
        );

        let vals = store
            .get_node(node_id, &[0u32])
            .expect("get_node after reopen");
        assert_eq!(vals.len(), 1);
        assert_eq!(
            vals[0].1,
            Value::Int64(42),
            "node col_0 must be 42 after reopen"
        );
    }
}

// ── Test 3: WAL has NodeCreate record before Commit ───────────────────────────

/// After commit(), the WAL segment must contain a NodeCreate record followed
/// by a Commit record.  This proves the WAL-first ordering.
#[test]
fn committed_node_has_wal_node_create_before_commit() {
    use sparrowdb_storage::wal::codec::{WalPayload, WalRecord, WalRecordKind};

    let (_dir, db_path) = fresh_db_path();

    {
        let db = GraphDb::open(&db_path).expect("open");
        let mut tx = db.begin_write().expect("begin_write");
        tx.create_node(3, &[(0u32, Value::Int64(99))])
            .expect("create_node");
        tx.commit().expect("commit");
    }

    // Scan WAL for a NodeCreate record that precedes a Commit record.
    let wal_dir = db_path.join("wal");
    assert!(wal_dir.exists(), "WAL directory must be created after commit");

    let mut all_kinds: Vec<WalRecordKind> = Vec::new();
    let mut node_id_in_wal: Option<u64> = None;

    for entry in std::fs::read_dir(&wal_dir)
        .expect("read wal dir")
        .flatten()
    {
        if !entry.file_name().to_string_lossy().ends_with(".wal") {
            continue;
        }
        let data = std::fs::read(entry.path()).expect("read WAL segment");
        let mut offset = 0usize;
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((rec, consumed)) => {
                    if rec.kind == WalRecordKind::NodeCreate {
                        if let WalPayload::NodeCreate { node_id, .. } = &rec.payload {
                            node_id_in_wal = Some(*node_id);
                        }
                    }
                    all_kinds.push(rec.kind);
                    offset += consumed;
                }
                Err(_) => break, // torn/padding
            }
        }
    }

    // NodeCreate must appear before Commit.
    let nc_pos = all_kinds
        .iter()
        .position(|k| *k == WalRecordKind::NodeCreate)
        .expect("WAL must contain a NodeCreate record");
    let commit_pos = all_kinds
        .iter()
        .position(|k| *k == WalRecordKind::Commit)
        .expect("WAL must contain a Commit record");

    assert!(
        nc_pos < commit_pos,
        "NodeCreate (pos {nc_pos}) must appear before Commit (pos {commit_pos}) in WAL"
    );
    assert!(
        node_id_in_wal.is_some(),
        "WAL NodeCreate record must carry a valid node_id"
    );
}

// ── Test 4: committed edge IS visible on reopen ───────────────────────────────

/// After create_edge + commit, the edge must survive a simulated restart.
/// Verified by inspecting the edge delta log directly via the storage layer.
#[test]
fn committed_edge_visible_on_reopen() {
    use sparrowdb_catalog::catalog::Catalog;
    use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};

    let (_dir, db_path) = fresh_db_path();

    // Session 1: create two nodes and a KNOWS edge using the low-level API.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");

        let mut tx = db.begin_write().expect("begin_write");
        let src = tx
            .create_node(0, &[(0u32, Value::Int64(1))])
            .expect("src node");
        let dst = tx
            .create_node(0, &[(0u32, Value::Int64(2))])
            .expect("dst node");
        tx.commit().expect("commit nodes");

        let mut tx = db.begin_write().expect("begin_write 2");
        tx.create_edge(src, dst, "KNOWS", std::collections::HashMap::new())
            .expect("create_edge");
        tx.commit().expect("commit edge");
        // Drop db — simulates process exit.
    }

    // Session 2: reopen and verify the edge delta log has entries.
    {
        let cat = Catalog::open(&db_path).expect("open catalog");
        let rel_tables = cat.list_rel_tables().expect("list_rel_tables");
        let knows_entry = rel_tables.iter().find(|(_, _, name)| name == "KNOWS");
        assert!(
            knows_entry.is_some(),
            "KNOWS rel type must be in catalog after reopen: {:?}",
            rel_tables
        );

        let (src_label, dst_label, _) = *knows_entry.unwrap();
        // get_rel_table returns the catalog RelTableId (u64); cast to storage RelTableId (u32).
        let catalog_rel_id = cat
            .get_rel_table(src_label, dst_label, "KNOWS")
            .expect("get_rel_table")
            .expect("KNOWS rel table must exist");
        let storage_rel_id = RelTableId(catalog_rel_id as u32);

        let es = EdgeStore::open(&db_path, storage_rel_id).expect("EdgeStore open");
        let deltas = es.read_delta().expect("read_delta");
        assert!(
            !deltas.is_empty(),
            "committed edge must appear in delta log after reopen (got {} entries)",
            deltas.len()
        );
    }
}

// ── Test 5: multiple commits all visible after reopen ─────────────────────────

/// Commits across multiple WriteTx sessions must all survive restart.
#[test]
fn multiple_commits_all_visible_on_reopen() {
    let (_dir, db_path) = fresh_db_path();

    // Session 1: write three separate transactions.
    {
        let db = GraphDb::open(&db_path).expect("open");
        for i in 0u64..3 {
            let mut tx = db.begin_write().expect("begin_write");
            tx.create_node(4, &[(0u32, Value::Int64(i as i64))])
                .expect("create_node");
            tx.commit().expect("commit");
        }
    }

    // Session 2: all three nodes must be visible.
    {
        let store = NodeStore::open(&db_path).expect("node store");
        let hwm = store.hwm_for_label(4).expect("hwm_for_label 4");
        assert_eq!(
            hwm, 3,
            "all 3 committed nodes must be visible on reopen (hwm = {hwm})"
        );
    }
}
