//! SPA-250: `GraphDb::execute_batch` — grouped writes with single WAL fsync.
//!
//! ## What this tests
//!
//! * **Happy path** — 100 CREATE statements execute as one batch; all nodes
//!   are visible immediately after `execute_batch` returns.
//! * **Atomicity** — when the batch contains an invalid statement, no nodes
//!   from that batch are committed (all-or-nothing rollback).
//! * **Empty batch** — `execute_batch(&[])` returns `Ok(vec![])` without
//!   acquiring the write lock or touching the WAL.
//! * **Single-item batch** — equivalent to a single `execute()` call; the
//!   created node is visible afterwards.
//! * **WAL single-commit** — after a 100-item batch the WAL contains exactly
//!   one Commit record (proving a single fsync rather than 100).

use sparrowdb::GraphDb;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open GraphDb");
    (dir, db)
}

/// Count nodes with label `label` via `MATCH (n:L) RETURN count(n)`.
fn count_nodes(db: &GraphDb, label: &str) -> u64 {
    let q = format!("MATCH (n:{label}) RETURN count(n)");
    let res = db.execute(&q).expect("count query");
    if res.rows.is_empty() {
        return 0;
    }
    match &res.rows[0][0] {
        sparrowdb_execution::Value::Int64(n) => *n as u64,
        sparrowdb_execution::Value::Float64(f) => *f as u64,
        _ => 0,
    }
}

// ── Test 1: 100 CREATEs — all nodes visible after batch ─────────────────────

#[test]
fn batch_100_creates_all_visible() {
    let (_dir, db) = fresh_db();

    let queries: Vec<String> = (0..100)
        .map(|i| format!("CREATE (n:BatchNode {{id: {i}}})", i = i))
        .collect();
    let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();

    let results = db.execute_batch(&query_refs).expect("execute_batch");

    // One result per query.
    assert_eq!(results.len(), 100, "should have 100 results");

    // All 100 nodes must be visible via a MATCH query.
    let count = count_nodes(&db, "BatchNode");
    assert_eq!(
        count, 100,
        "all 100 batch-created nodes must be visible (got {count})"
    );
}

// ── Test 2: batch failure mid-way — atomicity / full rollback ────────────────

/// If any query in the batch fails, **none** of the preceding creates must
/// be committed — the WriteTx is dropped without commit.
#[test]
fn batch_failure_rolls_back_entire_batch() {
    let (_dir, db) = fresh_db();

    // First, write one node outside the batch so we have a baseline.
    db.execute("CREATE (n:Baseline {v: 1})").expect("baseline");

    // Build a batch: 50 valid CREATEs, then one invalid query, then 50 more.
    let mut queries: Vec<String> = Vec::new();
    for i in 0..50 {
        queries.push(format!("CREATE (n:AtomicNode {{id: {i}}})"));
    }
    // This is syntactically invalid — will cause a parse/bind error.
    queries.push("THIS IS NOT VALID CYPHER".to_string());
    for i in 50..100 {
        queries.push(format!("CREATE (n:AtomicNode {{id: {i}}})"));
    }
    let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();

    // The batch must return an error.
    let result = db.execute_batch(&query_refs);
    assert!(result.is_err(), "batch with invalid query must return Err");

    // No AtomicNode must have been committed.
    let count = count_nodes(&db, "AtomicNode");
    assert_eq!(
        count, 0,
        "failed batch must not commit any nodes (got {count} AtomicNodes)"
    );

    // The baseline node must still be present (unrelated transaction).
    let baseline = count_nodes(&db, "Baseline");
    assert_eq!(baseline, 1, "Baseline node must survive the failed batch");
}

// ── Test 3: empty batch returns Ok(vec![]) ───────────────────────────────────

#[test]
fn empty_batch_returns_empty_vec() {
    let (_dir, db) = fresh_db();

    let results = db.execute_batch(&[]).expect("empty batch");
    assert!(results.is_empty(), "empty batch must return Ok(empty vec)");
}

// ── Test 4: single-item batch equivalent to single execute ────────────────────

#[test]
fn single_item_batch_equivalent_to_execute() {
    let (_dir, db) = fresh_db();

    let results = db
        .execute_batch(&["CREATE (n:Solo {v: 42})"])
        .expect("single-item batch");

    // One result returned.
    assert_eq!(results.len(), 1, "single-item batch must return 1 result");

    // Node must be visible.
    let count = count_nodes(&db, "Solo");
    assert_eq!(
        count, 1,
        "single-item batch must create 1 node (got {count})"
    );
}

// ── Test 5: WAL has exactly one Commit record after a 100-item batch ─────────

/// Verifies the "single WAL fsync" guarantee: after `execute_batch(&100_queries)`,
/// the WAL should contain exactly **one** Commit record for that batch
/// (rather than 100 Commit records as individual `execute()` calls would emit).
#[test]
fn batch_produces_single_wal_commit() {
    use sparrowdb_storage::wal::codec::{WalRecord, WalRecordKind};

    let (dir, db) = fresh_db();
    let db_path = dir.path().to_path_buf();

    let queries: Vec<String> = (0..100)
        .map(|i| format!("CREATE (n:WalNode {{id: {i}}})"))
        .collect();
    let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();
    db.execute_batch(&query_refs).expect("execute_batch");

    // Count Commit records in the WAL.
    let wal_dir = db_path.join("wal");
    assert!(wal_dir.exists(), "WAL directory must exist after batch");

    let mut commit_count = 0usize;
    let mut node_create_count = 0usize;

    for entry in std::fs::read_dir(&wal_dir).expect("read wal").flatten() {
        if !entry.file_name().to_string_lossy().ends_with(".wal") {
            continue;
        }
        let data = std::fs::read(entry.path()).expect("read segment");
        // Skip the 1-byte WAL format version header.
        let mut offset = if data.is_empty() { 0usize } else { 1usize };
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((rec, consumed)) => {
                    match rec.kind {
                        WalRecordKind::Commit => commit_count += 1,
                        WalRecordKind::NodeCreate => node_create_count += 1,
                        _ => {}
                    }
                    offset += consumed;
                }
                Err(_) => break,
            }
        }
    }

    assert_eq!(
        commit_count, 1,
        "execute_batch of 100 queries must produce exactly 1 WAL Commit record (got {commit_count})"
    );
    assert_eq!(
        node_create_count, 100,
        "WAL must contain 100 NodeCreate records (got {node_create_count})"
    );
}

// ── Test 6: mixed read and write in batch ─────────────────────────────────────

/// Demonstrates that read-only queries (MATCH … RETURN) can be included in
/// the batch alongside writes.  The write results are empty; the read results
/// carry data.
#[test]
fn batch_mixed_read_write() {
    let (_dir, db) = fresh_db();

    // Pre-populate one node.
    db.execute("CREATE (n:Existing {v: 99})")
        .expect("pre-populate");

    let queries = [
        "CREATE (n:MixedNode {id: 1})",
        "MATCH (n:Existing) RETURN n.v",
        "CREATE (n:MixedNode {id: 2})",
    ];

    let results = db.execute_batch(&queries).expect("mixed batch");
    assert_eq!(results.len(), 3);

    // The read result (index 1) must have one row.
    assert_eq!(
        results[1].rows.len(),
        1,
        "MATCH result in batch must return 1 row"
    );

    // Both MixedNodes must be visible.
    let count = count_nodes(&db, "MixedNode");
    assert_eq!(count, 2, "both MixedNodes must be visible after batch");
}
