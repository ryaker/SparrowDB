//! Integration tests for `ReadTx::query` — snapshot-isolated Cypher reads.
//!
//! ## What SparrowDB snapshot isolation covers today
//!
//! SparrowDB uses a Single-Writer-Multiple-Reader (SWMR) model.
//! The `snapshot_txn_id` captured at `begin_read` time is used for **property
//! value** isolation via an in-memory MVCC version chain: `set_node_col` writes
//! go through the version chain and a `ReadTx` pinned before the write will see
//! the old value via `ReadTx::get_node`.
//!
//! **Structural changes** (new nodes written via `CREATE`) are flushed straight
//! to the on-disk node store on commit and are therefore immediately visible to
//! all readers, including `ReadTx` handles opened before the write.  This is a
//! known architectural constraint: snapshot isolation of node creation requires
//! a per-node visibility bitmap which is not yet implemented.
//!
//! The `ReadTx::query` Cypher path reads from disk and therefore sees all
//! committed nodes regardless of the `snapshot_txn_id`.  The snapshot_txn_id
//! provides isolation only for property-value updates (`set_node_col`) read via
//! the MVCC version chain.
//!
//! Tests in this file cover:
//!   1. `ReadTx::query` returns correct rows for a stable (non-mutated) dataset.
//!   2. `ReadTx::query` is re-usable on the same handle.
//!   3. Mutation / DDL statements return `Error::ReadOnly`.
//!   4. Multiple concurrent `ReadTx` handles work without blocking.
//!   5. A `ReadTx` opened after a commit sees the committed data.

use sparrowdb::GraphDb;
use sparrowdb_common::Error;
use sparrowdb_execution::Value as ExecValue;

fn open_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");
    (dir, db)
}

// ── Test 1: basic read-only query ────────────────────────────────────────────

/// A `ReadTx::query` over a stable dataset returns the committed rows.
#[test]
fn readtx_query_basic() {
    let (_dir, db) = open_db();

    db.execute("CREATE (n:Person {name: 1, age: 30})")
        .expect("create Alice");
    db.execute("CREATE (n:Person {name: 2, age: 25})")
        .expect("create Bob");

    let tx = db.begin_read().expect("begin_read");
    let r = tx
        .query("MATCH (n:Person) RETURN n.age")
        .expect("query persons");
    assert_eq!(r.rows.len(), 2, "should see both Alice and Bob");
}

// ── Test 2: new ReadTx after commit sees committed data ───────────────────────

/// A `ReadTx` opened after a write commit sees the new data.
#[test]
fn readtx_opened_after_commit_sees_new_data() {
    let (_dir, db) = open_db();

    db.execute("CREATE (n:Person {name: 1, age: 30})")
        .expect("create Alice");

    let snap1 = db.begin_read().expect("begin_read after Alice");
    assert_eq!(
        snap1
            .query("MATCH (n:Person) RETURN n.age")
            .expect("query")
            .rows
            .len(),
        1,
        "first snapshot sees Alice"
    );

    db.execute("CREATE (n:Person {name: 2, age: 25})")
        .expect("create Bob");

    let snap2 = db.begin_read().expect("begin_read after Bob");
    assert!(
        snap2.snapshot_txn_id > snap1.snapshot_txn_id,
        "newer snapshot should have higher txn_id"
    );
    let r = snap2
        .query("MATCH (n:Person) RETURN n.age")
        .expect("query after Bob");
    assert_eq!(r.rows.len(), 2, "second snapshot sees both Alice and Bob");
}

// ── Test 3: returned values have the right type ───────────────────────────────

#[test]
fn readtx_query_value_types() {
    let (_dir, db) = open_db();

    db.execute("CREATE (n:Person {name: 1, age: 42})")
        .expect("create node");

    let tx = db.begin_read().expect("begin_read");
    let r = tx
        .query("MATCH (n:Person) RETURN n.age")
        .expect("query age");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0][0],
        ExecValue::Int64(42),
        "age should be Int64(42)"
    );
}

// ── Test 4: mutation statements are rejected ─────────────────────────────────

/// Submitting a CREATE to ReadTx::query must return `Error::ReadOnly`.
#[test]
fn readtx_query_rejects_create() {
    let (_dir, db) = open_db();
    let tx = db.begin_read().expect("begin_read");
    let err = tx
        .query("CREATE (n:Person {name: 99})")
        .expect_err("CREATE must fail in ReadTx");
    assert!(
        matches!(err, Error::ReadOnly),
        "expected ReadOnly, got: {err:?}"
    );
}

/// Submitting a MATCH … SET to ReadTx::query must return `Error::ReadOnly`.
#[test]
fn readtx_query_rejects_set() {
    let (_dir, db) = open_db();
    db.execute("CREATE (n:Person {name: 1, age: 30})")
        .expect("seed data");
    let tx = db.begin_read().expect("begin_read");
    let err = tx
        .query("MATCH (n:Person) SET n.age = 99")
        .expect_err("SET must fail in ReadTx");
    assert!(
        matches!(err, Error::ReadOnly),
        "expected ReadOnly, got: {err:?}"
    );
}

/// Submitting CHECKPOINT to ReadTx::query must return `Error::ReadOnly`.
#[test]
fn readtx_query_rejects_checkpoint() {
    let (_dir, db) = open_db();
    let tx = db.begin_read().expect("begin_read");
    let err = tx
        .query("CHECKPOINT")
        .expect_err("CHECKPOINT must fail in ReadTx");
    assert!(
        matches!(err, Error::ReadOnly),
        "expected ReadOnly, got: {err:?}"
    );
}

/// Submitting MERGE to ReadTx::query must return `Error::ReadOnly`.
#[test]
fn readtx_query_rejects_merge() {
    let (_dir, db) = open_db();
    let tx = db.begin_read().expect("begin_read");
    let err = tx
        .query("MERGE (n:Person {name: 1})")
        .expect_err("MERGE must fail in ReadTx");
    assert!(
        matches!(err, Error::ReadOnly),
        "expected ReadOnly, got: {err:?}"
    );
}

// ── Test 5: empty-db read returns zero rows (no panic) ───────────────────────

#[test]
fn readtx_query_empty_db() {
    let (_dir, db) = open_db();
    let tx = db.begin_read().expect("begin_read");
    let r = tx
        .query("MATCH (n:Person) RETURN n.name")
        .expect("query empty db");
    assert_eq!(r.rows.len(), 0);
}

// ── Test 6: multiple concurrent ReadTx instances ─────────────────────────────

/// Multiple `ReadTx` handles coexist and query without blocking each other.
#[test]
fn readtx_query_concurrent_readers() {
    let (_dir, db) = open_db();
    db.execute("CREATE (n:Person {name: 1, age: 30})")
        .expect("create person");

    let tx1 = db.begin_read().expect("tx1");
    let tx2 = db.begin_read().expect("tx2");
    let tx3 = db.begin_read().expect("tx3");

    let r1 = tx1.query("MATCH (n:Person) RETURN n.age").expect("tx1 query");
    let r2 = tx2.query("MATCH (n:Person) RETURN n.age").expect("tx2 query");
    let r3 = tx3.query("MATCH (n:Person) RETURN n.age").expect("tx3 query");

    assert_eq!(r1.rows.len(), 1);
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(r3.rows.len(), 1);
}

// ── Test 7: ReadTx can be re-used for multiple queries ───────────────────────

#[test]
fn readtx_query_reusable() {
    let (_dir, db) = open_db();
    db.execute("CREATE (n:Person {name: 1, age: 30})")
        .expect("create person");

    let tx = db.begin_read().expect("begin_read");
    // Query the same ReadTx twice — should not error or panic.
    let r1 = tx.query("MATCH (n:Person) RETURN n.name").expect("first query");
    let r2 = tx.query("MATCH (n:Person) RETURN n.age").expect("second query");
    assert_eq!(r1.rows.len(), 1);
    assert_eq!(r2.rows.len(), 1);
}

// ── Test 8: coexistence with an active WriteTx ───────────────────────────────

/// A ReadTx can query while a WriteTx is open (uncommitted), and sees only
/// the committed state at the time of `begin_read`.
#[test]
fn readtx_query_coexists_with_open_writetx() {
    let (_dir, db) = open_db();
    db.execute("CREATE (n:Person {name: 1, age: 30})")
        .expect("seed Alice");

    // Open a ReadTx.
    let read = db.begin_read().expect("begin_read");

    // Open a WriteTx and leave it uncommitted.
    let write = db.begin_write().expect("begin_write");

    // ReadTx::query must succeed even while the writer is open.
    let r = read
        .query("MATCH (n:Person) RETURN n.age")
        .expect("query while writer is open");
    assert_eq!(r.rows.len(), 1, "Alice should be visible");

    // Drop the writer without committing.
    drop(write);
}
