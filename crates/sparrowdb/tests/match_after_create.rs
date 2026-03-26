//! Regression test for SPA-242: MATCH (n:Label {prop: value}) silently
//! returns empty for nodes committed after the PropertyIndex was built.
//!
//! ## Root Cause
//!
//! `Engine::write_back_prop_index` merges the engine's lazily-built index into
//! the shared per-`GraphDb` cache after each query.  Without a generation check,
//! a read query that began BEFORE a write committed can write its stale (partial)
//! index back AFTER the write cleared the cache, leaving the shared cache with
//! an outdated BTree that misses the newly committed nodes.
//!
//! The next query clones this stale cache, calls `build_for` (which is a
//! cache-hit no-op because `loaded` already contains the key), and gets
//! `Some([])` from the index — zero candidates — silently returning no rows
//! even though the node exists on disk.
//!
//! ## Fix (SPA-242)
//!
//! `PropertyIndex::clear()` now increments a `generation` counter.  Cloning
//! the cache copies the current generation.  `write_back_prop_index` only
//! merges back when the shared cache's generation matches the engine's
//! generation, preventing stale write-backs that cross a cache-invalidation
//! boundary.

use sparrowdb::open;
use sparrowdb_storage::node_store::NodeStore;
use sparrowdb_storage::property_index::PropertyIndex;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: post-checkpoint + WAL-only nodes ─────────────────────────────────

/// Creates 1 000 checkpointed nodes and 1 000 WAL-only nodes, then MATCHes
/// all 2 000 by uid.  Every uid must be found.
#[test]
fn match_finds_all_nodes_after_wal_only_creates() {
    let (_dir, db) = make_db();

    // Phase 1: 1 000 nodes → checkpoint so they are in column files on disk.
    for uid in 1i64..=1000 {
        db.execute(&format!("CREATE (:User {{uid: {uid}}})"))
            .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();

    // Phase 2: 1 000 more nodes — WAL only (no checkpoint).
    for uid in 1001i64..=2000 {
        db.execute(&format!("CREATE (:User {{uid: {uid}}})"))
            .unwrap();
    }

    // Phase 3: every uid must be found.
    let mut misses = Vec::new();
    for uid in 1i64..=2000 {
        let r = db
            .execute(&format!("MATCH (n:User {{uid: {uid}}}) RETURN n.uid"))
            .unwrap();
        if r.rows.is_empty() {
            misses.push(uid);
        }
    }
    assert!(
        misses.is_empty(),
        "SPA-242: MATCH returned empty for {} uid(s): {:?}",
        misses.len(),
        &misses[..misses.len().min(10)]
    );
}

// ── Test 2: MATCH after sequential CREATEs ───────────────────────────────────

/// Basic smoke test: create uid=10, then uid=20, verify both are findable.
#[test]
fn match_finds_node_after_second_create() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {uid: 10})").unwrap();
    // Warm the shared index cache by running a MATCH.
    let r10a = db.execute("MATCH (n:User {uid: 10}) RETURN n.uid").unwrap();
    assert_eq!(r10a.rows.len(), 1, "uid=10 must be found");

    // Commit a second node — this clears the shared index cache.
    db.execute("CREATE (:User {uid: 20})").unwrap();

    // Both nodes must be found.  Without the SPA-242 fix, uid=20 would be
    // missed if a concurrent (or previous) read wrote back a stale index.
    let r10b = db.execute("MATCH (n:User {uid: 10}) RETURN n.uid").unwrap();
    let r20 = db.execute("MATCH (n:User {uid: 20}) RETURN n.uid").unwrap();
    assert_eq!(
        r10b.rows.len(),
        1,
        "uid=10 must still be found after uid=20 CREATE"
    );
    assert_eq!(r20.rows.len(), 1, "uid=20 must be found after its CREATE");
}

// ── Test 3: MATCH…CREATE edges over 100 nodes ────────────────────────────────

/// Simulates the Twitter import pattern: create 100 nodes, then MATCH pairs
/// and create edges.  All 99 edges must be created (no silent misses).
#[test]
fn match_create_edges_finds_all_nodes() {
    let (_dir, db) = make_db();

    for uid in 1i64..=100 {
        db.execute(&format!("CREATE (:User {{uid: {uid}}})"))
            .unwrap();
    }

    for src_uid in 1i64..=99 {
        let dst_uid = src_uid + 1;
        db.execute(&format!(
            "MATCH (a:User {{uid: {src_uid}}}), (b:User {{uid: {dst_uid}}}) \
             CREATE (a)-[:FOLLOWS]->(b)"
        ))
        .unwrap_or_else(|e| panic!("match create {src_uid}→{dst_uid} failed: {e}"));
    }

    let r = db
        .execute("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN COUNT(*)")
        .unwrap();
    let edge_count = match r.rows.first().and_then(|row| row.first()) {
        Some(sparrowdb_execution::types::Value::Int64(n)) => *n,
        other => panic!("unexpected count result: {other:?}"),
    };
    assert_eq!(
        edge_count, 99,
        "SPA-242: expected 99 FOLLOWS edges but got {edge_count}"
    );
}

// ── Test 4: generation counter prevents stale write-back (unit) ──────────────

/// Directly tests the `PropertyIndex` generation counter introduced by SPA-242.
///
/// Sequence:
///   1. Shared cache at gen=0.
///   2. Clone the cache → engine gets gen=0.
///   3. Shared cache cleared (gen → 1).
///   4. Engine attempts write-back: gen mismatch (0 ≠ 1) → skipped.
///   5. Shared cache must be empty — no stale data was written back.
#[test]
fn generation_prevents_stale_writeback() {
    let shared = std::sync::RwLock::new(PropertyIndex::new());

    // Step 1: clone the shared index at gen=0.
    let mut engine_index = shared.read().unwrap().clone();
    assert_eq!(engine_index.generation, 0, "fresh index must be gen=0");

    // Simulate build_for loading a column into the engine's index so that the
    // key ends up in both `loaded` and `index`.  Using `insert` alone would not
    // mark the key in `loaded`, making `merge_from` a no-op regardless of the
    // generation guard and rendering the test unable to detect regressions.
    let tmp_store = tempfile::tempdir().expect("tempdir for NodeStore");
    let store = NodeStore::open(tmp_store.path()).expect("open NodeStore");
    engine_index
        .build_for(&store, 1, 42)
        .expect("build_for on empty NodeStore must succeed");
    // Confirm the key is now loaded (a stale write-back would pollute the cache).
    assert!(
        engine_index.is_indexed(1, 42),
        "build_for must mark (1,42) as loaded"
    );

    // Step 2: simulate a write commit — clear the shared cache (gen → 1).
    {
        let mut guard = shared.write().unwrap();
        guard.clear();
        assert_eq!(guard.generation, 1, "clear() must increment generation");
    }

    // Step 3: attempt write-back (mirrors Engine::write_back_prop_index).
    {
        let mut guard = shared.write().unwrap();
        let engine_gen = engine_index.generation;
        if guard.generation == engine_gen {
            guard.merge_from(&engine_index);
        }
        // SPA-242 fix: guard.generation (1) != engine_gen (0) → skipped.
    }

    // Step 4: shared cache must be empty — stale data was NOT written back.
    {
        let guard = shared.read().unwrap();
        assert_eq!(guard.generation, 1, "generation must remain 1");
        assert!(
            !guard.is_indexed(1, 42),
            "SPA-242: stale write-back must not poison the shared cache"
        );
    }
}

// ── Test 5: clear increments generation monotonically ────────────────────────

/// `clear()` must increment the generation counter each time it is called.
#[test]
fn clear_increments_generation_monotonically() {
    let mut idx = PropertyIndex::new();
    assert_eq!(idx.generation, 0, "fresh index must be gen=0");
    idx.clear();
    assert_eq!(idx.generation, 1, "first clear → gen=1");
    idx.clear();
    assert_eq!(idx.generation, 2, "second clear → gen=2");
    idx.clear();
    assert_eq!(idx.generation, 3, "third clear → gen=3");
}

// ── Test 6: generation is preserved across Clone ─────────────────────────────

/// `Clone` must copy the generation so that the write-back guard can compare
/// the engine's generation against the shared cache's generation.
#[test]
fn clone_preserves_generation() {
    let mut idx = PropertyIndex::new();
    idx.clear(); // gen=1
    idx.clear(); // gen=2
    let cloned = idx.clone();
    assert_eq!(
        cloned.generation, 2,
        "Clone must preserve generation so write-back guard works"
    );
}
