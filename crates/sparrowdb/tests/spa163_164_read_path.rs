//! Regression tests for SPA-163 and SPA-164 read-path bugs.
//!
//! SPA-163: Edges written via `WriteTx::create_edge` must be visible to hop
//!          queries *without* a CHECKPOINT.
//!
//! SPA-164: Nodes deleted via `WriteTx::delete_node` must not appear in scan
//!          results.

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::types::Value as ExecValue;
use sparrowdb_storage::node_store::Value;
use std::collections::HashMap;

// ── helpers ──────────────────────────────────────────────────────────────────

fn db_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

// ── SPA-163: delta log edges visible without checkpoint ───────────────────────

/// Create Person nodes, connect them with a KNOWS edge, then query via MATCH
/// without calling CHECKPOINT.  The edge lives only in the delta log at this
/// point; the execution engine must consult the delta log to see it.
#[test]
fn spa163_delta_edges_visible_without_checkpoint() {
    let dir = db_dir();
    let db = open(dir.path()).expect("open db");

    // Register the Person label and KNOWS relationship so the query engine can
    // resolve them.  The catalog persists immediately on write; WriteTx does
    // not expose create_rel_table, so we use the Catalog API directly.
    let label_id;
    {
        let mut cat = Catalog::open(dir.path()).expect("catalog");
        label_id = cat.create_label("Person").expect("create_label Person") as u32;
        cat.create_rel_table(label_id as u16, label_id as u16, "KNOWS")
            .expect("create_rel_table KNOWS");
    }

    // Create two Person nodes.
    let alice;
    let bob;
    {
        let mut tx = db.begin_write().unwrap();
        alice = tx
            .create_node(label_id, &[(0u32, Value::Int64(1))])
            .expect("create Alice");
        bob = tx
            .create_node(label_id, &[(0u32, Value::Int64(2))])
            .expect("create Bob");
        tx.commit().unwrap();
    }

    // Connect Alice → Bob with a KNOWS edge.  No checkpoint — edge stays in
    // the delta log.
    {
        let mut tx = db.begin_write().unwrap();
        tx.create_edge(alice, bob, "KNOWS", HashMap::new())
            .expect("create_edge");
        tx.commit().unwrap();
    }

    // Execute a 1-hop query.  The delta log must be merged into the hop result.
    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.col_0")
        .expect("execute");

    assert!(
        !result.rows.is_empty(),
        "SPA-163: delta log edges must be visible without checkpoint; got 0 rows"
    );

    // Bob's col_0 should be 2.
    let bob_col0: Vec<i64> = result
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(ExecValue::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert!(
        bob_col0.contains(&2),
        "SPA-163: expected Bob (col_0=2) in results; got {bob_col0:?}"
    );
}

// ── SPA-164: tombstoned nodes invisible to scan ───────────────────────────────

/// Create a Person node, delete it, then scan all Person nodes.  The deleted
/// node must not appear in the results.
#[test]
fn spa164_deleted_node_absent_from_scan() {
    let dir = db_dir();
    let db = open(dir.path()).expect("open db");

    // Register the Person label.
    let label_id;
    {
        let mut cat = Catalog::open(dir.path()).expect("catalog");
        label_id = cat.create_label("Person").expect("create_label Person") as u32;
    }

    // Create Alice (to survive deletion) and Dave (to be deleted).
    let alice;
    let dave;
    {
        let mut tx = db.begin_write().unwrap();
        alice = tx
            .create_node(label_id, &[(0u32, Value::Int64(1))])
            .expect("create Alice");
        dave = tx
            .create_node(label_id, &[(0u32, Value::Int64(4))])
            .expect("create Dave");
        tx.commit().unwrap();
    }
    let _ = alice; // Alice stays alive.

    // Delete Dave.
    {
        let mut tx = db.begin_write().unwrap();
        tx.delete_node(dave).expect("delete_node");
        tx.commit().unwrap();
    }

    // Scan all Person nodes.  Dave must not appear.
    let result = db
        .execute("MATCH (n:Person) RETURN n.col_0")
        .expect("execute");

    let col0_vals: Vec<i64> = result
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(ExecValue::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();

    assert!(
        !col0_vals.contains(&4),
        "SPA-164: deleted node (col_0=4, Dave) must not appear in scan; got {col0_vals:?}"
    );

    // Alice (col_0=1) should still be visible.
    assert!(
        col0_vals.contains(&1),
        "SPA-164: live node (col_0=1, Alice) must remain visible; got {col0_vals:?}"
    );
}

// ── SPA-164: tombstoned node col_0 sentinel is u64::MAX ─────────────────────

/// Verify that the tombstone value (u64::MAX) stored in col_0 is correctly
/// detected as "deleted" and not surfaced as an integer row.
#[test]
fn spa164_tombstone_sentinel_not_emitted() {
    let dir = db_dir();
    let db = open(dir.path()).expect("open db");

    let label_id;
    {
        let mut cat = Catalog::open(dir.path()).expect("catalog");
        label_id = cat.create_label("Widget").expect("create_label Widget") as u32;
    }

    // Create and immediately delete a single Widget node.
    let widget;
    {
        let mut tx = db.begin_write().unwrap();
        widget = tx
            .create_node(label_id, &[(0u32, Value::Int64(42))])
            .expect("create Widget");
        tx.commit().unwrap();
    }
    {
        let mut tx = db.begin_write().unwrap();
        tx.delete_node(widget).expect("delete_node Widget");
        tx.commit().unwrap();
    }

    let result = db
        .execute("MATCH (w:Widget) RETURN w.col_0")
        .expect("execute");

    assert!(
        result.rows.is_empty(),
        "SPA-164: all rows for a fully-deleted label must be empty; got {} rows",
        result.rows.len()
    );
}
