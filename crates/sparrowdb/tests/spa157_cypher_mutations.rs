//! SPA-157 acceptance tests — Cypher-level mutation round-trips.
//!
//! Tests that MERGE, MATCH … SET, and MATCH … DELETE are correctly parsed,
//! bound, and executed through the full Cypher pipeline.

use sparrowdb::{fnv1a_col_id, open, GraphDb};
use sparrowdb_storage::node_store::Value;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── MERGE: creates a new node when none exists ────────────────────────────────

#[test]
fn merge_creates_node_when_absent() {
    let (dir, db) = make_db();

    // MERGE with an integer identity prop.
    db.execute("MERGE (:Person {col_0: 42})")
        .expect("MERGE must succeed");

    // Verify the node was created via WriteTx / ReadTx.
    let rx = db.begin_read().unwrap();
    let catalog = sparrowdb_catalog::catalog::Catalog::open(dir.path()).unwrap();
    let store = sparrowdb_storage::node_store::NodeStore::open(dir.path()).unwrap();
    let label_id = catalog.get_label("Person").unwrap().expect("label") as u32;
    let hwm = store.hwm_for_label(label_id).unwrap();
    assert_eq!(hwm, 1, "one node must exist after MERGE");

    drop((rx, dir));
}

// ── MERGE: idempotent — calling twice yields the same node count ──────────────

#[test]
fn merge_is_idempotent_via_cypher() {
    let (dir, db) = make_db();

    // Two identical MERGEs.
    db.execute("MERGE (:Person {col_0: 99})")
        .expect("first MERGE");
    db.execute("MERGE (:Person {col_0: 99})")
        .expect("second MERGE");

    let catalog = sparrowdb_catalog::catalog::Catalog::open(dir.path()).unwrap();
    let store = sparrowdb_storage::node_store::NodeStore::open(dir.path()).unwrap();
    let label_id = catalog.get_label("Person").unwrap().expect("label") as u32;
    let hwm = store.hwm_for_label(label_id).unwrap();
    assert_eq!(hwm, 1, "two identical MERGEs must produce exactly one node");

    drop(dir);
}

// ── MATCH … SET: updates property on matched node ────────────────────────────

#[test]
fn match_set_updates_property() {
    let (dir, db) = make_db();

    // Seed: create a Person node with col_0 = 7.
    {
        use sparrowdb_storage::node_store::Value;
        let mut tx = db.begin_write().unwrap();
        let catalog_path = dir.path();
        // Use label_id = 1 for Person (create via catalog).
        let mut cat = sparrowdb_catalog::catalog::Catalog::open(catalog_path).unwrap();
        let label_id = cat.create_label("Person").unwrap() as u32;
        tx.create_node(label_id, &[(0u32, Value::Int64(7))])
            .expect("create");
        tx.commit().unwrap();
    }

    // SET via Cypher: update the "score" property on all Person nodes.
    db.execute("MATCH (n:Person) SET n.score = 42")
        .expect("MATCH SET must succeed");

    // Verify "score" = 42 on the node.
    // The "score" column is stored at fnv1a_col_id("score").
    {
        let catalog = sparrowdb_catalog::catalog::Catalog::open(dir.path()).unwrap();
        let label_id = catalog.get_label("Person").unwrap().expect("label") as u32;
        let node_id = sparrowdb_common::NodeId((label_id as u64) << 32);
        let score_col = fnv1a_col_id("score");
        let rx = db.begin_read().unwrap();
        let vals = rx
            .get_node(node_id, &[score_col])
            .expect("read score via ReadTx");
        assert_eq!(
            vals.first().map(|(_, v)| v.clone()),
            Some(Value::Int64(42)),
            "score must be 42 after MATCH … SET"
        );
    }

    drop(dir);
}

// ── MATCH … DELETE: tombstones the matched node ───────────────────────────────

#[test]
fn match_delete_tombstones_node() {
    let (dir, db) = make_db();

    // Seed: create a Person node.
    let node_id;
    {
        let mut cat = sparrowdb_catalog::catalog::Catalog::open(dir.path()).unwrap();
        let label_id = cat.create_label("Person").unwrap() as u32;
        let mut tx = db.begin_write().unwrap();
        node_id = tx
            .create_node(label_id, &[(0u32, Value::Int64(5))])
            .expect("create");
        tx.commit().unwrap();
    }

    // Verify node exists before delete.
    {
        let rx = db.begin_read().unwrap();
        let vals = rx.get_node(node_id, &[0u32]).unwrap();
        assert_eq!(vals[0].1, Value::Int64(5));
    }

    // DELETE via Cypher.
    db.execute("MATCH (n:Person) DELETE n")
        .expect("MATCH DELETE must succeed");

    // col_0 must be tombstoned (u64::MAX = -1 as Int64).
    {
        let store = sparrowdb_storage::node_store::NodeStore::open(dir.path()).unwrap();
        let vals = store
            .get_node_raw(node_id, &[0u32])
            .expect("read tombstone");
        assert_eq!(
            vals.first().map(|&(_, v)| v),
            Some(u64::MAX),
            "deleted node col_0 must be u64::MAX tombstone"
        );
    }

    drop(dir);
}
