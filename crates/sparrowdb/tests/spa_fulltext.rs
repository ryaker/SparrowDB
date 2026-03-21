//! SPA-170 acceptance tests — CALL procedure dispatch + full-text search.
//!
//! Covers:
//!   1. Create a fulltext index, insert nodes, CALL search returns them.
//!   2. Multi-word query matches documents containing any term (OR semantics).
//!   3. CALL YIELD node variable is usable in RETURN.
//!   4. CALL unknown.proc() returns Err (not a panic).

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::Value;
use sparrowdb_storage::node_store::Value as StoreValue;

/// Open a fresh database in a temp directory.
fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: create index, insert nodes via WriteTx, CALL returns matching nodes.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn create_index_and_search() {
    let (_dir, db) = make_db();

    // Create the full-text index.
    db.create_fulltext_index("searchIndex")
        .expect("create_fulltext_index");

    // Insert two Fact nodes and index their content.
    let node_id_a = {
        let mut tx = db.begin_write().expect("begin_write");
        let label_id = tx.create_label("Fact").expect("create_label") as u32;
        let text = "machine learning fundamentals";
        // Store the text as a Bytes property (col_0).
        let node_id = tx
            .create_node(
                label_id,
                &[(0, StoreValue::Bytes(text.as_bytes().to_vec()))],
            )
            .expect("create_node");
        tx.add_to_fulltext_index("searchIndex", node_id, text)
            .expect("index node A");
        tx.commit().expect("commit");
        node_id
    };

    let node_id_b = {
        let mut tx = db.begin_write().expect("begin_write");
        let catalog = sparrowdb_catalog::catalog::Catalog::open(_dir.path()).unwrap();
        let label_id = catalog.get_label("Fact").unwrap().unwrap() as u32;
        let text = "deep learning neural networks";
        let node_id = tx
            .create_node(
                label_id,
                &[(0, StoreValue::Bytes(text.as_bytes().to_vec()))],
            )
            .expect("create_node");
        tx.add_to_fulltext_index("searchIndex", node_id, text)
            .expect("index node B");
        tx.commit().expect("commit");
        node_id
    };

    // CALL the fulltext search.
    let result = db
        .execute("CALL db.index.fulltext.queryNodes('searchIndex', 'learning') YIELD node")
        .expect("CALL must succeed");

    assert_eq!(
        result.columns,
        vec!["node"],
        "yield column should be 'node'"
    );
    assert_eq!(result.rows.len(), 2, "both nodes should match 'learning'");

    // Verify the NodeRef values match the inserted nodes.
    let returned_ids: Vec<u64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::NodeRef(nid) => nid.0,
            other => panic!("expected NodeRef, got {other:?}"),
        })
        .collect();

    assert!(
        returned_ids.contains(&node_id_a.0),
        "node A should be in results"
    );
    assert!(
        returned_ids.contains(&node_id_b.0),
        "node B should be in results"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: multi-word query returns union of matching nodes (OR semantics).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn search_partial_match() {
    let (_dir, db) = make_db();

    db.create_fulltext_index("kmsIndex")
        .expect("create_fulltext_index");

    // Node A has unique term "economics".
    // Node B has unique term "biology".
    // A query for "economics biology" should return both (OR).
    let node_a = {
        let mut tx = db.begin_write().expect("begin_write");
        let label_id = tx.create_label("Article").expect("create_label") as u32;
        let node_id = tx
            .create_node(label_id, &[(0, StoreValue::Int64(1))])
            .expect("create_node A");
        tx.add_to_fulltext_index("kmsIndex", node_id, "economics supply demand")
            .expect("index A");
        tx.commit().expect("commit");
        node_id
    };

    let node_b = {
        let mut tx = db.begin_write().expect("begin_write");
        let catalog = sparrowdb_catalog::catalog::Catalog::open(_dir.path()).unwrap();
        let label_id = catalog.get_label("Article").unwrap().unwrap() as u32;
        let node_id = tx
            .create_node(label_id, &[(0, StoreValue::Int64(2))])
            .expect("create_node B");
        tx.add_to_fulltext_index("kmsIndex", node_id, "biology cells organism")
            .expect("index B");
        tx.commit().expect("commit");
        node_id
    };

    let result = db
        .execute("CALL db.index.fulltext.queryNodes('kmsIndex', 'economics biology') YIELD node")
        .expect("CALL");

    assert_eq!(
        result.rows.len(),
        2,
        "OR semantics: both nodes should match"
    );

    let ids: Vec<u64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::NodeRef(n) => n.0,
            other => panic!("expected NodeRef, got {other:?}"),
        })
        .collect();
    assert!(
        ids.contains(&node_a.0),
        "node A (economics) must be returned"
    );
    assert!(ids.contains(&node_b.0), "node B (biology) must be returned");

    // A query with only one matching term should return exactly one node.
    let result2 = db
        .execute("CALL db.index.fulltext.queryNodes('kmsIndex', 'supply') YIELD node")
        .expect("CALL supply");
    assert_eq!(result2.rows.len(), 1);
    let id2 = match &result2.rows[0][0] {
        Value::NodeRef(n) => n.0,
        other => panic!("expected NodeRef, got {other:?}"),
    };
    assert_eq!(id2, node_a.0, "only node A matches 'supply'");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: CALL YIELD node usable in RETURN (property projection).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn call_yield_node_usable_in_return() {
    let (_dir, db) = make_db();

    db.create_fulltext_index("propIndex")
        .expect("create fulltext index");

    // Create a node with a known integer col_0 value.
    let expected_val: i64 = 42;
    {
        let mut tx = db.begin_write().expect("begin_write");
        let label_id = tx.create_label("Doc").expect("create_label") as u32;
        let node_id = tx
            .create_node(label_id, &[(0, StoreValue::Int64(expected_val))])
            .expect("create_node");
        tx.add_to_fulltext_index("propIndex", node_id, "knowledge graph")
            .expect("index node");
        tx.commit().expect("commit");
    }

    // CALL with RETURN node — verifies YIELD variable passes through.
    let result = db
        .execute(
            "CALL db.index.fulltext.queryNodes('propIndex', 'knowledge') YIELD node \
             RETURN node",
        )
        .expect("CALL with RETURN node");

    assert_eq!(result.rows.len(), 1, "one node should match");
    assert!(
        matches!(&result.rows[0][0], Value::NodeRef(_)),
        "RETURN node should produce a NodeRef, got {:?}",
        &result.rows[0][0]
    );

    // CALL with RETURN node.col_0 — verifies property projection from yielded NodeRef.
    let result2 = db
        .execute(
            "CALL db.index.fulltext.queryNodes('propIndex', 'knowledge') YIELD node \
             RETURN node.col_0",
        )
        .expect("CALL with RETURN node.col_0");

    assert_eq!(result2.rows.len(), 1, "one node should match");
    assert_eq!(
        result2.rows[0][0],
        Value::Int64(expected_val),
        "RETURN node.col_0 should project the stored property value, got {:?}",
        &result2.rows[0][0]
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: CALL unknown.proc() returns Err (not a panic).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn unknown_procedure_returns_error() {
    let (_dir, db) = make_db();

    let result = db.execute("CALL unknown.procedure('arg') YIELD node");
    assert!(
        result.is_err(),
        "unknown procedure must return Err, got: {result:?}"
    );

    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("unknown procedure") || msg.contains("procedure"),
        "error should mention the procedure: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: CALL on non-existent index returns empty results (not Err).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn call_missing_index_returns_empty() {
    let (_dir, db) = make_db();

    // The index was never created — FulltextIndex::open returns an empty index.
    let result = db
        .execute("CALL db.index.fulltext.queryNodes('nonExistentIndex', 'hello') YIELD node")
        .expect("CALL on missing index should succeed with empty results");

    assert_eq!(
        result.rows.len(),
        0,
        "missing index should return zero rows"
    );
}
