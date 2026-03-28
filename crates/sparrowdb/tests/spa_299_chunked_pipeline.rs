//! Integration tests for the Phase 1 chunked vectorized pipeline (SPA-299, PR #335).
//!
//! Each test runs the same Cypher query through both execution paths and
//! asserts they produce IDENTICAL results:
//!
//! - Row-at-a-time (default): `GraphDb::execute()`
//! - Chunked vectorized pipeline: raw `Engine::new(...).with_chunked_pipeline()`
//!
//! # Phase 1 scope
//!
//! The chunked pipeline activates for queries that satisfy ALL of:
//! - Single node pattern with no relationship hops
//! - At least one label specified
//! - No aggregation in RETURN
//! - No ORDER BY / SKIP / LIMIT
//!
//! All other shapes fall back to row-at-a-time. Tests for those shapes still
//! validate result equivalence (both paths produce identical output via the
//! same row-at-a-time code).
//!
//! # Chunk boundary sizes
//!
//! `CHUNK_CAPACITY = 2048`. Interesting boundary cases:
//! - 2048 nodes  → exactly one full chunk
//! - 2049 nodes  → spills to a second chunk
//! - 4096 nodes  → exactly two full chunks

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::{Engine, Value};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;
use std::collections::HashMap;

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Open a fresh database at a temporary directory.
fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Build a read-only `Engine` that uses the chunked pipeline, reading from the
/// same on-disk snapshot that was written by `GraphDb::execute` mutations.
///
/// Uses `Engine::new` (no shared caches) so it always reads from disk — the
/// same data that the row-at-a-time engine will see via `GraphDb::execute`.
fn chunked_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("NodeStore::open");
    let cat = Catalog::open(dir).expect("Catalog::open");
    // Load all per-rel-type CSR files (needed for hop queries, no-op for node-only).
    let csrs = open_csr_map(dir);
    Engine::new(store, cat, csrs, dir).with_chunked_pipeline()
}

/// Build a read-only `Engine` WITHOUT the chunked pipeline.
/// Used to produce the reference result when `GraphDb::execute` cannot be
/// used directly (e.g., when we need to compare raw Engine outputs).
fn row_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("NodeStore::open");
    let cat = Catalog::open(dir).expect("Catalog::open");
    let csrs = open_csr_map(dir);
    Engine::new(store, cat, csrs, dir)
}

/// Replicate the CSR-loading logic from `sparrowdb::lib::open_csr_map`.
fn open_csr_map(path: &std::path::Path) -> HashMap<u32, CsrForward> {
    use sparrowdb_storage::edge_store::EdgeStore;
    use sparrowdb_storage::edge_store::RelTableId;

    let catalog = match Catalog::open(path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };
    let mut map = HashMap::new();
    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();
    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }
    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    map
}

/// Sort rows of scalar values for order-insensitive comparison.
/// Each row is sorted by its string representation for a stable key.
fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by(|a, b| {
        let ka: Vec<String> = a.iter().map(|v| format!("{v:?}")).collect();
        let kb: Vec<String> = b.iter().map(|v| format!("{v:?}")).collect();
        ka.cmp(&kb)
    });
    rows
}

/// Run `cypher` on both engines and assert the result sets are identical
/// (same columns, same rows — order-insensitive).
///
/// Returns the sorted rows from the row-at-a-time engine so callers can
/// make additional assertions on values if needed.
fn assert_engines_agree(
    dir: &std::path::Path,
    cypher: &str,
) -> (Vec<String>, Vec<Vec<Value>>) {
    let row_result = row_engine(dir).execute(cypher).unwrap_or_else(|e| {
        panic!("row-at-a-time engine failed for `{cypher}`: {e}");
    });
    let chunked_result = chunked_engine(dir).execute(cypher).unwrap_or_else(|e| {
        panic!("chunked pipeline engine failed for `{cypher}`: {e}");
    });

    assert_eq!(
        row_result.columns, chunked_result.columns,
        "column names differ for `{cypher}`"
    );

    let row_rows = sort_rows(row_result.rows.clone());
    let chunked_rows = sort_rows(chunked_result.rows.clone());

    assert_eq!(
        row_rows.len(),
        chunked_rows.len(),
        "row count differs for `{cypher}`: row-at-a-time={}, chunked={}",
        row_rows.len(),
        chunked_rows.len()
    );

    assert_eq!(
        row_rows, chunked_rows,
        "row values differ for `{cypher}`"
    );

    (row_result.columns, row_rows)
}

// ── 1. Simple MATCH + RETURN — single label, no filter ────────────────────────

#[test]
fn simple_match_return_single_label() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Animal {name: 'Dog', legs: 4})").unwrap();
    db.execute("CREATE (:Animal {name: 'Bird', legs: 2})").unwrap();
    db.execute("CREATE (:Animal {name: 'Snake', legs: 0})").unwrap();

    let (_, rows) = assert_engines_agree(dir.path(), "MATCH (a:Animal) RETURN a.name, a.legs");
    assert_eq!(rows.len(), 3, "all 3 animals must be returned");
}

// ── 2a. MATCH with WHERE filter — integer equality ────────────────────────────

#[test]
fn where_integer_equality() {
    let (dir, db) = make_db();
    for i in 1..=10i64 {
        db.execute(&format!("CREATE (:Item {{id: {i}, score: {}}}) ", i * 10))
            .unwrap();
    }

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:Item) WHERE n.id = 5 RETURN n.id, n.score");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(5));
    assert_eq!(rows[0][1], Value::Int64(50));
}

// ── 2b. MATCH with WHERE filter — string equality ────────────────────────────

#[test]
fn where_string_equality() {
    let (dir, db) = make_db();
    db.execute("CREATE (:City {name: 'Paris', pop: 2000000})").unwrap();
    db.execute("CREATE (:City {name: 'Lyon', pop: 500000})").unwrap();
    db.execute("CREATE (:City {name: 'Marseille', pop: 900000})").unwrap();

    let (_, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (c:City) WHERE c.name = 'Lyon' RETURN c.name, c.pop",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String("Lyon".to_string()));
    assert_eq!(rows[0][1], Value::Int64(500000));
}

// ── 2c. MATCH with WHERE filter — property IS NULL ────────────────────────────

#[test]
fn where_property_is_null() {
    let (dir, db) = make_db();
    // Alice has an email; Bob does not (property missing = null).
    db.execute("CREATE (:Member {name: 'Alice', email: 'alice@example.com'})").unwrap();
    db.execute("CREATE (:Member {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Member {name: 'Carol', email: 'carol@example.com'})").unwrap();

    let (_, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (m:Member) WHERE m.email IS NULL RETURN m.name",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String("Bob".to_string()));
}

// ── 3. MATCH with LIMIT — result count bounded ────────────────────────────────

#[test]
fn match_with_limit_count_bounded() {
    let (dir, db) = make_db();
    for i in 1..=20i64 {
        db.execute(&format!("CREATE (:Log {{seq: {i}}})")).unwrap();
    }

    // LIMIT falls back to row-at-a-time in Phase 1 — both engines must agree.
    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (l:Log) RETURN l.seq LIMIT 5");
    assert_eq!(rows.len(), 5, "LIMIT 5 should return exactly 5 rows");
}

// ── 4. MATCH with ORDER BY — verify sorted order matches ──────────────────────

#[test]
fn match_with_order_by_sorted_order_matches() {
    let (dir, db) = make_db();
    for i in [7i64, 3, 9, 1, 5].iter() {
        db.execute(&format!("CREATE (:Num {{val: {i}}})")).unwrap();
    }

    // ORDER BY falls back to row-at-a-time — both must agree on sorted order.
    let row_result = row_engine(dir.path())
        .execute("MATCH (n:Num) RETURN n.val ORDER BY n.val ASC")
        .expect("row engine ORDER BY");
    let chunked_result = chunked_engine(dir.path())
        .execute("MATCH (n:Num) RETURN n.val ORDER BY n.val ASC")
        .expect("chunked engine ORDER BY");

    // For ORDER BY we compare in-order (not sorted), because the order IS the result.
    assert_eq!(
        row_result.rows, chunked_result.rows,
        "ORDER BY results must be identical (same order)"
    );
    // Sanity: ascending order.
    assert_eq!(row_result.rows[0][0], Value::Int64(1));
    assert_eq!(row_result.rows[4][0], Value::Int64(9));
}

// ── 5. Large graph — 10K nodes, full scan ─────────────────────────────────────

#[test]
fn large_graph_10k_nodes_full_scan() {
    let (dir, db) = make_db();
    // Seed 10 000 nodes. Use batch_write for speed.
    let batch: Vec<String> = (1..=10_000i64)
        .map(|i| format!("CREATE (:BigNode {{n: {i}}})"))
        .collect();
    db.execute_batch(&batch.iter().map(String::as_str).collect::<Vec<_>>())
        .expect("batch seed");

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:BigNode) RETURN n.n");
    assert_eq!(rows.len(), 10_000, "all 10 000 nodes must be scanned");
}

// ── 6a. Chunk boundary — exactly 2048 nodes (one full chunk) ──────────────────

#[test]
fn chunk_boundary_exactly_2048_nodes() {
    let (dir, db) = make_db();
    let batch: Vec<String> = (1..=2048i64)
        .map(|i| format!("CREATE (:Chunk2048 {{id: {i}}})"))
        .collect();
    db.execute_batch(&batch.iter().map(String::as_str).collect::<Vec<_>>())
        .expect("seed 2048");

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:Chunk2048) RETURN n.id");
    assert_eq!(rows.len(), 2048);
}

// ── 6b. Chunk boundary — 2049 nodes (spills to 2nd chunk) ────────────────────

#[test]
fn chunk_boundary_2049_nodes_spills_to_second_chunk() {
    let (dir, db) = make_db();
    let batch: Vec<String> = (1..=2049i64)
        .map(|i| format!("CREATE (:Chunk2049 {{id: {i}}})"))
        .collect();
    db.execute_batch(&batch.iter().map(String::as_str).collect::<Vec<_>>())
        .expect("seed 2049");

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:Chunk2049) RETURN n.id");
    assert_eq!(rows.len(), 2049);
}

// ── 6c. Chunk boundary — exactly 4096 nodes (two full chunks) ────────────────

#[test]
fn chunk_boundary_exactly_4096_nodes_two_full_chunks() {
    let (dir, db) = make_db();
    let batch: Vec<String> = (1..=4096i64)
        .map(|i| format!("CREATE (:Chunk4096 {{id: {i}}})"))
        .collect();
    db.execute_batch(&batch.iter().map(String::as_str).collect::<Vec<_>>())
        .expect("seed 4096");

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:Chunk4096) RETURN n.id");
    assert_eq!(rows.len(), 4096);
}

// ── 7. Empty result — WHERE clause matches nothing ────────────────────────────

#[test]
fn empty_result_where_matches_nothing() {
    let (dir, db) = make_db();
    for i in 1..=5i64 {
        db.execute(&format!("CREATE (:Widget {{code: {i}}})")).unwrap();
    }

    let (_, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (w:Widget) WHERE w.code = 999 RETURN w.code",
    );
    assert_eq!(rows.len(), 0, "WHERE matching nothing must return 0 rows");
}

// ── 8. Multi-property filter — WHERE n.a = x AND n.b = y ─────────────────────

#[test]
fn multi_property_filter_where_and() {
    let (dir, db) = make_db();
    // Create nodes with varying combinations of (colour, size).
    db.execute("CREATE (:Box {colour: 'red', size: 1})").unwrap();
    db.execute("CREATE (:Box {colour: 'red', size: 2})").unwrap();
    db.execute("CREATE (:Box {colour: 'blue', size: 1})").unwrap();
    db.execute("CREATE (:Box {colour: 'blue', size: 2})").unwrap();

    let (_, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (b:Box) WHERE b.colour = 'red' AND b.size = 2 RETURN b.colour, b.size",
    );
    assert_eq!(rows.len(), 1, "only one box matches red + size 2");
    assert_eq!(rows[0][0], Value::String("red".to_string()));
    assert_eq!(rows[0][1], Value::Int64(2));
}

// ── 9. MATCH + 1-hop — (a)-[:R]->(b) ─────────────────────────────────────────
//
// Phase 1 does NOT activate the chunked pipeline for hop queries — both
// engines fall back to row-at-a-time. The test still validates that the
// two execution paths agree on the result.
//
// Phase 2 will extend the pipeline to cover GetNeighbors.

#[test]
fn one_hop_match_engines_agree() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // Both engines should return the same two (src, dst) name pairs.
    assert_engines_agree(
        dir.path(),
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    );
}

// ── 10. Integer zero values — regression test for #333 / #325 ────────────────
//
// `PropertyIndex` previously treated `Int64(0)` as NULL because the null
// bitmap was not stored.  Fix landed in #333 (closes #325).
//
// Verify that nodes with `id: 0` (and properties whose value IS zero) are
// correctly returned by BOTH the row-at-a-time and chunked pipeline engines.

#[test]
fn integer_zero_values_found_regression_325() {
    let (dir, db) = make_db();
    // Node with id = 0 and score = 0 must NOT be treated as NULL.
    db.execute("CREATE (:Score {id: 0, value: 0})").unwrap();
    db.execute("CREATE (:Score {id: 1, value: 100})").unwrap();
    db.execute("CREATE (:Score {id: 2, value: 0})").unwrap();

    // All three nodes must be returned.
    let (_, rows) = assert_engines_agree(dir.path(), "MATCH (s:Score) RETURN s.id, s.value");
    assert_eq!(rows.len(), 3, "all 3 Score nodes must be returned (id=0 must not be dropped)");

    // The node with id = 0 must appear.
    let has_zero_id = rows.iter().any(|r| r[0] == Value::Int64(0));
    assert!(has_zero_id, "node with id=0 must be present in results (#333 regression)");

    // Two nodes have value = 0; both must appear.
    let zero_value_count = rows.iter().filter(|r| r[1] == Value::Int64(0)).count();
    assert_eq!(
        zero_value_count, 2,
        "both nodes with value=0 must be returned (#333 regression)"
    );
}

#[test]
fn integer_zero_exact_filter_returns_correct_rows_regression_325() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Counter {n: 0})").unwrap();
    db.execute("CREATE (:Counter {n: 1})").unwrap();
    db.execute("CREATE (:Counter {n: 0})").unwrap();
    db.execute("CREATE (:Counter {n: 2})").unwrap();

    // Filter for n = 0; must return the 2 nodes with value zero, not 0 nodes.
    let (_, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (c:Counter) WHERE c.n = 0 RETURN c.n",
    );
    assert_eq!(
        rows.len(),
        2,
        "WHERE n = 0 must match exactly 2 nodes (#333 regression)"
    );
    for row in &rows {
        assert_eq!(row[0], Value::Int64(0));
    }
}

// ── Additional: mixed-engine scan with inline prop filter on node pattern ─────

#[test]
fn inline_prop_filter_on_node_pattern() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Product {sku: 'ABC', qty: 10})").unwrap();
    db.execute("CREATE (:Product {sku: 'DEF', qty: 5})").unwrap();
    db.execute("CREATE (:Product {sku: 'ABC', qty: 20})").unwrap();

    // Inline prop filter `{sku: 'ABC'}` in the node pattern.
    let (_, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (p:Product {sku: 'ABC'}) RETURN p.sku, p.qty",
    );
    assert_eq!(rows.len(), 2, "two products with sku=ABC");
    for row in &rows {
        assert_eq!(row[0], Value::String("ABC".to_string()));
    }
}

// ── Additional: empty label (no nodes) returns empty result ───────────────────

#[test]
fn empty_database_returns_empty_result() {
    let (dir, db) = make_db();
    // No nodes of label Ghost exist.
    let _ = db; // keep the db alive so the tempdir doesn't vanish

    let (_, rows) = assert_engines_agree(dir.path(), "MATCH (g:Ghost) RETURN g.name");
    assert_eq!(rows.len(), 0, "no Ghost nodes → 0 rows");
}

// ── Additional: unknown label returns empty result ────────────────────────────

#[test]
fn unknown_label_returns_empty_result() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Known {x: 1})").unwrap();
    let _ = db;

    let (_, rows) = assert_engines_agree(dir.path(), "MATCH (n:NonExistent) RETURN n.x");
    assert_eq!(rows.len(), 0, "unknown label must return 0 rows");
}

// ── I1: Scan equivalence — 20 Person nodes, RETURN n.name ─────────────────────

#[test]
fn i1_scan_equivalence_20_person_nodes() {
    let (dir, db) = make_db();
    for i in 1..=20i64 {
        db.execute(&format!("CREATE (:Person {{name: 'Person{i}'}})"))
            .unwrap();
    }

    let (_, rows) = assert_engines_agree(dir.path(), "MATCH (n:Person) RETURN n.name");
    assert_eq!(rows.len(), 20, "all 20 Person nodes must be returned");
}

// ── I2: Inline prop filter equivalence — 5 Item nodes, val: 3 ─────────────────

#[test]
fn i2_inline_prop_filter_equivalence() {
    let (dir, db) = make_db();
    for i in 1..=5i64 {
        db.execute(&format!("CREATE (:Item {{val: {i}}})")).unwrap();
    }

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:Item {val: 3}) RETURN n.val");
    assert_eq!(rows.len(), 1, "exactly one Item with val=3");
    assert_eq!(rows[0][0], Value::Int64(3));
}

// ── I3: WHERE clause equivalence — 10 Num nodes, n > 5 ───────────────────────

#[test]
fn i3_where_clause_equivalence() {
    let (dir, db) = make_db();
    for i in 0..10i64 {
        db.execute(&format!("CREATE (:Num {{n: {i}}})")).unwrap();
    }

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (x:Num) WHERE x.n > 5 RETURN x.n");
    // n > 5 in range 0..9: values 6, 7, 8, 9 → 4 rows.
    assert_eq!(rows.len(), 4, "WHERE n > 5 must return 4 rows (6,7,8,9)");
    for row in &rows {
        if let Value::Int64(v) = &row[0] {
            assert!(*v > 5, "all returned values must satisfy n > 5, got {v}");
        }
    }
}

// ── I4: Empty label returns zero rows ────────────────────────────────────────

#[test]
fn i4_empty_label_returns_zero_rows() {
    let (dir, db) = make_db();
    let _ = db; // no nodes created for label NoSuchLabel

    let (_, rows) =
        assert_engines_agree(dir.path(), "MATCH (n:NoSuchLabel) RETURN n.name");
    assert_eq!(rows.len(), 0, "no nodes of label NoSuchLabel → 0 rows");
}

// ── I5: COUNT(*) equivalence — 7 Widget nodes ────────────────────────────────

#[test]
fn i5_count_star_equivalence_7_widgets() {
    let (dir, db) = make_db();
    for i in 1..=7i64 {
        db.execute(&format!("CREATE (:Widget {{id: {i}}})")).unwrap();
    }

    let (_, rows) = assert_engines_agree(dir.path(), "MATCH (n:Widget) RETURN COUNT(*)");
    assert_eq!(rows.len(), 1, "COUNT(*) returns exactly one row");
    assert_eq!(
        rows[0][0],
        Value::Int64(7),
        "COUNT(*) over 7 Widget nodes must equal 7"
    );
}
