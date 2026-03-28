//! Integration tests for the Phase 1 chunked vectorized pipeline (#299).
//!
//! Tests verify that:
//! 1. `ScanByLabel` + `Filter` via the chunked pipeline returns the same results
//!    as the row-at-a-time engine for simple label scans.
//! 2. `GetNeighbors` via the chunked pipeline matches existing hop results.
//! 3. The `use_chunked_pipeline` flag is truly opt-in (default = false).

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::chunk::{ColumnVector, DataChunk, CHUNK_CAPACITY, COL_ID_SLOT};
use sparrowdb_execution::pipeline::{Filter, GetNeighbors, PipelineOperator, ScanByLabel};
use sparrowdb_execution::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Build a minimal test database with a `Person` label and three nodes.
/// Returns `(store, catalog, label_id)`.
///
/// Nodes: Alice(age=30, score=100), Bob(age=25, score=200), Carol(age=40, score=300).
/// Col IDs: age = col_id_of("age"), score = col_id_of("score").
fn setup_person_db(dir: &std::path::Path) -> (NodeStore, Catalog, u32) {
    let mut store = NodeStore::open(dir).expect("node store");
    let mut cat = Catalog::open(dir).expect("catalog");
    let label_id = cat.create_label("Person").expect("Person") as u32;

    // Use simple integer col IDs that col_id_of("age") and col_id_of("score")
    // would produce. In practice col_id_of uses djb2 — but for integration tests
    // we test the full engine path which resolves names correctly.
    let age_col = sparrowdb_common::col_id_of("age");
    let score_col = sparrowdb_common::col_id_of("score");

    store
        .create_node(
            label_id,
            &[
                (age_col, StoreValue::Int64(30)),
                (score_col, StoreValue::Int64(100)),
            ],
        )
        .expect("Alice");
    store
        .create_node(
            label_id,
            &[
                (age_col, StoreValue::Int64(25)),
                (score_col, StoreValue::Int64(200)),
            ],
        )
        .expect("Bob");
    store
        .create_node(
            label_id,
            &[
                (age_col, StoreValue::Int64(40)),
                (score_col, StoreValue::Int64(300)),
            ],
        )
        .expect("Carol");

    (store, cat, label_id)
}

fn make_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("store");
    let cat = Catalog::open(dir).expect("catalog");
    let csrs = std::collections::HashMap::new();
    Engine::new(store, cat, csrs, dir)
}

fn make_engine_pipeline(dir: &std::path::Path) -> Engine {
    make_engine(dir).with_chunked_pipeline()
}

// ── Unit: DataChunk filter ─────────────────────────────────────────────────────

#[test]
fn data_chunk_filter_reduces_sel_vector() {
    let data: Vec<u64> = (0u64..10).collect();
    let col = ColumnVector::from_data(COL_ID_SLOT, data);
    let mut chunk = DataChunk::from_columns(vec![col]);

    assert_eq!(chunk.live_len(), 10);
    assert!(chunk.sel().is_none());

    // Pre-compute the keep mask to avoid simultaneous &chunk / &mut chunk borrows.
    let keep: Vec<bool> = (0..chunk.len())
        .map(|i| chunk.column(0).data[i].is_multiple_of(2))
        .collect();
    chunk.filter_sel(|i| keep[i]);

    assert_eq!(chunk.live_len(), 5);
    let sel = chunk.sel().expect("sel should be Some");
    assert_eq!(sel, &[0u32, 2, 4, 6, 8]);
}

#[test]
fn data_chunk_successive_filters_narrow_sel() {
    let data: Vec<u64> = (0u64..20).collect();
    let col = ColumnVector::from_data(COL_ID_SLOT, data);
    let mut chunk = DataChunk::from_columns(vec![col]);

    // First filter: keep even rows. Pre-compute mask.
    let keep1: Vec<bool> = (0..chunk.len())
        .map(|i| chunk.column(0).data[i].is_multiple_of(2))
        .collect();
    chunk.filter_sel(|i| keep1[i]);
    assert_eq!(chunk.live_len(), 10);

    // Second filter: keep rows where value > 12.
    let keep2: Vec<bool> = (0..chunk.len())
        .map(|i| chunk.column(0).data[i] > 12)
        .collect();
    chunk.filter_sel(|i| keep2[i]);
    // Even values > 12 in 0..20: 14, 16, 18 → 3 rows.
    assert_eq!(chunk.live_len(), 3);
    let live: Vec<usize> = chunk.live_rows().collect();
    assert_eq!(live, vec![14, 16, 18]);
}

// ── Integration: ScanByLabel + Filter = same results as row-at-a-time ─────────

#[test]
fn scan_by_label_returns_same_count_as_row_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (_, _, _) = setup_person_db(dir.path());

    // Row-at-a-time engine.
    let row_result = {
        let mut eng = make_engine(dir.path());
        eng.execute("MATCH (p:Person) RETURN p.age").unwrap()
    };

    // Chunked pipeline engine.
    let chunk_result = {
        let mut eng = make_engine_pipeline(dir.path());
        eng.execute("MATCH (p:Person) RETURN p.age").unwrap()
    };

    assert_eq!(
        row_result.rows.len(),
        chunk_result.rows.len(),
        "chunked pipeline must return same row count as row-at-a-time"
    );
}

#[test]
fn scan_with_where_filter_matches_row_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (_, _, _) = setup_person_db(dir.path());

    // Row-at-a-time engine with WHERE filter.
    let row_result = {
        let mut eng = make_engine(dir.path());
        eng.execute("MATCH (p:Person) WHERE p.age > 28 RETURN p.age")
            .unwrap()
    };

    // Chunked pipeline engine.
    let chunk_result = {
        let mut eng = make_engine_pipeline(dir.path());
        eng.execute("MATCH (p:Person) WHERE p.age > 28 RETURN p.age")
            .unwrap()
    };

    // Both engines should agree on how many rows pass the filter.
    assert_eq!(
        row_result.rows.len(),
        chunk_result.rows.len(),
        "WHERE filter: chunked and row-at-a-time must return same count"
    );

    // Sort both result sets for deterministic comparison.
    let mut row_ages: Vec<i64> = row_result
        .rows
        .iter()
        .filter_map(|row| {
            row.first().and_then(|v| match v {
                sparrowdb_execution::Value::Int64(n) => Some(*n),
                _ => None,
            })
        })
        .collect();
    let mut chunk_ages: Vec<i64> = chunk_result
        .rows
        .iter()
        .filter_map(|row| {
            row.first().and_then(|v| match v {
                sparrowdb_execution::Value::Int64(n) => Some(*n),
                _ => None,
            })
        })
        .collect();
    row_ages.sort();
    chunk_ages.sort();
    assert_eq!(row_ages, chunk_ages, "age values must match");
}

#[test]
fn chunked_pipeline_default_is_off() {
    // The flag must default to false. Without `with_chunked_pipeline()`,
    // the engine should use the row-at-a-time path — verified by confirming
    // the use_chunked_pipeline field is false.
    let dir = tempfile::tempdir().unwrap();
    let (_, _, _) = setup_person_db(dir.path());
    let eng = make_engine(dir.path());
    assert!(
        !eng.use_chunked_pipeline,
        "use_chunked_pipeline must default to false"
    );
}

// ── Integration: GetNeighbors via chunked pipeline ────────────────────────────

#[test]
fn get_neighbors_chunked_matches_hop_results() {
    // Build a graph: nodes 0, 1, 2, 3; edges 0→1, 0→2, 1→3.
    let edges: Vec<(u64, u64)> = vec![(0, 1), (0, 2), (1, 3)];
    let csr = CsrForward::build(4, &edges);

    // Use GetNeighbors pipeline: scan all 4 slots, expand neighbors.
    let scan = ScanByLabel::from_slots(vec![0u64, 1, 2, 3]);
    let mut gn = GetNeighbors::new(scan, csr, &[], 0, 2);

    // Collect all (src, dst) pairs.
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    while let Some(chunk) = gn.next_chunk().unwrap() {
        let src_col = chunk.column(0);
        let dst_col = chunk.column(1);
        for row in chunk.live_rows() {
            pairs.push((src_col.data[row], dst_col.data[row]));
        }
    }

    // Expected: (0,1), (0,2), (1,3).
    pairs.sort();
    assert_eq!(pairs, vec![(0u64, 1u64), (0, 2), (1, 3)]);
}

#[test]
fn get_neighbors_large_expansion_is_complete() {
    // High-degree star node: 0 → [1..N].
    let n: u64 = (CHUNK_CAPACITY as u64) * 2 + 17;
    let edges: Vec<(u64, u64)> = (1..=n).map(|d| (0u64, d)).collect();
    let csr = CsrForward::build(n + 1, &edges);

    let scan = ScanByLabel::from_slots(vec![0u64]);
    let mut gn = GetNeighbors::new(scan, csr, &[], 0, 10);

    let mut total_pairs: usize = 0;
    while let Some(chunk) = gn.next_chunk().unwrap() {
        total_pairs += chunk.live_len();
    }
    assert_eq!(total_pairs, n as usize, "must emit all dst slots");
}

// ── ScanByLabel pipeline unit tests ───────────────────────────────────────────

#[test]
fn pipeline_scan_by_label_yields_all_slots() {
    let mut scan = ScanByLabel::new(5);
    let chunk = scan.next_chunk().unwrap().expect("first chunk");
    assert_eq!(chunk.live_len(), 5);
    assert_eq!(chunk.column(0).data, vec![0u64, 1, 2, 3, 4]);
    assert!(scan.next_chunk().unwrap().is_none());
}

#[test]
fn pipeline_filter_reduces_live_count() {
    let scan = ScanByLabel::new(10);
    // Keep only odd slots.
    let mut filter = Filter::new(scan, |chunk, i| chunk.column(0).data[i] % 2 == 1);
    let chunk = filter.next_chunk().unwrap().expect("chunk");
    assert_eq!(chunk.live_len(), 5);
    let live: Vec<usize> = chunk.live_rows().collect();
    assert_eq!(live, vec![1, 3, 5, 7, 9]);
}
