//! Extended unit tests for the Phase 1 chunked vectorized pipeline (SPA-299).
//!
//! Covers edge cases not exercised by the main `pipeline_tests.rs`:
//!
//! - T1: `GetNeighbors` with real delta records (no CSR edges, delta-only).
//! - T3: `NullBitmap` word-boundary correctness at exactly 64 slots.
//! - T4: `Filter` wrapping a zero-row scan returns `Ok(None)`.
//! - T5: `ScanByLabel::new(CHUNK_CAPACITY)` emits exactly one full chunk.
//! - T6: `GetNeighbors` with a `Filter` upstream returns only the filtered
//!   node's neighbors.
//!
//! T2 (`data_chunk_successive_filters_narrow_sel`) already exists in
//! `pipeline_tests.rs` and is not duplicated here.

use sparrowdb_common::NodeId;
use sparrowdb_execution::chunk::{NullBitmap, CHUNK_CAPACITY};
use sparrowdb_execution::pipeline::{Filter, GetNeighbors, PipelineOperator, ScanByLabel};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{DeltaRecord, RelTableId};

// ── T1: GetNeighbors with delta-only records ───────────────────────────────────
//
// Build a CSR with zero edges (empty graph).  Inject a delta record for the
// edge slot-0 → slot-1 with src_label_id = 1.  Verify that `GetNeighbors`
// surfaces the pair (0, 1) purely from the delta index.

#[test]
fn get_neighbors_delta_only_edge_appears_in_output() {
    // src NodeId: label_id=1 in high 32 bits, slot=0 in low 32 bits.
    let src_node_id = NodeId(1u64 << 32);
    // dst NodeId: label_id=1 in high 32 bits, slot=1 in low 32 bits.
    let dst_node_id = NodeId((1u64 << 32) | 1u64);

    let delta_records = vec![DeltaRecord {
        src: src_node_id,
        dst: dst_node_id,
        rel_id: RelTableId(0),
    }];

    // CSR with 2 nodes, no persisted edges.
    let csr = CsrForward::build(2, &[]);

    // Scan slot 0 only (the source node).
    let scan = ScanByLabel::from_slots(vec![0u64]);

    // src_label_id = 1 so the delta-index lookup uses key (1, 0).
    let mut gn = GetNeighbors::new(scan, csr, &delta_records, 1, 1);

    let mut pairs: Vec<(u64, u64)> = Vec::new();
    while let Some(chunk) = gn.next_chunk().unwrap() {
        let src_col = chunk.column(0);
        let dst_col = chunk.column(1);
        for row in chunk.live_rows() {
            pairs.push((src_col.data[row], dst_col.data[row]));
        }
    }

    assert_eq!(
        pairs,
        vec![(0u64, 1u64)],
        "delta record (0→1) must appear in GetNeighbors output"
    );
}

// ── T3: NullBitmap word-boundary correctness (64 slots) ──────────────────────
//
// A `NullBitmap` with exactly 64 slots spans exactly one `u64` word.
// Mark slots 0..63 as null and leave slot 63 as non-null.
// Verify the word boundary does not bleed: is_null(62) == true, is_null(63) == false.

#[test]
fn null_bitmap_word_boundary_64_slots() {
    let mut bitmap = NullBitmap::with_len(64);

    // Mark every slot except the last one as null.
    for i in 0..63 {
        bitmap.set_null(i);
    }

    assert!(
        bitmap.is_null(62),
        "slot 62 should be null (set_null was called)"
    );
    assert!(
        !bitmap.is_null(63),
        "slot 63 should NOT be null (set_null was not called)"
    );

    // Double-check slot 0 as well.
    assert!(bitmap.is_null(0), "slot 0 should be null");
}

// ── T4: Empty DataChunk through Filter returns None ────────────────────────────
//
// A `ScanByLabel::from_slots(vec![])` (empty slot list) produces no chunks.
// A `Filter` wrapping it must propagate exhaustion and return `Ok(None)` on
// the first call.

#[test]
fn filter_on_empty_scan_returns_none() {
    let scan = ScanByLabel::from_slots(vec![]);
    // Predicate that always accepts — the scan itself is the source of emptiness.
    let mut filter = Filter::new(scan, |_chunk, _i| true);

    let result = filter.next_chunk().unwrap();
    assert!(
        result.is_none(),
        "Filter on an empty scan must return Ok(None)"
    );
}

// ── T5: ScanByLabel at hwm = CHUNK_CAPACITY emits exactly one full chunk ───────
//
// When the high-water mark equals `CHUNK_CAPACITY`, the first call to
// `next_chunk()` must return a chunk of `live_len() == CHUNK_CAPACITY` and
// the second call must return `None`.

#[test]
fn scan_by_label_at_hwm_equals_chunk_capacity() {
    let mut scan = ScanByLabel::new(CHUNK_CAPACITY as u64);

    let chunk = scan
        .next_chunk()
        .unwrap()
        .expect("first chunk must be Some");
    assert_eq!(
        chunk.live_len(),
        CHUNK_CAPACITY,
        "first chunk must be exactly CHUNK_CAPACITY rows"
    );

    let next = scan.next_chunk().unwrap();
    assert!(
        next.is_none(),
        "after the single full chunk, ScanByLabel must return None"
    );
}

// ── T6: GetNeighbors with pre-filtered input ──────────────────────────────────
//
// Topology: 3 source nodes, edges (0→1), (0→2), (1→3).
// Upstream Filter keeps only slot 0.
// GetNeighbors must emit (0,1) and (0,2), and NOT (1,3).

#[test]
fn get_neighbors_with_pre_filtered_input_yields_only_filtered_neighbors() {
    let edges: Vec<(u64, u64)> = vec![(0, 1), (0, 2), (1, 3)];
    // 4 nodes (0..=3) so the CSR is large enough.
    let csr = CsrForward::build(4, &edges);

    // Scan slots 0, 1, 2.
    let scan = ScanByLabel::from_slots(vec![0u64, 1, 2]);

    // Filter: keep only slot 0 (column 0 value == 0).
    let filtered = Filter::new(scan, |chunk, i| chunk.column(0).data[i] == 0);

    // No delta records; label_id = 0.
    let mut gn = GetNeighbors::new(filtered, csr, &[], 0, 2);

    let mut pairs: Vec<(u64, u64)> = Vec::new();
    while let Some(chunk) = gn.next_chunk().unwrap() {
        let src_col = chunk.column(0);
        let dst_col = chunk.column(1);
        for row in chunk.live_rows() {
            pairs.push((src_col.data[row], dst_col.data[row]));
        }
    }

    pairs.sort();

    // Only slot-0's neighbors: (0,1) and (0,2). NOT (1,3).
    assert_eq!(
        pairs,
        vec![(0u64, 1u64), (0u64, 2u64)],
        "only slot-0's neighbors must appear; (1,3) must be absent"
    );
}
