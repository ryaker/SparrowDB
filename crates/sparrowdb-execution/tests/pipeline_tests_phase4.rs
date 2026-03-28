//! Unit tests for Phase 4 pipeline additions (SPA-299, #340).
//!
//! Covers:
//! - T1: `SlotIntersect` — empty right stream returns None.
//! - T2: `SlotIntersect` — empty left stream returns None.
//! - T3: `SlotIntersect` — no overlap returns None.
//! - T4: `SlotIntersect` — partial overlap yields correct sorted slots.
//! - T5: `SlotIntersect` — output is sorted ascending even for out-of-order inputs.
//! - T6: `SlotIntersect` — full overlap yields all slots.
//! - T7: `SlotIntersect` — large input spans multiple output chunks.
//! - T8: `ChunkedPlan` — Display impl shows expected plan names.
//! - T9: `ChunkedPlan` — PartialEq between same variants.
//! - T10: `SlotIntersect` — deduplication: duplicate slots in build side count once.

use sparrowdb_execution::chunk::{CHUNK_CAPACITY, COL_ID_SLOT};
use sparrowdb_execution::engine::pipeline_exec::ChunkedPlan;
use sparrowdb_execution::pipeline::{PipelineOperator, ScanByLabel, SlotIntersect};

// ── T1: empty right returns None ─────────────────────────────────────────────

#[test]
fn slot_intersect_t1_empty_right() {
    let left = ScanByLabel::from_slots(vec![1, 2, 3]);
    let right = ScanByLabel::from_slots(vec![]);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    assert!(
        isect.next_chunk().unwrap().is_none(),
        "empty right side must yield None"
    );
}

// ── T2: empty left returns None ───────────────────────────────────────────────

#[test]
fn slot_intersect_t2_empty_left() {
    let left = ScanByLabel::from_slots(vec![]);
    let right = ScanByLabel::from_slots(vec![10, 20, 30]);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    assert!(
        isect.next_chunk().unwrap().is_none(),
        "empty left side must yield None"
    );
}

// ── T3: no overlap returns None ───────────────────────────────────────────────

#[test]
fn slot_intersect_t3_no_overlap() {
    let left = ScanByLabel::from_slots(vec![1, 3, 5]);
    let right = ScanByLabel::from_slots(vec![2, 4, 6]);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    assert!(
        isect.next_chunk().unwrap().is_none(),
        "disjoint sets must yield None"
    );
}

// ── T4: partial overlap yields correct sorted slots ───────────────────────────

#[test]
fn slot_intersect_t4_partial_overlap() {
    // left = [1, 2, 3, 4], right = [2, 4, 6] → intersection = [2, 4]
    let left = ScanByLabel::from_slots(vec![1, 2, 3, 4]);
    let right = ScanByLabel::from_slots(vec![2, 4, 6]);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    let chunk = isect.next_chunk().unwrap().expect("should produce chunk");
    let col = chunk.find_column(COL_ID_SLOT).expect("slot column");
    assert_eq!(col.data, vec![2u64, 4], "intersection must be [2, 4]");
    assert!(isect.next_chunk().unwrap().is_none(), "must be exhausted");
}

// ── T5: output is sorted ascending even for out-of-order inputs ───────────────

#[test]
fn slot_intersect_t5_sorted_output() {
    // left = [5, 1, 3], right = [3, 1, 7] → intersection = [1, 3] sorted
    let left = ScanByLabel::from_slots(vec![5, 1, 3]);
    let right = ScanByLabel::from_slots(vec![3, 1, 7]);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    let chunk = isect.next_chunk().unwrap().expect("chunk");
    let col = chunk.find_column(COL_ID_SLOT).expect("slot column");
    assert_eq!(col.data, vec![1u64, 3], "output must be sorted ascending");
}

// ── T6: full overlap yields all slots ─────────────────────────────────────────

#[test]
fn slot_intersect_t6_full_overlap() {
    let slots = vec![10u64, 20, 30, 40];
    let left = ScanByLabel::from_slots(slots.clone());
    let right = ScanByLabel::from_slots(slots.clone());
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    let chunk = isect.next_chunk().unwrap().expect("chunk");
    let col = chunk.find_column(COL_ID_SLOT).expect("slot column");
    assert_eq!(col.data, slots, "full overlap must yield all slots sorted");
    assert!(isect.next_chunk().unwrap().is_none());
}

// ── T7: large input spans multiple output chunks ──────────────────────────────

#[test]
fn slot_intersect_t7_large_spans_chunks() {
    // Intersection of 0..N with 0..N should produce CHUNK_CAPACITY + extra.
    let n = CHUNK_CAPACITY + 77;
    let slots: Vec<u64> = (0..n as u64).collect();
    let left = ScanByLabel::from_slots(slots.clone());
    let right = ScanByLabel::from_slots(slots);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, usize::MAX);

    let c1 = isect.next_chunk().unwrap().expect("first chunk");
    assert_eq!(c1.live_len(), CHUNK_CAPACITY, "first chunk must be full");

    let c2 = isect.next_chunk().unwrap().expect("second chunk");
    assert_eq!(c2.live_len(), 77, "second chunk must have remainder");

    assert!(
        isect.next_chunk().unwrap().is_none(),
        "must be exhausted after two chunks"
    );
}

// ── T8: ChunkedPlan Display ───────────────────────────────────────────────────

#[test]
fn chunked_plan_t8_display() {
    assert_eq!(format!("{}", ChunkedPlan::Scan), "Scan");
    assert_eq!(format!("{}", ChunkedPlan::OneHop), "OneHop");
    assert_eq!(format!("{}", ChunkedPlan::TwoHop), "TwoHop");
    assert_eq!(
        format!("{}", ChunkedPlan::MutualNeighbors),
        "MutualNeighbors"
    );
}

// ── T9: ChunkedPlan PartialEq ─────────────────────────────────────────────────

#[test]
fn chunked_plan_t9_partial_eq() {
    assert_eq!(ChunkedPlan::Scan, ChunkedPlan::Scan);
    assert_eq!(ChunkedPlan::OneHop, ChunkedPlan::OneHop);
    assert_eq!(ChunkedPlan::TwoHop, ChunkedPlan::TwoHop);
    assert_eq!(ChunkedPlan::MutualNeighbors, ChunkedPlan::MutualNeighbors);
    assert_ne!(ChunkedPlan::Scan, ChunkedPlan::OneHop);
    assert_ne!(ChunkedPlan::TwoHop, ChunkedPlan::MutualNeighbors);
}

// ── T10: deduplication — duplicate slots in build side count once ─────────────

#[test]
fn slot_intersect_t10_build_side_dedup() {
    // If the build side has duplicates, intersect must still yield each shared
    // slot exactly once (dedup in sorted output).
    // left = [1, 2, 3], right = [2, 2, 3, 3] → intersection = [2, 3]
    let left = ScanByLabel::from_slots(vec![1, 2, 3]);
    let right = ScanByLabel::from_slots(vec![2, 2, 3, 3]);
    let mut isect = SlotIntersect::new(left, right, COL_ID_SLOT, COL_ID_SLOT, 1024);
    let chunk = isect.next_chunk().unwrap().expect("chunk");
    let col = chunk.find_column(COL_ID_SLOT).expect("slot column");
    assert_eq!(col.data, vec![2u64, 3], "each shared slot must appear once");
    assert!(isect.next_chunk().unwrap().is_none());
}
