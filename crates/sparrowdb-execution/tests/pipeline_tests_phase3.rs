//! Unit tests for Phase 3 pipeline additions (SPA-299, #339).
//!
//! Covers:
//! - T1: `FrontierScratch::new` — initial state is empty.
//! - T2: `FrontierScratch::advance` — swaps current ↔ next and clears next.
//! - T3: `FrontierScratch::advance` multiple times — reuse without re-alloc.
//! - T4: `FrontierScratch::bytes_allocated` — reflects capacity of both buffers.
//! - T5: `FrontierScratch::clear` — resets both buffers to empty.

use sparrowdb_execution::FrontierScratch;

// ── T1: FrontierScratch initial state ─────────────────────────────────────────

#[test]
fn frontier_scratch_initial_state() {
    let fs = FrontierScratch::new(64);
    assert!(
        fs.current().is_empty(),
        "current must be empty on construction"
    );
    // With capacity=64 for both buffers: 64*8 + 64*8 = 1024 bytes.
    assert_eq!(fs.bytes_allocated(), 64 * 8 + 64 * 8);
}

// ── T2: FrontierScratch::advance swaps and clears ─────────────────────────────

#[test]
fn frontier_scratch_advance_swaps() {
    let mut fs = FrontierScratch::new(8);

    // Push to current, then some entries to next.
    fs.current_mut().extend([10u64, 20, 30]);
    fs.next_mut().extend([40u64, 50]);

    fs.advance();

    // After advance: current == old next, next == empty.
    assert_eq!(fs.current(), &[40u64, 50]);
    assert!(
        fs.next_mut().is_empty(),
        "next must be cleared after advance"
    );
}

// ── T3: multiple advance cycles ───────────────────────────────────────────────

#[test]
fn frontier_scratch_multiple_advances() {
    let mut fs = FrontierScratch::new(4);

    // Level 0 → 1.
    fs.current_mut().push(1u64);
    fs.next_mut().push(2u64);
    fs.advance(); // current=[2], next=[]
    assert_eq!(fs.current(), &[2u64]);

    // Level 1 → 2.
    fs.next_mut().extend([3u64, 4]);
    fs.advance(); // current=[3,4], next=[]
    assert_eq!(fs.current(), &[3u64, 4]);
    assert!(fs.next_mut().is_empty());

    // Level 2 → 3 (nothing found).
    fs.advance(); // current=[], next=[]
    assert!(fs.current().is_empty());
}

// ── T4: bytes_allocated reflects capacity ─────────────────────────────────────

#[test]
fn frontier_scratch_bytes_allocated() {
    let mut fs = FrontierScratch::new(0);
    assert_eq!(fs.bytes_allocated(), 0);

    // After pushing (may cause realloc, but capacity ≥ len).
    fs.current_mut().push(1u64);
    // bytes_allocated = capacity_current * 8 + capacity_next * 8; must be ≥ 8.
    assert!(fs.bytes_allocated() >= 8);
}

// ── T5: FrontierScratch::clear resets both buffers ────────────────────────────

#[test]
fn frontier_scratch_clear_resets() {
    let mut fs = FrontierScratch::new(8);
    fs.current_mut().extend([1u64, 2, 3]);
    fs.next_mut().push(4u64);
    fs.clear();
    assert!(fs.current().is_empty(), "current must be empty after clear");
    assert!(fs.next_mut().is_empty(), "next must be empty after clear");
}
