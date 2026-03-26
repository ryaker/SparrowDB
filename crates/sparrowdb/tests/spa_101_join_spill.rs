//! SPA-101 — ASP-Join hash build spill to disk.
//!
//! Tests that [`SpillingHashJoin`] produces the same results as the baseline
//! in-memory [`AspJoin`] in both the in-memory fast path and the spill path.
//!
//! 1. Small graph that stays in memory — regression test.
//! 2. Large ring graph that forces a spill — correctness test.

use sparrowdb_execution::join::AspJoin;
use sparrowdb_execution::join_spill::SpillingHashJoin;
use sparrowdb_storage::csr::CsrForward;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn social_graph() -> CsrForward {
    // Alice=0, Bob=1, Carol=2, Dave=3, Eve=4
    // Alice->Bob, Alice->Carol, Bob->Dave, Carol->Dave, Carol->Eve
    let edges = vec![(0u64, 1u64), (0, 2), (1, 3), (2, 3), (2, 4)];
    CsrForward::build(5, &edges)
}

// ── Test 1: small join stays in memory (regression) ─────────────────────────

#[test]
fn spill_join_small_graph_matches_baseline() {
    let csr = social_graph();
    let baseline = AspJoin::new(&csr);
    let spilling = SpillingHashJoin::new(&csr);

    for src in 0u64..5 {
        let expected = baseline.two_hop(src).unwrap();
        let got = spilling.two_hop(src).unwrap();
        assert_eq!(got, expected, "mismatch for src={src} (in-memory path)");
    }
}

// ── Test 2: large join forces spill (correctness) ───────────────────────────

#[test]
fn spill_join_large_ring_forced_spill() {
    // Build a ring: node i -> i+1 (mod N).
    // Each node's 2-hop fof is [(i+2) % N].
    const N: u64 = 5_000;

    let edges: Vec<(u64, u64)> = (0..N).map(|i| (i, (i + 1) % N)).collect();
    let csr = CsrForward::build(N, &edges);

    let baseline = AspJoin::new(&csr);

    // Threshold=1 forces every node into the spill path.
    let spilling = SpillingHashJoin::with_thresholds(&csr, 1, 4);

    for src in 0..N {
        let expected = baseline.two_hop(src).unwrap();
        let got = spilling.two_hop(src).unwrap();
        assert_eq!(
            got, expected,
            "ring fof mismatch for src={src} (spill path)"
        );
    }
}

// ── Test 3: fan-out graph with overlapping fof ──────────────────────────────

#[test]
fn spill_join_fanout_graph_forced_spill() {
    // Node 0 connects to nodes 1..=50.
    // Each of those connects to nodes 100..=149.
    // So node 0's fof = {100..=149}, reachable via every mid node.
    // This creates a large build side (50 * 50 = 2500 entries).
    let mut edges: Vec<(u64, u64)> = Vec::new();
    for mid in 1u64..=50 {
        edges.push((0, mid));
        for fof in 100u64..=149 {
            edges.push((mid, fof));
        }
    }
    let csr = CsrForward::build(150, &edges);

    let baseline = AspJoin::new(&csr);
    let expected = baseline.two_hop(0).unwrap();

    // Force spill with threshold lower than total entries (2500).
    let spilling = SpillingHashJoin::with_thresholds(&csr, 100, 8);
    let got = spilling.two_hop(0).unwrap();

    assert_eq!(got, expected, "fan-out fof mismatch (spill path)");
    assert_eq!(got.len(), 50, "should have exactly 50 fof nodes");
}
