//! 2-hop ASP-Join tests — RED phase.

use sparrowdb_execution::join::AspJoin;
use sparrowdb_execution::types::Value;
use sparrowdb_storage::csr::CsrForward;

/// Build a tiny social graph: 5 nodes, Alice=0, Bob=1, Carol=2, Dave=3, Eve=4
/// Alice->Bob, Alice->Carol, Bob->Dave, Carol->Dave, Carol->Eve
fn social_graph() -> CsrForward {
    let edges = vec![(0, 1), (0, 2), (1, 3), (2, 3), (2, 4)];
    CsrForward::build(5, &edges)
}

#[test]
fn asp_join_1hop_alice_friends() {
    let csr = social_graph();
    let alice = 0u64;
    // Alice's direct friends: nodes 1 and 2
    let friends: Vec<u64> = csr.neighbors(alice).to_vec();
    assert_eq!(friends, vec![1, 2]);
}

#[test]
fn asp_join_2hop_fof_no_oom() {
    let csr = social_graph();
    let alice = 0u64;

    // Build 2-hop via ASP-Join
    let mut join = AspJoin::new(&csr);
    let fof = join.two_hop(alice).expect("2-hop must succeed");

    // fof of alice: Dave(3) and Eve(4) but NOT Bob(1) or Carol(2) (direct friends)
    // Bob->Dave(3), Carol->Dave(3), Carol->Eve(4)
    // Excluding direct friends: fof = {Dave(3), Eve(4)}
    let mut fof_sorted = fof.clone();
    fof_sorted.sort();
    assert_eq!(fof_sorted, vec![3, 4], "2-hop fof should be Dave and Eve");
}

#[test]
fn asp_join_2hop_multiplicity_preserved() {
    // Multiplicity test: check that the factorized representation does not
    // materialize cartesian products during join state construction.
    let csr = social_graph();
    let alice = 0u64;

    let mut join = AspJoin::new(&csr);
    // Must not OOM or allocate O(N^2) during construction
    let result = join.two_hop_factorized(alice).expect("factorized 2-hop");

    // The chunk's multiplicity must be tracked, not a flat list
    let count = result.logical_row_count();
    // Alice has 2 friends, each with 2 2nd-hop neighbors overlapping at Dave
    // Dave reachable from both Bob and Carol; Eve only from Carol
    // Logical count without dedup: Bob->Dave + Carol->Dave + Carol->Eve = 3
    assert_eq!(count, 3, "multiplicity should be 3 without dedup");
}

#[test]
fn asp_join_semijoin_filter_eliminates_non_matches() {
    // Build a larger graph where the semijoin filter must eliminate build rows
    let edges: Vec<(u64, u64)> = vec![
        (0, 1), // Alice -> Bob
        (1, 5), // Bob -> Frank (not reachable from Alice directly)
        (2, 3), // unrelated
    ];
    let csr = CsrForward::build(6, &edges);
    let alice = 0u64;

    let mut join = AspJoin::new(&csr);
    let fof = join.two_hop(alice).expect("2-hop");

    // Alice's friend is Bob(1), Bob's friend is Frank(5)
    // Frank(5) is not Alice's direct friend, so fof = {5}
    assert_eq!(fof, vec![5], "fof should be [Frank]");
}
