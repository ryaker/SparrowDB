//! Binary ASP-Join for 2-hop traversals.
//!
//! Implements the factorized join contract from spec Section 13.3.
//!
//! Algorithm:
//! 1. Collect probe-side (src node's direct neighbors).
//! 2. Build a Roaring semijoin filter from the probe keys.
//! 3. For build-side, scan mid-node neighbors — only those admitted by filter.
//! 4. Build hash state: {mid_node_slot → Vec<dst_slot>}.
//! 5. Re-probe: for each src neighbor, look up in hash to get fof set.
//! 6. Propagate multiplicity without materializing the full Cartesian product.

use std::collections::HashMap;

use roaring::RoaringBitmap;
use sparrowdb_common::Result;
use sparrowdb_storage::csr::CsrForward;

/// Binary ASP-Join: 2-hop traversal over a CSR graph.
pub struct AspJoin<'a> {
    csr: &'a CsrForward,
}

impl<'a> AspJoin<'a> {
    pub fn new(csr: &'a CsrForward) -> Self {
        AspJoin { csr }
    }

    /// Compute 2-hop friends-of-friends for `src_slot`.
    ///
    /// Returns the deduplicated set of fof node slots.
    /// Does NOT exclude direct friends of `src_slot` — that filtering is
    /// handled separately in the WHERE NOT clause at the planner level.
    pub fn two_hop(&self, src_slot: u64) -> Result<Vec<u64>> {
        // Step 1: probe side — direct neighbors of src.
        let direct = self.csr.neighbors(src_slot);

        if direct.is_empty() {
            return Ok(vec![]);
        }

        // Step 2: build semijoin filter from direct neighbors.
        // RoaringBitmap only supports u32 keys — return an error rather than
        // silently dropping nodes whose slot exceeds u32::MAX.
        let mut filter = RoaringBitmap::new();
        for &mid in direct {
            let mid32 = u32::try_from(mid).map_err(|_| {
                sparrowdb_common::Error::InvalidArgument(format!(
                    "node slot {mid} exceeds u32::MAX; cannot use RoaringBitmap semijoin filter"
                ))
            })?;
            filter.insert(mid32);
        }

        // Step 3 & 4: for each mid node admitted by the filter, collect fof.
        let mut hash: HashMap<u32, Vec<u64>> = HashMap::new();
        for &mid in direct {
            // Safety: all mids were validated as u32 in the filter step above.
            if !filter.contains(mid as u32) {
                continue;
            }
            let fof_list = self.csr.neighbors(mid);
            hash.entry(mid as u32)
                .or_default()
                .extend_from_slice(fof_list);
        }

        // Step 5: re-probe — collect all fof nodes, deduplicate.
        let mut fof_set: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for &mid in direct {
            if let Some(fof_list) = hash.get(&(mid as u32)) {
                for &fof in fof_list {
                    fof_set.insert(fof);
                }
            }
        }

        let mut result: Vec<u64> = fof_set.into_iter().collect();
        result.sort_unstable();
        Ok(result)
    }

    /// Compute 2-hop in factorized form: preserves multiplicity without
    /// materializing a flat list.
    ///
    /// Returns a FactorizedChunk where:
    /// - Each VectorGroup represents one mid-node with its fof set.
    /// - Multiplicity is preserved per group.
    /// - `logical_row_count()` returns the total count of (mid, fof) pairs.
    pub fn two_hop_factorized(&self, src_slot: u64) -> Result<TwoHopChunk> {
        let direct = self.csr.neighbors(src_slot);
        if direct.is_empty() {
            return Ok(TwoHopChunk {
                groups: vec![],
                total_count: 0,
            });
        }

        // Build semijoin filter.
        let mut filter = RoaringBitmap::new();
        for &mid in direct {
            if mid <= u32::MAX as u64 {
                filter.insert(mid as u32);
            }
        }

        // Build hash state: mid → [fof slots].
        let mut hash: HashMap<u32, Vec<u64>> = HashMap::new();
        for &mid in direct {
            if !filter.contains(mid as u32) {
                continue;
            }
            let fof_list = self.csr.neighbors(mid);
            if !fof_list.is_empty() {
                hash.entry(mid as u32)
                    .or_default()
                    .extend_from_slice(fof_list);
            }
        }

        // Re-probe in factorized form: one group per (src, mid) pair.
        let mut groups = Vec::new();
        let mut total_count = 0u64;

        for (&mid, fof_list) in &hash {
            // Each VectorGroup represents one mid-node with all its fof neighbors.
            // Multiplicity = 1 because each mid produces exactly these fof nodes.
            let count = fof_list.len() as u64;
            total_count += count;
            groups.push(TwoHopGroup {
                mid_slot: mid as u64,
                fof_slots: fof_list.clone(),
                multiplicity: 1,
            });
        }

        Ok(TwoHopChunk {
            groups,
            total_count,
        })
    }
}

/// A factorized 2-hop chunk: each group is one (mid, [fof...]) set.
pub struct TwoHopChunk {
    pub groups: Vec<TwoHopGroup>,
    pub total_count: u64,
}

impl TwoHopChunk {
    /// Total logical row count (sum of fof set sizes).
    pub fn logical_row_count(&self) -> u64 {
        self.total_count
    }
}

/// One group in a factorized 2-hop chunk.
pub struct TwoHopGroup {
    pub mid_slot: u64,
    pub fof_slots: Vec<u64>,
    pub multiplicity: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn social_graph() -> CsrForward {
        // Alice=0, Bob=1, Carol=2, Dave=3, Eve=4
        // Alice->Bob, Alice->Carol, Bob->Dave, Carol->Dave, Carol->Eve
        let edges = vec![(0, 1), (0, 2), (1, 3), (2, 3), (2, 4)];
        CsrForward::build(5, &edges)
    }

    #[test]
    fn two_hop_alice_fof() {
        let csr = social_graph();
        let join = AspJoin::new(&csr);
        let fof = join.two_hop(0).unwrap();
        assert_eq!(fof, vec![3, 4]); // Dave and Eve
    }

    #[test]
    fn two_hop_no_friends() {
        let csr = CsrForward::build(3, &[(1, 2)]);
        let join = AspJoin::new(&csr);
        // Node 0 has no friends.
        let fof = join.two_hop(0).unwrap();
        assert!(fof.is_empty());
    }

    #[test]
    fn two_hop_factorized_count() {
        let csr = social_graph();
        let join = AspJoin::new(&csr);
        let chunk = join.two_hop_factorized(0).unwrap();
        // Bob->Dave (1), Carol->Dave (1), Carol->Eve (1) = 3 logical rows
        assert_eq!(chunk.logical_row_count(), 3);
    }
}
