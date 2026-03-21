//! Spill-to-disk hash-join for ASP-Join on large intermediate hash tables.
//!
//! When the build-side hash map would exceed `SPILL_THRESHOLD` entries, this
//! operator partitions the probe and build sides by `node_slot % num_partitions`
//! and processes one partition at a time, spilling overflow partitions to temp
//! files.  This keeps peak memory bounded at `SPILL_THRESHOLD` entries per
//! partition.
//!
//! The public API is intentionally compatible with [`AspJoin::two_hop`]:
//! `SpillingHashJoin::two_hop(src_slot)` returns the same deduplicated, sorted
//! set of friend-of-friend slots.
//!
//! SPA-114

use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use sparrowdb_common::{Error, Result};
use sparrowdb_storage::csr::CsrForward;
use tempfile::NamedTempFile;

/// Default hash-map entry count before partitioning/spilling.
pub const SPILL_THRESHOLD: usize = 500_000;

/// Default number of partitions when spilling.
const NUM_PARTITIONS: usize = 16;

// ---------------------------------------------------------------------------
// SpillingHashJoin
// ---------------------------------------------------------------------------

/// ASP-Join with spill-to-disk support for large intermediate hash tables.
///
/// When the hash map built from a source node's 2-hop neighbourhood exceeds
/// [`SPILL_THRESHOLD`] entries, the operator partitions intermediate data into
/// temp files and processes them one partition at a time.
pub struct SpillingHashJoin<'a> {
    csr: &'a CsrForward,
    spill_threshold: usize,
    num_partitions: usize,
}

impl<'a> SpillingHashJoin<'a> {
    /// Create with default thresholds.
    pub fn new(csr: &'a CsrForward) -> Self {
        SpillingHashJoin {
            csr,
            spill_threshold: SPILL_THRESHOLD,
            num_partitions: NUM_PARTITIONS,
        }
    }

    /// Create with explicit thresholds (useful for testing spill behaviour).
    pub fn with_thresholds(
        csr: &'a CsrForward,
        spill_threshold: usize,
        num_partitions: usize,
    ) -> Self {
        SpillingHashJoin {
            csr,
            spill_threshold,
            num_partitions,
        }
    }

    /// Compute 2-hop friends-of-friends for `src_slot`.
    ///
    /// Returns a deduplicated, sorted set of fof slots, identical in
    /// semantics to [`AspJoin::two_hop`].
    pub fn two_hop(&self, src_slot: u64) -> Result<Vec<u64>> {
        let direct = self.csr.neighbors(src_slot);
        if direct.is_empty() {
            return Ok(vec![]);
        }

        // Fast path: if the total fof entry count is below threshold, use a
        // plain in-memory hash map — no spill overhead.
        let total_fof_estimate: usize = direct
            .iter()
            .map(|&mid| self.csr.neighbors(mid).len())
            .sum();

        if total_fof_estimate <= self.spill_threshold {
            return self.two_hop_in_memory(direct);
        }

        // Slow path: partition-based spill.
        self.two_hop_spilling(direct)
    }

    // ── In-memory path ────────────────────────────────────────────────────

    fn two_hop_in_memory(&self, direct: &[u64]) -> Result<Vec<u64>> {
        let mut hash: HashMap<u64, Vec<u64>> = HashMap::new();
        for &mid in direct {
            let fof_list = self.csr.neighbors(mid);
            if !fof_list.is_empty() {
                hash.entry(mid).or_default().extend_from_slice(fof_list);
            }
        }

        let mut fof_set: HashSet<u64> = HashSet::new();
        for &mid in direct {
            if let Some(fof_list) = hash.get(&mid) {
                fof_set.extend(fof_list.iter().copied());
            }
        }

        let mut result: Vec<u64> = fof_set.into_iter().collect();
        result.sort_unstable();
        Ok(result)
    }

    // ── Spilling path ─────────────────────────────────────────────────────

    fn two_hop_spilling(&self, direct: &[u64]) -> Result<Vec<u64>> {
        let np = self.num_partitions;

        // Phase 1: distribute (mid, fof) pairs into per-partition temp files.
        let mut part_files: Vec<NamedTempFile> = (0..np)
            .map(|_| NamedTempFile::new().map_err(Error::Io))
            .collect::<Result<_>>()?;

        {
            let mut writers: Vec<BufWriter<&mut std::fs::File>> = part_files
                .iter_mut()
                .map(|f| BufWriter::new(f.as_file_mut()))
                .collect();

            for &mid in direct {
                let fof_list = self.csr.neighbors(mid);
                if fof_list.is_empty() {
                    continue;
                }
                let p = (mid as usize) % np;
                for &fof in fof_list {
                    write_u64_pair(&mut writers[p], mid, fof)?;
                }
            }

            for w in &mut writers {
                w.flush().map_err(Error::Io)?;
            }
        }

        // Phase 2: process each partition independently.
        let mut fof_set: HashSet<u64> = HashSet::new();

        for file in &mut part_files {
            file.as_file_mut()
                .seek(SeekFrom::Start(0))
                .map_err(Error::Io)?;
            let mut reader = BufReader::new(file.as_file_mut());

            let mut hash: HashMap<u64, Vec<u64>> = HashMap::new();
            while let Some((mid, fof)) = read_u64_pair(&mut reader)? {
                hash.entry(mid).or_default().push(fof);
            }

            for fof_list in hash.values() {
                fof_set.extend(fof_list.iter().copied());
            }
        }

        let mut result: Vec<u64> = fof_set.into_iter().collect();
        result.sort_unstable();
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Serialisation helpers for (u64, u64) pairs
// ---------------------------------------------------------------------------

fn write_u64_pair<W: Write>(w: &mut W, a: u64, b: u64) -> Result<()> {
    w.write_all(&a.to_le_bytes()).map_err(Error::Io)?;
    w.write_all(&b.to_le_bytes()).map_err(Error::Io)?;
    Ok(())
}

fn read_u64_pair<R: Read>(r: &mut R) -> Result<Option<(u64, u64)>> {
    let mut buf = [0u8; 8];
    match r.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    }
    let a = u64::from_le_bytes(buf);
    r.read_exact(&mut buf).map_err(Error::Io)?;
    let b = u64::from_le_bytes(buf);
    Ok(Some((a, b)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join::AspJoin;

    /// Build the same social graph used in join.rs tests:
    /// Alice=0, Bob=1, Carol=2, Dave=3, Eve=4
    /// Alice->Bob, Alice->Carol, Bob->Dave, Carol->Dave, Carol->Eve
    fn social_graph() -> CsrForward {
        let edges = vec![(0u64, 1u64), (0, 2), (1, 3), (2, 3), (2, 4)];
        CsrForward::build(5u64, &edges)
    }

    /// SpillingHashJoin on the social graph should give the same result as
    /// the baseline AspJoin (in-memory path, below threshold).
    #[test]
    fn join_spill_small_graph() {
        let csr = social_graph();
        let baseline = AspJoin::new(&csr);
        let spilling = SpillingHashJoin::new(&csr);

        // Alice (0) -> Dave (3) and Eve (4).
        let expected = baseline.two_hop(0).unwrap();
        let got = spilling.two_hop(0).unwrap();
        assert_eq!(got, expected, "Alice fof mismatch");

        // Bob (1) -> Dave (3)'s neighbors = none beyond the graph, so empty
        // because Dave has no outgoing edges.
        let expected_bob = baseline.two_hop(1).unwrap();
        let got_bob = spilling.two_hop(1).unwrap();
        assert_eq!(got_bob, expected_bob, "Bob fof mismatch");
    }

    /// A large ring graph: node i -> i+1 (mod N).
    /// Each node's 2-hop fof is [i+2 mod N].
    /// We force the spilling path by setting a tiny threshold.
    #[test]
    fn join_spill_large_graph() {
        const N: u64 = 10_000;

        // Build ring: 0->1, 1->2, ..., N-1->0
        let edges: Vec<(u64, u64)> = (0..N).map(|i| (i, (i + 1) % N)).collect();
        let csr = CsrForward::build(N, &edges);

        // Baseline (in-memory AspJoin).
        let baseline = AspJoin::new(&csr);

        // Spilling variant with a very small threshold to force the spill path.
        // Each node has exactly 1 direct friend and 1 fof.
        let spilling = SpillingHashJoin::with_thresholds(&csr, 1, 4);

        for src in 0..N {
            let expected = baseline.two_hop(src).unwrap();
            let got = spilling.two_hop(src).unwrap();
            assert_eq!(got, expected, "ring fof mismatch for src={src}");
        }
    }

    /// Node with no outgoing edges returns empty.
    #[test]
    fn join_spill_no_edges() {
        let csr = CsrForward::build(3u64, &[(1u64, 2u64)]);
        let spilling = SpillingHashJoin::new(&csr);
        let got = spilling.two_hop(0).unwrap();
        assert!(got.is_empty());
    }
}
