//! CSR (Compressed Sparse Row) edge storage.
//!
//! ## Binary layout (both forward and backward files)
//!
//! ```text
//! [n_nodes:   u64 LE]
//! [offsets:   (n_nodes + 1) × u64 LE]   — offsets[i] = start of node i's neighbors
//! [neighbors: n_edges × u64 LE]          — packed neighbor node IDs
//! ```
//!
//! `offsets[n_nodes]` equals `n_edges` (sentinel).
//!
//! ## File naming convention
//!
//! - Forward edges: `base.fwd.csr`   — `offsets[src]` → neighbor = dst
//! - Backward edges: `base.bwd.csr`  — `offsets[dst]` → neighbor = src

use std::fs;
use std::path::Path;

use sparrowdb_common::{Error, Result};

// ── CSR Forward ───────────────────────────────────────────────────────────────

/// CSR forward-edge file: for each source node, the set of destination nodes.
///
/// Memory-maps (or copies) the entire file into a flat byte buffer, then provides
/// zero-copy slice access to neighbor lists.
pub struct CsrForward {
    n_nodes: u64,
    offsets: Vec<u64>,    // length = n_nodes + 1
    neighbors: Vec<u64>,  // length = n_edges
}

impl CsrForward {
    /// Build a new [`CsrForward`] from a sorted edge list.
    ///
    /// `edges` is a slice of `(src, dst)` pairs.
    /// `n_nodes` is the number of nodes (node IDs are `0..n_nodes`).
    pub fn build(n_nodes: u64, edges: &[(u64, u64)]) -> Self {
        // Count out-degree of each source node.
        let mut degree = vec![0u64; n_nodes as usize];
        for &(src, _dst) in edges {
            degree[src as usize] += 1;
        }

        // Build offset array (prefix sum).
        let mut offsets = vec![0u64; n_nodes as usize + 1];
        for i in 0..n_nodes as usize {
            offsets[i + 1] = offsets[i] + degree[i];
        }

        // Fill neighbors in CSR order using a cursor per node.
        let mut neighbors = vec![0u64; edges.len()];
        let mut cursor = offsets[..n_nodes as usize].to_vec();
        for &(src, dst) in edges {
            let pos = cursor[src as usize] as usize;
            neighbors[pos] = dst;
            cursor[src as usize] += 1;
        }

        CsrForward {
            n_nodes,
            offsets,
            neighbors,
        }
    }

    /// Open an existing CSR forward file from disk.
    pub fn open(path: &Path) -> Result<Self> {
        let bytes = fs::read(path).map_err(Error::Io)?;
        Self::decode(&bytes)
    }

    /// Encode this CSR to its binary representation.
    pub fn encode(&self) -> Vec<u8> {
        encode_csr(self.n_nodes, &self.offsets, &self.neighbors)
    }

    /// Decode a CSR from its binary representation.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (n_nodes, offsets, neighbors) = decode_csr(bytes)?;
        Ok(CsrForward {
            n_nodes,
            offsets,
            neighbors,
        })
    }

    /// Write this CSR to a file on disk.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        fs::write(path, self.encode()).map_err(Error::Io)
    }

    /// Return the sorted slice of destination node IDs for `node_id`.
    pub fn neighbors(&self, node_id: u64) -> &[u64] {
        if node_id >= self.n_nodes {
            return &[];
        }
        let start = self.offsets[node_id as usize] as usize;
        let end = self.offsets[node_id as usize + 1] as usize;
        &self.neighbors[start..end]
    }

    pub fn n_nodes(&self) -> u64 {
        self.n_nodes
    }

    pub fn n_edges(&self) -> u64 {
        self.neighbors.len() as u64
    }
}

// ── CSR Backward ──────────────────────────────────────────────────────────────

/// CSR backward-edge file: for each destination node, the set of source nodes.
pub struct CsrBackward {
    n_nodes: u64,
    offsets: Vec<u64>,
    neighbors: Vec<u64>,
}

impl CsrBackward {
    /// Build a new [`CsrBackward`] from a sorted edge list.
    ///
    /// Stores reverse edges (`dst → src`).
    pub fn build(n_nodes: u64, edges: &[(u64, u64)]) -> Self {
        // Reverse each edge and build a forward CSR over `(dst, src)`.
        let reversed: Vec<(u64, u64)> = edges.iter().map(|&(src, dst)| (dst, src)).collect();
        let fwd = CsrForward::build(n_nodes, &reversed);
        CsrBackward {
            n_nodes: fwd.n_nodes,
            offsets: fwd.offsets,
            neighbors: fwd.neighbors,
        }
    }

    /// Open an existing CSR backward file from disk.
    pub fn open(path: &Path) -> Result<Self> {
        let bytes = fs::read(path).map_err(Error::Io)?;
        Self::decode(&bytes)
    }

    /// Encode this CSR to its binary representation.
    pub fn encode(&self) -> Vec<u8> {
        encode_csr(self.n_nodes, &self.offsets, &self.neighbors)
    }

    /// Decode a CSR from its binary representation.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (n_nodes, offsets, neighbors) = decode_csr(bytes)?;
        Ok(CsrBackward {
            n_nodes,
            offsets,
            neighbors,
        })
    }

    /// Write this CSR to a file on disk.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        fs::write(path, self.encode()).map_err(Error::Io)
    }

    /// Return the sorted slice of source node IDs that point to `node_id`.
    pub fn predecessors(&self, node_id: u64) -> &[u64] {
        if node_id >= self.n_nodes {
            return &[];
        }
        let start = self.offsets[node_id as usize] as usize;
        let end = self.offsets[node_id as usize + 1] as usize;
        &self.neighbors[start..end]
    }

    pub fn n_nodes(&self) -> u64 {
        self.n_nodes
    }

    pub fn n_edges(&self) -> u64 {
        self.neighbors.len() as u64
    }
}

// ── Shared encode / decode ────────────────────────────────────────────────────

/// Encode `[n_nodes][offsets × (n_nodes+1)][neighbors × n_edges]` into bytes.
fn encode_csr(n_nodes: u64, offsets: &[u64], neighbors: &[u64]) -> Vec<u8> {
    // capacity: 8 + (n_nodes+1)*8 + n_edges*8
    let n_edges = neighbors.len();
    let cap = 8 + (n_nodes as usize + 1) * 8 + n_edges * 8;
    let mut buf = Vec::with_capacity(cap);

    buf.extend_from_slice(&n_nodes.to_le_bytes());
    for &off in offsets {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    for &nb in neighbors {
        buf.extend_from_slice(&nb.to_le_bytes());
    }

    debug_assert_eq!(buf.len(), cap);
    buf
}

/// Decode `[n_nodes][offsets][neighbors]` from bytes.
fn decode_csr(bytes: &[u8]) -> Result<(u64, Vec<u64>, Vec<u64>)> {
    if bytes.len() < 8 {
        return Err(Error::Corruption("CSR file too short for n_nodes".into()));
    }

    let n_nodes = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let offset_count = n_nodes as usize + 1;
    let offsets_bytes = offset_count * 8;

    if bytes.len() < 8 + offsets_bytes {
        return Err(Error::Corruption(format!(
            "CSR file too short for offsets: need {} bytes, have {}",
            8 + offsets_bytes,
            bytes.len()
        )));
    }

    let mut offsets = Vec::with_capacity(offset_count);
    for i in 0..offset_count {
        let start = 8 + i * 8;
        let val = u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
        offsets.push(val);
    }

    // sentinel: offsets[n_nodes] == n_edges
    let n_edges = offsets[n_nodes as usize] as usize;
    let neighbors_start = 8 + offsets_bytes;
    let neighbors_end = neighbors_start + n_edges * 8;

    if bytes.len() < neighbors_end {
        return Err(Error::Corruption(format!(
            "CSR file too short for neighbors: need {} bytes, have {}",
            neighbors_end,
            bytes.len()
        )));
    }

    let mut neighbors = Vec::with_capacity(n_edges);
    for i in 0..n_edges {
        let start = neighbors_start + i * 8;
        let val = u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
        neighbors.push(val);
    }

    Ok((n_nodes, offsets, neighbors))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ── Graph used for unit tests ─────────────────────────────────────────────
    //
    // 5 nodes (0..4), 8 edges:
    //   0→1, 0→2, 1→3, 2→3, 2→4, 3→4, 4→0, 4→1
    //
    // This is the same graph encoded in the golden fixtures.

    const TEST_EDGES: &[(u64, u64)] = &[
        (0, 1),
        (0, 2),
        (1, 3),
        (2, 3),
        (2, 4),
        (3, 4),
        (4, 0),
        (4, 1),
    ];
    const N_NODES: u64 = 5;

    // ── CSR forward ──────────────────────────────────────────────────────────

    #[test]
    fn test_csr_forward_neighbors() {
        let fwd = CsrForward::build(N_NODES, TEST_EDGES);

        assert_eq!(fwd.neighbors(0), &[1, 2]);
        assert_eq!(fwd.neighbors(1), &[3]);
        assert_eq!(fwd.neighbors(2), &[3, 4]);
        assert_eq!(fwd.neighbors(3), &[4]);
        assert_eq!(fwd.neighbors(4), &[0, 1]);
    }

    #[test]
    fn test_csr_forward_out_of_range_returns_empty() {
        let fwd = CsrForward::build(N_NODES, TEST_EDGES);
        assert_eq!(fwd.neighbors(99), &[]);
    }

    #[test]
    fn test_csr_forward_roundtrip() {
        let fwd = CsrForward::build(N_NODES, TEST_EDGES);
        let bytes = fwd.encode();
        let fwd2 = CsrForward::decode(&bytes).unwrap();
        assert_eq!(fwd2.n_nodes(), N_NODES);
        assert_eq!(fwd2.n_edges(), TEST_EDGES.len() as u64);
        for &(src, _) in TEST_EDGES {
            assert_eq!(fwd.neighbors(src), fwd2.neighbors(src));
        }
    }

    #[test]
    fn test_csr_forward_persists_to_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("base.fwd.csr");

        let fwd = CsrForward::build(N_NODES, TEST_EDGES);
        fwd.write(&path).unwrap();

        let fwd2 = CsrForward::open(&path).unwrap();
        assert_eq!(fwd2.neighbors(0), &[1, 2]);
        assert_eq!(fwd2.neighbors(4), &[0, 1]);
    }

    // ── CSR backward ─────────────────────────────────────────────────────────

    #[test]
    fn test_csr_backward_predecessors() {
        let bwd = CsrBackward::build(N_NODES, TEST_EDGES);

        // who points to 0?  → 4
        assert_eq!(bwd.predecessors(0), &[4]);
        // who points to 1?  → 0, 4
        assert_eq!(bwd.predecessors(1), &[0, 4]);
        // who points to 2?  → 0
        assert_eq!(bwd.predecessors(2), &[0]);
        // who points to 3?  → 1, 2
        assert_eq!(bwd.predecessors(3), &[1, 2]);
        // who points to 4?  → 2, 3
        assert_eq!(bwd.predecessors(4), &[2, 3]);
    }

    #[test]
    fn test_csr_backward_out_of_range_returns_empty() {
        let bwd = CsrBackward::build(N_NODES, TEST_EDGES);
        assert_eq!(bwd.predecessors(99), &[]);
    }

    #[test]
    fn test_csr_backward_roundtrip() {
        let bwd = CsrBackward::build(N_NODES, TEST_EDGES);
        let bytes = bwd.encode();
        let bwd2 = CsrBackward::decode(&bytes).unwrap();
        assert_eq!(bwd2.n_nodes(), N_NODES);
        assert_eq!(bwd2.n_edges(), TEST_EDGES.len() as u64);
        for &(_, dst) in TEST_EDGES {
            assert_eq!(bwd.predecessors(dst), bwd2.predecessors(dst));
        }
    }

    #[test]
    fn test_csr_backward_persists_to_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("base.bwd.csr");

        let bwd = CsrBackward::build(N_NODES, TEST_EDGES);
        bwd.write(&path).unwrap();

        let bwd2 = CsrBackward::open(&path).unwrap();
        assert_eq!(bwd2.predecessors(3), &[1, 2]);
        assert_eq!(bwd2.predecessors(0), &[4]);
    }

    // ── Consistency: acceptance gate #9 ──────────────────────────────────────

    #[test]
    fn test_csr_forward_backward_consistency() {
        // Build graph: 10 nodes, 20 directed edges.
        let edges: Vec<(u64, u64)> = vec![
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 4),
            (1, 5),
            (2, 5),
            (2, 6),
            (3, 6),
            (3, 7),
            (4, 8),
            (5, 8),
            (5, 9),
            (6, 9),
            (6, 0),
            (7, 0),
            (7, 1),
            (8, 2),
            (8, 3),
            (9, 4),
            (9, 5),
        ];
        let n = 10u64;

        let dir = tempdir().unwrap();
        let fwd_path = dir.path().join("base.fwd.csr");
        let bwd_path = dir.path().join("base.bwd.csr");

        // Build and persist.
        CsrForward::build(n, &edges).write(&fwd_path).unwrap();
        CsrBackward::build(n, &edges).write(&bwd_path).unwrap();

        // Reopen from disk (close+reopen).
        let fwd = CsrForward::open(&fwd_path).unwrap();
        let bwd = CsrBackward::open(&bwd_path).unwrap();

        // For every edge (src→dst):
        //   forward.neighbors(src) contains dst  AND
        //   backward.predecessors(dst) contains src
        for &(src, dst) in &edges {
            assert!(
                fwd.neighbors(src).contains(&dst),
                "fwd.neighbors({src}) does not contain {dst}"
            );
            assert!(
                bwd.predecessors(dst).contains(&src),
                "bwd.predecessors({dst}) does not contain {src}"
            );
        }

        // Also verify no phantom edges.
        assert_eq!(fwd.n_edges(), edges.len() as u64);
        assert_eq!(bwd.n_edges(), edges.len() as u64);
    }

    // ── Golden fixture tests ──────────────────────────────────────────────────

    #[test]
    fn test_csr_golden_fixture_forward() {
        // The golden fixture encodes the same 5-node, 8-edge test graph.
        let fixture_path = std::path::Path::new(
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/csr_forward.bin"),
        );
        let bytes = std::fs::read(fixture_path).expect("golden fixture csr_forward.bin not found");
        let fwd = CsrForward::decode(&bytes).unwrap();

        assert_eq!(fwd.n_nodes(), 5);
        assert_eq!(fwd.n_edges(), 8);
        assert_eq!(fwd.neighbors(0), &[1, 2]);
        assert_eq!(fwd.neighbors(1), &[3]);
        assert_eq!(fwd.neighbors(2), &[3, 4]);
        assert_eq!(fwd.neighbors(3), &[4]);
        assert_eq!(fwd.neighbors(4), &[0, 1]);

        // Round-trip: re-encode must be byte-exact.
        let rebuilt = CsrForward::build(5, TEST_EDGES).encode();
        assert_eq!(bytes, rebuilt, "golden fixture byte mismatch on re-encode");
    }

    #[test]
    fn test_csr_golden_fixture_backward() {
        let fixture_path = std::path::Path::new(
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/csr_backward.bin"),
        );
        let bytes =
            std::fs::read(fixture_path).expect("golden fixture csr_backward.bin not found");
        let bwd = CsrBackward::decode(&bytes).unwrap();

        assert_eq!(bwd.n_nodes(), 5);
        assert_eq!(bwd.n_edges(), 8);
        assert_eq!(bwd.predecessors(0), &[4]);
        assert_eq!(bwd.predecessors(1), &[0, 4]);
        assert_eq!(bwd.predecessors(2), &[0]);
        assert_eq!(bwd.predecessors(3), &[1, 2]);
        assert_eq!(bwd.predecessors(4), &[2, 3]);

        // Round-trip: re-encode must be byte-exact.
        let rebuilt = CsrBackward::build(5, TEST_EDGES).encode();
        assert_eq!(bytes, rebuilt, "golden fixture byte mismatch on re-encode");
    }
}
