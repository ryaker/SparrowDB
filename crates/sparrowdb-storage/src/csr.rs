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
//!
//! ## Lazy loading (SPA-222)
//!
//! `open()` memory-maps the file instead of reading it into heap-allocated
//! vectors.  The OS pages data in on first access and evicts cold pages under
//! memory pressure.  `Clone` on a mapped CSR is an `Arc` bump — no data copy.

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;
use sparrowdb_common::{Error, Result};

// ── Shared backing store ────────────────────────────────────────────────────

/// Backing data for CSR arrays — either heap-owned or memory-mapped.
///
/// `Clone` on the `Mapped` variant bumps the `Arc` reference count; no data
/// is copied.  `Clone` on `Owned` deep-copies the vectors.
#[derive(Clone)]
enum CsrData {
    Owned {
        offsets: Vec<u64>,
        neighbors: Vec<u64>,
    },
    Mapped {
        mmap: Arc<Mmap>,
        /// Byte offset where the offsets array starts (always 8).
        offsets_byte_start: usize,
        /// Number of u64 entries in the offsets array (n_nodes + 1).
        offsets_count: usize,
        /// Byte offset where the neighbors array starts.
        neighbors_byte_start: usize,
        /// Number of u64 entries in the neighbors array.
        neighbors_count: usize,
    },
}

impl CsrData {
    /// Interpret mmap bytes as a `&[u64]` slice (little-endian, naturally aligned).
    ///
    /// # Safety
    /// The CSR binary layout stores u64 values at 8-byte aligned offsets
    /// starting from the beginning of the file, so alignment is guaranteed.
    #[inline]
    fn slice_from_mmap(mmap: &Mmap, byte_start: usize, count: usize) -> &[u64] {
        if count == 0 {
            return &[];
        }
        let ptr = mmap[byte_start..].as_ptr();
        debug_assert!(ptr as usize % std::mem::align_of::<u64>() == 0);
        unsafe { std::slice::from_raw_parts(ptr as *const u64, count) }
    }

    fn offsets(&self) -> &[u64] {
        match self {
            CsrData::Owned { offsets, .. } => offsets,
            CsrData::Mapped {
                mmap,
                offsets_byte_start,
                offsets_count,
                ..
            } => Self::slice_from_mmap(mmap, *offsets_byte_start, *offsets_count),
        }
    }

    fn neighbors(&self) -> &[u64] {
        match self {
            CsrData::Owned { neighbors, .. } => neighbors,
            CsrData::Mapped {
                mmap,
                neighbors_byte_start,
                neighbors_count,
                ..
            } => Self::slice_from_mmap(mmap, *neighbors_byte_start, *neighbors_count),
        }
    }

    /// Create mapped data from a validated mmap.
    fn from_mmap(mmap: Mmap) -> Result<(u64, Self)> {
        let bytes = &mmap[..];
        if bytes.len() < 8 {
            return Err(Error::Corruption("CSR file too short for n_nodes".into()));
        }
        let n_nodes = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let offsets_count = n_nodes as usize + 1;
        let offsets_byte_start = 8;
        let offsets_byte_end = offsets_byte_start + offsets_count * 8;

        if bytes.len() < offsets_byte_end {
            return Err(Error::Corruption(format!(
                "CSR file too short for offsets: need {} bytes, have {}",
                offsets_byte_end,
                bytes.len()
            )));
        }

        // Read n_edges from the sentinel offset[n_nodes].
        let sentinel_start = offsets_byte_start + n_nodes as usize * 8;
        let n_edges = u64::from_le_bytes(
            bytes[sentinel_start..sentinel_start + 8]
                .try_into()
                .unwrap(),
        ) as usize;

        let neighbors_byte_start = offsets_byte_end;
        let neighbors_byte_end = neighbors_byte_start + n_edges * 8;

        if bytes.len() < neighbors_byte_end {
            return Err(Error::Corruption(format!(
                "CSR file too short for neighbors: need {} bytes, have {}",
                neighbors_byte_end,
                bytes.len()
            )));
        }

        Ok((
            n_nodes,
            CsrData::Mapped {
                mmap: Arc::new(mmap),
                offsets_byte_start,
                offsets_count,
                neighbors_byte_start,
                neighbors_count: n_edges,
            },
        ))
    }
}

// ── CSR Forward ───────────────────────────────────────────────────────────────

/// CSR forward-edge file: for each source node, the set of destination nodes.
///
/// When opened from disk, the file is memory-mapped for lazy, OS-managed
/// paging.  `Clone` on a mapped CSR is an `Arc` reference-count bump.
#[derive(Clone)]
pub struct CsrForward {
    n_nodes: u64,
    data: CsrData,
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
            data: CsrData::Owned { offsets, neighbors },
        }
    }

    /// Open an existing CSR forward file from disk via memory-map (SPA-222).
    ///
    /// # Safety
    /// `Mmap::map` is safe here because the file handle lives for the duration
    /// of the call; the resulting `Mmap` owns its mapping and extends the
    /// lifetime independently of `file`.  No other code aliases the raw pointer
    /// before the `Mmap` is fully constructed.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(Error::Io)?;
        // SAFETY: see doc comment above.
        let mmap = unsafe { Mmap::map(&file) }.map_err(Error::Io)?;
        let (n_nodes, data) = CsrData::from_mmap(mmap)?;
        Ok(CsrForward { n_nodes, data })
    }

    /// Encode this CSR to its binary representation.
    pub fn encode(&self) -> Vec<u8> {
        encode_csr(self.n_nodes, self.data.offsets(), self.data.neighbors())
    }

    /// Decode a CSR from its binary representation (heap-allocated).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (n_nodes, offsets, neighbors) = decode_csr(bytes)?;
        Ok(CsrForward {
            n_nodes,
            data: CsrData::Owned { offsets, neighbors },
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
        let offsets = self.data.offsets();
        let start = offsets[node_id as usize] as usize;
        let end = offsets[node_id as usize + 1] as usize;
        &self.data.neighbors()[start..end]
    }

    pub fn n_nodes(&self) -> u64 {
        self.n_nodes
    }

    pub fn n_edges(&self) -> u64 {
        self.data.neighbors().len() as u64
    }
}

// ── CSR Backward ──────────────────────────────────────────────────────────────

/// CSR backward-edge file: for each destination node, the set of source nodes.
pub struct CsrBackward {
    n_nodes: u64,
    data: CsrData,
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
            data: fwd.data,
        }
    }

    /// Open an existing CSR backward file from disk via memory-map (SPA-222).
    ///
    /// # Safety
    /// `Mmap::map` is safe here because the file handle lives for the duration
    /// of the call; the resulting `Mmap` owns its mapping and extends the
    /// lifetime independently of `file`.  No other code aliases the raw pointer
    /// before the `Mmap` is fully constructed.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(Error::Io)?;
        // SAFETY: see doc comment above.
        let mmap = unsafe { Mmap::map(&file) }.map_err(Error::Io)?;
        let (n_nodes, data) = CsrData::from_mmap(mmap)?;
        Ok(CsrBackward { n_nodes, data })
    }

    /// Encode this CSR to its binary representation.
    pub fn encode(&self) -> Vec<u8> {
        encode_csr(self.n_nodes, self.data.offsets(), self.data.neighbors())
    }

    /// Decode a CSR from its binary representation (heap-allocated).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (n_nodes, offsets, neighbors) = decode_csr(bytes)?;
        Ok(CsrBackward {
            n_nodes,
            data: CsrData::Owned { offsets, neighbors },
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
        let offsets = self.data.offsets();
        let start = offsets[node_id as usize] as usize;
        let end = offsets[node_id as usize + 1] as usize;
        &self.data.neighbors()[start..end]
    }

    pub fn n_nodes(&self) -> u64 {
        self.n_nodes
    }

    pub fn n_edges(&self) -> u64 {
        self.data.neighbors().len() as u64
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
        let fixture_path = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/csr_forward.bin"
        ));
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
        let fixture_path = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/csr_backward.bin"
        ));
        let bytes = std::fs::read(fixture_path).expect("golden fixture csr_backward.bin not found");
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

    // ── Mmap-specific tests (SPA-222) ───────────────────────────────────────

    #[test]
    fn test_mmap_open_returns_correct_neighbors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fwd.csr");

        let built = CsrForward::build(N_NODES, TEST_EDGES);
        built.write(&path).unwrap();

        // open() now uses mmap instead of fs::read
        let mapped = CsrForward::open(&path).unwrap();
        assert_eq!(mapped.n_nodes(), N_NODES);
        assert_eq!(mapped.n_edges(), TEST_EDGES.len() as u64);
        for node in 0..N_NODES {
            assert_eq!(built.neighbors(node), mapped.neighbors(node));
        }
    }

    #[test]
    fn test_mmap_clone_is_cheap() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fwd.csr");

        CsrForward::build(N_NODES, TEST_EDGES).write(&path).unwrap();
        let mapped = CsrForward::open(&path).unwrap();

        // Clone should work and produce identical results.
        let cloned = mapped.clone();
        assert_eq!(mapped.neighbors(0), cloned.neighbors(0));
        assert_eq!(mapped.neighbors(4), cloned.neighbors(4));
        assert_eq!(mapped.n_edges(), cloned.n_edges());
    }

    #[test]
    fn test_mmap_large_graph() {
        // 50K nodes with edges to verify mmap works at scale.
        let n: u64 = 50_000;
        let edges: Vec<(u64, u64)> = (0..n)
            .flat_map(|i| {
                let next = (i + 1) % n;
                let skip = (i + 7) % n;
                vec![(i, next), (i, skip)]
            })
            .collect();

        let dir = tempdir().unwrap();
        let fwd_path = dir.path().join("large.fwd.csr");

        let built = CsrForward::build(n, &edges);
        built.write(&fwd_path).unwrap();

        let mapped = CsrForward::open(&fwd_path).unwrap();
        assert_eq!(mapped.n_nodes(), n);
        assert_eq!(mapped.n_edges(), edges.len() as u64);

        // Spot-check some neighbors.
        assert_eq!(mapped.neighbors(0), built.neighbors(0));
        assert_eq!(mapped.neighbors(25_000), built.neighbors(25_000));
        assert_eq!(mapped.neighbors(49_999), built.neighbors(49_999));
    }
}
