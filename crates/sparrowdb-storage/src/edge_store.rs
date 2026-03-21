//! Edge storage — delta log append + CSR rebuild on CHECKPOINT.
//!
//! ## Delta log format
//!
//! Each record is written sequentially:
//!
//! ```text
//! [src:    u64 LE]
//! [dst:    u64 LE]
//! [rel_id: u32 LE]
//! ```
//!
//! Total: 20 bytes per record.  The file is append-only and has no global header;
//! record count is inferred from `file_length / 20`.
//!
//! On CHECKPOINT, the delta log is replayed in insertion order to rebuild the
//! CSR forward and backward files.
//!
//! ## File layout
//!
//! ```text
//! {root}/edges/{rel_table_id}/delta.log
//! {root}/edges/{rel_table_id}/base.fwd.csr
//! {root}/edges/{rel_table_id}/base.bwd.csr
//! ```

use std::fs;
use std::io::{self, Write as IoWrite};
use std::path::{Path, PathBuf};

use sparrowdb_common::{EdgeId, Error, NodeId, Result};

use crate::csr::{CsrBackward, CsrForward};

// ── Relationship table ID ─────────────────────────────────────────────────────

/// Identifies a directed relationship table `(src_label, rel_type, dst_label)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelTableId(pub u32);

// ── Delta record ──────────────────────────────────────────────────────────────

/// A single appended edge record in the delta log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaRecord {
    pub src: NodeId,
    pub dst: NodeId,
    pub rel_id: RelTableId,
}

const DELTA_RECORD_SIZE: usize = 8 + 8 + 4; // 20 bytes

impl DeltaRecord {
    fn encode(&self) -> [u8; DELTA_RECORD_SIZE] {
        let mut buf = [0u8; DELTA_RECORD_SIZE];
        buf[0..8].copy_from_slice(&self.src.0.to_le_bytes());
        buf[8..16].copy_from_slice(&self.dst.0.to_le_bytes());
        buf[16..20].copy_from_slice(&self.rel_id.0.to_le_bytes());
        buf
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < DELTA_RECORD_SIZE {
            return Err(Error::Corruption(format!(
                "delta record too short: {} bytes",
                bytes.len()
            )));
        }
        let src = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let dst = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let rel_id = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        Ok(DeltaRecord {
            src: NodeId(src),
            dst: NodeId(dst),
            rel_id: RelTableId(rel_id),
        })
    }
}

// ── EdgeStore ────────────────────────────────────────────────────────────────

/// Persistent edge store for a single relationship table.
///
/// New edges are appended to the delta log.  A checkpoint rebuilds the CSR
/// files from the full delta log and truncates (replaces) the log.
pub struct EdgeStore {
    rel_dir: PathBuf,
    /// The relationship table this store manages.  Used to validate callers.
    rel_table_id: RelTableId,
    /// Monotonically increasing edge ID counter.
    next_edge_id: u64,
}

impl EdgeStore {
    /// Open (or create) an edge store for `rel_table_id` under `db_root`.
    pub fn open(db_root: &Path, rel_table_id: RelTableId) -> Result<Self> {
        let rel_dir = db_root.join("edges").join(rel_table_id.0.to_string());
        fs::create_dir_all(&rel_dir).map_err(Error::Io)?;

        // Derive next_edge_id from the current delta log size.
        let delta_path = rel_dir.join("delta.log");
        let next_edge_id = if delta_path.exists() {
            let meta = fs::metadata(&delta_path).map_err(Error::Io)?;
            meta.len() / DELTA_RECORD_SIZE as u64
        } else {
            0
        };

        Ok(EdgeStore {
            rel_dir,
            rel_table_id,
            next_edge_id,
        })
    }

    fn delta_path(&self) -> PathBuf {
        self.rel_dir.join("delta.log")
    }

    fn fwd_path(&self) -> PathBuf {
        self.rel_dir.join("base.fwd.csr")
    }

    fn bwd_path(&self) -> PathBuf {
        self.rel_dir.join("base.bwd.csr")
    }

    /// Append a new directed edge `src → dst` to the delta log.
    ///
    /// Returns the new [`EdgeId`] (monotonic index into the delta log).
    /// Returns [`Error::InvalidInput`] if `rel_id` does not match this store's
    /// relationship table.
    pub fn create_edge(&mut self, src: NodeId, rel_id: RelTableId, dst: NodeId) -> Result<EdgeId> {
        if rel_id != self.rel_table_id {
            return Err(Error::InvalidArgument(format!(
                "rel_id mismatch: store owns {:?} but caller passed {:?}",
                self.rel_table_id, rel_id
            )));
        }
        let record = DeltaRecord { src, dst, rel_id };
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.delta_path())
            .map_err(Error::Io)?;
        file.write_all(&record.encode()).map_err(Error::Io)?;

        let edge_id = EdgeId(self.next_edge_id);
        self.next_edge_id += 1;
        Ok(edge_id)
    }

    /// Read all delta records from the log.
    pub fn read_delta(&self) -> Result<Vec<DeltaRecord>> {
        let path = self.delta_path();
        if !path.exists() {
            return Ok(vec![]);
        }
        let bytes = fs::read(&path).map_err(Error::Io)?;
        if bytes.len() % DELTA_RECORD_SIZE != 0 {
            return Err(Error::Corruption(format!(
                "delta.log size {} is not a multiple of {}",
                bytes.len(),
                DELTA_RECORD_SIZE
            )));
        }
        let count = bytes.len() / DELTA_RECORD_SIZE;
        let mut records = Vec::with_capacity(count);
        for i in 0..count {
            let offset = i * DELTA_RECORD_SIZE;
            records.push(DeltaRecord::decode(&bytes[offset..])?);
        }
        Ok(records)
    }

    /// CHECKPOINT: rebuild CSR forward and backward files from the delta log.
    ///
    /// `n_nodes` is the total number of nodes across both endpoint labels.
    /// Caller must supply this from the metapage / node-store high-water marks.
    ///
    /// Returns [`Error::InvalidArgument`] if any node ID in the delta is >= `n_nodes`.
    ///
    /// Atomicity guarantee: CSR files are written to temp paths, then renamed
    /// into place.  A crash before both renames leaves the old base files valid
    /// for recovery.  After both renames the delta log is truncated to zero.
    pub fn checkpoint(&mut self, n_nodes: u64) -> Result<()> {
        let edges = self.build_sorted_edges(n_nodes)?;
        self.write_csr_atomic(&edges, n_nodes)?;
        self.truncate_delta()?;
        Ok(())
    }

    /// OPTIMIZE: like CHECKPOINT but additionally sort each source node's
    /// neighbor list by `(dst_node_id)` ascending.
    ///
    /// The CSR builder already receives edges sorted by `(src, dst)`, so the
    /// neighbor arrays are naturally sorted after a regular checkpoint.  This
    /// method exists as a named entry-point that makes the sort guarantee
    /// explicit and can be extended in the future (e.g. secondary sort by
    /// edge_id once edge properties are tracked in the CSR).
    pub fn optimize(&mut self, n_nodes: u64) -> Result<()> {
        // Collect delta records and sort by (src, dst) — identical to checkpoint
        // but we name this method separately to convey intent.
        let mut edges = self.build_sorted_edges(n_nodes)?;
        // Ensure strict (src, dst) order for each src block (already sorted by
        // build_sorted_edges, but we make it explicit here for OPTIMIZE).
        edges.sort_unstable_by_key(|&(src, dst)| (src, dst));
        self.write_csr_atomic(&edges, n_nodes)?;
        self.truncate_delta()?;
        Ok(())
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Read the current CSR base (if any) plus all delta records, merge them,
    /// validate bounds, and return a deduplicated sorted `(src, dst)` edge list.
    ///
    /// This implements the "fold base + delta → fresh base" semantics: the new
    /// CSR captures every edge that was previously checkpointed AND every edge
    /// added since the last checkpoint.
    fn build_sorted_edges(&self, n_nodes: u64) -> Result<Vec<(u64, u64)>> {
        // ── 1. Load existing CSR base edges (may not exist on first checkpoint). ──
        let mut edges: Vec<(u64, u64)> = Vec::new();
        match CsrForward::open(&self.fwd_path()) {
            Ok(fwd) => {
                for src in 0..fwd.n_nodes() {
                    for &dst in fwd.neighbors(src) {
                        edges.push((src, dst));
                    }
                }
            }
            // File does not exist yet — normal on the first checkpoint.
            Err(Error::Io(ref e)) if e.kind() == io::ErrorKind::NotFound => {}
            // Any other failure (permission denied, I/O error, corruption) must
            // not be silently ignored: proceeding with an empty base would fold
            // only the delta into the new CSR, permanently discarding all edges
            // that were written during previous checkpoints.
            Err(e) => return Err(e),
        }

        // ── 2. Apply delta records (insert-only for now). ─────────────────────
        let records = self.read_delta()?;
        for r in &records {
            edges.push((r.src.0, r.dst.0));
        }

        // ── 3. Sort and deduplicate. ──────────────────────────────────────────
        edges.sort_unstable_by_key(|&(src, dst)| (src, dst));
        edges.dedup();

        // ── 4. Validate bounds. ───────────────────────────────────────────────
        // The CSR builder indexes its degree/cursor arrays by node ID, so any
        // out-of-range ID would panic.
        for &(src, dst) in &edges {
            if src >= n_nodes {
                return Err(Error::InvalidArgument(format!(
                    "src node id {src} >= n_nodes {n_nodes}"
                )));
            }
            if dst >= n_nodes {
                return Err(Error::InvalidArgument(format!(
                    "dst node id {dst} >= n_nodes {n_nodes}"
                )));
            }
        }

        Ok(edges)
    }

    /// Build CSR structs from `edges`, write them to temp files, then atomically
    /// rename into the canonical base paths.
    ///
    /// Crash before rename: old base files (if any) remain intact.
    /// Crash after rename: new files are in place, delta will be truncated on
    /// the next call.
    fn write_csr_atomic(&self, edges: &[(u64, u64)], n_nodes: u64) -> Result<()> {
        let fwd = CsrForward::build(n_nodes, edges);
        let bwd = CsrBackward::build(n_nodes, edges);

        // Write forward CSR to a temp file, then rename.
        let fwd_tmp = self.rel_dir.join("base.fwd.csr.tmp");
        let bwd_tmp = self.rel_dir.join("base.bwd.csr.tmp");
        fwd.write(&fwd_tmp)?;
        bwd.write(&bwd_tmp)?;

        // Atomic rename — if rename fails after the first but before the second,
        // the old bwd file is still consistent with the old (pre-checkpoint) state.
        // Recovery will replay from the WAL CheckpointBegin LSN in that case.
        fs::rename(&fwd_tmp, self.fwd_path()).map_err(Error::Io)?;
        fs::rename(&bwd_tmp, self.bwd_path()).map_err(Error::Io)?;

        Ok(())
    }

    /// Truncate the delta log to zero bytes and reset the in-memory counter.
    fn truncate_delta(&mut self) -> Result<()> {
        let delta = self.delta_path();
        if delta.exists() {
            fs::OpenOptions::new()
                .write(true)
                .open(&delta)
                .and_then(|f| f.set_len(0))
                .map_err(Error::Io)?;
        }
        self.next_edge_id = 0;
        Ok(())
    }

    /// Open the CSR forward file written by [`checkpoint`].
    pub fn open_fwd(&self) -> Result<CsrForward> {
        CsrForward::open(&self.fwd_path())
    }

    /// Open the CSR backward file written by [`checkpoint`].
    pub fn open_bwd(&self) -> Result<CsrBackward> {
        CsrBackward::open(&self.bwd_path())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn nid(v: u64) -> NodeId {
        NodeId(v)
    }

    const REL: RelTableId = RelTableId(0);

    #[test]
    fn test_edge_create_and_delta_roundtrip() {
        let dir = tempdir().unwrap();
        let mut store = EdgeStore::open(dir.path(), REL).unwrap();

        let e0 = store.create_edge(nid(0), REL, nid(1)).unwrap();
        let e1 = store.create_edge(nid(1), REL, nid(2)).unwrap();

        assert_eq!(e0.0, 0);
        assert_eq!(e1.0, 1);

        let records = store.read_delta().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].src.0, 0);
        assert_eq!(records[0].dst.0, 1);
        assert_eq!(records[1].src.0, 1);
        assert_eq!(records[1].dst.0, 2);
    }

    #[test]
    fn test_edge_checkpoint_builds_csr() {
        let dir = tempdir().unwrap();
        let mut store = EdgeStore::open(dir.path(), REL).unwrap();

        // Build a small graph: 4 nodes, 4 edges.
        store.create_edge(nid(0), REL, nid(1)).unwrap();
        store.create_edge(nid(0), REL, nid(2)).unwrap();
        store.create_edge(nid(1), REL, nid(3)).unwrap();
        store.create_edge(nid(2), REL, nid(3)).unwrap();

        store.checkpoint(4).unwrap();

        let fwd = store.open_fwd().unwrap();
        let bwd = store.open_bwd().unwrap();

        assert!(fwd.neighbors(0).contains(&1));
        assert!(fwd.neighbors(0).contains(&2));
        assert!(fwd.neighbors(1).contains(&3));
        assert!(bwd.predecessors(3).contains(&1));
        assert!(bwd.predecessors(3).contains(&2));
    }

    #[test]
    fn test_edge_store_persists_across_reopen() {
        let dir = tempdir().unwrap();

        // Session 1: create edges.
        {
            let mut store = EdgeStore::open(dir.path(), REL).unwrap();
            store.create_edge(nid(5), REL, nid(7)).unwrap();
            store.create_edge(nid(7), REL, nid(9)).unwrap();
        }

        // Session 2: reopen, verify delta, add more edges.
        {
            let mut store = EdgeStore::open(dir.path(), REL).unwrap();
            let records = store.read_delta().unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(records[0].src.0, 5);
            assert_eq!(records[0].dst.0, 7);

            // next_edge_id should continue from 2.
            let e2 = store.create_edge(nid(9), REL, nid(5)).unwrap();
            assert_eq!(e2.0, 2);
        }
    }

    #[test]
    fn test_edge_delta_record_codec() {
        let rec = DeltaRecord {
            src: NodeId(0x0000_0001_0000_0002),
            dst: NodeId(0x0000_0003_0000_0004),
            rel_id: RelTableId(42),
        };
        let encoded = rec.encode();
        assert_eq!(encoded.len(), DELTA_RECORD_SIZE);
        let decoded = DeltaRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
    }

    #[test]
    fn test_create_edge_wrong_rel_id_rejected() {
        let dir = tempdir().unwrap();
        let mut store = EdgeStore::open(dir.path(), REL).unwrap();
        let wrong_rel = RelTableId(99);
        let result = store.create_edge(nid(0), wrong_rel, nid(1));
        assert!(
            result.is_err(),
            "create_edge with wrong rel_id must return an error"
        );
        match result.unwrap_err() {
            sparrowdb_common::Error::InvalidArgument(_) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_checkpoint_truncates_delta_log() {
        let dir = tempdir().unwrap();
        let mut store = EdgeStore::open(dir.path(), REL).unwrap();

        store.create_edge(nid(0), REL, nid(1)).unwrap();
        store.create_edge(nid(1), REL, nid(2)).unwrap();

        store.checkpoint(4).unwrap();

        // Delta log must be empty after checkpoint.
        let records = store.read_delta().unwrap();
        assert_eq!(records.len(), 0, "delta log must be empty after checkpoint");
    }

    #[test]
    fn test_checkpoint_rejects_out_of_bounds_node_id() {
        let dir = tempdir().unwrap();
        let mut store = EdgeStore::open(dir.path(), REL).unwrap();

        // Node ID 5 is out of bounds for n_nodes=4.
        store.create_edge(nid(0), REL, nid(5)).unwrap();

        let result = store.checkpoint(4);
        assert!(
            result.is_err(),
            "checkpoint with out-of-bounds node ID must fail"
        );
        match result.unwrap_err() {
            sparrowdb_common::Error::InvalidArgument(_) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }
}
