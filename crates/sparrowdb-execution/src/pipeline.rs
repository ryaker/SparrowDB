//! Pull-based vectorized pipeline operators (Phase 1).
//!
//! # Architecture
//!
//! Each operator implements [`PipelineOperator`]: a pull-based interface where
//! the sink drives execution by calling `next_chunk()` on its child, which
//! recursively calls its child. This naturally supports LIMIT short-circuiting —
//! when the sink has enough rows it stops pulling.
//!
//! ## Phase 1 Operators
//!
//! | Operator | Input | Output |
//! |----------|-------|--------|
//! | [`ScanByLabel`] | hwm (u64) | chunks of slot numbers |
//! | [`GetNeighbors`] | child of src_slots | chunks of (src_slot, dst_slot) |
//! | [`Filter`] | child + predicate | child chunks with sel vector updated |
//!
//! # Integration
//!
//! Operators consume data from the existing storage layer without changing its
//! structure. The pipeline is an opt-in code path activated by
//! `Engine::use_chunked_pipeline`. All existing tests continue to use the
//! row-at-a-time engine unchanged.

use sparrowdb_common::Result;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::DeltaRecord;

use crate::chunk::{
    ColumnVector, DataChunk, CHUNK_CAPACITY, COL_ID_DST_SLOT, COL_ID_SLOT, COL_ID_SRC_SLOT,
};
use crate::engine::{build_delta_index, node_id_parts, DeltaIndex};

// ── PipelineOperator trait ────────────────────────────────────────────────────

/// Pull-based pipeline operator interface.
///
/// # Contract
/// - `next_chunk()` returns `Ok(Some(chunk))` while more data is available.
/// - `next_chunk()` returns `Ok(None)` when exhausted. After that, continued
///   calls must keep returning `Ok(None)`.
/// - Returned chunks always have `live_len() > 0`. Operators must internally
///   skip empty results and only surface non-empty chunks to callers.
pub trait PipelineOperator {
    /// Pull the next chunk of output. Returns `None` when exhausted.
    fn next_chunk(&mut self) -> Result<Option<DataChunk>>;

    /// Estimated output cardinality (rows) hint for pre-allocation.
    fn cardinality_hint(&self) -> Option<usize> {
        None
    }
}

// ── ScanByLabel ───────────────────────────────────────────────────────────────

/// Yields chunks of node slot numbers for a single label.
///
/// Each output chunk contains one `COL_ID_SLOT` column with at most
/// `CHUNK_CAPACITY` consecutive slot numbers starting from 0.
///
/// Phase 1 simplification: the slot list is pre-populated from `hwm` at
/// construction time. Phase 2 will replace this with streaming reads
/// directly from the label's column file.
pub struct ScanByLabel {
    /// All slot numbers for this label (0..hwm).
    slots: Vec<u64>,
    /// Cursor into `slots` — next index to emit.
    cursor: usize,
}

impl ScanByLabel {
    /// Create a `ScanByLabel` operator.
    ///
    /// `hwm` — high-water mark from `NodeStore::hwm_for_label(label_id)`.
    /// Emits slot numbers 0..hwm in order.
    pub fn new(hwm: u64) -> Self {
        let slots: Vec<u64> = (0..hwm).collect();
        ScanByLabel { slots, cursor: 0 }
    }

    /// Create from a pre-built slot list (for tests and custom scan patterns).
    pub fn from_slots(slots: Vec<u64>) -> Self {
        ScanByLabel { slots, cursor: 0 }
    }
}

impl PipelineOperator for ScanByLabel {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.cursor >= self.slots.len() {
            return Ok(None);
        }
        let end = (self.cursor + CHUNK_CAPACITY).min(self.slots.len());
        let data: Vec<u64> = self.slots[self.cursor..end].to_vec();
        self.cursor = end;
        let col = ColumnVector::from_data(COL_ID_SLOT, data);
        Ok(Some(DataChunk::from_columns(vec![col])))
    }

    fn cardinality_hint(&self) -> Option<usize> {
        Some(self.slots.len())
    }
}

// ── GetNeighbors ──────────────────────────────────────────────────────────────

/// Batch CSR offset lookup + delta merge for one relationship type.
///
/// Consumes a child that yields chunks of source slots (column at position 0,
/// `col_id = COL_ID_SLOT`). For each batch of live source slots:
///
/// 1. CSR forward lookup — zero-copy `&[u64]` slice from mmap.
/// 2. Delta-index lookup — O(1) hash lookup per slot.
/// 3. Emits `(src_slot, dst_slot)` pairs packed into output chunks.
///
/// When one input chunk expands to more than `CHUNK_CAPACITY` pairs, the output
/// is buffered and split across successive `next_chunk()` calls.
///
/// # Delta Index Key Convention (Phase 1)
///
/// The delta index is keyed by `(src_label_id, src_slot)`. In Phase 1 we build
/// the delta index from the per-rel-table delta records; because all records in
/// a single-rel-table delta log share the same `src_label_id`, we use
/// `src_label_id = 0` as a placeholder key for the lookup — see `build_delta_index`.
/// Phase 2 will supply the real `src_label_id` extracted from the NodeId encoding.
pub struct GetNeighbors<C: PipelineOperator> {
    child: C,
    csr: CsrForward,
    delta_index: DeltaIndex,
    avg_degree_hint: usize,
    /// Buffered (src_slot, dst_slot) pairs waiting to be chunked and returned.
    buf_src: Vec<u64>,
    buf_dst: Vec<u64>,
    buf_cursor: usize,
    child_done: bool,
}

impl<C: PipelineOperator> GetNeighbors<C> {
    /// Create a `GetNeighbors` operator.
    ///
    /// - `child` — upstream operator yielding src-slot chunks.
    /// - `csr` — forward CSR file for the relationship type.
    /// - `delta_records` — per-rel-table delta log (built into a hash index once).
    /// - `avg_degree_hint` — estimated average out-degree for buffer pre-allocation.
    pub fn new(
        child: C,
        csr: CsrForward,
        delta_records: &[DeltaRecord],
        avg_degree_hint: usize,
    ) -> Self {
        let delta_index = build_delta_index(delta_records);
        GetNeighbors {
            child,
            csr,
            delta_index,
            avg_degree_hint: avg_degree_hint.max(1),
            buf_src: Vec::new(),
            buf_dst: Vec::new(),
            buf_cursor: 0,
            child_done: false,
        }
    }

    /// Attempt to fill the internal buffer from the next child chunk.
    ///
    /// Returns `true` when the buffer has data; `false` when both child and
    /// buffer are exhausted.
    fn fill_buffer(&mut self) -> Result<bool> {
        loop {
            // Buffer has unconsumed data — report ready.
            if self.buf_cursor < self.buf_src.len() {
                return Ok(true);
            }

            // Buffer exhausted — reset and pull the next input chunk.
            self.buf_src.clear();
            self.buf_dst.clear();
            self.buf_cursor = 0;

            if self.child_done {
                return Ok(false);
            }

            let input = match self.child.next_chunk()? {
                Some(chunk) => chunk,
                None => {
                    self.child_done = true;
                    return Ok(false);
                }
            };

            if input.is_empty() {
                continue;
            }

            let est = input.live_len() * self.avg_degree_hint;
            self.buf_src.reserve(est);
            self.buf_dst.reserve(est);

            // Slot column is always at position 0 in ScanByLabel output.
            let slot_col = input.column(0);

            for row_idx in input.live_rows() {
                let src_slot = slot_col.data[row_idx];

                // CSR forward neighbors (zero-copy slice from mmap).
                let csr_nb = self.csr.neighbors(src_slot);
                for &dst_slot in csr_nb {
                    self.buf_src.push(src_slot);
                    self.buf_dst.push(dst_slot);
                }

                // Delta neighbors — O(1) hash lookup.
                // Phase 1: key uses src_label_id = 0 (single-rel-table convention).
                if let Some(delta_recs) = self.delta_index.get(&(0u32, src_slot)) {
                    for r in delta_recs {
                        let dst_slot = node_id_parts(r.dst.0).1;
                        self.buf_src.push(src_slot);
                        self.buf_dst.push(dst_slot);
                    }
                }
            }

            if !self.buf_src.is_empty() {
                return Ok(true);
            }
            // Input chunk produced no output — try the next one.
        }
    }
}

impl<C: PipelineOperator> PipelineOperator for GetNeighbors<C> {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if !self.fill_buffer()? {
            return Ok(None);
        }

        let start = self.buf_cursor;
        let end = (start + CHUNK_CAPACITY).min(self.buf_src.len());
        let src: Vec<u64> = self.buf_src[start..end].to_vec();
        let dst: Vec<u64> = self.buf_dst[start..end].to_vec();
        self.buf_cursor = end;

        Ok(Some(DataChunk::from_two_vecs(
            COL_ID_SRC_SLOT,
            src,
            COL_ID_DST_SLOT,
            dst,
        )))
    }
}

// ── Filter ────────────────────────────────────────────────────────────────────

/// Updates the selection vector without copying column data.
///
/// Evaluates a predicate on each live row of each incoming chunk. Failing rows
/// are removed from the selection vector — column data is never moved or copied.
/// Chunks where all rows fail are silently consumed; the operator loops to the
/// next chunk so callers always receive non-empty chunks (or `None`).
pub struct Filter<C: PipelineOperator> {
    child: C,
    predicate: Box<dyn Fn(&DataChunk, usize) -> bool + Send + Sync>,
}

impl<C: PipelineOperator> Filter<C> {
    /// Create a `Filter` operator.
    ///
    /// `predicate(chunk, row_idx)` — called with the physical (pre-selection)
    /// row index. Returns `true` to keep the row, `false` to discard it.
    pub fn new<F>(child: C, predicate: F) -> Self
    where
        F: Fn(&DataChunk, usize) -> bool + Send + Sync + 'static,
    {
        Filter {
            child,
            predicate: Box::new(predicate),
        }
    }
}

impl<C: PipelineOperator> PipelineOperator for Filter<C> {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        loop {
            let mut chunk = match self.child.next_chunk()? {
                Some(c) => c,
                None => return Ok(None),
            };

            // Evaluate the predicate for each row first (immutable borrow on chunk),
            // then apply the result bitmask via filter_sel (mutable borrow).
            // This avoids the simultaneous &chunk / &mut chunk borrow conflict.
            let keep: Vec<bool> = {
                let pred = &self.predicate;
                (0..chunk.len()).map(|i| pred(&chunk, i)).collect()
            };
            chunk.filter_sel(|i| keep[i]);

            if chunk.live_len() > 0 {
                return Ok(Some(chunk));
            }
            // All rows dead — loop to the next chunk.
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use sparrowdb_storage::csr::CsrForward;

    // ── ScanByLabel ────────────────────────────────────────────────────────

    #[test]
    fn scan_yields_all_slots() {
        let mut scan = ScanByLabel::new(5);
        let chunk = scan.next_chunk().unwrap().expect("first chunk");
        assert_eq!(chunk.live_len(), 5);
        assert_eq!(chunk.column(0).data, vec![0u64, 1, 2, 3, 4]);
        assert!(scan.next_chunk().unwrap().is_none(), "exhausted");
    }

    #[test]
    fn scan_splits_at_chunk_capacity() {
        let hwm = CHUNK_CAPACITY as u64 + 7;
        let mut scan = ScanByLabel::new(hwm);
        let c1 = scan.next_chunk().unwrap().expect("first chunk");
        assert_eq!(c1.live_len(), CHUNK_CAPACITY);
        let c2 = scan.next_chunk().unwrap().expect("second chunk");
        assert_eq!(c2.live_len(), 7);
        assert!(scan.next_chunk().unwrap().is_none());
    }

    #[test]
    fn scan_empty_returns_none() {
        let mut scan = ScanByLabel::new(0);
        assert!(scan.next_chunk().unwrap().is_none());
    }

    // ── Filter ─────────────────────────────────────────────────────────────

    #[test]
    fn filter_keeps_matching_rows() {
        // Scan 10 slots; keep only slot % 3 == 0 → rows 0, 3, 6, 9.
        let scan = ScanByLabel::new(10);
        // Predicate evaluates col(0).data[i] % 3 == 0.
        let mut filter = Filter::new(scan, |chunk, i| {
            let v = chunk.column(0).data[i];
            v % 3 == 0
        });
        let chunk = filter.next_chunk().unwrap().expect("chunk");
        assert_eq!(chunk.live_len(), 4);
        let live: Vec<usize> = chunk.live_rows().collect();
        assert_eq!(live, vec![0, 3, 6, 9]);
    }

    #[test]
    fn filter_skips_empty_chunk_pulls_next() {
        // First chunk has slots 0..CHUNK_CAPACITY (all rejected), second has 5 slots.
        let cap = CHUNK_CAPACITY as u64;
        let scan = ScanByLabel::new(cap + 5);
        let mut filter = Filter::new(scan, move |chunk, i| chunk.column(0).data[i] >= cap);
        let chunk = filter.next_chunk().unwrap().expect("second chunk");
        assert_eq!(chunk.live_len(), 5);
    }

    #[test]
    fn filter_all_rejected_returns_none() {
        let scan = ScanByLabel::new(3);
        let mut filter = Filter::new(scan, |_c, _i| false);
        assert!(filter.next_chunk().unwrap().is_none());
    }

    // ── GetNeighbors ───────────────────────────────────────────────────────

    #[test]
    fn get_neighbors_empty_csr_returns_none() {
        // Build a CsrForward with no edges (n_nodes=5, no edges).
        let csr = CsrForward::build(5, &[]);
        let scan = ScanByLabel::new(5);
        let mut gn = GetNeighbors::new(scan, csr, &[], 1);
        assert!(gn.next_chunk().unwrap().is_none());
    }

    #[test]
    fn get_neighbors_yields_correct_pairs() {
        // Build a CSR: node 0 → [1, 2], node 1 → [3], node 2 → [].
        let edges: Vec<(u64, u64)> = vec![(0, 1), (0, 2), (1, 3)];
        let csr = CsrForward::build(4, &edges);

        // Scan all 4 slots (nodes 0, 1, 2, 3).
        let scan = ScanByLabel::new(4);
        let mut gn = GetNeighbors::new(scan, csr, &[], 2);

        let chunk = gn.next_chunk().unwrap().expect("chunk");
        // Expected pairs: (0,1), (0,2), (1,3) = 3 pairs.
        assert_eq!(chunk.live_len(), 3);
        let src_col = chunk.column(0);
        let dst_col = chunk.column(1);
        assert_eq!(src_col.data, vec![0u64, 0, 1]);
        assert_eq!(dst_col.data, vec![1u64, 2, 3]);

        assert!(gn.next_chunk().unwrap().is_none());
    }

    #[test]
    fn get_neighbors_buffers_large_expansion() {
        // Build a star graph: node 0 → [1..CHUNK_CAPACITY+1]
        // This forces the output to span multiple chunks.
        let n: u64 = (CHUNK_CAPACITY as u64) + 50;
        let edges: Vec<(u64, u64)> = (1..=n).map(|d| (0u64, d)).collect();
        let csr = CsrForward::build(n + 1, &edges);

        let scan = ScanByLabel::from_slots(vec![0u64]);
        let mut gn = GetNeighbors::new(scan, csr, &[], 10);

        let c1 = gn.next_chunk().unwrap().expect("first output chunk");
        assert_eq!(c1.live_len(), CHUNK_CAPACITY);

        let c2 = gn.next_chunk().unwrap().expect("second output chunk");
        assert_eq!(c2.live_len(), 50);

        assert!(gn.next_chunk().unwrap().is_none());
    }
}
