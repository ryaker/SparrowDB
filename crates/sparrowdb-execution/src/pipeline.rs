//! Pull-based vectorized pipeline operators (Phase 1 + Phase 2).
//!
//! # Architecture
//!
//! Each operator implements [`PipelineOperator`]: a pull-based interface where
//! the sink drives execution by calling `next_chunk()` on its child, which
//! recursively calls its child. This naturally supports LIMIT short-circuiting —
//! when the sink has enough rows it stops pulling.
//!
//! ## Operators
//!
//! | Operator | Input | Output |
//! |----------|-------|--------|
//! | [`ScanByLabel`] | hwm (u64) | chunks of slot numbers |
//! | [`GetNeighbors`] | child of src_slots | chunks of (src_slot, dst_slot) |
//! | [`Filter`] | child + predicate | child chunks with sel vector updated |
//! | [`ReadNodeProps`] | child chunk + NodeStore | child chunk + property columns |
//!
//! # Integration
//!
//! Operators consume data from the existing storage layer without changing its
//! structure. The pipeline is an opt-in code path activated by
//! `Engine::use_chunked_pipeline`. All existing tests continue to use the
//! row-at-a-time engine unchanged.

use std::sync::Arc;

use sparrowdb_common::Result;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::DeltaRecord;
use sparrowdb_storage::node_store::NodeStore;

use crate::chunk::{
    ColumnVector, DataChunk, NullBitmap, CHUNK_CAPACITY, COL_ID_DST_SLOT, COL_ID_SLOT,
    COL_ID_SRC_SLOT,
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
/// `CHUNK_CAPACITY` consecutive slot numbers.
///
/// Phase 2: uses a cursor-based approach (`next_slot`/`end_slot`) rather than
/// pre-allocating the entire `Vec<u64>` at construction time.  This reduces
/// startup allocation from O(hwm) to O(1) — critical for large labels.
pub struct ScanByLabel {
    /// Next slot number to emit.
    next_slot: u64,
    /// One past the last slot to emit (exclusive upper bound).
    end_slot: u64,
    /// Optional pre-built slot list, used only by `from_slots` (tests / custom
    /// scan patterns).  When `Some`, the cursor pair is unused.
    slots_override: Option<Vec<u64>>,
    /// Cursor into `slots_override` when `Some`.
    override_cursor: usize,
}

impl ScanByLabel {
    /// Create a `ScanByLabel` operator.
    ///
    /// `hwm` — high-water mark from `NodeStore::hwm_for_label(label_id)`.
    /// Emits slot numbers 0..hwm in order, allocating at most one chunk at a time.
    pub fn new(hwm: u64) -> Self {
        ScanByLabel {
            next_slot: 0,
            end_slot: hwm,
            slots_override: None,
            override_cursor: 0,
        }
    }

    /// Create from a pre-built slot list (for tests and custom scan patterns).
    ///
    /// Retained for backward compatibility with existing unit tests and
    /// special scan patterns.  Prefer [`ScanByLabel::new`] for production use.
    pub fn from_slots(slots: Vec<u64>) -> Self {
        ScanByLabel {
            next_slot: 0,
            end_slot: 0,
            slots_override: Some(slots),
            override_cursor: 0,
        }
    }
}

impl PipelineOperator for ScanByLabel {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        // from_slots path (tests / custom).
        if let Some(ref slots) = self.slots_override {
            if self.override_cursor >= slots.len() {
                return Ok(None);
            }
            let end = (self.override_cursor + CHUNK_CAPACITY).min(slots.len());
            let data: Vec<u64> = slots[self.override_cursor..end].to_vec();
            self.override_cursor = end;
            let col = ColumnVector::from_data(COL_ID_SLOT, data);
            return Ok(Some(DataChunk::from_columns(vec![col])));
        }

        // Cursor-based path (no startup allocation).
        if self.next_slot >= self.end_slot {
            return Ok(None);
        }
        let chunk_end = (self.next_slot + CHUNK_CAPACITY as u64).min(self.end_slot);
        let data: Vec<u64> = (self.next_slot..chunk_end).collect();
        self.next_slot = chunk_end;
        let col = ColumnVector::from_data(COL_ID_SLOT, data);
        Ok(Some(DataChunk::from_columns(vec![col])))
    }

    fn cardinality_hint(&self) -> Option<usize> {
        if let Some(ref s) = self.slots_override {
            return Some(s.len());
        }
        Some((self.end_slot - self.next_slot) as usize)
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
/// # Delta Index Key Convention
///
/// The delta index is keyed by `(src_label_id, src_slot)` matching the encoding
/// produced by `build_delta_index`. `GetNeighbors` is constructed with the
/// `src_label_id` of the scanned label so lookups use the correct key.
pub struct GetNeighbors<C: PipelineOperator> {
    child: C,
    csr: CsrForward,
    delta_index: DeltaIndex,
    /// Label ID of the source nodes — used as the high key in delta-index lookups.
    src_label_id: u32,
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
    /// - `src_label_id` — label ID of the source nodes (high bits of NodeId).
    /// - `avg_degree_hint` — estimated average out-degree for buffer pre-allocation.
    pub fn new(
        child: C,
        csr: CsrForward,
        delta_records: &[DeltaRecord],
        src_label_id: u32,
        avg_degree_hint: usize,
    ) -> Self {
        let delta_index = build_delta_index(delta_records);
        GetNeighbors {
            child,
            csr,
            delta_index,
            src_label_id,
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

                // Delta neighbors — O(1) hash lookup keyed by (src_label_id, src_slot).
                if let Some(delta_recs) = self.delta_index.get(&(self.src_label_id, src_slot)) {
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

/// Predicate function used by [`Filter`]: given a chunk and a physical row
/// index, returns `true` to keep the row.
type FilterPredicate = Box<dyn Fn(&DataChunk, usize) -> bool + Send + Sync>;

/// Updates the selection vector without copying column data.
///
/// Evaluates a predicate on each live row of each incoming chunk. Failing rows
/// are removed from the selection vector — column data is never moved or copied.
/// Chunks where all rows fail are silently consumed; the operator loops to the
/// next chunk so callers always receive non-empty chunks (or `None`).
pub struct Filter<C: PipelineOperator> {
    child: C,
    predicate: FilterPredicate,
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

// ── ReadNodeProps ─────────────────────────────────────────────────────────────

/// Appends property columns to a chunk for live (selection-vector-passing) rows
/// only.
///
/// Reads one batch of node properties per `next_chunk()` call using
/// [`NodeStore::batch_read_node_props_nullable`], building a [`NullBitmap`] from
/// the `Option<u64>` results and appending one [`ColumnVector`] per `col_id` to
/// the chunk.
///
/// Rows that are already dead (not in the selection vector) are **never read** —
/// this enforces the late-materialization principle: no I/O for filtered rows.
pub struct ReadNodeProps<C: PipelineOperator> {
    child: C,
    store: Arc<NodeStore>,
    label_id: u32,
    /// Which column in the child chunk holds slot numbers (typically `COL_ID_SLOT`
    /// for src nodes or `COL_ID_DST_SLOT` for dst nodes).
    slot_col_id: u32,
    /// Property column IDs to read from storage.
    col_ids: Vec<u32>,
}

impl<C: PipelineOperator> ReadNodeProps<C> {
    /// Create a `ReadNodeProps` operator.
    ///
    /// - `child`       — upstream operator yielding chunks that contain a slot column.
    /// - `store`       — shared reference to the node store.
    /// - `label_id`    — label whose column files to read.
    /// - `slot_col_id` — column ID in the child chunk that holds slot numbers.
    /// - `col_ids`     — property column IDs to append to each output chunk.
    pub fn new(
        child: C,
        store: Arc<NodeStore>,
        label_id: u32,
        slot_col_id: u32,
        col_ids: Vec<u32>,
    ) -> Self {
        ReadNodeProps {
            child,
            store,
            label_id,
            slot_col_id,
            col_ids,
        }
    }
}

impl<C: PipelineOperator> PipelineOperator for ReadNodeProps<C> {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        loop {
            let mut chunk = match self.child.next_chunk()? {
                Some(c) => c,
                None => return Ok(None),
            };

            if chunk.is_empty() {
                continue;
            }

            // If no property columns requested, pass through unchanged.
            if self.col_ids.is_empty() {
                return Ok(Some(chunk));
            }

            // Collect live slots only — no I/O for dead rows.
            let slot_col = chunk
                .find_column(self.slot_col_id)
                .expect("slot column not found in ReadNodeProps input");
            let live_slots: Vec<u32> = chunk.live_rows().map(|i| slot_col.data[i] as u32).collect();

            // No live rows — skip I/O, return the chunk as-is (caller will skip
            // it since live_len() == 0).
            if live_slots.is_empty() {
                return Ok(Some(chunk));
            }

            // Batch-read with null semantics.
            // raw[i][j] = Option<u64> for live_slots[i], col_ids[j].
            let raw = self.store.batch_read_node_props_nullable(
                self.label_id,
                &live_slots,
                &self.col_ids,
            )?;

            // Build one ColumnVector per col_id, full chunk length with nulls for
            // dead rows.
            let n = chunk.len(); // physical (pre-selection) length
            for (col_idx, &col_id) in self.col_ids.iter().enumerate() {
                let mut data = vec![0u64; n];
                let mut nulls = NullBitmap::with_len(n);
                // Mark all rows null initially; we'll fill in live rows below.
                for i in 0..n {
                    nulls.set_null(i);
                }

                // Fill live rows from the batch result.
                for (live_idx, phys_row) in chunk.live_rows().enumerate() {
                    match raw[live_idx][col_idx] {
                        Some(v) => {
                            data[phys_row] = v;
                            // Clear null bit (present) — NullBitmap uses set=null,
                            // clear=present, so we rebuild without the null bit.
                        }
                        None => {
                            // Already null by default; leave data[phys_row] = 0.
                        }
                    }
                }

                // Rebuild null bitmap correctly: clear bits for present rows.
                let mut corrected_nulls = NullBitmap::with_len(n);
                for (live_idx, phys_row) in chunk.live_rows().enumerate() {
                    if raw[live_idx][col_idx].is_none() {
                        corrected_nulls.set_null(phys_row);
                    }
                    // present rows leave the bit clear (default)
                }

                let col = ColumnVector {
                    data,
                    nulls: corrected_nulls,
                    col_id,
                };
                chunk.push_column(col);
            }

            return Ok(Some(chunk));
        }
    }
}

// ── ChunkPredicate ────────────────────────────────────────────────────────────

/// Narrow predicate representation for the vectorized pipeline (Phase 2).
///
/// Covers only simple conjunctive property predicates that can be compiled
/// directly from a Cypher `WHERE` clause without a full expression evaluator.
/// Unsupported `WHERE` shapes (CONTAINS, function calls, subqueries, cross-
/// variable predicates) fall back to the row-at-a-time engine.
///
/// All comparisons are on the raw `u64` storage encoding.  NULL handling:
/// `IsNull` matches rows where the column's null bitmap bit is set; all
/// comparison variants (`Eq`, `Lt`, etc.) automatically fail for null rows.
#[derive(Debug, Clone)]
pub enum ChunkPredicate {
    /// Equal: `col_id = rhs_raw`.
    Eq { col_id: u32, rhs_raw: u64 },
    /// Not equal: `col_id <> rhs_raw`.
    Ne { col_id: u32, rhs_raw: u64 },
    /// Greater-than: `col_id > rhs_raw` (unsigned comparison on raw bits).
    Gt { col_id: u32, rhs_raw: u64 },
    /// Greater-than-or-equal: `col_id >= rhs_raw`.
    Ge { col_id: u32, rhs_raw: u64 },
    /// Less-than: `col_id < rhs_raw`.
    Lt { col_id: u32, rhs_raw: u64 },
    /// Less-than-or-equal: `col_id <= rhs_raw`.
    Le { col_id: u32, rhs_raw: u64 },
    /// Is-null: matches rows where the column's null-bitmap bit is set.
    IsNull { col_id: u32 },
    /// Is-not-null: matches rows where the column's null-bitmap bit is clear.
    IsNotNull { col_id: u32 },
    /// Conjunction of child predicates (all must pass).
    And(Vec<ChunkPredicate>),
}

impl ChunkPredicate {
    /// Evaluate this predicate for a single physical row index.
    ///
    /// Returns `true` if the row should remain live.
    pub fn eval(&self, chunk: &DataChunk, row_idx: usize) -> bool {
        match self {
            ChunkPredicate::Eq { col_id, rhs_raw } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx) && col.data[row_idx] == *rhs_raw
                } else {
                    false
                }
            }
            ChunkPredicate::Ne { col_id, rhs_raw } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx) && col.data[row_idx] != *rhs_raw
                } else {
                    false
                }
            }
            ChunkPredicate::Gt { col_id, rhs_raw } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx) && col.data[row_idx] > *rhs_raw
                } else {
                    false
                }
            }
            ChunkPredicate::Ge { col_id, rhs_raw } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx) && col.data[row_idx] >= *rhs_raw
                } else {
                    false
                }
            }
            ChunkPredicate::Lt { col_id, rhs_raw } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx) && col.data[row_idx] < *rhs_raw
                } else {
                    false
                }
            }
            ChunkPredicate::Le { col_id, rhs_raw } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx) && col.data[row_idx] <= *rhs_raw
                } else {
                    false
                }
            }
            ChunkPredicate::IsNull { col_id } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    col.nulls.is_null(row_idx)
                } else {
                    // Column not present → property is absent → treat as null.
                    true
                }
            }
            ChunkPredicate::IsNotNull { col_id } => {
                if let Some(col) = chunk.find_column(*col_id) {
                    !col.nulls.is_null(row_idx)
                } else {
                    false
                }
            }
            ChunkPredicate::And(children) => children.iter().all(|c| c.eval(chunk, row_idx)),
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
        let mut gn = GetNeighbors::new(scan, csr, &[], 0, 1);
        assert!(gn.next_chunk().unwrap().is_none());
    }

    #[test]
    fn get_neighbors_yields_correct_pairs() {
        // Build a CSR: node 0 → [1, 2], node 1 → [3], node 2 → [].
        let edges: Vec<(u64, u64)> = vec![(0, 1), (0, 2), (1, 3)];
        let csr = CsrForward::build(4, &edges);

        // Scan all 4 slots (nodes 0, 1, 2, 3).
        let scan = ScanByLabel::new(4);
        let mut gn = GetNeighbors::new(scan, csr, &[], 0, 2);

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
        let mut gn = GetNeighbors::new(scan, csr, &[], 0, 10);

        let c1 = gn.next_chunk().unwrap().expect("first output chunk");
        assert_eq!(c1.live_len(), CHUNK_CAPACITY);

        let c2 = gn.next_chunk().unwrap().expect("second output chunk");
        assert_eq!(c2.live_len(), 50);

        assert!(gn.next_chunk().unwrap().is_none());
    }
}
