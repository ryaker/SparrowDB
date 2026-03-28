//! Chunked columnar data representation for the vectorized execution pipeline.
//!
//! # Design
//!
//! A [`DataChunk`] is a fixed-capacity batch of tuples stored in **columnar layout**.
//! Each column is a contiguous `Vec<u64>` (SparrowDB stores all values as raw `u64`
//! internally). Filtering uses a **selection vector** instead of compacting columns,
//! avoiding memcpy and preserving alignment.
//!
//! ## Key Design Points
//! - `u64` everywhere — SparrowDB already encodes strings, ints, floats, bools as `u64`.
//! - Selection vectors instead of compaction: filtering updates `sel` (indices of live
//!   rows) without moving data. Compaction happens only at chunk boundaries.
//! - Fixed chunk capacity: default `CHUNK_CAPACITY` (2048) rows. Small enough to stay
//!   in L1/L2 cache, large enough to amortize per-chunk overhead.
//! - Null bitmaps: one bit per row per column, packed into `u64` words.

/// Default number of rows in a chunk. Tunable per-query via planner hint.
pub const CHUNK_CAPACITY: usize = 2048;

// ── NullBitmap ────────────────────────────────────────────────────────────────

/// A bit-packed null bitmap. Bit `i` is set (1) when row `i` is NULL.
///
/// Stored as a `Vec<u64>` where word `i/64` bit `i%64` represents row `i`.
#[derive(Debug, Clone, Default)]
pub struct NullBitmap {
    words: Vec<u64>,
    len: usize,
}

impl NullBitmap {
    /// Create a bitmap with `len` bits, all clear (non-null).
    pub fn with_len(len: usize) -> Self {
        let words = len.div_ceil(64);
        NullBitmap {
            words: vec![0u64; words],
            len,
        }
    }

    /// Set bit `i` (mark row `i` as null).
    #[inline]
    pub fn set_null(&mut self, i: usize) {
        self.words[i / 64] |= 1u64 << (i % 64);
    }

    /// Return true if row `i` is null.
    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        (self.words[i / 64] >> (i % 64)) & 1 == 1
    }

    /// Number of rows represented.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ── ColumnVector ──────────────────────────────────────────────────────────────

/// A single column of raw `u64` values with a null bitmap.
///
/// SparrowDB encodes every value type (int, float, bool, string pointer) as a
/// raw `u64`. Decoding happens only at the final projection stage.
#[derive(Debug, Clone)]
pub struct ColumnVector {
    /// Raw encoded values — one entry per row.
    pub data: Vec<u64>,
    /// Null bitmap — bit `i` set means `data[i]` is NULL.
    pub nulls: NullBitmap,
    /// Which property column this vector represents (matches `col_id_of` encoding).
    pub col_id: u32,
}

impl ColumnVector {
    /// Create a zero-filled column for `len` rows with no nulls.
    pub fn zeroed(col_id: u32, len: usize) -> Self {
        ColumnVector {
            data: vec![0u64; len],
            nulls: NullBitmap::with_len(len),
            col_id,
        }
    }

    /// Create a column from a pre-populated data vector.
    pub fn from_data(col_id: u32, data: Vec<u64>) -> Self {
        let len = data.len();
        ColumnVector {
            data,
            nulls: NullBitmap::with_len(len),
            col_id,
        }
    }

    /// Number of rows in this column.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// ── DataChunk ─────────────────────────────────────────────────────────────────

/// A batch of tuples in columnar layout.
///
/// Rows 0..`len` hold data; `sel` narrows which rows are "live" after filtering.
/// When `sel` is `None`, all rows 0..`len` are live (avoids allocation on non-
/// filtering operators).
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// Per-column vectors. All columns must have the same length (`len`).
    columns: Vec<ColumnVector>,
    /// Number of valid rows (may be ≤ `CHUNK_CAPACITY`).
    len: usize,
    /// Selection vector: indices of "live" rows after filtering.
    /// `None` means all rows 0..len are live.
    sel: Option<Vec<u32>>,
}

impl DataChunk {
    /// Create an empty chunk with no columns and no rows.
    pub fn empty() -> Self {
        DataChunk {
            columns: Vec::new(),
            len: 0,
            sel: None,
        }
    }

    /// Create a chunk from a set of column vectors.
    ///
    /// # Panics
    /// Panics in debug mode if columns have different lengths.
    pub fn from_columns(columns: Vec<ColumnVector>) -> Self {
        let len = columns.first().map(|c| c.len()).unwrap_or(0);
        debug_assert!(
            columns.iter().all(|c| c.len() == len),
            "all columns must have the same length"
        );
        DataChunk {
            columns,
            len,
            sel: None,
        }
    }

    /// Create a chunk with exactly two u64 columns (typically src_slot, dst_slot).
    ///
    /// Convenience constructor used by `GetNeighbors`.
    pub fn from_two_vecs(col0_id: u32, col0: Vec<u64>, col1_id: u32, col1: Vec<u64>) -> Self {
        debug_assert_eq!(col0.len(), col1.len());
        let len = col0.len();
        DataChunk {
            columns: vec![
                ColumnVector::from_data(col0_id, col0),
                ColumnVector::from_data(col1_id, col1),
            ],
            len,
            sel: None,
        }
    }

    /// Number of physical rows (before selection).
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of "live" rows after applying the selection vector.
    pub fn live_len(&self) -> usize {
        match &self.sel {
            None => self.len,
            Some(sel) => sel.len(),
        }
    }

    /// Get a reference to a column by position.
    pub fn column(&self, pos: usize) -> &ColumnVector {
        &self.columns[pos]
    }

    /// Get a mutable reference to a column by position.
    pub fn column_mut(&mut self, pos: usize) -> &mut ColumnVector {
        &mut self.columns[pos]
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Find a column by `col_id`. Returns `None` if not present.
    pub fn find_column(&self, col_id: u32) -> Option<&ColumnVector> {
        self.columns.iter().find(|c| c.col_id == col_id)
    }

    /// Add a column to the chunk.
    ///
    /// # Panics
    /// Panics in debug mode if the new column length differs from `self.len`.
    pub fn push_column(&mut self, col: ColumnVector) {
        debug_assert!(
            self.columns.is_empty() || col.len() == self.len,
            "column length mismatch: expected {}, got {}",
            self.len,
            col.len()
        );
        if self.columns.is_empty() {
            self.len = col.len();
        }
        self.columns.push(col);
    }

    /// Return the current selection vector, or `None` if all rows are live.
    pub fn sel(&self) -> Option<&[u32]> {
        self.sel.as_deref()
    }

    /// Apply a predicate to each live row and update the selection vector.
    ///
    /// `pred` receives the row index `i` and returns `true` if the row should
    /// remain live. Rows that return `false` are removed from `sel`.
    ///
    /// This is the core **Filter** operation: it never copies column data,
    /// only updates the selection vector. O(live_len) time, O(1) extra memory
    /// (modifies sel in place).
    pub fn filter_sel<F>(&mut self, mut pred: F)
    where
        F: FnMut(usize) -> bool,
    {
        match self.sel.take() {
            None => {
                // All rows were live — build a new selection vector keeping only
                // rows that pass the predicate.
                let mut new_sel = Vec::with_capacity(self.len);
                for i in 0..self.len {
                    if pred(i) {
                        new_sel.push(i as u32);
                    }
                }
                // Optimization: if all rows passed, keep sel=None (no allocation needed).
                if new_sel.len() == self.len {
                    self.sel = None;
                } else {
                    self.sel = Some(new_sel);
                }
            }
            Some(old_sel) => {
                // Narrow the existing selection vector.
                let new_sel: Vec<u32> = old_sel.into_iter().filter(|&i| pred(i as usize)).collect();
                if new_sel.len() == self.len {
                    self.sel = None;
                } else {
                    self.sel = Some(new_sel);
                }
            }
        }
    }

    /// Materialize this chunk into an iterator over live row indices.
    ///
    /// When `sel` is `None`, yields `0..len`. When `sel` is `Some(sel)`,
    /// yields indices from the selection vector.
    pub fn live_rows(&self) -> LiveRows<'_> {
        LiveRows {
            chunk: self,
            pos: 0,
        }
    }

    /// Consume the chunk and extract its column vectors.
    pub fn into_columns(self) -> Vec<ColumnVector> {
        self.columns
    }
}

/// Iterator over live row indices of a [`DataChunk`].
pub struct LiveRows<'a> {
    chunk: &'a DataChunk,
    pos: usize,
}

impl<'a> Iterator for LiveRows<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.chunk.sel {
            None => {
                if self.pos < self.chunk.len {
                    let i = self.pos;
                    self.pos += 1;
                    Some(i)
                } else {
                    None
                }
            }
            Some(sel) => {
                if self.pos < sel.len() {
                    let i = sel[self.pos] as usize;
                    self.pos += 1;
                    Some(i)
                } else {
                    None
                }
            }
        }
    }
}

// ── Column-ID constants used by pipeline operators ───────────────────────────

/// Synthetic column-id used for "slot" (node position within a label's column file).
/// Chosen to be in a range that won't collide with real property col_ids
/// (which are `col_id_of(name)` = djb2 hash, always ≥ 1).
pub const COL_ID_SLOT: u32 = 0;

/// Synthetic column-id for "source slot" in `GetNeighbors` output.
pub const COL_ID_SRC_SLOT: u32 = u32::MAX - 1;

/// Synthetic column-id for "destination slot" in `GetNeighbors` output.
pub const COL_ID_DST_SLOT: u32 = u32::MAX - 2;

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_bitmap_set_and_query() {
        let mut bm = NullBitmap::with_len(130);
        assert!(!bm.is_null(0));
        assert!(!bm.is_null(63));
        assert!(!bm.is_null(64));
        assert!(!bm.is_null(129));

        bm.set_null(0);
        bm.set_null(63);
        bm.set_null(64);
        bm.set_null(129);

        assert!(bm.is_null(0));
        assert!(bm.is_null(63));
        assert!(bm.is_null(64));
        assert!(bm.is_null(129));
        assert!(!bm.is_null(1));
        assert!(!bm.is_null(65));
    }

    #[test]
    fn data_chunk_filter_reduces_sel_vector() {
        // Build a chunk with one column containing values 0..10.
        let data: Vec<u64> = (0u64..10).collect();
        let col = ColumnVector::from_data(COL_ID_SLOT, data);
        let mut chunk = DataChunk::from_columns(vec![col]);

        assert_eq!(chunk.live_len(), 10);
        assert!(
            chunk.sel().is_none(),
            "sel should be None before any filter"
        );

        // Filter: keep only even rows (indices 0, 2, 4, 6, 8).
        // Pre-compute keep vec to avoid simultaneous &chunk / &mut chunk borrow.
        let keep: Vec<bool> = (0..chunk.len())
            .map(|i| chunk.column(0).data[i] % 2 == 0)
            .collect();
        chunk.filter_sel(|i| keep[i]);

        assert_eq!(chunk.live_len(), 5);
        let sel = chunk.sel().expect("sel should be Some after filtering");
        assert_eq!(sel, &[0u32, 2, 4, 6, 8]);

        // Apply a second filter: keep only values > 4 (i.e. indices 6, 8).
        let keep2: Vec<bool> = (0..chunk.len())
            .map(|i| chunk.column(0).data[i] > 4)
            .collect();
        chunk.filter_sel(|i| keep2[i]);
        assert_eq!(chunk.live_len(), 2);
        let sel2 = chunk.sel().unwrap();
        assert_eq!(sel2, &[6u32, 8]);
    }

    #[test]
    fn data_chunk_filter_all_pass_keeps_sel_none() {
        let data: Vec<u64> = vec![10, 20, 30];
        let col = ColumnVector::from_data(COL_ID_SLOT, data);
        let mut chunk = DataChunk::from_columns(vec![col]);
        // Filter that keeps all rows.
        chunk.filter_sel(|_| true);
        assert!(
            chunk.sel().is_none(),
            "sel should remain None when all rows pass"
        );
    }

    #[test]
    fn live_rows_iteration() {
        let data: Vec<u64> = (0u64..5).collect();
        let col = ColumnVector::from_data(COL_ID_SLOT, data);
        let mut chunk = DataChunk::from_columns(vec![col]);
        // Keep only odd indices (1, 3).
        chunk.filter_sel(|i| i % 2 == 1);
        let live: Vec<usize> = chunk.live_rows().collect();
        assert_eq!(live, vec![1, 3]);
    }

    #[test]
    fn chunk_capacity_constant() {
        assert_eq!(CHUNK_CAPACITY, 2048);
    }
}
