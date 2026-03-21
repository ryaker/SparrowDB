//! Spill-to-disk sort for ORDER BY on large result sets.
//!
//! Implements a k-way external merge sort:
//! - Rows are buffered in memory up to `row_threshold` rows OR `byte_threshold` bytes.
//! - When either threshold is exceeded, the in-memory buffer is sorted and written to
//!   a `NamedTempFile` as a sorted run.
//! - `finish()` merges all sorted runs (plus any remaining in-memory rows) using a
//!   binary-heap-based k-way merge, returning a single sorted iterator.
//!
//! Row type `T` must implement `serde::Serialize + serde::de::DeserializeOwned + Ord`.
//!
//! SPA-113

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use serde::{de::DeserializeOwned, Serialize};
use sparrowdb_common::{Error, Result};
use tempfile::NamedTempFile;

/// Default in-memory row threshold before spilling.
pub const DEFAULT_ROW_THRESHOLD: usize = 100_000;

/// Default in-memory byte threshold (64 MiB) before spilling.
pub const DEFAULT_BYTE_THRESHOLD: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// SpillingSorter
// ---------------------------------------------------------------------------

/// A sort operator that buffers rows in memory and spills sorted runs to disk
/// when either `row_threshold` or `byte_threshold` is exceeded.
pub struct SpillingSorter<T> {
    /// In-memory row buffer.
    buffer: Vec<T>,
    /// Sorted run temp files (each file holds a contiguous sorted sequence).
    runs: Vec<NamedTempFile>,
    /// Maximum number of rows to hold in memory before spilling.
    row_threshold: usize,
    /// Maximum estimated in-memory bytes before spilling.
    byte_threshold: usize,
    /// Rough estimate of current in-memory bytes.
    byte_estimate: usize,
    /// Bytes per row estimate (seed value; refined as rows arrive).
    bytes_per_row: usize,
}

impl<T> SpillingSorter<T>
where
    T: Serialize + DeserializeOwned + Ord + Clone,
{
    /// Create a new `SpillingSorter` with default thresholds.
    pub fn new() -> Self {
        SpillingSorter::with_thresholds(DEFAULT_ROW_THRESHOLD, DEFAULT_BYTE_THRESHOLD)
    }

    /// Create with explicit thresholds (useful for testing spill behaviour with
    /// a small threshold).
    pub fn with_thresholds(row_threshold: usize, byte_threshold: usize) -> Self {
        SpillingSorter {
            buffer: Vec::new(),
            runs: Vec::new(),
            row_threshold,
            byte_threshold,
            byte_estimate: 0,
            bytes_per_row: 64, // initial guess
        }
    }

    /// Push a single row.  Spills the in-memory buffer if a threshold is
    /// exceeded after the push.
    pub fn push(&mut self, row: T) -> Result<()> {
        self.byte_estimate += self.bytes_per_row;
        self.buffer.push(row);

        if self.buffer.len() >= self.row_threshold || self.byte_estimate >= self.byte_threshold {
            self.spill()?;
        }
        Ok(())
    }

    /// Sort and merge all data, returning a sorted iterator over every row
    /// that was pushed.
    pub fn finish(mut self) -> Result<impl Iterator<Item = T>> {
        if self.runs.is_empty() {
            // No spill happened — sort in memory and return a plain iterator.
            self.buffer.sort();
            return Ok(SortedOutput::Memory(self.buffer.into_iter()));
        }

        // Spill any remaining in-memory rows as a final sorted run.
        if !self.buffer.is_empty() {
            self.spill()?;
        }

        // K-way merge using a min-heap.
        let mut readers: Vec<RunReader<T>> = self
            .runs
            .into_iter()
            .map(RunReader::new)
            .collect::<Result<Vec<_>>>()?;

        // Seed the heap.
        let mut heap: BinaryHeap<HeapEntry<T>> = BinaryHeap::new();
        for (idx, reader) in readers.iter_mut().enumerate() {
            if let Some(row) = reader.next_row()? {
                heap.push(HeapEntry {
                    row: Reverse(row),
                    run_idx: idx,
                });
            }
        }

        Ok(SortedOutput::Merge(MergeIter {
            heap,
            readers,
            exhausted: false,
        }))
    }

    // ── Private helpers ───────────────────────────────────────────────────

    /// Sort the in-memory buffer and write it to a new temp file as a run.
    fn spill(&mut self) -> Result<()> {
        self.buffer.sort();

        // Refine the bytes-per-row estimate from actual serialized size.
        // We serialize a sample (the first row) to get a real estimate.
        if let Some(first) = self.buffer.first() {
            if let Ok(encoded) = bincode::serialize(first) {
                // length-prefix (8 bytes varint-style) + payload
                self.bytes_per_row = encoded.len() + 8;
            }
        }

        let mut tmp = NamedTempFile::new().map_err(Error::Io)?;
        {
            let mut writer = BufWriter::new(tmp.as_file_mut());
            for row in &self.buffer {
                write_row(&mut writer, row)?;
            }
            writer.flush().map_err(Error::Io)?;
        }

        self.runs.push(tmp);
        self.buffer.clear();
        self.byte_estimate = 0;
        Ok(())
    }
}

impl<T> Default for SpillingSorter<T>
where
    T: Serialize + DeserializeOwned + Ord + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Row serialisation helpers
// ---------------------------------------------------------------------------

/// Write a length-prefixed bincode frame.
fn write_row<W: Write, T: Serialize>(writer: &mut W, row: &T) -> Result<()> {
    let encoded = bincode::serialize(row)
        .map_err(|e| Error::InvalidArgument(format!("bincode encode: {e}")))?;
    let len = encoded.len() as u64;
    writer.write_all(&len.to_le_bytes()).map_err(Error::Io)?;
    writer.write_all(&encoded).map_err(Error::Io)?;
    Ok(())
}

/// Read the next length-prefixed bincode frame, returning `None` on EOF.
fn read_row<R: Read, T: DeserializeOwned>(reader: &mut R) -> Result<Option<T>> {
    let mut len_buf = [0u8; 8];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    }
    let len = u64::from_le_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    reader.read_exact(&mut data).map_err(Error::Io)?;
    let row: T = bincode::deserialize(&data)
        .map_err(|e| Error::Corruption(format!("bincode decode: {e}")))?;
    Ok(Some(row))
}

// ---------------------------------------------------------------------------
// RunReader — reads rows from a single sorted run file
// ---------------------------------------------------------------------------

struct RunReader<T> {
    reader: BufReader<std::fs::File>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned> RunReader<T> {
    fn new(mut tmp: NamedTempFile) -> Result<Self> {
        tmp.as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(Error::Io)?;
        // Convert NamedTempFile → underlying File (keeps it alive via BufReader).
        let file = tmp.into_file();
        Ok(RunReader {
            reader: BufReader::new(file),
            _marker: std::marker::PhantomData,
        })
    }

    fn next_row(&mut self) -> Result<Option<T>> {
        read_row(&mut self.reader)
    }
}

// ---------------------------------------------------------------------------
// HeapEntry — wrapper for the k-way merge min-heap
// ---------------------------------------------------------------------------

struct HeapEntry<T: Ord> {
    row: Reverse<T>,
    run_idx: usize,
}

impl<T: Ord> PartialEq for HeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}
impl<T: Ord> Eq for HeapEntry<T> {}
impl<T: Ord> PartialOrd for HeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Ord> Ord for HeapEntry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; Reverse makes it a min-heap on T.
        self.row.cmp(&other.row)
    }
}

// ---------------------------------------------------------------------------
// SortedOutput — unifies the two output paths
// ---------------------------------------------------------------------------

enum SortedOutput<T: Ord + DeserializeOwned> {
    Memory(std::vec::IntoIter<T>),
    Merge(MergeIter<T>),
}

impl<T: Ord + DeserializeOwned> Iterator for SortedOutput<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match self {
            SortedOutput::Memory(it) => it.next(),
            SortedOutput::Merge(m) => m.next(),
        }
    }
}

// ---------------------------------------------------------------------------
// MergeIter — k-way merge iterator
// ---------------------------------------------------------------------------

struct MergeIter<T: Ord + DeserializeOwned> {
    heap: BinaryHeap<HeapEntry<T>>,
    readers: Vec<RunReader<T>>,
    exhausted: bool,
}

impl<T: Ord + DeserializeOwned> Iterator for MergeIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.exhausted {
            return None;
        }
        let entry = self.heap.pop()?;
        let row = entry.row.0;
        let idx = entry.run_idx;

        // Refill from the same run.
        match self.readers[idx].next_row() {
            Ok(Some(next_row)) => {
                self.heap.push(HeapEntry {
                    row: Reverse(next_row),
                    run_idx: idx,
                });
            }
            Ok(None) => { /* run exhausted */ }
            Err(_) => {
                self.exhausted = true;
            }
        }

        Some(row)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Sort 1,000 rows that fit entirely in memory.
    #[test]
    fn sort_fits_in_memory() {
        let mut sorter: SpillingSorter<i64> = SpillingSorter::new();
        // Push in reverse order.
        for i in (0i64..1_000).rev() {
            sorter.push(i).unwrap();
        }
        let result: Vec<i64> = sorter.finish().unwrap().collect();
        let expected: Vec<i64> = (0..1_000).collect();
        assert_eq!(result, expected);
    }

    /// Sort more than the row threshold, triggering at least one spill.
    #[test]
    fn sort_spills_to_disk() {
        // Use a tiny threshold so we definitely spill.
        let mut sorter: SpillingSorter<i64> = SpillingSorter::with_thresholds(100, usize::MAX);

        let n = 500i64;
        for i in (0..n).rev() {
            sorter.push(i).unwrap();
        }
        // Verify that spill files were actually created.
        assert!(!sorter.runs.is_empty(), "expected at least one spill run");

        let result: Vec<i64> = sorter.finish().unwrap().collect();
        let expected: Vec<i64> = (0..n).collect();
        assert_eq!(result, expected);
    }

    /// Empty input produces empty output.
    #[test]
    fn sort_empty() {
        let sorter: SpillingSorter<i64> = SpillingSorter::new();
        let result: Vec<i64> = sorter.finish().unwrap().collect();
        assert!(result.is_empty());
    }

    /// Multi-column sort: tuples (key, value) sorted by key.
    #[test]
    fn sort_tuples() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        struct Row {
            key: i64,
            val: String,
        }

        let mut sorter: SpillingSorter<Row> = SpillingSorter::with_thresholds(3, usize::MAX);

        let rows = vec![
            Row {
                key: 3,
                val: "c".into(),
            },
            Row {
                key: 1,
                val: "a".into(),
            },
            Row {
                key: 2,
                val: "b".into(),
            },
            Row {
                key: 5,
                val: "e".into(),
            },
            Row {
                key: 4,
                val: "d".into(),
            },
        ];
        for r in rows {
            sorter.push(r).unwrap();
        }
        let result: Vec<Row> = sorter.finish().unwrap().collect();
        assert_eq!(result[0].key, 1);
        assert_eq!(result[4].key, 5);
    }
}
