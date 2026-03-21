//! WAL writer — append records to segment files with fsync and rotation.
//!
//! ## Segment naming
//! `wal/segment-{:020}.wal`
//!
//! ## Rotation
//! Each segment is at most 64 MiB. If a record would overflow the active
//! segment, the writer pads remaining bytes with zeros and rotates.
//!
//! ## 6-step commit sequence
//!
//! The commit sequence is invoked by callers; the writer exposes `append` only.
//! The caller is responsible for ordering:
//!   1. Write BEGIN record
//!   2. Write WRITE records (one per dirty page)
//!   3. Write COMMIT record
//!   4. fsync WAL file          ← `WalWriter::fsync()`
//!   5. Apply pages to storage  ← caller's responsibility
//!   6. Advance last_applied_lsn in metapage  ← caller's responsibility
//!
//! The writer provides `append`, `fsync`, and `commit_transaction` helpers.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use sparrowdb_common::{Lsn, Result, TxnId};

use super::codec::{WalPayload, WalRecord, WalRecordKind};

/// 64 MiB per segment.
pub const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Segment file name for a given segment number.
pub fn segment_path(wal_dir: &std::path::Path, seg_no: u64) -> PathBuf {
    wal_dir.join(format!("segment-{:020}.wal", seg_no))
}

/// WAL writer — single-writer, append-only.
pub struct WalWriter {
    wal_dir: PathBuf,
    /// Current segment file handle.
    file: File,
    /// Current segment number.
    seg_no: u64,
    /// Bytes written to the current segment.
    seg_offset: u64,
    /// Next LSN to assign (monotonically increasing).
    next_lsn: AtomicU64,
}

impl WalWriter {
    /// Open or create the WAL writer rooted at `wal_dir`.
    ///
    /// Scans existing segments to find the highest segment number and the
    /// highest LSN in use, then continues from there.
    pub fn open(wal_dir: &std::path::Path) -> Result<Self> {
        std::fs::create_dir_all(wal_dir)?;

        // Find highest existing segment number and last LSN.
        let (seg_no, seg_offset, next_lsn) = Self::scan_wal_state(wal_dir)?;

        let path = segment_path(wal_dir, seg_no);

        // Truncate the active segment to the last clean record boundary.  This
        // removes any partial (torn) record that may have been written before a
        // crash, preventing replay from encountering a spurious CRC failure at
        // the start of a fresh write run.
        if path.exists() {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(seg_offset)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            file,
            seg_no,
            seg_offset,
            next_lsn: AtomicU64::new(next_lsn),
        })
    }

    /// Scan existing WAL segments to reconstruct state (highest seg, offset, LSN).
    fn scan_wal_state(wal_dir: &std::path::Path) -> Result<(u64, u64, u64)> {
        let mut segments: Vec<u64> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("segment-") && name.ends_with(".wal") {
                    let num_str = &name["segment-".len()..name.len() - ".wal".len()];
                    if let Ok(n) = num_str.parse::<u64>() {
                        segments.push(n);
                    }
                }
            }
        }
        segments.sort();

        if segments.is_empty() {
            return Ok((0, 0, 1));
        }

        let last_seg = *segments.last().unwrap();
        let path = segment_path(wal_dir, last_seg);
        let data = std::fs::read(&path)?;

        // Scan records to find offset and max LSN.
        let mut offset = 0usize;
        let mut max_lsn = 0u64;
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((rec, consumed)) => {
                    if rec.lsn.0 > max_lsn {
                        max_lsn = rec.lsn.0;
                    }
                    offset += consumed;
                }
                Err(_) => break, // Torn record or padding — stop here.
            }
        }

        Ok((last_seg, offset as u64, max_lsn + 1))
    }

    /// Allocate the next LSN.
    fn alloc_lsn(&self) -> Lsn {
        Lsn(self.next_lsn.fetch_add(1, Ordering::Relaxed))
    }

    /// Append a WAL record and return its assigned LSN.
    ///
    /// Rotates to a new segment if this record would overflow the current one.
    pub fn append(
        &mut self,
        kind: WalRecordKind,
        txn_id: TxnId,
        payload: WalPayload,
    ) -> Result<Lsn> {
        let lsn = self.alloc_lsn();
        let record = WalRecord {
            lsn,
            txn_id,
            kind,
            payload,
        };
        let encoded = record.encode();
        let record_len = encoded.len() as u64;

        // Rotate if needed.
        if self.seg_offset + record_len > SEGMENT_SIZE {
            self.rotate()?;
        }

        self.file.write_all(&encoded)?;
        self.seg_offset += record_len;

        Ok(lsn)
    }

    /// Rotate to a new segment file.
    fn rotate(&mut self) -> Result<()> {
        // Pad remaining bytes to zero (no-op for append-only files — we just move on).
        self.file.flush()?;
        self.seg_no += 1;
        self.seg_offset = 0;
        let path = segment_path(&self.wal_dir, self.seg_no);
        self.file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(())
    }

    /// fsync the current WAL segment to durable storage.
    pub fn fsync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Current WAL directory.
    pub fn wal_dir(&self) -> &std::path::Path {
        &self.wal_dir
    }

    /// Last LSN that has been allocated (useful for replay horizon checks).
    pub fn last_lsn(&self) -> Lsn {
        Lsn(self.next_lsn.load(Ordering::Relaxed).saturating_sub(1))
    }

    /// High-level: execute the full 6-step commit sequence for a transaction.
    ///
    /// Steps:
    ///   1. Append BEGIN record.
    ///   2. Append one WRITE record per `(page_id, image)` pair.
    ///   3. Append COMMIT record.
    ///   4. fsync WAL.
    ///
    /// Steps 5 and 6 (apply pages, advance LSN) are the caller's
    /// responsibility after this function returns successfully.
    ///
    /// Returns the LSN of the COMMIT record.
    pub fn commit_transaction(
        &mut self,
        txn_id: TxnId,
        dirty_pages: &[(u64, Vec<u8>)],
    ) -> Result<Lsn> {
        // Step 1: BEGIN
        self.append(WalRecordKind::Begin, txn_id, WalPayload::Empty)?;

        // Step 2: WRITE records
        for (page_id, image) in dirty_pages {
            self.append(
                WalRecordKind::Write,
                txn_id,
                WalPayload::Write {
                    page_id: *page_id,
                    image: image.clone(),
                },
            )?;
        }

        // Step 3: COMMIT
        let commit_lsn = self.append(WalRecordKind::Commit, txn_id, WalPayload::Empty)?;

        // Step 4: fsync
        self.fsync()?;

        Ok(commit_lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_writer(dir: &TempDir) -> WalWriter {
        WalWriter::open(dir.path()).unwrap()
    }

    #[test]
    fn test_writer_creates_segment_file() {
        let dir = TempDir::new().unwrap();
        let _writer = open_writer(&dir);
        let seg = segment_path(dir.path(), 0);
        assert!(seg.exists());
    }

    #[test]
    fn test_writer_append_returns_monotonic_lsns() {
        let dir = TempDir::new().unwrap();
        let mut writer = open_writer(&dir);
        let txn = TxnId(1);
        let l1 = writer
            .append(WalRecordKind::Begin, txn, WalPayload::Empty)
            .unwrap();
        let l2 = writer
            .append(WalRecordKind::Commit, txn, WalPayload::Empty)
            .unwrap();
        assert!(l1 < l2);
    }

    #[test]
    fn test_writer_commit_transaction() {
        let dir = TempDir::new().unwrap();
        let mut writer = open_writer(&dir);
        let dirty = vec![(0u64, vec![0xAAu8; 64])];
        let commit_lsn = writer.commit_transaction(TxnId(42), &dirty).unwrap();
        assert!(commit_lsn.0 >= 1);
    }

    #[test]
    fn test_writer_segment_rotation() {
        let dir = TempDir::new().unwrap();
        let mut writer = open_writer(&dir);
        // Override SEGMENT_SIZE effectively by filling a large write.
        // We'll test rotation by writing a record that fits and checking seg_no.
        let initial_seg = writer.seg_no;
        // Write enough to trigger rotation (simulate by directly setting seg_offset).
        writer.seg_offset = SEGMENT_SIZE; // force rotation on next write
        writer
            .append(WalRecordKind::Begin, TxnId(1), WalPayload::Empty)
            .unwrap();
        assert_eq!(writer.seg_no, initial_seg + 1);
    }

    #[test]
    fn test_writer_fsync_does_not_panic() {
        let dir = TempDir::new().unwrap();
        let mut writer = open_writer(&dir);
        writer
            .append(WalRecordKind::Begin, TxnId(1), WalPayload::Empty)
            .unwrap();
        writer.fsync().unwrap();
    }

    #[test]
    fn test_writer_reopen_continues_lsn() {
        let dir = TempDir::new().unwrap();
        let last_lsn = {
            let mut writer = open_writer(&dir);
            writer
                .append(WalRecordKind::Begin, TxnId(1), WalPayload::Empty)
                .unwrap();
            writer
                .append(WalRecordKind::Commit, TxnId(1), WalPayload::Empty)
                .unwrap()
        };
        // Reopen — should continue from last_lsn + 1
        let mut writer2 = open_writer(&dir);
        let new_lsn = writer2
            .append(WalRecordKind::Begin, TxnId(2), WalPayload::Empty)
            .unwrap();
        assert!(
            new_lsn > last_lsn,
            "reopened writer must continue from last LSN"
        );
    }
}
