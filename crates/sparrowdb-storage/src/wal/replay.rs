//! WAL replay — scan committed transactions and apply them to a page store.
//!
//! ## Replay algorithm
//!
//! 1. Scan all WAL segments in order.
//! 2. Collect only records with `lsn > last_applied_lsn`.
//! 3. Identify committed transactions: a txn is committed iff its BEGIN record
//!    is followed (at some later LSN) by a COMMIT record before the next BEGIN
//!    for the same txn_id.
//! 4. For each committed txn in LSN order, apply all WRITE records by calling
//!    `apply_fn` with (page_id, image, lsn).
//! 5. Stop at the first CRC32 failure — torn page boundary.
//!
//! ## Idempotency
//!
//! The `last_applied_lsn` parameter acts as the replay horizon.  Passing the
//! same horizon twice produces identical results because records at or below
//! the horizon are skipped.  The caller is responsible for persisting the
//! advanced `last_applied_lsn` returned by `replay`.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
};

use sparrowdb_common::{Error, Lsn, Result};

use super::codec::{WalPayload, WalRecord, WalRecordKind};
use super::writer::segment_path;
use crate::encryption::EncryptionContext;

/// Result of a WAL replay pass.
pub struct ReplayResult {
    /// The highest LSN successfully applied.
    pub last_applied_lsn: Lsn,
    /// Number of transactions replayed.
    pub txns_replayed: usize,
    /// Number of page writes applied.
    pub pages_applied: usize,
}

/// WAL replayer — stateless, driven by a callback.
pub struct WalReplayer;

impl WalReplayer {
    /// Replay WAL records from `wal_dir` with `lsn > last_applied_lsn`.
    ///
    /// For each committed WRITE record (in LSN order), `apply_fn` is called:
    /// `apply_fn(page_id: u64, image: &[u8], lsn: Lsn) -> Result<()>`.
    ///
    /// Stops cleanly at the first CRC32 failure (torn page).
    /// Returns the new `last_applied_lsn` after all committed writes are applied.
    pub fn replay(
        wal_dir: &Path,
        last_applied_lsn: Lsn,
        apply_fn: impl FnMut(u64, &[u8], Lsn) -> Result<()>,
    ) -> Result<ReplayResult> {
        Self::replay_inner(wal_dir, last_applied_lsn, EncryptionContext::none(), apply_fn)
    }

    /// Replay an encrypted WAL written with [`WalWriter::open_encrypted`].
    ///
    /// Identical to [`replay`] but decrypts each non-empty payload using the
    /// provided key before dispatching to `apply_fn`.
    pub fn replay_encrypted(
        wal_dir: &Path,
        last_applied_lsn: Lsn,
        key: [u8; 32],
        apply_fn: impl FnMut(u64, &[u8], Lsn) -> Result<()>,
    ) -> Result<ReplayResult> {
        Self::replay_inner(
            wal_dir,
            last_applied_lsn,
            EncryptionContext::with_key(key),
            apply_fn,
        )
    }

    fn replay_inner(
        wal_dir: &Path,
        last_applied_lsn: Lsn,
        enc: EncryptionContext,
        mut apply_fn: impl FnMut(u64, &[u8], Lsn) -> Result<()>,
    ) -> Result<ReplayResult> {
        let segments = collect_segments(wal_dir)?;

        // Pass 1: read all records up to first CRC failure.
        // We collect them into a BTreeMap keyed by LSN for ordering.
        let mut all_records: BTreeMap<u64, WalRecord> = BTreeMap::new();
        let mut torn_at: Option<u64> = None; // LSN at which we stopped

        'outer: for seg_no in &segments {
            let path = segment_path(wal_dir, *seg_no);
            let data = match std::fs::read(&path) {
                Ok(d) => d,
                Err(e) => return Err(Error::Io(e)),
            };

            let mut offset = 0usize;
            while offset < data.len() {
                // Skip zero-padding (rotation padding or end of segment).
                if data[offset..].iter().all(|&b| b == 0) {
                    break;
                }
                match WalRecord::decode(&data[offset..]) {
                    Ok((rec, consumed)) => {
                        all_records.insert(rec.lsn.0, rec);
                        offset += consumed;
                    }
                    Err(Error::ChecksumMismatch) => {
                        // Torn page — stop here, do not apply any more records.
                        torn_at = Some(offset as u64);
                        break 'outer;
                    }
                    Err(Error::Corruption(_)) => {
                        // Partial or corrupt record — treat as torn boundary.
                        break 'outer;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // Pass 2: identify committed transactions.
        // A txn_id is committed iff we see both a Begin and a Commit for it,
        // in that order, with no intervening Abort.
        let mut begun: HashMap<u64, bool> = HashMap::new(); // txn_id -> seen_begin
        let mut committed: HashSet<u64> = HashSet::new();
        let mut aborted: HashSet<u64> = HashSet::new();

        for rec in all_records.values() {
            match rec.kind {
                WalRecordKind::Begin => {
                    begun.insert(rec.txn_id.0, true);
                }
                WalRecordKind::Commit => {
                    if begun.contains_key(&rec.txn_id.0) {
                        committed.insert(rec.txn_id.0);
                    }
                }
                WalRecordKind::Abort => {
                    begun.remove(&rec.txn_id.0);
                    aborted.insert(rec.txn_id.0);
                }
                _ => {}
            }
        }

        // Pass 3: apply WRITE records for committed txns with lsn > last_applied_lsn.
        let mut last_applied = last_applied_lsn.0;
        let mut txns_replayed_set: HashSet<u64> = HashSet::new();
        let mut pages_applied = 0usize;

        for (lsn, rec) in &all_records {
            if *lsn <= last_applied_lsn.0 {
                continue;
            }
            if let Some(lsn_limit) = torn_at {
                // lsn_limit is a byte offset, not an LSN; but we stopped
                // collecting at the torn boundary, so all records in all_records
                // are already safe.
                let _ = lsn_limit;
            }
            if rec.kind == WalRecordKind::Write && committed.contains(&rec.txn_id.0) {
                // Resolve the payload — decrypt if it came from an encrypted WAL.
                let resolved = match &rec.payload {
                    WalPayload::Raw(encrypted_bytes) => {
                        // Decrypt using the record's own LSN as AAD.
                        let plaintext = enc.decrypt_wal_payload(*lsn, encrypted_bytes)?;
                        WalPayload::decode_raw(rec.kind, &plaintext)?
                    }
                    other => other.clone(),
                };

                if let WalPayload::Write { page_id, image } = &resolved {
                    apply_fn(*page_id, image, Lsn(*lsn))?;
                    pages_applied += 1;
                    txns_replayed_set.insert(rec.txn_id.0);
                    if *lsn > last_applied {
                        last_applied = *lsn;
                    }
                }
            }
        }

        // Also advance last_applied past COMMIT records so we don't re-replay.
        for (lsn, rec) in &all_records {
            if *lsn <= last_applied_lsn.0 {
                continue;
            }
            if rec.kind == WalRecordKind::Commit
                && committed.contains(&rec.txn_id.0)
                && *lsn > last_applied
            {
                last_applied = *lsn;
            }
        }

        Ok(ReplayResult {
            last_applied_lsn: Lsn(last_applied),
            txns_replayed: txns_replayed_set.len(),
            pages_applied,
        })
    }
}

/// Collect all segment numbers in `wal_dir`, sorted ascending.
///
/// Returns an error if `wal_dir` cannot be read, so that callers are not
/// silently handed an empty segment list when the directory is inaccessible.
fn collect_segments(wal_dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    let entries = std::fs::read_dir(wal_dir).map_err(Error::Io)?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy().to_string();
        if name.starts_with("segment-") && name.ends_with(".wal") {
            let num_str = &name["segment-".len()..name.len() - ".wal".len()];
            if let Ok(n) = num_str.parse::<u64>() {
                segments.push(n);
            }
        }
    }
    segments.sort();
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use sparrowdb_common::TxnId;
    use std::collections::HashMap;
    use std::io::Write as _;
    use tempfile::TempDir;

    /// Helper: write N transactions each writing one page.
    fn write_txns(writer: &mut WalWriter, count: u64, start_page: u64) -> Vec<(u64, Vec<u8>)> {
        let mut written = Vec::new();
        for i in 0..count {
            let txn_id = TxnId(100 + i);
            let page_id = start_page + i;
            let image = vec![(i as u8).wrapping_add(0xAA); 32];
            writer
                .commit_transaction(txn_id, &[(page_id, image.clone())])
                .unwrap();
            written.push((page_id, image));
        }
        written
    }

    #[test]
    fn test_wal_append_and_replay() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        let txns = write_txns(&mut writer, 3, 0);
        drop(writer);

        let mut applied: HashMap<u64, Vec<u8>> = HashMap::new();
        let result = WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _lsn| {
            applied.insert(page_id, image.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(result.pages_applied, 3);
        for (page_id, image) in &txns {
            assert_eq!(applied.get(page_id), Some(image));
        }
    }

    #[test]
    fn test_wal_replay_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();
        write_txns(&mut writer, 2, 0);
        drop(writer);

        let mut apply_count = 0usize;
        let r1 = WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| {
            apply_count += 1;
            Ok(())
        })
        .unwrap();

        // Second replay with same horizon: same number of applies.
        let mut apply_count2 = 0usize;
        WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| {
            apply_count2 += 1;
            Ok(())
        })
        .unwrap();

        assert_eq!(apply_count, apply_count2, "replay must be idempotent");

        // Third replay past the commit LSN: nothing applied.
        let mut apply_count3 = 0usize;
        WalReplayer::replay(dir.path(), r1.last_applied_lsn, |_, _, _| {
            apply_count3 += 1;
            Ok(())
        })
        .unwrap();

        assert_eq!(
            apply_count3, 0,
            "replaying past commit horizon must apply nothing"
        );
    }

    #[test]
    fn test_torn_page_detected_and_recovered() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Write a committed transaction.
        writer
            .commit_transaction(TxnId(1), &[(0, vec![0xAA; 32])])
            .unwrap();
        drop(writer);

        // Append a partial (torn) record to the segment.
        let seg_path = super::super::writer::segment_path(dir.path(), 0);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        // Write something that looks like the start of a record but has a bad CRC.
        // length field says 21 bytes follow (empty payload); we write random garbage.
        let torn: &[u8] = &[
            21, 0, 0, 0,    // length = 21
            0x02, // kind = Write
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // lsn garbage
            0x01, 0, 0, 0, 0, 0, 0, 0, // txn_id = 1
            // no payload
            0xDE, 0xAD, 0xBE, 0xEF, // bad CRC
        ];
        f.write_all(torn).unwrap();
        drop(f);

        // Replay must stop at the torn record and still return the first txn's data.
        let mut applied: HashMap<u64, Vec<u8>> = HashMap::new();
        let result = WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _| {
            applied.insert(page_id, image.to_vec());
            Ok(())
        })
        .unwrap();

        // The committed txn (page 0) should have been applied.
        assert!(
            applied.contains_key(&0),
            "committed page must be applied before torn record"
        );
        // The torn record's data must NOT be applied.
        assert_eq!(applied.len(), 1, "only the committed page must be applied");
        assert!(result.pages_applied >= 1);
    }

    #[test]
    fn test_empty_wal_replay_returns_zero() {
        let dir = TempDir::new().unwrap();
        // Don't write anything.
        std::fs::create_dir_all(dir.path()).unwrap();
        let result = WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| Ok(())).unwrap();
        assert_eq!(result.pages_applied, 0);
        assert_eq!(result.txns_replayed, 0);
    }

    #[test]
    fn test_replay_aborted_txn_not_applied() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Write a committed transaction.
        writer
            .commit_transaction(TxnId(1), &[(0, vec![0xAA; 32])])
            .unwrap();

        // Write an aborted transaction manually.
        let txn2 = TxnId(2);
        writer
            .append(WalRecordKind::Begin, txn2, WalPayload::Empty)
            .unwrap();
        writer
            .append(
                WalRecordKind::Write,
                txn2,
                WalPayload::Write {
                    page_id: 1,
                    image: vec![0xBB; 32],
                },
            )
            .unwrap();
        writer
            .append(WalRecordKind::Abort, txn2, WalPayload::Empty)
            .unwrap();
        writer.fsync().unwrap();
        drop(writer);

        let mut applied: HashMap<u64, Vec<u8>> = HashMap::new();
        WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _| {
            applied.insert(page_id, image.to_vec());
            Ok(())
        })
        .unwrap();

        assert!(
            applied.contains_key(&0),
            "committed txn page must be applied"
        );
        assert!(
            !applied.contains_key(&1),
            "aborted txn page must NOT be applied"
        );
    }

    #[test]
    fn test_replay_multiple_writes_same_page() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Two committed transactions touching the same page.
        writer
            .commit_transaction(TxnId(1), &[(0, vec![0x11; 32])])
            .unwrap();
        writer
            .commit_transaction(TxnId(2), &[(0, vec![0x22; 32])])
            .unwrap();
        drop(writer);

        let mut last_image: Option<Vec<u8>> = None;
        WalReplayer::replay(dir.path(), Lsn(0), |_, image, _| {
            last_image = Some(image.to_vec());
            Ok(())
        })
        .unwrap();

        // The last write (txn 2) must win.
        assert_eq!(last_image, Some(vec![0x22; 32]));
    }
}
