//! WAL migration — upgrade legacy WAL segments from v21 (CRC32 IEEE) to v2 (CRC32C).
//!
//! ## Algorithm
//!
//! For each segment file in the WAL directory:
//!
//! 1. Read the version header byte.
//! 2. If it is already [`WAL_FORMAT_VERSION`] (2), skip it.
//! 3. If it is [`WAL_FORMAT_VERSION_LEGACY`] (21), decode every record using
//!    CRC32 IEEE verification, then re-encode each record with CRC32C and
//!    write to a temporary file.
//! 4. Atomically replace the original segment with the converted file.
//!
//! The migration is idempotent: running it on an already-migrated database is
//! a no-op.

use std::path::Path;

use sparrowdb_common::{Error, Result};

use super::codec::{WalRecord, WAL_FORMAT_VERSION, WAL_FORMAT_VERSION_LEGACY};
use super::writer::segment_path;

/// Result of a WAL migration pass.
#[derive(Debug, Clone)]
pub struct MigrationResult {
    /// Number of segment files inspected.
    pub segments_inspected: usize,
    /// Number of segment files that were converted (v21 -> v2).
    pub segments_converted: usize,
    /// Number of segment files already at v2 (skipped).
    pub segments_skipped: usize,
    /// Total number of records re-encoded.
    pub records_converted: usize,
}

/// Migrate all WAL segments in `wal_dir` from legacy v21 (CRC32) to v2 (CRC32C).
///
/// This is safe to run on a database that is already at v2 — those segments are
/// skipped. The function is **not** safe to call while a [`WalWriter`] has the
/// directory open; shut down the database first.
///
/// # Errors
///
/// Returns an error if any segment contains an unrecognised version byte, or if
/// a record fails CRC verification (indicating on-disk corruption that predates
/// the migration attempt).
pub fn migrate_wal(wal_dir: &Path) -> Result<MigrationResult> {
    let segments = collect_segments(wal_dir)?;

    let mut result = MigrationResult {
        segments_inspected: segments.len(),
        segments_converted: 0,
        segments_skipped: 0,
        records_converted: 0,
    };

    for seg_no in &segments {
        let path = segment_path(wal_dir, *seg_no);
        let data = std::fs::read(&path).map_err(Error::Io)?;

        if data.is_empty() {
            result.segments_skipped += 1;
            continue;
        }

        let version = data[0];

        match version {
            WAL_FORMAT_VERSION => {
                // Already current format — nothing to do.
                result.segments_skipped += 1;
            }
            WAL_FORMAT_VERSION_LEGACY => {
                // Decode all records using CRC32 IEEE, re-encode with CRC32C.
                let (new_data, record_count) = convert_segment(&data)?;
                result.records_converted += record_count;
                result.segments_converted += 1;

                // Write to a temporary file next to the original, then rename
                // for atomic replacement.
                let tmp_path = path.with_extension("wal.migrating");
                std::fs::write(&tmp_path, &new_data).map_err(Error::Io)?;
                std::fs::rename(&tmp_path, &path).map_err(Error::Io)?;
            }
            other => {
                return Err(Error::Corruption(format!(
                    "WAL segment {} has unrecognised version byte {other}. \
                     Expected {WAL_FORMAT_VERSION} (current) or \
                     {WAL_FORMAT_VERSION_LEGACY} (legacy 0.1.2).",
                    seg_no
                )));
            }
        }
    }

    Ok(result)
}

/// Convert a single segment's raw bytes from v21 (CRC32) to v2 (CRC32C).
///
/// Returns `(new_segment_bytes, record_count)`.
fn convert_segment(data: &[u8]) -> Result<(Vec<u8>, usize)> {
    debug_assert_eq!(data[0], WAL_FORMAT_VERSION_LEGACY);

    // Start the output buffer with the new version header.
    let mut out = Vec::with_capacity(data.len());
    out.push(WAL_FORMAT_VERSION);

    let mut offset = 1usize; // skip the version byte
    let mut record_count = 0usize;

    while offset < data.len() {
        // Skip zero-padding at the end of a segment.
        if data[offset..].iter().all(|&b| b == 0) {
            // Preserve the padding length so the output is the same size.
            out.extend_from_slice(&data[offset..]);
            break;
        }

        // Decode using the legacy CRC32 IEEE algorithm.
        let (record, consumed) =
            WalRecord::decode_with_version(&data[offset..], WAL_FORMAT_VERSION_LEGACY)?;

        // Re-encode with CRC32C (the default encode path).
        let encoded = record.encode();
        out.extend_from_slice(&encoded);

        offset += consumed;
        record_count += 1;
    }

    Ok((out, record_count))
}

/// Collect and sort WAL segment numbers from a directory.
///
/// This is a local copy of the same logic in `replay.rs` to keep the migration
/// module self-contained. It is not worth making the replay helper `pub(crate)`
/// just for this one call site.
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
    use super::codec::{WalPayload, WalRecordKind};
    use super::*;
    use sparrowdb_common::{Lsn, TxnId};
    use tempfile::TempDir;

    /// Build a legacy v21 segment with CRC32 IEEE checksums.
    ///
    /// We cannot use `WalRecord::encode()` because that always produces CRC32C.
    /// Instead, we manually construct the binary representation using CRC32 IEEE.
    fn build_legacy_segment(records: &[WalRecord]) -> Vec<u8> {
        let mut buf = Vec::new();
        // Version header byte — legacy.
        buf.push(WAL_FORMAT_VERSION_LEGACY);

        for rec in records {
            let payload_bytes = rec.payload.encode();
            let payload_len = payload_bytes.len();

            // length field: kind(1) + lsn(8) + txn_id(8) + payload + crc(4)
            let length_val = (1u32 + 8 + 8) + payload_len as u32 + 4u32;
            buf.extend_from_slice(&length_val.to_le_bytes());

            // kind
            buf.push(rec.kind.as_byte());

            // lsn
            buf.extend_from_slice(&rec.lsn.0.to_le_bytes());

            // txn_id
            buf.extend_from_slice(&rec.txn_id.0.to_le_bytes());

            // payload
            buf.extend_from_slice(&payload_bytes);

            // CRC32 IEEE over [kind + lsn + txn_id + payload]
            let crc_start = buf.len() - (1 + 8 + 8 + payload_len);
            let crc = crc32fast::hash(&buf[crc_start..]);
            buf.extend_from_slice(&crc.to_le_bytes());
        }

        buf
    }

    fn sample_records() -> Vec<WalRecord> {
        vec![
            WalRecord {
                lsn: Lsn(1),
                txn_id: TxnId(100),
                kind: WalRecordKind::Begin,
                payload: WalPayload::Empty,
            },
            WalRecord {
                lsn: Lsn(2),
                txn_id: TxnId(100),
                kind: WalRecordKind::NodeCreate,
                payload: WalPayload::NodeCreate {
                    node_id: 42,
                    label_id: 1,
                    props: vec![("name".to_string(), b"Alice".to_vec())],
                },
            },
            WalRecord {
                lsn: Lsn(3),
                txn_id: TxnId(100),
                kind: WalRecordKind::Commit,
                payload: WalPayload::Empty,
            },
        ]
    }

    #[test]
    fn test_legacy_segment_roundtrip_through_decode_with_version() {
        let records = sample_records();
        let seg_data = build_legacy_segment(&records);

        // Verify we can decode with the legacy version.
        let mut offset = 1; // skip version byte
        for expected in &records {
            let (decoded, consumed) =
                WalRecord::decode_with_version(&seg_data[offset..], WAL_FORMAT_VERSION_LEGACY)
                    .expect("should decode legacy record");
            assert_eq!(decoded.lsn, expected.lsn);
            assert_eq!(decoded.kind, expected.kind);
            offset += consumed;
        }
    }

    #[test]
    fn test_migrate_converts_legacy_to_current() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let records = sample_records();
        let seg_data = build_legacy_segment(&records);
        let seg_path = segment_path(&wal_dir, 0);
        std::fs::write(&seg_path, &seg_data).unwrap();

        // Run migration.
        let result = migrate_wal(&wal_dir).unwrap();
        assert_eq!(result.segments_inspected, 1);
        assert_eq!(result.segments_converted, 1);
        assert_eq!(result.segments_skipped, 0);
        assert_eq!(result.records_converted, 3);

        // Verify the migrated segment is now v2.
        let migrated = std::fs::read(&seg_path).unwrap();
        assert_eq!(migrated[0], WAL_FORMAT_VERSION);

        // Verify all records decode correctly with v2 (CRC32C).
        let mut offset = 1;
        for expected in &records {
            let (decoded, consumed) =
                WalRecord::decode_with_version(&migrated[offset..], WAL_FORMAT_VERSION)
                    .expect("should decode migrated record with CRC32C");
            assert_eq!(decoded.lsn, expected.lsn);
            assert_eq!(decoded.kind, expected.kind);
            offset += consumed;
        }
    }

    #[test]
    fn test_migrate_skips_already_current() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write a v2 segment using the normal encode path.
        let records = sample_records();
        let mut v2_data = vec![WAL_FORMAT_VERSION];
        for rec in &records {
            v2_data.extend_from_slice(&rec.encode());
        }
        let seg_path = segment_path(&wal_dir, 0);
        std::fs::write(&seg_path, &v2_data).unwrap();

        let result = migrate_wal(&wal_dir).unwrap();
        assert_eq!(result.segments_inspected, 1);
        assert_eq!(result.segments_converted, 0);
        assert_eq!(result.segments_skipped, 1);
        assert_eq!(result.records_converted, 0);

        // Content should be unchanged.
        let after = std::fs::read(&seg_path).unwrap();
        assert_eq!(after, v2_data);
    }

    #[test]
    fn test_migrate_idempotent() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let records = sample_records();
        let seg_data = build_legacy_segment(&records);
        let seg_path = segment_path(&wal_dir, 0);
        std::fs::write(&seg_path, &seg_data).unwrap();

        // First migration.
        let r1 = migrate_wal(&wal_dir).unwrap();
        assert_eq!(r1.segments_converted, 1);

        let after_first = std::fs::read(&seg_path).unwrap();

        // Second migration — should be a no-op.
        let r2 = migrate_wal(&wal_dir).unwrap();
        assert_eq!(r2.segments_converted, 0);
        assert_eq!(r2.segments_skipped, 1);

        let after_second = std::fs::read(&seg_path).unwrap();
        assert_eq!(after_first, after_second);
    }

    #[test]
    fn test_migrate_corrupt_segment_errors() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let records = sample_records();
        let mut seg_data = build_legacy_segment(&records);

        // Corrupt a byte in the middle of the first record's CRC range.
        // The version byte is at [0], the first record starts at [1].
        // Flip a byte in the payload area.
        if seg_data.len() > 15 {
            seg_data[15] ^= 0xFF;
        }

        let seg_path = segment_path(&wal_dir, 0);
        std::fs::write(&seg_path, &seg_data).unwrap();

        let err = migrate_wal(&wal_dir).unwrap_err();
        // Should be a ChecksumMismatch or Corruption error.
        match err {
            Error::ChecksumMismatch | Error::Corruption(_) => {} // expected
            other => panic!("expected checksum/corruption error, got: {other:?}"),
        }
    }

    #[test]
    fn test_migrate_mixed_segments() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let records = sample_records();

        // Segment 0: legacy v21
        let legacy_data = build_legacy_segment(&records);
        std::fs::write(segment_path(&wal_dir, 0), &legacy_data).unwrap();

        // Segment 1: current v2
        let mut v2_data = vec![WAL_FORMAT_VERSION];
        for rec in &records {
            v2_data.extend_from_slice(&rec.encode());
        }
        std::fs::write(segment_path(&wal_dir, 1), &v2_data).unwrap();

        let result = migrate_wal(&wal_dir).unwrap();
        assert_eq!(result.segments_inspected, 2);
        assert_eq!(result.segments_converted, 1);
        assert_eq!(result.segments_skipped, 1);
    }

    #[test]
    fn test_migrate_preserves_edge_create_records() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let records = vec![
            WalRecord {
                lsn: Lsn(1),
                txn_id: TxnId(200),
                kind: WalRecordKind::Begin,
                payload: WalPayload::Empty,
            },
            WalRecord {
                lsn: Lsn(2),
                txn_id: TxnId(200),
                kind: WalRecordKind::EdgeCreate,
                payload: WalPayload::EdgeCreate {
                    edge_id: 99,
                    src: 1,
                    dst: 2,
                    rel_type: "FOLLOWS".to_string(),
                    props: vec![("weight".to_string(), 42u64.to_le_bytes().to_vec())],
                },
            },
            WalRecord {
                lsn: Lsn(3),
                txn_id: TxnId(200),
                kind: WalRecordKind::Commit,
                payload: WalPayload::Empty,
            },
        ];

        let seg_data = build_legacy_segment(&records);
        std::fs::write(segment_path(&wal_dir, 0), &seg_data).unwrap();

        let result = migrate_wal(&wal_dir).unwrap();
        assert_eq!(result.records_converted, 3);

        // Verify payload survives the round-trip.
        let migrated = std::fs::read(segment_path(&wal_dir, 0)).unwrap();
        let mut offset = 1;
        let (_, consumed) =
            WalRecord::decode_with_version(&migrated[offset..], WAL_FORMAT_VERSION).unwrap();
        offset += consumed;

        let (edge_rec, _) =
            WalRecord::decode_with_version(&migrated[offset..], WAL_FORMAT_VERSION).unwrap();
        assert_eq!(edge_rec.kind, WalRecordKind::EdgeCreate);
        match edge_rec.payload {
            WalPayload::EdgeCreate {
                edge_id,
                src,
                dst,
                rel_type,
                props,
            } => {
                assert_eq!(edge_id, 99);
                assert_eq!(src, 1);
                assert_eq!(dst, 2);
                assert_eq!(rel_type, "FOLLOWS");
                assert_eq!(props.len(), 1);
                assert_eq!(props[0].0, "weight");
            }
            other => panic!("expected EdgeCreate, got: {other:?}"),
        }
    }
}
