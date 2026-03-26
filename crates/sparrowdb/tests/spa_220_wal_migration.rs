//! SPA-220: WAL migration utility — upgrade from v21 (CRC32) to v2 (CRC32C).
//!
//! These integration tests verify that a database created with the legacy WAL
//! format (version byte 21, CRC32 IEEE checksums) can be migrated in-place to
//! the current format (version byte 2, CRC32C Castagnoli) and that all records
//! survive the conversion intact.

use sparrowdb_common::{Lsn, TxnId};
use sparrowdb_storage::wal::codec::{
    WalPayload, WalRecord, WalRecordKind, WAL_FORMAT_VERSION, WAL_FORMAT_VERSION_LEGACY,
};
use sparrowdb_storage::wal::writer::segment_path;
use tempfile::TempDir;

/// Build a legacy v21 WAL segment with CRC32 IEEE checksums.
fn build_legacy_segment(records: &[WalRecord]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(WAL_FORMAT_VERSION_LEGACY);

    for rec in records {
        let payload_bytes = rec.payload.encode();
        let payload_len = payload_bytes.len();

        let length_val = (1u32 + 8 + 8) + payload_len as u32 + 4u32;
        buf.extend_from_slice(&length_val.to_le_bytes());
        buf.push(rec.kind.as_byte());
        buf.extend_from_slice(&rec.lsn.0.to_le_bytes());
        buf.extend_from_slice(&rec.txn_id.0.to_le_bytes());
        buf.extend_from_slice(&payload_bytes);

        let crc_start = buf.len() - (1 + 8 + 8 + payload_len);
        let crc = crc32fast::hash(&buf[crc_start..]);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    buf
}

/// Create a minimal legacy database directory with WAL segments containing
/// node and edge records, then return the db path.
fn create_legacy_db(dir: &TempDir) -> std::path::PathBuf {
    let db_path = dir.path().join("test.sparrow");
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let records = vec![
        WalRecord {
            lsn: Lsn(1),
            txn_id: TxnId(1),
            kind: WalRecordKind::Begin,
            payload: WalPayload::Empty,
        },
        WalRecord {
            lsn: Lsn(2),
            txn_id: TxnId(1),
            kind: WalRecordKind::NodeCreate,
            payload: WalPayload::NodeCreate {
                node_id: 1,
                label_id: 0,
                props: vec![("name".to_string(), b"Alice".to_vec())],
            },
        },
        WalRecord {
            lsn: Lsn(3),
            txn_id: TxnId(1),
            kind: WalRecordKind::NodeCreate,
            payload: WalPayload::NodeCreate {
                node_id: 2,
                label_id: 0,
                props: vec![("name".to_string(), b"Bob".to_vec())],
            },
        },
        WalRecord {
            lsn: Lsn(4),
            txn_id: TxnId(1),
            kind: WalRecordKind::EdgeCreate,
            payload: WalPayload::EdgeCreate {
                edge_id: 100,
                src: 1,
                dst: 2,
                rel_type: "KNOWS".to_string(),
                props: vec![],
            },
        },
        WalRecord {
            lsn: Lsn(5),
            txn_id: TxnId(1),
            kind: WalRecordKind::Commit,
            payload: WalPayload::Empty,
        },
    ];

    let seg_data = build_legacy_segment(&records);
    std::fs::write(segment_path(&wal_dir, 0), &seg_data).unwrap();

    db_path
}

#[test]
fn migrate_legacy_wal_then_verify_records() {
    let dir = TempDir::new().unwrap();
    let db_path = create_legacy_db(&dir);
    let wal_dir = db_path.join("wal");

    // Before migration: version byte should be 21 (legacy).
    let before = std::fs::read(segment_path(&wal_dir, 0)).unwrap();
    assert_eq!(before[0], WAL_FORMAT_VERSION_LEGACY);

    // Run migration.
    let result = sparrowdb::migrate_wal(&db_path).unwrap();
    assert_eq!(result.segments_converted, 1);
    assert_eq!(result.records_converted, 5);

    // After migration: version byte should be 2 (current).
    let after = std::fs::read(segment_path(&wal_dir, 0)).unwrap();
    assert_eq!(after[0], WAL_FORMAT_VERSION);

    // All 5 records should decode with CRC32C.
    let mut offset = 1;
    let mut count = 0;
    while offset < after.len() {
        if after[offset..].iter().all(|&b| b == 0) {
            break;
        }
        let (_rec, consumed) = WalRecord::decode_with_version(&after[offset..], WAL_FORMAT_VERSION)
            .expect("should decode migrated record");
        offset += consumed;
        count += 1;
    }
    assert_eq!(count, 5);
}

#[test]
fn migrate_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let db_path = create_legacy_db(&dir);

    // First migration.
    let r1 = sparrowdb::migrate_wal(&db_path).unwrap();
    assert_eq!(r1.segments_converted, 1);

    // Second migration — no-op.
    let r2 = sparrowdb::migrate_wal(&db_path).unwrap();
    assert_eq!(r2.segments_converted, 0);
    assert_eq!(r2.segments_skipped, 1);
}

#[test]
fn migrate_nonexistent_wal_dir_is_noop() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("empty.sparrow");
    // Don't create the wal/ subdirectory at all.

    let result = sparrowdb::migrate_wal(&db_path).unwrap();
    assert_eq!(result.segments_inspected, 0);
    assert_eq!(result.segments_converted, 0);
}

#[test]
fn migrate_corrupt_legacy_segment_returns_error() {
    let dir = TempDir::new().unwrap();
    let db_path = create_legacy_db(&dir);
    let wal_dir = db_path.join("wal");

    // Corrupt the segment by flipping bits in the record area.
    let seg_path = segment_path(&wal_dir, 0);
    let mut data = std::fs::read(&seg_path).unwrap();
    // Flip byte 15 which is inside the first record's CRC-covered region.
    if data.len() > 15 {
        data[15] ^= 0xFF;
    }
    std::fs::write(&seg_path, &data).unwrap();

    let err = sparrowdb::migrate_wal(&db_path);
    assert!(err.is_err(), "corrupt segment should cause migration error");
}

#[test]
fn migrate_preserves_payload_data() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.sparrow");
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let records = vec![
        WalRecord {
            lsn: Lsn(1),
            txn_id: TxnId(42),
            kind: WalRecordKind::Begin,
            payload: WalPayload::Empty,
        },
        WalRecord {
            lsn: Lsn(2),
            txn_id: TxnId(42),
            kind: WalRecordKind::EdgeCreate,
            payload: WalPayload::EdgeCreate {
                edge_id: 777,
                src: 10,
                dst: 20,
                rel_type: "RELATED_TO".to_string(),
                props: vec![
                    ("score".to_string(), 99u64.to_le_bytes().to_vec()),
                    ("label".to_string(), b"important".to_vec()),
                ],
            },
        },
        WalRecord {
            lsn: Lsn(3),
            txn_id: TxnId(42),
            kind: WalRecordKind::NodeUpdate,
            payload: WalPayload::NodeUpdate {
                node_id: 10,
                key: "age".to_string(),
                col_id: 5,
                before: 25u64.to_le_bytes().to_vec(),
                after: 26u64.to_le_bytes().to_vec(),
            },
        },
        WalRecord {
            lsn: Lsn(4),
            txn_id: TxnId(42),
            kind: WalRecordKind::Commit,
            payload: WalPayload::Empty,
        },
    ];

    std::fs::write(segment_path(&wal_dir, 0), build_legacy_segment(&records)).unwrap();

    sparrowdb::migrate_wal(&db_path).unwrap();

    // Read back and verify payloads.
    let migrated = std::fs::read(segment_path(&wal_dir, 0)).unwrap();
    let mut offset = 1;
    let mut decoded = Vec::new();
    while offset < migrated.len() {
        if migrated[offset..].iter().all(|&b| b == 0) {
            break;
        }
        let (rec, consumed) =
            WalRecord::decode_with_version(&migrated[offset..], WAL_FORMAT_VERSION).unwrap();
        decoded.push(rec);
        offset += consumed;
    }

    assert_eq!(decoded.len(), 4);

    // Check EdgeCreate payload.
    match &decoded[1].payload {
        WalPayload::EdgeCreate {
            edge_id,
            src,
            dst,
            rel_type,
            props,
        } => {
            assert_eq!(*edge_id, 777);
            assert_eq!(*src, 10);
            assert_eq!(*dst, 20);
            assert_eq!(rel_type, "RELATED_TO");
            assert_eq!(props.len(), 2);
            assert_eq!(props[0].0, "score");
            assert_eq!(props[1].0, "label");
        }
        other => panic!("expected EdgeCreate, got: {other:?}"),
    }

    // Check NodeUpdate payload.
    match &decoded[2].payload {
        WalPayload::NodeUpdate {
            node_id,
            key,
            col_id,
            before,
            after,
        } => {
            assert_eq!(*node_id, 10);
            assert_eq!(key, "age");
            assert_eq!(*col_id, 5);
            assert_eq!(before, &25u64.to_le_bytes().to_vec());
            assert_eq!(after, &26u64.to_le_bytes().to_vec());
        }
        other => panic!("expected NodeUpdate, got: {other:?}"),
    }
}
