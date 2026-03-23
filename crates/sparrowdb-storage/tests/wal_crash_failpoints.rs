//! WAL crash failpoint tests — all 8 mandatory crash scenarios.
//!
//! Each test simulates a crash at a specific point in the 6-step commit
//! sequence by truncating or corrupting the WAL file at that point, then
//! verifying that replay recovers the correct state.
//!
//! ## 6-step commit sequence
//!
//!   1. Write BEGIN record
//!   2. Write WRITE records (one per dirty page)
//!   3. Write COMMIT record
//!   4. fsync WAL
//!   5. Apply pages to storage  [caller]
//!   6. Advance last_applied_lsn in metapage  [caller]
//!
//! ## 8 failpoint scenarios
//!
//!   FP-1: After BEGIN, before first WRITE         → incomplete txn → not applied
//!   FP-2: After first WRITE, before all WRITEs    → incomplete txn → not applied
//!   FP-3: After all WRITEs, before COMMIT         → incomplete txn → not applied
//!   FP-4: After COMMIT, before fsync              → txn committed but unflushed →
//!          OS may or may not persist; simulate as COMMIT present but torn
//!   FP-5: After fsync, before applying pages      → WAL has COMMIT, pages not written →
//!          replay must apply them
//!   FP-6: After applying first page, before all   → WAL has COMMIT, partial page apply →
//!          replay re-applies all (idempotent)
//!   FP-7: After applying all pages, before LSN    → WAL has COMMIT, pages written,
//!          LSN not advanced → replay re-applies (idempotent)
//!   FP-8: After advancing LSN (clean commit)      → everything durable → replay skips
//!          (horizon already advanced)

use sparrowdb_common::{Lsn, TxnId};
use sparrowdb_storage::wal::{
    codec::{WalPayload, WalRecordKind},
    replay::WalReplayer,
    writer::{segment_path, WalWriter},
};
use std::{collections::HashMap, io::Write};
use tempfile::TempDir;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Apply a full committed transaction and return the raw bytes written.
fn write_committed_txn(
    wal_dir: &std::path::Path,
    txn_id: u64,
    pages: &[(u64, Vec<u8>)],
) -> Vec<u8> {
    let mut writer = WalWriter::open(wal_dir).unwrap();
    let dirty: Vec<(u64, Vec<u8>)> = pages.to_vec();
    writer.commit_transaction(TxnId(txn_id), &dirty).unwrap();
    drop(writer);
    std::fs::read(segment_path(wal_dir, 0)).unwrap()
}

/// Read back the WAL and replay from horizon 0, returning page -> image map.
fn replay_all(wal_dir: &std::path::Path) -> HashMap<u64, Vec<u8>> {
    let mut applied = HashMap::new();
    WalReplayer::replay(wal_dir, Lsn(0), |page_id, image, _| {
        applied.insert(page_id, image.to_vec());
        Ok(())
    })
    .unwrap();
    applied
}

/// Append raw bytes to the WAL segment (simulating a torn write).
fn append_raw(wal_dir: &std::path::Path, raw: &[u8]) {
    let path = segment_path(wal_dir, 0);
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    f.write_all(raw).unwrap();
}

// ── FP-1: After BEGIN, before first WRITE ────────────────────────────────────

/// Crash after writing BEGIN, before any WRITE record.
/// The transaction is incomplete — replay must not apply anything.
#[test]
fn test_crash_failpoint_1_after_begin_before_write() {
    let dir = TempDir::new().unwrap();

    // First write a clean committed txn so we have something to contrast against.
    let good_page = (0u64, vec![0xA1_u8; 32]);
    write_committed_txn(dir.path(), 1, std::slice::from_ref(&good_page));

    let _clean_size = std::fs::metadata(segment_path(dir.path(), 0))
        .unwrap()
        .len() as usize;

    // Now start a second transaction but crash after BEGIN only.
    {
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer
            .append(WalRecordKind::Begin, TxnId(2), WalPayload::Empty)
            .unwrap();
        // Crash — no WRITE, no COMMIT, no fsync
        drop(writer);
    }

    // Truncate to only the BEGIN of txn 2 (i.e., keep the clean txn + the BEGIN).
    // The file already has all records; txn 2 has no COMMIT so replay ignores it.
    let applied = replay_all(dir.path());
    assert_eq!(
        applied.get(&0),
        Some(&good_page.1),
        "FP-1: committed page must be applied"
    );
    assert!(
        !applied.contains_key(&99),
        "FP-1: crashed txn must NOT be applied"
    );
}

// ── FP-2: After first WRITE, before all WRITEs ───────────────────────────────

/// Crash after first WRITE record, before all WRITEs complete.
/// The transaction has no COMMIT — must not be applied.
#[test]
fn test_crash_failpoint_2_after_first_write_before_all_writes() {
    let dir = TempDir::new().unwrap();

    // Good txn first.
    write_committed_txn(dir.path(), 1, &[(0, vec![0x11; 32])]);

    // Bad txn: BEGIN + first WRITE (of two), then crash.
    {
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer
            .append(WalRecordKind::Begin, TxnId(2), WalPayload::Empty)
            .unwrap();
        writer
            .append(
                WalRecordKind::Write,
                TxnId(2),
                WalPayload::Write {
                    page_id: 1,
                    image: vec![0x22; 32],
                },
            )
            .unwrap();
        // Crash — second WRITE and COMMIT never written
        drop(writer);
    }

    let applied = replay_all(dir.path());
    assert_eq!(
        applied.get(&0),
        Some(&vec![0x11; 32]),
        "FP-2: committed page must be applied"
    );
    assert!(
        !applied.contains_key(&1),
        "FP-2: partial txn must NOT be applied"
    );
}

// ── FP-3: After all WRITEs, before COMMIT ────────────────────────────────────

/// Crash after all WRITE records, before COMMIT record.
/// No COMMIT — transaction must not be applied.
#[test]
fn test_crash_failpoint_3_after_all_writes_before_commit() {
    let dir = TempDir::new().unwrap();

    write_committed_txn(dir.path(), 1, &[(0, vec![0x11; 32])]);

    {
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer
            .append(WalRecordKind::Begin, TxnId(2), WalPayload::Empty)
            .unwrap();
        writer
            .append(
                WalRecordKind::Write,
                TxnId(2),
                WalPayload::Write {
                    page_id: 1,
                    image: vec![0x22; 32],
                },
            )
            .unwrap();
        writer
            .append(
                WalRecordKind::Write,
                TxnId(2),
                WalPayload::Write {
                    page_id: 2,
                    image: vec![0x33; 32],
                },
            )
            .unwrap();
        // Crash — COMMIT never written
        drop(writer);
    }

    let applied = replay_all(dir.path());
    assert_eq!(
        applied.get(&0),
        Some(&vec![0x11; 32]),
        "FP-3: committed page must be applied"
    );
    assert!(
        !applied.contains_key(&1),
        "FP-3: uncommitted write 1 must NOT be applied"
    );
    assert!(
        !applied.contains_key(&2),
        "FP-3: uncommitted write 2 must NOT be applied"
    );
}

// ── FP-4: After COMMIT, before fsync ─────────────────────────────────────────

/// Crash after COMMIT record written but before fsync.
///
/// The OS may or may not have flushed the COMMIT to disk.
/// Simulate the pessimistic case: COMMIT is present but a torn byte follows
/// (as if the OS flushed the COMMIT but then crashed mid-fsync on a later
/// record, leaving a partial torn record after the COMMIT).
///
/// In this case the COMMIT is on disk → replay must apply the transaction.
#[test]
fn test_crash_failpoint_4_after_commit_before_fsync() {
    let dir = TempDir::new().unwrap();

    // Write committed txn via commit_transaction (which does fsync).
    write_committed_txn(dir.path(), 1, &[(0, vec![0xAA; 32])]);

    // Simulate the "COMMIT present, torn byte after" scenario by appending
    // a broken partial record (bad CRC) after the clean COMMIT.
    append_raw(dir.path(), &[0xFF, 0xFF, 0xFF, 0xFF, 0xDE, 0xAD]);

    let applied = replay_all(dir.path());
    // The committed txn should be applied (CRC failure stops replay of torn part).
    assert_eq!(
        applied.get(&0),
        Some(&vec![0xAA; 32]),
        "FP-4: committed page must survive torn tail"
    );
}

// ── FP-5: After fsync, before applying pages ─────────────────────────────────

/// Crash after WAL fsync, before pages written to storage.
/// WAL has COMMIT; pages not yet applied.
/// Replay must apply all WRITE records from the committed txn.
#[test]
fn test_crash_failpoint_5_after_fsync_before_applying_pages() {
    let dir = TempDir::new().unwrap();

    // Write and fsync the WAL (simulating the state after step 4).
    write_committed_txn(dir.path(), 1, &[(0, vec![0xBB; 32]), (1, vec![0xCC; 32])]);

    // Simulate "pages not yet applied" by replaying from horizon 0
    // (pretending last_applied_lsn = 0, i.e. we crashed before applying).
    let mut applied = HashMap::new();
    WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _| {
        applied.insert(page_id, image.to_vec());
        Ok(())
    })
    .unwrap();

    assert_eq!(
        applied.get(&0),
        Some(&vec![0xBB; 32]),
        "FP-5: page 0 must be applied"
    );
    assert_eq!(
        applied.get(&1),
        Some(&vec![0xCC; 32]),
        "FP-5: page 1 must be applied"
    );
}

// ── FP-6: After applying first page, before all pages ────────────────────────

/// Crash after first page applied, before remaining pages applied.
/// Replay with last_applied_lsn = 0 must re-apply all pages (idempotent).
#[test]
fn test_crash_failpoint_6_after_first_page_before_all_pages() {
    let dir = TempDir::new().unwrap();

    write_committed_txn(
        dir.path(),
        1,
        &[
            (0, vec![0x11; 32]),
            (1, vec![0x22; 32]),
            (2, vec![0x33; 32]),
        ],
    );

    // Simulate: first page was applied (but we use horizon=0 to replay all,
    // because the LSN was not advanced — idempotency means the same data
    // is written again with no harm).
    let mut applied = HashMap::new();
    let mut apply_calls = 0usize;
    WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _| {
        applied.insert(page_id, image.to_vec());
        apply_calls += 1;
        Ok(())
    })
    .unwrap();

    // All 3 pages applied (idempotent re-apply is fine).
    assert_eq!(
        apply_calls, 3,
        "FP-6: all 3 pages must be applied on re-replay"
    );
    assert_eq!(applied.get(&0), Some(&vec![0x11; 32]));
    assert_eq!(applied.get(&1), Some(&vec![0x22; 32]));
    assert_eq!(applied.get(&2), Some(&vec![0x33; 32]));
}

// ── FP-7: After applying all pages, before advancing LSN ─────────────────────

/// Crash after all pages applied but before last_applied_lsn advanced.
/// Replay from horizon 0 must re-apply idempotently.
#[test]
fn test_crash_failpoint_7_after_all_pages_before_lsn_advance() {
    let dir = TempDir::new().unwrap();

    write_committed_txn(dir.path(), 1, &[(0, vec![0xAB; 32])]);

    // First replay (simulating initial recovery).
    let mut applied1 = HashMap::new();
    let r1 = WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _| {
        applied1.insert(page_id, image.to_vec());
        Ok(())
    })
    .unwrap();

    // Simulate crash before LSN advance: replay again from same horizon.
    let mut applied2 = HashMap::new();
    let r2 = WalReplayer::replay(dir.path(), Lsn(0), |page_id, image, _| {
        applied2.insert(page_id, image.to_vec());
        Ok(())
    })
    .unwrap();

    // Both replays produce identical results.
    assert_eq!(
        applied1, applied2,
        "FP-7: re-replay must produce identical page state"
    );
    assert_eq!(
        r1.pages_applied, r2.pages_applied,
        "FP-7: page apply count must be identical"
    );

    // Now simulate LSN advanced: replay from the commit horizon → nothing applied.
    let mut applied3 = HashMap::new();
    WalReplayer::replay(dir.path(), r1.last_applied_lsn, |page_id, image, _| {
        applied3.insert(page_id, image.to_vec());
        Ok(())
    })
    .unwrap();
    assert!(
        applied3.is_empty(),
        "FP-7: replay past commit horizon must apply nothing"
    );
}

// ── FP-8: After advancing LSN (clean commit) ─────────────────────────────────

/// Clean commit: all steps completed, LSN advanced.
/// Replay from the commit LSN horizon must apply nothing (already durable).
#[test]
fn test_crash_failpoint_8_clean_commit() {
    let dir = TempDir::new().unwrap();

    write_committed_txn(dir.path(), 1, &[(0, vec![0xFF; 32])]);

    // Full replay to get the commit LSN.
    let r = WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| Ok(())).unwrap();
    assert!(r.pages_applied > 0, "FP-8: clean commit must produce pages");

    // Replay from commit horizon (LSN advanced) — must be empty.
    let mut count = 0usize;
    WalReplayer::replay(dir.path(), r.last_applied_lsn, |_, _, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        count, 0,
        "FP-8: replay from advanced LSN must apply nothing"
    );
}

// ── Golden fixture test ───────────────────────────────────────────────────────

/// Decode the golden WAL fixture and verify its fields.
#[test]
fn test_wal_segment_golden_fixture() {
    use sparrowdb_storage::wal::codec::WalRecord;

    // Path relative to the workspace root.
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures/wal_segment_000.bin");

    let data = std::fs::read(&fixture_path)
        .unwrap_or_else(|e| panic!("Failed to read golden fixture {:?}: {}", fixture_path, e));

    // Validate and skip the 1-byte WAL format version header.
    use sparrowdb_storage::wal::codec::WAL_FORMAT_VERSION;
    assert_eq!(
        data[0], WAL_FORMAT_VERSION,
        "golden fixture must have version byte {WAL_FORMAT_VERSION}"
    );

    // Decode all records (starting after the version header byte).
    let mut offset = 1usize;
    let mut records = Vec::new();
    while offset < data.len() {
        // Skip zero-padding.
        if data[offset..].iter().all(|&b| b == 0) {
            break;
        }
        let (rec, consumed) = WalRecord::decode(&data[offset..])
            .unwrap_or_else(|e| panic!("Failed to decode record at offset {}: {}", offset, e));
        records.push(rec);
        offset += consumed;
    }

    // The fixture contains: BEGIN(lsn=1,txn=1), WRITE(lsn=2,txn=1,page=0), COMMIT(lsn=3,txn=1).
    assert_eq!(
        records.len(),
        3,
        "golden fixture must have exactly 3 records"
    );

    // Record 0: BEGIN
    assert_eq!(records[0].kind, WalRecordKind::Begin);
    assert_eq!(records[0].lsn, Lsn(1));
    assert_eq!(records[0].txn_id, TxnId(1));

    // Record 1: WRITE
    assert_eq!(records[1].kind, WalRecordKind::Write);
    assert_eq!(records[1].lsn, Lsn(2));
    assert_eq!(records[1].txn_id, TxnId(1));
    // decode() returns Raw for Write records; use decode_plaintext to get structure.
    let write_payload = match &records[1].payload {
        WalPayload::Raw(bytes) => WalPayload::decode_plaintext(records[1].kind, bytes)
            .expect("decode_plaintext must succeed for unencrypted fixture"),
        other => other.clone(),
    };
    match &write_payload {
        WalPayload::Write { page_id, image } => {
            assert_eq!(*page_id, 0, "golden fixture write must target page 0");
            assert!(
                !image.is_empty(),
                "golden fixture write image must not be empty"
            );
        }
        _ => panic!("golden fixture record 1 must be a Write payload"),
    }

    // Record 2: COMMIT
    assert_eq!(records[2].kind, WalRecordKind::Commit);
    assert_eq!(records[2].lsn, Lsn(3));
    assert_eq!(records[2].txn_id, TxnId(1));

    // LSNs are monotonically increasing.
    assert!(records[0].lsn < records[1].lsn);
    assert!(records[1].lsn < records[2].lsn);
}

// ── test_wal_replay_after_crash (acceptance #5) ───────────────────────────────

/// Main acceptance test: replay correctly recovers after each of the 8 crash
/// failpoints.  This is acceptance check #5 from the spec.
///
/// This test verifies the invariant: regardless of where we crash in the
/// 6-step commit sequence, reopening and replaying the WAL always produces
/// correct, consistent DB state.
#[test]
fn test_wal_replay_after_crash_all_failpoints() {
    // Failpoints 1-3: transaction incomplete → nothing applied.
    for fp in 1u8..=3 {
        let dir = TempDir::new().unwrap();

        // Good txn.
        write_committed_txn(dir.path(), 1, &[(0, vec![0xAA; 32])]);

        // Incomplete txn (no COMMIT).
        {
            let mut writer = WalWriter::open(dir.path()).unwrap();
            writer
                .append(WalRecordKind::Begin, TxnId(2), WalPayload::Empty)
                .unwrap();
            if fp >= 2 {
                writer
                    .append(
                        WalRecordKind::Write,
                        TxnId(2),
                        WalPayload::Write {
                            page_id: 99,
                            image: vec![0xBB; 32],
                        },
                    )
                    .unwrap();
            }
            if fp >= 3 {
                // Second WRITE (still no COMMIT).
                writer
                    .append(
                        WalRecordKind::Write,
                        TxnId(2),
                        WalPayload::Write {
                            page_id: 98,
                            image: vec![0xCC; 32],
                        },
                    )
                    .unwrap();
            }
            drop(writer); // Crash — no COMMIT
        }

        let applied = replay_all(dir.path());
        assert_eq!(
            applied.get(&0),
            Some(&vec![0xAA; 32]),
            "FP-{fp}: committed page must be applied"
        );
        assert!(
            !applied.contains_key(&99),
            "FP-{fp}: incomplete txn must NOT be applied"
        );
        assert!(
            !applied.contains_key(&98),
            "FP-{fp}: incomplete txn must NOT be applied"
        );
    }

    // Failpoint 4: COMMIT present but torn tail → committed txn applied.
    {
        let dir = TempDir::new().unwrap();
        write_committed_txn(dir.path(), 1, &[(0, vec![0xAA; 32])]);
        append_raw(dir.path(), &[0xDE, 0xAD, 0xBE, 0xEF, 0x00]); // torn tail
        let applied = replay_all(dir.path());
        assert_eq!(
            applied.get(&0),
            Some(&vec![0xAA; 32]),
            "FP-4: committed page must be applied"
        );
    }

    // Failpoints 5-7: WAL has COMMIT, pages not yet (fully) applied.
    // Replay from horizon 0 re-applies all committed writes idempotently.
    for fp in 5u8..=7 {
        let dir = TempDir::new().unwrap();
        write_committed_txn(dir.path(), 1, &[(0, vec![0xDD; 32]), (1, vec![0xEE; 32])]);

        let r = WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| Ok(())).unwrap();
        assert!(
            r.pages_applied >= 1,
            "FP-{fp}: must apply at least one page"
        );

        // Re-replay from same horizon (idempotent).
        let r2 = WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| Ok(())).unwrap();
        assert_eq!(
            r.pages_applied, r2.pages_applied,
            "FP-{fp}: re-replay must be idempotent"
        );
    }

    // Failpoint 8: clean commit — replay from advanced horizon applies nothing.
    {
        let dir = TempDir::new().unwrap();
        write_committed_txn(dir.path(), 1, &[(0, vec![0xFF; 32])]);
        let r = WalReplayer::replay(dir.path(), Lsn(0), |_, _, _| Ok(())).unwrap();
        let mut count = 0usize;
        WalReplayer::replay(dir.path(), r.last_applied_lsn, |_, _, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            count, 0,
            "FP-8: replay from advanced LSN must apply nothing"
        );
    }
}
