//! SPA-105 — Golden fixture freeze: binary compatibility round-trip + corruption tests.
//!
//! This test suite loads each committed fixture from `tests/fixtures/`, decodes it
//! with the current code, verifies the decoded fields match known-good values, and
//! then flips a single byte to confirm the decoder rejects corrupted data.
//!
//! ## Covered fixtures
//!
//! | Fixture                | Format                                       |
//! |------------------------|----------------------------------------------|
//! | `metapage_ab.bin`      | Dual metapage A + B (2 × 512 bytes)          |
//! | `wal_segment_000.bin`  | WAL segment: version byte + 3 records        |
//! | `catalog_tlv.bin`      | TLV catalog: 3 labels + 2 rel tables         |
//! | `csr_forward.bin`      | CSR forward edge file (5 nodes, 8 edges)     |
//! | `csr_backward.bin`     | CSR backward edge file (5 nodes, 8 edges)    |
//! | `encrypted_page.bin`   | XChaCha20-Poly1305 encrypted page            |
//!
//! Run: `cargo test -p sparrowdb spa_105`

use sparrowdb_catalog::tlv::{LabelEntry, RelTableEntry, TlvEntry};
use sparrowdb_storage::csr::{CsrBackward, CsrForward};
use sparrowdb_storage::encryption::EncryptionContext;
use sparrowdb_storage::metapage::{Metapage, METAPAGE_SIZE};
use sparrowdb_storage::wal::codec::{WalRecord, WalRecordKind, WAL_FORMAT_VERSION};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Resolve a fixture path relative to the repository root.
fn fixture_path(name: &str) -> std::path::PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent() // crates/
        .unwrap()
        .parent() // repo root
        .unwrap()
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn load_fixture(name: &str) -> Vec<u8> {
    let path = fixture_path(name);
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "failed to load fixture {}: {e}\nExpected at: {}",
            name,
            path.display()
        )
    })
}

// ── Metapage A+B ─────────────────────────────────────────────────────────────

#[test]
fn metapage_ab_round_trip() {
    let data = load_fixture("metapage_ab.bin");
    assert_eq!(
        data.len(),
        2 * METAPAGE_SIZE,
        "metapage_ab.bin must be exactly 1024 bytes (2 × 512)"
    );

    let buf_a: &[u8; METAPAGE_SIZE] = data[..METAPAGE_SIZE].try_into().unwrap();
    let buf_b: &[u8; METAPAGE_SIZE] = data[METAPAGE_SIZE..].try_into().unwrap();

    let meta_a = Metapage::decode(buf_a).expect("metapage A decode must succeed");
    let meta_b = Metapage::decode(buf_b).expect("metapage B decode must succeed");

    // Metapage A: txn_id=1
    assert_eq!(meta_a.txn_id, 1);
    assert_eq!(meta_a.catalog_root_page_id, 0);
    assert_eq!(meta_a.node_root_page_id, u64::MAX);
    assert_eq!(meta_a.edge_root_page_id, u64::MAX);
    assert_eq!(meta_a.wal_checkpoint_lsn, 0);
    assert_eq!(meta_a.global_node_count, 0);
    assert_eq!(meta_a.global_edge_count, 0);
    assert_eq!(meta_a.next_edge_id, 1);

    // Metapage B: txn_id=2 (higher → winner)
    assert_eq!(meta_b.txn_id, 2);
    assert_eq!(meta_b.catalog_root_page_id, 0);
    assert_eq!(meta_b.node_root_page_id, u64::MAX);
    assert_eq!(meta_b.edge_root_page_id, u64::MAX);
    assert_eq!(meta_b.wal_checkpoint_lsn, 0);
    assert_eq!(meta_b.global_node_count, 0);
    assert_eq!(meta_b.global_edge_count, 0);
    assert_eq!(meta_b.next_edge_id, 1);

    // Re-encode round-trip: encode → decode produces identical struct.
    let re_a = Metapage::decode(&meta_a.encode()).unwrap();
    assert_eq!(meta_a, re_a, "metapage A encode→decode round-trip failed");

    let re_b = Metapage::decode(&meta_b.encode()).unwrap();
    assert_eq!(meta_b, re_b, "metapage B encode→decode round-trip failed");

    // select_winner should pick B (higher txn_id).
    let winner =
        sparrowdb_storage::metapage::select_winner(buf_a, buf_b).expect("select_winner must work");
    assert_eq!(winner.txn_id, 2, "winner must be metapage B (txn_id=2)");
}

#[test]
fn metapage_corruption_detected() {
    let data = load_fixture("metapage_ab.bin");
    let mut buf_a: [u8; METAPAGE_SIZE] = data[..METAPAGE_SIZE].try_into().unwrap();

    // Flip one byte in the payload region (txn_id at offset 16).
    buf_a[16] ^= 0xFF;

    let result = Metapage::decode(&buf_a);
    assert!(
        result.is_err(),
        "corrupted metapage must fail to decode (CRC mismatch)"
    );
}

#[test]
fn metapage_magic_corruption_detected() {
    let data = load_fixture("metapage_ab.bin");
    let mut buf_a: [u8; METAPAGE_SIZE] = data[..METAPAGE_SIZE].try_into().unwrap();

    // Corrupt the magic bytes at offset 0.
    buf_a[0] ^= 0xFF;

    let result = Metapage::decode(&buf_a);
    assert!(
        result.is_err(),
        "corrupted magic must fail to decode (InvalidMagic)"
    );
}

// ── WAL segment ──────────────────────────────────────────────────────────────

#[test]
fn wal_segment_round_trip() {
    let data = load_fixture("wal_segment_000.bin");
    assert_eq!(data.len(), 120, "wal_segment_000.bin must be 120 bytes");

    // First byte is the WAL format version.
    assert_eq!(
        data[0], WAL_FORMAT_VERSION,
        "WAL segment version must be {WAL_FORMAT_VERSION}"
    );

    // Decode records sequentially after the version byte.
    let mut pos = 1usize;
    let mut records = Vec::new();
    while pos < data.len() {
        let (record, consumed) = WalRecord::decode(&data[pos..])
            .unwrap_or_else(|e| panic!("WAL record decode at offset {pos} failed: {e}"));
        records.push(record);
        pos += consumed;
    }

    assert_eq!(records.len(), 3, "segment must contain exactly 3 records");

    // Record 0: Begin (kind=0x01, lsn=1, txn=1)
    assert_eq!(records[0].kind, WalRecordKind::Begin);
    assert_eq!(records[0].lsn.0, 1);
    assert_eq!(records[0].txn_id.0, 1);

    // Record 1: Write (kind=0x02, lsn=2, txn=1)
    assert_eq!(records[1].kind, WalRecordKind::Write);
    assert_eq!(records[1].lsn.0, 2);
    assert_eq!(records[1].txn_id.0, 1);

    // Record 2: Commit (kind=0x03, lsn=3, txn=1)
    assert_eq!(records[2].kind, WalRecordKind::Commit);
    assert_eq!(records[2].lsn.0, 3);
    assert_eq!(records[2].txn_id.0, 1);

    // Re-encode round-trip: each record's encode→decode must produce the same record.
    for (i, rec) in records.iter().enumerate() {
        let encoded = rec.encode();
        let (decoded, _) = WalRecord::decode(&encoded)
            .unwrap_or_else(|e| panic!("re-encode→decode failed for record {i}: {e}"));
        assert_eq!(
            rec.kind, decoded.kind,
            "record {i} kind mismatch after round-trip"
        );
        assert_eq!(
            rec.lsn, decoded.lsn,
            "record {i} lsn mismatch after round-trip"
        );
        assert_eq!(
            rec.txn_id, decoded.txn_id,
            "record {i} txn_id mismatch after round-trip"
        );
    }
}

#[test]
fn wal_segment_corruption_detected() {
    let data = load_fixture("wal_segment_000.bin");

    // Corrupt one byte inside the first record's payload (offset 10 = inside lsn field).
    let mut corrupted = data.clone();
    corrupted[10] ^= 0xFF;

    // Try to decode the first record after the version byte.
    let result = WalRecord::decode(&corrupted[1..]);
    assert!(
        result.is_err(),
        "corrupted WAL record must fail with checksum mismatch"
    );
}

// ── Catalog TLV ──────────────────────────────────────────────────────────────

#[test]
fn catalog_tlv_round_trip() {
    let data = load_fixture("catalog_tlv.bin");
    assert_eq!(data.len(), 105, "catalog_tlv.bin must be 105 bytes");

    let entries = TlvEntry::decode_all(&data).expect("TLV decode must succeed");
    assert_eq!(
        entries.len(),
        5,
        "catalog must have 5 entries (3 labels + 2 rel tables)"
    );

    // Verify labels.
    let labels: Vec<&LabelEntry> = entries
        .iter()
        .filter_map(|e| match e {
            TlvEntry::Label(l) => Some(l),
            _ => None,
        })
        .collect();
    assert_eq!(labels.len(), 3);
    assert_eq!(labels[0].label_id, 0);
    assert_eq!(labels[0].name, "Person");
    assert_eq!(labels[1].label_id, 1);
    assert_eq!(labels[1].name, "Movie");
    assert_eq!(labels[2].label_id, 2);
    assert_eq!(labels[2].name, "Director");

    // Verify relationship tables.
    let rels: Vec<&RelTableEntry> = entries
        .iter()
        .filter_map(|e| match e {
            TlvEntry::RelTable(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(rels.len(), 2);
    assert_eq!(rels[0].rel_table_id, 0);
    assert_eq!(rels[0].src_label_id, 0);
    assert_eq!(rels[0].dst_label_id, 1);
    assert_eq!(rels[0].rel_type, "ACTED_IN");
    assert_eq!(rels[1].rel_table_id, 1);
    assert_eq!(rels[1].src_label_id, 2);
    assert_eq!(rels[1].dst_label_id, 1);
    assert_eq!(rels[1].rel_type, "DIRECTED");

    // Re-encode round-trip.
    let mut re_encoded = Vec::new();
    for entry in &entries {
        re_encoded.extend_from_slice(&entry.encode());
    }
    let re_decoded = TlvEntry::decode_all(&re_encoded).expect("re-decode must succeed");
    assert_eq!(
        entries, re_decoded,
        "TLV encode→decode round-trip must produce identical entries"
    );
}

#[test]
fn catalog_tlv_corruption_detected() {
    let data = load_fixture("catalog_tlv.bin");

    // Corrupt the first entry's tag (bytes 0-1).
    let mut corrupted = data.clone();
    corrupted[0] = 0xFF;
    corrupted[1] = 0xFF;

    let result = TlvEntry::decode_all(&corrupted);
    assert!(
        result.is_err(),
        "corrupted TLV tag must cause decode to fail"
    );
}

#[test]
fn catalog_tlv_truncation_detected() {
    let data = load_fixture("catalog_tlv.bin");

    // Truncate the data mid-entry.
    let truncated = &data[..10];

    let result = TlvEntry::decode_all(truncated);
    assert!(
        result.is_err(),
        "truncated TLV stream must cause decode to fail"
    );
}

// ── CSR forward ──────────────────────────────────────────────────────────────

#[test]
fn csr_forward_round_trip() {
    let data = load_fixture("csr_forward.bin");
    assert_eq!(data.len(), 120, "csr_forward.bin must be 120 bytes");

    let csr = CsrForward::decode(&data).expect("CSR forward decode must succeed");
    assert_eq!(csr.n_nodes(), 5);
    assert_eq!(csr.n_edges(), 8);

    // Verify neighbor lists for each node.
    assert_eq!(csr.neighbors(0), &[1, 2]); // node 0 → {1, 2}
    assert_eq!(csr.neighbors(1), &[3]); // node 1 → {3}
    assert_eq!(csr.neighbors(2), &[3, 4]); // node 2 → {3, 4}
    assert_eq!(csr.neighbors(3), &[4]); // node 3 → {4}
    assert_eq!(csr.neighbors(4), &[0, 1]); // node 4 → {0, 1}

    // Re-encode round-trip.
    let re_encoded = csr.encode();
    assert_eq!(
        data, re_encoded,
        "CSR forward encode must produce identical bytes"
    );

    let re_decoded = CsrForward::decode(&re_encoded).expect("re-decode must succeed");
    assert_eq!(re_decoded.n_nodes(), 5);
    assert_eq!(re_decoded.n_edges(), 8);
}

#[test]
fn csr_forward_corruption_detected() {
    let data = load_fixture("csr_forward.bin");

    // Corrupt n_nodes field (first 8 bytes) to an absurdly large value.
    let mut corrupted = data.clone();
    corrupted[0] = 0xFF;
    corrupted[1] = 0xFF;
    corrupted[2] = 0xFF;
    corrupted[3] = 0xFF;

    let result = CsrForward::decode(&corrupted);
    assert!(
        result.is_err(),
        "corrupted n_nodes must cause CSR decode to fail (file too short for claimed offsets)"
    );
}

// ── CSR backward ─────────────────────────────────────────────────────────────

#[test]
fn csr_backward_round_trip() {
    let data = load_fixture("csr_backward.bin");
    assert_eq!(data.len(), 120, "csr_backward.bin must be 120 bytes");

    let csr = CsrBackward::decode(&data).expect("CSR backward decode must succeed");
    assert_eq!(csr.n_nodes(), 5);
    assert_eq!(csr.n_edges(), 8);

    // Verify predecessor lists for each node (reverse edges).
    assert_eq!(csr.predecessors(0), &[4]); // node 0 ← {4}
    assert_eq!(csr.predecessors(1), &[0, 4]); // node 1 ← {0, 4}
    assert_eq!(csr.predecessors(2), &[0]); // node 2 ← {0}
    assert_eq!(csr.predecessors(3), &[1, 2]); // node 3 ← {1, 2}
    assert_eq!(csr.predecessors(4), &[2, 3]); // node 4 ← {2, 3}

    // Re-encode round-trip.
    let re_encoded = csr.encode();
    assert_eq!(
        data, re_encoded,
        "CSR backward encode must produce identical bytes"
    );
}

#[test]
fn csr_backward_corruption_detected() {
    let data = load_fixture("csr_backward.bin");

    // Corrupt n_nodes to a huge value.
    let mut corrupted = data.clone();
    corrupted[0] = 0xFF;
    corrupted[1] = 0xFF;
    corrupted[2] = 0xFF;
    corrupted[3] = 0xFF;

    let result = CsrBackward::decode(&corrupted);
    assert!(
        result.is_err(),
        "corrupted n_nodes must cause CSR backward decode to fail"
    );
}

// ── Encrypted page ───────────────────────────────────────────────────────────

#[test]
fn encrypted_page_round_trip() {
    let data = load_fixture("encrypted_page.bin");
    // 512-byte plaintext + 24-byte nonce + 16-byte auth tag = 552 bytes.
    assert_eq!(
        data.len(),
        552,
        "encrypted_page.bin must be 552 bytes (512 + 40)"
    );

    // Key = [0x42; 32], page_id = 0, plaintext = [0xAB; 512]
    let ctx = EncryptionContext::with_key([0x42u8; 32]);
    let plaintext = ctx
        .decrypt_page(0, &data)
        .expect("decrypt of golden fixture must succeed");

    assert_eq!(plaintext.len(), 512);
    assert!(
        plaintext.iter().all(|&b| b == 0xAB),
        "decrypted plaintext must be 512 bytes of 0xAB"
    );

    // Re-encrypt and decrypt round-trip (nonce will differ, but plaintext must match).
    let re_encrypted = ctx
        .encrypt_page(0, &plaintext)
        .expect("re-encrypt must succeed");
    assert_eq!(re_encrypted.len(), 552);

    let re_decrypted = ctx
        .decrypt_page(0, &re_encrypted)
        .expect("re-decrypt must succeed");
    assert_eq!(
        plaintext, re_decrypted,
        "encrypt→decrypt round-trip must preserve plaintext"
    );
}

#[test]
fn encrypted_page_wrong_key_rejected() {
    let data = load_fixture("encrypted_page.bin");

    // Try to decrypt with the wrong key.
    let wrong_ctx = EncryptionContext::with_key([0x99u8; 32]);
    let result = wrong_ctx.decrypt_page(0, &data);
    assert!(
        result.is_err(),
        "decrypt with wrong key must fail (AEAD auth failure)"
    );
}

#[test]
fn encrypted_page_wrong_page_id_rejected() {
    let data = load_fixture("encrypted_page.bin");

    // Correct key but wrong page_id (AAD mismatch).
    let ctx = EncryptionContext::with_key([0x42u8; 32]);
    let result = ctx.decrypt_page(999, &data);
    assert!(
        result.is_err(),
        "decrypt with wrong page_id must fail (AAD mismatch)"
    );
}

#[test]
fn encrypted_page_bit_flip_rejected() {
    let data = load_fixture("encrypted_page.bin");

    // Flip one bit in the ciphertext region (after the 24-byte nonce).
    let mut corrupted = data.clone();
    corrupted[30] ^= 0x01;

    let ctx = EncryptionContext::with_key([0x42u8; 32]);
    let result = ctx.decrypt_page(0, &corrupted);
    assert!(
        result.is_err(),
        "bit-flipped ciphertext must fail AEAD authentication"
    );
}

// ── Cross-fixture: all fixtures exist ────────────────────────────────────────

#[test]
fn all_golden_fixtures_exist() {
    let required = [
        "metapage_ab.bin",
        "wal_segment_000.bin",
        "catalog_tlv.bin",
        "csr_forward.bin",
        "csr_backward.bin",
        "encrypted_page.bin",
    ];

    for name in &required {
        let path = fixture_path(name);
        assert!(
            path.exists(),
            "required golden fixture missing: {} (expected at {})",
            name,
            path.display()
        );
    }
}
