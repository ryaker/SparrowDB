//! Dual-metapage codec for `catalog.bin`.
//!
//! Layout (§10.2, §10.6 of the v3 spec):
//! - Metapage A at byte offset 128
//! - Metapage B at byte offset 640
//! - Each metapage is exactly 512 bytes
//!
//! Structure:
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!   0      16   page_header (magic u32, crc32c u32, version u32, flags u32)
//!  16       8   txn_id: u64
//!  24       8   catalog_root_page_id: u64
//!  32       8   node_root_page_id: u64
//!  40       8   edge_root_page_id: u64
//!  48       8   wal_checkpoint_lsn: u64
//!  56       8   global_node_count: u64
//!  64       8   global_edge_count: u64
//!  72       8   next_edge_id: u64
//!  80     432   reserved: zero-filled
//! ```
//!
//! The winning metapage is the valid one with the highest `txn_id`.
//! A metapage is valid when: magic matches AND crc32c matches.

use sparrowdb_common::{Error, Result};

/// Byte offset of Metapage A within `catalog.bin`.
pub const METAPAGE_A_OFFSET: u64 = 128;
/// Byte offset of Metapage B within `catalog.bin`.
pub const METAPAGE_B_OFFSET: u64 = 640;
/// Size in bytes of each metapage.
pub const METAPAGE_SIZE: usize = 512;

/// Page-family magic constant for Metapage (§10.4).
pub const METAPAGE_MAGIC: u32 = 0x4D455441;

/// Page header version for v1.
pub const PAGE_HEADER_VERSION: u32 = 1;

/// A decoded metapage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metapage {
    pub txn_id: u64,
    pub catalog_root_page_id: u64,
    pub node_root_page_id: u64,
    pub edge_root_page_id: u64,
    pub wal_checkpoint_lsn: u64,
    pub global_node_count: u64,
    pub global_edge_count: u64,
    pub next_edge_id: u64,
}

impl Metapage {
    /// Encode the metapage into exactly `METAPAGE_SIZE` bytes.
    pub fn encode(&self) -> [u8; METAPAGE_SIZE] {
        let mut buf = [0u8; METAPAGE_SIZE];
        // page_header: magic (4), crc32c placeholder (4), version (4), flags (4)
        buf[0..4].copy_from_slice(&METAPAGE_MAGIC.to_le_bytes());
        // crc32c at [4..8] — zeroed during calculation, filled after
        buf[8..12].copy_from_slice(&PAGE_HEADER_VERSION.to_le_bytes());
        // flags at [12..16] — zero
        // payload fields
        buf[16..24].copy_from_slice(&self.txn_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.catalog_root_page_id.to_le_bytes());
        buf[32..40].copy_from_slice(&self.node_root_page_id.to_le_bytes());
        buf[40..48].copy_from_slice(&self.edge_root_page_id.to_le_bytes());
        buf[48..56].copy_from_slice(&self.wal_checkpoint_lsn.to_le_bytes());
        buf[56..64].copy_from_slice(&self.global_node_count.to_le_bytes());
        buf[64..72].copy_from_slice(&self.global_edge_count.to_le_bytes());
        buf[72..80].copy_from_slice(&self.next_edge_id.to_le_bytes());
        // reserved [80..512] stays zero
        let crc = crate::crc32_of(&buf);
        buf[4..8].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a metapage from a 512-byte buffer.
    /// Returns `Err(Error::InvalidMagic)` or `Err(Error::ChecksumMismatch)` on corruption.
    pub fn decode(buf: &[u8; METAPAGE_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != METAPAGE_MAGIC {
            return Err(Error::InvalidMagic);
        }
        let stored_crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let computed = crate::crc32_zeroed_at(buf, 4, 4);
        if computed != stored_crc {
            return Err(Error::ChecksumMismatch);
        }
        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        if version != PAGE_HEADER_VERSION {
            return Err(Error::VersionMismatch);
        }
        Ok(Metapage {
            txn_id: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            catalog_root_page_id: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            node_root_page_id: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            edge_root_page_id: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            wal_checkpoint_lsn: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
            global_node_count: u64::from_le_bytes(buf[56..64].try_into().unwrap()),
            global_edge_count: u64::from_le_bytes(buf[64..72].try_into().unwrap()),
            next_edge_id: u64::from_le_bytes(buf[72..80].try_into().unwrap()),
        })
    }
}

/// Select the winning metapage from two candidates.
///
/// Winner = valid metapage with highest `txn_id`.
/// If both are valid, returns the one with the greater `txn_id`.
/// If exactly one is valid, returns that one.
/// If neither is valid, returns `Err(Error::Corruption)`.
pub fn select_winner(a: &[u8; METAPAGE_SIZE], b: &[u8; METAPAGE_SIZE]) -> Result<Metapage> {
    let a_result = Metapage::decode(a);
    let b_result = Metapage::decode(b);
    match (a_result, b_result) {
        (Ok(a), Ok(b)) => {
            if a.txn_id >= b.txn_id {
                Ok(a)
            } else {
                Ok(b)
            }
        }
        (Ok(a), Err(_)) => Ok(a),
        (Err(_), Ok(b)) => Ok(b),
        (Err(_), Err(_)) => Err(Error::Corruption("both metapages are invalid".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metapage(txn_id: u64) -> Metapage {
        Metapage {
            txn_id,
            catalog_root_page_id: 0,
            node_root_page_id: u64::MAX,
            edge_root_page_id: u64::MAX,
            wal_checkpoint_lsn: 0,
            global_node_count: 0,
            global_edge_count: 0,
            next_edge_id: 1,
        }
    }

    #[test]
    fn test_metapage_encode_decode_roundtrip() {
        let mp = sample_metapage(42);
        let encoded = mp.encode();
        assert_eq!(encoded.len(), METAPAGE_SIZE);
        let decoded = Metapage::decode(&encoded).expect("decode should succeed");
        assert_eq!(decoded, mp);
    }

    #[test]
    fn test_metapage_invalid_magic() {
        let mp = sample_metapage(1);
        let mut encoded = mp.encode();
        encoded[0] ^= 0xFF; // corrupt magic
        let result = Metapage::decode(&encoded);
        assert!(matches!(result, Err(Error::InvalidMagic)));
    }

    #[test]
    fn test_metapage_checksum_mismatch() {
        let mp = sample_metapage(1);
        let mut encoded = mp.encode();
        encoded[20] ^= 0xFF; // corrupt txn_id byte
        let result = Metapage::decode(&encoded);
        assert!(matches!(result, Err(Error::ChecksumMismatch)));
    }

    #[test]
    fn test_dual_metapage_winner_selection() {
        // A valid, B valid — A has higher txn_id
        let a = sample_metapage(10).encode();
        let b = sample_metapage(5).encode();
        let winner = select_winner(&a, &b).expect("should pick A");
        assert_eq!(winner.txn_id, 10);

        // A valid, B valid — B has higher txn_id
        let a2 = sample_metapage(3).encode();
        let b2 = sample_metapage(7).encode();
        let winner2 = select_winner(&a2, &b2).expect("should pick B");
        assert_eq!(winner2.txn_id, 7);

        // A valid, B corrupt
        let a3 = sample_metapage(5).encode();
        let mut b3 = sample_metapage(99).encode();
        b3[0] ^= 0xFF; // corrupt B's magic
        let winner3 = select_winner(&a3, &b3).expect("should pick A even though B has higher txn");
        assert_eq!(winner3.txn_id, 5);

        // A corrupt, B valid
        let mut a4 = sample_metapage(99).encode();
        a4[0] ^= 0xFF;
        let b4 = sample_metapage(2).encode();
        let winner4 = select_winner(&a4, &b4).expect("should pick B");
        assert_eq!(winner4.txn_id, 2);

        // Both corrupt
        let mut a5 = sample_metapage(1).encode();
        a5[0] ^= 0xFF;
        let mut b5 = sample_metapage(1).encode();
        b5[0] ^= 0xFF;
        let result5 = select_winner(&a5, &b5);
        assert!(matches!(result5, Err(Error::Corruption(_))));
    }

    #[test]
    fn test_metapage_size_is_512() {
        let mp = sample_metapage(1);
        let encoded = mp.encode();
        assert_eq!(encoded.len(), 512);
    }

    #[test]
    fn test_metapage_reserved_bytes_are_zero() {
        let mp = sample_metapage(1);
        let encoded = mp.encode();
        // bytes 80..512 are reserved and must be zero
        for (i, &byte) in encoded[80..512].iter().enumerate() {
            assert_eq!(byte, 0, "reserved byte at offset {} must be zero", 80 + i);
        }
    }

    #[test]
    fn test_metapage_offsets() {
        // Validate that the offsets match the spec
        assert_eq!(METAPAGE_A_OFFSET, 128);
        assert_eq!(METAPAGE_B_OFFSET, 640);
    }

    #[test]
    fn test_metapage_golden_fixture() {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests/fixtures/metapage_ab.bin");
        let data = std::fs::read(&fixture_path).expect("metapage_ab.bin fixture must exist");
        assert_eq!(
            data.len(),
            METAPAGE_SIZE * 2,
            "fixture must be 1024 bytes (2 metapages)"
        );

        let mut a_buf = [0u8; METAPAGE_SIZE];
        let mut b_buf = [0u8; METAPAGE_SIZE];
        a_buf.copy_from_slice(&data[..METAPAGE_SIZE]);
        b_buf.copy_from_slice(&data[METAPAGE_SIZE..]);

        let a = Metapage::decode(&a_buf).expect("fixture metapage A must be valid");
        let b = Metapage::decode(&b_buf).expect("fixture metapage B must be valid");

        // Fixture: A has txn_id=1, B has txn_id=2
        assert_eq!(a.txn_id, 1);
        assert_eq!(b.txn_id, 2);

        // Winner selection should pick B
        let winner = select_winner(&a_buf, &b_buf).expect("winner selection must succeed");
        assert_eq!(winner.txn_id, 2);

        // Round-trip: re-encode A and B, check byte-exact match
        let a_reencoded = a.encode();
        let b_reencoded = b.encode();
        assert_eq!(
            &a_reencoded[..],
            &data[..METAPAGE_SIZE],
            "A re-encode must be byte-exact"
        );
        assert_eq!(
            &b_reencoded[..],
            &data[METAPAGE_SIZE..],
            "B re-encode must be byte-exact"
        );
    }
}
