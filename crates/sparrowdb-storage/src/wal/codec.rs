//! WAL record codec — encode and decode WAL records to/from bytes.
//!
//! ## Binary layout (per record)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!   0       4   length: u32 LE  (covers everything after this field)
//!   4       1   kind: u8
//!   5       8   lsn: u64 LE
//!  13       8   txn_id: u64 LE
//!  21       *   payload (kind-specific, may be empty)
//!  21+n     4   crc32: u32 LE  (over bytes [4 .. 21+n])
//! ```
//!
//! Total fixed overhead: 4 (len) + 1 (kind) + 8 (lsn) + 8 (txn_id) + 4 (crc32) = 25 bytes.
//! The `length` field value = 1 + 8 + 8 + payload_len + 4 = 21 + payload_len.

use crc32fast::Hasher as Crc32Hasher;
use sparrowdb_common::{Error, Lsn, Result, TxnId};

// ── Record kind byte values ──────────────────────────────────────────────────

/// WAL record type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordKind {
    /// Begin a transaction.
    Begin = 0x01,
    /// Write (page mutation) within a transaction.
    Write = 0x02,
    /// Commit the transaction.
    Commit = 0x03,
    /// Abort the transaction.
    Abort = 0x04,
    /// Begin a checkpoint.
    CheckpointBegin = 0x05,
    /// End a checkpoint.
    CheckpointEnd = 0x06,
}

impl WalRecordKind {
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Self::Begin),
            0x02 => Ok(Self::Write),
            0x03 => Ok(Self::Commit),
            0x04 => Ok(Self::Abort),
            0x05 => Ok(Self::CheckpointBegin),
            0x06 => Ok(Self::CheckpointEnd),
            other => Err(Error::Corruption(format!(
                "unknown WAL record kind: {other:#x}"
            ))),
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

// ── Payload ──────────────────────────────────────────────────────────────────

/// Type-specific payload carried inside a WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalPayload {
    /// No payload (Begin, Commit, Abort, CheckpointBegin, CheckpointEnd).
    Empty,
    /// Page-image write record.
    Write {
        /// Target page identifier.
        page_id: u64,
        /// Raw page image bytes.
        image: Vec<u8>,
    },
}

impl WalPayload {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            WalPayload::Empty => vec![],
            WalPayload::Write { page_id, image } => {
                let mut buf = Vec::with_capacity(8 + 4 + image.len());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&(image.len() as u32).to_le_bytes());
                buf.extend_from_slice(image);
                buf
            }
        }
    }

    pub fn decode(kind: WalRecordKind, bytes: &[u8]) -> Result<Self> {
        match kind {
            WalRecordKind::Begin
            | WalRecordKind::Commit
            | WalRecordKind::Abort
            | WalRecordKind::CheckpointBegin
            | WalRecordKind::CheckpointEnd => {
                if !bytes.is_empty() {
                    return Err(Error::Corruption(format!(
                        "expected empty payload for {:?} but got {} bytes",
                        kind,
                        bytes.len()
                    )));
                }
                Ok(WalPayload::Empty)
            }
            WalRecordKind::Write => {
                if bytes.len() < 12 {
                    return Err(Error::Corruption(format!(
                        "Write payload too short: {} bytes",
                        bytes.len()
                    )));
                }
                let page_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                let image_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
                if bytes.len() < 12 + image_len {
                    return Err(Error::Corruption("Write payload image truncated".into()));
                }
                let image = bytes[12..12 + image_len].to_vec();
                Ok(WalPayload::Write { page_id, image })
            }
        }
    }
}

// ── Record ───────────────────────────────────────────────────────────────────

/// A fully-decoded WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub kind: WalRecordKind,
    pub payload: WalPayload,
}

// Fixed-overhead constants
const HEADER_LEN: usize = 4 + 1 + 8 + 8; // length + kind + lsn + txn_id = 21
const CRC_LEN: usize = 4;
// The `length` field covers: kind(1) + lsn(8) + txn_id(8) + payload + crc(4)
// i.e. `length = 21 + payload_len`

impl WalRecord {
    /// Encode this record into a byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let payload_bytes = self.payload.encode();
        let payload_len = payload_bytes.len();

        // The CRC covers: kind + lsn + txn_id + payload
        let crc_input_len = 1 + 8 + 8 + payload_len;
        let total_len = HEADER_LEN + payload_len + CRC_LEN;

        let mut buf = Vec::with_capacity(total_len);

        // length field: covers everything after the 4-byte length field itself
        let length_val = (1u32 + 8 + 8) + payload_len as u32 + CRC_LEN as u32;
        buf.extend_from_slice(&length_val.to_le_bytes());

        // kind
        buf.push(self.kind.as_byte());

        // lsn
        buf.extend_from_slice(&self.lsn.0.to_le_bytes());

        // txn_id
        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());

        // payload
        buf.extend_from_slice(&payload_bytes);

        // crc32 over [kind + lsn + txn_id + payload] = bytes [4..4+crc_input_len]
        let crc = compute_crc32(&buf[4..4 + crc_input_len]);
        buf.extend_from_slice(&crc.to_le_bytes());

        debug_assert_eq!(buf.len(), total_len);
        buf
    }

    /// Decode a WAL record from the start of `bytes`.
    ///
    /// Returns `(record, bytes_consumed)`.
    /// Returns `Err(ChecksumMismatch)` on CRC failure (torn page boundary).
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Corruption(format!(
                "WAL record too short: {} bytes (need at least {})",
                bytes.len(),
                HEADER_LEN
            )));
        }

        // length field
        let length_val = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let total_record_len = 4 + length_val;

        if bytes.len() < total_record_len {
            return Err(Error::Corruption(format!(
                "WAL record truncated: have {} bytes, need {}",
                bytes.len(),
                total_record_len
            )));
        }

        // kind
        let kind = WalRecordKind::from_byte(bytes[4])?;

        // lsn
        let lsn = u64::from_le_bytes(bytes[5..13].try_into().unwrap());

        // txn_id
        let txn_id = u64::from_le_bytes(bytes[13..21].try_into().unwrap());

        // payload bytes = everything between txn_id end and crc
        let payload_end = total_record_len - CRC_LEN;
        let payload_bytes = &bytes[21..payload_end];

        // crc
        let stored_crc =
            u32::from_le_bytes(bytes[payload_end..payload_end + 4].try_into().unwrap());

        // verify crc: covers [kind + lsn + txn_id + payload] = bytes[4..payload_end]
        let computed_crc = compute_crc32(&bytes[4..payload_end]);
        if computed_crc != stored_crc {
            return Err(Error::ChecksumMismatch);
        }

        let payload = WalPayload::decode(kind, payload_bytes)?;

        Ok((
            WalRecord {
                lsn: Lsn(lsn),
                txn_id: TxnId(txn_id),
                kind,
                payload,
            },
            total_record_len,
        ))
    }
}

// ── CRC32 helper ─────────────────────────────────────────────────────────────

pub fn compute_crc32(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(kind: WalRecordKind, lsn: u64, txn_id: u64, payload: WalPayload) -> WalRecord {
        WalRecord {
            lsn: Lsn(lsn),
            txn_id: TxnId(txn_id),
            kind,
            payload,
        }
    }

    #[test]
    fn test_wal_record_codec_round_trip_begin() {
        let rec = make_record(WalRecordKind::Begin, 1, 100, WalPayload::Empty);
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_wal_record_codec_round_trip_commit() {
        let rec = make_record(WalRecordKind::Commit, 3, 100, WalPayload::Empty);
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_wal_record_codec_round_trip_abort() {
        let rec = make_record(WalRecordKind::Abort, 4, 100, WalPayload::Empty);
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_wal_record_codec_round_trip_checkpoint_begin() {
        let rec = make_record(WalRecordKind::CheckpointBegin, 10, 0, WalPayload::Empty);
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_wal_record_codec_round_trip_checkpoint_end() {
        let rec = make_record(WalRecordKind::CheckpointEnd, 11, 0, WalPayload::Empty);
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_wal_record_codec_round_trip_write() {
        let image = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
        let rec = make_record(
            WalRecordKind::Write,
            2,
            100,
            WalPayload::Write {
                page_id: 42,
                image: image.clone(),
            },
        );
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, encoded.len());
        match &decoded.payload {
            WalPayload::Write {
                page_id,
                image: img,
            } => {
                assert_eq!(*page_id, 42);
                assert_eq!(img, &image);
            }
            _ => panic!("wrong payload variant"),
        }
    }

    #[test]
    fn test_wal_record_crc32_mismatch_detected() {
        let rec = make_record(WalRecordKind::Begin, 1, 100, WalPayload::Empty);
        let mut encoded = rec.encode();
        // Corrupt the last byte (CRC)
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let result = WalRecord::decode(&encoded);
        assert!(matches!(result, Err(Error::ChecksumMismatch)));
    }

    #[test]
    fn test_wal_record_crc32_covers_all_fields() {
        let rec = make_record(WalRecordKind::Begin, 1, 100, WalPayload::Empty);
        let mut encoded = rec.encode();
        // Corrupt the kind byte (byte 4)
        encoded[4] ^= 0x01;
        let result = WalRecord::decode(&encoded);
        // Either CRC mismatch or unknown kind
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_record_lsn_monotonic_encoding() {
        let r1 = make_record(WalRecordKind::Begin, 100, 1, WalPayload::Empty);
        let r2 = make_record(WalRecordKind::Begin, 200, 2, WalPayload::Empty);
        let e1 = r1.encode();
        let e2 = r2.encode();
        let (d1, _) = WalRecord::decode(&e1).unwrap();
        let (d2, _) = WalRecord::decode(&e2).unwrap();
        assert!(d1.lsn < d2.lsn);
    }

    #[test]
    fn test_wal_record_truncated_returns_error() {
        let rec = make_record(WalRecordKind::Begin, 1, 100, WalPayload::Empty);
        let encoded = rec.encode();
        // Provide only first 10 bytes
        let result = WalRecord::decode(&encoded[..10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_record_write_large_page() {
        let image = vec![0u8; 4096];
        let rec = make_record(
            WalRecordKind::Write,
            5,
            200,
            WalPayload::Write {
                page_id: 999,
                image,
            },
        );
        let encoded = rec.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.lsn.0, 5);
        assert_eq!(decoded.txn_id.0, 200);
        assert_eq!(consumed, encoded.len());
        match &decoded.payload {
            WalPayload::Write { page_id, image } => {
                assert_eq!(*page_id, 999);
                assert_eq!(image.len(), 4096);
            }
            _ => panic!("wrong payload"),
        }
    }

    #[test]
    fn test_decode_multiple_records_sequential() {
        let mut buf = Vec::new();
        for i in 1u64..=5 {
            let rec = make_record(WalRecordKind::Begin, i, i, WalPayload::Empty);
            buf.extend_from_slice(&rec.encode());
        }
        let mut offset = 0;
        let mut lsns = Vec::new();
        while offset < buf.len() {
            let (rec, consumed) = WalRecord::decode(&buf[offset..]).unwrap();
            lsns.push(rec.lsn.0);
            offset += consumed;
        }
        assert_eq!(lsns, vec![1, 2, 3, 4, 5]);
    }
}
