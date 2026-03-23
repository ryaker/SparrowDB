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
//!  21+n     4   crc32c: u32 LE  (Castagnoli, over bytes [4 .. 21+n])
//! ```
//!
//! Total fixed overhead: 4 (len) + 1 (kind) + 8 (lsn) + 8 (txn_id) + 4 (crc32c) = 25 bytes.
//! The `length` field value = 1 + 8 + 8 + payload_len + 4 = 21 + payload_len.
//!
//! ## WAL format version
//!
//! [`WAL_FORMAT_VERSION`] is stored as the first byte of every segment file.
//! Version 1 used standard CRC32 (IEEE 802.3).
//! Version 2 (current) uses CRC32C (Castagnoli / iSCSI), which has better
//! torn-write detection properties and hardware acceleration on SSE4.2 / ARM.

use sparrowdb_common::{Error, Lsn, Result, TxnId};

// ── WAL format version ────────────────────────────────────────────────────────

/// Version byte written as the first byte of every WAL segment file.
///
/// | Version | Checksum algorithm |
/// |---------|--------------------|
/// | 1       | CRC32 (IEEE 802.3) |
/// | 2       | CRC32C (Castagnoli) — current |
pub const WAL_FORMAT_VERSION: u8 = 2;

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
    /// Create a new node (Phase 7 mutation).
    NodeCreate = 0x10,
    /// Update a node property (Phase 7 mutation).
    NodeUpdate = 0x11,
    /// Delete a node (Phase 7 mutation).
    NodeDelete = 0x12,
    /// Create a new edge (Phase 7 mutation).
    EdgeCreate = 0x13,
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
            0x10 => Ok(Self::NodeCreate),
            0x11 => Ok(Self::NodeUpdate),
            0x12 => Ok(Self::NodeDelete),
            0x13 => Ok(Self::EdgeCreate),
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

/// A (key, value) property pair serialised for WAL records.
///
/// The value is stored as raw bytes (currently the little-endian `u64`
/// representation of `node_store::Value::to_u64()`).
pub type WalProp = (String, Vec<u8>);

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
    /// Opaque pre-encoded (possibly encrypted) payload bytes.
    ///
    /// Used internally by the WAL writer when encryption is enabled: the
    /// plaintext `WalPayload::encode()` bytes are encrypted and stored here
    /// so the codec writes them verbatim.  The replayer decrypts them and
    /// calls `WalPayload::decode_raw()` to recover the structured payload.
    Raw(Vec<u8>),
    /// Phase 7 — node was created.
    NodeCreate {
        node_id: u64,
        label_id: u32,
        props: Vec<WalProp>,
    },
    /// Phase 7 — a single property on a node was updated.
    NodeUpdate {
        node_id: u64,
        key: String,
        col_id: u32,
        before: Vec<u8>,
        after: Vec<u8>,
    },
    /// Phase 7 — a node was deleted.
    NodeDelete { node_id: u64 },
    /// Phase 7 — an edge was created.
    EdgeCreate {
        edge_id: u64,
        src: u64,
        dst: u64,
        rel_type: String,
        props: Vec<WalProp>,
    },
}

// ── Payload encoding helpers ──────────────────────────────────────────────────

/// Encode a `Vec<WalProp>` into a length-prefixed byte sequence.
///
/// Format: `[count: u32 LE]` followed by for each prop:
///   `[key_len: u32 LE][key bytes][val_len: u32 LE][val bytes]`
fn encode_props(props: &[WalProp]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(props.len() as u32).to_le_bytes());
    for (key, val) in props {
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        buf.extend_from_slice(val);
    }
    buf
}

/// Decode a `Vec<WalProp>` from `bytes` starting at `offset`.
///
/// Returns `(props, bytes_consumed)`.
fn decode_props(bytes: &[u8], offset: usize) -> Result<(Vec<WalProp>, usize)> {
    let mut pos = offset;
    if bytes.len() < pos + 4 {
        return Err(Error::Corruption("props count truncated".into()));
    }
    let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut props = Vec::with_capacity(count);
    for _ in 0..count {
        if bytes.len() < pos + 4 {
            return Err(Error::Corruption("prop key_len truncated".into()));
        }
        let key_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if bytes.len() < pos + key_len {
            return Err(Error::Corruption("prop key truncated".into()));
        }
        let key = String::from_utf8(bytes[pos..pos + key_len].to_vec())
            .map_err(|_| Error::Corruption("prop key not UTF-8".into()))?;
        pos += key_len;
        if bytes.len() < pos + 4 {
            return Err(Error::Corruption("prop val_len truncated".into()));
        }
        let val_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if bytes.len() < pos + val_len {
            return Err(Error::Corruption("prop val truncated".into()));
        }
        let val = bytes[pos..pos + val_len].to_vec();
        pos += val_len;
        props.push((key, val));
    }
    Ok((props, pos - offset))
}

/// Encode a length-prefixed string.
fn encode_str(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut buf = Vec::with_capacity(4 + b.len());
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
    buf
}

/// Decode a length-prefixed string from `bytes[offset..]`.
///
/// Returns `(string, bytes_consumed)`.
fn decode_str(bytes: &[u8], offset: usize) -> Result<(String, usize)> {
    if bytes.len() < offset + 4 {
        return Err(Error::Corruption("string length truncated".into()));
    }
    let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    if bytes.len() < offset + 4 + len {
        return Err(Error::Corruption("string data truncated".into()));
    }
    let s = String::from_utf8(bytes[offset + 4..offset + 4 + len].to_vec())
        .map_err(|_| Error::Corruption("string not UTF-8".into()))?;
    Ok((s, 4 + len))
}

impl WalPayload {
    /// Encode to on-disk bytes (plaintext structured form).
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
            // Raw bytes are already in their final on-disk form.
            WalPayload::Raw(bytes) => bytes.clone(),
            WalPayload::NodeCreate {
                node_id,
                label_id,
                props,
            } => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&node_id.to_le_bytes());
                buf.extend_from_slice(&label_id.to_le_bytes());
                buf.extend_from_slice(&encode_props(props));
                buf
            }
            WalPayload::NodeUpdate {
                node_id,
                key,
                col_id,
                before,
                after,
            } => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&node_id.to_le_bytes());
                buf.extend_from_slice(&col_id.to_le_bytes());
                buf.extend_from_slice(&encode_str(key));
                buf.extend_from_slice(&(before.len() as u32).to_le_bytes());
                buf.extend_from_slice(before);
                buf.extend_from_slice(&(after.len() as u32).to_le_bytes());
                buf.extend_from_slice(after);
                buf
            }
            WalPayload::NodeDelete { node_id } => node_id.to_le_bytes().to_vec(),
            WalPayload::EdgeCreate {
                edge_id,
                src,
                dst,
                rel_type,
                props,
            } => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&edge_id.to_le_bytes());
                buf.extend_from_slice(&src.to_le_bytes());
                buf.extend_from_slice(&dst.to_le_bytes());
                buf.extend_from_slice(&encode_str(rel_type));
                buf.extend_from_slice(&encode_props(props));
                buf
            }
        }
    }

    /// Decode from the raw byte slice in a WAL record.
    ///
    /// This is used during WAL replay to reconstruct the payload.  For records
    /// without a payload (Begin, Commit, Abort, CheckpointBegin, CheckpointEnd)
    /// it returns `Empty`.  For Write records it returns `Raw(bytes)` so that
    /// the replayer can optionally decrypt the bytes first (P6-3) and then call
    /// [`decode_plaintext`] to get the structured `Write { page_id, image }`.
    /// For mutation records (NodeCreate, NodeUpdate, NodeDelete, EdgeCreate) the
    /// bytes are decoded inline and the structured variant is returned directly.
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
                // Return the raw bytes; the replayer will decrypt if needed,
                // then call decode_plaintext to get the Write variant.
                Ok(WalPayload::Raw(bytes.to_vec()))
            }
            // Mutation records: decode inline (not encrypted).
            WalRecordKind::NodeCreate
            | WalRecordKind::NodeUpdate
            | WalRecordKind::NodeDelete
            | WalRecordKind::EdgeCreate => Self::decode_plaintext(kind, bytes),
        }
    }

    /// Decode a plaintext payload from raw bytes.
    ///
    /// Called by the replayer after optional decryption.
    /// Returns `Err(Corruption)` if the bytes do not match the expected layout.
    pub fn decode_plaintext(kind: WalRecordKind, bytes: &[u8]) -> Result<Self> {
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
            WalRecordKind::NodeCreate => {
                // node_id(8) + label_id(4) + props(variable)
                if bytes.len() < 12 {
                    return Err(Error::Corruption("NodeCreate payload too short".into()));
                }
                let node_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                let label_id = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
                let (props, _consumed) = decode_props(bytes, 12)?;
                Ok(WalPayload::NodeCreate {
                    node_id,
                    label_id,
                    props,
                })
            }
            WalRecordKind::NodeUpdate => {
                // node_id(8) + col_id(4) + key(variable) + before(variable) + after(variable)
                if bytes.len() < 12 {
                    return Err(Error::Corruption("NodeUpdate payload too short".into()));
                }
                let node_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                let col_id = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
                let (key, kconsumed) = decode_str(bytes, 12)?;
                let mut pos = 12 + kconsumed;
                if bytes.len() < pos + 4 {
                    return Err(Error::Corruption("NodeUpdate before_len truncated".into()));
                }
                let before_len =
                    u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if bytes.len() < pos + before_len {
                    return Err(Error::Corruption("NodeUpdate before truncated".into()));
                }
                let before = bytes[pos..pos + before_len].to_vec();
                pos += before_len;
                if bytes.len() < pos + 4 {
                    return Err(Error::Corruption("NodeUpdate after_len truncated".into()));
                }
                let after_len =
                    u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if bytes.len() < pos + after_len {
                    return Err(Error::Corruption("NodeUpdate after truncated".into()));
                }
                let after = bytes[pos..pos + after_len].to_vec();
                Ok(WalPayload::NodeUpdate {
                    node_id,
                    key,
                    col_id,
                    before,
                    after,
                })
            }
            WalRecordKind::NodeDelete => {
                if bytes.len() < 8 {
                    return Err(Error::Corruption("NodeDelete payload too short".into()));
                }
                let node_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                Ok(WalPayload::NodeDelete { node_id })
            }
            WalRecordKind::EdgeCreate => {
                // edge_id(8) + src(8) + dst(8) + rel_type(variable) + props(variable)
                if bytes.len() < 24 {
                    return Err(Error::Corruption("EdgeCreate payload too short".into()));
                }
                let edge_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                let src = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
                let dst = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
                let (rel_type, rconsumed) = decode_str(bytes, 24)?;
                let (props, _pconsumed) = decode_props(bytes, 24 + rconsumed)?;
                Ok(WalPayload::EdgeCreate {
                    edge_id,
                    src,
                    dst,
                    rel_type,
                    props,
                })
            }
        }
    }

    /// Alias for [`decode_plaintext`] — used at the decryption path in the replayer.
    pub fn decode_raw(kind: WalRecordKind, plaintext_bytes: &[u8]) -> Result<Self> {
        Self::decode_plaintext(kind, plaintext_bytes)
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

        // crc32c over [kind + lsn + txn_id + payload] = bytes [4..4+crc_input_len]
        let crc = compute_crc32c(&buf[4..4 + crc_input_len]);
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

        // The minimum length covers kind(1) + lsn(8) + txn_id(8) + crc(4) = 21.
        // A value below 21 would cause payload_end to underflow when computing
        // `total_record_len - CRC_LEN`, so we reject it as corrupt.
        if length_val < 21 {
            return Err(Error::Corruption(format!(
                "WAL record length field too small: {length_val} (minimum 21)"
            )));
        }

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

        // verify crc32c: covers [kind + lsn + txn_id + payload] = bytes[4..payload_end]
        let computed_crc = compute_crc32c(&bytes[4..payload_end]);
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

// ── CRC32C helper ────────────────────────────────────────────────────────────

/// Compute CRC32C (Castagnoli) checksum over `data`.
///
/// Uses hardware acceleration (SSE4.2 / ARM CRC32C instructions) when
/// available via the `crc32c` crate.
pub fn compute_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
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
        // decode() returns Raw for Write records; decode_plaintext() gives Write.
        let (decoded_raw, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded_raw.lsn.0, 2);
        assert_eq!(decoded_raw.txn_id.0, 100);
        let structured = match &decoded_raw.payload {
            WalPayload::Raw(bytes) => {
                WalPayload::decode_plaintext(decoded_raw.kind, bytes).unwrap()
            }
            other => other.clone(),
        };
        match &structured {
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
    fn test_wal_record_crc32c_mismatch_detected() {
        let rec = make_record(WalRecordKind::Begin, 1, 100, WalPayload::Empty);
        let mut encoded = rec.encode();
        // Corrupt the last byte (CRC32C)
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let result = WalRecord::decode(&encoded);
        assert!(matches!(result, Err(Error::ChecksumMismatch)));
    }

    #[test]
    fn test_wal_record_crc32c_covers_all_fields() {
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
        // decode() returns Raw for Write records; decode_plaintext() gives Write.
        let (decoded_raw, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(decoded_raw.lsn.0, 5);
        assert_eq!(decoded_raw.txn_id.0, 200);
        assert_eq!(consumed, encoded.len());
        let structured = match &decoded_raw.payload {
            WalPayload::Raw(bytes) => {
                WalPayload::decode_plaintext(decoded_raw.kind, bytes).unwrap()
            }
            other => other.clone(),
        };
        match &structured {
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

#[cfg(test)]
mod generate_fixture {
    use super::*;

    /// Regenerate the golden WAL fixture with CRC32C checksums.
    /// Run with: cargo test -p sparrowdb-storage -- generate_wal_fixture --ignored --nocapture
    #[test]
    #[ignore]
    fn generate_wal_fixture() {
        let mut segment = vec![WAL_FORMAT_VERSION];

        let begin = WalRecord {
            lsn: Lsn(1),
            txn_id: TxnId(1),
            kind: WalRecordKind::Begin,
            payload: WalPayload::Empty,
        };
        segment.extend_from_slice(&begin.encode());

        let write = WalRecord {
            lsn: Lsn(2),
            txn_id: TxnId(1),
            kind: WalRecordKind::Write,
            payload: WalPayload::Write {
                page_id: 0,
                image: vec![0xABu8; 32],
            },
        };
        segment.extend_from_slice(&write.encode());

        let commit = WalRecord {
            lsn: Lsn(3),
            txn_id: TxnId(1),
            kind: WalRecordKind::Commit,
            payload: WalPayload::Empty,
        };
        segment.extend_from_slice(&commit.encode());

        let out_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests/fixtures/wal_segment_000.bin");

        std::fs::write(&out_path, &segment).expect("write fixture");
        println!("Written {} bytes to {:?}", segment.len(), out_path);

        // Verify round-trip decode.
        let data = std::fs::read(&out_path).unwrap();
        assert_eq!(data[0], WAL_FORMAT_VERSION);
        let mut offset = 1usize;
        let mut count = 0;
        while offset < data.len() {
            let (rec, consumed) = WalRecord::decode(&data[offset..]).unwrap();
            println!("Record {}: {:?} lsn={}", count, rec.kind, rec.lsn.0);
            offset += consumed;
            count += 1;
        }
        assert_eq!(count, 3, "fixture must have 3 records");
    }
}
