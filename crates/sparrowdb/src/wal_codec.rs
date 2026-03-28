// ── WAL mutation emission and value encoding ──────────────────────────────────

use crate::types::{StagedUpdate, WalMutation};
use sparrowdb_common::TxnId;
use sparrowdb_storage::node_store::Value;
use sparrowdb_storage::wal::codec::{WalPayload, WalRecordKind};
use sparrowdb_storage::wal::writer::WalWriter;
use std::sync::Mutex;

// ── WAL value type tags ────────────────────────────────────────────────────────

// Type tags for the explicit WAL value encoding.
// Byte 0 of every new-format WAL value payload is one of these tags.
pub(crate) const WAL_TAG_INT64: u8 = 0x01;
pub(crate) const WAL_TAG_BYTES_INLINE: u8 = 0x02;
pub(crate) const WAL_TAG_BYTES_OVERFLOW: u8 = 0x03;
pub(crate) const WAL_TAG_FLOAT: u8 = 0x04;

/// Encode a `Value` as a WAL property byte slice.
///
/// New format (tagged, written for all new records):
/// - `Int64`  → `[TAG_INT64(0x01), i64_le_bytes(8)]`  — 9 bytes total
/// - `Bytes` inline (≤ 255 bytes) → `[TAG_BYTES_INLINE(0x02), len(1), data...]`
/// - `Bytes` overflow (> 255 bytes) → `[TAG_BYTES_OVERFLOW(0x03), len_le(4), data...]`
/// - `Float`  → `[TAG_FLOAT(0x04), f64_bits_le(8)]`   — 9 bytes total
pub(crate) fn value_to_wal_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Int64(n) => {
            let mut buf = vec![WAL_TAG_INT64];
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Bytes(b) => {
            if b.len() <= 255 {
                let mut buf = vec![WAL_TAG_BYTES_INLINE, b.len() as u8];
                buf.extend_from_slice(b);
                buf
            } else {
                let mut buf = vec![WAL_TAG_BYTES_OVERFLOW];
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
                buf
            }
        }
        Value::Float(f) => {
            let mut buf = vec![WAL_TAG_FLOAT];
            buf.extend_from_slice(&f.to_bits().to_le_bytes());
            buf
        }
    }
}

/// Decode a WAL property value byte slice back to a `Value`.
///
/// Supports two formats:
///
/// **New tagged format** (byte 0 is a known tag):
/// - `TAG_INT64 (0x01)`           → 8-byte little-endian i64 at bytes[1..9]
/// - `TAG_BYTES_INLINE (0x02)`    → length byte at [1], data at [2..2+len]
/// - `TAG_BYTES_OVERFLOW (0x03)`  → 4-byte LE length at [1..5], data at [5..]
/// - `TAG_FLOAT (0x04)`           → 8-byte IEEE-754 bits at bytes[1..9]
///
/// **Legacy 8-byte format** (backward compat for records written before the
/// tagged format was introduced; detected by byte 0 not being a known tag):
/// - Top byte (raw >> 56) == 0x00 → `Int64` (sign-extended from 56 bits)
/// - Top byte == 0x01             → `Bytes` inline (≤7 bytes, trailing-zero trimmed)
/// - Any other top byte           → `Float` (raw IEEE-754 bits)
pub(crate) fn wal_bytes_to_value(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::Int64(0);
    }
    match bytes[0] {
        WAL_TAG_INT64 => {
            if bytes.len() < 9 {
                return Value::Int64(0);
            }
            let n = i64::from_le_bytes(bytes[1..9].try_into().unwrap_or([0u8; 8]));
            Value::Int64(n)
        }
        WAL_TAG_BYTES_INLINE => {
            if bytes.len() < 2 {
                return Value::Bytes(vec![]);
            }
            let len = bytes[1] as usize;
            let end = 2 + len;
            if bytes.len() < end {
                return Value::Bytes(bytes[2..].to_vec());
            }
            Value::Bytes(bytes[2..end].to_vec())
        }
        WAL_TAG_BYTES_OVERFLOW => {
            if bytes.len() < 5 {
                return Value::Bytes(vec![]);
            }
            let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap_or([0u8; 4])) as usize;
            let end = 5 + len;
            if bytes.len() < end {
                return Value::Bytes(bytes[5..].to_vec());
            }
            Value::Bytes(bytes[5..end].to_vec())
        }
        WAL_TAG_FLOAT => {
            if bytes.len() < 9 {
                return Value::Float(0.0);
            }
            let bits = u64::from_le_bytes(bytes[1..9].try_into().unwrap_or([0u8; 8]));
            Value::Float(f64::from_bits(bits))
        }
        _ => {
            // Legacy 8-byte format: no explicit tag.  Detect by examining the
            // top byte of the 8-byte little-endian word.
            if bytes.len() < 8 {
                return Value::Int64(0);
            }
            let raw = u64::from_le_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]));
            let top = (raw >> 56) as u8;
            match top {
                0x00 => {
                    // Legacy TAG_INT64: sign-extend from 56 bits.
                    let shifted = (raw << 8) as i64;
                    Value::Int64(shifted >> 8)
                }
                0x01 => {
                    // Legacy TAG_BYTES inline (≤7 bytes, trailing-zero trimmed).
                    let arr = raw.to_le_bytes();
                    let data: Vec<u8> = arr[..7].iter().copied().take_while(|&b| b != 0).collect();
                    Value::Bytes(data)
                }
                _ => {
                    // Legacy float — raw IEEE-754 bits.
                    Value::Float(f64::from_bits(raw))
                }
            }
        }
    }
}

// ── WAL mutation emission (SPA-127) ───────────────────────────────────────────

/// Append mutation WAL records for a committed transaction.
///
/// Uses the persistent `wal_writer` cached in [`DbInner`] — no segment scan
/// or file-open overhead on each call (SPA-210).  Emits:
/// - `Begin`
/// - `NodeUpdate` for each staged property change in `updates`
/// - `NodeCreate` / `NodeUpdate` / `NodeDelete` / `EdgeCreate` for each structural mutation
/// - `Commit`
/// - fsync
pub(crate) fn write_mutation_wal(
    wal_writer: &Mutex<WalWriter>,
    txn_id: u64,
    updates: &[((u64, u32), StagedUpdate)],
    mutations: &[WalMutation],
) -> crate::Result<()> {
    // Nothing to record if there were no mutations or property updates.
    if updates.is_empty() && mutations.is_empty() {
        return Ok(());
    }

    let mut wal = wal_writer.lock().unwrap_or_else(|e| e.into_inner());
    let txn = TxnId(txn_id);

    wal.append(WalRecordKind::Begin, txn, WalPayload::Empty)?;

    // NodeUpdate records for each property change in the write buffer.
    for ((node_raw, col_id), staged) in updates {
        let before_bytes = staged
            .before_image
            .as_ref()
            .map(|(_, v)| value_to_wal_bytes(v))
            .unwrap_or_else(|| 0u64.to_le_bytes().to_vec());
        let after_bytes = value_to_wal_bytes(&staged.new_value);
        wal.append(
            WalRecordKind::NodeUpdate,
            txn,
            WalPayload::NodeUpdate {
                node_id: *node_raw,
                key: staged.key_name.clone(),
                col_id: *col_id,
                before: before_bytes,
                after: after_bytes,
            },
        )?;
    }

    // Structural mutations.
    for m in mutations {
        match m {
            WalMutation::NodeCreate {
                node_id,
                label_id,
                props,
                prop_names,
            } => {
                // Use human-readable names when available; fall back to
                // "col_{hash}" when only the col_id is known (low-level API).
                let wal_props: Vec<(String, Vec<u8>)> = props
                    .iter()
                    .enumerate()
                    .map(|(i, (col_id, v))| {
                        let name = prop_names
                            .get(i)
                            .filter(|n| !n.is_empty())
                            .cloned()
                            .unwrap_or_else(|| format!("col_{col_id}"));
                        (name, value_to_wal_bytes(v))
                    })
                    .collect();
                wal.append(
                    WalRecordKind::NodeCreate,
                    txn,
                    WalPayload::NodeCreate {
                        node_id: node_id.0,
                        label_id: *label_id,
                        props: wal_props,
                    },
                )?;
            }
            WalMutation::NodeDelete { node_id } => {
                wal.append(
                    WalRecordKind::NodeDelete,
                    txn,
                    WalPayload::NodeDelete { node_id: node_id.0 },
                )?;
            }
            WalMutation::EdgeCreate {
                edge_id,
                src,
                dst,
                rel_type,
                prop_entries,
            } => {
                let wal_props: Vec<(String, Vec<u8>)> = prop_entries
                    .iter()
                    .map(|(name, val)| (name.clone(), value_to_wal_bytes(val)))
                    .collect();
                wal.append(
                    WalRecordKind::EdgeCreate,
                    txn,
                    WalPayload::EdgeCreate {
                        edge_id: edge_id.0,
                        src: src.0,
                        dst: dst.0,
                        rel_type: rel_type.clone(),
                        props: wal_props,
                    },
                )?;
            }
            WalMutation::EdgeDelete { src, dst, rel_type } => {
                wal.append(
                    WalRecordKind::EdgeDelete,
                    txn,
                    WalPayload::EdgeDelete {
                        src: src.0,
                        dst: dst.0,
                        rel_type: rel_type.clone(),
                    },
                )?;
            }
        }
    }

    wal.append(WalRecordKind::Commit, txn, WalPayload::Empty)?;
    wal.fsync()?;
    Ok(())
}
