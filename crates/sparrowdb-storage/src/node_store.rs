//! Node property storage.
//!
//! Nodes are stored as typed property columns.  Each `(label_id, col_id)` pair
//! maps to a flat binary file of fixed-width values.  Valid `u64` values pack
//! `(label_id: u32, slot: u32)` into a single `u64` node ID consistent with
//! [`sparrowdb_common::NodeId`] semantics.
//!
//! ## File layout
//!
//! ```text
//! nodes/{label_id}/col_{col_id}.bin
//! ```
//!
//! Each column file is a flat array of `u64` LE values (one per slot).
//! The high-water mark is tracked in-memory and written to a small header file:
//!
//! ```text
//! nodes/{label_id}/hwm.bin   — [hwm: u64 LE]
//! ```
//!
//! ## Node ID packing
//!
//! ```text
//! node_id = (label_id as u64) << 32 | slot as u64
//! ```
//!
//! Upper 32 bits are `label_id`, lower 32 bits are the within-label slot number.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, NodeId, Result};

// ── Value type ────────────────────────────────────────────────────────────────

/// A typed property value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Signed 64-bit integer, stored as raw `u64` bits (two's-complement).
    Int64(i64),
    /// Raw byte blob, stored as a fixed-width 8-byte reference in v1.
    /// The actual bytes are placed inline for values ≤ 8 bytes; longer blobs
    /// are truncated and marked with a sentinel in v1 (overflow deferred).
    Bytes(Vec<u8>),
    /// IEEE-754 double-precision float.  Stored as 8 raw bytes in the overflow
    /// heap so that no bits are masked by the type-tag scheme (SPA-267).
    Float(f64),
}

/// Type tag embedded in the top byte (byte index 7 in LE) of a stored `u64`.
///
/// - `0x00` = `Int64`         — lower 7 bytes hold the signed integer (56-bit range).
/// - `0x01` = `Bytes`         — lower 7 bytes hold up to 7 bytes of inline string data.
/// - `0x02` = `BytesOverflow` — lower 7 bytes encode `(offset: u40 LE, len: u16 LE)`
///   pointing into `strings.bin` (SPA-212).
/// - `0x03` = `Float`         — lower 7 bytes encode `(offset: u40 LE, len: u16 LE)`
///   pointing into `strings.bin` where 8 raw IEEE-754 bytes are stored (SPA-267).
///
/// The overflow encoding packs a heap pointer into 7 bytes:
///   bytes[0..5] = heap byte offset (u40 LE, max ~1 TiB)
///   bytes[5..7] = byte length (u16 LE, max 65535 bytes)
///
/// This lets `decode_raw_value` reconstruct strings of any length, fixing SPA-212.
const TAG_INT64: u8 = 0x00;
const TAG_BYTES: u8 = 0x01;
/// Tag for strings > 7 bytes stored in the overflow string heap (SPA-212).
const TAG_BYTES_OVERFLOW: u8 = 0x02;
/// Tag for f64 values stored as 8 raw IEEE-754 bytes in the overflow heap (SPA-267).
/// Using the heap ensures all 64 bits of the float are preserved without any masking.
const TAG_FLOAT: u8 = 0x03;
/// Maximum bytes that fit inline in the 7-byte payload (one byte is the tag).
const MAX_INLINE_BYTES: usize = 7;

impl Value {
    /// Encode as a packed `u64` for column storage.
    ///
    /// The top byte (byte 7 in little-endian) is a type tag; the remaining
    /// 7 bytes carry the payload.  This allows `from_u64` to reconstruct the
    /// correct variant at read time (SPA-169).
    ///
    /// For `Bytes` values that exceed 7 bytes, this method only encodes the
    /// first 7 bytes inline.  Callers that need full overflow support must use
    /// [`NodeStore::encode_value`] instead, which writes long strings to the
    /// heap and returns an overflow-tagged u64 (SPA-212).
    ///
    /// # Int64 range
    /// Only the lower 56 bits of the integer are stored.  This covers all
    /// practical node IDs and numeric property values; very large i64 values
    /// (> 2^55 or < -2^55) would be truncated.  Full 64-bit range is deferred
    /// to a later overflow encoding.
    pub fn to_u64(&self) -> u64 {
        match self {
            Value::Int64(v) => {
                // Top byte = TAG_INT64 (0x00); lower 7 bytes = lower 56 bits of v.
                // For TAG_INT64 = 0x00 this is just the value masked to 56 bits,
                // which is a no-op for any i64 whose top byte is already 0x00.
                let payload = (*v as u64) & 0x00FF_FFFF_FFFF_FFFF;
                // Tag byte goes into byte 7 (the most significant byte in LE).
                payload | ((TAG_INT64 as u64) << 56)
            }
            Value::Bytes(b) => {
                let mut arr = [0u8; 8];
                arr[7] = TAG_BYTES; // type tag in top byte
                let len = b.len().min(MAX_INLINE_BYTES);
                arr[..len].copy_from_slice(&b[..len]);
                u64::from_le_bytes(arr)
            }
            Value::Float(_) => {
                // Float values require heap storage — callers must use
                // NodeStore::encode_value instead of Value::to_u64.
                panic!("Value::Float cannot be inline-encoded; use NodeStore::encode_value");
            }
        }
    }

    /// Reconstruct a `Value` from a stored `u64`, using the top byte as a
    /// type tag (SPA-169).
    ///
    /// Only handles inline encodings (`TAG_INT64` and `TAG_BYTES`).
    /// For overflow strings (`TAG_BYTES_OVERFLOW`), use [`NodeStore::decode_raw_value`]
    /// which has access to the string heap (SPA-212).
    pub fn from_u64(v: u64) -> Self {
        let bytes = v.to_le_bytes(); // bytes[7] = top byte = tag
        match bytes[7] {
            TAG_BYTES => {
                // Inline string: bytes[0..7] hold the data; strip trailing zeros.
                let data: Vec<u8> = bytes[..7].iter().copied().take_while(|&b| b != 0).collect();
                Value::Bytes(data)
            }
            _ => {
                // TAG_INT64 (0x00) or any unrecognised tag → Int64.
                // Sign-extend from 56 bits: shift left 8 to bring bit 55 into
                // sign position, then arithmetic shift right 8.
                let shifted = (v << 8) as i64;
                Value::Int64(shifted >> 8)
            }
        }
    }

    /// Reconstruct an `Int64` value from a stored `u64`.
    ///
    /// Preserved for callers that know the column type is always Int64 (e.g.
    /// pre-SPA-169 paths).  New code should prefer `from_u64`.
    pub fn int64_from_u64(v: u64) -> Self {
        Value::Int64(v as i64)
    }
}

// ── NodeStore ─────────────────────────────────────────────────────────────────

/// Persistent node property store rooted at a database directory.
///
/// On-disk layout:
/// ```text
/// {root}/nodes/{label_id}/hwm.bin            — high-water mark (u64 LE)
/// {root}/nodes/{label_id}/col_{col_id}.bin   — flat u64 column array
/// {root}/strings.bin                         — overflow string heap (SPA-212)
/// ```
///
/// The overflow heap is an append-only byte file.  Each entry is a raw byte
/// sequence (no length prefix); the offset and length are encoded into the
/// `TAG_BYTES_OVERFLOW` u64 stored in the column file.
pub struct NodeStore {
    root: PathBuf,
    /// In-memory high-water marks per label.  Loaded lazily from disk.
    hwm: HashMap<u32, u64>,
    /// Labels whose in-memory HWM has been advanced but not yet persisted to
    /// `hwm.bin`.  Flushed atomically by [`flush_hwms`] at transaction commit.
    hwm_dirty: HashSet<u32>,
}

impl NodeStore {
    /// Open (or create) a node store rooted at `db_root`.
    pub fn open(db_root: &Path) -> Result<Self> {
        Ok(NodeStore {
            root: db_root.to_path_buf(),
            hwm: HashMap::new(),
            hwm_dirty: HashSet::new(),
        })
    }

    /// Return the root directory path of this node store.
    pub fn root_path(&self) -> &Path {
        &self.root
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn label_dir(&self, label_id: u32) -> PathBuf {
        self.root.join("nodes").join(label_id.to_string())
    }

    fn hwm_path(&self, label_id: u32) -> PathBuf {
        self.label_dir(label_id).join("hwm.bin")
    }

    fn col_path(&self, label_id: u32, col_id: u32) -> PathBuf {
        self.label_dir(label_id).join(format!("col_{col_id}.bin"))
    }

    /// Path to the null-bitmap sidecar for a column (SPA-207).
    ///
    /// Bit N = 1 means slot N has a real value (present/non-null).
    /// Bit N = 0 means slot N was zero-padded (absent/null).
    fn null_bitmap_path(&self, label_id: u32, col_id: u32) -> PathBuf {
        self.label_dir(label_id)
            .join(format!("col_{col_id}_null.bin"))
    }

    /// Mark slot `slot` as present (has a real value) in the null bitmap (SPA-207).
    fn set_null_bit(&self, label_id: u32, col_id: u32, slot: u32) -> Result<()> {
        let path = self.null_bitmap_path(label_id, col_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let byte_idx = (slot / 8) as usize;
        let bit_idx = slot % 8;
        let mut bits = if path.exists() {
            fs::read(&path).map_err(Error::Io)?
        } else {
            vec![]
        };
        if bits.len() <= byte_idx {
            bits.resize(byte_idx + 1, 0);
        }
        bits[byte_idx] |= 1 << bit_idx;
        fs::write(&path, &bits).map_err(Error::Io)
    }

    /// Returns `true` if slot `slot` has a real value (present/non-null) (SPA-207).
    ///
    /// Backward-compatible: if no bitmap file exists (old data written before
    /// the null-bitmap fix), every slot is treated as present.
    fn get_null_bit(&self, label_id: u32, col_id: u32, slot: u32) -> Result<bool> {
        let path = self.null_bitmap_path(label_id, col_id);
        if !path.exists() {
            // No bitmap file → backward-compatible: treat all slots as present.
            return Ok(true);
        }
        let bits = fs::read(&path).map_err(Error::Io)?;
        let byte_idx = (slot / 8) as usize;
        if byte_idx >= bits.len() {
            return Ok(false);
        }
        Ok((bits[byte_idx] >> (slot % 8)) & 1 == 1)
    }

    /// Path to the overflow string heap (shared across all labels).
    fn strings_bin_path(&self) -> PathBuf {
        self.root.join("strings.bin")
    }

    // ── Overflow string heap (SPA-212) ────────────────────────────────────────

    /// Append `bytes` to the overflow string heap and return an
    /// `TAG_BYTES_OVERFLOW`-tagged `u64` encoding the (offset, len) pair.
    ///
    /// Layout of the returned `u64` (little-endian bytes):
    ///   bytes[0..5] = heap byte offset as u40 LE  (max ~1 TiB)
    ///   bytes[5..7] = byte length as u16 LE       (max 65 535 bytes)
    ///   bytes[7]    = `TAG_BYTES_OVERFLOW` (0x02)
    fn append_to_string_heap(&self, bytes: &[u8]) -> Result<u64> {
        use std::io::{Seek, SeekFrom, Write};
        let path = self.strings_bin_path();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .append(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)?;

        // The heap offset is the current end of the file.
        let offset = file.seek(SeekFrom::End(0)).map_err(Error::Io)?;
        file.write_all(bytes).map_err(Error::Io)?;

        // Encode (offset, len) into a 7-byte payload.
        let len = bytes.len() as u64;
        debug_assert!(
            offset <= 0x00FF_FFFF_FFFF_u64,
            "string heap too large for 5-byte offset"
        );
        debug_assert!(len <= 0xFFFF, "string longer than 65535 bytes");

        let mut arr = [0u8; 8];
        // Store offset in bytes[0..5] (40-bit LE).
        arr[0] = offset as u8;
        arr[1] = (offset >> 8) as u8;
        arr[2] = (offset >> 16) as u8;
        arr[3] = (offset >> 24) as u8;
        arr[4] = (offset >> 32) as u8;
        // Store len in bytes[5..7] (16-bit LE).
        arr[5] = len as u8;
        arr[6] = (len >> 8) as u8;
        // Tag byte.
        arr[7] = TAG_BYTES_OVERFLOW;
        Ok(u64::from_le_bytes(arr))
    }

    /// Read string bytes from the overflow heap given an `TAG_BYTES_OVERFLOW`
    /// tagged `u64` produced by [`append_to_string_heap`].
    ///
    /// Uses `seek + read_exact` to load only the `len` bytes at `offset`,
    /// avoiding a full file load regardless of `strings.bin` size (SPA-208).
    fn read_from_string_heap(&self, tagged: u64) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let arr = tagged.to_le_bytes();
        debug_assert_eq!(arr[7], TAG_BYTES_OVERFLOW, "not an overflow pointer");

        // Decode (offset, len).
        let offset = arr[0] as u64
            | ((arr[1] as u64) << 8)
            | ((arr[2] as u64) << 16)
            | ((arr[3] as u64) << 24)
            | ((arr[4] as u64) << 32);
        let len = arr[5] as usize | ((arr[6] as usize) << 8);

        let path = self.strings_bin_path();
        let mut file = fs::File::open(&path).map_err(Error::Io)?;
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::Corruption(format!("string heap seek failed (offset={offset}): {e}"))
        })?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).map_err(|e| {
            Error::Corruption(format!(
                "string heap too short: need {} bytes at offset {offset}: {e}",
                len
            ))
        })?;
        Ok(buf)
    }

    // ── Value encode / decode with overflow support ───────────────────────────

    /// Encode a `Value` for column storage, writing long `Bytes` strings to
    /// the overflow heap (SPA-212).
    ///
    /// - `Int64`          → identical to `Value::to_u64()`.
    /// - `Bytes` ≤ 7 B    → inline `TAG_BYTES` encoding, identical to `Value::to_u64()`.
    /// - `Bytes` > 7 B    → appended to `strings.bin`; returns `TAG_BYTES_OVERFLOW` u64.
    /// - `Float`          → 8 raw IEEE-754 bytes appended to `strings.bin`;
    ///   returns a `TAG_FLOAT` u64 so all 64 float bits are preserved (SPA-267).
    pub fn encode_value(&self, val: &Value) -> Result<u64> {
        match val {
            Value::Int64(_) => Ok(val.to_u64()),
            Value::Bytes(b) if b.len() <= MAX_INLINE_BYTES => Ok(val.to_u64()),
            Value::Bytes(b) => self.append_to_string_heap(b),
            // SPA-267: store all 8 float bytes in the heap so no bits are masked.
            // The heap pointer uses the same (offset: u40, len: u16) layout as
            // TAG_BYTES_OVERFLOW but with TAG_FLOAT in byte 7.
            Value::Float(f) => {
                let bits = f.to_bits().to_le_bytes();
                let heap_tagged = self.append_to_string_heap(&bits)?;
                // Replace the TAG_BYTES_OVERFLOW tag byte with TAG_FLOAT.
                let payload = heap_tagged & 0x00FF_FFFF_FFFF_FFFF;
                Ok((TAG_FLOAT as u64) << 56 | payload)
            }
        }
    }

    /// Decode a raw `u64` column value back to a `Value`, reading the
    /// overflow string heap when the tag is `TAG_BYTES_OVERFLOW` or `TAG_FLOAT` (SPA-212, SPA-267).
    ///
    /// Handles all four tags:
    /// - `TAG_INT64`          → `Value::Int64`
    /// - `TAG_BYTES`          → `Value::Bytes` (inline, ≤ 7 bytes)
    /// - `TAG_BYTES_OVERFLOW` → `Value::Bytes` (from heap)
    /// - `TAG_FLOAT`          → `Value::Float` (8 raw IEEE-754 bytes from heap)
    pub fn decode_raw_value(&self, raw: u64) -> Value {
        let tag = (raw >> 56) as u8;
        match tag {
            TAG_BYTES_OVERFLOW => match self.read_from_string_heap(raw) {
                Ok(bytes) => Value::Bytes(bytes),
                Err(e) => {
                    // Corruption fallback: return empty bytes and log.
                    eprintln!(
                        "WARN: failed to read overflow string from heap (raw={raw:#018x}): {e}"
                    );
                    Value::Bytes(Vec::new())
                }
            },
            // SPA-267: float values are stored as 8-byte IEEE-754 blobs in the heap.
            // Reconstruct the heap pointer by swapping TAG_FLOAT → TAG_BYTES_OVERFLOW
            // so read_from_string_heap can locate the bytes.
            TAG_FLOAT => {
                let payload = raw & 0x00FF_FFFF_FFFF_FFFF;
                let heap_tagged = (TAG_BYTES_OVERFLOW as u64) << 56 | payload;
                match self.read_from_string_heap(heap_tagged) {
                    Ok(bytes) if bytes.len() == 8 => {
                        let arr: [u8; 8] = bytes.try_into().unwrap();
                        Value::Float(f64::from_bits(u64::from_le_bytes(arr)))
                    }
                    Ok(bytes) => {
                        eprintln!(
                            "WARN: float heap blob has unexpected length {} (raw={raw:#018x})",
                            bytes.len()
                        );
                        Value::Float(f64::NAN)
                    }
                    Err(e) => {
                        eprintln!("WARN: failed to read float from heap (raw={raw:#018x}): {e}");
                        Value::Float(f64::NAN)
                    }
                }
            }
            _ => Value::from_u64(raw),
        }
    }

    /// Check whether a raw stored `u64` encodes a string equal to `s`.
    ///
    /// Handles both inline (`TAG_BYTES`) and overflow (`TAG_BYTES_OVERFLOW`)
    /// encodings (SPA-212).  Used by WHERE-clause and prop-filter comparison.
    pub fn raw_str_matches(&self, raw: u64, s: &str) -> bool {
        let tag = (raw >> 56) as u8;
        match tag {
            TAG_BYTES => {
                // Fast inline comparison: encode s the same way and compare u64s.
                raw == Value::Bytes(s.as_bytes().to_vec()).to_u64()
            }
            TAG_BYTES_OVERFLOW => {
                // Overflow: read from heap and compare bytes.
                match self.read_from_string_heap(raw) {
                    Ok(bytes) => bytes == s.as_bytes(),
                    Err(_) => false,
                }
            }
            _ => false, // INT64 or unknown — not a string
        }
    }

    /// Read the high-water mark for `label_id` from disk (or return 0).
    ///
    /// Recovery path (SPA-211): if `hwm.bin` is missing or corrupt but
    /// `hwm.bin.tmp` exists (leftover from a previous crashed write), we
    /// promote the tmp file to `hwm.bin` and use its value.
    fn load_hwm(&self, label_id: u32) -> Result<u64> {
        let path = self.hwm_path(label_id);
        let tmp_path = self.hwm_tmp_path(label_id);

        // Try to read the canonical file first.
        let try_read = |p: &std::path::Path| -> Option<u64> {
            let bytes = fs::read(p).ok()?;
            if bytes.len() < 8 {
                return None;
            }
            Some(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
        };

        if path.exists() {
            match try_read(&path) {
                Some(v) => return Ok(v),
                None => {
                    // hwm.bin exists but is corrupt — fall through to tmp recovery.
                }
            }
        }

        // hwm.bin is absent or unreadable.  Check for a tmp leftover.
        if tmp_path.exists() {
            if let Some(v) = try_read(&tmp_path) {
                // Promote: atomically rename tmp → canonical so the next open
                // is clean even if we crash again immediately here.
                let _ = fs::rename(&tmp_path, &path);
                return Ok(v);
            }
        }

        // Last resort: infer the HWM from the sizes of the column files.
        //
        // Each `col_{n}.bin` is a flat array of 8-byte u64 LE values, one per
        // slot.  The number of slots written equals `file_len / 8`.  If
        // `hwm.bin` is corrupt we can reconstruct the HWM as the maximum slot
        // count across all column files for this label.
        {
            let inferred = self.infer_hwm_from_cols(label_id);
            if inferred > 0 {
                // Persist the recovered HWM so the next open is clean.
                let _ = self.save_hwm(label_id, inferred);
                return Ok(inferred);
            }
        }

        // No usable file at all — fresh label, HWM is 0.
        Ok(0)
    }

    /// Infer the high-water mark for `label_id` from the sizes of the column
    /// files on disk.  Returns 0 if no column files exist or none are readable.
    ///
    /// Each `col_{n}.bin` stores one u64 per slot, so `file_len / 8` gives the
    /// slot count.  We return the maximum over all columns.
    fn infer_hwm_from_cols(&self, label_id: u32) -> u64 {
        let dir = self.label_dir(label_id);
        let read_dir = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(_) => return 0,
        };
        read_dir
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name();
                let name_str = name.to_string_lossy().into_owned();
                // Only consider col_{n}.bin files.
                name_str.strip_prefix("col_")?.strip_suffix(".bin")?;
                let meta = entry.metadata().ok()?;
                Some(meta.len() / 8)
            })
            .max()
            .unwrap_or(0)
    }

    /// Write the high-water mark for `label_id` to disk atomically.
    ///
    /// Strategy (SPA-211): write to `hwm.bin.tmp`, fsync, then rename to
    /// `hwm.bin`.  On POSIX, `rename(2)` is atomic, so a crash at any point
    /// leaves either the old `hwm.bin` intact or the fully-written new one.
    fn save_hwm(&self, label_id: u32, hwm: u64) -> Result<()> {
        use std::io::Write as _;

        let path = self.hwm_path(label_id);
        let tmp_path = self.hwm_tmp_path(label_id);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }

        // Write to the tmp file and fsync before renaming.
        {
            let mut file = fs::File::create(&tmp_path).map_err(Error::Io)?;
            file.write_all(&hwm.to_le_bytes()).map_err(Error::Io)?;
            file.sync_all().map_err(Error::Io)?;
        }

        // Atomic rename: on success the old hwm.bin is replaced in one syscall.
        fs::rename(&tmp_path, &path).map_err(Error::Io)
    }

    fn hwm_tmp_path(&self, label_id: u32) -> PathBuf {
        self.label_dir(label_id).join("hwm.bin.tmp")
    }

    /// Persist all dirty in-memory HWMs to disk atomically.
    ///
    /// Called **once per transaction commit** rather than once per node creation,
    /// so that bulk imports do not incur one fsync per node (SPA-217 regression fix).
    ///
    /// Each dirty label's HWM is written via the same tmp+fsync+rename strategy
    /// used by [`save_hwm`], preserving the SPA-211 crash-safety guarantee.
    /// After all writes succeed the dirty set is cleared.
    pub fn flush_hwms(&mut self) -> Result<()> {
        let dirty: Vec<u32> = self.hwm_dirty.iter().copied().collect();
        for label_id in dirty {
            let hwm = match self.hwm.get(&label_id) {
                Some(&v) => v,
                None => continue,
            };
            self.save_hwm(label_id, hwm)?;
        }
        self.hwm_dirty.clear();
        Ok(())
    }

    /// Append a `u64` value to a column file.
    fn append_col(&self, label_id: u32, col_id: u32, slot: u32, value: u64) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let path = self.col_path(label_id, col_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        // Open without truncation so we can inspect the current length.
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)?;

        // Pad with zeros for any slots that were skipped (sparse write pattern).
        // Without padding, a later slot's value would be written at offset 0,
        // causing earlier slots to incorrectly read that value.
        let existing_len = file.seek(SeekFrom::End(0)).map_err(Error::Io)?;
        let expected_offset = slot as u64 * 8;
        if existing_len < expected_offset {
            file.seek(SeekFrom::Start(existing_len))
                .map_err(Error::Io)?;
            const CHUNK: usize = 65536;
            let zeros = [0u8; CHUNK];
            let mut remaining = (expected_offset - existing_len) as usize;
            while remaining > 0 {
                let n = remaining.min(CHUNK);
                file.write_all(&zeros[..n]).map_err(Error::Io)?;
                remaining -= n;
            }
        }

        // Seek to the correct slot position and write the value.
        file.seek(SeekFrom::Start(expected_offset))
            .map_err(Error::Io)?;
        file.write_all(&value.to_le_bytes()).map_err(Error::Io)
    }

    /// Read the `u64` stored at `slot` in the given column file.
    ///
    /// Returns `Ok(0)` when the column file does not exist yet — a missing file
    /// means no value has ever been written for this `(label_id, col_id)` pair,
    /// which is represented as the zero bit-pattern (SPA-166).
    fn read_col_slot(&self, label_id: u32, col_id: u32, slot: u32) -> Result<u64> {
        let path = self.col_path(label_id, col_id);
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(Error::Io(e)),
        };
        let offset = slot as usize * 8;
        if bytes.len() < offset + 8 {
            return Err(Error::NotFound);
        }
        Ok(u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().unwrap(),
        ))
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Return the high-water mark (slot count) for a label.
    ///
    /// Returns `0` if no nodes have been created for that label yet.
    pub fn hwm_for_label(&self, label_id: u32) -> Result<u64> {
        if let Some(&h) = self.hwm.get(&label_id) {
            return Ok(h);
        }
        self.load_hwm(label_id)
    }

    /// Discover all column IDs that currently exist on disk for `label_id`.
    ///
    /// Scans the label directory for `col_{id}.bin` files and returns the
    /// parsed `col_id` values.  Used by `create_node` to zero-pad columns
    /// that are not supplied for a new node (SPA-187).
    ///
    /// Returns `Err` when the directory exists but cannot be read (e.g.
    /// permissions failure or I/O error).  A missing directory is not an
    /// error — it simply means no nodes of this label have been created yet.
    pub fn col_ids_for_label(&self, label_id: u32) -> Result<Vec<u32>> {
        self.existing_col_ids(label_id)
    }

    fn existing_col_ids(&self, label_id: u32) -> Result<Vec<u32>> {
        let dir = self.label_dir(label_id);
        let read_dir = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            // Directory does not exist yet → no columns on disk.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::Io(e)),
        };
        let mut ids: Vec<u32> = read_dir
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name();
                let name_str = name.to_string_lossy().into_owned();
                // Match "col_{col_id}.bin" filenames.
                let id_str = name_str.strip_prefix("col_")?.strip_suffix(".bin")?;
                id_str.parse::<u32>().ok()
            })
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    /// Return the **on-disk** high-water mark for a label, bypassing any
    /// in-memory advances made by `peek_next_slot`.
    ///
    /// Used by [`WriteTx::merge_node`] to limit the disk scan to only slots
    /// that have actually been persisted.
    pub fn disk_hwm_for_label(&self, label_id: u32) -> Result<u64> {
        self.load_hwm(label_id)
    }

    /// Reserve the slot index that the *next* `create_node` call will use for
    /// `label_id`, advancing the in-memory HWM so that the slot is not
    /// assigned again within the same [`NodeStore`] instance.
    ///
    /// This is used by [`WriteTx::create_node`] to pre-compute a [`NodeId`]
    /// before the actual disk write, so the ID can be returned to the caller
    /// while the write is deferred until commit (SPA-181).
    ///
    /// The on-disk HWM is **not** updated here; it is updated when the
    /// buffered `NodeCreate` operation is applied in `commit()`.
    pub fn peek_next_slot(&mut self, label_id: u32) -> Result<u32> {
        // Load from disk if not cached yet.
        if !self.hwm.contains_key(&label_id) {
            let h = self.load_hwm(label_id)?;
            self.hwm.insert(label_id, h);
        }
        let h = *self.hwm.get(&label_id).unwrap();
        // Advance the in-memory HWM so a subsequent peek returns the next slot.
        self.hwm.insert(label_id, h + 1);
        Ok(h as u32)
    }

    /// Write a node at a pre-reserved `slot` (SPA-181 commit path).
    ///
    /// Like [`create_node`] but uses the caller-specified `slot` index instead
    /// of deriving it from the HWM.  Used by [`WriteTx::commit`] to flush
    /// buffered node-create operations in the exact order they were issued,
    /// with slots that were already pre-allocated by [`peek_next_slot`].
    ///
    /// Advances the on-disk HWM to `slot + 1` (or higher if already past that).
    pub fn create_node_at_slot(
        &mut self,
        label_id: u32,
        slot: u32,
        props: &[(u32, Value)],
    ) -> Result<NodeId> {
        // Snapshot original column sizes for rollback on partial failure.
        let original_sizes: Vec<(u32, u64)> = props
            .iter()
            .map(|&(col_id, _)| {
                let path = self.col_path(label_id, col_id);
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                (col_id, size)
            })
            .collect();

        let write_result = (|| {
            for &(col_id, ref val) in props {
                self.append_col(label_id, col_id, slot, self.encode_value(val)?)?;
                // Mark this slot as present in the null bitmap (SPA-207).
                self.set_null_bit(label_id, col_id, slot)?;
            }
            Ok::<(), sparrowdb_common::Error>(())
        })();

        if let Err(e) = write_result {
            for (col_id, original_size) in &original_sizes {
                let path = self.col_path(label_id, *col_id);
                if path.exists() {
                    if let Err(rollback_err) = fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .and_then(|f| f.set_len(*original_size))
                    {
                        eprintln!(
                            "CRITICAL: Failed to roll back column file {} to size {}: {}. Data may be corrupt.",
                            path.display(),
                            original_size,
                            rollback_err
                        );
                    }
                }
            }
            return Err(e);
        }

        // Advance the in-memory HWM to at least slot + 1 and mark the label
        // dirty so that flush_hwms() will persist it at commit boundary.
        //
        // We do NOT call save_hwm here.  The caller (WriteTx::commit) is
        // responsible for calling flush_hwms() once after all PendingOp::NodeCreate
        // entries have been applied.  This avoids one fsync per node during bulk
        // imports (SPA-217 regression fix) while preserving crash-safety: the WAL
        // record is already durable at this point, so recovery can reconstruct the
        // HWM if we crash before flush_hwms() completes.
        //
        // NOTE: peek_next_slot() may have already advanced self.hwm to slot+1,
        // so new_hwm > mem_hwm might be false.  We mark the label dirty
        // unconditionally so that flush_hwms() always writes through to disk.
        let new_hwm = slot as u64 + 1;
        let mem_hwm = self.hwm.get(&label_id).copied().unwrap_or(0);
        if new_hwm > mem_hwm {
            self.hwm.insert(label_id, new_hwm);
        }
        // Always mark dirty — flush_hwms() must write the post-commit HWM even
        // if peek_next_slot already set mem HWM == new_hwm.
        self.hwm_dirty.insert(label_id);

        Ok(NodeId((label_id as u64) << 32 | slot as u64))
    }

    /// Batch-write column data for multiple nodes created in a single transaction
    /// commit (SPA-212 write-amplification fix).
    ///
    /// # Why this exists
    ///
    /// The naive path calls `create_node_at_slot` per node, which opens and
    /// closes every column file once per node.  For a transaction that creates
    /// `N` nodes each with `C` columns, that is `O(N × C)` file-open/close
    /// syscalls.
    ///
    /// This method instead:
    /// 1. Accepts pre-encoded `(label_id, col_id, slot, raw_value, is_present)`
    ///    tuples from the caller (value encoding happens in `commit()` before
    ///    the call).
    /// 2. Sorts by `(label_id, col_id)` so all writes to the same column file
    ///    are contiguous.
    /// 3. Opens each `(label_id, col_id)` file exactly **once**, writes all
    ///    slots for that column, then closes it — reducing file opens to
    ///    `O(labels × cols)`.
    /// 4. Updates each null-bitmap file once per `(label_id, col_id)` group.
    ///
    /// HWM advances are applied for every `(label_id, slot)` in `node_slots`,
    /// exactly as `create_node_at_slot` would do them.  `node_slots` must
    /// include **all** created nodes — including those with zero properties —
    /// so that the HWM is advanced even for property-less nodes.
    ///
    /// # Rollback
    ///
    /// On I/O failure the method truncates every file that was opened back to
    /// its pre-call size, matching the rollback contract of
    /// `create_node_at_slot`.
    pub fn batch_write_node_creates(
        &mut self,
        // (label_id, col_id, slot, raw_u64, is_present)
        mut writes: Vec<(u32, u32, u32, u64, bool)>,
        // All (label_id, slot) pairs for every created node, including those
        // with zero properties.
        node_slots: &[(u32, u32)],
    ) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        if writes.is_empty() && node_slots.is_empty() {
            return Ok(());
        }

        // Sort by (label_id, col_id, slot) so all writes to the same file are
        // contiguous.
        writes.sort_unstable_by_key(|&(lid, cid, slot, _, _)| (lid, cid, slot));

        // Snapshot original sizes for rollback.  We only need one entry per
        // (label_id, col_id) pair.
        let mut original_sizes: Vec<(u32, u32, PathBuf, u64)> = Vec::new();
        {
            let mut prev_key: Option<(u32, u32)> = None;
            for &(lid, cid, _, _, _) in &writes {
                let key = (lid, cid);
                if prev_key == Some(key) {
                    continue;
                }
                prev_key = Some(key);
                let path = self.col_path(lid, cid);
                let original = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                original_sizes.push((lid, cid, path, original));
            }
        }
        // Also snapshot null-bitmap files (one per col group).
        let mut bitmap_originals: Vec<(u32, u32, PathBuf, u64)> = Vec::new();
        {
            let mut prev_key: Option<(u32, u32)> = None;
            for &(lid, cid, _, _, is_present) in &writes {
                if !is_present {
                    continue;
                }
                let key = (lid, cid);
                if prev_key == Some(key) {
                    continue;
                }
                prev_key = Some(key);
                let path = self.null_bitmap_path(lid, cid);
                let original = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                bitmap_originals.push((lid, cid, path, original));
            }
        }

        let write_result = (|| -> Result<()> {
            let mut i = 0;
            while i < writes.len() {
                let (lid, cid, _, _, _) = writes[i];

                // Find the end of this (label_id, col_id) group.
                let group_start = i;
                while i < writes.len() && writes[i].0 == lid && writes[i].1 == cid {
                    i += 1;
                }
                let group = &writes[group_start..i];

                // ── Column file ──────────────────────────────────────────────
                let path = self.col_path(lid, cid);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).map_err(Error::Io)?;
                }
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(Error::Io)?;

                let mut current_len = file.seek(SeekFrom::End(0)).map_err(Error::Io)?;

                for &(_, _, slot, raw_value, _) in group {
                    let expected_offset = slot as u64 * 8;
                    // Pad with zeros for any skipped slots.
                    if current_len < expected_offset {
                        file.seek(SeekFrom::Start(current_len)).map_err(Error::Io)?;
                        const CHUNK: usize = 65536;
                        let zeros = [0u8; CHUNK];
                        let mut remaining = (expected_offset - current_len) as usize;
                        while remaining > 0 {
                            let n = remaining.min(CHUNK);
                            file.write_all(&zeros[..n]).map_err(Error::Io)?;
                            remaining -= n;
                        }
                        current_len = expected_offset;
                    }
                    // Seek to exact slot and write.
                    file.seek(SeekFrom::Start(expected_offset))
                        .map_err(Error::Io)?;
                    file.write_all(&raw_value.to_le_bytes())
                        .map_err(Error::Io)?;
                    // Advance current_len to reflect what we wrote.
                    let after = expected_offset + 8;
                    if after > current_len {
                        current_len = after;
                    }
                }

                // ── Null bitmap (one read-modify-write per col group) ────────
                let present_slots: Vec<u32> = group
                    .iter()
                    .filter(|&&(_, _, _, _, is_present)| is_present)
                    .map(|&(_, _, slot, _, _)| slot)
                    .collect();

                if !present_slots.is_empty() {
                    let bmap_path = self.null_bitmap_path(lid, cid);
                    if let Some(parent) = bmap_path.parent() {
                        fs::create_dir_all(parent).map_err(Error::Io)?;
                    }
                    let mut bits = if bmap_path.exists() {
                        fs::read(&bmap_path).map_err(Error::Io)?
                    } else {
                        vec![]
                    };
                    for slot in present_slots {
                        let byte_idx = (slot / 8) as usize;
                        let bit_idx = slot % 8;
                        if bits.len() <= byte_idx {
                            bits.resize(byte_idx + 1, 0);
                        }
                        bits[byte_idx] |= 1 << bit_idx;
                    }
                    fs::write(&bmap_path, &bits).map_err(Error::Io)?;
                }
            }
            Ok(())
        })();

        if let Err(e) = write_result {
            // Roll back column files.
            for (_, _, path, original_size) in &original_sizes {
                if path.exists() {
                    if let Err(rollback_err) = fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .and_then(|f| f.set_len(*original_size))
                    {
                        eprintln!(
                            "CRITICAL: Failed to roll back column file {} to size {}: {}. Data may be corrupt.",
                            path.display(),
                            original_size,
                            rollback_err
                        );
                    }
                }
            }
            // Roll back null bitmap files.
            for (_, _, path, original_size) in &bitmap_originals {
                if path.exists() {
                    if let Err(rollback_err) = fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .and_then(|f| f.set_len(*original_size))
                    {
                        eprintln!(
                            "CRITICAL: Failed to roll back null bitmap file {} to size {}: {}. Data may be corrupt.",
                            path.display(),
                            original_size,
                            rollback_err
                        );
                    }
                }
            }
            return Err(e);
        }

        // Advance HWMs using the explicit node_slots list so that nodes with
        // zero properties also advance the HWM, matching the contract of
        // create_node_at_slot.
        for &(lid, slot) in node_slots {
            let new_hwm = slot as u64 + 1;
            let mem_hwm = self.hwm.get(&lid).copied().unwrap_or(0);
            if new_hwm > mem_hwm {
                self.hwm.insert(lid, new_hwm);
            }
            self.hwm_dirty.insert(lid);
        }

        Ok(())
    }

    /// Create a new node in `label_id` with the given properties.
    ///
    /// Returns the new [`NodeId`] packed as `(label_id << 32) | slot`.
    ///
    /// ## Slot alignment guarantee (SPA-187)
    ///
    /// Every column file for `label_id` must have exactly `node_count * 8`
    /// bytes so that slot N always refers to node N across all columns.  When
    /// a node is created without a value for an already-known column, that
    /// column file is zero-padded to `(slot + 1) * 8` bytes.  The zero
    /// sentinel is recognised by `read_col_slot_nullable` as "absent" and
    /// surfaces as `Value::Null` in query results.
    pub fn create_node(&mut self, label_id: u32, props: &[(u32, Value)]) -> Result<NodeId> {
        // Load or get cached hwm.
        let hwm = if let Some(h) = self.hwm.get(&label_id) {
            *h
        } else {
            let h = self.load_hwm(label_id)?;
            self.hwm.insert(label_id, h);
            h
        };

        let slot = hwm as u32;

        // Collect the set of col_ids supplied for this node.
        let supplied_col_ids: std::collections::HashSet<u32> =
            props.iter().map(|&(col_id, _)| col_id).collect();

        // Discover all columns already on disk for this label.  Any that are
        // NOT in `supplied_col_ids` must be zero-padded so their slot count
        // stays in sync with the HWM (SPA-187).
        let existing_col_ids = self.existing_col_ids(label_id)?;

        // Columns that need zero-padding: exist on disk but not in this node's props.
        let cols_to_zero_pad: Vec<u32> = existing_col_ids
            .iter()
            .copied()
            .filter(|col_id| !supplied_col_ids.contains(col_id))
            .collect();

        // Build the full list of columns to touch for rollback tracking:
        // supplied columns + columns that need zero-padding.
        let all_col_ids_to_touch: Vec<u32> = supplied_col_ids
            .iter()
            .copied()
            .chain(cols_to_zero_pad.iter().copied())
            .collect();

        // Snapshot the original size of each column file so we can roll back
        // on partial failure.  A column that does not yet exist has size 0.
        let original_sizes: Vec<(u32, u64)> = all_col_ids_to_touch
            .iter()
            .map(|&col_id| {
                let path = self.col_path(label_id, col_id);
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                (col_id, size)
            })
            .collect();

        // Write each property column.  For columns not in `props`, write a
        // zero-padded entry (the zero sentinel means "absent" / NULL).
        // On failure, roll back all columns that were already written to avoid
        // slot misalignment.
        let write_result = (|| {
            // Write supplied columns with their actual values.
            for &(col_id, ref val) in props {
                self.append_col(label_id, col_id, slot, self.encode_value(val)?)?;
                // Mark this slot as present in the null bitmap (SPA-207).
                self.set_null_bit(label_id, col_id, slot)?;
            }
            // Zero-pad existing columns that were NOT supplied for this node.
            // Do NOT set null bitmap bits for these — they remain absent/null.
            for &col_id in &cols_to_zero_pad {
                self.append_col(label_id, col_id, slot, 0u64)?;
            }
            Ok::<(), sparrowdb_common::Error>(())
        })();

        if let Err(e) = write_result {
            // Truncate each column back to its original size.
            for (col_id, original_size) in &original_sizes {
                let path = self.col_path(label_id, *col_id);
                if path.exists() {
                    if let Err(rollback_err) = fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .and_then(|f| f.set_len(*original_size))
                    {
                        eprintln!(
                            "CRITICAL: Failed to roll back column file {} to size {}: {}. Data may be corrupt.",
                            path.display(),
                            original_size,
                            rollback_err
                        );
                    }
                }
            }
            return Err(e);
        }

        // Update hwm.
        let new_hwm = hwm + 1;
        self.save_hwm(label_id, new_hwm)?;
        *self.hwm.get_mut(&label_id).unwrap() = new_hwm;

        // Pack node ID.
        let node_id = ((label_id as u64) << 32) | (slot as u64);
        Ok(NodeId(node_id))
    }

    /// Write a deletion tombstone (`u64::MAX`) into `col_0.bin` for `node_id`.
    ///
    /// Creates `col_0.bin` (and its parent directory) if it does not exist,
    /// zero-padding all preceding slots.  This ensures that nodes which were
    /// created without any `col_0` property are still properly marked as deleted
    /// and become invisible to subsequent scans.
    ///
    /// Called from [`WriteTx::commit`] when flushing a buffered `NodeDelete`.
    pub fn tombstone_node(&self, node_id: NodeId) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;
        let col0 = self.col_path(label_id, 0);

        // Ensure the parent directory exists.
        if let Some(parent) = col0.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }

        let mut f = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&col0)
            .map_err(Error::Io)?;

        // Pad col_0.bin to cover all existing nodes for this label (the HWM),
        // not just up to the tombstoned slot.  Without this, a node at slot S+1
        // would receive Err(NotFound) when reading col_0 after the file is
        // created here with only (S+1)*8 bytes, because read_col_slot returns
        // NotFound when the requested slot is beyond the file length.
        let hwm = self.hwm_for_label(label_id).unwrap_or(slot as u64 + 1);
        let needed_len = std::cmp::max(hwm, slot as u64 + 1) * 8;
        let existing_len = f.seek(SeekFrom::End(0)).map_err(Error::Io)?;
        if existing_len < needed_len {
            let zeros = vec![0u8; (needed_len - existing_len) as usize];
            f.write_all(&zeros).map_err(Error::Io)?;
        }

        // Seek to the slot and write the tombstone value.
        f.seek(SeekFrom::Start(slot as u64 * 8))
            .map_err(Error::Io)?;
        f.write_all(&u64::MAX.to_le_bytes()).map_err(Error::Io)
    }

    /// Overwrite the value of a single column for an existing node.
    ///
    /// Seeks to the slot's offset within `col_{col_id}.bin` and writes the new
    /// 8-byte little-endian value in-place.  Returns `Err(NotFound)` if the
    /// slot does not exist yet.
    pub fn set_node_col(&self, node_id: NodeId, col_id: u32, value: &Value) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;
        let path = self.col_path(label_id, col_id);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|_| Error::NotFound)?;
        let offset = slot as u64 * 8;
        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
        file.write_all(&self.encode_value(value)?.to_le_bytes())
            .map_err(Error::Io)
    }

    /// Write or create a column value for a node, creating and zero-padding the
    /// column file if it does not yet exist.
    ///
    /// Unlike [`set_node_col`], this method creates the column file and fills all
    /// slots from 0 to `slot - 1` with zeros before writing the target value.
    /// This supports adding new property columns to existing nodes (Phase 7
    /// `set_property` semantics).
    pub fn upsert_node_col(&self, node_id: NodeId, col_id: u32, value: &Value) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;
        let path = self.col_path(label_id, col_id);

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }

        // Open the file (create if absent), then pad with zeros up to and
        // including the target slot, and finally seek back to write the value.
        // We must NOT truncate: we only update a specific slot, not the whole file.
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)?;

        // How many bytes are already in the file?
        let existing_len = file.seek(SeekFrom::End(0)).map_err(Error::Io)?;
        let needed_len = (slot as u64 + 1) * 8;
        if existing_len < needed_len {
            // Extend file with zeros from existing_len to needed_len.
            // Write in fixed-size chunks to avoid a single large allocation
            // that could OOM when the node slot ID is very high.
            file.seek(SeekFrom::Start(existing_len))
                .map_err(Error::Io)?;
            const CHUNK: usize = 65536; // 64 KiB
            let zeros = [0u8; CHUNK];
            let mut remaining = (needed_len - existing_len) as usize;
            while remaining > 0 {
                let n = remaining.min(CHUNK);
                file.write_all(&zeros[..n]).map_err(Error::Io)?;
                remaining -= n;
            }
        }

        // Seek to target slot and overwrite.
        file.seek(SeekFrom::Start(slot as u64 * 8))
            .map_err(Error::Io)?;
        file.write_all(&self.encode_value(value)?.to_le_bytes())
            .map_err(Error::Io)
    }

    /// Retrieve all stored properties of a node.
    ///
    /// Returns `(col_id, raw_u64)` pairs in the order the columns were defined.
    /// The caller knows the schema (col IDs) from the catalog.
    pub fn get_node_raw(&self, node_id: NodeId, col_ids: &[u32]) -> Result<Vec<(u32, u64)>> {
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;

        let mut result = Vec::with_capacity(col_ids.len());
        for &col_id in col_ids {
            let val = self.read_col_slot(label_id, col_id, slot)?;
            result.push((col_id, val));
        }
        Ok(result)
    }

    /// Like [`get_node_raw`] but treats absent columns as `None` rather than
    /// propagating [`Error::NotFound`].
    ///
    /// A column is considered absent when:
    /// - Its column file does not exist (property never written for the label).
    /// - Its column file is shorter than `slot + 1` entries (sparse write —
    ///   an earlier node never wrote this column; a later node that did write it
    ///   padded the file, but this slot's value was never explicitly stored).
    ///
    /// This is the correct read path for IS NULL evaluation: absent properties
    /// must appear as `Value::Null`, not as an error or as integer 0.
    pub fn get_node_raw_nullable(
        &self,
        node_id: NodeId,
        col_ids: &[u32],
    ) -> Result<Vec<(u32, Option<u64>)>> {
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;

        let mut result = Vec::with_capacity(col_ids.len());
        for &col_id in col_ids {
            let opt_val = self.read_col_slot_nullable(label_id, col_id, slot)?;
            result.push((col_id, opt_val));
        }
        Ok(result)
    }

    /// Read a single column slot, returning `None` when the column was never
    /// written for this node (file absent or slot out of bounds / not set).
    ///
    /// Unlike [`read_col_slot`], this function distinguishes between:
    /// - Column file does not exist → `None` (property never set on any node).
    /// - Slot is beyond the file end → `None` (property not set on this node).
    /// - Null bitmap says slot is absent → `None` (slot was zero-padded for alignment).
    /// - Null bitmap says slot is present → `Some(raw)` (real value, may be 0 for Int64(0)).
    ///
    /// The null-bitmap sidecar (`col_{id}_null.bin`) is used to distinguish
    /// legitimately-stored 0 values (e.g. `Int64(0)`) from absent/zero-padded
    /// slots (SPA-207).  Backward compat: if no bitmap file exists, all slots
    /// within the file are treated as present.
    fn read_col_slot_nullable(&self, label_id: u32, col_id: u32, slot: u32) -> Result<Option<u64>> {
        let path = self.col_path(label_id, col_id);
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        };
        let offset = slot as usize * 8;
        if bytes.len() < offset + 8 {
            return Ok(None);
        }
        // Use the null-bitmap sidecar to determine whether this slot has a real
        // value.  This replaces the old raw==0 sentinel which incorrectly treated
        // Int64(0) as absent (SPA-207).
        if !self.get_null_bit(label_id, col_id, slot)? {
            return Ok(None);
        }
        let raw = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        Ok(Some(raw))
    }

    /// Retrieve the typed property values for a node.
    ///
    /// Convenience wrapper over [`get_node_raw`] that decodes every raw `u64`
    /// back to a `Value`, reading the overflow string heap when needed (SPA-212).
    pub fn get_node(&self, node_id: NodeId, col_ids: &[u32]) -> Result<Vec<(u32, Value)>> {
        let raw = self.get_node_raw(node_id, col_ids)?;
        Ok(raw
            .into_iter()
            .map(|(col_id, v)| (col_id, self.decode_raw_value(v)))
            .collect())
    }

    /// Read the entire contents of `col_{col_id}.bin` for `label_id` as a
    /// flat `Vec<u64>`.  Returns an empty vec when the file does not exist yet.
    ///
    /// This is used by [`crate::property_index::PropertyIndex::build`] to scan
    /// all slot values in one `fs::read` call rather than one `read_col_slot`
    /// per node, making index construction O(n) rather than O(n * syscall-overhead).
    pub fn read_col_all(&self, label_id: u32, col_id: u32) -> Result<Vec<u64>> {
        let path = self.col_path(label_id, col_id);
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::Io(e)),
        };
        // Interpret the byte slice as a flat array of little-endian u64s.
        let count = bytes.len() / 8;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let offset = i * 8;
            let v = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            out.push(v);
        }
        Ok(out)
    }

    /// Read the null-bitmap for `(label_id, col_id)` and return a `Vec<bool>`
    /// where index `i` is `true` iff slot `i` has a real value (was explicitly
    /// written, as opposed to zero-padded for alignment or never written).
    ///
    /// If no bitmap file exists (old data written before SPA-207), returns
    /// `None` to signal backward-compat mode — callers should fall back to the
    /// `raw != 0` sentinel in that case.
    ///
    /// Used by [`PropertyIndex::build_for`] and [`PropertyIndex::build`] to
    /// correctly index slots whose value is `Int64(0)` (raw = 0), which is
    /// otherwise indistinguishable from the absent sentinel without the bitmap.
    pub fn read_null_bitmap_all(&self, label_id: u32, col_id: u32) -> Result<Option<Vec<bool>>> {
        let path = self.null_bitmap_path(label_id, col_id);
        if !path.exists() {
            return Ok(None); // backward compat: no bitmap file
        }
        let bits = fs::read(&path).map_err(Error::Io)?;
        // Each byte encodes 8 slots: bit i of byte j → slot (j*8 + i).
        let mut out = Vec::with_capacity(bits.len() * 8);
        for &byte in &bits {
            for bit_idx in 0..8u32 {
                out.push((byte >> bit_idx) & 1 == 1);
            }
        }
        Ok(Some(out))
    }

    /// Selective sorted-slot read — O(K) seeks instead of O(N) reads.
    ///
    /// For each column, opens the file once and reads only the `slots` needed,
    /// in slot-ascending order (for sequential-ish I/O).  This is the hot path
    /// for hop queries where K neighbor slots ≪ N total nodes.
    ///
    /// `slots` **must** be pre-sorted ascending by the caller.
    /// Returns a `Vec` indexed parallel to `slots`; each inner `Vec` is indexed
    /// parallel to `col_ids`.  Out-of-range or missing-file slots return 0.
    ///
    /// SPA-200: replaces full O(N) column loads with O(K) per-column seeks.
    fn read_col_slots_sorted(
        &self,
        label_id: u32,
        slots: &[u32],
        col_ids: &[u32],
    ) -> Result<Vec<Vec<u64>>> {
        if slots.is_empty() || col_ids.is_empty() {
            // Return a row of empty vecs parallel to slots
            return Ok(slots.iter().map(|_| vec![0u64; col_ids.len()]).collect());
        }

        // result[slot_idx][col_idx]
        let mut result: Vec<Vec<u64>> = slots.iter().map(|_| vec![0u64; col_ids.len()]).collect();

        for (col_idx, &col_id) in col_ids.iter().enumerate() {
            let path = self.col_path(label_id, col_id);
            let mut file = match fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Column file doesn't exist — all slots stay 0
                    continue;
                }
                Err(e) => return Err(Error::Io(e)),
            };

            let file_len = file.seek(SeekFrom::End(0)).map_err(Error::Io)?;
            // Reset to start for sequential reads
            file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;

            let mut buf = [0u8; 8];
            let mut current_pos: u64 = 0;

            for (slot_idx, &slot) in slots.iter().enumerate() {
                let target_pos = slot as u64 * 8;
                if target_pos + 8 > file_len {
                    // Slot beyond end of file — leave as 0
                    continue;
                }
                if target_pos != current_pos {
                    file.seek(SeekFrom::Start(target_pos)).map_err(Error::Io)?;
                    current_pos = target_pos;
                }
                file.read_exact(&mut buf).map_err(Error::Io)?;
                current_pos += 8;
                result[slot_idx][col_idx] = u64::from_le_bytes(buf);
            }
        }

        Ok(result)
    }

    /// Batch-read multiple slots from multiple columns, returning nullable results.
    ///
    /// Like [`batch_read_node_props`] but preserves null semantics using the
    /// null-bitmap sidecar (`col_{id}_null.bin`).  Each `Option<u64>` is:
    /// - `Some(raw)` — the slot has a real stored value (may be 0 for `Int64(0)`).
    /// - `None` — the slot was zero-padded, never written, or the null bitmap
    ///   marks it absent.
    ///
    /// Backward compatible: if no bitmap file exists for a column (data written
    /// before SPA-207), every slot whose column file entry is within bounds is
    /// treated as present (`Some`).
    ///
    /// This replaces the raw-`0`-as-absent semantic of [`batch_read_node_props`]
    /// and is the correct API for the chunked pipeline (Phase 2, SPA-299).
    pub fn batch_read_node_props_nullable(
        &self,
        label_id: u32,
        slots: &[u32],
        col_ids: &[u32],
    ) -> Result<Vec<Vec<Option<u64>>>> {
        if slots.is_empty() {
            return Ok(vec![]);
        }
        if col_ids.is_empty() {
            return Ok(slots.iter().map(|_| vec![]).collect());
        }

        let hwm = self.hwm_for_label(label_id).unwrap_or(0) as usize;
        // Use sorted-slot reads when K < 50% of N, identical heuristic as
        // batch_read_node_props.
        let use_sorted = hwm == 0 || slots.len() * 2 < hwm;

        // Pre-load null bitmaps for each column (None = no bitmap = backward compat).
        let null_bitmaps: Vec<Option<Vec<bool>>> = col_ids
            .iter()
            .map(|&col_id| self.read_null_bitmap_all(label_id, col_id))
            .collect::<Result<_>>()?;

        if use_sorted {
            // Build sorted permutation for sequential I/O.
            let mut order: Vec<usize> = (0..slots.len()).collect();
            order.sort_unstable_by_key(|&i| slots[i]);
            let sorted_slots: Vec<u32> = order.iter().map(|&i| slots[i]).collect();

            // Raw values (0 for missing/out-of-range).
            let raw = self.read_col_slots_sorted(label_id, &sorted_slots, col_ids)?;

            // Build result in original order, applying null bitmaps.
            let mut result = vec![vec![None; col_ids.len()]; slots.len()];
            for (sorted_idx, orig_idx) in order.into_iter().enumerate() {
                let slot = sorted_slots[sorted_idx];
                for (col_idx, bm) in null_bitmaps.iter().enumerate() {
                    let is_present = match bm {
                        None => {
                            // No bitmap — backward compat: treat as present if
                            // the raw value is non-zero.  This mirrors the old
                            // `raw != 0` sentinel from before SPA-207.
                            raw[sorted_idx][col_idx] != 0
                        }
                        Some(bits) => bits.get(slot as usize).copied().unwrap_or(false),
                    };
                    if is_present {
                        result[orig_idx][col_idx] = Some(raw[sorted_idx][col_idx]);
                    }
                }
            }
            Ok(result)
        } else {
            // Full-column load path.
            let col_data: Vec<Vec<u64>> = col_ids
                .iter()
                .map(|&col_id| self.read_col_all(label_id, col_id))
                .collect::<Result<_>>()?;
            Ok(slots
                .iter()
                .map(|&slot| {
                    col_ids
                        .iter()
                        .enumerate()
                        .map(|(ci, _)| {
                            let raw = col_data[ci].get(slot as usize).copied().unwrap_or(0);
                            let is_present = match &null_bitmaps[ci] {
                                None => raw != 0,
                                Some(bits) => bits.get(slot as usize).copied().unwrap_or(false),
                            };
                            if is_present {
                                Some(raw)
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .collect())
        }
    }

    /// Batch-read multiple slots from multiple columns.
    ///
    /// Chooses between two strategies based on the K/N ratio:
    /// - **Sorted-slot reads** (SPA-200): when K ≪ N, seeks to each slot
    ///   offset — O(K × C) I/O instead of O(N × C).  Slots are sorted
    ///   ascending before the read so seeks are sequential.
    /// - **Full-column load**: when K is close to N (>50% of column), reading
    ///   the whole file is cheaper than many random seeks.
    ///
    /// All `slots` must belong to `label_id`.
    /// Returns a `Vec` indexed parallel to `slots`; inner `Vec` indexed
    /// parallel to `col_ids`.  Missing/out-of-range slots return 0.
    pub fn batch_read_node_props(
        &self,
        label_id: u32,
        slots: &[u32],
        col_ids: &[u32],
    ) -> Result<Vec<Vec<u64>>> {
        if slots.is_empty() {
            return Ok(vec![]);
        }

        // Determine the column high-water mark (total node count for this label).
        let hwm = self.hwm_for_label(label_id).unwrap_or(0) as usize;

        // Use sorted-slot reads when K < 50% of N.  For K=100, N=4039 that is
        // a ~40× reduction in bytes read per column.  The HWM=0 guard handles
        // labels that have no nodes yet (shouldn't reach here, but be safe).
        let use_sorted = hwm == 0 || slots.len() * 2 < hwm;

        if use_sorted {
            // Sort slots ascending for sequential I/O; keep a permutation index
            // so we can return results in the original caller order.
            let mut order: Vec<usize> = (0..slots.len()).collect();
            order.sort_unstable_by_key(|&i| slots[i]);
            let sorted_slots: Vec<u32> = order.iter().map(|&i| slots[i]).collect();

            let sorted_result = self.read_col_slots_sorted(label_id, &sorted_slots, col_ids)?;

            // Re-order back to the original slot order expected by the caller.
            let mut result = vec![vec![0u64; col_ids.len()]; slots.len()];
            for (sorted_idx, orig_idx) in order.into_iter().enumerate() {
                result[orig_idx] = sorted_result[sorted_idx].clone();
            }
            Ok(result)
        } else {
            // Fall back to full column load when most slots are needed anyway.
            let col_data: Vec<Vec<u64>> = col_ids
                .iter()
                .map(|&col_id| self.read_col_all(label_id, col_id))
                .collect::<Result<_>>()?;
            Ok(slots
                .iter()
                .map(|&slot| {
                    col_ids
                        .iter()
                        .enumerate()
                        .map(|(ci, _)| col_data[ci].get(slot as usize).copied().unwrap_or(0))
                        .collect()
                })
                .collect())
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────── ───────────────────────────────────────────────────────────────────

/// Unpack `(label_id, slot)` from a [`NodeId`].
pub fn unpack_node_id(node_id: NodeId) -> (u32, u32) {
    let label_id = (node_id.0 >> 32) as u32;
    let slot = (node_id.0 & 0xFFFF_FFFF) as u32;
    (label_id, slot)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_node_create_and_get() {
        let dir = tempdir().unwrap();
        let mut store = NodeStore::open(dir.path()).unwrap();

        let label_id = 1u32;
        let props = vec![(0u32, Value::Int64(42)), (1u32, Value::Int64(100))];

        let node_id = store.create_node(label_id, &props).unwrap();

        // Verify the packed node ID.
        let (lid, slot) = unpack_node_id(node_id);
        assert_eq!(lid, label_id);
        assert_eq!(slot, 0); // first node → slot 0

        // Get back the values.
        let retrieved = store.get_node(node_id, &[0, 1]).unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0], (0, Value::Int64(42)));
        assert_eq!(retrieved[1], (1, Value::Int64(100)));
    }

    #[test]
    fn test_node_multiple_nodes_sequential_slots() {
        let dir = tempdir().unwrap();
        let mut store = NodeStore::open(dir.path()).unwrap();

        let n1 = store.create_node(1, &[(0, Value::Int64(10))]).unwrap();
        let n2 = store.create_node(1, &[(0, Value::Int64(20))]).unwrap();
        let n3 = store.create_node(1, &[(0, Value::Int64(30))]).unwrap();

        let (_, s1) = unpack_node_id(n1);
        let (_, s2) = unpack_node_id(n2);
        let (_, s3) = unpack_node_id(n3);
        assert_eq!(s1, 0);
        assert_eq!(s2, 1);
        assert_eq!(s3, 2);

        assert_eq!(store.get_node(n1, &[0]).unwrap()[0].1, Value::Int64(10));
        assert_eq!(store.get_node(n2, &[0]).unwrap()[0].1, Value::Int64(20));
        assert_eq!(store.get_node(n3, &[0]).unwrap()[0].1, Value::Int64(30));
    }

    #[test]
    fn test_node_persists_across_reopen() {
        let dir = tempdir().unwrap();

        let node_id = {
            let mut store = NodeStore::open(dir.path()).unwrap();
            store
                .create_node(2, &[(0, Value::Int64(999)), (1, Value::Int64(-1))])
                .unwrap()
        };

        // Reopen store from disk.
        let store2 = NodeStore::open(dir.path()).unwrap();
        let vals = store2.get_node(node_id, &[0, 1]).unwrap();
        assert_eq!(vals[0].1, Value::Int64(999));
        assert_eq!(vals[1].1, Value::Int64(-1));
    }

    #[test]
    fn test_node_hwm_persists_across_reopen() {
        let dir = tempdir().unwrap();

        // Create 3 nodes in session 1.
        {
            let mut store = NodeStore::open(dir.path()).unwrap();
            store.create_node(0, &[(0, Value::Int64(1))]).unwrap();
            store.create_node(0, &[(0, Value::Int64(2))]).unwrap();
            store.create_node(0, &[(0, Value::Int64(3))]).unwrap();
        }

        // Reopen and create a 4th node — must get slot 3.
        let mut store2 = NodeStore::open(dir.path()).unwrap();
        let n4 = store2.create_node(0, &[(0, Value::Int64(4))]).unwrap();
        let (_, slot) = unpack_node_id(n4);
        assert_eq!(slot, 3);
    }

    #[test]
    fn test_node_different_labels_independent() {
        let dir = tempdir().unwrap();
        let mut store = NodeStore::open(dir.path()).unwrap();

        let a = store.create_node(10, &[(0, Value::Int64(1))]).unwrap();
        let b = store.create_node(20, &[(0, Value::Int64(2))]).unwrap();

        let (la, sa) = unpack_node_id(a);
        let (lb, sb) = unpack_node_id(b);
        assert_eq!(la, 10);
        assert_eq!(sa, 0);
        assert_eq!(lb, 20);
        assert_eq!(sb, 0);
    }
}
