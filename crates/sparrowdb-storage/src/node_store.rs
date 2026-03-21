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

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, NodeId, Result};

// ── Value type ────────────────────────────────────────────────────────────────

/// A typed property value.  Phase 3 supports `Int64` and `Bytes` only.
/// Larger types (STRING overflow, VARIANT) are deferred to later phases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Signed 64-bit integer, stored as raw `u64` bits (two's-complement).
    Int64(i64),
    /// Raw byte blob, stored as a fixed-width 8-byte reference in v1.
    /// The actual bytes are placed inline for values ≤ 8 bytes; longer blobs
    /// are truncated and marked with a sentinel in v1 (overflow deferred).
    Bytes(Vec<u8>),
}

impl Value {
    /// Encode as a packed `u64` for column storage.
    pub fn to_u64(&self) -> u64 {
        match self {
            Value::Int64(v) => *v as u64,
            Value::Bytes(b) => {
                let mut arr = [0u8; 8];
                let len = b.len().min(8);
                arr[..len].copy_from_slice(&b[..len]);
                u64::from_le_bytes(arr)
            }
        }
    }

    /// Reconstruct an `Int64` value from a stored `u64`.
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
/// ```
pub struct NodeStore {
    root: PathBuf,
    /// In-memory high-water marks per label.  Loaded lazily from disk.
    hwm: HashMap<u32, u64>,
}

impl NodeStore {
    /// Open (or create) a node store rooted at `db_root`.
    pub fn open(db_root: &Path) -> Result<Self> {
        Ok(NodeStore {
            root: db_root.to_path_buf(),
            hwm: HashMap::new(),
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn label_dir(&self, label_id: u32) -> PathBuf {
        self.root.join("nodes").join(label_id.to_string())
    }

    fn hwm_path(&self, label_id: u32) -> PathBuf {
        self.label_dir(label_id).join("hwm.bin")
    }

    fn col_path(&self, label_id: u32, col_id: u32) -> PathBuf {
        self.label_dir(label_id)
            .join(format!("col_{col_id}.bin"))
    }

    /// Read the high-water mark for `label_id` from disk (or return 0).
    fn load_hwm(&self, label_id: u32) -> Result<u64> {
        let path = self.hwm_path(label_id);
        if !path.exists() {
            return Ok(0);
        }
        let bytes = fs::read(&path).map_err(Error::Io)?;
        if bytes.len() < 8 {
            return Err(Error::Corruption(format!(
                "hwm.bin for label {label_id} is truncated"
            )));
        }
        Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
    }

    /// Write the high-water mark for `label_id` to disk.
    fn save_hwm(&self, label_id: u32, hwm: u64) -> Result<()> {
        let path = self.hwm_path(label_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        fs::write(&path, hwm.to_le_bytes()).map_err(Error::Io)
    }

    /// Append a `u64` value to a column file.
    fn append_col(&self, label_id: u32, col_id: u32, value: u64) -> Result<()> {
        use std::io::Write;
        let path = self.col_path(label_id, col_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(Error::Io)?;
        file.write_all(&value.to_le_bytes()).map_err(Error::Io)
    }

    /// Read the `u64` stored at `slot` in the given column file.
    fn read_col_slot(&self, label_id: u32, col_id: u32, slot: u32) -> Result<u64> {
        let path = self.col_path(label_id, col_id);
        let bytes = fs::read(&path).map_err(Error::Io)?;
        let offset = slot as usize * 8;
        if bytes.len() < offset + 8 {
            return Err(Error::NotFound);
        }
        Ok(u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().unwrap(),
        ))
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Create a new node in `label_id` with the given properties.
    ///
    /// Returns the new [`NodeId`] packed as `(label_id << 32) | slot`.
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

        // Write each property column.
        for &(col_id, ref val) in props {
            self.append_col(label_id, col_id, val.to_u64())?;
        }

        // Update hwm.
        let new_hwm = hwm + 1;
        self.save_hwm(label_id, new_hwm)?;
        *self.hwm.get_mut(&label_id).unwrap() = new_hwm;

        // Pack node ID.
        let node_id = ((label_id as u64) << 32) | (slot as u64);
        Ok(NodeId(node_id))
    }

    /// Retrieve all stored properties of a node.
    ///
    /// Returns `(col_id, raw_u64)` pairs in the order the columns were defined.
    /// The caller knows the schema (col IDs) from the catalog.
    pub fn get_node_raw(
        &self,
        node_id: NodeId,
        col_ids: &[u32],
    ) -> Result<Vec<(u32, u64)>> {
        let label_id = (node_id.0 >> 32) as u32;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;

        let mut result = Vec::with_capacity(col_ids.len());
        for &col_id in col_ids {
            let val = self.read_col_slot(label_id, col_id, slot)?;
            result.push((col_id, val));
        }
        Ok(result)
    }

    /// Retrieve the `Int64` property values for a node.
    ///
    /// Convenience wrapper over [`get_node_raw`] that interprets every column
    /// as an `Int64` (two's-complement re-interpretation of the stored `u64`).
    pub fn get_node(&self, node_id: NodeId, col_ids: &[u32]) -> Result<Vec<(u32, Value)>> {
        let raw = self.get_node_raw(node_id, col_ids)?;
        Ok(raw
            .into_iter()
            .map(|(col_id, v)| (col_id, Value::int64_from_u64(v)))
            .collect())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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
        let props = vec![
            (0u32, Value::Int64(42)),
            (1u32, Value::Int64(100)),
        ];

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

        assert_eq!(
            store.get_node(n1, &[0]).unwrap()[0].1,
            Value::Int64(10)
        );
        assert_eq!(
            store.get_node(n2, &[0]).unwrap()[0].1,
            Value::Int64(20)
        );
        assert_eq!(
            store.get_node(n3, &[0]).unwrap()[0].1,
            Value::Int64(30)
        );
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
