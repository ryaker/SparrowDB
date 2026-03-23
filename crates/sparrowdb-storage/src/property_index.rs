//! In-memory B-tree property equality index (SPA-249).
//!
//! ## Motivation
//!
//! Without an index, `MATCH (n:Label {prop: 'value'})` must read every node slot
//! in the label, check for tombstones, and compare property values — O(n) in the
//! number of nodes.  For large graphs this is prohibitively slow.
//!
//! ## Design
//!
//! A [`PropertyIndex`] is an in-memory structure keyed by `(label_id, col_id)`.
//! For each such key it maintains a `BTreeMap<u64, Vec<u32>>` that maps the **raw
//! stored u64** of a property value to the list of slot indices that hold that
//! value.  This lets equality lookups run in O(log n) time and is valid for
//! integers and inline strings (≤ 7 bytes); overflow strings are stored as a
//! heap pointer whose raw u64 is *unique per string*, so equality on the raw u64
//! is equivalent to equality on the decoded string value.
//!
//! ## Lifecycle
//!
//! The index is built at `Engine` construction time by scanning all existing
//! column files via [`NodeStore::read_col_all`].  It is updated incrementally
//! after each `CREATE` (via [`PropertyIndex::insert`]) and each `SET`
//! (via [`PropertyIndex::update`]).
//!
//! Tombstoned nodes (`col_0 == u64::MAX`) are excluded during build and never
//! inserted during incremental updates.
//!
//! ## Thread safety
//!
//! The index is owned by a single `Engine` instance that is created fresh per
//! query execution.  No cross-thread sharing occurs, so no `Mutex` is needed.

use std::collections::BTreeMap;

use sparrowdb_common::NodeId;

use crate::node_store::NodeStore;

// ── Constants ──────────────────────────────────────────────────────────────────

/// Deletion tombstone sentinel written into `col_0` by `tombstone_node`.
const TOMBSTONE: u64 = u64::MAX;

/// Zero is the "absent / never written" sentinel in column files.
const ABSENT: u64 = 0;

// ── Index key ─────────────────────────────────────────────────────────────────

/// Compound key identifying a single indexed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexKey {
    pub label_id: u32,
    pub col_id: u32,
}

// ── PropertyIndex ─────────────────────────────────────────────────────────────

/// In-memory B-tree property equality index.
///
/// Maps `(label_id, col_id) → BTreeMap<raw_value, Vec<slot>>`.
///
/// Use [`PropertyIndex::build`] to construct from disk, then
/// [`PropertyIndex::lookup`] for O(log n) equality queries.
#[derive(Default)]
pub struct PropertyIndex {
    /// `index[(label_id, col_id)][raw_value] = [slot, ...]`
    index: std::collections::HashMap<IndexKey, BTreeMap<u64, Vec<u32>>>,
}

impl PropertyIndex {
    /// Construct a fresh empty index (no disk I/O).
    pub fn new() -> Self {
        PropertyIndex::default()
    }

    /// Build the index by scanning all label/column files in `store`.
    ///
    /// For every `(label_id, col_id)` pair on disk, reads the entire column
    /// file in one call and populates `value → [slot]` mappings.  Slots whose
    /// value is the tombstone sentinel (`u64::MAX` in col_0) or the absent
    /// sentinel (0) are skipped.
    ///
    /// This is O(total stored cells) in time and memory, which is the same
    /// asymptotic cost as a single full scan — but it is paid once at Engine
    /// open time rather than on every query.
    pub fn build(store: &NodeStore, label_ids: &[(u32, String)]) -> Self {
        let mut idx = PropertyIndex::new();

        for &(label_id, ref _label_name) in label_ids {
            // Discover which columns exist on disk for this label.
            let col_ids = match store.col_ids_for_label(label_id) {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!(
                        "WARN: PropertyIndex::build: failed to list col_ids for label {label_id}: {e}; skipping"
                    );
                    continue;
                }
            };

            // Read the tombstone column (col_0) once to know which slots are dead.
            let tombstone_slots: std::collections::HashSet<u32> =
                match store.read_col_all(label_id, 0) {
                    Ok(col0) => col0
                        .iter()
                        .enumerate()
                        .filter_map(|(slot, &raw)| {
                            if raw == TOMBSTONE {
                                Some(slot as u32)
                            } else {
                                None
                            }
                        })
                        .collect(),
                    Err(_) => std::collections::HashSet::new(),
                };

            for col_id in col_ids {
                let key = IndexKey { label_id, col_id };
                let raw_vals = match store.read_col_all(label_id, col_id) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "WARN: PropertyIndex::build: failed to read col_{col_id} for label {label_id}: {e}; skipping"
                        );
                        continue;
                    }
                };

                let btree = idx.index.entry(key).or_default();
                for (slot, raw) in raw_vals.into_iter().enumerate() {
                    let slot = slot as u32;
                    // Skip absent and tombstoned slots.
                    if raw == ABSENT || tombstone_slots.contains(&slot) {
                        continue;
                    }
                    btree.entry(raw).or_default().push(slot);
                }
            }
        }

        idx
    }

    /// Look up all live node slots for `(label_id, col_id)` whose raw stored
    /// value equals `raw_value`.
    ///
    /// Returns an empty slice when the column is not indexed or no match exists.
    /// The returned slots must still be filtered for tombstones by the caller
    /// (e.g. via `Engine::is_node_tombstoned`) to guard against races between
    /// index build and deletion, though in practice the Engine is rebuilt per
    /// query so this is not an issue today.
    pub fn lookup(&self, label_id: u32, col_id: u32, raw_value: u64) -> &[u32] {
        let key = IndexKey { label_id, col_id };
        match self.index.get(&key) {
            Some(btree) => btree.get(&raw_value).map(|v| v.as_slice()).unwrap_or(&[]),
            None => &[],
        }
    }

    /// Returns `true` when this `(label_id, col_id)` pair has been indexed.
    ///
    /// A column is indexed iff it exists on disk at index-build time.
    pub fn is_indexed(&self, label_id: u32, col_id: u32) -> bool {
        self.index.contains_key(&IndexKey { label_id, col_id })
    }

    /// Insert a new `(slot, raw_value)` entry into the index after a successful
    /// node CREATE.  A no-op for absent values (raw == 0).
    pub fn insert(&mut self, label_id: u32, col_id: u32, slot: u32, raw_value: u64) {
        if raw_value == ABSENT {
            return;
        }
        let key = IndexKey { label_id, col_id };
        self.index
            .entry(key)
            .or_default()
            .entry(raw_value)
            .or_default()
            .push(slot);
    }

    /// Update a slot from `old_raw` to `new_raw` after a SET operation.
    ///
    /// Removes the slot from the old bucket and inserts it into the new one.
    /// A no-op for the absent sentinel.
    pub fn update(&mut self, label_id: u32, col_id: u32, slot: u32, old_raw: u64, new_raw: u64) {
        let key = IndexKey { label_id, col_id };
        let btree = self.index.entry(key).or_default();

        // Remove slot from old bucket.
        if old_raw != ABSENT {
            if let Some(slots) = btree.get_mut(&old_raw) {
                slots.retain(|&s| s != slot);
                if slots.is_empty() {
                    btree.remove(&old_raw);
                }
            }
        }

        // Insert into new bucket.
        if new_raw != ABSENT {
            btree.entry(new_raw).or_default().push(slot);
        }
    }

    /// Remove a slot from all buckets for `(label_id, col_id)` after node
    /// deletion (tombstoning).  Used to keep the index consistent.
    pub fn remove(&mut self, label_id: u32, col_id: u32, slot: u32, raw_value: u64) {
        if raw_value == ABSENT {
            return;
        }
        let key = IndexKey { label_id, col_id };
        if let Some(btree) = self.index.get_mut(&key) {
            if let Some(slots) = btree.get_mut(&raw_value) {
                slots.retain(|&s| s != slot);
                if slots.is_empty() {
                    btree.remove(&raw_value);
                }
            }
        }
    }

    /// Convert a `NodeId` to the corresponding slot index for this label.
    #[inline]
    pub fn node_id_to_slot(node_id: NodeId) -> u32 {
        (node_id.0 & 0xFFFF_FFFF) as u32
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let mut idx = PropertyIndex::new();
        idx.insert(1, 0, 0, 42u64);
        idx.insert(1, 0, 1, 42u64);
        idx.insert(1, 0, 2, 99u64);

        let slots = idx.lookup(1, 0, 42u64);
        assert_eq!(slots, &[0u32, 1]);
        let slots = idx.lookup(1, 0, 99u64);
        assert_eq!(slots, &[2u32]);
        let slots = idx.lookup(1, 0, 777u64);
        assert!(slots.is_empty());
    }

    #[test]
    fn update_moves_slot() {
        let mut idx = PropertyIndex::new();
        idx.insert(1, 5, 0, 10u64);
        idx.update(1, 5, 0, 10u64, 20u64);

        assert!(idx.lookup(1, 5, 10u64).is_empty());
        assert_eq!(idx.lookup(1, 5, 20u64), &[0u32]);
    }

    #[test]
    fn remove_cleans_slot() {
        let mut idx = PropertyIndex::new();
        idx.insert(2, 3, 7, 55u64);
        idx.remove(2, 3, 7, 55u64);
        assert!(idx.lookup(2, 3, 55u64).is_empty());
    }

    #[test]
    fn absent_sentinel_not_indexed() {
        let mut idx = PropertyIndex::new();
        idx.insert(1, 0, 0, 0u64); // 0 == ABSENT, must be no-op
        assert!(idx.lookup(1, 0, 0u64).is_empty());
    }

    #[test]
    fn is_indexed_tracks_presence() {
        let mut idx = PropertyIndex::new();
        assert!(!idx.is_indexed(1, 0));
        idx.insert(1, 0, 0, 42u64);
        assert!(idx.is_indexed(1, 0));
    }
}
