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
//! For each such key it maintains a `BTreeMap<u64, Vec<u32>>` that maps the **sort
//! key** (see [`sort_key`]) of a property value to the list of slot indices that
//! hold that value.  This lets equality lookups run in O(log n) time and is valid
//! for integers and inline strings (≤ 7 bytes); overflow strings are stored as a
//! heap pointer whose raw u64 is *unique per string*, so equality on the raw u64
//! is equivalent to equality on the decoded string value.
//!
//! ### Sort-key transform for signed integers
//!
//! For `TAG_INT64` values (`tag byte = 0x00`) the 56-bit payload is two's-complement
//! signed.  Raw `u64` ordering would place negative integers above positive ones
//! (e.g. `-1` encodes as `0x00FF_FFFF_FFFF_FFFF`).  We fix this by flipping the
//! sign bit of the 56-bit payload before inserting into the BTreeMap:
//!
//! ```text
//! sort_key = raw ^ 0x0080_0000_0000_0000   (for TAG_INT64 only)
//! ```
//!
//! The same transform is applied to query values in [`PropertyIndex::lookup`] and
//! [`PropertyIndex::lookup_range`], so correctness is transparent to callers.
//!
//! ## Lifecycle
//!
//! The index is built **lazily** — only when a query actually filters on a
//! specific `(label_id, col_id)` pair, via [`PropertyIndex::build_for`].  This
//! eliminates the full-table-scan overhead on queries that do not use a property
//! filter (e.g. `COUNT(*)`, hop traversals).  Once a pair is loaded it is cached
//! for the lifetime of the `Engine` instance, so repeated queries within the same
//! engine reuse the in-memory index without additional I/O.
//!
//! The legacy [`PropertyIndex::build`] method is retained for callers that need
//! an eager full build (e.g. tests or future bulk-load paths).
//!
//! Tombstoned nodes (`col_0 == u64::MAX`) are excluded during build and never
//! inserted during incremental updates.
//!
//! ## Thread safety
//!
//! The index is owned by a single `Engine` instance that is created fresh per
//! query execution.  No cross-thread sharing occurs, so no `Mutex` is needed.

use std::collections::BTreeMap;
use std::collections::HashSet;

use sparrowdb_common::{NodeId, Result};

use crate::node_store::NodeStore;

// Bring tracing macros into scope for build_for warnings.
#[allow(unused_imports)]
use tracing;

// ── Constants ──────────────────────────────────────────────────────────────────

/// Deletion tombstone sentinel written into `col_0` by `tombstone_node`.
const TOMBSTONE: u64 = u64::MAX;

/// Zero is the "absent / never written" sentinel in column files.
const ABSENT: u64 = 0;

/// Tag byte for signed 64-bit integers (upper byte of the stored u64).
const INT64_TAG: u64 = 0x00;

// ── Sort-key transform ─────────────────────────────────────────────────────────

/// Map a raw stored `u64` value to a sort key suitable for BTreeMap ordering.
///
/// For `TAG_INT64` values the 56-bit payload is two's-complement signed, which
/// means that in raw `u64` order negative numbers sort *above* positive ones
/// (`-1 = 0x00FF_FFFF_FFFF_FFFF > 1 = 0x0000_0000_0000_0001`).  We fix this
/// by flipping the sign bit (bit 55) of the payload so that:
///   - The most-negative value maps to 0 (lowest key).
///   - Zero maps to `0x0080_0000_0000_0000` (mid-point).
///   - Positive values map above that.
///
/// All other tag bytes (strings, floats) are returned unchanged because their
/// in-memory sort order is either not meaningful for range queries or they use
/// a different encoding.
#[inline]
pub fn sort_key(raw: u64) -> u64 {
    if (raw >> 56) == INT64_TAG {
        raw ^ 0x0080_0000_0000_0000
    } else {
        raw
    }
}

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
/// Use [`PropertyIndex::build_for`] to load a specific column on demand, or
/// [`PropertyIndex::build`] for an upfront full build.  Then use
/// [`PropertyIndex::lookup`] for O(log n) equality queries.
#[derive(Default)]
pub struct PropertyIndex {
    /// `index[(label_id, col_id)][raw_value] = [slot, ...]`
    index: std::collections::HashMap<IndexKey, BTreeMap<u64, Vec<u32>>>,
    /// Set of `(label_id, col_id)` pairs whose column file has already been
    /// loaded into `index`.  A pair is present here even when its column file
    /// contained no indexable values, so that `build_for` can skip repeated I/O.
    loaded: HashSet<IndexKey>,
}

impl PropertyIndex {
    /// Construct a fresh empty index (no disk I/O).
    pub fn new() -> Self {
        PropertyIndex::default()
    }

    /// Lazily load the index for a single `(label_id, col_id)` pair.
    ///
    /// Reads `nodes/{label_id}/col_{col_id}.bin` and the corresponding tombstone
    /// column (`col_0`) from `store` and inserts `value → slot` mappings into
    /// the in-memory B-tree for this pair.
    ///
    /// **Cache hit**: if this `(label_id, col_id)` pair has already been loaded
    /// (including cases where the column had no indexable values), this method
    /// returns immediately without any I/O.
    ///
    /// **No-op for col_id 0**: the tombstone column is not indexed.
    ///
    /// Errors are logged as warnings and suppressed — the caller falls back to
    /// a full O(n) scan via `is_indexed` returning `false`.
    pub fn build_for(&mut self, store: &NodeStore, label_id: u32, col_id: u32) -> Result<()> {
        // col_0 is the tombstone column and is not a queryable property.
        if col_id == 0 {
            return Ok(());
        }

        let key = IndexKey { label_id, col_id };

        // Cache hit — already loaded.
        if self.loaded.contains(&key) {
            return Ok(());
        }

        // Mark as loaded regardless of outcome so we don't retry on I/O errors.
        self.loaded.insert(key);

        // Read tombstone column (col_0) to skip deleted slots.
        let tombstone_slots: HashSet<u32> = match store.read_col_all(label_id, 0) {
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
            Err(_) => HashSet::new(),
        };

        // Read the target column.
        let raw_vals = match store.read_col_all(label_id, col_id) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    label_id = label_id,
                    col_id = col_id,
                    error = ?e,
                    "PropertyIndex::build_for: failed to read column; index disabled for this pair"
                );
                return Ok(());
            }
        };

        let btree = self.index.entry(key).or_default();
        for (slot, raw) in raw_vals.into_iter().enumerate() {
            let slot = slot as u32;
            if raw == ABSENT || tombstone_slots.contains(&slot) {
                continue;
            }
            // Apply the same sort_key transform used by `build` and `lookup`
            // so that integer ordering and equality checks are consistent.
            btree.entry(sort_key(raw)).or_default().push(slot);
        }

        Ok(())
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
                    btree.entry(sort_key(raw)).or_default().push(slot);
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
            Some(btree) => btree
                .get(&sort_key(raw_value))
                .map(|v| v.as_slice())
                .unwrap_or(&[]),
            None => &[],
        }
    }

    /// Look up all live node slots for `(label_id, col_id)` whose sort-key
    /// falls within the specified range.
    ///
    /// `lo` and `hi` are `(sort_key_value, inclusive)` pairs; pass `None` for
    /// an unbounded end.  The sort-key transform (see [`sort_key`]) must already
    /// have been applied to the bound values by the caller — use
    /// `sort_key(raw_value)` on each bound before calling this method.
    ///
    /// Returns an empty `Vec` when the column is not indexed or no values fall
    /// in the range.  Callers must still apply tombstone filtering.
    pub fn lookup_range(
        &self,
        label_id: u32,
        col_id: u32,
        lo: Option<(u64, bool)>,
        hi: Option<(u64, bool)>,
    ) -> Vec<u32> {
        use std::ops::Bound;

        let key = IndexKey { label_id, col_id };
        let btree = match self.index.get(&key) {
            Some(bt) => bt,
            None => return vec![],
        };

        let lo_bound = match lo {
            None => Bound::Unbounded,
            Some((v, true)) => Bound::Included(v),
            Some((v, false)) => Bound::Excluded(v),
        };
        let hi_bound = match hi {
            None => Bound::Unbounded,
            Some((v, true)) => Bound::Included(v),
            Some((v, false)) => Bound::Excluded(v),
        };

        btree
            .range((lo_bound, hi_bound))
            .flat_map(|(_k, slots)| slots.iter().copied())
            .collect()
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
            .entry(sort_key(raw_value))
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
            let sk_old = sort_key(old_raw);
            if let Some(slots) = btree.get_mut(&sk_old) {
                slots.retain(|&s| s != slot);
                if slots.is_empty() {
                    btree.remove(&sk_old);
                }
            }
        }

        // Insert into new bucket.
        if new_raw != ABSENT {
            btree.entry(sort_key(new_raw)).or_default().push(slot);
        }
    }

    /// Remove a slot from all buckets for `(label_id, col_id)` after node
    /// deletion (tombstoning).  Used to keep the index consistent.
    pub fn remove(&mut self, label_id: u32, col_id: u32, slot: u32, raw_value: u64) {
        if raw_value == ABSENT {
            return;
        }
        let sk = sort_key(raw_value);
        let key = IndexKey { label_id, col_id };
        if let Some(btree) = self.index.get_mut(&key) {
            if let Some(slots) = btree.get_mut(&sk) {
                slots.retain(|&s| s != slot);
                if slots.is_empty() {
                    btree.remove(&sk);
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
