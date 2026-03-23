//! In-memory text search index for CONTAINS and STARTS WITH predicates (SPA-251).
//!
//! ## Motivation
//!
//! Without an index, `WHERE n.prop CONTAINS 'str'` and `WHERE n.prop STARTS WITH 'str'`
//! must decode every property value for every node in the label — O(n) disk reads.
//! The `TextIndex` pre-decodes all string values at Engine construction time (same
//! amortised cost as the existing `PropertyIndex`) and stores them as sorted
//! `(decoded_string, slot)` pairs, enabling:
//!
//! - **STARTS WITH** — binary search to find the first entry with the given prefix,
//!   then a linear walk while the prefix holds: O(log n + k) where k is the hit count.
//! - **CONTAINS** — linear scan of the sorted list with early-exit where possible;
//!   avoids the per-slot property-decode overhead for non-matching slots.
//!
//! ## Design
//!
//! The index is keyed by `(label_id, col_id)`.  For each key it stores a
//! `Vec<(String, u32)>` sorted lexicographically by the decoded string value.
//! Sorting enables the binary-search prefix range for `STARTS WITH`.
//!
//! ## Lifecycle
//!
//! Built at `Engine` construction alongside `PropertyIndex` by scanning all on-disk
//! column files and decoding string-typed values.  Integer/bool/null cells are skipped.
//! Updated incrementally after each `CREATE` via [`TextIndex::insert`].
//!
//! ## Thread safety
//!
//! Owned by a single `Engine` instance; no cross-thread sharing.

use std::collections::HashMap;

use crate::node_store::{NodeStore, Value as StoreValue};

// ── Sentinel constants (mirrored from property_index) ─────────────────────────

/// Deletion tombstone sentinel in `col_0`.
const TOMBSTONE: u64 = u64::MAX;

/// Zero sentinel — slot was never written.
const ABSENT: u64 = 0;

// ── Index key ─────────────────────────────────────────────────────────────────

/// Compound key identifying a single indexed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TextIndexKey {
    label_id: u32,
    col_id: u32,
}

// ── TextIndex ─────────────────────────────────────────────────────────────────

/// In-memory text search index for CONTAINS and STARTS WITH.
///
/// Maps `(label_id, col_id)` → `Vec<(decoded_string, slot)>` sorted by string.
///
/// Use [`TextIndex::build`] to construct from disk, then
/// [`TextIndex::lookup_contains`] / [`TextIndex::lookup_starts_with`] for
/// candidate-slot retrieval.
#[derive(Default)]
pub struct TextIndex {
    /// `entries[(label_id, col_id)]` = Vec sorted by `.0` (the decoded string).
    entries: HashMap<TextIndexKey, Vec<(String, u32)>>,
}

impl TextIndex {
    /// Construct a fresh empty index (no disk I/O).
    pub fn new() -> Self {
        TextIndex::default()
    }

    /// Build the index by scanning all label/column files in `store`.
    ///
    /// Only string-typed cells are indexed; integer and null cells are skipped.
    /// Tombstoned slots (col_0 == `u64::MAX`) are excluded.
    ///
    /// This is O(total stored string cells) in time and memory.
    pub fn build(store: &NodeStore, label_ids: &[(u32, String)]) -> Self {
        let mut idx = TextIndex::new();

        for &(label_id, ref _label_name) in label_ids {
            // Discover which columns exist for this label.
            let col_ids = match store.col_ids_for_label(label_id) {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!(
                        "WARN: TextIndex::build: failed to list col_ids for label {label_id}: {e}; skipping"
                    );
                    continue;
                }
            };

            // Read tombstone column (col_0) once to know which slots are dead.
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
                let raw_vals = match store.read_col_all(label_id, col_id) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "WARN: TextIndex::build: failed to read col_{col_id} for label {label_id}: {e}; skipping"
                        );
                        continue;
                    }
                };

                let key = TextIndexKey { label_id, col_id };
                let bucket = idx.entries.entry(key).or_default();

                for (slot, raw) in raw_vals.into_iter().enumerate() {
                    let slot = slot as u32;
                    if raw == ABSENT || tombstone_slots.contains(&slot) {
                        continue;
                    }
                    // Only index string values; skip integers and booleans.
                    if let StoreValue::Bytes(b) = store.decode_raw_value(raw) {
                        let s = String::from_utf8_lossy(&b).into_owned();
                        bucket.push((s, slot));
                    }
                }

                // Sort by string for binary-search prefix queries.
                bucket.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            }
        }

        idx
    }

    /// Returns `true` when `(label_id, col_id)` is present in the index.
    pub fn is_indexed(&self, label_id: u32, col_id: u32) -> bool {
        self.entries
            .contains_key(&TextIndexKey { label_id, col_id })
    }

    /// Insert a new string entry after a successful node CREATE.
    ///
    /// Maintains sort order by inserting at the correct position.
    /// Non-string raw values are silently ignored.
    pub fn insert(&mut self, label_id: u32, col_id: u32, slot: u32, decoded_string: String) {
        let key = TextIndexKey { label_id, col_id };
        let bucket = self.entries.entry(key).or_default();
        let pos = bucket.partition_point(|(s, _)| s.as_str() < decoded_string.as_str());
        bucket.insert(pos, (decoded_string, slot));
    }

    /// Return candidate slots where the decoded string **contains** `pattern`.
    ///
    /// Performs a linear scan of all string entries for this column;
    /// avoids the per-slot disk read for non-matching nodes.
    ///
    /// Returns an empty `Vec` when the column is not indexed.
    pub fn lookup_contains(&self, label_id: u32, col_id: u32, pattern: &str) -> Vec<u32> {
        let key = TextIndexKey { label_id, col_id };
        match self.entries.get(&key) {
            None => vec![],
            Some(bucket) => bucket
                .iter()
                .filter(|(s, _)| s.contains(pattern))
                .map(|(_, slot)| *slot)
                .collect(),
        }
    }

    /// Return candidate slots where the decoded string **starts with** `prefix`.
    ///
    /// Uses binary search to find the first matching entry, then walks forward
    /// while the prefix holds: O(log n + k).
    ///
    /// Returns an empty `Vec` when the column is not indexed.
    pub fn lookup_starts_with(&self, label_id: u32, col_id: u32, prefix: &str) -> Vec<u32> {
        let key = TextIndexKey { label_id, col_id };
        match self.entries.get(&key) {
            None => vec![],
            Some(bucket) => {
                let start = bucket.partition_point(|(s, _)| s.as_str() < prefix);
                bucket[start..]
                    .iter()
                    .take_while(|(s, _)| s.starts_with(prefix))
                    .map(|(_, slot)| *slot)
                    .collect()
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index_with_entries(entries: &[(&str, u32)]) -> TextIndex {
        let mut idx = TextIndex::new();
        let bucket = idx
            .entries
            .entry(TextIndexKey {
                label_id: 1,
                col_id: 1,
            })
            .or_default();
        for (s, slot) in entries {
            bucket.push((s.to_string(), *slot));
        }
        bucket.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        idx
    }

    #[test]
    fn contains_basic() {
        let idx = make_index_with_entries(&[
            ("Widget Pro", 0),
            ("Widget Lite", 1),
            ("Gadget Max", 2),
            ("Banana Phone", 3),
        ]);
        let mut hits = idx.lookup_contains(1, 1, "Widget");
        hits.sort();
        assert_eq!(hits, vec![0, 1]);
    }

    #[test]
    fn contains_no_match() {
        let idx = make_index_with_entries(&[("hello", 0), ("world", 1)]);
        assert!(idx.lookup_contains(1, 1, "xyz").is_empty());
    }

    #[test]
    fn starts_with_basic() {
        let idx = make_index_with_entries(&[
            ("Widget Pro", 0),
            ("Widget Lite", 1),
            ("Gadget Max", 2),
            ("Banana Phone", 3),
        ]);
        let mut hits = idx.lookup_starts_with(1, 1, "Widget");
        hits.sort();
        assert_eq!(hits, vec![0, 1]);
    }

    #[test]
    fn starts_with_no_match() {
        let idx = make_index_with_entries(&[("hello", 0), ("world", 1)]);
        assert!(idx.lookup_starts_with(1, 1, "xyz").is_empty());
    }

    #[test]
    fn starts_with_prefix_boundary() {
        let idx = make_index_with_entries(&[("aaa", 0), ("aab", 1), ("abc", 2), ("bcd", 3)]);
        let mut hits = idx.lookup_starts_with(1, 1, "aa");
        hits.sort();
        assert_eq!(hits, vec![0, 1]);
    }

    #[test]
    fn not_indexed_returns_empty() {
        let idx = TextIndex::new();
        assert!(idx.lookup_contains(99, 99, "x").is_empty());
        assert!(idx.lookup_starts_with(99, 99, "x").is_empty());
    }

    #[test]
    fn insert_maintains_sort_order() {
        let mut idx = TextIndex::new();
        idx.insert(1, 1, 2, "gamma".to_string());
        idx.insert(1, 1, 0, "alpha".to_string());
        idx.insert(1, 1, 1, "beta".to_string());
        // After inserts, should find "beta" with starts_with
        let hits = idx.lookup_starts_with(1, 1, "bet");
        assert_eq!(hits, vec![1]);
        // All three have nothing in common with "delta"
        assert!(idx.lookup_starts_with(1, 1, "delta").is_empty());
    }
}
