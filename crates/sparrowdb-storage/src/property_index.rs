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
use std::fs;
use std::io::Write;
use std::path::Path;

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
///
/// ## Clone cost (SPA-232 fix)
///
/// The inner B-tree for each `(label_id, col_id)` is wrapped in an `Arc` so
/// that cloning a `PropertyIndex` is O(number of indexed columns) rather than
/// O(total index entries).  This eliminates the per-query clone overhead when
/// the shared property-index cache is warm (e.g. after Q1 point-lookups build
/// a large uid index that Q4/Q5/Q8 multi-hop queries don't need).
///
/// Mutation methods (`build_for`, `insert`, `update`, `remove`) use
/// `Arc::make_mut` to obtain exclusive access (copy-on-write semantics):
/// only the first mutation of a shared Arc copies the BTreeMap; subsequent
/// mutations on the same Engine's private copy are in-place.
///
/// ## Generation counter (SPA-242 fix)
///
/// A monotonically increasing `generation` counter tracks each invalidation
/// of the shared property-index cache.  When a read `Engine` clones the
/// shared cache it records the generation.  `write_back_prop_index` only
/// merges data back when the shared cache's generation matches the clone
/// generation, preventing a slow read from writing stale index data back
/// after a write transaction has cleared the cache.
#[derive(Default, Clone)]
pub struct PropertyIndex {
    /// `index[(label_id, col_id)] = Arc<BTreeMap<raw_value, [slot, ...]>>`
    ///
    /// The `Arc` wrapper makes `Clone` O(loaded.len()) instead of
    /// O(Σ entries per column).  `Arc::make_mut` is used for mutations to
    /// ensure copy-on-write isolation between the shared cache and per-Engine
    /// copies.
    index: std::collections::HashMap<IndexKey, std::sync::Arc<BTreeMap<u64, Vec<u32>>>>,
    /// Set of `(label_id, col_id)` pairs whose column file has already been
    /// loaded into `index`.  A pair is present here even when its column file
    /// contained no indexable values, so that `build_for` can skip repeated I/O.
    loaded: HashSet<IndexKey>,
    /// Invalidation generation counter (SPA-242).
    ///
    /// Incremented by `clear()` each time the shared cache is invalidated by a
    /// write-transaction commit.  Cloned with the rest of the struct so that
    /// each per-Engine copy remembers the generation it was derived from.
    /// `write_back_prop_index` compares the engine's `generation` against the
    /// shared cache's current `generation` before merging: if they differ, a
    /// write committed and cleared the cache since this engine was created, so
    /// the engine's index data is stale and must not be written back.
    pub generation: u64,
    /// Number of index keys at the time of the last successful `save_to_dir`.
    /// Used by `persist_if_grew` to skip redundant disk writes when the
    /// shared cache has not actually gained new columns since the last persist.
    persisted_key_count: usize,
}

impl PropertyIndex {
    /// Construct a fresh empty index (no disk I/O).
    pub fn new() -> Self {
        PropertyIndex::default()
    }

    /// Reset the index to empty, discarding all cached column data.
    ///
    /// Used by `GraphDb` to invalidate the shared property-index cache after
    /// a write transaction commits (the on-disk column files may have changed).
    ///
    /// Increments `generation` so that any `Engine` that cloned the cache
    /// before this clear knows its data is stale (SPA-242).
    pub fn clear(&mut self) {
        self.index.clear();
        self.loaded.clear();
        self.generation = self.generation.wrapping_add(1);
        // Reset the persistence watermark so that persist_if_grew will write
        // again after the index is repopulated (the on-disk file is removed
        // separately via remove_persisted, but the count must be zeroed here
        // so the threshold comparison starts from scratch).
        self.persisted_key_count = 0;
    }

    /// Merge column data from `other` into `self` using union semantics.
    ///
    /// Only columns present in `other.loaded` but absent from `self.loaded` are
    /// incorporated — already-cached columns are left unchanged.  This avoids
    /// last-writer-wins races when multiple concurrent reads write back to the
    /// shared cache simultaneously.
    pub fn merge_from(&mut self, other: &PropertyIndex) {
        for key in &other.loaded {
            if self.loaded.insert(*key) {
                if let Some(btree) = other.index.get(key) {
                    self.index.insert(*key, btree.clone());
                }
            }
        }
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

        // Build a fresh BTreeMap for this column (always a new key at this
        // point since we checked `self.loaded` above and returned early on
        // cache hit).  Insert as `Arc::new` so later clones of this
        // `PropertyIndex` share the data without copying.
        let mut btree: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for (slot, raw) in raw_vals.into_iter().enumerate() {
            let slot = slot as u32;
            if raw == ABSENT || tombstone_slots.contains(&slot) {
                continue;
            }
            // Apply the same sort_key transform used by `build` and `lookup`
            // so that integer ordering and equality checks are consistent.
            btree.entry(sort_key(raw)).or_default().push(slot);
        }
        self.index.insert(key, std::sync::Arc::new(btree));

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

                let mut btree: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
                for (slot, raw) in raw_vals.into_iter().enumerate() {
                    let slot = slot as u32;
                    // Skip absent and tombstoned slots.
                    if raw == ABSENT || tombstone_slots.contains(&slot) {
                        continue;
                    }
                    btree.entry(sort_key(raw)).or_default().push(slot);
                }
                idx.index.insert(key, std::sync::Arc::new(btree));
                idx.loaded.insert(key);
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
        // `Arc::make_mut` gives us a `&mut BTreeMap` with copy-on-write
        // semantics: if the Arc is shared (e.g. was cloned from the cache),
        // the inner BTreeMap is cloned here; otherwise mutation is in-place.
        let btree = std::sync::Arc::make_mut(
            self.index
                .entry(key)
                .or_insert_with(|| std::sync::Arc::new(BTreeMap::new())),
        );
        btree.entry(sort_key(raw_value)).or_default().push(slot);
    }

    /// Update a slot from `old_raw` to `new_raw` after a SET operation.
    ///
    /// Removes the slot from the old bucket and inserts it into the new one.
    /// A no-op for the absent sentinel.
    pub fn update(&mut self, label_id: u32, col_id: u32, slot: u32, old_raw: u64, new_raw: u64) {
        let key = IndexKey { label_id, col_id };
        let btree = std::sync::Arc::make_mut(
            self.index
                .entry(key)
                .or_insert_with(|| std::sync::Arc::new(BTreeMap::new())),
        );

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
        if let Some(arc_btree) = self.index.get_mut(&key) {
            let btree = std::sync::Arc::make_mut(arc_btree);
            if let Some(slots) = btree.get_mut(&sk) {
                slots.retain(|&s| s != slot);
                if slots.is_empty() {
                    btree.remove(&sk);
                }
            }
        }
    }

    /// Return the number of distinct values stored in the index for
    /// `(label_id, col_id)`.
    ///
    /// Each unique raw-encoded value is one BTree key, so this equals the
    /// cardinality (number of distinct values) seen at index-build time.
    ///
    /// Returns `0` if the column has not been indexed yet (i.e. `build_for`
    /// has not been called for this pair).  Callers should ensure the index
    /// has been built before calling this method.
    pub fn n_distinct(&self, label_id: u32, col_id: u32) -> usize {
        let key = IndexKey { label_id, col_id };
        match self.index.get(&key) {
            Some(btree) => btree.len(),
            None => 0,
        }
    }

    /// Convert a `NodeId` to the corresponding slot index for this label.
    #[inline]
    pub fn node_id_to_slot(node_id: NodeId) -> u32 {
        (node_id.0 & 0xFFFF_FFFF) as u32
    }

    // ── Persistence (SPA-286) ────────────────────────────────────────────────

    /// File name for the persisted property index within the db root directory.
    const INDEX_FILE: &'static str = "prop_index.bin";

    /// Persist the current in-memory property index to disk as a flat binary
    /// file at `{db_root}/prop_index.bin`.
    ///
    /// ## Binary format
    ///
    /// ```text
    /// HEADER: [magic: u32 = 0x50524F50] [version: u32 = 1] [num_keys: u32]
    /// for each indexed (label_id, col_id):
    ///   [label_id: u32] [col_id: u32] [num_entries: u32]
    ///   for each BTree entry:
    ///     [sort_key: u64] [num_slots: u32] [slot_0: u32 .. slot_N: u32]
    /// ```
    ///
    /// Errors are logged and suppressed — failing to persist an index is not
    /// fatal; the next `Engine` will simply rebuild from column files.
    pub fn save_to_dir(&mut self, db_root: &Path) {
        if self.index.is_empty() {
            // Nothing to persist — remove any stale file.
            let _ = fs::remove_file(db_root.join(Self::INDEX_FILE));
            self.persisted_key_count = 0;
            return;
        }
        if let Err(e) = self.save_to_dir_inner(db_root) {
            tracing::warn!(error = ?e, "PropertyIndex::save_to_dir: failed to persist index");
        } else {
            self.persisted_key_count = self.index.len();
        }
    }

    /// Persist the index to disk only if new columns have been loaded since the
    /// last persist (i.e. `self.index.len() > self.persisted_key_count`).
    ///
    /// This avoids redundant disk writes on queries that don't load any new
    /// index columns (e.g. repeated MATCH queries on already-indexed columns).
    pub fn persist_if_grew(&mut self, db_root: &Path) {
        if self.index.len() > self.persisted_key_count {
            self.save_to_dir(db_root);
        }
    }

    fn save_to_dir_inner(&self, db_root: &Path) -> std::io::Result<()> {
        let path = db_root.join(Self::INDEX_FILE);
        let tmp_path = db_root.join("prop_index.bin.tmp");

        let mut buf: Vec<u8> = Vec::new();

        // Magic + version + num_keys
        buf.write_all(&0x50524F50u32.to_le_bytes())?;
        buf.write_all(&1u32.to_le_bytes())?;
        buf.write_all(&(self.index.len() as u32).to_le_bytes())?;

        for (key, btree) in &self.index {
            buf.write_all(&key.label_id.to_le_bytes())?;
            buf.write_all(&key.col_id.to_le_bytes())?;
            buf.write_all(&(btree.len() as u32).to_le_bytes())?;
            for (&sort_key_val, slots) in btree.as_ref() {
                buf.write_all(&sort_key_val.to_le_bytes())?;
                buf.write_all(&(slots.len() as u32).to_le_bytes())?;
                for &slot in slots {
                    buf.write_all(&slot.to_le_bytes())?;
                }
            }
        }

        // Atomic write: write to tmp then rename.
        fs::write(&tmp_path, &buf)?;
        fs::rename(&tmp_path, &path)?;
        Ok(())
    }

    /// Load a persisted property index from `{db_root}/prop_index.bin`.
    ///
    /// Returns a fully populated `PropertyIndex` on success, or `None` if the
    /// file does not exist, is corrupt, or has an unsupported version.  In the
    /// `None` case the caller should fall back to the default empty index
    /// (lazy rebuild from column files).
    pub fn load_from_dir(db_root: &Path) -> Option<PropertyIndex> {
        let path = db_root.join(Self::INDEX_FILE);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => return None,
        };
        Self::decode(&data)
    }

    fn decode(data: &[u8]) -> Option<PropertyIndex> {
        let mut cursor = 0usize;

        // Helper to read N bytes.
        let read_bytes = |cursor: &mut usize, n: usize| -> Option<&[u8]> {
            if *cursor + n > data.len() {
                return None;
            }
            let slice = &data[*cursor..*cursor + n];
            *cursor += n;
            Some(slice)
        };
        let read_u32 = |cursor: &mut usize| -> Option<u32> {
            let b = read_bytes(cursor, 4)?;
            Some(u32::from_le_bytes(b.try_into().ok()?))
        };
        let read_u64 = |cursor: &mut usize| -> Option<u64> {
            let b = read_bytes(cursor, 8)?;
            Some(u64::from_le_bytes(b.try_into().ok()?))
        };

        // Check magic and version.
        let magic = read_u32(&mut cursor)?;
        if magic != 0x50524F50 {
            return None;
        }
        let version = read_u32(&mut cursor)?;
        if version != 1 {
            return None;
        }

        let num_keys = read_u32(&mut cursor)? as usize;
        let mut index = std::collections::HashMap::with_capacity(num_keys);
        let mut loaded = HashSet::with_capacity(num_keys);

        for _ in 0..num_keys {
            let label_id = read_u32(&mut cursor)?;
            let col_id = read_u32(&mut cursor)?;
            let num_entries = read_u32(&mut cursor)? as usize;

            let mut btree = BTreeMap::new();
            for _ in 0..num_entries {
                let sk = read_u64(&mut cursor)?;
                let num_slots = read_u32(&mut cursor)? as usize;
                let mut slots = Vec::with_capacity(num_slots);
                for _ in 0..num_slots {
                    slots.push(read_u32(&mut cursor)?);
                }
                btree.insert(sk, slots);
            }

            let key = IndexKey { label_id, col_id };
            index.insert(key, std::sync::Arc::new(btree));
            loaded.insert(key);
        }

        // Verify we consumed exactly all bytes (guards against trailing garbage).
        if cursor != data.len() {
            return None;
        }

        let persisted_key_count = index.len();
        Some(PropertyIndex {
            index,
            loaded,
            generation: 0,
            persisted_key_count,
        })
    }

    /// Remove the persisted property index file from disk.
    ///
    /// Called when the index is invalidated (e.g. after a write-transaction
    /// commit) so that stale data is never loaded on the next open.
    pub fn remove_persisted(db_root: &Path) {
        let _ = fs::remove_file(db_root.join(Self::INDEX_FILE));
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

    // ── Persistence tests (SPA-286) ──────────────────────────────────────

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = PropertyIndex::new();
        idx.insert(1, 2, 0, 42u64);
        idx.insert(1, 2, 1, 42u64);
        idx.insert(1, 2, 2, 99u64);
        idx.insert(3, 5, 10, 7u64);

        idx.save_to_dir(dir.path());

        let loaded = PropertyIndex::load_from_dir(dir.path()).expect("should load persisted index");
        assert_eq!(loaded.lookup(1, 2, 42u64), &[0u32, 1]);
        assert_eq!(loaded.lookup(1, 2, 99u64), &[2u32]);
        assert_eq!(loaded.lookup(3, 5, 7u64), &[10u32]);
        assert!(loaded.is_indexed(1, 2));
        assert!(loaded.is_indexed(3, 5));
    }

    #[test]
    fn load_returns_none_when_no_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(PropertyIndex::load_from_dir(dir.path()).is_none());
    }

    #[test]
    fn load_returns_none_on_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("prop_index.bin"), b"garbage").unwrap();
        assert!(PropertyIndex::load_from_dir(dir.path()).is_none());
    }

    #[test]
    fn remove_persisted_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = PropertyIndex::new();
        idx.insert(1, 1, 0, 1u64);
        idx.save_to_dir(dir.path());
        assert!(dir.path().join("prop_index.bin").exists());

        PropertyIndex::remove_persisted(dir.path());
        assert!(!dir.path().join("prop_index.bin").exists());
    }

    #[test]
    fn empty_index_removes_file_on_save() {
        let dir = tempfile::tempdir().unwrap();
        // Create a file first.
        let mut idx = PropertyIndex::new();
        idx.insert(1, 1, 0, 1u64);
        idx.save_to_dir(dir.path());
        assert!(dir.path().join("prop_index.bin").exists());

        // Saving an empty index removes it.
        let mut empty = PropertyIndex::new();
        empty.save_to_dir(dir.path());
        assert!(!dir.path().join("prop_index.bin").exists());
    }

    #[test]
    fn range_lookup_survives_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = PropertyIndex::new();
        // Insert integer values (tag byte 0x00 + payload).
        // sort_key flips sign bit for INT64 tag, so we use sort_key directly.
        for i in 0u64..10 {
            let raw = i; // simplified: raw values without INT64 tag
            idx.insert(1, 1, i as u32, raw);
        }
        idx.save_to_dir(dir.path());

        let loaded = PropertyIndex::load_from_dir(dir.path()).unwrap();
        // Range query: sort_key(3) .. sort_key(7) inclusive.
        let results =
            loaded.lookup_range(1, 1, Some((sort_key(3), true)), Some((sort_key(7), true)));
        // Should contain slots 3, 4, 5, 6, 7.
        assert_eq!(results.len(), 5);
        for s in [3u32, 4, 5, 6, 7] {
            assert!(results.contains(&s), "expected slot {s} in range results");
        }
    }
}
