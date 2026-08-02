//! HNSW (Hierarchical Navigable Small World) vector similarity index.
//!
//! Pure-Rust, zero-dependency ANN implementation.
//! Implements the algorithm from Malkov & Yashunin (2018).
//!
//! ## Parameters
//! - `M`  — maximum number of bi-directional connections per layer (default 16).
//! - `ef_construction` — size of the dynamic candidate list during insert (default 200).
//! - `ef_search` — size of the dynamic candidate list during search (default 50).
//!
//! ## Thread safety
//! The `VectorIndex` itself is not `Sync`. Wrap in `Arc<RwLock<VectorIndex>>`
//! for shared-memory-writer-reads (SWMR) access patterns.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// On-disk format version for HNSW index files.
///
/// - `1` — bare `bincode` payload, no header, no integrity check (legacy).
///   Still readable; upgraded to v2 the next time the index is saved.
/// - `2` — 36-byte header (magic, version, payload length, generation counter
///   and CRC32C; see [`HNSW_HEADER_LEN`]) followed by the `bincode` payload.
///   Written atomically via temp-file + fsync + rename + directory fsync.
///
/// Increment when the serialisation format changes incompatibly.
pub const HNSW_FORMAT_VERSION: u32 = 2;

/// Magic bytes at offset 0 of a v2 index file.
const HNSW_MAGIC: &[u8; 8] = b"SPRWHNSW";

/// Size of the v2 header in bytes.
///
/// ```text
///  0..8   magic       b"SPRWHNSW"
///  8..12  version     u32 LE
/// 12..16  reserved    u32 LE (zero; keeps the u64 fields 8-byte aligned)
/// 16..24  payload_len u64 LE
/// 24..32  generation  u64 LE
/// 32..36  crc32c      u32 LE — over bytes[0..32] ++ payload
/// ```
///
/// The checksum is last and covers the rest of the header as well as the
/// payload, so a corrupted `generation` or `payload_len` is detected rather
/// than trusted.
const HNSW_HEADER_LEN: usize = 36;

/// Offset of the CRC field, i.e. the number of leading header bytes the CRC
/// itself covers.
const HNSW_CRC_OFFSET: usize = 32;

/// Prefix of the error message returned by [`VectorIndex::save`] when the file
/// on disk has advanced past the generation this handle loaded.  Callers that
/// want to distinguish a lost-update refusal from an I/O failure can match on
/// it; see [`VectorIndex::is_lost_update`].
const LOST_UPDATE_PREFIX: &str = "HNSW index generation conflict";

/// On-disk generation counter, held out of the serialised payload.
///
/// Wrapped in a newtype so `VectorIndex` can keep deriving `Clone` (an
/// `AtomicU64` is not `Clone`) and so `save(&self)` can record the generation
/// it just wrote without needing `&mut self` — which would ripple through every
/// caller that holds the index behind a lock guard.
#[derive(Debug, Default)]
struct Generation(std::sync::atomic::AtomicU64);

impl Generation {
    fn get(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
    fn set(&self, v: u64) {
        self.0.store(v, std::sync::atomic::Ordering::Release);
    }
}

impl Clone for Generation {
    fn clone(&self) -> Self {
        Generation(std::sync::atomic::AtomicU64::new(self.get()))
    }
}

// ── Distance metrics ──────────────────────────────────────────────────────────

/// Supported distance/similarity metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    Cosine,
    Euclidean,
    DotProduct,
}

/// Compute cosine similarity between two vectors.
/// Returns a value in [−1, 1]; higher = more similar.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimension mismatch");
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (&ai, &bi) in a.iter().zip(b.iter()) {
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < f32::EPSILON {
        0.0
    } else {
        (dot / denom).clamp(-1.0, 1.0)
    }
}

/// Compute dot product between two vectors.
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimension mismatch");
    a.iter().zip(b.iter()).map(|(&ai, &bi)| ai * bi).sum()
}

/// Compute Euclidean distance between two vectors (L2).
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimension mismatch");
    a.iter()
        .zip(b.iter())
        .map(|(&ai, &bi)| (ai - bi) * (ai - bi))
        .sum::<f32>()
        .sqrt()
}

/// Convert a distance/similarity score to an internal "distance" value used for
/// heap ordering (lower = better candidate).
///
/// For cosine and dot product, we invert the score so the heap pops the best match first.
fn to_internal_distance(score: f32, metric: Metric) -> f32 {
    match metric {
        Metric::Cosine | Metric::DotProduct => -score, // negate: higher score = lower distance
        Metric::Euclidean => score,
    }
}

/// Convert internal distance back to a user-facing score.
///
/// For Euclidean the internal distance is the raw L2 value (lower = closer).
/// We negate it here so that the universal "higher score = better match"
/// invariant holds for all three metrics, matching the contract documented on
/// `search()` and `brute_force_search()`.
fn to_score(internal_dist: f32, metric: Metric) -> f32 {
    match metric {
        Metric::Cosine | Metric::DotProduct => -internal_dist,
        Metric::Euclidean => -internal_dist,
    }
}

// ── HNSW node ─────────────────────────────────────────────────────────────────

/// One node in the HNSW graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnswNode {
    /// Application-level identifier (e.g. packed NodeId.0).
    node_id: u64,
    /// The raw embedding vector.
    vector: Vec<f32>,
    /// Per-level adjacency lists.  `connections[0]` is the base layer.
    connections: Vec<Vec<u32>>, // index into VectorIndex::nodes
}

// ── Insert outcome ────────────────────────────────────────────────────────────

/// What [`VectorIndex::insert`] actually did.
///
/// Before issue #441 `insert` returned `()` and silently discarded a write when
/// the `node_id` was already present, so an embedding backfill could report
/// "1721 written" while changing nothing.  Callers must be able to distinguish
/// the two cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The `node_id` was not in the index; a new vector was added.
    Inserted,
    /// The `node_id` already had a vector; it was replaced with the new one
    /// and the node's graph links were rebuilt.
    Updated,
}

// ── HNSW index ────────────────────────────────────────────────────────────────

/// HNSW vector similarity index.
///
/// Cheaply serialisable to disk via `bincode`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndex {
    /// All inserted nodes, indexed by their internal slot (0-based).
    nodes: Vec<HnswNode>,
    /// Map from application-level `node_id` → internal slot index.
    id_to_slot: HashMap<u64, u32>,
    /// The current entry-point slot (the node at the top layer).
    entry_point: Option<u32>,
    /// Highest layer index currently in use.
    max_layer: usize,
    // ── Hyperparameters ───────────────────────────────────────────────────────
    /// Max connections per node per layer (M in the paper).
    m: usize,
    /// Max connections at layer 0 (`m_max_0 = 2 * m` in the paper).
    m_max0: usize,
    /// ef_construction: size of the dynamic candidate list during insert.
    ef_construction: usize,
    /// ef_search: size of the dynamic candidate list during search.
    ef_search: usize,
    /// Expected vector dimensionality (informational; enforced at insert).
    pub dimensions: usize,
    /// Distance metric.
    pub metric: Metric,
    /// `1 / ln(m)` — level generation normalisation factor (mL in the paper).
    #[serde(skip)]
    ml: f64,
    /// Generation of the on-disk image this handle last agreed with: the value
    /// read by `load()`, or the value written by the most recent `save()`.
    ///
    /// Deliberately **not** serialised — it describes this handle's view of the
    /// file, not the contents of the index.  See [`VectorIndex::save`].
    #[serde(skip)]
    disk_generation: Generation,
}

impl VectorIndex {
    /// Create a new HNSW index with default hyperparameters.
    pub fn new(dimensions: usize, metric: Metric) -> Self {
        Self::with_params(dimensions, metric, 16, 200, 50)
    }

    /// Create a new HNSW index with explicit hyperparameters.
    ///
    /// - `m` — max connections per layer (16 is a good default).
    /// - `ef_construction` — exploration factor during insertion (200).
    /// - `ef_search` — exploration factor during search (50).
    pub fn with_params(
        dimensions: usize,
        metric: Metric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Self {
        let ml = 1.0 / (m as f64).ln();
        VectorIndex {
            nodes: Vec::new(),
            id_to_slot: HashMap::new(),
            entry_point: None,
            max_layer: 0,
            m,
            m_max0: m * 2,
            ef_construction,
            ef_search,
            dimensions,
            metric,
            ml,
            disk_generation: Generation::default(),
        }
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    /// Save the index to `<dir>/hnsw_<label>_<prop>.bin`, **atomically**, and
    /// only if no other handle has replaced the file since this one loaded it.
    ///
    /// # Why this is not a plain `fs::write`
    ///
    /// `fs::write` opens the destination with `O_TRUNC`: the previously good
    /// index is destroyed *before* the replacement is complete.  A crash, a
    /// `SIGKILL` from a service manager, or a full disk at any point during the
    /// write leaves the only copy of the index damaged — and a damaged HNSW
    /// file does not degrade gracefully, it disappears: `load()` fails, the
    /// caller treats the index as absent, and every subsequent vector write is
    /// silently dropped.  See issue #441.
    ///
    /// # Crash-safety protocol
    ///
    /// 1. Serialise into memory and compute the CRC32C of the payload.
    /// 2. Write `header || payload` to `<path>.tmp` and `fsync` it, so the
    ///    bytes are on stable storage before anything else changes.
    /// 3. `rename(<path>.tmp, <path>)` — atomic within a filesystem, so a
    ///    reader ever sees either the complete old file or the complete new
    ///    one, never a partial one.
    /// 4. `fsync` the containing directory so the rename itself survives a
    ///    power loss (a renamed file whose directory entry was never flushed
    ///    can revert after a crash).
    ///
    /// If any step fails, `<path>` still holds the previous index and the
    /// partial temp file is removed.
    ///
    /// # Lost-update refusal
    ///
    /// The index is loaded from disk exactly once, when the database is opened,
    /// and every writer then mutates and re-saves that one in-memory copy.
    /// Nothing reloads it.  Two processes that open the same database therefore
    /// hold two independent snapshots, and whichever saves last replaces the
    /// other's work wholesale — no race window required.  A daemon that opened
    /// before a backfill will revert the entire backfill the next time it
    /// writes a single vector.  That is what destroyed 1134 embeddings in the
    /// incident behind issue #441.
    ///
    /// Every file carries a generation counter.  `save()` re-reads it
    /// immediately before writing and **refuses** when it does not match the
    /// generation this handle loaded, returning an error whose message begins
    /// with `"HNSW index generation conflict"` (see [`Self::is_lost_update`]).
    /// Refusing is not a complete answer — the caller still has to decide
    /// whether to reload and retry — but a loud failure is strictly better than
    /// silently discarding another writer's vectors, and the caller's write
    /// transaction aborts rather than committing a half-truth.
    pub fn save(&self, dir: &Path, label: &str, prop: &str) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let path = Self::index_path(dir, label, prop);
        let tmp = Self::temp_path(&path);

        // ── Lost-update check ────────────────────────────────────────────────
        let expected = self.disk_generation.get();
        if let Some(on_disk) = Self::read_generation(&path)? {
            if on_disk != expected {
                return Err(std::io::Error::other(format!(
                    "{LOST_UPDATE_PREFIX}: {} is at generation {on_disk} but this handle loaded \
                     generation {expected}. Another writer has replaced the index since it was \
                     opened; saving would discard their vectors. Reopen the database to pick up \
                     the current index before writing again.",
                    path.display()
                )));
            }
        }
        let next_generation = expected.saturating_add(1);

        let payload = bincode::serialize(self).map_err(std::io::Error::other)?;

        // See HNSW_HEADER_LEN for the layout.  The CRC is computed over the
        // preceding header bytes as well as the payload, so a flipped bit in
        // `generation` or `payload_len` is caught too.
        let mut header = Vec::with_capacity(HNSW_HEADER_LEN);
        header.extend_from_slice(HNSW_MAGIC);
        header.extend_from_slice(&HNSW_FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes()); // reserved, keeps u64s aligned
        header.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        header.extend_from_slice(&next_generation.to_le_bytes());
        debug_assert_eq!(header.len(), HNSW_CRC_OFFSET);
        let crc = crc32c::crc32c_append(crate::crc32_of(&header), &payload);
        header.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(header.len(), HNSW_HEADER_LEN);

        // Write + fsync the temp file, then rename.  Any failure removes the
        // temp file and leaves the previous index untouched.
        let write_result = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&header)?;
            f.write_all(&payload)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, &path)
        })();

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }

        // The rename has happened: the file on disk is now at `next_generation`
        // whatever else fails, so record that before the directory fsync.
        // Otherwise a failed fsync would leave this handle believing it is a
        // generation behind and refuse all of its own subsequent saves.
        self.disk_generation.set(next_generation);

        // fsync the directory so the rename is durable.  Opening a directory
        // for fsync is a Unix idiom; on other platforms the rename durability
        // guarantee is provided by the OS and this step is skipped.
        #[cfg(unix)]
        {
            let dir_handle = std::fs::File::open(dir)?;
            dir_handle.sync_all()?;
        }

        Ok(())
    }

    /// `true` when `err` is a lost-update refusal from [`Self::save`] rather
    /// than an ordinary I/O failure.
    pub fn is_lost_update(err: &std::io::Error) -> bool {
        err.to_string().starts_with(LOST_UPDATE_PREFIX)
    }

    /// Read just the generation counter from an index file.
    ///
    /// Returns `Ok(None)` when the file does not exist, and `Ok(Some(0))` for a
    /// v1 file, which predates the counter — a v1 file is by definition the
    /// oldest generation, so a handle that loaded one may replace it.
    fn read_generation(path: &Path) -> std::io::Result<Option<u64>> {
        use std::io::Read;
        let mut f = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut header = [0u8; HNSW_HEADER_LEN];
        if f.read_exact(&mut header).is_err() || &header[..8] != HNSW_MAGIC {
            return Ok(Some(0));
        }
        Ok(Some(u64::from_le_bytes(
            header[24..32].try_into().expect("8 bytes"),
        )))
    }

    /// Load the index from `<dir>/hnsw_<label>_<prop>.bin`.
    ///
    /// Returns `Ok(None)` when the file does not exist.
    ///
    /// # Integrity
    ///
    /// A v2 file is accepted only when the magic, the declared payload length
    /// **and** the CRC32C all agree with the bytes on disk.  A v1 (headerless)
    /// file is accepted only when its length exactly matches the size the
    /// decoded index re-serialises to — this catches the "shorter index written
    /// over a longer file" damage pattern, which `bincode` would otherwise
    /// accept silently by ignoring the trailing bytes and returning an index
    /// with *fewer vectors than the caller wrote*.
    ///
    /// A file that fails any of these checks is **quarantined**: it is renamed
    /// to `<path>.corrupt.<unix_millis>` and an error is returned.  Quarantine
    /// exists because the damaged bytes are the only surviving copy of the
    /// vectors, and callers that treat a load failure as "no index here" would
    /// otherwise let the next `save()` overwrite them.
    pub fn load(dir: &Path, label: &str, prop: &str) -> std::io::Result<Option<Self>> {
        let path = Self::index_path(dir, label, prop);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;

        let decoded = if bytes.len() >= HNSW_HEADER_LEN && &bytes[..8] == HNSW_MAGIC {
            Self::decode_v2(&bytes)
        } else {
            Self::decode_legacy(&bytes).map(|idx| (idx, 0u64))
        };

        let (mut idx, generation) = match decoded {
            Ok(v) => v,
            Err(reason) => {
                // The bytes themselves are damaged.  Move them aside: they are
                // the only surviving copy of the vectors, and a caller that
                // reads a load failure as "no index here" would otherwise let
                // the next save() replace them with an empty index.
                let quarantined = Self::quarantine(&path);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "HNSW index {} is corrupt ({reason}); the damaged file was preserved as {}",
                        path.display(),
                        quarantined
                            .as_deref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "<quarantine failed>".to_owned()),
                    ),
                ));
            }
        };

        // Structural validation.  Unlike a checksum failure this is *not*
        // quarantined: the bytes decoded cleanly, so the file may well be
        // recoverable by hand, and the generation check in `save()` already
        // stops an empty index from replacing it.
        if let Err(reason) = idx.validate_invariants() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "HNSW index {} decoded but is internally inconsistent ({reason}); \
                     refusing to serve it. The file has been left in place for inspection.",
                    path.display()
                ),
            ));
        }

        // Restore derived field `ml` that was skipped during serialization.
        idx.ml = 1.0 / (idx.m as f64).ln();
        idx.disk_generation.set(generation);
        Ok(Some(idx))
    }

    /// Check the structural invariants every well-formed index satisfies.
    ///
    /// A file can pass its checksum and still be unusable — an index whose
    /// `id_to_slot` disagrees with `nodes` silently rejects inserts for ids it
    /// thinks it already has while never returning them from a search, and the
    /// condition persists across restarts because it lives in the file.
    /// Cost is O(nodes + edges) with no allocation, paid once per open.
    fn validate_invariants(&self) -> std::result::Result<(), String> {
        let n = self.nodes.len();
        if self.id_to_slot.len() != n {
            return Err(format!(
                "id_to_slot maps {} ids but there are {n} nodes",
                self.id_to_slot.len()
            ));
        }
        for (&node_id, &slot) in &self.id_to_slot {
            match self.nodes.get(slot as usize) {
                None => {
                    return Err(format!(
                        "id {node_id} maps to slot {slot}, out of {n} nodes"
                    ))
                }
                Some(node) if node.node_id != node_id => {
                    return Err(format!(
                        "id {node_id} maps to slot {slot}, which holds id {}",
                        node.node_id
                    ));
                }
                Some(_) => {}
            }
        }
        match self.entry_point {
            None if n != 0 => {
                return Err(format!("{n} nodes but no entry point"));
            }
            Some(ep) if ep as usize >= n => {
                return Err(format!("entry point {ep} is out of range for {n} nodes"));
            }
            Some(ep) if self.nodes[ep as usize].connections.len() <= self.max_layer => {
                return Err(format!(
                    "entry point {ep} exists on {} layers but max_layer is {}",
                    self.nodes[ep as usize].connections.len(),
                    self.max_layer
                ));
            }
            _ => {}
        }
        for (slot, node) in self.nodes.iter().enumerate() {
            for (layer, conns) in node.connections.iter().enumerate() {
                for &nb in conns {
                    if nb as usize >= n {
                        return Err(format!(
                            "slot {slot} layer {layer} links to slot {nb}, out of {n} nodes"
                        ));
                    }
                    if nb as usize == slot {
                        return Err(format!("slot {slot} links to itself at layer {layer}"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Decode a v2 (header-prefixed, checksummed) index image, returning the
    /// index and the generation stamped in its header.
    fn decode_v2(bytes: &[u8]) -> std::result::Result<(Self, u64), String> {
        let version = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        if version != HNSW_FORMAT_VERSION {
            return Err(format!(
                "unsupported format version {version} (this build writes v{HNSW_FORMAT_VERSION})"
            ));
        }
        let payload_len = u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes"));
        let generation = u64::from_le_bytes(bytes[24..32].try_into().expect("8 bytes"));
        let expect_crc = u32::from_le_bytes(bytes[32..36].try_into().expect("4 bytes"));

        let actual_payload_len = (bytes.len() - HNSW_HEADER_LEN) as u64;
        if actual_payload_len != payload_len {
            return Err(format!(
                "header declares a {payload_len}-byte payload but the file holds \
                 {actual_payload_len} bytes — the write was torn or overwritten"
            ));
        }
        let payload = &bytes[HNSW_HEADER_LEN..];
        let actual_crc = crc32c::crc32c_append(crate::crc32_of(&bytes[..HNSW_CRC_OFFSET]), payload);
        if actual_crc != expect_crc {
            return Err(format!(
                "CRC32C mismatch: header says {expect_crc:#010x}, payload hashes to {actual_crc:#010x}"
            ));
        }
        let idx =
            bincode::deserialize(payload).map_err(|e| format!("bincode decode failed: {e}"))?;
        Ok((idx, generation))
    }

    /// Decode a v1 (headerless) index image written by an older build.
    fn decode_legacy(bytes: &[u8]) -> std::result::Result<Self, String> {
        let idx: VectorIndex =
            bincode::deserialize(bytes).map_err(|e| format!("bincode decode failed: {e}"))?;
        // `bincode::deserialize` stops at the end of the encoded value and
        // ignores anything after it.  A v1 file therefore cannot distinguish
        // "20 vectors" from "5 vectors followed by the tail of a previous
        // 20-vector file".  Re-measuring the decoded value closes that hole:
        // any length disagreement means the file is not what it claims to be.
        let encoded_len =
            bincode::serialized_size(&idx).map_err(|e| format!("size probe failed: {e}"))?;
        if encoded_len != bytes.len() as u64 {
            return Err(format!(
                "legacy index decodes to {} vectors occupying {encoded_len} bytes, but the file \
                 is {} bytes — trailing bytes from a previous, larger index",
                idx.nodes.len(),
                bytes.len()
            ));
        }
        Ok(idx)
    }

    /// Move a damaged index file aside so the bytes are not lost to the next
    /// `save()`.  Returns the quarantine path on success.
    fn quarantine(path: &Path) -> Option<PathBuf> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut name = path.to_path_buf().into_os_string();
        name.push(format!(".corrupt.{stamp}"));
        let dest = PathBuf::from(name);
        match std::fs::rename(path, &dest) {
            Ok(()) => Some(dest),
            Err(_) => None,
        }
    }

    /// Delete the persisted index file (and any leftover temp file), if any.
    pub fn remove(dir: &Path, label: &str, prop: &str) {
        let path = Self::index_path(dir, label, prop);
        let _ = std::fs::remove_file(Self::temp_path(&path));
        let _ = std::fs::remove_file(path);
    }

    /// A cheap identity for the on-disk index image, used by callers that
    /// cache a deserialised index and need to know when it has been replaced.
    ///
    /// For a v2 file this is `(payload_len, crc32c)` read from the 24-byte
    /// header — no payload I/O, and strong enough that a same-size rewrite is
    /// still detected.  For a v1 file, where no checksum exists, it falls back
    /// to `(file_len, mtime_nanos)`.
    ///
    /// Returns `Ok(None)` when the index file does not exist.
    pub fn fingerprint(dir: &Path, label: &str, prop: &str) -> std::io::Result<Option<(u64, u64)>> {
        use std::io::Read;
        let path = Self::index_path(dir, label, prop);
        let mut f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let meta = f.metadata()?;
        let mut header = [0u8; HNSW_HEADER_LEN];
        if meta.len() >= HNSW_HEADER_LEN as u64
            && f.read_exact(&mut header).is_ok()
            && &header[..8] == HNSW_MAGIC
        {
            let generation = u64::from_le_bytes(header[24..32].try_into().expect("8 bytes"));
            let crc = u32::from_le_bytes(header[32..36].try_into().expect("4 bytes"));
            return Ok(Some((generation, crc as u64)));
        }
        // Legacy file: no checksum available, fall back to size + mtime.
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Ok(Some((meta.len(), mtime)))
    }

    fn index_path(dir: &Path, label: &str, prop: &str) -> std::path::PathBuf {
        // Sanitise label and prop names so they can appear in a file name.
        let safe_label = label.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        let safe_prop = prop.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        dir.join(format!("hnsw_{safe_label}_{safe_prop}.bin"))
    }

    /// Staging path used by `save()`.  Appending (rather than replacing) the
    /// extension keeps the temp file next to the real one so `rename` stays
    /// within the same filesystem, where it is atomic.
    fn temp_path(path: &Path) -> PathBuf {
        let mut s = path.to_path_buf().into_os_string();
        s.push(".tmp");
        PathBuf::from(s)
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Draw a random layer for a new node using the HNSW level generation formula.
    ///
    /// Implements the canonical formula from Malkov & Yashunin (2018):
    /// `level = floor(-ln(uniform) * mL)` where `mL = 1 / ln(m)`.
    /// This produces a geometric distribution with P(level >= k) = (1/m)^k,
    /// so upper layers are exponentially sparser as the paper requires.
    fn random_level(&self) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        // Use a deterministic-ish hash of the current node count as a
        // pseudo-random source (no rand dependency required).
        let mut hasher = DefaultHasher::new();
        (self.nodes.len() as u64)
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407)
            .hash(&mut hasher);
        let h = hasher.finish();
        // Map the 64-bit hash to a uniform float in (0, 1].
        // Avoid ln(0) by guaranteeing the value is at least 2^-64.
        let uniform = (h as f64) / (u64::MAX as f64) + f64::EPSILON;
        // HNSW paper formula: floor(-ln(u) * mL).
        let level = (-uniform.ln() * self.ml).floor() as usize;
        // Cap at a practical maximum to avoid degenerate graphs.
        level.min(16)
    }

    /// Compute the internal distance between the query vector and node at `slot`.
    fn distance_to_slot(&self, query: &[f32], slot: u32) -> f32 {
        let vec = &self.nodes[slot as usize].vector;
        let score = match self.metric {
            Metric::Cosine => cosine_similarity(query, vec),
            Metric::DotProduct => dot_product(query, vec),
            Metric::Euclidean => euclidean_distance(query, vec),
        };
        to_internal_distance(score, self.metric)
    }

    /// Run greedy search from `entry` towards `query`, descending to `target_layer`.
    /// Returns the slot with the smallest internal distance found.
    fn greedy_search_layer(
        &self,
        query: &[f32],
        entry: u32,
        layer: usize,
        target_layer: usize,
    ) -> u32 {
        let mut current = entry;
        let mut current_dist = self.distance_to_slot(query, current);

        let mut changed = true;
        while changed && layer > target_layer {
            changed = false;
            for &nb in &self.nodes[current as usize].connections[layer] {
                let d = self.distance_to_slot(query, nb);
                if d < current_dist {
                    current_dist = d;
                    current = nb;
                    changed = true;
                }
            }
        }
        current
    }

    /// Search one layer for the `ef` nearest neighbours of `query`, starting from `entry_points`.
    ///
    /// Returns a min-heap of `(internal_dist_bits, slot)` pairs (the heap
    /// allows us to efficiently maintain the ef-sized candidate window).
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[u32],
        ef: usize,
        layer: usize,
    ) -> Vec<(f32, u32)> {
        // visited: avoid re-processing nodes
        let mut visited: HashSet<u32> = HashSet::new();
        // candidates: min-heap by distance (closest first)
        let mut candidates: BinaryHeap<std::cmp::Reverse<(OrderedF32, u32)>> = BinaryHeap::new();
        // result: max-heap by distance (furthest first, so we can trim the worst)
        let mut result: BinaryHeap<(OrderedF32, u32)> = BinaryHeap::new();

        for &ep in entry_points {
            if visited.insert(ep) {
                let d = self.distance_to_slot(query, ep);
                candidates.push(std::cmp::Reverse((OrderedF32(d), ep)));
                result.push((OrderedF32(d), ep));
            }
        }

        while let Some(std::cmp::Reverse((OrderedF32(c_dist), c_slot))) = candidates.pop() {
            // If the closest candidate is farther than the ef-th result, stop.
            if let Some(&(OrderedF32(f_dist), _)) = result.peek() {
                if c_dist > f_dist && result.len() >= ef {
                    break;
                }
            }
            // Explore neighbours of c_slot at this layer.
            let neighbours = self.nodes[c_slot as usize].connections[layer].clone();
            for nb in neighbours {
                if visited.insert(nb) {
                    let d = self.distance_to_slot(query, nb);
                    let worst = result.peek().map(|&(OrderedF32(wd), _)| wd);
                    if result.len() < ef || worst.is_none_or(|wd| d < wd) {
                        candidates.push(std::cmp::Reverse((OrderedF32(d), nb)));
                        result.push((OrderedF32(d), nb));
                        // Trim the result set to ef elements.
                        if result.len() > ef {
                            result.pop();
                        }
                    }
                }
            }
        }

        result
            .into_iter()
            .map(|(OrderedF32(d), s)| (d, s))
            .collect()
    }

    /// Select the `m_max` best neighbours from `candidates` using the simple
    /// heuristic (nearest-first, no diversity pruning in this implementation).
    fn select_neighbours(&self, candidates: &mut [(f32, u32)], m_max: usize) -> Vec<u32> {
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.iter().take(m_max).map(|&(_, s)| s).collect()
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Return the number of vectors in the index.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return `true` if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Insert or update a vector in the HNSW index.
    ///
    /// - `node_id` — application-level identifier (e.g. `NodeId.0`).
    /// - `vector`  — the embedding; must have `self.dimensions` elements.
    ///
    /// Returns [`InsertOutcome::Inserted`] when `node_id` was new and
    /// [`InsertOutcome::Updated`] when an existing vector was replaced, so
    /// callers can report what actually changed instead of guessing.
    ///
    /// # Panics
    /// Panics if `vector.len() != self.dimensions` to prevent corrupted
    /// distance calculations across the graph.
    pub fn insert(&mut self, node_id: u64, vector: &[f32]) -> InsertOutcome {
        assert_eq!(
            vector.len(),
            self.dimensions,
            "insert: vector dimension {} does not match index dimension {}",
            vector.len(),
            self.dimensions
        );
        if let Some(&slot) = self.id_to_slot.get(&node_id) {
            self.replace_vector(slot, vector);
            return InsertOutcome::Updated;
        }

        let new_slot = self.nodes.len() as u32;
        let new_level = self.random_level();

        // Allocate `new_level + 1` empty adjacency lists.
        let connections = vec![Vec::new(); new_level + 1];
        self.nodes.push(HnswNode {
            node_id,
            vector: vector.to_vec(),
            connections,
        });
        self.id_to_slot.insert(node_id, new_slot);

        if self.entry_point.is_none() {
            // First node becomes the entry point.
            self.entry_point = Some(new_slot);
            self.max_layer = new_level;
            return InsertOutcome::Inserted;
        }

        self.link_slot(new_slot, new_level);

        // Update the global entry point if the new node sits on a higher layer.
        if new_level > self.max_layer {
            self.entry_point = Some(new_slot);
            self.max_layer = new_level;
        }
        InsertOutcome::Inserted
    }

    /// Replace the vector stored at `slot` and rebuild that node's outgoing
    /// links so the graph reflects the new position.
    ///
    /// The node keeps its slot, its `node_id` mapping and its layer, so the
    /// `entry_point` / `max_layer` invariants are untouched and no other node's
    /// slot index shifts.  Inbound links from nodes that were near the *old*
    /// vector are deliberately left in place: they are now merely sub-optimal
    /// navigation edges, never incorrect ones (distances are always recomputed
    /// against each node's current vector), and keeping them preserves
    /// reachability of the updated node from the rest of the graph.
    fn replace_vector(&mut self, slot: u32, vector: &[f32]) {
        self.nodes[slot as usize].vector = vector.to_vec();
        if self.nodes.len() == 1 {
            // Sole node: no neighbours to reconsider.
            return;
        }
        let level = self.nodes[slot as usize].connections.len() - 1;
        self.link_slot(slot, level);
    }

    /// Connect `slot` (which already holds its final vector) into the graph at
    /// every layer from `min(level, max_layer)` down to 0, replacing whatever
    /// outgoing links it currently has.
    ///
    /// Shared by the insert and the update paths.  Requires `entry_point` to be
    /// set; the very first node is handled by the caller.
    fn link_slot(&mut self, slot: u32, level: usize) {
        let Some(ep) = self.entry_point else {
            return;
        };
        let vector = self.nodes[slot as usize].vector.clone();

        // Phase 1: descend from the top layer down to `level + 1`,
        //          finding the single closest node at each upper layer.
        let mut ep_current = ep;
        if self.max_layer > level {
            for l in ((level + 1)..=self.max_layer).rev() {
                ep_current = self.greedy_search_layer(&vector, ep_current, l, l - 1);
            }
        }

        // Phase 2: for each layer from min(level, max_layer) down to 0,
        //          search for ef_construction neighbours and connect bi-directionally.
        let search_top = level.min(self.max_layer);
        for layer in (0..=search_top).rev() {
            let m_max = if layer == 0 { self.m_max0 } else { self.m };
            let ef = self.ef_construction;

            let mut candidates = self.search_layer(&vector, &[ep_current], ef, layer);
            // On the update path `slot` is already reachable from the graph and
            // would otherwise be selected as its own neighbour.
            candidates.retain(|&(_, s)| s != slot);
            let selected = self.select_neighbours(&mut candidates, m_max);

            // Wire slot → selected (replacing any previous links at this layer).
            self.nodes[slot as usize].connections[layer] = selected.clone();

            // Wire selected → slot (reciprocal links), pruning if needed.
            for &nb in &selected {
                let nb_connections = self.nodes[nb as usize].connections[layer].clone();
                if !nb_connections.contains(&slot) {
                    if nb_connections.len() < m_max {
                        self.nodes[nb as usize].connections[layer].push(slot);
                    } else {
                        // Prune: keep the m_max closest neighbours.
                        let nb_vec = self.nodes[nb as usize].vector.clone();
                        let mut all: Vec<(f32, u32)> = nb_connections
                            .iter()
                            .map(|&s| (self.distance_to_slot(&nb_vec, s), s))
                            .collect();
                        all.push((self.distance_to_slot(&nb_vec, slot), slot));
                        all.sort_by(|a, b| {
                            a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        self.nodes[nb as usize].connections[layer] =
                            all.iter().take(m_max).map(|&(_, s)| s).collect();
                    }
                }
            }

            // Update the entry point for the next lower layer.
            if let Some(&(_, best_slot)) = candidates
                .iter()
                .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            {
                ep_current = best_slot;
            }
        }
    }

    /// Search for the `k` approximate nearest neighbours of `query`.
    ///
    /// Returns a list of `(node_id, score)` pairs sorted by score descending
    /// (best match first).  The score's meaning depends on the metric:
    /// - **Cosine / Dot product** — higher is more similar.
    /// - **Euclidean** — lower distance is better (scores are negated distances).
    ///
    /// For Euclidean, the returned score is the *negated* L2 distance so that
    /// "higher score = better match" is universally true for callers.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(u64, f32)> {
        assert_eq!(
            query.len(),
            self.dimensions,
            "search: query dimension {} does not match index dimension {}",
            query.len(),
            self.dimensions
        );
        if self.nodes.is_empty() {
            return Vec::new();
        }
        let ep = match self.entry_point {
            Some(e) => e,
            None => return Vec::new(),
        };
        let ef = ef.max(k);

        // Descend from the top layer to layer 1 using greedy search.
        let mut ep_current = ep;
        for layer in (1..=self.max_layer).rev() {
            ep_current = self.greedy_search_layer(query, ep_current, layer, layer - 1);
        }

        // Search layer 0 with the full ef budget.
        let mut candidates = self.search_layer(query, &[ep_current], ef, 0);

        // Sort by internal distance (ascending = best first).
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        candidates
            .into_iter()
            .take(k)
            .map(|(d, slot)| {
                let node_id = self.nodes[slot as usize].node_id;
                let score = to_score(d, self.metric);
                (node_id, score)
            })
            .collect()
    }

    /// Brute-force linear scan — used as a fallback when no HNSW index exists,
    /// or for correctness validation in tests.
    pub fn brute_force_search(
        vectors: &[(u64, Vec<f32>)],
        query: &[f32],
        k: usize,
        metric: Metric,
    ) -> Vec<(u64, f32)> {
        let mut scored: Vec<(f32, u64)> = vectors
            .iter()
            .map(|(id, v)| {
                let raw = match metric {
                    Metric::Cosine => cosine_similarity(query, v),
                    Metric::DotProduct => dot_product(query, v),
                    Metric::Euclidean => -euclidean_distance(query, v),
                };
                (raw, *id)
            })
            .collect();
        // Sort descending by score.
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(k).map(|(s, id)| (id, s)).collect()
    }
}

// ── Ordered f32 for use in binary heaps ───────────────────────────────────────

/// f32 wrapper that implements `Ord` (NaN treated as the largest value).
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedF32(f32);

impl Eq for OrderedF32 {}

impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Greater) // NaN treated as largest
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn unit_vec(dims: usize, hot: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; dims];
        v[hot % dims] = 1.0;
        v
    }

    #[test]
    fn cosine_similarity_identical() {
        let a = vec![1.0f32, 0.0, 0.0];
        assert!((cosine_similarity(&a, &a) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_orthogonal() {
        let a = vec![1.0f32, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 1e-6);
    }

    #[test]
    fn insert_and_search_cosine() {
        let mut idx = VectorIndex::new(4, Metric::Cosine);
        for i in 0u64..20 {
            let v = unit_vec(4, i as usize);
            idx.insert(i, &v);
        }
        let query = unit_vec(4, 2); // [0, 0, 1, 0]
        let results = idx.search(&query, 3, 20);
        assert!(!results.is_empty());
        // Nodes {2, 6, 10, 14, 18} all have vector [0,0,1,0] — cosine sim = 1.0.
        // HNSW may return any of them; verify the top result IS one of them and
        // has similarity ≈ 1.0.
        let best_id = results[0].0;
        let best_score = results[0].1;
        assert!(
            best_id % 4 == 2,
            "top result id={best_id} must be in the group with hot dim 2"
        );
        assert!(
            (best_score - 1.0).abs() < 1e-5,
            "cosine similarity must be ≈1.0, got {best_score}"
        );
    }

    #[test]
    fn persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = VectorIndex::new(8, Metric::Cosine);
        for i in 0u64..10 {
            idx.insert(i, &[i as f32; 8]);
        }
        idx.save(dir.path(), "TestLabel", "embedding").unwrap();

        let loaded = VectorIndex::load(dir.path(), "TestLabel", "embedding")
            .unwrap()
            .expect("index file should exist");
        assert_eq!(loaded.len(), 10);

        // Verify search still works after reload.
        let query = vec![3.0f32; 8];
        let results = loaded.search(&query, 3, 20);
        assert!(!results.is_empty());
    }

    /// `decode_legacy` rejects a headerless file whose length disagrees with
    /// `bincode::serialized_size` of the decoded value.  That check is only
    /// sound if `serialized_size` and `serialize().len()` agree exactly for
    /// this type — if they ever diverge we would quarantine healthy indexes.
    #[test]
    fn serialized_size_matches_serialize_len() {
        for n in [0u64, 1, 7, 40] {
            let mut idx = VectorIndex::new(8, Metric::Cosine);
            for i in 0..n {
                idx.insert(i, &[i as f32; 8]);
            }
            let encoded = bincode::serialize(&idx).expect("serialize");
            let measured = bincode::serialized_size(&idx).expect("serialized_size");
            assert_eq!(
                measured,
                encoded.len() as u64,
                "size probe disagrees with the encoder at n={n}; the legacy \
                 trailing-byte check would produce false positives"
            );
        }
    }

    /// A v2 file must carry the magic, the payload length and the CRC32C of
    /// exactly the bytes that follow the 24-byte header.
    #[test]
    fn v2_header_describes_the_payload() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = VectorIndex::new(4, Metric::Cosine);
        for i in 0u64..5 {
            idx.insert(i, &[i as f32, 0.0, 0.0, 0.0]);
        }
        idx.save(dir.path(), "L", "p").unwrap();

        let bytes = std::fs::read(dir.path().join("hnsw_L_p.bin")).unwrap();
        assert_eq!(&bytes[..8], HNSW_MAGIC);
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            HNSW_FORMAT_VERSION
        );
        let declared_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        assert_eq!(
            declared_len,
            (bytes.len() - HNSW_HEADER_LEN) as u64,
            "declared payload length must equal the bytes after the header"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            1,
            "the first save of a fresh index writes generation 1"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
            crc32c::crc32c_append(
                crate::crc32_of(&bytes[..HNSW_CRC_OFFSET]),
                &bytes[HNSW_HEADER_LEN..]
            ),
            "stored CRC must cover the header fields as well as the payload"
        );
    }

    /// Tampering with the generation counter alone must be detected: it is the
    /// field the lost-update check trusts.
    #[test]
    fn corrupted_generation_field_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = VectorIndex::new(2, Metric::Cosine);
        idx.insert(1, &[1.0, 0.0]);
        idx.save(dir.path(), "L", "p").unwrap();

        let path = dir.path().join("hnsw_L_p.bin");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[24] ^= 0xFF; // low byte of `generation`
        std::fs::write(&path, &bytes).unwrap();

        let err = VectorIndex::load(dir.path(), "L", "p")
            .expect_err("a tampered generation must not be trusted");
        assert!(err.to_string().contains("CRC32C mismatch"), "got: {err}");
    }

    /// Two handles opened from the same file are two independent snapshots.
    /// The second to save must be refused, not allowed to erase the first.
    ///
    /// Derivation: handle A loads gen 1 and adds a vector → writes gen 2.
    /// Handle B still believes it is at gen 1, so its save must fail; the file
    /// must still hold A's 3 vectors, not B's 2.
    #[test]
    fn concurrent_handles_cannot_silently_overwrite_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let mut seed = VectorIndex::new(2, Metric::Cosine);
        seed.insert(1, &[1.0, 0.0]);
        seed.insert(2, &[0.0, 1.0]);
        seed.save(dir.path(), "L", "p").unwrap();

        let mut a = VectorIndex::load(dir.path(), "L", "p").unwrap().unwrap();
        let mut b = VectorIndex::load(dir.path(), "L", "p").unwrap().unwrap();

        a.insert(3, &[1.0, 1.0]);
        a.save(dir.path(), "L", "p").expect("first writer wins");

        b.insert(4, &[0.5, 0.5]);
        let err = b
            .save(dir.path(), "L", "p")
            .expect_err("the stale handle must be refused");
        assert!(
            VectorIndex::is_lost_update(&err),
            "expected a generation conflict, got: {err}"
        );

        let on_disk = VectorIndex::load(dir.path(), "L", "p").unwrap().unwrap();
        assert_eq!(
            on_disk.len(),
            3,
            "the file must still hold the first writer's 3 vectors"
        );
    }

    /// A handle that saves repeatedly keeps agreeing with its own writes.
    #[test]
    fn repeated_saves_from_one_handle_are_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = VectorIndex::new(2, Metric::Cosine);
        for i in 0u64..5 {
            idx.insert(i, &[i as f32, 1.0]);
            idx.save(dir.path(), "L", "p")
                .unwrap_or_else(|e| panic!("save {i} must succeed: {e}"));
        }
        assert_eq!(
            VectorIndex::load(dir.path(), "L", "p")
                .unwrap()
                .unwrap()
                .len(),
            5
        );
    }

    /// A fresh, empty index must not be able to replace an existing file — the
    /// "index failed to load, so create a new one" path is how an 8 MB index
    /// becomes an empty one.
    #[test]
    fn fresh_empty_index_cannot_replace_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut populated = VectorIndex::new(2, Metric::Cosine);
        for i in 0u64..9 {
            populated.insert(i, &[i as f32, 1.0]);
        }
        populated.save(dir.path(), "L", "p").unwrap();

        let empty = VectorIndex::new(2, Metric::Cosine);
        let err = empty
            .save(dir.path(), "L", "p")
            .expect_err("an unrelated fresh index must not overwrite a populated one");
        assert!(VectorIndex::is_lost_update(&err), "got: {err}");
        assert_eq!(
            VectorIndex::load(dir.path(), "L", "p")
                .unwrap()
                .unwrap()
                .len(),
            9,
            "all 9 vectors must survive"
        );
    }

    /// An index whose `id_to_slot` disagrees with `nodes` must be rejected at
    /// load, and — unlike a checksum failure — left in place for inspection.
    #[test]
    fn inconsistent_index_is_rejected_but_not_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = VectorIndex::new(2, Metric::Cosine);
        for i in 0u64..4 {
            idx.insert(i, &[i as f32, 1.0]);
        }
        // Drop one mapping: 4 nodes, 3 ids.  Such an index silently refuses
        // inserts for the orphaned id and never returns it from a search.
        idx.id_to_slot.remove(&2);
        idx.save(dir.path(), "L", "p").unwrap();

        let err = VectorIndex::load(dir.path(), "L", "p")
            .expect_err("a structurally inconsistent index must not be served");
        assert!(
            err.to_string().contains("internally inconsistent"),
            "got: {err}"
        );
        assert!(
            dir.path().join("hnsw_L_p.bin").exists(),
            "a decodable-but-inconsistent file must be left in place, not quarantined"
        );
    }

    /// Updating the sole node in an index must not corrupt the graph.
    #[test]
    fn update_single_node_index() {
        let mut idx = VectorIndex::new(2, Metric::Cosine);
        assert_eq!(idx.insert(1, &[1.0, 0.0]), InsertOutcome::Inserted);
        assert_eq!(idx.insert(1, &[0.0, 1.0]), InsertOutcome::Updated);
        assert_eq!(idx.len(), 1);
        let r = idx.search(&[0.0, 1.0], 1, 10);
        assert_eq!(r[0].0, 1);
        assert!((r[0].1 - 1.0).abs() < 1e-6);
    }

    /// Updating a node must never leave it linked to itself, and every stored
    /// slot index must stay in range.
    #[test]
    fn update_preserves_graph_invariants() {
        let mut idx = VectorIndex::new(4, Metric::Cosine);
        for i in 0u64..40 {
            idx.insert(i, &[i as f32, (i % 3) as f32, 1.0, 0.5]);
        }
        // Rewrite every third node with a different vector.
        for i in (0u64..40).step_by(3) {
            assert_eq!(
                idx.insert(i, &[100.0 - i as f32, 2.0, 0.0, 1.0]),
                InsertOutcome::Updated
            );
        }
        assert_eq!(idx.len(), 40, "updates must not add slots");
        for (slot, node) in idx.nodes.iter().enumerate() {
            for (layer, conns) in node.connections.iter().enumerate() {
                for &nb in conns {
                    assert_ne!(nb as usize, slot, "self-loop at slot {slot} layer {layer}");
                    assert!(
                        (nb as usize) < idx.nodes.len(),
                        "out-of-range neighbour {nb} at slot {slot}"
                    );
                }
            }
        }
        // The entry point must still be able to reach layer `max_layer`.
        let ep = idx.entry_point.expect("entry point");
        assert!(
            idx.nodes[ep as usize].connections.len() > idx.max_layer,
            "entry point must exist on the top layer"
        );
    }

    #[test]
    fn brute_force_search_correctness() {
        let vecs: Vec<(u64, Vec<f32>)> = (0u64..5).map(|i| (i, vec![i as f32, 0.0])).collect();
        let query = vec![3.5f32, 0.0];
        let results = VectorIndex::brute_force_search(&vecs, &query, 2, Metric::Euclidean);
        // Closest to 3.5 should be 3 and 4.
        let ids: Vec<u64> = results.iter().map(|&(id, _)| id).collect();
        assert!(ids.contains(&3));
        assert!(ids.contains(&4));
    }
}
