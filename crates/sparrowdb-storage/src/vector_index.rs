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

/// Suffix shared by every staging file written by [`VectorIndex::save`].
///
/// The full name is `<index>.tmp.<pid>.<nonce>`; this constant is the fixed
/// part, and is also the name a pre-#452 build used on its own (with nothing
/// after it), which is why [`VectorIndex::sweep_staging_files`] matches both.
const TEMP_SUFFIX: &str = ".tmp";

/// Suffix of the advisory-lock file that serialises writers of one index file.
const LOCK_SUFFIX: &str = ".lock";

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

/// An exclusive advisory lock over one index file, held for the whole of
/// [`VectorIndex::save`].
///
/// # Why a lock and not a tighter check
///
/// `save()` reads the on-disk generation, serialises, writes a staging file and
/// renames it.  Without exclusion, two writers that both read generation `N`
/// both pass the check, both write, and the second rename silently discards the
/// first writer's vectors — issue #441's failure mode, narrowed to the width of
/// the serialise-and-write window rather than eliminated.  Re-reading the
/// generation just before the rename shrinks that window again but never closes
/// it: there is no way to make "check, then rename" a single atomic step in the
/// filesystem API.  Only mutual exclusion across the whole read-modify-write
/// removes it, and then the generation check becomes what it should have been
/// all along — the mechanism that *reports* the conflict to the loser.
///
/// # Why `flock` and not an `O_EXCL` lock file
///
/// An `O_EXCL` lock file has to answer "what if the holder died?", and every
/// answer is a heuristic: a pid liveness probe races with pid reuse, an age
/// threshold either wedges the index after a crash or breaks the lock out from
/// under a slow-but-live writer, and a broken lock is exactly the data loss the
/// lock exists to prevent.  A `flock`-style advisory lock is released by the
/// kernel when the file descriptor closes — including when the process is
/// `SIGKILL`ed — so a crashed writer leaves nothing to reclaim.
///
/// [`std::fs::File::lock`] is `flock(2)` on Unix and `LockFileEx` on Windows.
/// On Unix the lock belongs to the *open file description*, so two threads of
/// one process that each open the lock file exclude each other just as two
/// processes do; a process-wide `Mutex` would not cover the cross-process case
/// and a `flock` alone covers both.
///
/// The lock file is created next to the index and is never unlinked by `save()`
/// — unlinking a lock file is how two holders end up inside the same critical
/// section, because the next writer creates a *new* inode and locks that.
struct SaveLock(std::fs::File);

impl SaveLock {
    /// Block until this process owns the write lock for `index_path`.
    ///
    /// Blocking (rather than `try_lock`) is deliberate: a caller that is merely
    /// second in line should be made to wait and then get a truthful answer
    /// from the generation check, not a spurious "busy" error it would have to
    /// interpret.  Saves are short — one serialise and one `fsync`.
    fn acquire(index_path: &Path) -> std::io::Result<Self> {
        let path = VectorIndex::lock_path(index_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!("cannot open HNSW save lock {}: {e}", path.display()),
                )
            })?;
        file.lock().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("cannot acquire HNSW save lock {}: {e}", path.display()),
            )
        })?;
        Ok(SaveLock(file))
    }
}

impl Drop for SaveLock {
    fn drop(&mut self) {
        // Closing the descriptor releases the lock on its own; unlocking first
        // keeps the release explicit and independent of when the `File` is
        // actually dropped.
        let _ = self.0.unlock();
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

/// What reading an index file found, before any policy is applied to it.
///
/// Private on purpose: the public surface is [`VectorIndex::load`] and
/// [`VectorIndex::load_and_quarantine`], which agree on how a file is judged and
/// differ only in whether a rejected file is moved aside.  Keeping the judgement
/// in one place is what stops the two from drifting apart — before #456 there
/// was only one function and the side effect was welded to the judgement.
enum LoadOutcome {
    /// Nothing exists at the path.  The only case that is not an error.
    Absent,
    /// A well-formed index, fully restored.  Boxed to keep the enum small.
    Loaded(Box<VectorIndex>),
    /// The directory entry exists but its bytes are not reachable — a dangling
    /// symlink.  Never quarantined: there are no bytes to preserve.
    Unreadable(String),
    /// Bytes are present and do not decode.  Quarantine-eligible: these are the
    /// only surviving copy of the vectors.
    Undecodable(String),
    /// The bytes decoded but the graph is internally inconsistent.  Never
    /// quarantined: a hand repair is plausible and `save()`'s generation check
    /// already stops an empty index from replacing it.
    Inconsistent(String),
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
    /// 2. Write `header || payload` to `<path>.tmp.<pid>.<nonce>` and `fsync`
    ///    it, so the bytes are on stable storage before anything else changes.
    /// 3. `rename(<staging>, <path>)` — atomic within a filesystem, so a
    ///    reader only ever sees either the complete old file or the complete
    ///    new one, never a partial one.
    /// 4. `fsync` the containing directory so the rename itself survives a
    ///    power loss (a renamed file whose directory entry was never flushed
    ///    can revert after a crash).
    ///
    /// If any step fails, `<path>` still holds the previous index and the
    /// partial staging file is removed.
    ///
    /// The staging name carries a pid and a per-process nonce.  A fixed
    /// `<path>.tmp` — what this function used between #442 and #452 — is a
    /// shared mutable file: two writers both `File::create` it, the first to
    /// rename vacates it, and the loser's rename fails with `ENOENT` and drops
    /// its vectors on the floor.  Worse, `ENOENT` is indistinguishable from a
    /// real disk fault, so a caller following the lost-update contract below
    /// would not retry.  See issue #452.
    ///
    /// # Exclusion
    ///
    /// The whole read-generation → serialise → write → rename sequence runs
    /// under an exclusive advisory lock on `<path>.lock` (see `SaveLock`).
    /// Without it the generation check is a time-of-check/time-of-use race:
    /// two writers that both read generation `N` both pass it.  With it, the
    /// second writer blocks, then observes generation `N + 1` and is refused —
    /// so a concurrent conflict surfaces as the *same* lost-update error a
    /// sequential one does, and [`Self::is_lost_update`] recognises both.
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

        // Held until this function returns.  Everything below — the generation
        // read, the staging write and the rename — is one critical section; see
        // `SaveLock` for why nothing narrower is sufficient.
        let _lock = SaveLock::acquire(&path)?;

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

        // Debris from a save that was killed between `File::create` and
        // `rename`.  Safe to delete unconditionally: the lock is held, so no
        // other writer can own a staging file for this index right now.
        Self::sweep_staging_files(dir, &path);
        let tmp = Self::temp_path(&path);

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
    ///
    /// This is the whole recovery contract: `true` means *reload the index and
    /// retry*, `false` means the storage itself is unhappy and retrying will
    /// not help.  It must therefore cover **every** way `save()` can refuse
    /// because someone else got there first — including the concurrent case.
    /// Between #442 and #452 it did not: two overlapping saves collided on a
    /// shared staging path and the loser got `ENOENT`, which this returns
    /// `false` for, so a correct caller read data loss as a disk fault and did
    /// not retry.  `save()` now serialises writers, so a concurrent conflict
    /// produces the same generation-conflict error a sequential one does.
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

    /// Load the index from `<dir>/hnsw_<label>_<prop>.bin` **without modifying
    /// anything on disk**.
    ///
    /// Returns `Ok(None)` only when nothing exists at that path.  A file that is
    /// present but unusable produces `Err`, and is left exactly where it was
    /// found.
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
    /// # Why this is not the quarantining variant (#456)
    ///
    /// #442 put the quarantine rename *inside* `load`, which made every reader
    /// destructive by inheritance — including the `hybrid_search` query path and
    /// the read-only `vector_index_load_failures` diagnostic.  A read query
    /// renamed the last live copy of an index aside and reported zero hits.
    ///
    /// Quarantine is now opt-in: callers that are about to take ownership of the
    /// `(label, prop)` slot — i.e. the open path, which is the only place a
    /// later `save()` could overwrite the damaged bytes — call
    /// [`VectorIndex::load_and_quarantine`] and say so.  Every other caller uses
    /// this method and gets the error without the side effect.
    pub fn load(dir: &Path, label: &str, prop: &str) -> std::io::Result<Option<Self>> {
        let path = Self::index_path(dir, label, prop);
        match Self::read_index(&path)? {
            LoadOutcome::Absent => Ok(None),
            LoadOutcome::Loaded(idx) => Ok(Some(*idx)),
            LoadOutcome::Undecodable(reason) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "HNSW index {} is corrupt ({reason}); the file has been left in place \
                     for inspection.",
                    path.display()
                ),
            )),
            LoadOutcome::Inconsistent(reason) => Err(Self::inconsistent_error(&path, &reason)),
            LoadOutcome::Unreadable(reason) => Err(Self::unreadable_error(&path, &reason)),
        }
    }

    /// Load the index, **quarantining** bytes that fail to decode: the file is
    /// renamed to `<path>.corrupt.<unix_millis>` before the error is returned.
    ///
    /// Quarantine exists because the damaged bytes are the only surviving copy
    /// of the vectors — an HNSW index is not derived state and cannot be rebuilt
    /// from column data — and a caller that reads a load failure as "no index
    /// here" would otherwise let the next `save()` replace them with an empty
    /// index.
    ///
    /// This is therefore for the **open path only**.  Readers and diagnostics
    /// must use [`VectorIndex::load`]; see #456 for what happens when they do
    /// not.
    ///
    /// Only *undecodable* bytes are moved.  A file that decodes cleanly and then
    /// fails the structural check is left in place: it may well be repairable by
    /// hand, and the generation check in `save()` already stops an empty index
    /// from replacing it.  A directory entry whose contents cannot be read at
    /// all (a dangling symlink) is also left alone — there are no bytes to
    /// preserve, so renaming it would destroy evidence and protect nothing.
    pub fn load_and_quarantine(
        dir: &Path,
        label: &str,
        prop: &str,
    ) -> std::io::Result<Option<Self>> {
        let path = Self::index_path(dir, label, prop);
        match Self::read_index(&path)? {
            LoadOutcome::Absent => Ok(None),
            LoadOutcome::Loaded(idx) => Ok(Some(*idx)),
            LoadOutcome::Undecodable(reason) => {
                let quarantined = Self::quarantine(&path);
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "HNSW index {} is corrupt ({reason}); the damaged file was preserved as {}",
                        path.display(),
                        quarantined
                            .as_deref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "<quarantine failed>".to_owned()),
                    ),
                ))
            }
            LoadOutcome::Inconsistent(reason) => Err(Self::inconsistent_error(&path, &reason)),
            LoadOutcome::Unreadable(reason) => Err(Self::unreadable_error(&path, &reason)),
        }
    }

    fn inconsistent_error(path: &Path, reason: &str) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "HNSW index {} decoded but is internally inconsistent ({reason}); \
                 refusing to serve it. The file has been left in place for inspection.",
                path.display()
            ),
        )
    }

    fn unreadable_error(path: &Path, reason: &str) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "HNSW index {} is present but its bytes could not be read ({reason}); \
                 refusing to treat it as absent. The entry has been left in place \
                 for inspection.",
                path.display()
            ),
        )
    }

    /// Read, decode and validate the file at `path`, touching nothing.
    ///
    /// The single place that knows how to turn bytes into an index; `load` and
    /// `load_and_quarantine` differ only in what they do with the rejecting
    /// outcomes.
    fn read_index(path: &Path) -> std::io::Result<LoadOutcome> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // `read` follows symlinks, so a symlink whose target is gone
                // reports NotFound even though the directory entry is sitting
                // right there.  `symlink_metadata` does not follow, so it still
                // sees it.  Reporting that as "absent" is exactly the
                // present-but-treated-as-absent failure #445 exists to kill
                // (#456, third sub-finding): the pair silently becomes "no
                // index configured" and every write for it is dropped.
                return Ok(match std::fs::symlink_metadata(path) {
                    Ok(_) => LoadOutcome::Unreadable(
                        "the directory entry exists but resolving it failed — most likely a \
                         symbolic link whose target has been removed"
                            .to_owned(),
                    ),
                    Err(_) => LoadOutcome::Absent,
                });
            }
            Err(e) => return Err(e),
        };

        let decoded = if bytes.len() >= HNSW_HEADER_LEN && &bytes[..8] == HNSW_MAGIC {
            Self::decode_v2(&bytes)
        } else {
            Self::decode_legacy(&bytes).map(|idx| (idx, 0u64))
        };

        let (mut idx, generation) = match decoded {
            Ok(v) => v,
            Err(reason) => return Ok(LoadOutcome::Undecodable(reason)),
        };

        if let Err(reason) = idx.validate_invariants() {
            return Ok(LoadOutcome::Inconsistent(reason));
        }

        // Restore derived field `ml` that was skipped during serialization.
        idx.ml = 1.0 / (idx.m as f64).ln();
        idx.disk_generation.set(generation);
        Ok(LoadOutcome::Loaded(Box::new(idx)))
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
    ///
    /// Reached only from [`VectorIndex::load_and_quarantine`] (#456).
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

    /// Delete the persisted index file, its staging files and its lock file.
    ///
    /// This is a teardown: it unlinks the lock file, so it must not run
    /// concurrently with a `save()` of the same index (a new lock file would be
    /// a different inode, and two writers could then both hold "the" lock).
    /// Dropping an index while something is still writing to it is undefined
    /// regardless.
    pub fn remove(dir: &Path, label: &str, prop: &str) {
        let path = Self::index_path(dir, label, prop);
        Self::sweep_staging_files(dir, &path);
        let _ = std::fs::remove_file(Self::lock_path(&path));
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

    /// Staging path used by `save()`: `<path>.tmp.<pid>.<nonce>`.
    ///
    /// Appending (rather than replacing) the extension keeps the staging file
    /// next to the real one so `rename` stays within the same filesystem, where
    /// it is atomic.
    ///
    /// The pid and nonce make the name unique to one in-flight save.  `save()`
    /// already holds an exclusive lock, so under normal operation no two writers
    /// are staging at once; uniqueness is what keeps the failure *contained*
    /// when that assumption does not hold — an older build that predates the
    /// lock, or a filesystem whose advisory locks are a no-op.  In those cases
    /// the writers still collide on the destination, but each one's bytes reach
    /// `rename` intact instead of one of them being deleted out from under the
    /// other and reported as `ENOENT`.  See issue #452.
    fn temp_path(path: &Path) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        let mut s = path.to_path_buf().into_os_string();
        s.push(format!("{TEMP_SUFFIX}.{}.{nonce}", std::process::id()));
        PathBuf::from(s)
    }

    /// Path of the advisory lock that serialises writers of `path`.
    fn lock_path(path: &Path) -> PathBuf {
        let mut s = path.to_path_buf().into_os_string();
        s.push(LOCK_SUFFIX);
        PathBuf::from(s)
    }

    /// Delete every staging file belonging to `index_path`.
    ///
    /// Per-pid staging names mean there is no single name to look for, so this
    /// matches the whole family: the pre-#452 fixed `<index>.tmp` as well as
    /// `<index>.tmp.<pid>.<nonce>`.  A crashed process leaves its staging file
    /// behind for good — nothing else ever removes it — and on a large index
    /// each one is megabytes.
    ///
    /// Callers must hold the save lock (or be tearing the index down), because
    /// a staging file that belongs to a *live* save is exactly the file whose
    /// deletion causes the `ENOENT` data loss this sweep is cleaning up after.
    fn sweep_staging_files(dir: &Path, index_path: &Path) {
        let Some(index_name) = index_path.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        let prefix = format!("{index_name}{TEMP_SUFFIX}");
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            // `""` is the legacy fixed name; `".<pid>.<nonce>"` is the current
            // one.  Anything else that merely starts with the same characters
            // (say `hnsw_L_p.bin.tmpfoo`) is left alone.
            match name.strip_prefix(&prefix) {
                Some(rest) if rest.is_empty() || rest.starts_with('.') => {
                    let _ = std::fs::remove_file(entry.path());
                }
                _ => {}
            }
        }
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
            //
            // `linked_back` records whether *any* neighbour ended up pointing at
            // `slot`.  Every branch below can legitimately decline: a full
            // neighbour keeps its m_max closest links, and `slot` is simply not
            // one of them.  Losing every contest leaves `slot` a pure sink —
            // full outgoing degree, zero incoming — and greedy descent only ever
            // follows outgoing edges, so nothing can reach it.  See issue #443.
            //
            // `adopted` carries the nodes this call has re-homed onto `slot`
            // (see [`Self::link_back`]).  They are by construction far from
            // `slot` — each was somebody else's *furthest* neighbour — so
            // without this set the next adoption evicts the previous one and
            // strands the very node it was protecting.  That is the sequence
            // that survived the first draft of this fix: inserting node 368
            // removed the last inbound edge of node 194.
            let mut adopted: HashSet<u32> = HashSet::new();
            let mut linked_back = false;
            for &nb in &selected {
                linked_back |= self.link_back(nb, slot, layer, m_max, false, &mut adopted);
            }

            // Reciprocal-link guarantee (#443).  `selected` is sorted
            // nearest-first by `select_neighbours`, so `selected[0]` is the best
            // available anchor to force the edge through.
            if !linked_back {
                if let Some(&nb0) = selected.first() {
                    self.link_back(nb0, slot, layer, m_max, true, &mut adopted);
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

    /// Add the edge `nb → slot` at `layer`, evicting from `nb`'s adjacency list
    /// if it is already at `m_max`.  Returns `true` when the edge exists
    /// afterwards.
    ///
    /// `force = false` keeps the original quality rule: `slot` is admitted only
    /// if it is closer to `nb` than `nb`'s current furthest neighbour, exactly
    /// as "keep the `m_max` closest of `conns ∪ {slot}`" did.  `force = true`
    /// admits it regardless — used for the last-resort reciprocal guarantee.
    ///
    /// # Why no eviction here can strand a node
    ///
    /// Dropping the edge `nb → x` is the step that silently destroys
    /// reachability: if that was `x`'s only inbound edge, wiring one node in has
    /// orphaned another, and nothing reports it.  Issue #443 is the accumulated
    /// result — both the ~1–5% of inserts that end with zero incoming edges and
    /// the closed islands of nodes that point at each other with no path back.
    ///
    /// The invariant restored here: **`slot` must point at whatever `slot`
    /// displaced.**  Then every path that used `nb → x` reroutes as
    /// `nb → slot → x`, and `slot` is reachable because `nb` is, so the set of
    /// reachable nodes never shrinks.  Two cases:
    ///
    /// 1. `slot → x` already exists (the common case — `nb` is one of `slot`'s
    ///    chosen neighbours, so they share neighbours). Nothing else to do.
    /// 2. Otherwise `slot` adopts `x`.  If `slot`'s own list is full this
    ///    displaces `slot`'s furthest edge `y`; `slot`'s links at this layer
    ///    were all (re)assigned moments ago by [`Self::link_slot`], so dropping
    ///    one cannot remove any inbound edge `y` had before this call.
    ///
    /// Cost: one extra adjacency scan on the eviction path.
    fn link_back(
        &mut self,
        nb: u32,
        slot: u32,
        layer: usize,
        m_max: usize,
        force: bool,
        adopted: &mut HashSet<u32>,
    ) -> bool {
        if nb == slot {
            return false;
        }
        let conns = self.nodes[nb as usize].connections[layer].clone();
        if conns.contains(&slot) {
            return true;
        }
        if conns.len() < m_max {
            self.nodes[nb as usize].connections[layer].push(slot);
            return true;
        }

        // `nb`'s list is full: `slot` takes the furthest occupant's place, but
        // only if it beats it (unless forced).
        let nb_vec = self.nodes[nb as usize].vector.clone();
        let d_slot = self.distance_to_slot(&nb_vec, slot);
        let mut furthest: Option<(f32, usize)> = None;
        for (i, &x) in conns.iter().enumerate() {
            let d = self.distance_to_slot(&nb_vec, x);
            if furthest.is_none_or(|(bd, _)| d > bd) {
                furthest = Some((d, i));
            }
        }
        let Some((d_worst, pos)) = furthest else {
            return false;
        };
        if !force && d_slot >= d_worst {
            return false; // `slot` lost the contest fairly; nothing changes.
        }

        // The eviction is only safe if `slot` can take `displaced` on.  When it
        // cannot, decline rather than strand it — unless forced, where leaving
        // `slot` itself unreachable is the worse outcome and [`Self::repair`] is
        // the backstop.
        // Most evictions are harmless: `displaced` keeps other inbound edges and
        // needs no help.  Only pay the adoption — which spends one of `slot`'s
        // own good links on a far node — when the edge being removed is the last
        // one into `displaced`.
        let displaced = conns[pos];
        let needs_adoption = !self.has_other_inbound(displaced, nb, layer);
        if needs_adoption && !force && !self.can_adopt(slot, displaced, layer, m_max, adopted) {
            return false;
        }
        self.nodes[nb as usize].connections[layer][pos] = slot;
        if needs_adoption {
            self.adopt(slot, displaced, layer, m_max, adopted);
        }
        true
    }

    /// Whether `displaced` still has an inbound edge at `layer` from some node
    /// other than `exclude`.
    ///
    /// Only `displaced`'s own out-neighbours are checked for a link back.  That
    /// is a *sufficient* test, not an exhaustive one — an inbound edge from a
    /// node `displaced` does not point at is missed and costs an unnecessary
    /// adoption, never a stranded node.  Because the reciprocal guarantee makes
    /// links overwhelmingly bidirectional, the first probe almost always hits,
    /// so this is O(1) in practice and O(m_max²) at worst.
    ///
    /// Soundness relies on the induction this whole scheme maintains: every node
    /// in the index is reachable, so *any* surviving inbound edge is an inbound
    /// edge from a reachable node.
    fn has_other_inbound(&self, displaced: u32, exclude: u32, layer: usize) -> bool {
        for &z in &self.nodes[displaced as usize].connections[layer] {
            if z != exclude
                && z != displaced
                && self.nodes[z as usize]
                    .connections
                    .get(layer)
                    .is_some_and(|c| c.contains(&displaced))
            {
                return true;
            }
        }
        false
    }

    /// Whether [`Self::adopt`] can take `displaced` on without evicting a node
    /// this call already adopted (and therefore may be the last link to).
    fn can_adopt(
        &self,
        slot: u32,
        displaced: u32,
        layer: usize,
        m_max: usize,
        adopted: &HashSet<u32>,
    ) -> bool {
        if slot == displaced {
            return true;
        }
        let conns = &self.nodes[slot as usize].connections[layer];
        conns.contains(&displaced)
            || conns.len() < m_max
            || conns.iter().any(|y| !adopted.contains(y))
    }

    /// Ensure `slot → displaced` exists at `layer`, so a node that just lost an
    /// inbound edge to `slot` keeps a path in.  See [`Self::link_back`].
    fn adopt(
        &mut self,
        slot: u32,
        displaced: u32,
        layer: usize,
        m_max: usize,
        adopted: &mut HashSet<u32>,
    ) {
        if slot == displaced {
            return;
        }
        let conns = &self.nodes[slot as usize].connections[layer];
        if conns.contains(&displaced) {
            adopted.insert(displaced);
            return;
        }
        if conns.len() < m_max {
            self.nodes[slot as usize].connections[layer].push(displaced);
            adopted.insert(displaced);
            return;
        }
        // Evict `slot`'s furthest link, skipping anything adopted earlier in
        // this call: those are precisely the nodes with no other inbound edge.
        let slot_vec = self.nodes[slot as usize].vector.clone();
        let worst = self.nodes[slot as usize].connections[layer]
            .iter()
            .enumerate()
            .filter(|(_, y)| !adopted.contains(y))
            .max_by(|a, b| {
                let da = self.distance_to_slot(&slot_vec, *a.1);
                let db = self.distance_to_slot(&slot_vec, *b.1);
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, _)| i);
        if let Some(i) = worst {
            self.nodes[slot as usize].connections[layer][i] = displaced;
            adopted.insert(displaced);
        }
    }

    // ── Reachability ──────────────────────────────────────────────────────────

    /// Mark every slot reachable from `entry_point` by following layer-0
    /// outgoing edges.
    ///
    /// This is the set `search()` can actually return.  Greedy descent through
    /// the upper layers only ever lands on nodes reachable from the entry point,
    /// and every node exists at layer 0, so layer-0 reachability from the entry
    /// point is the necessary condition for a vector to be findable at all.
    ///
    /// O(nodes + edges), no distance arithmetic.
    fn reachable_slots(&self) -> Vec<bool> {
        let mut seen = vec![false; self.nodes.len()];
        let Some(ep) = self.entry_point else {
            return seen;
        };
        if (ep as usize) >= self.nodes.len() {
            return seen;
        }
        seen[ep as usize] = true;
        let mut stack = vec![ep];
        while let Some(s) = stack.pop() {
            for &nb in &self.nodes[s as usize].connections[0] {
                if !seen[nb as usize] {
                    seen[nb as usize] = true;
                    stack.push(nb);
                }
            }
        }
        seen
    }

    /// `true` when `node_id` has a vector stored in this index.
    ///
    /// Answers the question `search()` cannot: "not returned by a search" and
    /// "absent from the index" are different facts, and before this existed
    /// callers had no way to tell them apart — which is how issue #443 stayed
    /// invisible.  O(1).
    pub fn has_vector(&self, node_id: u64) -> bool {
        self.id_to_slot.contains_key(&node_id)
    }

    /// Number of stored vectors that greedy search can actually reach.
    ///
    /// Compare against [`Self::len`]: a healthy index has
    /// `reachable_count() == len()`.  Any gap is silent recall loss.
    pub fn reachable_count(&self) -> usize {
        self.reachable_slots().iter().filter(|&&r| r).count()
    }

    /// The `node_id`s that are stored but unreachable — the ones
    /// [`Self::has_vector`] reports as present and `search` will never return.
    pub fn unreachable_ids(&self) -> Vec<u64> {
        let seen = self.reachable_slots();
        let mut ids: Vec<u64> = self
            .nodes
            .iter()
            .zip(seen.iter())
            .filter(|(_, &r)| !r)
            .map(|(n, _)| n.node_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Reconnect every stored-but-unreachable vector, returning how many nodes
    /// were re-linked.
    ///
    /// Needed independently of the insert-time guarantee for two reasons: an
    /// index written by an older build carries its orphans in the file, and a
    /// group of former sinks can become each other's chosen neighbours and form
    /// a closed island — non-zero in-degree, still no path from `entry_point`.
    /// An in-degree check calls those healthy; only a traversal finds them.
    ///
    /// Each unreachable node is attached to its nearest *reachable* neighbour
    /// via [`Self::link_back`], which never strands the edge it
    /// displaces.  A repaired node makes everything hanging off it reachable
    /// too, so the loop re-walks until it reaches a fixpoint.
    ///
    /// Returns 0 for a healthy index after one O(nodes + edges) walk.
    pub fn repair(&mut self) -> usize {
        let mut repaired = 0usize;
        // Each pass strictly grows the reachable set, so this terminates well
        // inside the bound; the bound only guards against a pathological index
        // where `reconnect_slot` cannot find a candidate.
        for _ in 0..8 {
            let seen = self.reachable_slots();
            let todo: Vec<u32> = (0..self.nodes.len() as u32)
                .filter(|&s| !seen[s as usize])
                .collect();
            if todo.is_empty() {
                break;
            }
            let before = repaired;
            for u in todo {
                if self.reconnect_slot(u) {
                    repaired += 1;
                }
            }
            if repaired == before {
                break;
            }
        }
        repaired
    }

    /// Attach `u` to its nearest reachable neighbour at layer 0.
    ///
    /// `u`'s own outgoing edges are deliberately left alone: they may be the
    /// only inbound edges of other unreachable nodes in the same island, and
    /// re-linking `u` makes that whole island reachable for free.
    fn reconnect_slot(&mut self, u: u32) -> bool {
        let Some(ep) = self.entry_point else {
            return false;
        };
        if ep == u || (u as usize) >= self.nodes.len() {
            return false;
        }
        let vector = self.nodes[u as usize].vector.clone();
        let mut ep_current = ep;
        for l in (1..=self.max_layer).rev() {
            ep_current = self.greedy_search_layer(&vector, ep_current, l, l - 1);
        }
        let mut candidates = self.search_layer(&vector, &[ep_current], self.ef_construction, 0);
        // `search_layer` walks outgoing edges from a reachable entry point, so
        // it can only return reachable slots; `u` is unreachable by definition.
        candidates.retain(|&(_, s)| s != u);
        let Some(&(_, nb0)) = candidates
            .iter()
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        else {
            return false;
        };
        self.link_back(nb0, u, 0, self.m_max0, true, &mut HashSet::new());
        true
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

    // ── Issue #443: reachability invariants ───────────────────────────────────
    //
    // These reach into private fields on purpose: in-degree and adjacency-list
    // capacity are exactly the things `search()` cannot show you, and not being
    // able to see them is why #443 survived for months.

    /// xorshift64 + Box–Muller.  Deterministic, dependency-free, and identical
    /// on every platform, so these fixtures are reproducible.
    struct TestRng(u64);
    impl TestRng {
        fn new(seed: u64) -> Self {
            TestRng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn unit(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
        fn normal(&mut self) -> f64 {
            let u1 = self.unit().max(1e-12);
            let u2 = self.unit();
            (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
        }
    }

    fn l2_normalise(v: &mut [f32]) {
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n > 0.0 {
            for x in v.iter_mut() {
                *x /= n;
            }
        }
    }

    fn random_direction(rng: &mut TestRng, dims: usize) -> Vec<f32> {
        let mut v: Vec<f32> = (0..dims).map(|_| rng.normal() as f32).collect();
        l2_normalise(&mut v);
        v
    }

    /// A corpus with the geometry that actually triggers #443.
    ///
    /// Every vector is `aniso * mu + centre + noise`, L2-normalised.  The shared
    /// `mu` term is what real sentence embeddings look like — they are not
    /// spread over the sphere, they sit in a narrow cone, so *all* pairwise
    /// distances are compressed and a new node inserted into a mature
    /// neighbourhood struggles to beat the occupants of its neighbours' lists.
    /// Isotropic random vectors (the shape most HNSW tests use) never reproduce
    /// this, which is the other reason the defect stayed hidden.
    fn cone_corpus(n: usize, dims: usize, clusters: usize, aniso: f32, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = TestRng::new(seed);
        let mu = random_direction(&mut rng, dims);
        let centres: Vec<Vec<f32>> = (0..clusters)
            .map(|_| random_direction(&mut rng, dims))
            .collect();
        (0..n)
            .map(|i| {
                // Cluster-by-cluster arrival, i.e. topical batches over time,
                // not a shuffled stream.
                let c = &centres[(i * clusters) / n];
                let mut v: Vec<f32> = (0..dims)
                    .map(|d| aniso * mu[d] + c[d] + (rng.normal() * 0.35) as f32)
                    .collect();
                l2_normalise(&mut v);
                v
            })
            .collect()
    }

    /// Layer-0 in-degree of every slot.
    fn layer0_in_degree(idx: &VectorIndex) -> Vec<usize> {
        let mut deg = vec![0usize; idx.nodes.len()];
        for node in &idx.nodes {
            for &nb in &node.connections[0] {
                deg[nb as usize] += 1;
            }
        }
        deg
    }

    /// The core invariant of #443: a vector that is stored must be a vector that
    /// greedy traversal can reach.
    ///
    /// Hand-derived expectations:
    /// - 700 inserts of the distinct ids `0..700` each take the "new id" path,
    ///   so `len() == 700`.
    /// - `reachable_count()` counts slots reachable from `entry_point` over
    ///   layer-0 edges. It can never exceed `len()`. A correct index has them
    ///   equal, because a node no path leads to can never be returned by
    ///   `search`, whatever `k` and `ef` are set to.
    /// - Therefore `unreachable_ids()` must be empty. Not "small" — empty. The
    ///   pre-fix code produces a non-empty list on this fixture.
    #[test]
    fn spa443_every_stored_vector_is_reachable() {
        const N: usize = 700;
        const DIMS: usize = 48;
        let vectors = cone_corpus(N, DIMS, 9, 2.0, 0x5EED);

        let mut idx = VectorIndex::new(DIMS, Metric::Cosine);
        for (i, v) in vectors.iter().enumerate() {
            assert_eq!(
                idx.insert(i as u64, v),
                InsertOutcome::Inserted,
                "id {i} is distinct, so it must take the insert path"
            );
        }
        assert_eq!(idx.len(), N, "700 distinct ids were inserted");

        let stranded = idx.unreachable_ids();
        assert!(
            stranded.is_empty(),
            "{} of {N} stored vectors are unreachable from the entry point and can \
             never be returned by search: {:?}",
            stranded.len(),
            &stranded[..stranded.len().min(20)]
        );
        assert_eq!(
            idx.reachable_count(),
            N,
            "every stored vector must be reachable"
        );
    }

    /// Every node must keep at least one inbound edge at layer 0.
    ///
    /// Zero in-degree is the mechanism behind #443: `insert` wires
    /// `new -> neighbours` unconditionally but `neighbours -> new` only when the
    /// new node beats an existing occupant on distance, so a node in a saturated
    /// neighbourhood can end up a pure sink — full outgoing degree, nothing
    /// pointing at it, invisible to a traversal that only follows outgoing
    /// edges.
    ///
    /// Hand-derived: with N >= 2, every slot other than the entry point must be
    /// pointed at by something, and the entry point too once the graph has more
    /// than one node, since `link_slot` always establishes an edge in both
    /// directions for at least one neighbour. So the minimum in-degree over all
    /// 700 slots is >= 1.
    #[test]
    fn spa443_no_node_has_zero_in_degree() {
        const N: usize = 700;
        const DIMS: usize = 48;
        let vectors = cone_corpus(N, DIMS, 9, 2.0, 0x5EED);
        let mut idx = VectorIndex::new(DIMS, Metric::Cosine);
        for (i, v) in vectors.iter().enumerate() {
            idx.insert(i as u64, v);
        }

        let deg = layer0_in_degree(&idx);
        let sinks: Vec<usize> = (0..idx.len()).filter(|&s| deg[s] == 0).collect();
        assert!(
            sinks.is_empty(),
            "{} slots have zero layer-0 in-degree (pure sinks): {:?}",
            sinks.len(),
            &sinks[..sinks.len().min(20)]
        );
    }

    /// The reciprocal-link guarantee must not be bought by letting adjacency
    /// lists grow past their cap — an unbounded list would trade a correctness
    /// bug for a memory and latency one.
    ///
    /// Hand-derived from the constructor: `VectorIndex::new` calls
    /// `with_params(.., m = 16, ..)` and `with_params` sets `m_max0 = m * 2`.
    /// So layer 0 admits at most 32 links per node and every layer above it at
    /// most 16.
    #[test]
    fn spa443_adjacency_lists_stay_within_capacity() {
        const N: usize = 700;
        const DIMS: usize = 48;
        let vectors = cone_corpus(N, DIMS, 9, 2.0, 0x5EED);
        let mut idx = VectorIndex::new(DIMS, Metric::Cosine);
        for (i, v) in vectors.iter().enumerate() {
            idx.insert(i as u64, v);
        }
        assert_eq!(idx.m, 16, "default M");
        assert_eq!(idx.m_max0, 32, "m_max0 = 2 * M");

        for (slot, node) in idx.nodes.iter().enumerate() {
            for (layer, conns) in node.connections.iter().enumerate() {
                let cap = if layer == 0 { idx.m_max0 } else { idx.m };
                assert!(
                    conns.len() <= cap,
                    "slot {slot} layer {layer} holds {} links, cap is {cap}",
                    conns.len()
                );
                // A self-link would make the node trivially "reachable" from
                // itself while still being unreachable from the entry point.
                assert!(
                    !conns.contains(&(slot as u32)),
                    "slot {slot} links to itself at layer {layer}"
                );
            }
        }
    }

    /// `repair()` must reconnect a genuinely marooned node, including one that
    /// still has inbound edges.
    ///
    /// This is the case an in-degree check reports as healthy and misses: a
    /// group of former sinks that became each other's chosen neighbours forms a
    /// closed island — non-zero in-degree, no path from `entry_point`. The live
    /// store had 7 such nodes alongside 33 pure sinks.
    ///
    /// The fixture builds a healthy 200-node index, then severs every inbound
    /// edge of slots 150 and 151 while leaving the edge between them in place.
    /// Hand-derived expectations:
    /// - before severing: `reachable_count() == 200`.
    /// - after severing: slots 150 and 151 point at each other but nothing else
    ///   points at either, so no path from the entry point reaches them —
    ///   `reachable_count() == 198`, and in-degree is 1 for both, i.e. non-zero.
    /// - after `repair()`: `reachable_count() == 200` again and the return value
    ///   is the number of nodes it had to re-link, which is at least 1 (linking
    ///   one member of the island makes the other reachable through it).
    #[test]
    fn spa443_repair_reconnects_a_marooned_island() {
        const N: usize = 200;
        const DIMS: usize = 32;
        let vectors = cone_corpus(N, DIMS, 6, 1.5, 0xBEEF);
        let mut idx = VectorIndex::new(DIMS, Metric::Cosine);
        for (i, v) in vectors.iter().enumerate() {
            idx.insert(i as u64, v);
        }
        assert_eq!(idx.reachable_count(), N, "fixture must start healthy");

        let (a, b) = (150u32, 151u32);
        assert_ne!(idx.entry_point, Some(a));
        assert_ne!(idx.entry_point, Some(b));
        // Sever every inbound edge of `a` and `b` at every layer.
        for slot in 0..idx.nodes.len() {
            if slot as u32 == a || slot as u32 == b {
                continue;
            }
            for conns in idx.nodes[slot].connections.iter_mut() {
                conns.retain(|&x| x != a && x != b);
            }
        }
        // Leave exactly the island: a -> b and b -> a at layer 0.
        idx.nodes[a as usize].connections[0] = vec![b];
        idx.nodes[b as usize].connections[0] = vec![a];

        let deg = layer0_in_degree(&idx);
        assert_eq!(
            deg[a as usize], 1,
            "island member keeps a non-zero in-degree"
        );
        assert_eq!(
            deg[b as usize], 1,
            "island member keeps a non-zero in-degree"
        );
        assert_eq!(
            idx.reachable_count(),
            N - 2,
            "the two island members must now be unreachable despite in-degree 1"
        );

        let repaired = idx.repair();
        assert!(
            repaired >= 1,
            "repair must re-link at least one island member, re-linked {repaired}"
        );
        assert_eq!(
            idx.reachable_count(),
            N,
            "repair must restore full reachability"
        );
        assert!(idx.unreachable_ids().is_empty());
    }

    /// `repair()` on a healthy index changes nothing and reports nothing.
    /// Hand-derived: a freshly built index satisfies the invariant, so the first
    /// walk finds no unreachable slots and the function returns 0 without
    /// touching an edge.
    #[test]
    fn spa443_repair_is_a_noop_on_a_healthy_index() {
        const N: usize = 300;
        const DIMS: usize = 32;
        let vectors = cone_corpus(N, DIMS, 6, 1.5, 0xF00D);
        let mut idx = VectorIndex::new(DIMS, Metric::Cosine);
        for (i, v) in vectors.iter().enumerate() {
            idx.insert(i as u64, v);
        }
        let before: Vec<Vec<Vec<u32>>> = idx.nodes.iter().map(|n| n.connections.clone()).collect();
        assert_eq!(idx.repair(), 0, "healthy index needs no repair");
        let after: Vec<Vec<Vec<u32>>> = idx.nodes.iter().map(|n| n.connections.clone()).collect();
        assert_eq!(before, after, "repair must not rewire a healthy index");
    }

    /// `has_vector` must answer from `id_to_slot`, independently of whether a
    /// search can find the node.
    ///
    /// Hand-derived: ids 0..50 are inserted, so `has_vector` is true for each of
    /// them and false for 50..60, which were never inserted.
    #[test]
    fn spa443_has_vector_reports_storage_not_findability() {
        let mut idx = VectorIndex::new(8, Metric::Cosine);
        let mut rng = TestRng::new(1);
        for i in 0u64..50 {
            let v = random_direction(&mut rng, 8);
            idx.insert(i, &v);
        }
        for i in 0u64..50 {
            assert!(idx.has_vector(i), "id {i} was inserted");
        }
        for i in 50u64..60 {
            assert!(!idx.has_vector(i), "id {i} was never inserted");
        }
        assert!(!idx.has_vector(u64::MAX));
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
