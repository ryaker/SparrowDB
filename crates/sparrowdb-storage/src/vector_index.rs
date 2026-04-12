//! HNSW (Hierarchical Navigable Small World) vector similarity index.
//!
//! Pure-Rust implementation — no C dependencies, no external HNSW crates.
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
use std::path::Path;

use serde::{Deserialize, Serialize};

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
        }
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    /// Save the index to `<dir>/hnsw_<label>_<prop>.bin`.
    pub fn save(&self, dir: &Path, label: &str, prop: &str) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let path = Self::index_path(dir, label, prop);
        let bytes = bincode::serialize(self).map_err(std::io::Error::other)?;
        std::fs::write(&path, bytes)?;
        Ok(())
    }

    /// Load the index from `<dir>/hnsw_<label>_<prop>.bin`.
    /// Returns `None` if the file does not exist.
    pub fn load(dir: &Path, label: &str, prop: &str) -> std::io::Result<Option<Self>> {
        let path = Self::index_path(dir, label, prop);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        let mut idx: VectorIndex = bincode::deserialize(&bytes).map_err(std::io::Error::other)?;
        // Restore derived field `ml` that was skipped during serialization.
        idx.ml = 1.0 / (idx.m as f64).ln();
        Ok(Some(idx))
    }

    /// Delete the persisted index file, if any.
    pub fn remove(dir: &Path, label: &str, prop: &str) {
        let path = Self::index_path(dir, label, prop);
        let _ = std::fs::remove_file(path);
    }

    fn index_path(dir: &Path, label: &str, prop: &str) -> std::path::PathBuf {
        // Sanitise label and prop names so they can appear in a file name.
        let safe_label = label.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        let safe_prop = prop.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        dir.join(format!("hnsw_{safe_label}_{safe_prop}.bin"))
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

    /// Insert a vector into the HNSW index.
    ///
    /// - `node_id` — application-level identifier (e.g. `NodeId.0`).
    /// - `vector`  — the embedding; must have `self.dimensions` elements.
    ///
    /// # Panics
    /// Panics if `vector.len() != self.dimensions` to prevent corrupted
    /// distance calculations across the graph.
    ///
    /// If a vector with the same `node_id` already exists it is silently ignored
    /// (upsert semantics can be added later).
    pub fn insert(&mut self, node_id: u64, vector: &[f32]) {
        assert_eq!(
            vector.len(),
            self.dimensions,
            "insert: vector dimension {} does not match index dimension {}",
            vector.len(),
            self.dimensions
        );
        if self.id_to_slot.contains_key(&node_id) {
            // Already present — skip (tombstone / update support is a future enhancement).
            return;
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
            return;
        }

        let ep = self.entry_point.unwrap();

        // Phase 1: descend from the top layer down to `new_level + 1`,
        //          finding the single closest node at each upper layer.
        let mut ep_current = ep;
        if self.max_layer > new_level {
            for l in ((new_level + 1)..=self.max_layer).rev() {
                ep_current = self.greedy_search_layer(vector, ep_current, l, l - 1);
            }
        }

        // Phase 2: for each layer from min(new_level, max_layer) down to 0,
        //          search for ef_construction neighbours and connect bi-directionally.
        let search_top = new_level.min(self.max_layer);
        for layer in (0..=search_top).rev() {
            let m_max = if layer == 0 { self.m_max0 } else { self.m };
            let ef = self.ef_construction;

            let mut candidates = self.search_layer(vector, &[ep_current], ef, layer);
            let selected = self.select_neighbours(&mut candidates, m_max);

            // Wire new_slot → selected.
            self.nodes[new_slot as usize].connections[layer] = selected.clone();

            // Wire selected → new_slot (reciprocal links), pruning if needed.
            for &nb in &selected {
                let nb_connections = self.nodes[nb as usize].connections[layer].clone();
                if !nb_connections.contains(&new_slot) {
                    if nb_connections.len() < m_max {
                        self.nodes[nb as usize].connections[layer].push(new_slot);
                    } else {
                        // Prune: keep the m_max closest neighbours.
                        let nb_vec = self.nodes[nb as usize].vector.clone();
                        let mut all: Vec<(f32, u32)> = nb_connections
                            .iter()
                            .map(|&s| (self.distance_to_slot(&nb_vec, s), s))
                            .collect();
                        all.push((self.distance_to_slot(&nb_vec, new_slot), new_slot));
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

        // Update the global entry point if the new node sits on a higher layer.
        if new_level > self.max_layer {
            self.entry_point = Some(new_slot);
            self.max_layer = new_level;
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
