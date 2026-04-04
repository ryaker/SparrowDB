//! BM25 inverted full-text index for SparrowDB (issue #395).
//!
//! ## Index layout
//!
//! Each index covers one `(label, property)` pair and is stored under
//! `{db_root}/fts/{label}__{property}.bin` as a `bincode`-serialised
//! `FtsIndexData` struct.
//!
//! ## Tokeniser
//! - Lowercase all text.
//! - Split on any character that is **not** `[a-z0-9]` (i.e. whitespace and
//!   all punctuation act as delimiters).
//! - Discard tokens shorter than 2 characters.
//! - No stemming in v1.
//!
//! ## BM25 formula
//! ```text
//! score(d, q) = Σ IDF(qi) * TF(qi,d) * (k1+1)
//!                           ─────────────────────────────
//!                           TF(qi,d) + k1*(1 − b + b*|d|/avgdl)
//!
//! IDF(qi) = ln( (N − df(qi) + 0.5) / (df(qi) + 0.5) + 1 )
//! ```
//! Default parameters: `k1 = 1.2`, `b = 0.75`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, Result};

// ── On-disk data ─────────────────────────────────────────────────────���────────

/// Serialisable payload that is written to / read from disk.
///
/// Uses a simple binary format (length-prefixed lists) to avoid a `bincode`
/// dependency in the storage crate — only `sparrowdb-common` is available here.
/// The format is not encrypted (derived from node properties which are also
/// unencrypted on disk).
#[derive(Debug, Default, Clone)]
struct FtsIndexData {
    /// term → posting list: `Vec<(node_id, term_frequency)>`.
    postings: HashMap<String, Vec<(u64, u32)>>,
    /// node_id → document length (token count).
    doc_lengths: HashMap<u64, u32>,
    /// Total document count.
    doc_count: u64,
    /// Cumulative token count across all documents (used to compute `avg_dl`).
    total_tokens: u64,
}

// ── FtsIndex ─────────────────────��────────────────────────────────────────────

/// BM25 inverted index for a single `(label, property)` pair.
#[derive(Debug)]
pub struct FtsIndex {
    /// Path on disk.
    file_path: PathBuf,
    /// In-memory state.
    data: FtsIndexData,
    /// Whether in-memory state differs from disk.
    dirty: bool,
    /// BM25 k1 parameter (term saturation). Default 1.2.
    k1: f32,
    /// BM25 b parameter (length normalisation). Default 0.75.
    b: f32,
}

impl FtsIndex {
    // ── Constructors ─────────────────────────────────────────────────────────

    /// Open (or create) an FTS index stored under `{db_root}/fts/`.
    ///
    /// `label` and `property` must be valid identifier strings; they become
    /// part of the file name so they must not contain path separators.
    pub fn open(db_root: &Path, label: &str, property: &str) -> Result<Self> {
        validate_component(label, "label")?;
        validate_component(property, "property")?;
        let file_path = index_path(db_root, label, property);
        let data = if file_path.exists() {
            load(&file_path)?
        } else {
            FtsIndexData::default()
        };
        Ok(Self {
            file_path,
            data,
            dirty: false,
            k1: 1.2,
            b: 0.75,
        })
    }

    /// Create (overwrite) a fresh FTS index, discarding any existing data.
    pub fn create(db_root: &Path, label: &str, property: &str) -> Result<Self> {
        let mut idx = Self::open(db_root, label, property)?;
        idx.data = FtsIndexData::default();
        idx.dirty = true;
        idx.save()?;
        Ok(idx)
    }

    // ── Write ──────────────────────────────────────────────────────────────��──

    /// Index `text` for `node_id`, adding/updating its posting-list entries.
    ///
    /// Calling this again with the same `node_id` replaces (not duplicates)
    /// the existing entry: the old document length and term frequencies are
    /// removed before re-indexing.
    pub fn insert(&mut self, node_id: u64, text: &str) {
        // Remove stale entry for this node (idempotent re-index).
        self.remove_internal(node_id);

        let tokens = tokenize(text);
        if tokens.is_empty() {
            return;
        }

        // Count per-term frequencies.
        let mut term_freq: HashMap<&str, u32> = HashMap::new();
        for t in &tokens {
            *term_freq.entry(t.as_str()).or_insert(0) += 1;
        }

        let doc_len = tokens.len() as u32;

        // Update posting lists.
        for (term, freq) in term_freq {
            self.data
                .postings
                .entry(term.to_owned())
                .or_default()
                .push((node_id, freq));
        }

        self.data.doc_lengths.insert(node_id, doc_len);
        self.data.doc_count += 1;
        self.data.total_tokens += doc_len as u64;
        self.dirty = true;
    }

    /// Remove a document from the index.
    pub fn delete(&mut self, node_id: u64) {
        self.remove_internal(node_id);
        self.dirty = true;
    }

    // ── Read ────────────────────────────────��─────────────────────────────────

    /// Search `query` returning `(node_id, bm25_score)` pairs sorted by
    /// descending score (highest first).  At most `k` results are returned;
    /// pass `usize::MAX` for all results.
    pub fn search(&self, query: &str, k: usize) -> Vec<(u64, f32)> {
        let terms = tokenize(query);
        if terms.is_empty() || self.data.doc_count == 0 {
            return vec![];
        }

        let avg_dl = if self.data.doc_count > 0 {
            self.data.total_tokens as f64 / self.data.doc_count as f64
        } else {
            1.0
        };
        let n = self.data.doc_count as f64;
        let k1 = self.k1 as f64;
        let b = self.b as f64;

        let mut scores: HashMap<u64, f64> = HashMap::new();

        for term in &terms {
            let Some(postings) = self.data.postings.get(term.as_str()) else {
                continue;
            };
            let df = postings.len() as f64;
            // Okapi IDF (Robertson et al., always positive).
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            for &(node_id, tf) in postings {
                let dl = *self.data.doc_lengths.get(&node_id).unwrap_or(&1) as f64;
                let tf_f = tf as f64;
                let score = idf * (tf_f * (k1 + 1.0)) / (tf_f + k1 * (1.0 - b + b * dl / avg_dl));
                *scores.entry(node_id).or_insert(0.0) += score;
            }
        }

        let mut result: Vec<(u64, f32)> =
            scores.into_iter().map(|(id, s)| (id, s as f32)).collect();

        // Sort: descending score, then ascending node_id for determinism.
        result.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        result.truncate(k);
        result
    }

    /// Compute the BM25 score for a single document identified by `node_id`
    /// against `query`.  Returns `0.0` if the node is not in the index.
    pub fn score(&self, node_id: u64, query: &str) -> f32 {
        self.search(query, usize::MAX)
            .into_iter()
            .find(|(id, _)| *id == node_id)
            .map(|(_, s)| s)
            .unwrap_or(0.0)
    }

    /// Returns `true` if `node_id` contains **all** of the given `terms` in
    /// its posting lists.  Used to accelerate CONTAINS pre-filtering.
    pub fn contains_all(&self, node_id: u64, terms: &[&str]) -> bool {
        terms.iter().all(|t| {
            let tl = t.to_lowercase();
            self.data
                .postings
                .get(tl.as_str())
                .map(|pl| pl.iter().any(|(id, _)| *id == node_id))
                .unwrap_or(false)
        })
    }

    /// Returns all node IDs that match any of the given `terms` (OR semantics).
    ///
    /// This is a posting-list bitmap pre-filter suitable for accelerating a
    /// subsequent CONTAINS or STARTS WITH check.
    pub fn candidates_for_terms(&self, terms: &[&str]) -> Vec<u64> {
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for t in terms {
            let tl = t.to_lowercase();
            if let Some(pl) = self.data.postings.get(tl.as_str()) {
                for &(id, _) in pl {
                    seen.insert(id);
                }
            }
        }
        seen.into_iter().collect()
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    /// Persist the index to disk (atomic write-rename).
    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        if let Some(parent) = self.file_path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        let tmp = self.file_path.with_extension("fts.tmp");
        let bytes = encode(&self.data)?;
        std::fs::write(&tmp, &bytes).map_err(Error::Io)?;
        std::fs::rename(&tmp, &self.file_path).map_err(Error::Io)?;
        self.dirty = false;
        Ok(())
    }

    /// Load an existing FTS index from disk.
    pub fn load(db_root: &Path, label: &str, property: &str) -> Result<Self> {
        Self::open(db_root, label, property)
    }

    /// Return `true` if an on-disk index file exists for this `(label, property)`.
    pub fn exists(db_root: &Path, label: &str, property: &str) -> bool {
        index_path(db_root, label, property).exists()
    }

    // ── Helpers ────────────────────────────────────────────────────────────��──

    fn remove_internal(&mut self, node_id: u64) {
        if let Some(&old_len) = self.data.doc_lengths.get(&node_id) {
            // Remove from every posting list.
            for pl in self.data.postings.values_mut() {
                pl.retain(|(id, _)| *id != node_id);
            }
            // Clean up empty posting lists.
            self.data.postings.retain(|_, pl| !pl.is_empty());
            self.data.doc_lengths.remove(&node_id);
            self.data.doc_count = self.data.doc_count.saturating_sub(1);
            self.data.total_tokens = self.data.total_tokens.saturating_sub(old_len as u64);
        }
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Persistent registry that tracks which `(label, property)` pairs have a
/// BM25 full-text index.
///
/// Stored at `{db_root}/fts/registry.json` as a simple JSON array.
#[derive(Debug, Default, Clone)]
pub struct FtsRegistry {
    /// List of `(label, property)` pairs with active indexes.
    pub entries: Vec<(String, String)>,
}

impl FtsRegistry {
    /// Path of the registry file.
    pub fn registry_path(db_root: &Path) -> PathBuf {
        db_root.join("fts").join("registry.json")
    }

    /// Load the registry from disk (returns empty registry if not found).
    pub fn load(db_root: &Path) -> Self {
        let path = Self::registry_path(db_root);
        if !path.exists() {
            return Self::default();
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(_) => return Self::default(),
        };
        // Simple JSON decode: `[["Label","prop"],...]`
        let s = String::from_utf8_lossy(&bytes);
        let entries = parse_registry_json(&s);
        Self { entries }
    }

    /// Register `(label, property)` and persist.
    pub fn register(&mut self, db_root: &Path, label: &str, property: &str) -> Result<()> {
        let pair = (label.to_owned(), property.to_owned());
        if !self.entries.contains(&pair) {
            self.entries.push(pair);
        }
        self.save(db_root)
    }

    /// Returns `true` if `(label, property)` is registered.
    pub fn contains(&self, label: &str, property: &str) -> bool {
        self.entries
            .iter()
            .any(|(l, p)| l == label && p == property)
    }

    fn save(&self, db_root: &Path) -> Result<()> {
        let dir = db_root.join("fts");
        std::fs::create_dir_all(&dir).map_err(Error::Io)?;
        let path = Self::registry_path(db_root);
        let json = format_registry_json(&self.entries);
        std::fs::write(&path, json.as_bytes()).map_err(Error::Io)?;
        Ok(())
    }
}

// ── Tokeniser ──────────────────────────────────────────────────────��──────────

/// Tokenise `text` into lowercase alphanumeric tokens, discarding tokens
/// shorter than 2 characters.
pub fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| s.len() >= 2)
        .map(String::from)
        .collect()
}

// ── Helpers ─────────────────────────────────��─────────────────────────────────

fn index_path(db_root: &Path, label: &str, property: &str) -> PathBuf {
    // Use double-underscore separator between label and property.
    db_root.join("fts").join(format!("{label}__{property}.bin"))
}

fn validate_component(s: &str, kind: &str) -> Result<()> {
    if s.is_empty() || s.contains('/') || s.contains('\\') || s.contains("..") {
        return Err(Error::InvalidArgument(format!(
            "invalid FTS index {kind}: {s:?} — must not be empty or contain path separators"
        )));
    }
    Ok(())
}

// ── Minimal binary codec (no external deps) ───────────────────────────────────
//
// Layout:
//   u64 doc_count
//   u64 total_tokens
//   u64 num_dl_entries
//   per entry: u64 node_id, u32 doc_len
//   u64 num_terms
//   per term:
//     u32 term_len
//     [u8] term bytes
//     u64 num_postings
//     per posting: u64 node_id, u32 term_freq

fn encode(data: &FtsIndexData) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();

    write_u64(&mut buf, data.doc_count);
    write_u64(&mut buf, data.total_tokens);

    // doc_lengths
    write_u64(&mut buf, data.doc_lengths.len() as u64);
    for (&node_id, &len) in &data.doc_lengths {
        write_u64(&mut buf, node_id);
        write_u32(&mut buf, len);
    }

    // postings
    write_u64(&mut buf, data.postings.len() as u64);
    for (term, postings) in &data.postings {
        let tb = term.as_bytes();
        write_u32(&mut buf, tb.len() as u32);
        buf.extend_from_slice(tb);
        write_u64(&mut buf, postings.len() as u64);
        for &(node_id, freq) in postings {
            write_u64(&mut buf, node_id);
            write_u32(&mut buf, freq);
        }
    }

    Ok(buf)
}

fn load(path: &Path) -> Result<FtsIndexData> {
    let bytes = std::fs::read(path).map_err(Error::Io)?;
    let mut cur = 0usize;

    let doc_count = read_u64(&bytes, &mut cur)?;
    let total_tokens = read_u64(&bytes, &mut cur)?;

    let num_dl = read_u64(&bytes, &mut cur)? as usize;
    let mut doc_lengths = HashMap::with_capacity(num_dl);
    for _ in 0..num_dl {
        let node_id = read_u64(&bytes, &mut cur)?;
        let len = read_u32(&bytes, &mut cur)?;
        doc_lengths.insert(node_id, len);
    }

    let num_terms = read_u64(&bytes, &mut cur)? as usize;
    let mut postings = HashMap::with_capacity(num_terms);
    for _ in 0..num_terms {
        let term_len = read_u32(&bytes, &mut cur)? as usize;
        if cur + term_len > bytes.len() {
            return Err(Error::Corruption(
                "FTS index: term length out of bounds".into(),
            ));
        }
        let term = String::from_utf8(bytes[cur..cur + term_len].to_vec())
            .map_err(|e| Error::Corruption(format!("FTS index: invalid UTF-8 term: {e}")))?;
        cur += term_len;

        let num_postings = read_u64(&bytes, &mut cur)? as usize;
        let mut pl = Vec::with_capacity(num_postings);
        for _ in 0..num_postings {
            let node_id = read_u64(&bytes, &mut cur)?;
            let freq = read_u32(&bytes, &mut cur)?;
            pl.push((node_id, freq));
        }
        postings.insert(term, pl);
    }

    Ok(FtsIndexData {
        postings,
        doc_lengths,
        doc_count,
        total_tokens,
    })
}

#[inline]
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[inline]
fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[inline]
fn read_u64(buf: &[u8], cur: &mut usize) -> Result<u64> {
    let end = *cur + 8;
    if end > buf.len() {
        return Err(Error::Corruption(
            "FTS index: unexpected end of data".into(),
        ));
    }
    let v = u64::from_le_bytes(buf[*cur..end].try_into().unwrap());
    *cur = end;
    Ok(v)
}
#[inline]
fn read_u32(buf: &[u8], cur: &mut usize) -> Result<u32> {
    let end = *cur + 4;
    if end > buf.len() {
        return Err(Error::Corruption(
            "FTS index: unexpected end of data".into(),
        ));
    }
    let v = u32::from_le_bytes(buf[*cur..end].try_into().unwrap());
    *cur = end;
    Ok(v)
}

// ── Minimal JSON helpers for registry ────────────────────────────────��───────

fn format_registry_json(entries: &[(String, String)]) -> String {
    let inner: Vec<String> = entries
        .iter()
        .map(|(l, p)| format!("[\"{}\",\"{}\"]", escape_json(l), escape_json(p)))
        .collect();
    format!("[{}]", inner.join(","))
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn parse_registry_json(s: &str) -> Vec<(String, String)> {
    // Very simple parser for `[["A","b"],["C","d"]]` without external deps.
    let mut out = Vec::new();
    let s = s.trim();
    let s = s
        .strip_prefix('[')
        .unwrap_or(s)
        .strip_suffix(']')
        .unwrap_or(s)
        .trim();
    if s.is_empty() {
        return out;
    }
    // Split on `],[` to get individual pair strings.
    for chunk in s.split("],[") {
        let chunk = chunk.trim().trim_start_matches('[').trim_end_matches(']');
        let parts: Vec<&str> = chunk.splitn(2, ',').collect();
        if parts.len() == 2 {
            let label = unquote_json(parts[0].trim());
            let prop = unquote_json(parts[1].trim());
            if !label.is_empty() && !prop.is_empty() {
                out.push((label, prop));
            }
        }
    }
    out
}

fn unquote_json(s: &str) -> String {
    let s = s.trim().strip_prefix('"').unwrap_or(s);
    let s = s.strip_suffix('"').unwrap_or(s);
    s.replace("\\\"", "\"").replace("\\\\", "\\")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basic() {
        let tokens = tokenize("Hello, World! This is a test.");
        // "a" and "is" are < 2 chars? No — both are 2 chars.
        // "a" is 1 char — filtered.
        assert!(tokens.contains(&"hello".to_owned()));
        assert!(tokens.contains(&"world".to_owned()));
        assert!(tokens.contains(&"test".to_owned()));
        assert!(!tokens.contains(&"a".to_owned())); // too short
    }

    #[test]
    fn insert_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FtsIndex::open(dir.path(), "Memory", "content").unwrap();

        idx.insert(1, "transformer attention mechanism");
        idx.insert(2, "neural network backpropagation");
        idx.insert(3, "transformer architecture layers");

        let results = idx.search("transformer attention", 10);
        assert!(results.len() >= 2);
        // Node 1 and 3 both have "transformer"; node 1 also has "attention".
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(
            ids.contains(&1),
            "node 1 should match transformer+attention"
        );
        assert!(ids.contains(&3), "node 3 should match transformer");

        // Node 1 should rank higher than 3 because it also has "attention".
        let score_1 = results
            .iter()
            .find(|(id, _)| *id == 1)
            .map(|(_, s)| *s)
            .unwrap_or(0.0);
        let score_3 = results
            .iter()
            .find(|(id, _)| *id == 3)
            .map(|(_, s)| *s)
            .unwrap_or(0.0);
        assert!(
            score_1 >= score_3,
            "node 1 (has both terms) should rank >= node 3 (has one term)"
        );
    }

    #[test]
    fn bm25_scores_rank_relevant_higher() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FtsIndex::open(dir.path(), "Doc", "text").unwrap();

        // Node 1: highly relevant — "rust" appears 3 times.
        idx.insert(1, "rust rust rust programming language");
        // Node 2: somewhat relevant — "rust" appears once.
        idx.insert(2, "rust is a systems language");
        // Node 3: not relevant.
        idx.insert(3, "python data science pandas");

        let results = idx.search("rust", 10);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(!ids.contains(&3));

        // Node 1 should have a higher score (3 occurrences of "rust").
        let s1 = results.iter().find(|(id, _)| *id == 1).unwrap().1;
        let s2 = results.iter().find(|(id, _)| *id == 2).unwrap().1;
        assert!(
            s1 > s2,
            "node 1 (tf=3) should score higher than node 2 (tf=1)"
        );
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut idx = FtsIndex::open(dir.path(), "Memory", "content").unwrap();
            idx.insert(10, "graph database knowledge");
            idx.insert(20, "relational database sql");
            idx.save().unwrap();
        }

        let idx2 = FtsIndex::open(dir.path(), "Memory", "content").unwrap();
        let results = idx2.search("database", 10);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&10), "node 10 should survive restart");
        assert!(ids.contains(&20), "node 20 should survive restart");
    }

    #[test]
    fn delete_removes_document() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FtsIndex::open(dir.path(), "Doc", "text").unwrap();

        idx.insert(1, "machine learning fundamentals");
        idx.insert(2, "deep learning neural networks");
        idx.delete(1);

        let results = idx.search("learning", 10);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(!ids.contains(&1), "deleted node should not appear");
        assert!(ids.contains(&2), "non-deleted node should appear");
    }

    #[test]
    fn contains_all_acceleration() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FtsIndex::open(dir.path(), "X", "y").unwrap();
        idx.insert(1, "alpha beta gamma");
        idx.insert(2, "alpha delta epsilon");

        assert!(idx.contains_all(1, &["alpha", "beta"]));
        assert!(!idx.contains_all(1, &["alpha", "delta"]));
        assert!(idx.contains_all(2, &["alpha", "delta"]));
    }

    #[test]
    fn registry_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = FtsRegistry::default();
        reg.register(dir.path(), "Memory", "content").unwrap();
        reg.register(dir.path(), "Doc", "body").unwrap();

        let loaded = FtsRegistry::load(dir.path());
        assert!(loaded.contains("Memory", "content"));
        assert!(loaded.contains("Doc", "body"));
        assert!(!loaded.contains("Other", "prop"));
    }
}
