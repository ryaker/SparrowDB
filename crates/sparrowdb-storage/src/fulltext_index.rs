//! Simple inverted full-text index for SparrowDB.
//!
//! Provides `db.index.fulltext.queryNodes` semantics over a named index.
//! Each index maps string terms → `Vec<NodeId>` and is persisted as a
//! plain-text file under `{db_root}/fulltext/{name}.fti`.
//!
//! ## On-disk format
//! Newline-delimited records, one per term bucket:
//! ```text
//! <term>\t<node_id_u64>,<node_id_u64>,...
//! ```
//! The file is rewritten atomically (write to `.tmp`, then rename) on flush.
//!
//! ## Tokenisation
//! Lowercase, split on any non-ASCII-alphanumeric character.  No stemming
//! for v1.
//!
//! ## Query semantics
//! Multi-word queries return the **union** of nodes matching any query term
//! (OR semantics), matching Neo4j's Lucene full-text search defaults.

use std::collections::{BTreeSet, HashMap};
use std::io::{BufRead, Write as IoWrite};
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, Result};

/// A full-text index for one named index configuration.
#[derive(Debug)]
pub struct FulltextIndex {
    /// Path of the `.fti` backing file.
    file_path: PathBuf,
    /// In-memory inverted index: term → BTreeSet<NodeId as u64>.
    ///
    /// `BTreeSet` is used instead of `Vec` so that:
    /// - Deduplication on insert is O(log N) rather than O(N).
    /// - On-disk output is deterministic (sorted) on every flush.
    entries: HashMap<String, BTreeSet<u64>>,
    /// Whether the in-memory state differs from disk.
    dirty: bool,
}

impl FulltextIndex {
    /// Open (or create) the named index stored under `db_root/fulltext/`.
    ///
    /// If the backing file does not exist yet, an empty index is returned
    /// and the directory is created lazily on the first [`flush`].
    pub fn open(db_root: &Path, name: &str) -> Result<Self> {
        validate_index_name(name)?;
        let dir = db_root.join("fulltext");
        let file_path = dir.join(format!("{name}.fti"));

        let entries = if file_path.exists() {
            load_from_file(&file_path)?
        } else {
            HashMap::new()
        };

        Ok(FulltextIndex {
            file_path,
            entries,
            dirty: false,
        })
    }

    /// Index a document: tokenise `text` and record `node_id` for each term.
    ///
    /// Idempotent per `(node_id, term)` pair.
    pub fn add_document(&mut self, node_id: u64, text: &str) {
        for term in tokenize(text) {
            let bucket = self.entries.entry(term).or_default();
            if bucket.insert(node_id) {
                self.dirty = true;
            }
        }
    }

    /// Search for `query` — returns all node IDs that match any query term.
    ///
    /// Results are deduplicated; order is unspecified.
    pub fn search(&self, query: &str) -> Vec<u64> {
        self.search_with_scores(query)
            .into_iter()
            .map(|(id, _score)| id)
            .collect()
    }

    /// Search for `query` — returns `(node_id, score)` pairs for every matching
    /// node, sorted by descending score then ascending node id for determinism.
    ///
    /// Score is the number of distinct query terms that matched the node,
    /// normalised to `[0.0, 1.0]` by dividing by the total number of unique
    /// query terms.  A single-term query always yields score `1.0` for every
    /// hit.  This is a simple term-frequency approximation — suitable for
    /// ranking without full BM25 bookkeeping.
    pub fn search_with_scores(&self, query: &str) -> Vec<(u64, f64)> {
        let terms = tokenize(query);
        let total_terms = terms.len().max(1) as f64;

        // Count how many distinct query terms match each node.
        let mut hit_counts: HashMap<u64, usize> = HashMap::new();
        for term in &terms {
            if let Some(ids) = self.entries.get(term.as_str()) {
                for &id in ids {
                    *hit_counts.entry(id).or_insert(0) += 1;
                }
            }
        }

        // Convert hit counts to normalised scores.
        let mut result: Vec<(u64, f64)> = hit_counts
            .into_iter()
            .map(|(id, count)| (id, count as f64 / total_terms))
            .collect();

        // Sort: descending score, then ascending node id for determinism.
        result.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        result
    }

    /// Persist the index to disk.
    ///
    /// Uses a write-to-temp-then-rename pattern for crash safety.
    pub fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        // Ensure the directory exists.
        if let Some(parent) = self.file_path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }

        let tmp = self.file_path.with_extension("fti.tmp");
        let mut f = std::fs::File::create(&tmp).map_err(Error::Io)?;

        // Sort terms for deterministic output.
        let mut terms: Vec<&String> = self.entries.keys().collect();
        terms.sort();
        for term in terms {
            let ids = &self.entries[term];
            let id_str: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
            writeln!(f, "{}\t{}", term, id_str.join(",")).map_err(Error::Io)?;
        }
        drop(f);

        std::fs::rename(&tmp, &self.file_path).map_err(Error::Io)?;
        self.dirty = false;
        Ok(())
    }

    /// Create (or overwrite) a named full-text index definition.
    ///
    /// Any existing index data is discarded — the resulting index is empty.
    /// The backing file is created (or truncated) immediately.
    pub fn create(db_root: &Path, name: &str) -> Result<Self> {
        let mut idx = Self::open(db_root, name)?;
        // Overwrite semantics: discard any entries loaded from disk.
        idx.entries.clear();
        idx.dirty = true;
        idx.flush()?;
        Ok(idx)
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Validate that an index name is safe to use as a filename component.
///
/// Rejects empty names and names containing path separators or `..` sequences
/// to prevent directory traversal attacks.
fn validate_index_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(Error::InvalidArgument(format!(
            "invalid fulltext index name: {name:?} — must not be empty or contain path separators"
        )));
    }
    Ok(())
}

/// Load the on-disk `.fti` file into a term → node-id map.
fn load_from_file(path: &Path) -> Result<HashMap<String, BTreeSet<u64>>> {
    let file = std::fs::File::open(path).map_err(Error::Io)?;
    let reader = std::io::BufReader::new(file);
    let mut entries: HashMap<String, BTreeSet<u64>> = HashMap::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.map_err(Error::Io)?;
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let term = parts.next().unwrap_or("").to_owned();
        let ids_str = parts.next().unwrap_or("");

        if term.is_empty() {
            continue;
        }

        let ids: BTreeSet<u64> = ids_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u64>().map_err(|e| {
                    Error::Corruption(format!(
                        "fulltext index: bad node id on line {line_no}: {e}"
                    ))
                })
            })
            .collect::<Result<BTreeSet<_>>>()?;

        entries.insert(term, ids);
    }

    Ok(entries)
}

/// Tokenise text into lowercase alphanumeric terms.
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_search_basic() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FulltextIndex::open(dir.path(), "test").unwrap();
        idx.add_document(1, "Hello World");
        idx.add_document(2, "world peace");
        idx.flush().unwrap();

        let hits = idx.search("world");
        assert!(hits.contains(&1), "node 1 should match 'world'");
        assert!(hits.contains(&2), "node 2 should match 'world'");

        let hits2 = idx.search("hello");
        assert_eq!(hits2, vec![1]);
    }

    #[test]
    fn roundtrip_persistence() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx = FulltextIndex::open(dir.path(), "persist").unwrap();
            idx.add_document(42, "persistent data");
            idx.flush().unwrap();
        }
        // Re-open and search.
        let idx2 = FulltextIndex::open(dir.path(), "persist").unwrap();
        let hits = idx2.search("data");
        assert_eq!(hits, vec![42]);
    }

    #[test]
    fn search_no_match_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let idx = FulltextIndex::open(dir.path(), "empty").unwrap();
        assert!(idx.search("anything").is_empty());
    }

    #[test]
    fn dedup_on_repeated_add() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FulltextIndex::open(dir.path(), "dedup").unwrap();
        idx.add_document(7, "test data");
        idx.add_document(7, "test data");
        let hits = idx.search("test");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], 7);
    }
}
