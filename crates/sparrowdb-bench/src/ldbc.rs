//! LDBC SNB (Social Network Benchmark) CSV data loader — SPA-145.
//!
//! Reads pipe-delimited (`|`) LDBC SNB CSV files from a directory and ingests
//! them into a SparrowDB database via [`GraphDb::begin_write`] / [`WriteTx`].
//!
//! ## Supported entity types (nodes)
//!
//! | LDBC file prefix      | SparrowDB label  |
//! |-----------------------|-----------------|
//! | `person_*`            | `Person`        |
//! | `post_*`              | `Post`          |
//! | `comment_*`           | `Comment`       |
//! | `forum_*`             | `Forum`         |
//! | `organisation_*`      | `Organisation`  |
//! | `place_*`             | `Place`         |
//! | `tag_*`               | `Tag`           |
//! | `tagclass_*`          | `TagClass`      |
//!
//! ## Supported relationship types (edges)
//!
//! LDBC SNB dynamic/static edge files follow the naming pattern
//! `<src>_<reltype>_<dst>_*.csv`.  The loader parses this pattern to derive
//! the source label, relation type, and destination label.
//!
//! ## Batch size
//!
//! Nodes and edges are committed every [`BATCH_SIZE`] records (default 10 000)
//! to bound memory usage while keeping WAL overhead low.

use sparrowdb::{GraphDb, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Records per write transaction (commit every N rows).
pub const BATCH_SIZE: usize = 10_000;

// ── Label / rel-type mappings ─────────────────────────────────────────────────

/// Derive a canonical SparrowDB label from an LDBC entity-file prefix.
pub fn label_for(raw: &str) -> &'static str {
    match raw.to_lowercase().as_str() {
        "person" => "Person",
        "post" => "Post",
        "comment" => "Comment",
        "forum" => "Forum",
        "organisation" | "organization" => "Organisation",
        "place" => "Place",
        "tag" => "Tag",
        "tagclass" => "TagClass",
        _ => "Unknown",
    }
}

// ── Progress tracker ──────────────────────────────────────────────────────────

/// Running totals emitted to stderr during ingestion.
#[derive(Default, Debug)]
pub struct LoadStats {
    pub nodes_loaded: u64,
    pub edges_loaded: u64,
    pub files_processed: u64,
    pub errors: u64,
}

impl LoadStats {
    pub(crate) fn print(&self, elapsed: std::time::Duration) {
        eprintln!(
            "[ldbc-load] files={} nodes={} edges={} errors={} elapsed={:.1}s",
            self.files_processed,
            self.nodes_loaded,
            self.edges_loaded,
            self.errors,
            elapsed.as_secs_f64(),
        );
    }
}

// ── LDBC file classification ──────────────────────────────────────────────────

/// The kind of data a single LDBC CSV file contains.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FileKind {
    /// Node file — all rows become nodes with `label`.
    Nodes { label: String },
    /// Edge file — rows become edges via `rel_type`.
    Edges {
        src_label: String,
        rel_type: String,
        dst_label: String,
    },
}

/// Classify an LDBC CSV filename stem (no extension, no directory) into a
/// [`FileKind`].
///
/// LDBC SNB naming conventions:
/// - Node files: `<entity>_[0-9]*.csv`  e.g. `person_0_0.csv`, `forum.csv`
/// - Edge files: `<src>_<REL>_<dst>_[0-9]*.csv`
///   e.g. `person_knows_person_0_0.csv`
pub(crate) fn classify_stem(stem: &str) -> Option<FileKind> {
    // Split on `_`, strip trailing numeric-only segments.
    let parts: Vec<&str> = stem.split('_').collect();
    let mut words: Vec<&str> = Vec::new();
    for p in &parts {
        if p.chars().all(|c| c.is_ascii_digit()) {
            break;
        }
        words.push(p);
    }

    match words.len() {
        1 => Some(FileKind::Nodes {
            label: label_for(words[0]).to_string(),
        }),
        3 => Some(FileKind::Edges {
            src_label: label_for(words[0]).to_string(),
            rel_type: words[1].to_string(),
            dst_label: label_for(words[2]).to_string(),
        }),
        n if n >= 3 => Some(FileKind::Edges {
            src_label: label_for(words[0]).to_string(),
            rel_type: words[1].to_string(),
            dst_label: label_for(words[2]).to_string(),
        }),
        _ => None,
    }
}

// ── Node ID resolution ────────────────────────────────────────────────────────

/// Maps raw LDBC integer IDs (per label) to SparrowDB [`NodeId`]s assigned at
/// insert time.
///
/// LDBC uses per-entity-type monotonic IDs that reset across entity types, so
/// we key on `(label, ldbc_id)`.
#[derive(Default)]
pub struct IdMap {
    inner: HashMap<(String, String), u64>,
}

impl IdMap {
    pub fn insert(&mut self, label: &str, ldbc_id: &str, node_id: sparrowdb::NodeId) {
        self.inner
            .insert((label.to_string(), ldbc_id.to_string()), node_id.0);
    }

    pub fn get(&self, label: &str, ldbc_id: &str) -> Option<sparrowdb::NodeId> {
        self.inner
            .get(&(label.to_string(), ldbc_id.to_string()))
            .map(|&raw| sparrowdb::NodeId(raw))
    }
}

// ── CSV row → SparrowDB properties ───────────────────────────────────────────

/// Convert a CSV record into named-property pairs for `create_node_named`.
///
/// The LDBC `id` column is renamed to `ldbc_id`.  Numeric columns are stored
/// as `Int64`; everything else as `Bytes` (UTF-8 string).
fn row_to_named_props(
    headers: &csv::StringRecord,
    row: &csv::StringRecord,
) -> Vec<(String, Value)> {
    headers
        .iter()
        .zip(row.iter())
        .map(|(h, v)| {
            let key = if h.to_lowercase() == "id" {
                "ldbc_id".to_string()
            } else {
                h.to_string()
            };
            let val = if let Ok(n) = v.parse::<i64>() {
                Value::Int64(n)
            } else {
                Value::Bytes(v.as_bytes().to_vec())
            };
            (key, val)
        })
        .collect()
}

// ── Node file ingestion ───────────────────────────────────────────────────────

/// Load one LDBC node CSV file into `db`.
///
/// Returns the number of nodes written.  Made `pub` so tests can call it
/// directly without going through the full directory discovery path.
pub fn load_node_file(
    db: &GraphDb,
    path: &Path,
    label: &str,
    id_map: &mut IdMap,
    stats: &mut LoadStats,
) -> sparrowdb::Result<u64> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(true)
        .from_path(path)
        .map_err(|e| sparrowdb::Error::Io(std::io::Error::other(e.to_string())))?;

    let headers = rdr
        .headers()
        .map_err(|e| sparrowdb::Error::Io(std::io::Error::other(e.to_string())))?
        .clone();

    let id_col_idx = headers
        .iter()
        .position(|h| h.to_lowercase() == "id")
        .unwrap_or(0);

    let mut count = 0u64;
    let mut tx = db.begin_write()?;

    for result in rdr.records() {
        let record =
            result.map_err(|e| sparrowdb::Error::Io(std::io::Error::other(e.to_string())))?;

        let ldbc_id = record.get(id_col_idx).unwrap_or("").to_string();
        let named_props = row_to_named_props(&headers, &record);

        let label_id = tx.get_or_create_label_id(label)?;
        let node_id = tx.create_node_named(label_id, &named_props)?;

        id_map.insert(label, &ldbc_id, node_id);
        count += 1;

        if count % BATCH_SIZE as u64 == 0 {
            tx.commit()?;
            tx = db.begin_write()?;
            eprintln!("[ldbc-load]   ... {count} {label} nodes written");
        }
    }

    tx.commit()?;
    stats.nodes_loaded += count;
    Ok(count)
}

// ── Edge file ingestion ───────────────────────────────────────────────────────

/// Load one LDBC edge CSV file into `db`.
///
/// The first two columns of an LDBC edge file are always the source and
/// destination entity IDs.  Additional columns are ignored.
///
/// Returns the number of edges written.
pub fn load_edge_file(
    db: &GraphDb,
    path: &Path,
    src_label: &str,
    rel_type: &str,
    dst_label: &str,
    id_map: &IdMap,
    stats: &mut LoadStats,
) -> sparrowdb::Result<u64> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(true)
        .from_path(path)
        .map_err(|e| sparrowdb::Error::Io(std::io::Error::other(e.to_string())))?;

    let mut count = 0u64;
    let mut skipped = 0u64;
    let mut tx = db.begin_write()?;

    for result in rdr.records() {
        let record =
            result.map_err(|e| sparrowdb::Error::Io(std::io::Error::other(e.to_string())))?;

        let src_raw = record.get(0).unwrap_or("").trim();
        let dst_raw = record.get(1).unwrap_or("").trim();

        let src_node = id_map.get(src_label, src_raw);
        let dst_node = id_map.get(dst_label, dst_raw);

        match (src_node, dst_node) {
            (Some(src), Some(dst)) => {
                tx.create_edge(src, dst, rel_type, HashMap::new())?;
                count += 1;
            }
            _ => {
                skipped += 1;
                stats.errors += 1;
            }
        }

        if count % BATCH_SIZE as u64 == 0 && count > 0 {
            tx.commit()?;
            tx = db.begin_write()?;
            eprintln!("[ldbc-load]   ... {count} {rel_type} edges written");
        }
    }

    tx.commit()?;

    if skipped > 0 {
        eprintln!("[ldbc-load] WARNING: {skipped} {rel_type} edges skipped (unresolved IDs)");
    }

    stats.edges_loaded += count;
    Ok(count)
}

// ── Top-level loader ──────────────────────────────────────────────────────────

/// Discover all LDBC CSV files under `data_dir` and load them into `db`.
///
/// Node files are processed first so that the [`IdMap`] is populated before
/// edge files attempt to resolve source/destination node IDs.
///
/// Returns the final [`LoadStats`].
pub fn load(db: &GraphDb, data_dir: &Path) -> sparrowdb::Result<LoadStats> {
    let start = Instant::now();
    let mut stats = LoadStats::default();
    let mut id_map = IdMap::default();

    let csv_files = collect_csv_files(data_dir);

    let mut node_files: Vec<(PathBuf, FileKind)> = Vec::new();
    let mut edge_files: Vec<(PathBuf, FileKind)> = Vec::new();

    for (path, kind) in csv_files {
        match &kind {
            FileKind::Nodes { .. } => node_files.push((path, kind)),
            FileKind::Edges { .. } => edge_files.push((path, kind)),
        }
    }

    eprintln!(
        "[ldbc-load] Discovered {} node file(s) and {} edge file(s) under {}",
        node_files.len(),
        edge_files.len(),
        data_dir.display()
    );

    for (path, kind) in &node_files {
        if let FileKind::Nodes { label } = kind {
            eprintln!("[ldbc-load] Loading nodes: {} → {label}", path.display());
            match load_node_file(db, path, label, &mut id_map, &mut stats) {
                Ok(n) => eprintln!("[ldbc-load]   {n} {label} nodes loaded"),
                Err(e) => {
                    eprintln!("[ldbc-load] ERROR loading {}: {e}", path.display());
                    stats.errors += 1;
                }
            }
            stats.files_processed += 1;
        }
    }

    for (path, kind) in &edge_files {
        if let FileKind::Edges {
            src_label,
            rel_type,
            dst_label,
        } = kind
        {
            eprintln!(
                "[ldbc-load] Loading edges: {} → ({src_label})-[:{rel_type}]→({dst_label})",
                path.display()
            );
            match load_edge_file(
                db, path, src_label, rel_type, dst_label, &id_map, &mut stats,
            ) {
                Ok(n) => eprintln!("[ldbc-load]   {n} {rel_type} edges loaded"),
                Err(e) => {
                    eprintln!("[ldbc-load] ERROR loading {}: {e}", path.display());
                    stats.errors += 1;
                }
            }
            stats.files_processed += 1;
        }
    }

    stats.print(start.elapsed());
    Ok(stats)
}

// ── File discovery ────────────────────────────────────────────────────────────

fn collect_csv_files(data_dir: &Path) -> Vec<(PathBuf, FileKind)> {
    let mut results = Vec::new();
    walk_dir(data_dir, &mut results);
    results
}

fn walk_dir(dir: &Path, out: &mut Vec<(PathBuf, FileKind)>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_dir(&path, out);
        } else if path.extension().map(|e| e == "csv").unwrap_or(false) {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Some(kind) = classify_stem(stem) {
                    out.push((path, kind));
                }
            }
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_for() {
        assert_eq!(label_for("person"), "Person");
        assert_eq!(label_for("PERSON"), "Person");
        assert_eq!(label_for("post"), "Post");
        assert_eq!(label_for("comment"), "Comment");
        assert_eq!(label_for("forum"), "Forum");
        assert_eq!(label_for("organisation"), "Organisation");
        assert_eq!(label_for("organization"), "Organisation");
        assert_eq!(label_for("place"), "Place");
        assert_eq!(label_for("tag"), "Tag");
        assert_eq!(label_for("tagclass"), "TagClass");
        assert_eq!(label_for("unknown_entity"), "Unknown");
    }

    #[test]
    fn test_classify_stem_node() {
        let k = classify_stem("person_0_0").unwrap();
        assert_eq!(
            k,
            FileKind::Nodes {
                label: "Person".to_string()
            }
        );

        let k = classify_stem("forum").unwrap();
        assert_eq!(
            k,
            FileKind::Nodes {
                label: "Forum".to_string()
            }
        );

        let k = classify_stem("tagclass_0_0").unwrap();
        assert_eq!(
            k,
            FileKind::Nodes {
                label: "TagClass".to_string()
            }
        );
    }

    #[test]
    fn test_classify_stem_edge() {
        let k = classify_stem("person_knows_person_0_0").unwrap();
        assert_eq!(
            k,
            FileKind::Edges {
                src_label: "Person".to_string(),
                rel_type: "knows".to_string(),
                dst_label: "Person".to_string(),
            }
        );

        let k = classify_stem("forum_hasMember_person_0_0").unwrap();
        assert_eq!(
            k,
            FileKind::Edges {
                src_label: "Forum".to_string(),
                rel_type: "hasMember".to_string(),
                dst_label: "Person".to_string(),
            }
        );
    }

    #[test]
    fn test_id_map_insert_get() {
        let mut map = IdMap::default();
        let nid = sparrowdb::NodeId(0x0001_0000_0000_0000);
        map.insert("Person", "42", nid);
        assert_eq!(map.get("Person", "42"), Some(nid));
        assert_eq!(map.get("Person", "99"), None);
        assert_eq!(map.get("Forum", "42"), None);
    }
}
