// ── Helper functions ──────────────────────────────────────────────────────────
//
// Utility functions used across multiple submodules.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::{col_id_of, Error};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::node_store::{NodeStore, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

// ── FNV-1a col_id derivation ─────────────────────────────────────────────────

/// Derive a stable `u32` column ID from a property key name.
///
/// Delegates to [`sparrowdb_common::col_id_of`] — the single canonical
/// FNV-1a implementation shared by storage and execution (SPA-160).
pub fn fnv1a_col_id(key: &str) -> u32 {
    col_id_of(key)
}

// ── Cypher string utilities ────────────────────────────────────────────────────

/// Escape a Rust `&str` so it can be safely interpolated inside a single-quoted
/// Cypher string literal.
///
/// Two characters require escaping inside Cypher single-quoted strings:
/// * `\` → `\\`  (backslash must be doubled first to avoid misinterpreting
///   the subsequent escape sequence)
/// * `'` → `\'`  (prevents premature termination of the string literal)
///
/// # Example
///
/// ```
/// use sparrowdb::cypher_escape_string;
/// let safe = cypher_escape_string("O'Reilly");
/// let cypher = format!("MATCH (n {{name: '{safe}'}}) RETURN n");
/// assert_eq!(cypher, "MATCH (n {name: 'O\\'Reilly'}) RETURN n");
/// ```
///
/// **Prefer parameterized queries** (`execute_with_params`) over string
/// interpolation whenever possible — this function is provided for the cases
/// where dynamic query construction cannot be avoided (SPA-218).
pub fn cypher_escape_string(s: &str) -> String {
    // Process backslash first so that the apostrophe replacement below does
    // not accidentally double-escape newly-inserted backslashes.
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

// ── Mutation value helpers ─────────────────────────────────────────────────────

/// Convert a Cypher [`Literal`] to a storage [`Value`].
pub(crate) fn literal_to_value(lit: &sparrowdb_cypher::ast::Literal) -> Value {
    use sparrowdb_cypher::ast::Literal;
    match lit {
        Literal::Int(n) => Value::Int64(*n),
        // Float stored as Value::Float — NodeStore::encode_value writes the full
        // 8 IEEE-754 bytes to the overflow heap (SPA-267).
        Literal::Float(f) => Value::Float(*f),
        Literal::Bool(b) => Value::Int64(if *b { 1 } else { 0 }),
        Literal::String(s) => Value::Bytes(s.as_bytes().to_vec()),
        Literal::Null | Literal::Param(_) => Value::Int64(0),
    }
}

/// Convert a Cypher [`Expr`] to a storage [`Value`].
pub(crate) fn expr_to_value(expr: &sparrowdb_cypher::ast::Expr) -> Value {
    use sparrowdb_cypher::ast::Expr;
    match expr {
        Expr::Literal(lit) => literal_to_value(lit),
        _ => Value::Int64(0),
    }
}

pub(crate) fn literal_to_value_with_params(
    lit: &sparrowdb_cypher::ast::Literal,
    params: &HashMap<String, sparrowdb_execution::Value>,
) -> crate::Result<Value> {
    use sparrowdb_cypher::ast::Literal;
    match lit {
        Literal::Int(n) => Ok(Value::Int64(*n)),
        Literal::Float(f) => Ok(Value::Float(*f)),
        Literal::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
        Literal::String(s) => Ok(Value::Bytes(s.as_bytes().to_vec())),
        Literal::Null => Ok(Value::Int64(0)),
        Literal::Param(p) => match params.get(p.as_str()) {
            Some(v) => Ok(exec_value_to_storage(v)),
            None => Err(sparrowdb_common::Error::InvalidArgument(format!(
                "parameter ${p} was referenced in the query but not supplied"
            ))),
        },
    }
}

pub(crate) fn expr_to_value_with_params(
    expr: &sparrowdb_cypher::ast::Expr,
    params: &HashMap<String, sparrowdb_execution::Value>,
) -> crate::Result<Value> {
    use sparrowdb_cypher::ast::Expr;
    match expr {
        Expr::Literal(lit) => literal_to_value_with_params(lit, params),
        _ => Err(sparrowdb_common::Error::InvalidArgument(
            "property value must be a literal or $parameter".into(),
        )),
    }
}

pub(crate) fn exec_value_to_storage(v: &sparrowdb_execution::Value) -> Value {
    use sparrowdb_execution::Value as EV;
    match v {
        EV::Int64(n) => Value::Int64(*n),
        EV::Float64(f) => Value::Float(*f),
        EV::Bool(b) => Value::Int64(if *b { 1 } else { 0 }),
        EV::String(s) => Value::Bytes(s.as_bytes().to_vec()),
        _ => Value::Int64(0),
    }
}

/// Convert a storage-layer `Value` (Int64 / Bytes / Float) to the execution-layer
/// `Value` (Int64 / String / Float64 / Null / …) used in `QueryResult` rows.
pub(crate) fn storage_value_to_exec(val: &Value) -> sparrowdb_execution::Value {
    match val {
        Value::Int64(n) => sparrowdb_execution::Value::Int64(*n),
        Value::Bytes(b) => {
            sparrowdb_execution::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        Value::Float(f) => sparrowdb_execution::Value::Float64(*f),
    }
}

/// Evaluate a RETURN expression against a simple name→ExecValue map built
/// from the merged node's properties.  Used exclusively by `execute_merge`.
///
/// Supports `PropAccess` (e.g. `n.name`) and `Literal`; everything else
/// falls back to `Null`.
pub(crate) fn eval_expr_merge(
    expr: &sparrowdb_cypher::ast::Expr,
    vals: &HashMap<String, sparrowdb_execution::Value>,
) -> sparrowdb_execution::Value {
    use sparrowdb_cypher::ast::{Expr, Literal};
    match expr {
        Expr::PropAccess { var, prop } => {
            let key = format!("{var}.{prop}");
            vals.get(&key)
                .cloned()
                .unwrap_or(sparrowdb_execution::Value::Null)
        }
        Expr::Literal(lit) => match lit {
            Literal::Int(n) => sparrowdb_execution::Value::Int64(*n),
            Literal::Float(f) => sparrowdb_execution::Value::Float64(*f),
            Literal::Bool(b) => sparrowdb_execution::Value::Bool(*b),
            Literal::String(s) => sparrowdb_execution::Value::String(s.clone()),
            Literal::Null | Literal::Param(_) => sparrowdb_execution::Value::Null,
        },
        Expr::Var(v) => vals
            .get(v.as_str())
            .cloned()
            .unwrap_or(sparrowdb_execution::Value::Null),
        _ => sparrowdb_execution::Value::Null,
    }
}

/// Returns `true` if the `DELETE` clause variable in a `MatchMutateStatement`
/// refers to a relationship pattern variable rather than a node variable.
///
/// Used to route `MATCH (a)-[r:REL]->(b) DELETE r` to the edge-delete path
/// instead of the node-delete path.
pub(crate) fn is_edge_delete_mutation(mm: &sparrowdb_cypher::ast::MatchMutateStatement) -> bool {
    // DELETE is always stored as a single-element mutations vec.
    if mm.mutations.len() != 1 {
        return false;
    }
    let sparrowdb_cypher::ast::Mutation::Delete { var, .. } = &mm.mutations[0] else {
        return false;
    };
    mm.match_patterns
        .iter()
        .any(|p| p.rels.iter().any(|r| !r.var.is_empty() && &r.var == var))
}

// ── Reserved label/type protection (SPA-208) ──────────────────────────────────

/// Returns `true` if `label` starts with the reserved `__SO_` prefix.
///
/// The `__SO_` namespace is reserved for internal SparrowDB system objects.
/// Any attempt to CREATE a node or relationship using a label/type in this
/// namespace is rejected with an [`Error::InvalidArgument`].
#[inline]
pub(crate) fn is_reserved_label(label: &str) -> bool {
    label.starts_with("__SO_")
}

// ── Constraint persistence helpers (issue #306) ─────────────────────────────

pub(crate) const CONSTRAINTS_FILE: &str = "constraints.bin";

/// Serialize the unique-constraint set to `<db_root>/constraints.bin`.
///
/// Format: `[count: u32 LE][label_id: u32 LE, col_id: u32 LE]*`
pub(crate) fn save_constraints(
    db_root: &Path,
    constraints: &HashSet<(u32, u32)>,
) -> crate::Result<()> {
    use std::io::Write;
    let path = db_root.join(CONSTRAINTS_FILE);
    let mut buf = Vec::with_capacity(4 + constraints.len() * 8);
    buf.extend_from_slice(&(constraints.len() as u32).to_le_bytes());
    for &(label_id, col_id) in constraints {
        buf.extend_from_slice(&label_id.to_le_bytes());
        buf.extend_from_slice(&col_id.to_le_bytes());
    }
    // Atomic write: write to a temp file then rename so a crash mid-write
    // never leaves a truncated constraints file.
    let tmp_path = db_root.join("constraints.bin.tmp");
    let mut f = std::fs::File::create(&tmp_path)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load the unique-constraint set from `<db_root>/constraints.bin`.
///
/// Returns an empty set if the file does not exist (fresh database).
pub(crate) fn load_constraints(db_root: &Path) -> HashSet<(u32, u32)> {
    let path = db_root.join(CONSTRAINTS_FILE);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(_) => return HashSet::new(),
    };
    if data.len() < 4 {
        return HashSet::new();
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let expected_len = 4 + count * 8;
    if data.len() < expected_len {
        return HashSet::new();
    }
    let mut set = HashSet::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 8;
        let label_id = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
        let col_id =
            u32::from_le_bytes([data[off + 4], data[off + 5], data[off + 6], data[off + 7]]);
        set.insert((label_id, col_id));
    }
    set
}

/// Build a `LabelId → node count` map by reading each label's HWM from disk
/// (SPA-190).  Called at `GraphDb::open()` and after node-mutating writes.
pub(crate) fn build_label_row_counts_from_disk(
    catalog: &Catalog,
    db_root: &Path,
) -> HashMap<sparrowdb_catalog::catalog::LabelId, usize> {
    let store = match NodeStore::open(db_root) {
        Ok(s) => s,
        Err(_) => return HashMap::new(),
    };
    catalog
        .list_labels()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(lid, _name)| {
            let hwm = store.hwm_for_label(lid as u32).unwrap_or(0);
            if hwm > 0 {
                Some((lid, hwm as usize))
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn open_csr_map(path: &Path) -> HashMap<u32, CsrForward> {
    let catalog = match Catalog::open(path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };
    let mut map = HashMap::new();

    // Collect rel IDs from catalog.
    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();

    // Always include the legacy table-0 slot so that checkpointed CSRs
    // written before the catalog had entries (pre-SPA-185 data) are loaded.
    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }

    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    map
}

/// Like [`open_csr_map`] but surfaces the catalog-open error so callers can
/// decide whether to replace an existing cache.  Used by
/// [`GraphDb::invalidate_csr_map`] to avoid clobbering a valid in-memory map
/// with an empty one when the catalog is transiently unreadable.
pub(crate) fn try_open_csr_map(path: &Path) -> crate::Result<HashMap<u32, CsrForward>> {
    let catalog = Catalog::open(path)?;
    let mut map = HashMap::new();

    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();

    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }

    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    Ok(map)
}

// ── Vector index helpers (issue #394) ────────────────────────────────────────

/// Recover the `(label, prop)` pair from an `hnsw_<label>_<prop>` file stem.
///
/// Labels may contain underscores, so the *last* underscore is the separator.
fn parse_index_stem(stem: &str) -> Option<(String, String)> {
    let rest = stem.strip_prefix("hnsw_")?;
    let sep = rest.rfind('_')?;
    let (label, prop) = (&rest[..sep], &rest[sep + 1..]);
    if label.is_empty() || prop.is_empty() {
        return None;
    }
    Some((label.to_string(), prop.to_string()))
}

/// One recognised entry in `<db_root>/vector_indexes/`.
enum IndexFile {
    /// `hnsw_<label>_<prop>.bin` — a live index the loader will try to read.
    Live { label: String, prop: String },
    /// `hnsw_<label>_<prop>.bin.corrupt.<millis>` — bytes a previous load
    /// attempt rejected and moved aside (#442).  Deliberately *not* live: it
    /// must never be loaded, and it must never block `open`.  It is still
    /// evidence of damage, so it must remain visible to the diagnostic.
    Quarantined {
        label: String,
        prop: String,
        path: PathBuf,
    },
}

/// Take one snapshot of `<dir>` and classify every recognised entry.
///
/// A single `read_dir` pass matters on the open path: `load_and_quarantine`
/// renames the file it rejects, so probing a live entry can *create* a
/// quarantine artifact while we are working; snapshotting first means such an
/// artifact cannot also be picked up in the same call and reported twice.
/// (Since #456 the diagnostic uses the non-destructive `load` and cannot create
/// artifacts at all, but the open path still can.)
///
/// Results are sorted so callers get a deterministic order regardless of
/// directory iteration order.
///
/// # "Nothing here" and "cannot tell" are different answers (#456)
///
/// `Ok(vec![])` means the directory was read and holds no recognised entry —
/// including the ordinary case of a database that never created a vector index,
/// where the directory does not exist at all.
///
/// `Err(reason)` means the directory could **not** be listed: a permission
/// block, a plain file or dangling symlink sitting where the directory belongs,
/// an I/O error.  This used to collapse into the same empty vector, so a
/// `chmod 000` on `vector_indexes/` produced a result byte-identical to a
/// genuinely healthy store.  A monitor could not distinguish green from blind,
/// and a misconfigured service account or a transient mount failure during a
/// poll reported "healthy".
fn scan_vector_index_dir(dir: &Path) -> std::result::Result<Vec<IndexFile>, String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // An absent directory is the ordinary "no vector index was ever
        // created" state.  `symlink_metadata` does not follow links, so a
        // dangling symlink in the directory's place is *not* absent — it lands
        // in the arm below, where it belongs.
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                && std::fs::symlink_metadata(dir).is_err() =>
        {
            return Ok(Vec::new())
        }
        Err(e) => {
            return Err(format!(
                "{} could not be listed ({e}); the contents of this database's vector \
                 index directory are unknown, so the absence of reported damage means \
                 nothing was observed, not that nothing is wrong",
                dir.display()
            ))
        }
    };
    let mut found = Vec::new();
    for entry in entries {
        // A per-entry error is the same blindness at a finer grain: we know
        // something is there and cannot say what.
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                return Err(format!(
                    "an entry in {} could not be read ({e}); the listing is incomplete, \
                     so no conclusion about damage can be drawn from it",
                    dir.display()
                ))
            }
        };
        let name = entry.file_name();
        // Our own names are ASCII by construction (`index_path` sanitises), so a
        // non-UTF-8 name cannot be an index of ours and is genuinely not ours to
        // report on.
        let Some(name) = name.to_str() else { continue };
        if let Some(stem) = name.strip_suffix(".bin") {
            if let Some((label, prop)) = parse_index_stem(stem) {
                found.push(IndexFile::Live { label, prop });
            }
        } else if let Some((head, _stamp)) = name.split_once(".bin.corrupt.") {
            if let Some((label, prop)) = parse_index_stem(head) {
                found.push(IndexFile::Quarantined {
                    label,
                    prop,
                    path: entry.path(),
                });
            }
        }
    }
    found.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
    Ok(found)
}

fn sort_key(f: &IndexFile) -> (&str, &str, &Path) {
    match f {
        IndexFile::Live { label, prop } => (label, prop, Path::new("")),
        IndexFile::Quarantined { label, prop, path } => (label, prop, path.as_path()),
    }
}

/// Scan `<db_root>/vector_indexes/` and load all persisted HNSW indexes.
///
/// The directory contains files named `hnsw_<label>_<prop>.bin`; the
/// `(label, prop)` key is reconstructed from the file name.
///
/// # Errors
///
/// Returns [`Error::Corruption`] if an index file **exists but cannot be
/// loaded** (truncated, corrupt, unreadable, or rejected by the header/CRC
/// validation added in #442).
///
/// This is deliberately fail-closed, and deliberately different from the
/// stance taken for [`PropertyIndex`](sparrowdb_storage::property_index::PropertyIndex),
/// whose load failures are *not* fatal because that index is derived state —
/// the next `Engine` simply rebuilds it from the column files.  An HNSW index
/// is **not** derived state: vectors added through `addToVectorIndex` (#441)
/// live only in this file and cannot be rebuilt from anything else.
///
/// Swallowing the error here is what turned one damaged file into a silent
/// total outage: a missing key in the map means "no index configured", so
/// every later vector write for that `(label, prop)` is dropped on the floor
/// and every `vectorSearch` returns nothing, while the engine reports "no
/// vector index; call createVectorIndex first" — indistinguishable from an
/// operator configuration mistake.  Refusing to open is louder, recoverable
/// (move the file aside and re-open, then rebuild deliberately) and never
/// destroys data that a running-but-broken database would have discarded.
///
/// An *absent* file is not an error: it legitimately means no index is
/// configured for that `(label, prop)`, and the map simply has no entry.
/// Neither is a quarantine artifact (#442) — those bytes have already been
/// taken out of service, so they must not hold the database hostage on every
/// subsequent start.  They stay visible through
/// [`vector_index_load_failures`] instead.
///
/// # Every damaged file is probed, not just the first (#456)
///
/// This used to `return` on the first `Err`, so exactly one bad file was probed
/// — and therefore quarantined — per `open()`.  With two damaged indexes that
/// cost three restarts to reach a database that opens, and the operator saw one
/// index named per attempt while the other stayed invisible.  The loop now runs
/// to completion and reports every failure in one error, so a single failed
/// `open` tells the operator the full extent of the damage.
pub(crate) fn load_vector_indexes(db_root: &Path) -> crate::Result<crate::types::VectorIndexMap> {
    let dir = db_root.join("vector_indexes");
    let mut map: crate::types::VectorIndexMap = HashMap::new();
    let mut failures: Vec<String> = Vec::new();
    // A directory we cannot list is not a directory we can declare empty.
    // Opening anyway would mean "no index configured" for every pair that might
    // be in there, and every vector write for them dropped on the floor — the
    // silent outage this whole path is fail-closed to prevent (#456).
    let entries = scan_vector_index_dir(&dir).map_err(Error::Corruption)?;
    for (label, prop) in entries.into_iter().filter_map(|f| match f {
        IndexFile::Live { label, prop } => Some((label, prop)),
        IndexFile::Quarantined { .. } => None,
    }) {
        // The open path is the one caller entitled to quarantine: it is about to
        // take ownership of this `(label, prop)` slot, so a later `save()` here
        // is what would overwrite the damaged bytes.  Readers use the
        // non-destructive `load` (#456).
        match sparrowdb_storage::VectorIndex::load_and_quarantine(&dir, &label, &prop) {
            Ok(Some(idx)) => {
                map.insert((label, prop), Arc::new(RwLock::new(idx)));
            }
            // The directory listing said the file was there but `load` found it
            // gone — it was removed between the scan and the read.  Nothing was
            // lost that we can observe, so treat it as absent.
            Ok(None) => continue,
            Err(e) => failures.push(format!(
                "({label}, {prop}) at {}: {e}",
                dir.join(format!("hnsw_{label}_{prop}.bin")).display(),
            )),
        }
    }
    if !failures.is_empty() {
        return Err(Error::Corruption(format!(
            "{} vector index file(s) exist but could not be loaded: {}. \
             Refusing to open: continuing would silently drop every vector write for these \
             indexes and return no results for every search. Move the files aside and re-open \
             to run without them, then rebuild the indexes.",
            failures.len(),
            failures.join("; "),
        )));
    }
    Ok(map)
}

/// One vector index that is not in service, and why.
///
/// `path` names bytes that are actually on disk when the report is handed to
/// you — the machine-readable field, and the one a caller can act on.  `reason`
/// is prose: for a live file it is the loader's own error, for a quarantine
/// artifact it is reconstructed, because #442 records the decode failure only in
/// the `io::Error` it returns at quarantine time and writes no sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexFailure {
    /// Node label the index belongs to.
    pub label: String,
    /// Property the index is built on.
    pub prop: String,
    /// The file these findings are about.  Exact, and it resolves.
    pub path: PathBuf,
    /// Human-readable explanation.  Not machine-parseable; not stable.
    pub reason: String,
}

/// What an inspection of `<db_root>/vector_indexes/` observed.
///
/// The three fields exist because a health report has three distinct things to
/// say and collapsing any two of them has already cost this project an incident:
///
/// * `unscannable` — "I could not look."  Distinct from "I looked and found
///   nothing," which is what the old `Vec` return said in both cases.
/// * `active` — unrecovered damage.  This is what a monitor should alert on.
/// * `historical` — damage that has since been recovered from.  Forensics, not
///   an alarm.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VectorIndexHealth {
    /// `Some(reason)` when `vector_indexes/` could not be listed at all, in
    /// which case `active` and `historical` are empty **for lack of
    /// observation, not lack of damage**.
    ///
    /// A `chmod 000` on the directory, a plain file where the directory belongs,
    /// a dangling symlink, a transient mount failure during a poll: each used to
    /// return an empty vector byte-identical to a genuinely healthy store, so a
    /// monitor could not tell green from blind.  Check this field before
    /// concluding anything from the other two.
    pub unscannable: Option<String>,
    /// Damage that is still unrecovered: the vectors for these pairs are not in
    /// service, and writes to them are being dropped.  Alert on this.
    pub active: Vec<VectorIndexFailure>,
    /// Quarantine artifacts whose `(label, prop)` has since been rebuilt — a
    /// working index now serves the pair, so the artifact is debris.
    ///
    /// Reported separately rather than suppressed because #442 records no reason
    /// at quarantine time, which makes the artifact the only surviving evidence
    /// that the incident happened at all.  A monitor ignores this field; an
    /// operator investigating "when did we lose vectors for this pair?" needs it.
    ///
    /// This is the field that decides whether the report is usable as an alarm.
    /// Artifacts are never cleaned up automatically, so a report that keeps
    /// naming a repaired pair can only ever go from green to red with no path
    /// back — and an alert that never clears after the problem is fixed trains
    /// people to ignore it, which is worse than no alert, because the attention
    /// has been spent and nothing was bought with it.
    pub historical: Vec<VectorIndexFailure>,
}

impl VectorIndexHealth {
    /// True only when the directory was successfully scanned **and** no
    /// unrecovered damage was found.
    ///
    /// An `unscannable` report is never healthy: not knowing is not the same as
    /// being fine.
    pub fn is_healthy(&self) -> bool {
        self.unscannable.is_none() && self.active.is_empty()
    }
}

/// How hard to look.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Depth {
    /// Directory metadata only: names, plus one `stat` per live entry to
    /// confirm it resolves.  No file *contents* are ever read, so the cost is
    /// independent of index size.
    Names,
    /// Additionally deserialise and structurally validate every live index.
    Contents,
}

/// Cheap health report: directory metadata only, no index contents read.
///
/// # Why the cheap tier exists (#456)
///
/// The deep tier fully deserialises every **healthy** index on every call.  For
/// KMSmcp's 8 MB `Knowledge.embedding` index, polled hourly, that is 8 MB of
/// I/O and a complete HNSW graph rebuild an hour spent to confirm that nothing
/// is wrong.  The mutation this issue is named for was a correctness problem in
/// an edge case; this is a permanent cost in the common case, and it is the
/// reason the split holds even for someone who thinks the mutation was
/// acceptable.
///
/// This tier is safe at any poll frequency: zero mutation, zero content I/O.
///
/// # What it can and cannot see
///
/// It reports quarantine artifacts (reconciled against live files, see
/// [`VectorIndexHealth::historical`]) and live entries that do not resolve.  It
/// does **not** verify that a live index still decodes.
///
/// That is the right trade for monitoring a *running* store, and the reason is
/// structural rather than convenient: `open` loads and validates every live
/// index, and refuses to start when one fails.  A store that is up has already
/// had every live file verified, and the residual question a monitor is asking
/// is "did we quarantine something and lose vectors?" — which is exactly what
/// this answers, without reading a byte of payload.
///
/// It cannot tell you why a database refuses to *open*.  `open`'s own error
/// names every damaged file and its reason; [`vector_index_load_failures`]
/// re-derives the same thing without opening.
pub(crate) fn vector_index_health(db_root: &Path) -> VectorIndexHealth {
    health(db_root, Depth::Names)
}

/// Deep health report: everything [`vector_index_health`] reports, plus a real
/// load and structural validation of every live index file.
///
/// This is the "why will this database not open, and which file is it?" call.
/// Use it from an operator's hands, from a failed-start path, or from a
/// deliberate integrity check — not from a poll loop, because it deserialises
/// every healthy index it finds.
///
/// It never returns `Err`, so it can be called on a database that refuses to
/// open.
///
/// # Side effects
///
/// None.  Read-only since #456: live entries are probed with the
/// non-destructive [`sparrowdb_storage::VectorIndex::load`], and the
/// quarantining variant is reserved for the open path.  Two consequences:
///
/// * A health check that runs twice gets the same answer as one that runs once,
///   and neither run changes the store.  It used to quarantine on the first
///   call, so the second call answered differently — and the first call was the
///   one that caused the change.
/// * The `path` of a live-arm entry still resolves when the caller receives it
///   (#455).  It used to name the `.bin` that the same probe had already
///   renamed away, so `exists(path)` — the obvious sanity check, and the one a
///   health check would use to confirm damage is real — was `false` on the very
///   first, most-acted-on report.
///
/// For the record, the pre-#456 mutation was idempotent: a given damaged file
/// was quarantined once and repeat polls did no further I/O.  It destroyed
/// exactly once per incident — silently, from something shaped like a getter.
pub(crate) fn vector_index_load_failures(db_root: &Path) -> VectorIndexHealth {
    health(db_root, Depth::Contents)
}

/// Shared implementation of both tiers.
///
/// Damage is reported in two places because #442 leaves it in two places, and
/// reporting only one of them recreates the very defect this exists to prevent:
///
/// * **Live but not serving** — `hnsw_<label>_<prop>.bin` is present and either
///   does not resolve (a dangling symlink) or, at `Depth::Contents`, does not
///   load.  `open` fails on these.  #442 quarantines *some* of them (bad
///   magic/length/CRC) but deliberately not all: a file that decodes cleanly and
///   then fails `validate_invariants` is left in place for inspection, so it
///   stays visible here as a live entry.
/// * **Quarantined** — `hnsw_<label>_<prop>.bin.corrupt.<millis>`, the bytes a
///   previous load attempt rejected and renamed aside (#442).  `open` succeeds
///   once a file has been quarantined, because the damaged bytes are out of
///   service; without this arm the *only* remaining evidence of the damage would
///   be invisible, and a health check built on this call would report a clean
///   bill on exactly the database whose vectors are gone (#450).
///
/// An artifact is `active` while its pair has nothing serving it and
/// `historical` once something does.  #450 is the case where the artifact is
/// the *only* file for the pair — still `active`, still alarming.  What changed
/// is only the case where a working index sits beside it.
///
/// The limit, stated plainly: neither tier can tell whether a rebuilt index
/// contains everything the artifact held.  Nothing can — the artifact does not
/// decode, which is why it is an artifact.  Completeness after a deliberate
/// rebuild is not a property this call is able to assert, and staying red
/// forever would not have asserted it either.
fn health(db_root: &Path, depth: Depth) -> VectorIndexHealth {
    let dir = db_root.join("vector_indexes");
    let entries = match scan_vector_index_dir(&dir) {
        Ok(entries) => entries,
        Err(reason) => {
            return VectorIndexHealth {
                unscannable: Some(reason),
                ..Default::default()
            }
        }
    };

    // Pass 1 — live entries.  Nothing here renames anything, at either depth.
    let mut serving: HashSet<(String, String)> = HashSet::new();
    let mut active: Vec<VectorIndexFailure> = Vec::new();
    for entry in &entries {
        let IndexFile::Live { label, prop } = entry else {
            continue;
        };
        let path = dir.join(format!("hnsw_{label}_{prop}.bin"));

        // One `stat`, no contents.  `read_dir` listed this name, so the entry is
        // there; `exists` follows symlinks, so a `false` here means the entry
        // resolves to nothing.  Classifying that as "absent" — which is what the
        // loader's old `if !path.exists()` did — is the present-but-treated-as-
        // absent failure #445 exists to kill: the pair silently becomes "no
        // index configured" and every write for it is dropped.
        if !path.exists() {
            active.push(VectorIndexFailure {
                label: label.clone(),
                prop: prop.clone(),
                path,
                reason: "a directory entry with this name exists but does not resolve to a \
                         readable file — most likely a symbolic link whose target has been \
                         removed. It is not an absent index: the pair is configured, and \
                         treating it as unconfigured would silently drop every vector write \
                         for it."
                    .to_owned(),
            });
            continue;
        }

        match depth {
            // Present and resolving is as much as this tier claims.  Enough to
            // reconcile artifacts against, and honest about being no more.
            Depth::Names => {
                serving.insert((label.clone(), prop.clone()));
            }
            Depth::Contents => match sparrowdb_storage::VectorIndex::load(&dir, label, prop) {
                Ok(Some(_)) => {
                    serving.insert((label.clone(), prop.clone()));
                }
                // Listed by the scan but gone by the time we read it — removed
                // underneath us.  Neither serving nor damaged; deliberately not
                // counted as serving, so an artifact for the same pair is still
                // reported as active below.
                Ok(None) => {}
                Err(e) => active.push(VectorIndexFailure {
                    label: label.clone(),
                    prop: prop.clone(),
                    path,
                    reason: e.to_string(),
                }),
            },
        }
    }

    // Pass 2 — artifacts, split on whether the pair has since been rebuilt.
    let mut historical: Vec<VectorIndexFailure> = Vec::new();
    for entry in entries {
        let IndexFile::Quarantined { label, prop, path } = entry else {
            continue;
        };
        let superseded = serving.contains(&(label.clone(), prop.clone()));
        let reason = if superseded {
            format!(
                "index file was rejected by a previous load attempt and preserved as {} \
                 (#442 quarantine). A working index now serves this (label, prop), so the \
                 pair is back in service and these bytes are debris — kept because they are \
                 the only surviving evidence that the incident happened. Whether the \
                 rebuilt index holds everything these bytes held cannot be determined: they \
                 do not decode. Remove the artifact when you are done with it.",
                path.display(),
            )
        } else {
            format!(
                "index file was rejected by a previous load attempt and preserved as {} \
                 (#442 quarantine). The vectors it held are not in service and cannot be \
                 rebuilt from column data; the original decode failure is not recorded on \
                 disk. Recover from these bytes or rebuild the index deliberately, then \
                 remove the artifact to clear this report.",
                path.display(),
            )
        };
        let failure = VectorIndexFailure {
            label,
            prop,
            path,
            reason,
        };
        if superseded {
            historical.push(failure);
        } else {
            active.push(failure);
        }
    }

    // Sorted for determinism: the two passes above would otherwise group all
    // live failures ahead of all artifacts regardless of name.
    let by_name = |a: &VectorIndexFailure, b: &VectorIndexFailure| {
        (&a.label, &a.prop, &a.path).cmp(&(&b.label, &b.prop, &b.path))
    };
    active.sort_by(by_name);
    historical.sort_by(by_name);
    VectorIndexHealth {
        unscannable: None,
        active,
        historical,
    }
}

// ── Storage-size helpers (SPA-171) ────────────────────────────────────────────

pub(crate) fn dir_size_bytes(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            total += dir_size_bytes(&p);
        } else if let Ok(m) = std::fs::metadata(&p) {
            total += m.len();
        }
    }
    total
}

// ── Maintenance helpers ───────────────────────────────────────────────────────

pub(crate) fn collect_maintenance_params(
    catalog: &Catalog,
    node_store: &NodeStore,
    db_root: &Path,
) -> Vec<(u32, u64)> {
    // SPA-185: collect all registered rel table IDs from the catalog instead
    // of hardcoding [0].  This ensures every per-type edge store is checkpointed.
    // Always include table-0 so that any edges written before the catalog had
    // entries (legacy data or pre-SPA-185 databases) are also checkpointed.
    let rel_table_entries = catalog.list_rel_table_ids();
    // Build (rel_table_id, src_label_id, dst_label_id) triples.
    let mut rel_triples: Vec<(u32, Option<u16>, Option<u16>)> = rel_table_entries
        .iter()
        .map(|(id, src, dst, _)| (*id as u32, Some(*src), Some(*dst)))
        .collect();
    // Always include the legacy table-0 slot.  Dedup if already present.
    if !rel_triples.iter().any(|(id, _, _)| *id == 0u32) {
        rel_triples.push((0u32, None, None));
    }

    // Fallback: max HWM across all known labels (for legacy table-0 or when
    // label HWMs are not available from the catalog).
    let global_max_hwm: u64 = catalog
        .list_labels()
        .unwrap_or_default()
        .iter()
        .map(|(label_id, _name)| node_store.hwm_for_label(*label_id as u32).unwrap_or(0))
        .max()
        .unwrap_or(0);

    // For each rel table, compute n_nodes as max(hwm(src_label), hwm(dst_label)).
    // This replaces the old sum-of-all-labels approach that overcounted (#309).
    rel_triples
        .iter()
        .map(|&(rel_id, src_label, dst_label)| {
            // Per-label HWM: max of src and dst label HWMs.
            // Query the node store directly -- labels may not be formally registered
            // in the catalog (e.g. low-level create_node by label_id).
            let hwm_n_nodes = match (src_label, dst_label) {
                (Some(src), Some(dst)) => {
                    let src_hwm = node_store.hwm_for_label(src as u32).unwrap_or(0);
                    let dst_hwm = node_store.hwm_for_label(dst as u32).unwrap_or(0);
                    src_hwm.max(dst_hwm)
                }
                // Legacy table-0 or unknown: use global max.
                _ => global_max_hwm,
            };

            // Also scan this rel table's delta records for the maximum slot index,
            // so the CSR bounds check passes even when edges were inserted without
            // going through the node-store API.
            let delta_max: u64 = EdgeStore::open(db_root, RelTableId(rel_id))
                .ok()
                .and_then(|s| s.read_delta().ok())
                .map(|records| {
                    records
                        .iter()
                        .flat_map(|r| {
                            // Strip label bits -- CSR needs slot indices only.
                            let src_slot = r.src.0 & 0xFFFF_FFFF;
                            let dst_slot = r.dst.0 & 0xFFFF_FFFF;
                            [src_slot, dst_slot].into_iter()
                        })
                        .max()
                        .map(|max_slot| max_slot + 1)
                        .unwrap_or(0)
                })
                .unwrap_or(0);

            let n_nodes = hwm_n_nodes.max(delta_max).max(1);
            (rel_id, n_nodes)
        })
        .collect()
}
