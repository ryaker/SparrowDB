// ── Helper functions ──────────────────────────────────────────────────────────
//
// Utility functions used across multiple submodules.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::col_id_of;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::node_store::{NodeStore, Value};
use std::collections::{HashMap, HashSet};
use std::path::Path;

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
