// ── Intra-batch visibility helpers (SPA-308) ─────────────────────────────────

use crate::helpers::expr_to_value;
use crate::types::PendingOp;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::{col_id_of, NodeId};
use std::collections::{HashMap, HashSet};

/// Collect `NodeId`s from `pending_ops` that match `label_id` and all
/// property filters in `node_props`.
///
/// `pending_ops` stores props as `Vec<(col_id: u32, Value)>`.
/// The prop entries in `node_props` carry key names; we derive the same
/// `col_id` via [`col_id_of`] to make the comparison.
pub(crate) fn pending_candidates_for(
    pending_ops: &[PendingOp],
    label_id: u32,
    node_props: &[sparrowdb_cypher::ast::PropEntry],
) -> Vec<NodeId> {
    pending_ops
        .iter()
        .filter_map(|op| {
            let PendingOp::NodeCreate {
                label_id: op_lid,
                slot,
                props: op_props,
            } = op
            else {
                return None;
            };
            if *op_lid != label_id {
                return None;
            }
            // All pattern props must match a corresponding pending prop.
            let all_match = node_props.iter().all(|pe| {
                let wanted_col = col_id_of(&pe.key);
                let wanted_val = expr_to_value(&pe.value);
                op_props
                    .iter()
                    .any(|&(c, ref v)| c == wanted_col && *v == wanted_val)
            });
            if all_match {
                Some(NodeId((label_id as u64) << 32 | *slot as u64))
            } else {
                None
            }
        })
        .collect()
}

/// Supplement a set of already-matched rows with rows that include
/// nodes created earlier in the same batch (`pending_ops`).
///
/// For each named node variable in `patterns`, the function scans
/// `pending_ops` for `NodeCreate` entries whose label and properties match
/// the pattern.  Any pending candidates not already present in `existing_rows`
/// are cross-joined with on-disk candidates for the other variables to form
/// new rows.
///
/// This is the fix for issue #308: `MATCH...MERGE` statements executed inside
/// `execute_batch` could not see nodes created by earlier `CREATE` statements
/// in the same batch because the Engine only reads committed on-disk state.
pub(crate) fn augment_rows_with_pending(
    patterns: &[sparrowdb_cypher::ast::PathPattern],
    pending_ops: &[PendingOp],
    catalog: &Catalog,
    existing_rows: &[HashMap<String, NodeId>],
) -> crate::Result<Vec<HashMap<String, NodeId>>> {
    // Collect the named node variables present in the patterns.
    // Patterns with relationships (multi-node path patterns) are not yet
    // augmented — only independent node-only patterns are handled here.
    // That covers the most common batch scenario:
    //   CREATE (:A {k:1})
    //   CREATE (:B {k:1})
    //   MATCH (a:A {k:1}), (b:B {k:1}) MERGE (a)-[:R]->(b)
    let mut var_candidates: HashMap<String, Vec<NodeId>> = HashMap::new();
    for pat in patterns {
        // Only process simple node-only patterns (no relationships in the path).
        if pat.rels.is_empty() {
            for node_pat in &pat.nodes {
                if node_pat.var.is_empty() {
                    continue;
                }
                if var_candidates.contains_key(&node_pat.var) {
                    continue;
                }
                let label = node_pat.labels.first().cloned().unwrap_or_default();
                let label_id: u32 = match catalog.get_label(&label)? {
                    Some(id) => id as u32,
                    None => {
                        // Label not registered at all — no pending nodes either.
                        var_candidates.insert(node_pat.var.clone(), vec![]);
                        continue;
                    }
                };
                let pending = pending_candidates_for(pending_ops, label_id, &node_pat.props);
                var_candidates.insert(node_pat.var.clone(), pending);
            }
        }
    }

    if var_candidates.is_empty() {
        return Ok(vec![]);
    }

    // Collect the set of NodeIds already present in existing_rows for each
    // variable so we can skip duplicates.
    let mut already_seen: HashMap<String, HashSet<NodeId>> = HashMap::new();
    for row in existing_rows {
        for (var, nid) in row {
            already_seen.entry(var.clone()).or_default().insert(*nid);
        }
    }

    // Build new rows: cross-join the per-variable pending candidates,
    // but only include a row if at least one variable has a pending candidate
    // not already seen in existing_rows.  This avoids duplicating rows that
    // the engine already returned.
    let vars: Vec<String> = var_candidates.keys().cloned().collect();
    let mut new_rows: Vec<HashMap<String, NodeId>> = vec![HashMap::new()];

    for var in &vars {
        let candidates = var_candidates.get(var).map(Vec::as_slice).unwrap_or(&[]);
        if candidates.is_empty() {
            // No pending candidates for this variable; keep current partial rows
            // alive by not expanding (they will be filtered below).
            continue;
        }
        let mut expanded: Vec<HashMap<String, NodeId>> = Vec::new();
        for partial in &new_rows {
            for &cand in candidates {
                let mut row = partial.clone();
                row.insert(var.clone(), cand);
                expanded.push(row);
            }
        }
        new_rows = expanded;
    }

    // Filter out rows that are fully contained in existing_rows (all variables
    // match a node already seen from the on-disk scan).
    let added: Vec<HashMap<String, NodeId>> = new_rows
        .into_iter()
        .filter(|row| {
            if row.is_empty() {
                return false;
            }
            // The row is "new" if ANY of its node assignments is a pending node
            // not already present in existing_rows for that variable.
            row.iter().any(|(var, nid)| {
                !already_seen
                    .get(var)
                    .map(|s| s.contains(nid))
                    .unwrap_or(false)
            })
        })
        .collect();

    Ok(added)
}
