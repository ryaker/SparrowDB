// ── WriteTx ───────────────────────────────────────────────────────────────────

use crate::helpers::fnv1a_col_id;
use crate::types::{
    DbInner, PendingOp, StagedUpdate, WalMutation, WriteBuffer, WriteGuard, GC_COMMIT_INTERVAL,
};
use crate::wal_codec::write_mutation_wal;
use sparrowdb_catalog::catalog::{Catalog, LabelId, RelTableId as CatalogRelTableId};
use sparrowdb_common::{col_id_of, EdgeId, NodeId, TxnId};
use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};
use sparrowdb_storage::node_store::{NodeStore, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// A write transaction.
///
/// Only one may be active at a time (writer-lock held for the lifetime of
/// this struct).  Commit by calling [`WriteTx::commit`]; uncommitted changes
/// are discarded on drop.
///
/// # Atomicity guarantee (SPA-181)
///
/// All mutations (`create_node`, `create_edge`, `delete_node`, `create_label`,
/// `merge_node`) are buffered in memory until [`commit`] is called.  If the
/// transaction is dropped without committing, **no changes are persisted** —
/// the database remains in the state it was at the time [`begin_write`] was
/// called.
///
/// [`begin_write`]: crate::GraphDb::begin_write
/// [`commit`]: WriteTx::commit
#[must_use = "call commit() to persist changes, or drop to discard"]
pub struct WriteTx {
    pub(crate) inner: Arc<DbInner>,
    pub(crate) store: NodeStore,
    pub(crate) catalog: Catalog,
    /// Staged property updates (not yet visible to readers).
    pub(crate) write_buf: WriteBuffer,
    /// Staged WAL mutation records to emit on commit.
    pub(crate) wal_mutations: Vec<WalMutation>,
    /// Set of node IDs written by this transaction (for MVCC conflict detection).
    pub(crate) dirty_nodes: HashSet<u64>,
    /// The committed txn_id at the time this WriteTx was opened (MVCC snapshot).
    pub(crate) snapshot_txn_id: u64,
    /// Held for the lifetime of this WriteTx; released on drop.
    /// Uses AtomicBool-based guard — no unsafe lifetime extension needed (SPA-181).
    pub(crate) _guard: WriteGuard,
    pub(crate) committed: bool,
    /// In-flight fulltext index updates — flushed to disk on commit.
    ///
    /// Caching open indexes here avoids one open+flush per `add_to_fulltext_index`
    /// call; instead we batch all additions and flush each index exactly once.
    pub(crate) fulltext_pending: HashMap<String, sparrowdb_storage::fulltext_index::FulltextIndex>,
    /// Buffered structural mutations (create_node, delete_node, create_edge,
    /// create_label) not yet written to disk.  Flushed atomically on commit.
    pub(crate) pending_ops: Vec<PendingOp>,
    /// Label ids that were staged in-memory during this transaction and must be
    /// flushed to the catalog TLV file on commit (closes #305).
    pub(crate) pending_label_creates: Vec<LabelId>,
    /// Rel-table ids that were staged in-memory during this transaction and must
    /// be flushed to the catalog TLV file on commit (closes #305).
    pub(crate) pending_rel_type_creates: Vec<CatalogRelTableId>,
}

impl WriteTx {
    // ── Core node/property API (pre-Phase 7) ─────────────────────────────────

    /// Create a new node under `label_id` with the given properties.
    ///
    /// Returns the packed [`NodeId`].
    ///
    /// The node is **not** written to disk until [`commit`] is called.
    /// Dropping the transaction without committing discards this operation.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn create_node(&mut self, label_id: u32, props: &[(u32, Value)]) -> crate::Result<NodeId> {
        // Allocate a node ID by consulting the in-memory HWM.  We peek at
        // the current HWM to compute the future NodeId without actually
        // writing anything to disk yet.
        let slot = self.store.peek_next_slot(label_id)?;
        let node_id = NodeId((label_id as u64) << 32 | slot as u64);
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeCreate {
            label_id,
            slot,
            props: props.to_vec(),
        });
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props: props.to_vec(),
            // Low-level create_node: no property names available.
            prop_names: Vec::new(),
        });
        Ok(node_id)
    }

    /// Create a new node with named properties, recording names in the WAL.
    ///
    /// Like [`create_node`] but accepts `(name, value)` pairs so that the
    /// property names are preserved in the WAL record for schema introspection
    /// (`CALL db.schema()`).  Col-ids are derived via [`col_id_of`].
    pub fn create_node_named(
        &mut self,
        label_id: u32,
        named_props: &[(String, Value)],
    ) -> crate::Result<NodeId> {
        let props: Vec<(u32, Value)> = named_props
            .iter()
            .map(|(name, v)| (col_id_of(name), v.clone()))
            .collect();
        let prop_names: Vec<String> = named_props.iter().map(|(n, _)| n.clone()).collect();
        let slot = self.store.peek_next_slot(label_id)?;
        let node_id = NodeId((label_id as u64) << 32 | slot as u64);
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeCreate {
            label_id,
            slot,
            props: props.clone(),
        });
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props,
            prop_names,
        });
        Ok(node_id)
    }

    /// Stage a property update.
    ///
    /// The new value is not visible to readers until [`commit`] is called.
    ///
    /// On the first update to a `(node_id, col_id)` key during this
    /// transaction, the current on-disk value is read and stored as the
    /// before-image so that readers with older snapshots continue to see the
    /// correct value after commit overwrites the column file.
    pub fn set_node_col(&mut self, node_id: NodeId, col_id: u32, value: Value) {
        self.set_node_col_named(node_id, col_id, format!("col_{col_id}"), value);
    }

    /// Stage a property update with an explicit human-readable key name for WAL.
    pub(crate) fn set_node_col_named(
        &mut self,
        node_id: NodeId,
        col_id: u32,
        key_name: String,
        value: Value,
    ) {
        let key = (node_id.0, col_id);
        self.dirty_nodes.insert(node_id.0);

        if self.write_buf.updates.contains_key(&key) {
            // Already staged this key — just update the new_value.
            let entry = self.write_buf.updates.get_mut(&key).unwrap();
            entry.new_value = value;
            // Keep the key_name from the first staging (it's the same column).
            return;
        }

        // First update to this key in this transaction.  Capture the
        // before-image so readers pinned before our commit retain access.
        let prev_txn_id = self.inner.current_txn_id.load(Ordering::Acquire);

        // Check whether the version chain already has an entry for this key.
        // If so, the chain is already correct and we don't need to add the
        // before-image separately.
        let already_in_chain = {
            let vs = self.inner.versions.read().expect("version lock");
            vs.map.contains_key(&key)
        };

        let before_image = if already_in_chain {
            None
        } else {
            // Read the current on-disk value as the before-image.
            let disk_val = self
                .store
                .get_node_raw(node_id, &[col_id])
                .ok()
                .and_then(|mut v| v.pop())
                .map(|(_, raw)| self.store.decode_raw_value(raw));
            disk_val.map(|v| (prev_txn_id, v))
        };

        self.write_buf.updates.insert(
            key,
            StagedUpdate {
                before_image,
                new_value: value,
                key_name,
            },
        );
    }

    /// Create a label in the schema catalog.
    ///
    /// The label is staged in memory and only written to the catalog file
    /// when [`commit`] is called.  Dropping the transaction without committing
    /// discards the label — no ghost entries are left on disk (closes #305).
    ///
    /// Returns `Err(AlreadyExists)` if a label with that name already exists.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn create_label(&mut self, name: &str) -> crate::Result<u16> {
        let id = self.catalog.stage_label(name)?;
        self.pending_label_creates.push(id);
        Ok(id)
    }

    /// Look up `name` in the catalog, creating it if it does not yet exist.
    ///
    /// Returns the `label_id` as a `u32` (upper 32 bits of a packed NodeId).
    /// Unlike [`create_label`], this method is idempotent: calling it multiple
    /// times with the same name always returns the same id.
    ///
    /// New labels are staged in memory and written to disk only on [`commit`].
    ///
    /// Primarily used by the bulk-import path (SPA-148) where labels may be
    /// seen for the first time on any row.
    ///
    /// [`create_label`]: WriteTx::create_label
    /// [`commit`]: WriteTx::commit
    pub fn get_or_create_label_id(&mut self, name: &str) -> crate::Result<u32> {
        match self.catalog.get_label(name)? {
            Some(id) => Ok(id as u32),
            None => {
                let id = self.catalog.stage_label(name)?;
                self.pending_label_creates.push(id);
                Ok(id as u32)
            }
        }
    }

    // ── Phase 7 mutation API (SPA-123 … SPA-126) ─────────────────────────────

    /// SPA-123: Find or create a node matching `label` + `props`.
    ///
    /// Scans the node store for a slot whose columns match every key→value
    /// pair in `props` (using [`fnv1a_col_id`] to derive column IDs from
    /// key strings).  Returns the existing [`NodeId`] if found, or creates a
    /// new node and returns the new id.
    ///
    /// The label is resolved (or created) in the catalog.
    pub fn merge_node(
        &mut self,
        label: &str,
        props: HashMap<String, Value>,
    ) -> crate::Result<NodeId> {
        // Resolve / create label (staged — not written to disk until commit).
        let label_id: u32 = match self.catalog.get_label(label)? {
            Some(id) => id as u32,
            None => {
                let id = self.catalog.stage_label(label)?;
                self.pending_label_creates.push(id);
                id as u32
            }
        };

        // Build col list from props keys.
        let col_kv: Vec<(String, u32, Value)> = props
            .into_iter()
            .map(|(k, v)| {
                let col_id = fnv1a_col_id(&k);
                (k, col_id, v)
            })
            .collect();
        let col_ids: Vec<u32> = col_kv.iter().map(|&(_, col_id, _)| col_id).collect();

        // First, check buffered (not-yet-committed) node creates in pending_ops.
        // This ensures merge_node is idempotent within a single transaction.
        for op in &self.pending_ops {
            if let PendingOp::NodeCreate {
                label_id: op_label_id,
                slot: op_slot,
                props: op_props,
            } = op
            {
                if *op_label_id == label_id {
                    let candidate = NodeId((label_id as u64) << 32 | *op_slot as u64);
                    let matches = col_kv.iter().all(|(_, col_id, want_val)| {
                        op_props
                            .iter()
                            .find(|&&(c, _)| c == *col_id)
                            // Compare in-memory Value objects directly so long
                            // strings (> 7 bytes) are not truncated (SPA-212).
                            .map(|(_, v)| v == want_val)
                            .unwrap_or(false)
                    });
                    if matches {
                        return Ok(candidate);
                    }
                }
            }
        }

        // Scan on-disk slots for a match (only checks committed/on-disk nodes).
        // Use disk_hwm_for_label to avoid scanning slots that were only reserved
        // in-memory by peek_next_slot but not yet flushed to disk.
        let disk_hwm = self.store.disk_hwm_for_label(label_id)?;
        for slot in 0..disk_hwm {
            let candidate = NodeId((label_id as u64) << 32 | slot);
            if let Ok(stored) = self.store.get_node_raw(candidate, &col_ids) {
                let matches = col_kv.iter().all(|(_, col_id, want_val)| {
                    stored
                        .iter()
                        .find(|&&(c, _)| c == *col_id)
                        .map(|&(_, raw)| {
                            // Compare decoded values so overflow strings (> 7 bytes)
                            // match correctly (SPA-212).
                            self.store.decode_raw_value(raw) == *want_val
                        })
                        .unwrap_or(false)
                });
                if matches {
                    return Ok(candidate);
                }
            }
        }

        // Not found — create a new node (buffered, same as create_node).
        let disk_props: Vec<(u32, Value)> = col_kv
            .iter()
            .map(|(_, col_id, v)| (*col_id, v.clone()))
            .collect();
        // Preserve property names from the col_kv tuples (key, col_id, value).
        let disk_prop_names: Vec<String> = col_kv.iter().map(|(k, _, _)| k.clone()).collect();
        let slot = self.store.peek_next_slot(label_id)?;
        let node_id = NodeId((label_id as u64) << 32 | slot as u64);
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeCreate {
            label_id,
            slot,
            props: disk_props.clone(),
        });
        self.wal_mutations.push(WalMutation::NodeCreate {
            node_id,
            label_id,
            props: disk_props,
            prop_names: disk_prop_names,
        });
        Ok(node_id)
    }

    /// SPA-247: Upsert a node — find an existing node with `(label, match_key=match_value)`
    /// and update its properties, or create a new one if none exists.
    ///
    /// Returns `(NodeId, created)` where `created` is `true` when a new node
    /// was inserted and `false` when an existing node was found and updated.
    ///
    /// The lookup scans both pending (in-transaction) nodes and committed
    /// on-disk nodes for a slot whose label matches and whose `match_key`
    /// column equals `match_value`.  On a hit the remaining `properties` are
    /// applied via [`set_property`]; on a miss a new node is created via
    /// [`merge_node`] with `match_key=match_value` merged into `properties`.
    pub fn merge_node_by_property(
        &mut self,
        label: &str,
        match_key: &str,
        match_value: &Value,
        properties: HashMap<String, Value>,
    ) -> crate::Result<(NodeId, bool)> {
        let label_id: u32 = match self.catalog.get_label(label)? {
            Some(id) => id as u32,
            None => {
                // Label doesn't exist yet — no node can match. Create it.
                let mut full_props = properties;
                full_props.insert(match_key.to_string(), match_value.clone());
                let node_id = self.merge_node(label, full_props)?;
                return Ok((node_id, true));
            }
        };

        let match_col_id = fnv1a_col_id(match_key);
        let match_col_ids = vec![match_col_id];

        // Step 1: Check pending (in-transaction) nodes.
        for op in &self.pending_ops {
            if let PendingOp::NodeCreate {
                label_id: op_label_id,
                slot: op_slot,
                props: op_props,
            } = op
            {
                if *op_label_id == label_id {
                    let matches = op_props
                        .iter()
                        .find(|&&(c, _)| c == match_col_id)
                        .map(|(_, v)| v == match_value)
                        .unwrap_or(false);
                    if matches {
                        let node_id = NodeId((label_id as u64) << 32 | *op_slot as u64);
                        // Update remaining properties on the existing node.
                        for (k, v) in &properties {
                            self.set_property(node_id, k, v.clone())?;
                        }
                        return Ok((node_id, false));
                    }
                }
            }
        }

        // Step 2: Scan on-disk committed nodes.
        let disk_hwm = self.store.disk_hwm_for_label(label_id)?;
        for slot in 0..disk_hwm {
            let candidate = NodeId((label_id as u64) << 32 | slot);
            if let Ok(stored) = self.store.get_node_raw(candidate, &match_col_ids) {
                let matches = stored
                    .iter()
                    .find(|&&(c, _)| c == match_col_id)
                    .map(|&(_, raw)| self.store.decode_raw_value(raw) == *match_value)
                    .unwrap_or(false);
                if matches {
                    // Update remaining properties on the existing node.
                    for (k, v) in &properties {
                        self.set_property(candidate, k, v.clone())?;
                    }
                    return Ok((candidate, false));
                }
            }
        }

        // Step 3: Not found — create new node with match_key included.
        let mut full_props = properties;
        full_props.insert(match_key.to_string(), match_value.clone());
        let node_id = self.merge_node(label, full_props)?;
        Ok((node_id, true))
    }

    /// SPA-124: Update a named property on a node.
    ///
    /// Derives a stable column ID from `key` via [`fnv1a_col_id`] and stages
    /// the update through [`set_node_col`] (which records the before-image in
    /// the write buffer).  WAL emission happens once at commit time via the
    /// `updates` loop in `write_mutation_wal`.
    pub fn set_property(&mut self, node_id: NodeId, key: &str, val: Value) -> crate::Result<()> {
        let col_id = fnv1a_col_id(key);
        self.dirty_nodes.insert(node_id.0);

        // Stage the update through the write buffer (records before-image for
        // WAL and MVCC). WAL emission happens exactly once at commit time via
        // the `updates` loop in `write_mutation_wal`.  Pass the human-readable
        // key name so the WAL record carries it (not the synthesized col_{id}).
        self.set_node_col_named(node_id, col_id, key.to_string(), val);

        Ok(())
    }

    /// SPA-125: Delete a node, with edge-attachment check.
    ///
    /// Returns [`Error::NodeHasEdges`] if the node is referenced by any edge
    /// in the delta log or checkpointed CSR files (SPA-304).  On success,
    /// queues a `NodeDelete` WAL record and
    /// buffers the tombstone write; the on-disk tombstone is only applied when
    /// [`commit`] is called.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn delete_node(&mut self, node_id: NodeId) -> crate::Result<()> {
        // SPA-185: check ALL per-type delta logs for attached edges, not just
        // the hardcoded RelTableId(0).  Always include table-0 so that any
        // edges written before the catalog had entries are still detected.
        let rel_entries = self.catalog.list_rel_table_ids();
        let mut rel_ids_to_check: Vec<u32> =
            rel_entries.iter().map(|(id, _, _, _)| *id as u32).collect();
        // Always include the legacy table-0 slot.  If it is already in the
        // catalog list this dedup prevents a double-read.
        if !rel_ids_to_check.contains(&0u32) {
            rel_ids_to_check.push(0u32);
        }
        for rel_id in rel_ids_to_check {
            let store = EdgeStore::open(&self.inner.path, RelTableId(rel_id));

            // Check delta log (un-checkpointed edges).
            if let Ok(ref s) = store {
                let delta = s.read_delta().unwrap_or_default();
                if delta.iter().any(|r| r.src == node_id || r.dst == node_id) {
                    return Err(sparrowdb_common::Error::NodeHasEdges { node_id: node_id.0 });
                }
            }

            // SPA-304: Check CSR forward file — the node may be a *source* of
            // checkpointed edges that are no longer in the delta log.
            if let Ok(ref s) = store {
                if let Ok(csr) = s.open_fwd() {
                    if !csr.neighbors(node_id.0).is_empty() {
                        return Err(sparrowdb_common::Error::NodeHasEdges { node_id: node_id.0 });
                    }
                }
            }

            // SPA-304: Check CSR backward file — the node may be a *destination*
            // of checkpointed edges.
            if let Ok(ref s) = store {
                if let Ok(csr) = s.open_bwd() {
                    if !csr.predecessors(node_id.0).is_empty() {
                        return Err(sparrowdb_common::Error::NodeHasEdges { node_id: node_id.0 });
                    }
                }
            }
        }

        // Also check buffered (not-yet-committed) edge creates in this
        // transaction — a node that has already been connected in the current
        // transaction cannot be deleted before commit.
        let has_buffered_edge = self.pending_ops.iter().any(|op| {
            matches!(op, PendingOp::EdgeCreate { src, dst, .. } if *src == node_id || *dst == node_id)
        });
        if has_buffered_edge {
            return Err(sparrowdb_common::Error::NodeHasEdges { node_id: node_id.0 });
        }

        // Buffer the tombstone — do NOT write to disk yet.
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeDelete { node_id });
        self.wal_mutations.push(WalMutation::NodeDelete { node_id });
        Ok(())
    }

    /// `DETACH DELETE` variant: first delete all edges incident to `node_id`
    /// (both outgoing and incoming, across all relationship types), then delete
    /// the node itself.
    ///
    /// This mirrors the semantics of Neo4j's `DETACH DELETE`: the caller does not
    /// need to explicitly remove edges before removing the node.
    ///
    /// Unlike calling `delete_node` directly, this method schedules all
    /// `EdgeDelete` ops in the same transaction and then queues the
    /// `NodeDelete` op without re-running the edge-presence safety check
    /// (which reads on-disk state and would still see the not-yet-committed
    /// edge deletions).
    pub fn detach_delete_node(&mut self, node_id: NodeId) -> crate::Result<()> {
        // Collect all (src, dst, rel_type) tuples for edges incident to node_id.
        let rel_entries = self.catalog.list_rel_table_ids();
        let mut rel_ids_to_check: Vec<(u32, String)> = rel_entries
            .iter()
            .map(|(id, _, _, rel_type)| (*id as u32, rel_type.clone()))
            .collect();
        // Always check the legacy table-0 slot (edges written before catalog entries existed).
        if !rel_ids_to_check.iter().any(|(id, _)| *id == 0u32) {
            rel_ids_to_check.push((0u32, String::new()));
        }

        let mut edges_to_delete: Vec<(NodeId, NodeId, String)> = Vec::new();

        for (rel_id, rel_type) in &rel_ids_to_check {
            if rel_type.is_empty() {
                // Skip the legacy-0 slot when there is no known type name.
                continue;
            }
            let store = EdgeStore::open(&self.inner.path, RelTableId(*rel_id));
            let Ok(ref s) = store else { continue };

            // Collect incident edges from the delta log (un-checkpointed).
            if let Ok(delta) = s.read_delta() {
                for rec in &delta {
                    if rec.src == node_id || rec.dst == node_id {
                        edges_to_delete.push((rec.src, rec.dst, rel_type.clone()));
                    }
                }
            }

            // Collect outgoing edges from the checkpointed CSR forward file.
            if let Ok(csr) = s.open_fwd() {
                for &dst_raw in csr.neighbors(node_id.0) {
                    edges_to_delete.push((node_id, NodeId(dst_raw), rel_type.clone()));
                }
            }

            // Collect incoming edges from the checkpointed CSR backward file.
            if let Ok(csr) = s.open_bwd() {
                for &src_raw in csr.predecessors(node_id.0) {
                    edges_to_delete.push((NodeId(src_raw), node_id, rel_type.clone()));
                }
            }
        }

        // Also cancel any buffered (not-yet-committed) EdgeCreate ops in this
        // transaction that touch node_id.
        let buffered: Vec<(NodeId, NodeId, String)> = self
            .pending_ops
            .iter()
            .filter_map(|op| {
                if let PendingOp::EdgeCreate {
                    src,
                    dst,
                    rel_table_id,
                    ..
                } = op
                {
                    if *src == node_id || *dst == node_id {
                        let rt = rel_entries
                            .iter()
                            .find(|(id, _, _, _)| *id as u32 == rel_table_id.0)
                            .map(|(_, _, _, rt)| rt.clone())
                            .unwrap_or_default();
                        if !rt.is_empty() {
                            return Some((*src, *dst, rt));
                        }
                    }
                }
                None
            })
            .collect();
        edges_to_delete.extend(buffered);

        // Deduplicate to avoid scheduling redundant edge deletions.
        edges_to_delete.sort_unstable();
        edges_to_delete.dedup();

        // Schedule all edge deletions.  `delete_edge` will either cancel a
        // buffered EdgeCreate or queue an EdgeDelete op for commit time.
        for (src, dst, rel_type) in edges_to_delete {
            self.delete_edge(src, dst, &rel_type)?;
        }

        // Directly queue the NodeDelete op.  We bypass `delete_node`'s
        // edge-presence safety check because: (a) we have scheduled EdgeDelete
        // ops for every incident edge above, and (b) those ops have not been
        // committed to disk yet — so reading on-disk state would still see the
        // edges and incorrectly return NodeHasEdges.
        self.dirty_nodes.insert(node_id.0);
        self.pending_ops.push(PendingOp::NodeDelete { node_id });
        self.wal_mutations.push(WalMutation::NodeDelete { node_id });
        Ok(())
    }

    /// SPA-126: Create a directed edge `src → dst` with the given type.
    ///
    /// Buffers the edge creation; the delta-log append and WAL record are only
    /// written to disk when [`commit`] is called.  If the transaction is dropped
    /// without committing, no edge is persisted.
    ///
    /// Registers the relationship type name in the catalog so that queries
    /// like `MATCH (a)-[:REL]->(b)` can resolve the type (SPA-158).
    /// Returns the new [`EdgeId`].
    ///
    /// [`commit`]: WriteTx::commit
    pub fn create_edge(
        &mut self,
        src: NodeId,
        dst: NodeId,
        rel_type: &str,
        props: HashMap<String, Value>,
    ) -> crate::Result<EdgeId> {
        // Derive label IDs from the packed node IDs (upper 32 bits).
        let src_label_id = (src.0 >> 32) as u16;
        let dst_label_id = (dst.0 >> 32) as u16;

        // Register (or retrieve) the rel type in the catalog.
        // New entries are staged in memory and only written to disk on commit
        // so that a dropped transaction leaves no ghost rel-type entries (closes #305).
        let (catalog_rel_id, is_new_rel_type) =
            self.catalog
                .stage_rel_table(src_label_id, dst_label_id, rel_type)?;
        if is_new_rel_type {
            self.pending_rel_type_creates.push(catalog_rel_id);
        }
        let rel_table_id = RelTableId(catalog_rel_id as u32);

        // Compute the edge ID from the on-disk delta log size, offset by the
        // number of edges already buffered in this transaction for the same
        // rel_table_id.  Without this offset, multiple create_edge calls in the
        // same transaction would all derive the same on-disk base and collide.
        let base_edge_id = EdgeStore::peek_next_edge_id(&self.inner.path, rel_table_id)?;
        let buffered_count = self
            .pending_ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    PendingOp::EdgeCreate {
                        rel_table_id: pending_rel_table_id,
                        ..
                    } if *pending_rel_table_id == rel_table_id
                )
            })
            .count() as u64;
        let edge_id = EdgeId(base_edge_id.0 + buffered_count);

        // Convert HashMap<String, Value> props to (col_id, value_u64) pairs
        // using the canonical FNV-1a col_id derivation so read and write agree.
        // SPA-229: use NodeStore::encode_value (not val.to_u64()) so that
        // Value::Float is stored via f64::to_bits() in the heap rather than
        // panicking with "cannot be inline-encoded".
        let encoded_props: Vec<(u32, u64)> = props
            .iter()
            .map(|(name, val)| -> crate::Result<(u32, u64)> {
                Ok((col_id_of(name), self.store.encode_value(val)?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Human-readable entries for WAL schema introspection.
        let prop_entries: Vec<(String, Value)> = props.into_iter().collect();

        // Buffer the edge append — do NOT write to disk yet.
        self.pending_ops.push(PendingOp::EdgeCreate {
            src,
            dst,
            rel_table_id,
            props: encoded_props,
        });
        self.wal_mutations.push(WalMutation::EdgeCreate {
            edge_id,
            src,
            dst,
            rel_type: rel_type.to_string(),
            prop_entries,
        });
        Ok(edge_id)
    }

    /// Delete the directed edge `src → dst` with the given relationship type.
    ///
    /// Resolves the relationship type to a `RelTableId` via the catalog, then
    /// buffers an `EdgeDelete` operation.  At commit time the edge is excised
    /// from the on-disk delta log.
    ///
    /// Returns [`Error::InvalidArgument`] if the relationship type is not
    /// registered in the catalog, or if no matching edge record exists in the
    /// delta log for the resolved table.
    ///
    /// Unblocks `SparrowOntology::init(force=true)` which needs to remove all
    /// existing edges before re-seeding the ontology graph.
    ///
    /// [`commit`]: WriteTx::commit
    pub fn delete_edge(&mut self, src: NodeId, dst: NodeId, rel_type: &str) -> crate::Result<()> {
        let src_label_id = (src.0 >> 32) as u16;
        let dst_label_id = (dst.0 >> 32) as u16;

        // Resolve the rel type in the catalog.  We do not create a new entry —
        // deleting a non-existent rel type is always an error.
        // The catalog's RelTableId is u64; EdgeStore's is RelTableId(u32).
        let catalog_rel_id = self
            .catalog
            .get_rel_table(src_label_id, dst_label_id, rel_type)?
            .ok_or_else(|| {
                sparrowdb_common::Error::InvalidArgument(format!(
                    "relationship type '{}' not found in catalog for labels ({src_label_id}, {dst_label_id})",
                    rel_type
                ))
            })?;
        let rel_table_id = RelTableId(catalog_rel_id as u32);

        // If the edge was created in this same transaction (not yet committed),
        // cancel the create rather than scheduling a delete.
        let buffered_pos = self.pending_ops.iter().position(|op| {
            matches!(
                op,
                PendingOp::EdgeCreate {
                    src: os,
                    dst: od,
                    rel_table_id: ort,
                    ..
                } if *os == src && *od == dst && *ort == rel_table_id
            )
        });

        if let Some(pos) = buffered_pos {
            // Remove the buffered create and its corresponding WAL mutation.
            self.pending_ops.remove(pos);
            // The WalMutation vec is parallel to pending_ops only for structural
            // ops; find and remove the matching EdgeCreate entry.
            if let Some(wpos) = self.wal_mutations.iter().position(|m| {
                matches!(m, WalMutation::EdgeCreate { src: ms, dst: md, .. } if *ms == src && *md == dst)
            }) {
                self.wal_mutations.remove(wpos);
            }
            return Ok(());
        }

        // Edge is on disk — schedule the deletion for commit time.
        self.pending_ops.push(PendingOp::EdgeDelete {
            src,
            dst,
            rel_table_id,
        });
        self.wal_mutations.push(WalMutation::EdgeDelete {
            src,
            dst,
            rel_type: rel_type.to_string(),
        });
        Ok(())
    }

    // ── MVCC conflict detection (SPA-128) ────────────────────────────────────

    /// Check for write-write conflicts before committing.
    ///
    /// A conflict is detected when another `WriteTx` has committed a change
    /// to a node that this transaction also dirtied, at a `txn_id` greater
    /// than our snapshot.
    fn detect_conflicts(&self) -> crate::Result<()> {
        let nv = self.inner.node_versions.read().expect("node_versions lock");
        for &raw in &self.dirty_nodes {
            let last_write = nv.get(NodeId(raw));
            if last_write > self.snapshot_txn_id {
                return Err(sparrowdb_common::Error::WriteWriteConflict { node_id: raw });
            }
        }
        Ok(())
    }

    // ── Full-text index maintenance ──────────────────────────────────────────

    /// Add a node document to a named full-text index.
    ///
    /// Call after creating or updating a node to keep the index current.
    /// The `text` should be the concatenated string value(s) of the indexed
    /// properties.  Changes are flushed to disk immediately (no WAL for v1).
    ///
    /// # Example
    /// ```no_run
    /// # use sparrowdb::GraphDb;
    /// # use sparrowdb_common::NodeId;
    /// # let db = GraphDb::open(std::path::Path::new("/tmp/test")).unwrap();
    /// # let mut tx = db.begin_write().unwrap();
    /// # let node_id = NodeId(0);
    /// tx.add_to_fulltext_index("searchIndex", node_id, "some searchable text")?;
    /// # Ok::<(), sparrowdb_common::Error>(())
    /// ```
    pub fn add_to_fulltext_index(
        &mut self,
        index_name: &str,
        node_id: NodeId,
        text: &str,
    ) -> crate::Result<()> {
        use sparrowdb_storage::fulltext_index::FulltextIndex;
        // Lazily open the index and cache it for the lifetime of this
        // transaction.  All additions are batched and flushed once on commit,
        // avoiding an open+flush round-trip per document.
        let idx = match self.fulltext_pending.get_mut(index_name) {
            Some(existing) => existing,
            None => {
                let opened = FulltextIndex::open(&self.inner.path, index_name)?;
                self.fulltext_pending.insert(index_name.to_owned(), opened);
                self.fulltext_pending.get_mut(index_name).unwrap()
            }
        };
        idx.add_document(node_id.0, text);
        Ok(())
    }

    // ── Commit ───────────────────────────────────────────────────────────────

    /// Commit the transaction.
    ///
    /// WAL-first protocol (SPA-184):
    ///
    /// 1. Detects write-write conflicts (SPA-128).
    /// 2. Drains staged property updates (does NOT apply to disk yet).
    /// 3. Atomically increments the global `current_txn_id` to obtain the new
    ///    transaction ID that will label all WAL records.
    /// 4. **Writes WAL records and fsyncs** (Begin + all structural mutations +
    ///    all property updates + Commit) so that the intent is durable before
    ///    any data page is touched (SPA-184).
    /// 5. Applies all buffered structural operations to storage (SPA-181):
    ///    node creates, node deletes, edge creates.
    /// 6. Flushes all staged `set_node_col` updates to disk.
    /// 7. Records before-images in the version chain at the previous `txn_id`,
    ///    preserving snapshot access for currently-open readers.
    /// 8. Records the new values in the version chain at the new `txn_id`.
    /// 9. Updates per-node version table for future conflict detection.
    #[must_use = "check the Result; a failed commit means nothing was written"]
    pub fn commit(mut self) -> crate::Result<TxnId> {
        // Step 1: MVCC conflict abort (SPA-128).
        self.detect_conflicts()?;

        // Step 2: Drain staged property updates — collect but do NOT write to
        // disk yet.  We need them in hand to emit WAL records before touching
        // any data page.
        let updates: Vec<((u64, u32), StagedUpdate)> = self.write_buf.updates.drain().collect();

        // Step 3: Increment txn_id with Release ordering.  We need the ID now
        // so that WAL records (emitted next) carry the correct txn_id.
        let new_id = self.inner.current_txn_id.fetch_add(1, Ordering::Release) + 1;

        // Step 4: WAL-first — write all mutation records and fsync before
        // touching any data page (SPA-184).  A crash between here and the end
        // of Step 6 is recoverable: WAL replay will re-apply the ops.
        write_mutation_wal(
            &self.inner.wal_writer,
            new_id,
            &updates,
            &self.wal_mutations,
        )?;

        // Step 4b: Flush catalog mutations that were staged in memory during
        // this transaction (closes #305).  WAL is already durable at this point.
        // Labels are flushed first (rel-table entries reference label ids).
        for label_id in self.pending_label_creates.drain(..) {
            self.catalog.flush_label(label_id)?;
        }
        for rel_table_id in self.pending_rel_type_creates.drain(..) {
            self.catalog.flush_rel_table(rel_table_id)?;
        }

        // Step 5: Apply buffered structural operations to disk (SPA-181).
        // WAL is already durable at this point; a crash here is safe.
        //
        // SPA-212 (write-amplification fix): NodeCreate ops are collected into a
        // single batch and written via `batch_write_node_creates`, which opens
        // each (label_id, col_id) file only once regardless of how many nodes
        // share that column.  This reduces file-open syscalls from O(nodes×cols)
        // to O(labels×cols) per transaction commit.
        let mut col_writes: Vec<(u32, u32, u32, u64, bool)> = Vec::new();
        // All (label_id, slot) pairs for created nodes — needed for HWM
        // advancement, even when a node has zero properties.
        let mut node_slots: Vec<(u32, u32)> = Vec::new();

        for op in self.pending_ops.drain(..) {
            match op {
                PendingOp::NodeCreate {
                    label_id,
                    slot,
                    props,
                } => {
                    // Track this node's (label_id, slot) for HWM advancement.
                    node_slots.push((label_id, slot));
                    // Encode each property value and push (label_id, col_id,
                    // slot, raw_u64, is_present) into the batch buffer.
                    for (col_id, ref val) in props {
                        let raw = self.store.encode_value(val)?;
                        col_writes.push((label_id, col_id, slot, raw, true));
                    }
                }
                PendingOp::NodeDelete { node_id } => {
                    self.store.tombstone_node(node_id)?;
                }
                PendingOp::EdgeCreate {
                    src,
                    dst,
                    rel_table_id,
                    props,
                } => {
                    let mut es = EdgeStore::open(&self.inner.path, rel_table_id)?;
                    es.create_edge(src, rel_table_id, dst)?;
                    // Persist edge properties keyed by (src_slot, dst_slot) so
                    // that reads work correctly after CHECKPOINT (SPA-240).
                    if !props.is_empty() {
                        let src_slot = src.0 & 0xFFFF_FFFF;
                        let dst_slot = dst.0 & 0xFFFF_FFFF;
                        for (col_id, value) in &props {
                            es.set_edge_prop(src_slot, dst_slot, *col_id, *value)?;
                        }
                        // SPA-261: invalidate cached edge props for this rel table.
                        self.inner
                            .edge_props_cache
                            .write()
                            .expect("edge_props_cache poisoned")
                            .remove(&rel_table_id.0);
                    }
                }
                PendingOp::EdgeDelete {
                    src,
                    dst,
                    rel_table_id,
                } => {
                    let mut es = EdgeStore::open(&self.inner.path, rel_table_id)?;
                    es.delete_edge(src, dst)?;
                }
            }
        }

        // Flush all NodeCreate column writes in one batched call.
        // O(labels × cols) file opens instead of O(nodes × cols).
        self.store
            .batch_write_node_creates(col_writes, &node_slots)?;

        // Step 5b: Persist HWMs for all labels that received new nodes in Step 5.
        //
        // batch_write_node_creates() advances the in-memory HWM and marks
        // labels dirty (hwm_dirty).  We flush all dirty HWMs here — once per
        // commit — avoiding an fsync storm during bulk imports (SPA-217
        // regression fix).  Crash safety is preserved: the WAL record written
        // in Step 4 is already durable; on crash-recovery, the WAL replayer
        // re-applies all NodeCreate ops and re-advances the HWM.
        self.store.flush_hwms()?;

        // Step 6: Flush property updates to disk.
        // Use `upsert_node_col` so that columns added by `set_property` (which
        // may not have been initialised during `create_node`) are created and
        // zero-padded automatically.
        for ((node_raw, col_id), ref staged) in &updates {
            self.store
                .upsert_node_col(NodeId(*node_raw), *col_id, &staged.new_value)?;
        }

        // Step 7+8: Publish versions.
        {
            let mut vs = self.inner.versions.write().expect("version lock poisoned");
            for ((node_raw, col_id), ref staged) in &updates {
                // Publish before-image at the previous txn_id so that readers
                // pinned at that snapshot continue to see the correct value.
                if let Some((prev_txn_id, ref before_val)) = staged.before_image {
                    vs.insert(NodeId(*node_raw), *col_id, prev_txn_id, before_val.clone());
                }
                // Publish new value at the current txn_id.
                vs.insert(NodeId(*node_raw), *col_id, new_id, staged.new_value.clone());
            }
        }

        // Step 9: Advance per-node version table.
        {
            let mut nv = self
                .inner
                .node_versions
                .write()
                .expect("node_versions lock");
            for &raw in &self.dirty_nodes {
                nv.set(NodeId(raw), new_id);
            }
        }

        // Step 9b: Periodically garbage-collect the version store (issue #307).
        //
        // Every GC_COMMIT_INTERVAL commits we compute the minimum active reader
        // snapshot and prune fully-superseded old versions below that watermark.
        // This bounds VersionStore memory to O(live_keys × active_reader_span)
        // rather than O(live_keys × total_write_count).
        {
            let prev = self.inner.commits_since_gc.fetch_add(1, Ordering::Relaxed);
            if prev + 1 >= GC_COMMIT_INTERVAL {
                self.inner.commits_since_gc.store(0, Ordering::Relaxed);
                // Compute min active snapshot watermark.
                let min_active = {
                    let ar = self
                        .inner
                        .active_readers
                        .lock()
                        .expect("active_readers lock poisoned");
                    // If there are no active readers, every version older than
                    // the current committed txn_id is safe to prune (keeping
                    // only the most recent).  Use current_txn_id + 1 so the
                    // gc() "last version before watermark" logic retains the
                    // latest version for future readers.
                    ar.keys().copied().next().unwrap_or(new_id + 1)
                };
                let pruned = self
                    .inner
                    .versions
                    .write()
                    .expect("version lock poisoned")
                    .gc(min_active);
                if pruned > 0 {
                    tracing::debug!(pruned, min_active, "versionstore gc complete");
                }
            }
        }

        // Step 10: Flush any pending fulltext index updates.
        // The primary DB mutations above are already durable (WAL written,
        // txn_id advanced).  A flush failure here must NOT return Err and
        // cause the caller to retry an already-committed transaction.  Log the
        // error so operators can investigate but treat the commit as successful.
        for (name, mut idx) in self.fulltext_pending.drain() {
            if let Err(e) = idx.flush() {
                tracing::error!(index = %name, error = %e, "fulltext index flush failed post-commit; index may be stale until next write");
            }
        }

        // Step 11: Refresh the shared catalog cache so subsequent reads see
        // any labels / rel types created in this transaction (SPA-188).
        // Also rebuild the label-row-count cache (SPA-190) from the freshly
        // opened catalog to avoid a second I/O trip.
        if let Ok(fresh) = sparrowdb_catalog::catalog::Catalog::open(&self.inner.path) {
            let new_counts =
                crate::helpers::build_label_row_counts_from_disk(&fresh, &self.inner.path);
            *self.inner.catalog.write().expect("catalog RwLock poisoned") = fresh;
            *self
                .inner
                .label_row_counts
                .write()
                .expect("label_row_counts RwLock poisoned") = new_counts;
        }

        // Step 12: Invalidate the shared property-index cache (SPA-259).
        //
        // This is the single canonical invalidation point (#312).
        // Callers using WriteTx directly (without GraphDb::execute) still
        // get correct cache invalidation.
        self.inner.invalidate_prop_index();

        self.committed = true;
        Ok(TxnId(new_id))
    }
}

impl Drop for WriteTx {
    fn drop(&mut self) {
        // Uncommitted staged updates are discarded here — no writes to disk.
        // The write lock is released by dropping `_guard` (WriteGuard).
        let _ = self.committed;
    }
}
