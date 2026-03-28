//! Catalog: label and relationship-table CRUD backed by a TLV file.
//!
//! The catalog file is a simple append-only TLV stream. On open, all
//! entries are decoded into memory. Writes append new TLV entries and
//! sync. This is the Phase 1 implementation; the full paged implementation
//! (with catalog payload pages, superblock, metapages) comes in Phase 2.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, NodeId, Result};

use crate::tlv::{encode_entries, LabelEntry, RelTableEntry, TlvEntry};

/// Label identifier (u16 matches the TLV label_id field).
pub type LabelId = u16;

/// Relationship table identifier (u64 matches the TLV rel_table_id field).
pub type RelTableId = u64;

/// The catalog, loaded from (and persisted to) a TLV file.
///
/// ## Multi-label support (SPA-200)
///
/// Every node has exactly one *primary* label that determines its `NodeId`
/// encoding and physical storage directory.  Additional ("secondary") labels
/// are tracked here in two in-memory indexes:
///
/// - `node_label_sets`: `NodeId → HashSet<LabelId>` — secondary labels for a
///   node (primary label is **not** stored here; it is encoded in the NodeId).
///   Used by `get_node_labels()` and MATCH intersection logic.
/// - `secondary_label_index`: `LabelId → HashSet<NodeId>` — reverse index
///   mapping a secondary label to all nodes that carry it.  Used by MATCH to
///   find nodes where the queried label is a *secondary* label without a full
///   primary-label store scan.
///
/// Both indexes are rebuilt from `NodeLabelSet` TLV entries on `open()` and
/// updated in-memory on every `record_secondary_labels()` call.
///
/// ## Property index limitation (Phase 1)
///
/// Property indexes are keyed by `(primary_label_id, col_id)`.  A node
/// created as `(:A:B {name: "x"})` stores its columns under label A.
/// `CREATE INDEX ON :B(name)` will **not** cover this node in Phase 1.
/// Cross-label index support is planned for Phase 2 (issue #289).
#[derive(Clone)]
pub struct Catalog {
    /// Path to the TLV catalog file.
    path: PathBuf,
    /// In-memory label entries, ordered by insertion.
    labels: Vec<LabelEntry>,
    /// In-memory relationship table entries.
    rel_tables: Vec<RelTableEntry>,
    /// Next label_id to assign.
    next_label_id: u16,
    /// Next rel_table_id to assign.
    next_rel_table_id: u64,
    /// Secondary label sets per node: NodeId → set of secondary LabelIds.
    ///
    /// Only nodes with at least one secondary label appear here.  Single-label
    /// nodes have no entry (absence means "primary label only").
    node_label_sets: HashMap<NodeId, HashSet<LabelId>>,
    /// Reverse index: secondary LabelId → set of NodeIds that carry that label.
    ///
    /// Used by MATCH resolution to find nodes by secondary label without
    /// performing a full scan of every primary-label store.
    secondary_label_index: HashMap<LabelId, HashSet<NodeId>>,
}

impl Catalog {
    /// Open the catalog from `path/catalog.tlv`.
    ///
    /// If the file does not exist, an empty catalog is returned (the file is
    /// created on first write).
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join("catalog.tlv");
        let mut catalog = Catalog {
            path,
            labels: Vec::new(),
            rel_tables: Vec::new(),
            next_label_id: 0,
            next_rel_table_id: 0,
            node_label_sets: HashMap::new(),
            secondary_label_index: HashMap::new(),
        };
        catalog.load()?;
        Ok(catalog)
    }

    // --- Label CRUD ---

    /// Create a new label with the given name.
    ///
    /// Returns `Err(Error::AlreadyExists)` if a label with that name already exists.
    pub fn create_label(&mut self, name: &str) -> Result<LabelId> {
        if self.labels.iter().any(|e| e.name == name) {
            return Err(Error::AlreadyExists);
        }
        let id = self.next_label_id;
        let entry = LabelEntry {
            label_id: id,
            name: name.to_string(),
        };
        let next = self
            .next_label_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("label_id overflow".to_string()))?;
        // Persist first; only update in-memory state if the write+fsync succeeds.
        self.append_entry(TlvEntry::Label(entry.clone()))?;
        self.labels.push(entry);
        self.next_label_id = next;
        Ok(id)
    }

    /// Find or create a label by name, returning its id.
    ///
    /// If the label already exists, returns the existing id.
    /// If it does not exist, creates it and returns the new id.
    pub fn get_or_create_label_id(&mut self, name: &str) -> Result<LabelId> {
        if let Some(id) = self.get_label(name)? {
            return Ok(id);
        }
        self.create_label(name)
    }

    /// Reserve a label id in memory **without** writing to disk.
    ///
    /// Used by `WriteTx` to buffer label creations so they can be committed
    /// atomically with the rest of the transaction.  The caller is responsible
    /// for calling [`flush_label`] for each staged label at commit time.
    ///
    /// Returns `Err(AlreadyExists)` if the label is already present (either
    /// persisted or already staged in this catalog instance).
    ///
    /// [`flush_label`]: Catalog::flush_label
    pub fn stage_label(&mut self, name: &str) -> Result<LabelId> {
        if self.labels.iter().any(|e| e.name == name) {
            return Err(Error::AlreadyExists);
        }
        let id = self.next_label_id;
        let entry = LabelEntry {
            label_id: id,
            name: name.to_string(),
        };
        let next = self
            .next_label_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("label_id overflow".to_string()))?;
        // In-memory only — no disk write.
        self.labels.push(entry);
        self.next_label_id = next;
        Ok(id)
    }

    /// Write a previously-staged label (by id) to the catalog file on disk.
    ///
    /// Called at `WriteTx::commit()` time to durably persist labels that were
    /// reserved in memory via [`stage_label`].
    ///
    /// [`stage_label`]: Catalog::stage_label
    pub fn flush_label(&self, label_id: LabelId) -> Result<()> {
        let entry = self
            .labels
            .iter()
            .find(|e| e.label_id == label_id)
            .ok_or_else(|| Error::InvalidArgument(format!("label_id {label_id} not in catalog")))?;
        self.append_entry(TlvEntry::Label(entry.clone()))
    }

    /// Look up a label by name.
    pub fn get_label(&self, name: &str) -> Result<Option<LabelId>> {
        Ok(self
            .labels
            .iter()
            .find(|e| e.name == name)
            .map(|e| e.label_id))
    }

    /// List all labels as `(LabelId, name)` pairs.
    pub fn list_labels(&self) -> Result<Vec<(LabelId, String)>> {
        Ok(self
            .labels
            .iter()
            .map(|e| (e.label_id, e.name.clone()))
            .collect())
    }

    // --- Multi-label side table (SPA-200) ---

    /// Record that `node_id` carries `secondary_label_ids` in addition to its
    /// primary label (which is encoded in the `NodeId` itself).
    ///
    /// Persists a `NodeLabelSet` TLV entry to `catalog.tlv` and updates the
    /// in-memory forward and reverse indexes.
    ///
    /// Calling this for a node with no secondary labels (empty slice) is a no-op.
    ///
    /// # WAL note (Phase 1)
    ///
    /// WAL replay for `NodeLabelSet` entries is not yet implemented (see issue
    /// #303).  This method persists directly to the catalog side table, which
    /// is correct for Phase 1 since the catalog is always written synchronously.
    pub fn record_secondary_labels(
        &mut self,
        node_id: NodeId,
        secondary_label_ids: &[LabelId],
    ) -> Result<()> {
        if secondary_label_ids.is_empty() {
            return Ok(());
        }

        let primary_label_id = (node_id.0 >> 32) as u16;
        let slot = (node_id.0 & 0xFFFF_FFFF) as u32;

        let entry = crate::tlv::NodeLabelSetEntry {
            primary_label_id,
            slot,
            secondary_label_ids: secondary_label_ids.to_vec(),
        };
        // Persist first; only update in-memory state if write+fsync succeeds.
        self.append_entry(TlvEntry::NodeLabelSet(entry))?;

        // Update forward index.
        let set = self.node_label_sets.entry(node_id).or_default();
        set.extend(secondary_label_ids.iter().copied());

        // Update reverse index.
        for &label_id in secondary_label_ids {
            self.secondary_label_index
                .entry(label_id)
                .or_default()
                .insert(node_id);
        }

        Ok(())
    }

    /// Return all labels for `node_id` (primary first, then secondary labels
    /// in insertion order).
    ///
    /// The primary label is derived from the `NodeId` encoding; secondary
    /// labels come from the `node_label_sets` side table.  Returns an empty
    /// `Vec` if the node's primary label is not registered in the catalog.
    pub fn get_node_labels(&self, node_id: NodeId) -> Vec<LabelId> {
        let primary_label_id = (node_id.0 >> 32) as LabelId;
        let mut labels = vec![primary_label_id];
        if let Some(secondary) = self.node_label_sets.get(&node_id) {
            // Stable ordering: sort secondary labels by ID for determinism.
            let mut sorted: Vec<LabelId> = secondary.iter().copied().collect();
            sorted.sort_unstable();
            labels.extend(sorted);
        }
        labels
    }

    /// Return all `NodeId`s that carry `label_id` as a **secondary** label.
    ///
    /// Used by MATCH resolution to union primary-label scan results with
    /// secondary-label hits.  Returns an empty iterator for labels that have
    /// never been assigned as a secondary label.
    pub fn nodes_with_secondary_label(
        &self,
        label_id: LabelId,
    ) -> impl Iterator<Item = NodeId> + '_ {
        self.secondary_label_index
            .get(&label_id)
            .into_iter()
            .flat_map(|set| set.iter().copied())
    }

    // --- Relationship table CRUD ---

    /// Find or create a relationship table entry for `(src_label_id, dst_label_id, rel_type)`.
    ///
    /// If a matching entry already exists, returns its id.
    /// Otherwise creates a new entry, persists it to disk, and returns the new id.
    pub fn get_or_create_rel_type_id(
        &mut self,
        src_label_id: u16,
        dst_label_id: u16,
        rel_type: &str,
    ) -> Result<RelTableId> {
        if let Some(id) = self.get_rel_table(src_label_id, dst_label_id, rel_type)? {
            return Ok(id);
        }
        self.create_rel_table(src_label_id, dst_label_id, rel_type)
    }

    /// Reserve a relationship table id in memory **without** writing to disk.
    ///
    /// Used by `WriteTx` to buffer rel-type creations so they can be committed
    /// atomically with the rest of the transaction.  The caller is responsible
    /// for calling [`flush_rel_table`] for each staged entry at commit time.
    ///
    /// Returns the existing id (no staging needed) if the entry is already
    /// present, or the newly-reserved id otherwise.
    ///
    /// [`flush_rel_table`]: Catalog::flush_rel_table
    pub fn stage_rel_table(
        &mut self,
        src_label_id: u16,
        dst_label_id: u16,
        rel_type: &str,
    ) -> Result<(RelTableId, bool)> {
        // Return (id, is_new).  Caller only needs to flush if is_new.
        if let Some(id) = self.get_rel_table(src_label_id, dst_label_id, rel_type)? {
            return Ok((id, false));
        }
        let id = self.next_rel_table_id;
        let entry = RelTableEntry {
            rel_table_id: id,
            src_label_id,
            dst_label_id,
            rel_type: rel_type.to_string(),
        };
        let next = self
            .next_rel_table_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("rel_table_id overflow".to_string()))?;
        // In-memory only — no disk write.
        self.rel_tables.push(entry);
        self.next_rel_table_id = next;
        Ok((id, true))
    }

    /// Write a previously-staged rel table entry (by id) to the catalog file on disk.
    ///
    /// Called at `WriteTx::commit()` time to durably persist rel types that were
    /// reserved in memory via [`stage_rel_table`].
    ///
    /// [`stage_rel_table`]: Catalog::stage_rel_table
    pub fn flush_rel_table(&self, rel_table_id: RelTableId) -> Result<()> {
        let entry = self
            .rel_tables
            .iter()
            .find(|e| e.rel_table_id == rel_table_id)
            .ok_or_else(|| {
                Error::InvalidArgument(format!("rel_table_id {rel_table_id} not in catalog"))
            })?;
        self.append_entry(TlvEntry::RelTable(entry.clone()))
    }

    /// Create a new relationship table entry.
    pub fn create_rel_table(
        &mut self,
        src_label_id: u16,
        dst_label_id: u16,
        rel_type: &str,
    ) -> Result<RelTableId> {
        if self.rel_tables.iter().any(|e| {
            e.src_label_id == src_label_id
                && e.dst_label_id == dst_label_id
                && e.rel_type == rel_type
        }) {
            return Err(Error::AlreadyExists);
        }
        let id = self.next_rel_table_id;
        let entry = RelTableEntry {
            rel_table_id: id,
            src_label_id,
            dst_label_id,
            rel_type: rel_type.to_string(),
        };
        let next = self
            .next_rel_table_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("rel_table_id overflow".to_string()))?;
        // Persist first; only update in-memory state if the write+fsync succeeds.
        self.append_entry(TlvEntry::RelTable(entry.clone()))?;
        self.rel_tables.push(entry);
        self.next_rel_table_id = next;
        Ok(id)
    }

    /// List all relationship tables as `(src_label_id, dst_label_id, rel_type)` triples.
    pub fn list_rel_tables(&self) -> Result<Vec<(u16, u16, String)>> {
        Ok(self
            .rel_tables
            .iter()
            .map(|e| (e.src_label_id, e.dst_label_id, e.rel_type.clone()))
            .collect())
    }

    /// List all relationship table IDs.
    ///
    /// Returns `(rel_table_id, src_label_id, dst_label_id, rel_type)` for every
    /// registered relationship table.  Used by maintenance operations (CHECKPOINT,
    /// OPTIMIZE) to discover all per-type edge stores.
    pub fn list_rel_table_ids(&self) -> Vec<(u64, u16, u16, String)> {
        self.rel_tables
            .iter()
            .map(|e| {
                (
                    e.rel_table_id,
                    e.src_label_id,
                    e.dst_label_id,
                    e.rel_type.clone(),
                )
            })
            .collect()
    }

    /// Look up a relationship table by label IDs and type.
    pub fn get_rel_table(
        &self,
        src_label_id: u16,
        dst_label_id: u16,
        rel_type: &str,
    ) -> Result<Option<RelTableId>> {
        Ok(self
            .rel_tables
            .iter()
            .find(|e| {
                e.src_label_id == src_label_id
                    && e.dst_label_id == dst_label_id
                    && e.rel_type == rel_type
            })
            .map(|e| e.rel_table_id))
    }

    /// Look up the relationship type name for a given `RelTableId`.
    ///
    /// Returns `None` if no such rel table is registered in the catalog.
    pub fn get_rel_type_name(&self, rel_table_id: RelTableId) -> Option<String> {
        self.rel_tables
            .iter()
            .find(|e| e.rel_table_id == rel_table_id)
            .map(|e| e.rel_type.clone())
    }

    /// List all relationship tables with their catalog IDs.
    ///
    /// Returns `(RelTableId, src_label_id, dst_label_id, rel_type)` tuples so
    /// that callers can open the correct per-table delta log file.
    pub fn list_rel_tables_with_ids(&self) -> Vec<(RelTableId, u16, u16, String)> {
        self.rel_tables
            .iter()
            .map(|e| {
                (
                    e.rel_table_id,
                    e.src_label_id,
                    e.dst_label_id,
                    e.rel_type.clone(),
                )
            })
            .collect()
    }

    // --- Private helpers ---

    /// Load all TLV entries from the catalog file into memory.
    fn load(&mut self) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }
        let mut file = std::fs::File::open(&self.path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let entries = TlvEntry::decode_all(&data)?;
        for entry in entries {
            match entry {
                TlvEntry::Label(e) => {
                    if self.labels.iter().any(|x| x.label_id == e.label_id) {
                        return Err(Error::Corruption(format!(
                            "duplicate label_id {} in catalog file",
                            e.label_id
                        )));
                    }
                    if e.label_id >= self.next_label_id {
                        self.next_label_id = e.label_id + 1;
                    }
                    self.labels.push(e);
                }
                TlvEntry::RelTable(e) => {
                    if self
                        .rel_tables
                        .iter()
                        .any(|x| x.rel_table_id == e.rel_table_id)
                    {
                        return Err(Error::Corruption(format!(
                            "duplicate rel_table_id {} in catalog file",
                            e.rel_table_id
                        )));
                    }
                    if e.rel_table_id >= self.next_rel_table_id {
                        self.next_rel_table_id = e.rel_table_id + 1;
                    }
                    self.rel_tables.push(e);
                }
                TlvEntry::NodeLabelSet(e) => {
                    // Reconstruct node_label_sets and secondary_label_index
                    // from persisted NodeLabelSet entries (SPA-200).
                    let node_id = NodeId(((e.primary_label_id as u64) << 32) | (e.slot as u64));
                    let set = self.node_label_sets.entry(node_id).or_default();
                    for &sid in &e.secondary_label_ids {
                        set.insert(sid);
                        self.secondary_label_index
                            .entry(sid)
                            .or_default()
                            .insert(node_id);
                    }
                }
                _ => {} // other entry types are not processed in Phase 1
            }
        }
        Ok(())
    }

    /// Append a single TLV entry to the catalog file and fsync.
    fn append_entry(&self, entry: TlvEntry) -> Result<()> {
        let bytes = encode_entries(&[entry]);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_label_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir must succeed");
        let path = dir.path();

        // Create labels in a fresh catalog
        {
            let mut catalog = Catalog::open(path).expect("open must succeed");
            let id_person = catalog.create_label("Person").expect("create Person");
            let id_movie = catalog.create_label("Movie").expect("create Movie");
            assert_eq!(id_person, 0);
            assert_eq!(id_movie, 1);
        }

        // Reopen and verify labels are present
        {
            let catalog = Catalog::open(path).expect("reopen must succeed");
            let labels = catalog.list_labels().expect("list_labels must succeed");
            assert_eq!(labels.len(), 2);
            let person_id = catalog
                .get_label("Person")
                .expect("get must not error")
                .expect("Person must exist");
            let movie_id = catalog
                .get_label("Movie")
                .expect("get must not error")
                .expect("Movie must exist");
            assert_eq!(person_id, 0);
            assert_eq!(movie_id, 1);
        }
    }

    #[test]
    fn test_create_label_already_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut catalog = Catalog::open(dir.path()).expect("open");
        catalog.create_label("Person").expect("first create");
        let result = catalog.create_label("Person");
        assert!(matches!(result, Err(Error::AlreadyExists)));
    }

    #[test]
    fn test_get_label_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = Catalog::open(dir.path()).expect("open");
        let result = catalog.get_label("Ghost").expect("get must not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_list_labels_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = Catalog::open(dir.path()).expect("open");
        let labels = catalog.list_labels().expect("list");
        assert!(labels.is_empty());
    }

    #[test]
    fn test_create_rel_table_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path();

        {
            let mut catalog = Catalog::open(path).expect("open");
            let p_id = catalog.create_label("Person").expect("Person");
            let m_id = catalog.create_label("Movie").expect("Movie");
            let rt_id = catalog
                .create_rel_table(p_id, m_id, "ACTED_IN")
                .expect("ACTED_IN");
            assert_eq!(rt_id, 0);
        }

        {
            let catalog = Catalog::open(path).expect("reopen");
            // labels still present
            assert!(catalog.get_label("Person").unwrap().is_some());
            assert!(catalog.get_label("Movie").unwrap().is_some());
            // rel table must also survive reopen — SPA-191
            let rel_tables = catalog.list_rel_tables().expect("list_rel_tables");
            assert_eq!(rel_tables.len(), 1, "rel table must survive reopen");
            assert_eq!(
                rel_tables[0].2, "ACTED_IN",
                "rel type name must survive reopen"
            );
            let p_id = catalog.get_label("Person").unwrap().unwrap();
            let m_id = catalog.get_label("Movie").unwrap().unwrap();
            let recovered = catalog
                .get_rel_table(p_id, m_id, "ACTED_IN")
                .expect("get_rel_table must not error")
                .expect("ACTED_IN rel table must be present after reopen");
            assert_eq!(recovered, 0, "rel_table_id must be stable across reopen");
        }
    }

    #[test]
    fn test_label_ids_are_sequential() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut catalog = Catalog::open(dir.path()).expect("open");
        for i in 0u16..5 {
            let id = catalog.create_label(&format!("Label{i}")).expect("create");
            assert_eq!(id, i);
        }
    }

    #[test]
    fn test_catalog_survives_multiple_reopens() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path();

        // Create 3 labels across 3 sessions
        for i in 0u16..3 {
            let mut catalog = Catalog::open(path).expect("open");
            catalog.create_label(&format!("L{i}")).expect("create");
        }

        let catalog = Catalog::open(path).expect("final open");
        let labels = catalog.list_labels().expect("list");
        assert_eq!(labels.len(), 3);
        assert_eq!(catalog.get_label("L0").unwrap(), Some(0));
        assert_eq!(catalog.get_label("L1").unwrap(), Some(1));
        assert_eq!(catalog.get_label("L2").unwrap(), Some(2));
    }
}
