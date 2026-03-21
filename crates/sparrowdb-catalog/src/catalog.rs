//! Catalog: label and relationship-table CRUD backed by a TLV file.
//!
//! The catalog file is a simple append-only TLV stream. On open, all
//! entries are decoded into memory. Writes append new TLV entries and
//! sync. This is the Phase 1 implementation; the full paged implementation
//! (with catalog payload pages, superblock, metapages) comes in Phase 2.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, Result};

use crate::tlv::{encode_entries, LabelEntry, RelTableEntry, TlvEntry};

/// Label identifier (u16 matches the TLV label_id field).
pub type LabelId = u16;

/// Relationship table identifier (u64 matches the TLV rel_table_id field).
pub type RelTableId = u64;

/// The catalog, loaded from (and persisted to) a TLV file.
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
        self.labels.push(entry.clone());
        self.next_label_id = self
            .next_label_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("label_id overflow".to_string()))?;
        self.append_entry(TlvEntry::Label(entry))?;
        Ok(id)
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

    // --- Relationship table CRUD ---

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
        self.rel_tables.push(entry.clone());
        self.next_rel_table_id = self
            .next_rel_table_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("rel_table_id overflow".to_string()))?;
        self.append_entry(TlvEntry::RelTable(entry))?;
        Ok(id)
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
                    if e.label_id >= self.next_label_id {
                        self.next_label_id = e.label_id + 1;
                    }
                    self.labels.push(e);
                }
                TlvEntry::RelTable(e) => {
                    if e.rel_table_id >= self.next_rel_table_id {
                        self.next_rel_table_id = e.rel_table_id + 1;
                    }
                    self.rel_tables.push(e);
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
