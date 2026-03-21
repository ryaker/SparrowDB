//! Maintenance operations: CHECKPOINT and OPTIMIZE.
//!
//! ## CHECKPOINT algorithm
//!
//! 1. Emit `CheckpointBegin` WAL record and fsync.
//! 2. Read current delta log; fold into CSR base files via atomic temp-file rename.
//! 3. Emit `CheckpointEnd` WAL record and fsync.
//! 4. Update metapage with new `wal_checkpoint_lsn` equal to the CheckpointEnd LSN.
//!
//! ## Crash safety
//!
//! - Crash before `CheckpointEnd` fsync → recovery finds `CheckpointBegin` without
//!   `CheckpointEnd`; replays from the prior `wal_checkpoint_lsn`, ignoring the
//!   partial checkpoint.
//! - Crash after `CheckpointEnd` fsync but before metapage update → recovery
//!   finds the `CheckpointEnd` LSN via WAL scan and treats it as the new horizon.
//!
//! ## OPTIMIZE algorithm
//!
//! Identical to CHECKPOINT but uses `EdgeStore::optimize` which produces
//! neighbor lists sorted by `(dst_node_id)` ascending.

use std::fs;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};

use sparrowdb_common::{Lsn, Result, TxnId};

use crate::edge_store::{EdgeStore, RelTableId};
use crate::metapage::{Metapage, METAPAGE_A_OFFSET, METAPAGE_B_OFFSET, METAPAGE_SIZE};
use crate::wal::codec::{WalPayload, WalRecordKind};
use crate::wal::writer::WalWriter;

/// The "checkpoint transaction ID" used for WAL records emitted by maintenance.
///
/// Maintenance is not a user transaction; we use TxnId(0) as a sentinel.
const CHECKPOINT_TXN_ID: TxnId = TxnId(0);

/// Maintenance engine: wraps edge stores + WAL + metapage for CHECKPOINT/OPTIMIZE.
pub struct MaintenanceEngine {
    db_root: PathBuf,
    /// Path to `catalog.bin` where dual metapages live.
    catalog_path: PathBuf,
}

impl MaintenanceEngine {
    /// Create a new `MaintenanceEngine` rooted at `db_root`.
    pub fn new(db_root: &Path) -> Self {
        MaintenanceEngine {
            db_root: db_root.to_path_buf(),
            catalog_path: db_root.join("catalog.bin"),
        }
    }

    /// Run CHECKPOINT over the given relationship tables.
    ///
    /// `rel_table_ids` is the list of relationship tables to checkpoint.
    /// `n_nodes` is passed to each `EdgeStore::checkpoint` call for bounds
    /// checking.
    ///
    /// WAL sequence:
    ///   1. Write + fsync `CheckpointBegin`.
    ///   2. For each rel_table: fold delta → CSR atomically.
    ///   3. Write + fsync `CheckpointEnd`.
    ///   4. Publish new `wal_checkpoint_lsn` to metapage.
    pub fn checkpoint(&self, rel_table_ids: &[u32], n_nodes: u64) -> Result<()> {
        self.run_maintenance(rel_table_ids, n_nodes, false)
    }

    /// Run OPTIMIZE over the given relationship tables.
    ///
    /// Same as CHECKPOINT but uses `EdgeStore::optimize`, which sorts neighbor
    /// lists by `(dst_node_id)` ascending.
    pub fn optimize(&self, rel_table_ids: &[u32], n_nodes: u64) -> Result<()> {
        self.run_maintenance(rel_table_ids, n_nodes, true)
    }

    // ── Private ───────────────────────────────────────────────────────────────

    fn wal_dir(&self) -> PathBuf {
        self.db_root.join("wal")
    }

    fn run_maintenance(&self, rel_table_ids: &[u32], n_nodes: u64, sorted: bool) -> Result<()> {
        let wal_dir = self.wal_dir();

        // Step 1: emit CheckpointBegin and fsync.
        let mut wal = WalWriter::open(&wal_dir)?;
        wal.append(
            WalRecordKind::CheckpointBegin,
            CHECKPOINT_TXN_ID,
            WalPayload::Empty,
        )?;
        wal.fsync()?;

        // Step 2: fold each rel_table's delta into CSR base files.
        for &rel_id in rel_table_ids {
            let mut store = EdgeStore::open(&self.db_root, RelTableId(rel_id))?;
            if sorted {
                store.optimize(n_nodes)?;
            } else {
                store.checkpoint(n_nodes)?;
            }
        }

        // Step 3: emit CheckpointEnd and fsync.
        let end_lsn = wal.append(
            WalRecordKind::CheckpointEnd,
            CHECKPOINT_TXN_ID,
            WalPayload::Empty,
        )?;
        wal.fsync()?;

        // Step 4: publish new wal_checkpoint_lsn to the metapage.
        self.publish_checkpoint_lsn(end_lsn)?;

        Ok(())
    }

    /// Write the updated `wal_checkpoint_lsn` to the next metapage slot.
    ///
    /// Reads both metapage slots from `catalog.bin`, selects the current winner,
    /// increments its `txn_id`, sets the new `wal_checkpoint_lsn`, and writes
    /// to the *other* slot (dual-page protocol).
    ///
    /// If `catalog.bin` does not exist yet, creates a minimal one.
    fn publish_checkpoint_lsn(&self, checkpoint_lsn: Lsn) -> Result<()> {
        use crate::metapage::select_winner;

        // Ensure catalog.bin exists and is at least 1152 bytes
        // (128 byte pre-A pad + 512 byte A + 0 gap + 512 byte B = 1152 bytes total
        //  at offset B = 640, so min size = 640 + 512 = 1152).
        let min_size = (METAPAGE_B_OFFSET as usize) + METAPAGE_SIZE;
        let current_size = if self.catalog_path.exists() {
            fs::metadata(&self.catalog_path)
                .map_err(sparrowdb_common::Error::Io)?
                .len() as usize
        } else {
            0
        };

        if current_size < min_size {
            // Extend the file (or create it) with zeros.
            let f = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&self.catalog_path)
                .map_err(sparrowdb_common::Error::Io)?;
            f.set_len(min_size as u64)
                .map_err(sparrowdb_common::Error::Io)?;
        }

        // Read both metapage slots.
        let data = fs::read(&self.catalog_path).map_err(sparrowdb_common::Error::Io)?;

        let mut a_buf = [0u8; METAPAGE_SIZE];
        let mut b_buf = [0u8; METAPAGE_SIZE];
        let a_start = METAPAGE_A_OFFSET as usize;
        let b_start = METAPAGE_B_OFFSET as usize;

        if data.len() >= a_start + METAPAGE_SIZE {
            a_buf.copy_from_slice(&data[a_start..a_start + METAPAGE_SIZE]);
        }
        if data.len() >= b_start + METAPAGE_SIZE {
            b_buf.copy_from_slice(&data[b_start..b_start + METAPAGE_SIZE]);
        }

        // Determine which slot to write next (the one NOT currently winning),
        // and what the next txn_id should be.
        let (winner, write_a) = match select_winner(&a_buf, &b_buf) {
            Ok(w) => {
                // Write to the slot with the lower txn_id (the loser).
                let a_txn = Metapage::decode(&a_buf).map(|m| m.txn_id).unwrap_or(0);
                let b_txn = Metapage::decode(&b_buf).map(|m| m.txn_id).unwrap_or(0);
                let write_a = a_txn <= b_txn; // write to A if A is older (or tied)
                (w, write_a)
            }
            Err(_) => {
                // No valid metapage yet — write a fresh one to slot A.
                let fresh = Metapage {
                    txn_id: 1,
                    catalog_root_page_id: 0,
                    node_root_page_id: u64::MAX,
                    edge_root_page_id: u64::MAX,
                    wal_checkpoint_lsn: checkpoint_lsn.0,
                    global_node_count: 0,
                    global_edge_count: 0,
                    next_edge_id: 0,
                };
                return self.write_metapage_slot(&fresh, true);
            }
        };

        let new_meta = Metapage {
            txn_id: winner.txn_id + 1,
            catalog_root_page_id: winner.catalog_root_page_id,
            node_root_page_id: winner.node_root_page_id,
            edge_root_page_id: winner.edge_root_page_id,
            wal_checkpoint_lsn: checkpoint_lsn.0,
            global_node_count: winner.global_node_count,
            global_edge_count: winner.global_edge_count,
            next_edge_id: winner.next_edge_id,
        };

        self.write_metapage_slot(&new_meta, write_a)
    }

    /// Write a metapage into slot A (`write_slot_a == true`) or slot B.
    fn write_metapage_slot(&self, meta: &Metapage, write_slot_a: bool) -> Result<()> {
        let encoded = meta.encode();
        let offset = if write_slot_a {
            METAPAGE_A_OFFSET as usize
        } else {
            METAPAGE_B_OFFSET as usize
        };

        // Open for write (not truncating) and seek to the correct offset.
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.catalog_path)
            .map_err(sparrowdb_common::Error::Io)?;
        use std::io::Seek;
        f.seek(std::io::SeekFrom::Start(offset as u64))
            .map_err(sparrowdb_common::Error::Io)?;
        f.write_all(&encoded).map_err(sparrowdb_common::Error::Io)?;
        f.sync_all().map_err(sparrowdb_common::Error::Io)?;
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge_store::{EdgeStore, RelTableId};
    use sparrowdb_common::NodeId;
    use tempfile::tempdir;

    const REL: u32 = 0;

    fn nid(v: u64) -> NodeId {
        NodeId(v)
    }

    #[test]
    fn test_checkpoint_emits_wal_records() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Write some edges.
        {
            let mut store = EdgeStore::open(db_root, RelTableId(REL)).unwrap();
            store.create_edge(nid(0), RelTableId(REL), nid(1)).unwrap();
            store.create_edge(nid(1), RelTableId(REL), nid(2)).unwrap();
        }

        // Run checkpoint via maintenance engine.
        let engine = MaintenanceEngine::new(db_root);
        engine.checkpoint(&[REL], 4).unwrap();

        // WAL directory must exist with at least one segment.
        let wal_dir = db_root.join("wal");
        assert!(wal_dir.exists(), "WAL directory must be created");
        let entries: Vec<_> = fs::read_dir(&wal_dir).unwrap().flatten().collect();
        assert!(!entries.is_empty(), "WAL must have at least one segment");

        // Delta log must be empty after checkpoint.
        let store = EdgeStore::open(db_root, RelTableId(REL)).unwrap();
        let delta = store.read_delta().unwrap();
        assert_eq!(delta.len(), 0, "delta.log must be empty after checkpoint");
    }

    #[test]
    fn test_checkpoint_wal_contains_begin_and_end() {
        use crate::wal::codec::{WalRecord, WalRecordKind};

        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create some edges.
        {
            let mut store = EdgeStore::open(db_root, RelTableId(REL)).unwrap();
            store.create_edge(nid(0), RelTableId(REL), nid(1)).unwrap();
        }

        let engine = MaintenanceEngine::new(db_root);
        engine.checkpoint(&[REL], 4).unwrap();

        // Read the WAL segment and look for CheckpointBegin and CheckpointEnd.
        let wal_dir = db_root.join("wal");
        let seg_path = crate::wal::writer::segment_path(&wal_dir, 0);
        let data = fs::read(&seg_path).unwrap();

        let mut offset = 0usize;
        let mut kinds = Vec::new();
        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((rec, consumed)) => {
                    kinds.push(rec.kind);
                    offset += consumed;
                }
                Err(_) => break,
            }
        }

        assert!(
            kinds.contains(&WalRecordKind::CheckpointBegin),
            "WAL must contain CheckpointBegin"
        );
        assert!(
            kinds.contains(&WalRecordKind::CheckpointEnd),
            "WAL must contain CheckpointEnd"
        );

        // CheckpointBegin must come before CheckpointEnd.
        let begin_pos = kinds
            .iter()
            .position(|k| *k == WalRecordKind::CheckpointBegin)
            .unwrap();
        let end_pos = kinds
            .iter()
            .position(|k| *k == WalRecordKind::CheckpointEnd)
            .unwrap();
        assert!(
            begin_pos < end_pos,
            "CheckpointBegin must precede CheckpointEnd"
        );
    }

    #[test]
    fn test_checkpoint_publishes_metapage() {
        use crate::metapage::{select_winner, METAPAGE_A_OFFSET, METAPAGE_B_OFFSET, METAPAGE_SIZE};

        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Add edges and checkpoint.
        {
            let mut store = EdgeStore::open(db_root, RelTableId(REL)).unwrap();
            store.create_edge(nid(0), RelTableId(REL), nid(1)).unwrap();
        }
        let engine = MaintenanceEngine::new(db_root);
        engine.checkpoint(&[REL], 4).unwrap();

        // Read the catalog.bin and verify metapage has a non-zero checkpoint LSN.
        let catalog_path = db_root.join("catalog.bin");
        assert!(
            catalog_path.exists(),
            "catalog.bin must exist after checkpoint"
        );
        let data = fs::read(&catalog_path).unwrap();
        assert!(data.len() >= (METAPAGE_B_OFFSET as usize) + METAPAGE_SIZE);

        let mut a_buf = [0u8; METAPAGE_SIZE];
        let mut b_buf = [0u8; METAPAGE_SIZE];
        a_buf.copy_from_slice(
            &data[METAPAGE_A_OFFSET as usize..METAPAGE_A_OFFSET as usize + METAPAGE_SIZE],
        );
        b_buf.copy_from_slice(
            &data[METAPAGE_B_OFFSET as usize..METAPAGE_B_OFFSET as usize + METAPAGE_SIZE],
        );

        let winner = select_winner(&a_buf, &b_buf).expect("metapage must be valid");
        assert!(
            winner.wal_checkpoint_lsn > 0,
            "wal_checkpoint_lsn must be non-zero after checkpoint"
        );
    }

    #[test]
    fn test_optimize_sorts_neighbor_lists() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Insert edges in non-sorted order.
        {
            let mut store = EdgeStore::open(db_root, RelTableId(REL)).unwrap();
            // src=0 → dst=3, then dst=1, then dst=2
            store.create_edge(nid(0), RelTableId(REL), nid(3)).unwrap();
            store.create_edge(nid(0), RelTableId(REL), nid(1)).unwrap();
            store.create_edge(nid(0), RelTableId(REL), nid(2)).unwrap();
        }

        let engine = MaintenanceEngine::new(db_root);
        engine.optimize(&[REL], 4).unwrap();

        // Read the CSR and check neighbor list for node 0 is sorted.
        let store = EdgeStore::open(db_root, RelTableId(REL)).unwrap();
        let fwd = store.open_fwd().unwrap();
        let neighbors = fwd.neighbors(0);
        let sorted: Vec<u64> = {
            let mut v = neighbors.to_vec();
            v.sort_unstable();
            v
        };
        assert_eq!(
            neighbors,
            sorted.as_slice(),
            "neighbor list must be sorted after OPTIMIZE"
        );
    }
}
