// ── ReadTx ────────────────────────────────────────────────────────────────────

use crate::types::DbInner;
use sparrowdb_common::{NodeId, TxnId};
use sparrowdb_execution::{Engine, QueryResult};
use sparrowdb_storage::node_store::{NodeStore, Value};
use std::sync::Arc;
use tracing::info_span;

/// A read-only snapshot transaction.
///
/// Pinned at the `txn_id` current when this handle was opened; immune to
/// subsequent writer commits for the lifetime of this handle.
pub struct ReadTx {
    /// The committed `txn_id` this reader is pinned to.
    pub snapshot_txn_id: u64,
    pub(crate) store: NodeStore,
    pub(crate) inner: Arc<DbInner>,
}

impl ReadTx {
    /// Read the `Int64` property values of a node at the pinned snapshot.
    ///
    /// For each column the version chain is consulted first; if a value was
    /// committed at or before `snapshot_txn_id` it shadows the on-disk value.
    pub fn get_node(&self, node_id: NodeId, col_ids: &[u32]) -> crate::Result<Vec<(u32, Value)>> {
        let versions = self.inner.versions.read().expect("version lock poisoned");
        let raw = self.store.get_node_raw(node_id, col_ids)?;
        let result = raw
            .into_iter()
            .map(|(col_id, raw_val)| {
                // Check version chain first.
                if let Some(v) = versions.get_at(node_id, col_id, self.snapshot_txn_id) {
                    (col_id, v)
                } else {
                    (col_id, self.store.decode_raw_value(raw_val))
                }
            })
            .collect();
        Ok(result)
    }

    /// Return the snapshot `TxnId` this reader is pinned to.
    pub fn snapshot(&self) -> TxnId {
        TxnId(self.snapshot_txn_id)
    }

    /// Execute a read-only Cypher query against the pinned snapshot.
    ///
    /// ## Snapshot isolation
    ///
    /// The query sees exactly the committed state at the moment
    /// [`begin_read`](crate::GraphDb::begin_read) was called.  Any writes committed
    /// after that point — even fully committed ones — are invisible until a
    /// new `ReadTx` is opened.
    ///
    /// ## Concurrency
    ///
    /// Multiple `ReadTx` handles may run `query` concurrently.  No write lock
    /// is acquired; only the shared read-paths of the catalog, CSR, and
    /// property-index caches are accessed.
    ///
    /// ## Mutation statements rejected
    ///
    /// Passing a mutation statement (`CREATE`, `MERGE`, `MATCH … SET`,
    /// `MATCH … DELETE`, `CHECKPOINT`, `OPTIMIZE`, etc.) returns
    /// [`Error::ReadOnly`].  Use [`GraphDb::execute`](crate::GraphDb::execute) for mutations.
    ///
    /// # Example
    /// ```no_run
    /// use sparrowdb::GraphDb;
    ///
    /// let db = GraphDb::open(std::path::Path::new("/tmp/g.sparrow")).unwrap();
    /// let tx = db.begin_read().unwrap();
    /// let result = tx.query("MATCH (n:Person) RETURN n.name").unwrap();
    /// println!("{} rows", result.rows.len());
    /// ```
    pub fn query(&self, cypher: &str) -> crate::Result<QueryResult> {
        use sparrowdb_cypher::{bind, parse};

        let stmt = parse(cypher)?;

        // Take a snapshot of the catalog from the shared cache (no disk I/O if
        // the catalog is already warm).
        let catalog_snap = self
            .inner
            .catalog
            .read()
            .expect("catalog RwLock poisoned")
            .clone();

        let bound = bind(stmt, &catalog_snap)?;

        // Reject any statement that would mutate state — ReadTx is read-only.
        if Engine::is_mutation(&bound.inner) {
            return Err(crate::Error::ReadOnly);
        }

        // Also reject DDL / maintenance statements.
        use sparrowdb_cypher::ast::Statement;
        match &bound.inner {
            Statement::Checkpoint | Statement::Optimize | Statement::CreateConstraint { .. } => {
                return Err(crate::Error::ReadOnly);
            }
            _ => {}
        }

        let _span = info_span!("sparrowdb.readtx.query").entered();

        let csrs = self
            .inner
            .csr_map
            .read()
            .expect("csr_map RwLock poisoned")
            .clone();

        let mut engine = {
            let _open_span = info_span!("sparrowdb.readtx.open_engine").entered();
            let row_counts = self
                .inner
                .label_row_counts
                .read()
                .expect("label_row_counts RwLock poisoned")
                .clone();
            Engine::new_with_all_caches(
                NodeStore::open(&self.inner.path)?,
                catalog_snap,
                csrs,
                &self.inner.path,
                Some(&self.inner.prop_index),
                Some(row_counts),
                Some(Arc::clone(&self.inner.edge_props_cache)),
            )
        };

        let result = {
            let _exec_span = info_span!("sparrowdb.readtx.execute").entered();
            engine.execute_statement(bound.inner)?
        };

        // Write lazily-loaded columns back to the shared property-index cache
        // so subsequent queries benefit from warm column data.
        engine.write_back_prop_index(&self.inner.prop_index);
        // SPA-286: persist updated index to disk if new columns were loaded.
        self.inner.persist_prop_index();

        tracing::debug!(
            rows = result.rows.len(),
            snapshot_txn_id = self.snapshot_txn_id,
            "readtx query complete"
        );
        Ok(result)
    }
}

impl Drop for ReadTx {
    fn drop(&mut self) {
        // Unregister this reader's snapshot from the active-readers map.
        // When the count drops to zero the entry is removed so GC can advance
        // the watermark past this snapshot.
        if let Ok(mut ar) = self.inner.active_readers.lock() {
            if let std::collections::btree_map::Entry::Occupied(mut e) =
                ar.entry(self.snapshot_txn_id)
            {
                let count = e.get_mut();
                if *count <= 1 {
                    e.remove();
                } else {
                    *count -= 1;
                }
            }
        }
    }
}
