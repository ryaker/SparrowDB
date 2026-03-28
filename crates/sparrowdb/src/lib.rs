// Suppress rustdoc warnings for Markdown links that are valid on GitHub/docs.rs
// but cannot be resolved as Rust item paths (e.g., [LICENSE], [docs/...]).
#![allow(rustdoc::bare_urls)]
#![allow(rustdoc::broken_intra_doc_links)]
//! SparrowDB — top-level public API.
//!
//! ## Transaction model (Phase 5 — SWMR)
//!
//! * **Single Writer**: at most one [`WriteTx`] may be active at a time.
//!   [`GraphDb::begin_write`] returns [`Error::WriterBusy`] if a writer is
//!   already open.
//!
//! * **Multiple Readers**: any number of [`ReadTx`] handles may coexist with
//!   an active writer.  Each reader pins the committed `txn_id` at open time
//!   and sees only data committed at or before that snapshot.
//!
//! ## Snapshot isolation for `set_node_col`
//!
//! Property updates (`set_node_col`) are recorded in an **in-memory version
//! chain** keyed by `(NodeId, col_id)`.  Each entry is a `(txn_id, Value)`
//! pair.  When a `ReadTx` reads a property it first consults the version chain
//! and returns the most-recent value with `txn_id <= snapshot_txn_id`, falling
//! back to the on-disk value written by `create_node` if no such entry exists.
//!
//! ## Quick start
//!
//! ```no_run
//! use sparrowdb::GraphDb;
//! let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
//! db.checkpoint().unwrap();
//! db.optimize().unwrap();
//! ```

// ── Internal submodules ───────────────────────────────────────────────────────
mod batch;
mod db;
mod helpers;
mod read_tx;
mod types;
mod wal_codec;
mod write_tx;

// ── Public submodules ─────────────────────────────────────────────────────────

/// Export/dump utilities for serializing the graph to portable formats.
pub mod export;
pub use export::{EdgeDump, GraphDump, NodeDump};

/// Bulk-import loader for high-throughput node/edge ingestion.
pub mod bulk;
pub use bulk::BulkLoader;

// ── Public re-exports ─────────────────────────────────────────────────────────
//
// Re-export the types that consumers of the top-level `sparrowdb` crate need
// without also having to depend on the sub-crates directly.

/// The top-level SparrowDB handle.
pub use db::GraphDb;

/// Storage-size snapshot returned by [`GraphDb::stats`] (SPA-171).
pub use db::DbStats;

/// WAL segment migration from legacy v21 CRC32 to v2 CRC32C format.
pub use db::migrate_wal;

/// A read-only snapshot transaction.
pub use read_tx::ReadTx;

/// A write transaction.
pub use write_tx::WriteTx;

/// Query result returned by [`GraphDb::execute`] and [`GraphDb::execute_write`].
/// Contains column names and rows of scalar [`Value`] cells.
pub use sparrowdb_execution::QueryResult;

/// Scalar value type used in query results and node property reads/writes.
pub use sparrowdb_storage::node_store::Value;

/// Opaque 64-bit node identifier.
pub use sparrowdb_common::NodeId;

/// Opaque 64-bit edge identifier.
pub use sparrowdb_common::EdgeId;

/// Error type returned by all SparrowDB operations.
pub use sparrowdb_common::Error;

/// Convenience alias: `std::result::Result<T, sparrowdb::Error>`.
pub use sparrowdb_common::Result;

// ── Public helpers ────────────────────────────────────────────────────────────

/// Derive a stable `u32` column ID from a property key name.
pub use helpers::fnv1a_col_id;

/// Escape a Rust `&str` for safe interpolation inside a Cypher single-quoted string.
pub use helpers::cypher_escape_string;

// ── Legacy alias ──────────────────────────────────────────────────────────────

/// Legacy alias kept for backward compatibility with Phase 0 tests.
pub type SparrowDB = GraphDb;

// ── Top-level convenience function ───────────────────────────────────────────

use std::path::Path;

/// Convenience wrapper — equivalent to [`GraphDb::open`].
pub fn open(path: &Path) -> Result<GraphDb> {
    GraphDb::open(path)
}
