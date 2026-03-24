//! `sparrowdb-bench` — benchmarks and data loaders for SparrowDB.
//!
//! Public API re-exported from the LDBC SNB loader module.

pub mod ldbc;

// Re-export the most-used items at crate root for convenience.
pub use ldbc::{label_for, load, load_edge_file, load_node_file, IdMap, LoadStats, BATCH_SIZE};
