//! `sparrowdb-bench` — benchmarks and data loaders for SparrowDB.
//!
//! Public API re-exported from the LDBC SNB loader module.

pub mod ic_queries;
pub mod ldbc;
pub mod mlm;
pub mod realworld;

// Re-export the most-used items at crate root for convenience.
pub use ldbc::{label_for, load, load_edge_file, load_node_file, IdMap, LoadStats, BATCH_SIZE};
