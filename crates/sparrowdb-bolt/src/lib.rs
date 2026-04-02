//! SparrowDB Bolt Protocol Server library.
//!
//! Provides a Bolt v4.x TCP server that routes Cypher queries to a SparrowDB
//! database instance.

pub mod handler;
pub mod packstream;

pub use handler::handle_connection;
