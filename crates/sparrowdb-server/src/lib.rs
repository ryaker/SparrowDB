//! SparrowDB HTTP server library (Phase A).
//!
//! Provides a synchronous JSON HTTP API that routes Cypher queries to a
//! [`sparrowdb::GraphDb`] instance.  Built on top of `tiny_http` to match the
//! single-writer multi-reader (SWMR) engine model — every request runs on a
//! worker thread with a cloned `GraphDb` handle.
//!
//! # Routes
//!
//! | Method | Path        | Auth | Description                                    |
//! |--------|-------------|------|------------------------------------------------|
//! | GET    | `/health`   | no   | Liveness probe; returns `{ "status": "ok", … }` |
//! | GET    | `/info`     | yes  | DB metadata: labels, rel types, node/edge counts |
//! | POST   | `/cypher`   | yes  | Execute a Cypher query, returns columns + rows |
//!
//! # Auth
//!
//! Requests must carry `Authorization: Bearer <token>` unless the server was
//! constructed with [`AuthConfig::None`] **and** bound to a loopback address.
//!
//! # Streaming
//!
//! Phase A buffers the full result set into a JSON document.  SSE / chunked
//! NDJSON streaming is Phase B.

pub mod auth;
pub mod handlers;
pub mod server;

pub use auth::{is_loopback_addr, AuthConfig};
pub use server::{Server, ServerConfig};

/// The crate version, used by `/health`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
