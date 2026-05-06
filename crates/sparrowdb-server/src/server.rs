//! HTTP server orchestrator.

use crate::auth::AuthConfig;
use crate::handlers;
use sparrowdb::GraphDb;
use std::net::SocketAddr;

/// Server runtime configuration (auth + worker thread count).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Authentication policy.
    pub auth: AuthConfig,
    /// Number of worker threads handling requests.  Defaults to
    /// `min(num_cpus, 8)`.
    pub workers: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get().min(8))
            .unwrap_or(4);
        Self {
            auth: AuthConfig::None,
            workers,
        }
    }
}

/// HTTP server that routes Cypher queries to a [`GraphDb`].
///
/// The server is single-threaded for `accept`, with a fixed pool of worker
/// threads pulling requests off the shared `tiny_http::Server` queue.
pub struct Server {
    inner: tiny_http::Server,
    addr: SocketAddr,
    config: ServerConfig,
}

impl Server {
    /// Bind a server to `addr` with the given config.
    ///
    /// Validates the auth policy against the bind address — refuses
    /// `--no-auth` on any non-loopback interface.
    pub fn new(addr: SocketAddr, config: ServerConfig) -> Result<Self, String> {
        config
            .auth
            .validate_against_bind(addr)
            .map_err(|e| e.to_string())?;

        let inner = tiny_http::Server::http(addr).map_err(|e| e.to_string())?;
        let bound = inner
            .server_addr()
            .to_ip()
            .ok_or_else(|| "tiny_http did not return an IP socket".to_string())?;

        Ok(Self {
            inner,
            addr: bound,
            config,
        })
    }

    /// Return the resolved local socket address (useful when port `0` was
    /// supplied to bind to a random free port — common in tests).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Serve requests forever, blocking the caller.
    ///
    /// Internally spawns `config.workers` OS threads; each pulls requests off
    /// the shared queue and dispatches them to [`handlers::dispatch`].  The
    /// underlying `GraphDb` is cloned for each worker so SWMR semantics are
    /// preserved.
    pub fn serve(self, db: GraphDb) {
        tracing::info!(addr = %self.addr, workers = self.config.workers, "sparrowdb-server: HTTP listening");

        let server = std::sync::Arc::new(self.inner);
        let auth = std::sync::Arc::new(self.config.auth);

        let mut handles = Vec::with_capacity(self.config.workers);
        for i in 0..self.config.workers {
            let server = std::sync::Arc::clone(&server);
            let auth = std::sync::Arc::clone(&auth);
            let db = db.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(format!("sparrowdb-http-{i}"))
                    .spawn(move || loop {
                        match server.recv() {
                            Ok(req) => handlers::dispatch(req, &db, &auth),
                            Err(e) => {
                                tracing::error!("sparrowdb-server: recv error: {e}");
                                break;
                            }
                        }
                    })
                    .expect("spawn worker thread"),
            );
        }

        // Block on workers — `tiny_http::Server::recv` only returns an error
        // when the server is dropped, so the workers exit together.
        for h in handles {
            let _ = h.join();
        }
    }
}
