//! Prometheus metrics exporter for SparrowDB runtime (SPA-154).
//!
//! ## Quick start
//!
//! ```no_run
//! use sparrowdb::GraphDb;
//! use sparrowdb_metrics::{MetricsRegistry, MetricsServer};
//! use std::sync::Arc;
//!
//! let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
//! let registry = Arc::new(MetricsRegistry::new(db.clone()));
//!
//! // Execute queries through the registry so counters are bumped.
//! registry.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
//!
//! // Spawn the HTTP server on :9091 — serves /metrics in Prometheus text format.
//! let server = MetricsServer::new(Arc::clone(&registry), "127.0.0.1:9091").unwrap();
//! let _handle = server.spawn(); // background thread
//! ```

use sparrowdb::{GraphDb, QueryResult};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ── Histogram bucket upper bounds (seconds) ───────────────────────────────────

/// Bucket upper bounds for query duration histogram (in seconds).
const DURATION_BUCKETS: &[f64] = &[
    0.000_1, // 0.1 ms
    0.000_5, // 0.5 ms
    0.001,   // 1 ms
    0.005,   // 5 ms
    0.010,   // 10 ms
    0.050,   // 50 ms
    0.100,   // 100 ms
    0.500,   // 500 ms
    1.0,     // 1 s
    5.0,     // 5 s
];

// ── Operation labels ──────────────────────────────────────────────────────────

/// Labels for query operations tracked by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Match = 0,
    Create = 1,
    Delete = 2,
    Merge = 3,
    Other = 4,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Operation::Match => "match",
            Operation::Create => "create",
            Operation::Delete => "delete",
            Operation::Merge => "merge",
            Operation::Other => "other",
        }
    }
}

/// All variants in index order for iteration.
const ALL_OPS: &[Operation] = &[
    Operation::Match,
    Operation::Create,
    Operation::Delete,
    Operation::Merge,
    Operation::Other,
];

const N_OPS: usize = 5;

// ── Histogram ─────────────────────────────────────────────────────────────────

/// A single Prometheus-style histogram for one operation label.
///
/// Buckets are cumulative (each count includes all observations ≤ upper bound).
#[derive(Debug)]
struct Histogram {
    /// `bucket_counts[i]` is the cumulative count for `DURATION_BUCKETS[i]`.
    /// Index `DURATION_BUCKETS.len()` is the +Inf bucket (== total count).
    bucket_counts: Box<[AtomicU64]>,
    /// Sum of all observed values stored as integer microseconds to avoid
    /// floating-point non-atomicity; convert to seconds on render.
    sum_us: AtomicU64,
    /// Total observation count.
    count: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        let n = DURATION_BUCKETS.len() + 1; // +Inf bucket
        let bucket_counts = (0..n)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Histogram {
            bucket_counts,
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one observation (duration in seconds).
    fn observe(&self, seconds: f64) {
        // Increment every bucket whose upper bound >= observed value.
        for (i, &bound) in DURATION_BUCKETS.iter().enumerate() {
            if seconds <= bound {
                self.bucket_counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket always incremented.
        let inf_idx = DURATION_BUCKETS.len();
        self.bucket_counts[inf_idx].fetch_add(1, Ordering::Relaxed);

        // Accumulate sum (integer microseconds, max ~584 years before overflow).
        let us = (seconds * 1_000_000.0) as u64;
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn bucket_count(&self, idx: usize) -> u64 {
        self.bucket_counts[idx].load(Ordering::Relaxed)
    }

    fn sum_seconds(&self) -> f64 {
        self.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

// ── MetricsRegistry ───────────────────────────────────────────────────────────

/// Thread-safe registry of all SparrowDB Prometheus metrics.
///
/// Wrap in an [`Arc`] and share between the query path and the HTTP server
/// thread.  [`GraphDb`] is cheaply cloneable (wraps an `Arc` internally), so
/// holding a clone here adds no overhead.
pub struct MetricsRegistry {
    /// `sparrowdb_query_total{operation="..."}` counters.
    query_total: [AtomicU64; N_OPS],
    /// `sparrowdb_query_duration_seconds{operation="..."}` histograms.
    query_duration: [Histogram; N_OPS],
    /// Database handle used for computing storage/WAL gauges on render.
    db: GraphDb,
}

impl MetricsRegistry {
    /// Create a new registry bound to `db`.
    pub fn new(db: GraphDb) -> Self {
        MetricsRegistry {
            query_total: std::array::from_fn(|_| AtomicU64::new(0)),
            query_duration: std::array::from_fn(|_| Histogram::new()),
            db,
        }
    }

    /// Execute a Cypher query, recording operation count and latency.
    ///
    /// This is a thin wrapper around [`GraphDb::execute`] that bumps the
    /// relevant counters before returning the result unchanged.
    pub fn execute(&self, cypher: &str) -> sparrowdb::Result<QueryResult> {
        let op = classify_query(cypher);
        let idx = op as usize;

        let start = Instant::now();
        let result = self.db.execute(cypher);
        let elapsed = start.elapsed().as_secs_f64();

        // Record metrics unconditionally (even on error the latency matters).
        self.query_total[idx].fetch_add(1, Ordering::Relaxed);
        self.query_duration[idx].observe(elapsed);

        result
    }

    /// Return the count for a specific operation (used in tests).
    pub fn query_count(&self, op: Operation) -> u64 {
        self.query_total[op as usize].load(Ordering::Relaxed)
    }

    /// Render all metrics in Prometheus exposition text format (UTF-8).
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);

        self.render_query_total(&mut out);
        self.render_query_duration(&mut out);
        self.render_db_gauges(&mut out);
        self.render_wal_gauge(&mut out);
        self.render_storage_gauge(&mut out);

        out
    }

    // ── Section renderers ─────────────────────────────────────────────────────

    fn render_query_total(&self, out: &mut String) {
        out.push_str("# HELP sparrowdb_query_total Total number of queries executed\n");
        out.push_str("# TYPE sparrowdb_query_total counter\n");
        for (i, op) in ALL_OPS.iter().enumerate() {
            let val = self.query_total[i].load(Ordering::Relaxed);
            out.push_str(&format!(
                "sparrowdb_query_total{{operation=\"{}\"}} {}\n",
                op.as_str(),
                val
            ));
        }
    }

    fn render_query_duration(&self, out: &mut String) {
        out.push_str("# HELP sparrowdb_query_duration_seconds Query execution time in seconds\n");
        out.push_str("# TYPE sparrowdb_query_duration_seconds histogram\n");
        for (i, op) in ALL_OPS.iter().enumerate() {
            let hist = &self.query_duration[i];
            let label = op.as_str();

            for (bi, &bound) in DURATION_BUCKETS.iter().enumerate() {
                out.push_str(&format!(
                    "sparrowdb_query_duration_seconds_bucket{{operation=\"{}\",le=\"{:.4}\"}} {}\n",
                    label,
                    bound,
                    hist.bucket_count(bi)
                ));
            }
            // +Inf bucket
            let inf_idx = DURATION_BUCKETS.len();
            out.push_str(&format!(
                "sparrowdb_query_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}\n",
                label,
                hist.bucket_count(inf_idx)
            ));
            out.push_str(&format!(
                "sparrowdb_query_duration_seconds_sum{{operation=\"{}\"}} {:.6}\n",
                label,
                hist.sum_seconds()
            ));
            out.push_str(&format!(
                "sparrowdb_query_duration_seconds_count{{operation=\"{}\"}} {}\n",
                label,
                hist.count()
            ));
        }
    }

    fn render_db_gauges(&self, out: &mut String) {
        if let Ok((node_count, edge_count)) = self.db.db_counts() {
            out.push_str(
                "# HELP sparrowdb_node_count Total node count (approximate, all labels)\n",
            );
            out.push_str("# TYPE sparrowdb_node_count gauge\n");
            out.push_str(&format!("sparrowdb_node_count {}\n", node_count));

            out.push_str("# HELP sparrowdb_edge_count Total edge count (all rel types)\n");
            out.push_str("# TYPE sparrowdb_edge_count gauge\n");
            out.push_str(&format!("sparrowdb_edge_count {}\n", edge_count));
        }
    }

    fn render_wal_gauge(&self, out: &mut String) {
        let wal_dir = self.db.path().join("wal");
        let depth = wal_depth(&wal_dir);
        out.push_str(
            "# HELP sparrowdb_wal_entries_total Approximate WAL entry count (LSN depth)\n",
        );
        out.push_str("# TYPE sparrowdb_wal_entries_total gauge\n");
        out.push_str(&format!("sparrowdb_wal_entries_total {}\n", depth));
    }

    fn render_storage_gauge(&self, out: &mut String) {
        let bytes = dir_size(self.db.path());
        out.push_str(
            "# HELP sparrowdb_storage_bytes On-disk size of the database directory in bytes\n",
        );
        out.push_str("# TYPE sparrowdb_storage_bytes gauge\n");
        out.push_str(&format!("sparrowdb_storage_bytes {}\n", bytes));
    }
}

// ── MetricsServer ─────────────────────────────────────────────────────────────

/// HTTP server that serves `/metrics` in Prometheus exposition text format.
///
/// Spawns a background OS thread on [`MetricsServer::spawn`].
pub struct MetricsServer {
    registry: Arc<MetricsRegistry>,
    server: tiny_http::Server,
}

impl MetricsServer {
    /// Bind the server to `addr` (e.g. `"127.0.0.1:9091"`).
    pub fn new(registry: Arc<MetricsRegistry>, addr: &str) -> std::io::Result<Self> {
        let server =
            tiny_http::Server::http(addr).map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(MetricsServer { registry, server })
    }

    /// Return the local socket address the server is listening on.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.server
            .server_addr()
            .to_ip()
            .expect("metrics server uses TCP/IP")
    }

    /// Spawn a background thread that serves requests until the server is dropped.
    ///
    /// Returns the [`std::thread::JoinHandle`] — callers may drop it (detach)
    /// or join it at shutdown.
    pub fn spawn(self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            tracing::info!(
                addr = %self.local_addr(),
                "sparrowdb-metrics: HTTP server listening"
            );
            loop {
                match self.server.recv() {
                    Ok(req) => {
                        let path = req.url().to_owned();
                        if path == "/metrics" || path == "/metrics/" {
                            let body = self.registry.render();
                            let response = tiny_http::Response::from_string(body)
                                .with_header(
                                    "Content-Type: text/plain; version=0.0.4; charset=utf-8"
                                        .parse::<tiny_http::Header>()
                                        .expect("static header is valid"),
                                )
                                .with_status_code(200);
                            if let Err(e) = req.respond(response) {
                                tracing::warn!("sparrowdb-metrics: response error: {e}");
                            }
                        } else {
                            let response = tiny_http::Response::from_string("404 not found\n")
                                .with_status_code(404);
                            let _ = req.respond(response);
                        }
                    }
                    Err(e) => {
                        tracing::error!("sparrowdb-metrics: recv error: {e}");
                        break;
                    }
                }
            }
        })
    }
}

// ── Public helpers ────────────────────────────────────────────────────────────

/// Classify a Cypher query string into an [`Operation`] variant.
///
/// Inspects the first non-whitespace keyword only (e.g. `MATCH … DELETE` is
/// classified as `Match`).  O(n) scan with no heap allocation.
pub fn classify_query(cypher: &str) -> Operation {
    let s = cypher.trim_start();
    // Slice off the first alphabetic run, then compare case-insensitively.
    let end = s.find(|c: char| !c.is_alphabetic()).unwrap_or(s.len());
    let first_word = &s[..end];
    if first_word.eq_ignore_ascii_case("MATCH") {
        Operation::Match
    } else if first_word.eq_ignore_ascii_case("CREATE") {
        Operation::Create
    } else if first_word.eq_ignore_ascii_case("MERGE") {
        Operation::Merge
    } else if first_word.eq_ignore_ascii_case("DELETE") || first_word.eq_ignore_ascii_case("DETACH")
    {
        Operation::Delete
    } else {
        Operation::Other
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Approximate WAL depth from the WAL directory file sizes.
///
/// Each WAL record has a minimum 32-byte framing header, so we divide total
/// WAL file sizes by 32 to get an upper-bound entry count.
fn wal_depth(wal_dir: &Path) -> u64 {
    if !wal_dir.exists() {
        return 0;
    }
    let Ok(entries) = std::fs::read_dir(wal_dir) else {
        return 0;
    };
    let total_bytes: u64 = entries
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum();
    total_bytes.saturating_div(32)
}

/// Recursively sum file sizes under `dir`.
fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let Ok(meta) = e.metadata() else {
                tracing::warn!(entry = ?e.path(), "sparrowdb-metrics: failed to read metadata in dir_size");
                return 0;
            };
            if meta.is_dir() {
                dir_size(&e.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn open_registry(dir: &TempDir) -> Arc<MetricsRegistry> {
        let db = GraphDb::open(dir.path()).expect("open db");
        Arc::new(MetricsRegistry::new(db))
    }

    // ── Test 1: counters increment correctly ──────────────────────────────────

    /// Counter increments once per query, segregated by operation type.
    #[test]
    fn test_counter_increments_on_query() {
        let dir = TempDir::new().unwrap();
        let reg = open_registry(&dir);

        // Two CREATE queries.
        reg.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        reg.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        // One MATCH query.
        reg.execute("MATCH (n:Person) RETURN n").unwrap();

        assert_eq!(reg.query_count(Operation::Create), 2, "create count");
        assert_eq!(reg.query_count(Operation::Match), 1, "match count");
        assert_eq!(reg.query_count(Operation::Merge), 0, "merge should be 0");
    }

    // ── Test 2: histogram records duration ────────────────────────────────────

    /// Histogram sum and count are non-zero after an observation.
    #[test]
    fn test_histogram_records_duration() {
        let dir = TempDir::new().unwrap();
        let reg = open_registry(&dir);

        reg.execute("CREATE (n:Item {v: 1})").unwrap();

        let hist = &reg.query_duration[Operation::Create as usize];
        assert_eq!(hist.count(), 1, "histogram count");
        assert!(hist.sum_seconds() >= 0.0, "sum non-negative");

        // +Inf bucket must equal count.
        let inf_idx = DURATION_BUCKETS.len();
        assert_eq!(
            hist.bucket_count(inf_idx),
            1,
            "+Inf bucket must equal count"
        );
    }

    // ── Test 3: /metrics endpoint returns HTTP 200 ────────────────────────────

    /// Spawning a MetricsServer and requesting /metrics returns 200 OK with
    /// Prometheus body containing the expected metric names.
    #[test]
    fn test_metrics_endpoint_returns_200() {
        let dir = TempDir::new().unwrap();
        let reg = open_registry(&dir);

        // Insert one node so db_counts returns something.
        reg.execute("CREATE (n:Test {x: 42})").unwrap();

        // Bind on an OS-assigned ephemeral port.
        let server = MetricsServer::new(Arc::clone(&reg), "127.0.0.1:0").unwrap();
        let port = server.local_addr().port();

        // Spawn server in background thread.
        let _handle = server.spawn();

        // Give the thread a moment to enter recv().
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Send a raw HTTP/1.0 request (no keep-alive complications).
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(stream, "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
        stream.flush().unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();

        assert!(
            response.contains("200 OK"),
            "expected 200 OK, got: {response}"
        );
        assert!(
            response.contains("sparrowdb_query_total"),
            "expected sparrowdb_query_total in body"
        );
        assert!(
            response.contains("sparrowdb_node_count"),
            "expected sparrowdb_node_count in body"
        );
    }

    // ── Test 4: classify_query correctness ───────────────────────────────────

    /// classify_query maps keywords to the right operation variants.
    #[test]
    fn test_classify_query() {
        assert_eq!(classify_query("MATCH (n) RETURN n"), Operation::Match);
        assert_eq!(classify_query("  create (n:Person)"), Operation::Create);
        assert_eq!(classify_query("MERGE (n:X {id: 1})"), Operation::Merge);
        assert_eq!(classify_query("DETACH DELETE (n)"), Operation::Delete);
        assert_eq!(classify_query("CHECKPOINT"), Operation::Other);
        assert_eq!(classify_query("OPTIMIZE"), Operation::Other);
    }

    // ── Test 5: render output is valid Prometheus text format ─────────────────

    /// render() output contains all required metric families.
    #[test]
    fn test_render_contains_all_metric_families() {
        let dir = TempDir::new().unwrap();
        let reg = open_registry(&dir);
        reg.execute("CREATE (a:X)").unwrap();
        reg.execute("MATCH (a:X) RETURN a").unwrap();

        let output = reg.render();

        let required = [
            "sparrowdb_query_total",
            "sparrowdb_query_duration_seconds_bucket",
            "sparrowdb_query_duration_seconds_sum",
            "sparrowdb_query_duration_seconds_count",
            "sparrowdb_node_count",
            "sparrowdb_edge_count",
            "sparrowdb_wal_entries_total",
            "sparrowdb_storage_bytes",
        ];
        for metric in required {
            assert!(
                output.contains(metric),
                "missing metric family '{metric}' in render output"
            );
        }
    }
}
