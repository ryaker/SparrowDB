//! Phase 2 criterion benchmark suite for the chunked vectorized pipeline (SPA-299, #338).
//!
//! Benchmarks the 1-hop fast path added in Phase 2 versus the row-at-a-time engine
//! on a 1000-node Person graph with ring KNOWS edges.
//!
//! # Baseline (SPA-299 spec §1)
//!
//! // v0.1.12 baseline: ~42ms for IC3 1-hop+filter on SF-01
//!
//! # Run (compile-check only — used in CI):
//!
//! ```text
//! cargo bench --bench pipeline_phase2_benchmarks -- --test
//! ```
//!
//! # Run for real measurements:
//!
//! ```text
//! cargo bench --bench pipeline_phase2_benchmarks
//! ```
//!
//! HTML reports land in `target/criterion/`.

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::{GraphDb, Value};
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;
use std::collections::HashMap;

/// Encode a string as the inline-bytes `Value` used by NodeStore.
#[inline]
fn str_val(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

/// Encode an i64 as `Value::Int64`.
#[inline]
fn int_val(n: i64) -> Value {
    Value::Int64(n)
}

// ── Constants ─────────────────────────────────────────────────────────────────

/// Number of nodes / edges in the benchmark graph.
const N: usize = 1_000;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open GraphDb");
    (dir, db)
}

/// Build a 1000-node Person ring graph with KNOWS edges (i → i+1 mod N).
///
/// Returns the tempdir (keeps DB alive) and the opened database.
fn build_knows_ring() -> (tempfile::TempDir, GraphDb) {
    let (dir, db) = fresh_db();

    let mut node_ids = Vec::with_capacity(N);
    let mut tx = db.begin_write().expect("begin_write nodes");
    let label_id = tx.get_or_create_label_id("Person").expect("label");

    for i in 0..N {
        let age = (20 + (i % 60)) as i64;
        let nid = tx
            .create_node_named(
                label_id,
                &[
                    ("pid".to_string(), int_val(i as i64)),
                    ("name".to_string(), str_val(&format!("P{i}"))),
                    ("age".to_string(), int_val(age)),
                ],
            )
            .expect("create_node_named");
        node_ids.push(nid);
    }
    tx.commit().expect("commit nodes");

    // Create ring edges in chunks of 100.
    let chunk_size = 100;
    for chunk_start in (0..N).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(N);
        let mut tx = db.begin_write().expect("begin_write edges");
        for i in chunk_start..chunk_end {
            let j = (i + 1) % N;
            tx.create_edge(node_ids[i], node_ids[j], "KNOWS", HashMap::new())
                .expect("create_edge");
        }
        tx.commit().expect("commit edges");
    }

    (dir, db)
}

/// Open a CSR map from all rel tables in the database.
fn open_csr_map(path: &std::path::Path) -> HashMap<u32, CsrForward> {
    use sparrowdb_storage::edge_store::{EdgeStore, RelTableId};

    let catalog = match Catalog::open(path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };
    let mut map = HashMap::new();
    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();
    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }
    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    map
}

// ── bench_one_hop_phase2 ──────────────────────────────────────────────────────

/// Compare the Phase 2 chunked pipeline vs. row-at-a-time for a 1-hop traversal
/// with an integer filter on the destination node:
///
/// `MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 RETURN a.pid, b.pid`
///
/// This mirrors IC3 1-hop + filter, the primary Phase 2 target.
/// v0.1.12 baseline: ~42ms for IC3 1-hop+filter on SF-01
fn bench_one_hop_phase2(c: &mut Criterion) {
    let (dir, db) = build_knows_ring();
    let path = dir.path().to_path_buf();

    let mut group = c.benchmark_group("phase2_pipeline");

    // Row-at-a-time engine baseline.
    group.bench_function("one_hop_filter_row_engine", |b| {
        b.iter(|| {
            let result = db
                .execute(
                    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 \
                     RETURN a.pid, b.pid",
                )
                .expect("row engine 1-hop");
            criterion::black_box(result);
        });
    });

    // Phase 2 chunked pipeline.
    group.bench_function("one_hop_filter_chunked_pipeline", |b| {
        b.iter(|| {
            let store = NodeStore::open(&path).expect("NodeStore");
            let cat = Catalog::open(&path).expect("Catalog");
            let csrs = open_csr_map(&path);
            let result = Engine::new(store, cat, csrs, &path)
                .with_chunked_pipeline()
                .execute(
                    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 \
                     RETURN a.pid, b.pid",
                )
                .expect("chunked pipeline 1-hop");
            criterion::black_box(result);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_one_hop_phase2);
criterion_main!(benches);
