//! Criterion benchmarks for MATCH-by-property-index + CREATE edge throughput.
//!
//! These benchmarks catch O(N) scan regressions in the property index path.
//! The correctness test lives in tests/match_property_index.rs; only timing
//! lives here so CI is not flaky under machine load.
//!
//! ## Run
//!
//!   cargo bench --bench property_index_benchmarks
//!
//! ## Compile-check only
//!
//!   cargo bench --bench property_index_benchmarks -- --test

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::GraphDb;
use tempfile::TempDir;

/// Smaller node/edge counts than the original test — enough to distinguish
/// O(1) indexed lookup from O(N) scan without making each bench iteration slow.
const N_NODES: i64 = 1_000;
const N_EDGES: i64 = 200;

fn setup_db() -> (TempDir, GraphDb) {
    let dir = TempDir::new().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    for uid in 1..=N_NODES {
        db.execute(&format!("CREATE (:User {{uid: {uid}}})"))
            .unwrap();
    }
    (dir, db)
}

fn bench_match_create_edges(c: &mut Criterion) {
    c.bench_function("match_create_edges_indexed", |b| {
        b.iter_with_setup(setup_db, |(_dir, db)| {
            for i in 1..=N_EDGES {
                let dst = if i < N_NODES { i + 1 } else { 1 };
                db.execute(&format!(
                    "MATCH (a:User {{uid: {i}}}), (b:User {{uid: {dst}}}) CREATE (a)-[:EDGE]->(b)"
                ))
                .unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_match_create_edges);
criterion_main!(benches);
