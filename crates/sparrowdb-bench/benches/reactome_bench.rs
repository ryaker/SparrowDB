//! Criterion benchmarks for Reactome biological-pathway queries — issue #301.
//!
//! Benchmarks Q3/Q4/Q8-equivalent queries against a synthetic Reactome-shaped
//! graph for comparison against TuringDB's PathExplorer results.
//!
//! Run with:
//! ```text
//! cargo bench -p sparrowdb-bench --bench reactome_bench
//! ```

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::GraphDb;
use sparrowdb_bench::realworld::{self, ReactomeConfig};

/// Build a Reactome graph and return `(TempDir, GraphDb, first_pathway_pid)`.
/// The `TempDir` must stay alive for the database to remain valid.
fn build_reactome_db() -> (tempfile::TempDir, GraphDb, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let cfg = ReactomeConfig::default();
    let stats = realworld::load_reactome(&db, &cfg).expect("load reactome");
    assert!(stats.nodes_created > 0, "should create nodes");
    assert!(stats.edges_created > 0, "should create edges");

    db.checkpoint().expect("checkpoint");

    // First Pathway has pid = entities + 1 (entities come first in generation).
    let first_pathway_pid = cfg.entities as u64 + 1;
    (dir, db, first_pathway_pid)
}

fn bench_reactome_q3(c: &mut Criterion) {
    let (_dir, db, first_pid) = build_reactome_db();

    c.bench_function("reactome_q3_1hop_components", |b| {
        b.iter(|| {
            let result = realworld::q3_pathway_components(&db, first_pid);
            std::hint::black_box(result)
        });
    });
}

fn bench_reactome_q4(c: &mut Criterion) {
    let (_dir, db, first_pid) = build_reactome_db();
    // Sub-pathway is the node immediately after the parent Pathway.
    let sub_pid = first_pid + 1;

    c.bench_function("reactome_q4_2hop_reactions", |b| {
        b.iter(|| {
            let result = realworld::q4_pathway_reactions_2hop(&db, sub_pid);
            std::hint::black_box(result)
        });
    });
}

fn bench_reactome_q8(c: &mut Criterion) {
    let (_dir, db, first_pid) = build_reactome_db();
    // Sub-pathway is one node after the Pathway (generated in pairs).
    let sub_pid = first_pid + 1;

    c.bench_function("reactome_q8_shared_catalysts", |b| {
        b.iter(|| {
            let result = realworld::q8_shared_catalysts(&db, sub_pid);
            std::hint::black_box(result)
        });
    });
}

criterion_group!(
    reactome_benches,
    bench_reactome_q3,
    bench_reactome_q4,
    bench_reactome_q8
);
criterion_main!(reactome_benches);
