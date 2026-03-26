//! Criterion benchmarks for LDBC SNB queries on a synthetic mini dataset.
//!
//! The benchmark generates a small in-memory LDBC-style graph (the `mini`
//! fixture data) and times IC1 and IC2 query execution.
//!
//! Run with:
//! ```text
//! cargo bench -p sparrowdb-bench --bench ldbc_snb
//! ```

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::GraphDb;
use sparrowdb_bench::ic_queries;
use std::path::PathBuf;

/// Build a SparrowDB from the mini LDBC fixture CSVs.
///
/// Returns `(TempDir, GraphDb)` — the `TempDir` must be kept alive for the
/// database to remain valid.
fn load_mini_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("ldbc")
        .join("mini");

    let stats = sparrowdb_bench::load(&db, &fixture_dir).expect("load mini LDBC data");
    assert!(stats.nodes_loaded > 0, "should load at least one node");
    assert!(stats.edges_loaded > 0, "should load at least one edge");

    db.checkpoint().expect("checkpoint after load");
    (dir, db)
}

fn bench_ic1(c: &mut Criterion) {
    let (_dir, db) = load_mini_db();

    c.bench_function("ldbc_ic1_friends_named_Bob", |b| {
        b.iter(|| {
            let result = ic_queries::ic1_friends_named(&db, "Bob");
            // Force the result to be evaluated, not optimized away.
            std::hint::black_box(result)
        });
    });

    c.bench_function("ldbc_ic1_friends_named_Alice", |b| {
        b.iter(|| {
            let result = ic_queries::ic1_friends_named(&db, "Alice");
            std::hint::black_box(result)
        });
    });
}

fn bench_ic2(c: &mut Criterion) {
    let (_dir, db) = load_mini_db();

    c.bench_function("ldbc_ic2_recent_friends_person_1", |b| {
        b.iter(|| {
            let result = ic_queries::ic2_recent_friends(&db, 1);
            std::hint::black_box(result)
        });
    });

    c.bench_function("ldbc_ic2_recent_friends_person_3", |b| {
        b.iter(|| {
            let result = ic_queries::ic2_recent_friends(&db, 3);
            std::hint::black_box(result)
        });
    });
}

criterion_group!(ldbc_snb, bench_ic1, bench_ic2);
criterion_main!(ldbc_snb);
