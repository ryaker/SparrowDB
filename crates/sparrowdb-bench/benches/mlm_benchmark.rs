//! Criterion benchmarks for MLM referral-tree queries — SPA-206.
//!
//! Generates a small (200-member) MLM tree and benchmarks the five standard
//! MLM query patterns: downline volume, level-3 slice, upline path, subtree
//! rollup, and top-recruiter ranking.
//!
//! Run with:
//! ```text
//! cargo bench -p sparrowdb-bench --bench mlm_benchmark
//! ```

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::GraphDb;
use sparrowdb_bench::mlm::{self, MlmConfig};

/// Build a small MLM graph for benchmarking.
///
/// Returns `(TempDir, GraphDb)` — `TempDir` must stay alive.
fn build_mlm_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let cfg = MlmConfig {
        members: 200,
        max_depth: 6,
        avg_fanout: 3,
        seed: 42,
    };

    let stats = mlm::generate(&db, &cfg).expect("generate MLM tree");
    assert!(stats.nodes_created > 0, "should create members");
    assert!(stats.edges_created > 0, "should create edges");

    db.checkpoint().expect("checkpoint");
    (dir, db)
}

fn bench_mlm_queries(c: &mut Criterion) {
    let (_dir, db) = build_mlm_db();

    // Root is uid=1; pick a mid-tree node as well.
    let root_uid = 1;
    let mid_uid = 5;

    c.bench_function("mlm_q1_downline_volume_root", |b| {
        b.iter(|| {
            let result = mlm::q_mlm1_downline_volume(&db, root_uid);
            std::hint::black_box(result)
        });
    });

    c.bench_function("mlm_q1_downline_volume_mid", |b| {
        b.iter(|| {
            let result = mlm::q_mlm1_downline_volume(&db, mid_uid);
            std::hint::black_box(result)
        });
    });

    c.bench_function("mlm_q2_level3_downline_root", |b| {
        b.iter(|| {
            let result = mlm::q_mlm2_level3_downline(&db, root_uid);
            std::hint::black_box(result)
        });
    });

    c.bench_function("mlm_q3_upline_path_mid", |b| {
        b.iter(|| {
            let result = mlm::q_mlm3_upline_path(&db, mid_uid);
            std::hint::black_box(result)
        });
    });

    c.bench_function("mlm_q4_subtree_volume_root", |b| {
        b.iter(|| {
            let result = mlm::q_mlm4_subtree_volume(&db, root_uid);
            std::hint::black_box(result)
        });
    });

    c.bench_function("mlm_q5_top_recruiters", |b| {
        b.iter(|| {
            let result = mlm::q_mlm5_top_recruiters(&db);
            std::hint::black_box(result)
        });
    });
}

criterion_group!(mlm_benches, bench_mlm_queries);
criterion_main!(mlm_benches);
