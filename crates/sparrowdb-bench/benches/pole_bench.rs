//! Criterion benchmarks for POLE investigation-graph queries — issue #301.
//!
//! Benchmarks Q3/Q4/Q8-equivalent queries on a dense multi-hop POLE graph
//! (Person-Object-Location-Event) for TuringDB PathExplorer comparison.
//!
//! Run with:
//! ```text
//! cargo bench -p sparrowdb-bench --bench pole_bench
//! ```

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::GraphDb;
use sparrowdb_bench::realworld::{self, PoleConfig};

/// Build a POLE graph and return `(TempDir, GraphDb, person_base_nid)`.
/// The `TempDir` must stay alive for the database to remain valid.
fn build_pole_db() -> (tempfile::TempDir, GraphDb, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let cfg = PoleConfig::default();
    let stats = realworld::load_pole(&db, &cfg).expect("load POLE");
    assert!(stats.nodes_created > 0, "should create nodes");
    assert!(stats.edges_created > 0, "should create edges");

    db.checkpoint().expect("checkpoint");

    // Persons start at nid=1.
    let person_base = 1_u64;
    (dir, db, person_base)
}

fn bench_pole_q3(c: &mut Criterion) {
    let (_dir, db, person_base) = build_pole_db();

    c.bench_function("pole_q3_knows_1hop", |b| {
        b.iter(|| {
            let result = realworld::q3_knows_1hop(&db, person_base);
            std::hint::black_box(result)
        });
    });
}

fn bench_pole_q4(c: &mut Criterion) {
    let (_dir, db, person_base) = build_pole_db();

    c.bench_function("pole_q4_knows_2hop", |b| {
        b.iter(|| {
            let result = realworld::q4_knows_2hop(&db, person_base);
            std::hint::black_box(result)
        });
    });
}

fn bench_pole_q8(c: &mut Criterion) {
    let (_dir, db, person_base) = build_pole_db();

    c.bench_function("pole_q8_co_party_events", |b| {
        b.iter(|| {
            let result = realworld::q8_co_party_events(&db, person_base);
            std::hint::black_box(result)
        });
    });
}

criterion_group!(pole_benches, bench_pole_q3, bench_pole_q4, bench_pole_q8);
criterion_main!(pole_benches);
