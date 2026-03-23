//! SPA-250 — Criterion benchmarks for `execute_batch` vs individual `execute`.
//!
//! ## Benchmarks
//!
//!   1. `bench_100_individual_creates` — 100 separate `execute()` calls,
//!      each opening a WriteTx and fsyncing the WAL independently.
//!   2. `bench_100_batch_creates` — same 100 CREATE statements via a single
//!      `execute_batch()` call with one WAL fsync.
//!
//! ## Expected outcome
//!
//! `bench_100_batch_creates` should be significantly faster (often 10–50×)
//! because it does one fsync instead of 100.  The exact speedup depends on
//! storage latency (SSD vs HDD vs tmpfs).
//!
//! ## Run (compile-check only)
//!
//!   cargo bench --bench batch_benchmarks -- --test
//!
//! ## Run for real measurements
//!
//!   cargo bench --bench batch_benchmarks

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::GraphDb;

/// Number of CREATE statements to use in each benchmark iteration.
const N: usize = 100;

/// Open a fresh, temporary database.  The `TempDir` is returned so the caller
/// keeps the DB files alive for the lifetime of the benchmark.
fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open GraphDb");
    (dir, db)
}

// ─── bench 1: 100 individual execute() calls ─────────────────────────────────

fn bench_100_individual_creates(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");

    group.bench_function("100_individual_creates", |b| {
        b.iter_with_setup(fresh_db, |(_dir, db)| {
            // Each call opens a WriteTx, fsyncs WAL, commits.
            for i in 0..N {
                let q = format!("CREATE (n:BenchNode {{id: {i}}})", i = i);
                db.execute(&q).expect("individual create");
            }
        });
    });

    group.finish();
}

// ─── bench 2: 100 creates via execute_batch() ────────────────────────────────

fn bench_100_batch_creates(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");

    group.bench_function("100_batch_creates", |b| {
        b.iter_with_setup(
            || {
                let (dir, db) = fresh_db();
                // Pre-build query strings so string formatting is not measured.
                let queries: Vec<String> = (0..N)
                    .map(|i| format!("CREATE (n:BenchNode {{id: {i}}})", i = i))
                    .collect();
                (dir, db, queries)
            },
            |(_dir, db, queries)| {
                let refs: Vec<&str> = queries.iter().map(String::as_str).collect();
                // Single WriteTx + single fsync for all 100 creates.
                db.execute_batch(&refs).expect("execute_batch");
            },
        );
    });

    group.finish();
}

// ─── register all groups ─────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_100_individual_creates,
    bench_100_batch_creates,
);
criterion_main!(benches);
