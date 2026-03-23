//! SPA-147 Criterion benchmark suite for SparrowDB.
//!
//! Covers the six key workloads:
//!   1. bench_create_node        — CREATE 1000 nodes, measure throughput
//!   2. bench_match_scan         — MATCH (n:Person) over a 1000-node graph
//!   3. bench_one_hop            — MATCH (a)-[:KNOWS]->(b) over 1000 KNOWS edges
//!   4. bench_two_hop            — MATCH (a)-[:KNOWS]->()-[:KNOWS]->(c) (2-hop)
//!   5. bench_checkpoint         — CHECKPOINT after 1000 writes
//!   6. bench_aggregation        — COUNT(*), AVG, MAX over 1000 nodes
//!
//! Run (dry-run, compile check only):
//!   cargo bench --bench graph_benchmarks -- --test
//!
//! Run for real measurements:
//!   cargo bench --bench graph_benchmarks
//!
//! HTML reports land in target/criterion/.

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::{GraphDb, Value};
use std::collections::HashMap;

/// Convert a string to the storage `Value` representation (Bytes).
#[inline]
fn str_val(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

/// Convert an i64 to the storage `Value` representation.
#[inline]
fn int_val(n: i64) -> Value {
    Value::Int64(n)
}

// ─── constants ────────────────────────────────────────────────────────────────

/// Number of nodes / edges used in all "graph already built" benchmarks.
const N: usize = 1_000;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Open a fresh, temporary database.  The `TempDir` is returned so the caller
/// can keep it alive for the lifetime of the benchmark.
fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open GraphDb");
    (dir, db)
}

/// Build a graph with N Person nodes via the WriteTx API.
/// Properties: name (String), age (Int64).
/// Returns the tempdir (to keep DB files alive) and the opened database.
fn build_person_graph() -> (tempfile::TempDir, GraphDb) {
    let (dir, db) = fresh_db();

    let mut tx = db.begin_write().expect("begin_write");
    let label_id = tx.get_or_create_label_id("Person").expect("label");

    for i in 0..N {
        let age = 20 + (i % 60) as i64;
        tx.create_node_named(
            label_id,
            &[
                ("name".to_string(), str_val(&format!("Person{i}"))),
                ("age".to_string(), int_val(age)),
            ],
        )
        .expect("create_node_named");
    }
    tx.commit().expect("commit");

    (dir, db)
}

/// Build a graph with N Person nodes and N directed KNOWS edges forming a ring
/// (node i → node (i+1) % N).  Uses the WriteTx API for both nodes and edges.
fn build_knows_graph() -> (tempfile::TempDir, GraphDb) {
    let (dir, db) = fresh_db();

    // ── Phase 1: create nodes ───────────────────────────────────────────────
    let mut node_ids = Vec::with_capacity(N);
    let mut tx = db.begin_write().expect("begin_write nodes");
    let label_id = tx.get_or_create_label_id("Person").expect("label");

    for i in 0..N {
        let age = 20 + (i % 60) as i64;
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

    // ── Phase 2: create ring edges in batches to avoid one huge transaction ──
    // Create edges in chunks of 100 to keep each transaction small.
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

// ─── bench_create_node ────────────────────────────────────────────────────────

/// Measure the cost of creating a single Person node in an otherwise-empty DB.
/// Criterion will call this in a loop and report ns/iter.
fn bench_create_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("create");
    group.bench_function("create_node", |b| {
        // Fresh DB per iteration so storage growth doesn't skew results.
        b.iter_with_setup(fresh_db, |(_dir, db)| {
            db.execute("CREATE (n:Person {name: 'Bench', age: 30})")
                .expect("CREATE");
        });
    });
    group.finish();
}

// ─── bench_match_scan ─────────────────────────────────────────────────────────

/// Measure a full label scan: MATCH (n:Person) RETURN n.name over 1000 nodes.
fn bench_match_scan(c: &mut Criterion) {
    // Build the graph once outside the measured loop.
    let (_dir, db) = build_person_graph();

    let mut group = c.benchmark_group("read");
    group.bench_function("match_scan_1k", |b| {
        b.iter(|| {
            let result = db
                .execute("MATCH (n:Person) RETURN n.name")
                .expect("MATCH scan");
            criterion::black_box(result);
        });
    });
    group.finish();
}

// ─── bench_one_hop ────────────────────────────────────────────────────────────

/// Measure a 1-hop traversal: MATCH (a)-[:KNOWS]->(b) RETURN a.pid, b.pid
/// over a ring graph with N KNOWS edges.
fn bench_one_hop(c: &mut Criterion) {
    let (_dir, db) = build_knows_graph();

    let mut group = c.benchmark_group("traversal");
    group.bench_function("one_hop_1k_edges", |b| {
        b.iter(|| {
            let result = db
                .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.pid, b.pid")
                .expect("1-hop");
            criterion::black_box(result);
        });
    });
    group.finish();
}

// ─── bench_two_hop ────────────────────────────────────────────────────────────

/// Measure a 2-hop traversal: MATCH (a)-[:KNOWS]->()-[:KNOWS]->(c)
/// over the same ring graph.  With a ring of N nodes every node has exactly
/// one 2-hop neighbour, so this returns N rows.
fn bench_two_hop(c: &mut Criterion) {
    let (_dir, db) = build_knows_graph();

    let mut group = c.benchmark_group("traversal");
    group.bench_function("two_hop_1k_edges", |b| {
        b.iter(|| {
            let result = db
                .execute(
                    "MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(c:Person) \
                     RETURN a.pid, c.pid",
                )
                .expect("2-hop");
            criterion::black_box(result);
        });
    });
    group.finish();
}

// ─── bench_checkpoint ─────────────────────────────────────────────────────────

/// Measure CHECKPOINT latency after 1000 CREATE operations.
/// Each Criterion iteration builds a fresh 1000-node graph so the WAL size
/// is constant across samples.
fn bench_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("maintenance");
    group.bench_function("checkpoint_after_1k_writes", |b| {
        b.iter_with_setup(
            || {
                // Build the 1000-node graph; do NOT checkpoint yet.
                build_person_graph()
            },
            |(_dir, db)| {
                db.execute("CHECKPOINT").expect("CHECKPOINT");
            },
        );
    });
    group.finish();
}

// ─── bench_aggregation ────────────────────────────────────────────────────────

/// Measure aggregation queries over 1000 Person nodes.
/// Three sub-benchmarks: COUNT(*), AVG(age), MAX(age).
fn bench_aggregation(c: &mut Criterion) {
    let (_dir, db) = build_person_graph();

    let mut group = c.benchmark_group("aggregation");

    group.bench_function("count_star_1k", |b| {
        b.iter(|| {
            let r = db
                .execute("MATCH (n:Person) RETURN COUNT(*)")
                .expect("COUNT(*)");
            criterion::black_box(r);
        });
    });

    group.bench_function("avg_age_1k", |b| {
        b.iter(|| {
            let r = db
                .execute("MATCH (n:Person) RETURN AVG(n.age)")
                .expect("AVG");
            criterion::black_box(r);
        });
    });

    group.bench_function("max_age_1k", |b| {
        b.iter(|| {
            let r = db
                .execute("MATCH (n:Person) RETURN MAX(n.age)")
                .expect("MAX");
            criterion::black_box(r);
        });
    });

    group.finish();
}

// ─── register all groups ──────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_create_node,
    bench_match_scan,
    bench_one_hop,
    bench_two_hop,
    bench_checkpoint,
    bench_aggregation,
);
criterion_main!(benches);
