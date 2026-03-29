//! Benchmarks modelling the SPA-299 Q4/Q5/Q8 queries.
//!
//! Uses a synthetic graph sized to approximate the SNAP Facebook dataset
//! (4 000 nodes, 8 000+ KNOWS edges).  The graph is built once per
//! benchmark function using WriteTx, then the query is run in the hot loop.
//!
//! Benchmarks:
//!   q4_distinct_two_hop   — MATCH (a)-[:KNOWS]->()-[:KNOWS]->(c) RETURN DISTINCT c.uid
//!   q5_variable_path      — MATCH (a:User {uid:0})-[:KNOWS*1..3]->(b) RETURN b.uid
//!   q8_mutual_friends     — MATCH (a:User {uid:0})-[:KNOWS]->(m)<-[:KNOWS]-(b:User {uid:1}) RETURN m.uid
//!
//! Run compile-check only (CI-fast):
//!   cargo bench --bench snap_benchmarks -- --test
//!
//! Run for real measurements:
//!   cargo bench --bench snap_benchmarks

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::{GraphDb, Value};
use sparrowdb::NodeId;
use std::collections::HashMap;

// ── graph constants ─────────────────────────────────────────────────────────

/// Number of nodes.  Use 1 000 for measurable iterations; the perf
/// characteristics scale the same as the SNAP 4K graph.
const N_NODES: usize = 1_000;
/// Fan-out per node: each node knows the next FAN_OUT nodes (wrapping).
const FAN_OUT: usize = 20;

// ── helpers ──────────────────────────────────────────────────────────────────

fn fresh_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open");
    (dir, db)
}

/// Build a 4 000-node "User" graph with a uid property and KNOWS edges.
///
/// Edge structure: node i KNOWS nodes (i+1)..(i+FAN_OUT+1) mod N_NODES.
/// This gives every node FAN_OUT outgoing edges (88 000 total edges ≈ SNAP).
/// Nodes 0 and 1 share ~FAN_OUT common neighbors via the ring structure.
fn build_snap_graph() -> (tempfile::TempDir, GraphDb) {
    let (dir, db) = fresh_db();

    // Phase 1: create N_NODES User nodes with uid property.
    let mut node_ids: Vec<NodeId> = Vec::with_capacity(N_NODES);
    {
        let chunk = 500;
        for start in (0..N_NODES).step_by(chunk) {
            let end = (start + chunk).min(N_NODES);
            let mut tx = db.begin_write().expect("begin_write nodes");
            let label_id = tx.get_or_create_label_id("User").expect("label");
            for i in start..end {
                let nid = tx
                    .create_node_named(
                        label_id,
                        &[("uid".to_string(), Value::Int64(i as i64))],
                    )
                    .expect("create_node");
                node_ids.push(nid);
            }
            tx.commit().expect("commit nodes");
        }
    }

    // Phase 2: create FAN_OUT KNOWS edges per node in chunks.
    {
        let edge_chunk = 200; // edges per transaction
        let mut pending: Vec<(NodeId, NodeId)> = Vec::with_capacity(edge_chunk);
        let flush = |db: &GraphDb, batch: &mut Vec<(NodeId, NodeId)>| {
            if batch.is_empty() {
                return;
            }
            let mut tx = db.begin_write().expect("begin_write edges");
            for (src, dst) in batch.drain(..) {
                tx.create_edge(src, dst, "KNOWS", HashMap::new())
                    .expect("create_edge");
            }
            tx.commit().expect("commit edges");
        };
        for i in 0..N_NODES {
            for k in 1..=FAN_OUT {
                let j = (i + k) % N_NODES;
                pending.push((node_ids[i], node_ids[j]));
                if pending.len() >= edge_chunk {
                    flush(&db, &mut pending);
                }
            }
        }
        flush(&db, &mut pending);
    }

    (dir, db)
}

// ── Q8: mutual friends ────────────────────────────────────────────────────────

/// Q8: MATCH (a:User {uid:0})-[:KNOWS]->(m)<-[:KNOWS]-(b:User {uid:1}) RETURN m.uid
///
/// Measures the full cost of binding two anchor nodes by inline prop lookup
/// plus the SlotIntersect intersection step.  Before the fix, anchor lookup
/// did O(N × file_reads); after, it does 2 bulk column reads + O(N) scan,
/// or O(1) if the uid column is indexed.
fn bench_q8_mutual_friends(c: &mut Criterion) {
    let (_dir, db) = build_snap_graph();

    let mut group = c.benchmark_group("snap");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(15));
    group.bench_function("q8_mutual_friends", |b| {
        b.iter(|| {
            let r = db
                .execute(
                    "MATCH (a:User {uid:0})-[:KNOWS]->(m:User)<-[:KNOWS]-(b:User {uid:1}) \
                     RETURN m.uid",
                )
                .expect("Q8");
            criterion::black_box(r);
        });
    });
    group.finish();
}

// ── Q4: DISTINCT 2-hop ────────────────────────────────────────────────────────

/// Q4: MATCH (a)-[:KNOWS]->()-[:KNOWS]->(c) RETURN DISTINCT c.uid
///
/// Measures the dedup cost on large result sets.  With FAN_OUT=22 and 4 000
/// nodes each node has 22 2-hop neighbors → up to ~88 000 rows before DISTINCT.
/// Before the fix, deduplicate_rows was O(N²) on those rows.
fn bench_q4_distinct_two_hop(c: &mut Criterion) {
    let (_dir, db) = build_snap_graph();

    let mut group = c.benchmark_group("snap");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.bench_function("q4_distinct_two_hop", |b| {
        b.iter(|| {
            let r = db
                .execute(
                    "MATCH (a:User)-[:KNOWS]->()-[:KNOWS]->(c:User) \
                     RETURN DISTINCT c.uid",
                )
                .expect("Q4");
            criterion::black_box(r);
        });
    });
    group.finish();
}

// ── Q5: variable-length path from single source ───────────────────────────────

/// Q5: MATCH (a:User {uid:0})-[:KNOWS*1..3]->(b) RETURN b.uid
///
/// Measures source resolution cost.  Before the fix, execute_variable_length
/// scanned all 4 000 slots before finding uid=0.  After, try_index_lookup
/// resolves uid=0 in O(1) when the column is indexed.
fn bench_q5_variable_path(c: &mut Criterion) {
    let (_dir, db) = build_snap_graph();

    let mut group = c.benchmark_group("snap");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(15));
    group.bench_function("q5_variable_path_single_source", |b| {
        b.iter(|| {
            let r = db
                .execute(
                    "MATCH (a:User {uid:0})-[:KNOWS*1..3]->(b:User) \
                     RETURN b.uid",
                )
                .expect("Q5");
            criterion::black_box(r);
        });
    });
    group.finish();
}

// ── register ──────────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_q8_mutual_friends,
    bench_q4_distinct_two_hop,
    bench_q5_variable_path,
);
criterion_main!(benches);
