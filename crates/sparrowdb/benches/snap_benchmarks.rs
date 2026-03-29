//! Benchmarks modelling the SPA-299 Q3/Q4/Q5/Q8 queries.
//!
//! Two graph backends are supported:
//!
//! 1. **Synthetic** (default, CI-safe): a 1 000-node ring with FAN_OUT=20
//!    KNOWS edges per node — same topology as the SNAP Facebook graph.
//!
//! 2. **Real SNAP Facebook dataset** (optional): set the env var
//!    `SNAP_DATASET` to the path of `facebook_combined.txt` (edge list,
//!    space-separated, 4 039 nodes / 88 234 edges) to run Q3/Q4/Q5/Q8
//!    against the actual dataset.
//!
//! Benchmarks:
//!   q3_single_hop         — MATCH (a:User {uid:0})-[:KNOWS]->(b) RETURN b.uid
//!   q4_distinct_two_hop   — MATCH (a)-[:KNOWS]->()-[:KNOWS]->(c) RETURN DISTINCT c.uid
//!   q5_variable_path      — MATCH (a:User {uid:0})-[:KNOWS*1..3]->(b) RETURN b.uid
//!   q8_mutual_friends     — MATCH (a:User {uid:0})-[:KNOWS]->(m)<-[:KNOWS]-(b:User {uid:1}) RETURN m.uid
//!
//! Run compile-check only (CI-fast):
//!   cargo bench --bench snap_benchmarks -- --test
//!
//! Run for real measurements (synthetic):
//!   cargo bench --bench snap_benchmarks
//!
//! Run against real SNAP Facebook dataset:
//!   SNAP_DATASET=/path/to/facebook_combined.txt cargo bench --bench snap_benchmarks

use criterion::{criterion_group, criterion_main, Criterion};
use sparrowdb::NodeId;
use sparrowdb::{GraphDb, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

// ── graph constants ─────────────────────────────────────────────────────────

/// Number of nodes.  Use 1 000 for measurable iterations; the perf
/// characteristics scale the same as the SNAP 4K graph.
const N_NODES: usize = 1_000;
/// Fan-out per node: each node knows the next FAN_OUT nodes (wrapping).
const FAN_OUT: usize = 20;

// ── SNAP file loader ─────────────────────────────────────────────────────────

/// Load the real SNAP Facebook edge-list into a fresh SparrowDB instance.
///
/// File format: one edge per line, `src dst` (space-separated, 0-based node
/// IDs, no header).  Nodes are created lazily on first reference.
///
/// Returns `(TempDir, GraphDb)` and the node ID for SNAP uid 0 (used as the
/// anchor for Q3/Q5/Q8).
fn build_snap_graph_from_file(path: &str) -> (tempfile::TempDir, GraphDb, NodeId) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let file = File::open(path).unwrap_or_else(|e| panic!("Cannot open SNAP file {path}: {e}"));
    let reader = BufReader::new(file);

    // Collect all edges first so we can batch-insert nodes then edges.
    let mut edges: Vec<(u64, u64)> = Vec::with_capacity(88_234);
    let mut max_uid: u64 = 0;

    for line in reader.lines() {
        let line = line.expect("read line");
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let src: u64 = parts.next().expect("src").parse().expect("parse src");
        let dst: u64 = parts.next().expect("dst").parse().expect("parse dst");
        if src > max_uid {
            max_uid = src;
        }
        if dst > max_uid {
            max_uid = dst;
        }
        edges.push((src, dst));
    }

    let n_nodes = (max_uid + 1) as usize;
    let mut node_ids: Vec<NodeId> = Vec::with_capacity(n_nodes);

    // Create all nodes in batches of 500.
    let chunk = 500;
    for start in (0..n_nodes).step_by(chunk) {
        let end = (start + chunk).min(n_nodes);
        let mut tx = db.begin_write().expect("begin_write nodes");
        let label_id = tx.get_or_create_label_id("User").expect("label");
        for uid in start..end {
            let nid = tx
                .create_node_named(label_id, &[("uid".to_string(), Value::Int64(uid as i64))])
                .expect("create node");
            node_ids.push(nid);
        }
        tx.commit().expect("commit nodes");
    }

    // Insert edges in batches of 500.
    let edge_chunk = 500;
    let flush = |db: &GraphDb, batch: &mut Vec<(NodeId, NodeId)>| {
        if batch.is_empty() {
            return;
        }
        let mut tx = db.begin_write().expect("begin_write edges");
        for (src, dst) in batch.drain(..) {
            tx.create_edge(src, dst, "KNOWS", HashMap::new())
                .expect("create edge");
        }
        tx.commit().expect("commit edges");
    };

    let mut pending: Vec<(NodeId, NodeId)> = Vec::with_capacity(edge_chunk);
    for (src_uid, dst_uid) in &edges {
        pending.push((node_ids[*src_uid as usize], node_ids[*dst_uid as usize]));
        if pending.len() >= edge_chunk {
            flush(&db, &mut pending);
        }
    }
    flush(&db, &mut pending);

    db.checkpoint().expect("checkpoint after SNAP load");

    let anchor = node_ids[0];
    (dir, db, anchor)
}

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
                    .create_node_named(label_id, &[("uid".to_string(), Value::Int64(i as i64))])
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

// ── Q3: single-hop from source (synthetic) ───────────────────────────────────

/// Q3: MATCH (a:User {uid:0})-[:KNOWS]->(b:User) RETURN b.uid
///
/// Measures 1-hop expansion cost from a single anchor node.
fn bench_q3_single_hop(c: &mut Criterion) {
    let (_dir, db) = build_snap_graph();

    let mut group = c.benchmark_group("snap");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(15));
    group.bench_function("q3_single_hop", |b| {
        b.iter(|| {
            let r = db
                .execute("MATCH (a:User {uid:0})-[:KNOWS]->(b:User) RETURN b.uid")
                .expect("Q3");
            criterion::black_box(r);
        });
    });
    group.finish();
}

// ── Real SNAP dataset benches (gated on SNAP_DATASET env var) ─────────────────

/// Q3 on real SNAP Facebook dataset.
fn bench_snap_real_q3(c: &mut Criterion) {
    let path = match std::env::var("SNAP_DATASET") {
        Ok(p) => p,
        Err(_) => return, // skip if env var not set
    };
    let (_dir, db, _anchor) = build_snap_graph_from_file(&path);

    let mut group = c.benchmark_group("snap_real");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.bench_function("q3_single_hop_uid0", |b| {
        b.iter(|| {
            let r = db
                .execute("MATCH (a:User {uid:0})-[:KNOWS]->(b:User) RETURN b.uid")
                .expect("Q3 real");
            criterion::black_box(r);
        });
    });
    group.finish();
}

/// Q4 on real SNAP Facebook dataset.
fn bench_snap_real_q4(c: &mut Criterion) {
    let path = match std::env::var("SNAP_DATASET") {
        Ok(p) => p,
        Err(_) => return,
    };
    let (_dir, db, _anchor) = build_snap_graph_from_file(&path);

    let mut group = c.benchmark_group("snap_real");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.bench_function("q4_distinct_two_hop", |b| {
        b.iter(|| {
            let r = db
                .execute(
                    "MATCH (a:User)-[:KNOWS]->()-[:KNOWS]->(c:User) \
                     RETURN DISTINCT c.uid",
                )
                .expect("Q4 real");
            criterion::black_box(r);
        });
    });
    group.finish();
}

/// Q5 on real SNAP Facebook dataset.
fn bench_snap_real_q5(c: &mut Criterion) {
    let path = match std::env::var("SNAP_DATASET") {
        Ok(p) => p,
        Err(_) => return,
    };
    let (_dir, db, _anchor) = build_snap_graph_from_file(&path);

    let mut group = c.benchmark_group("snap_real");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.bench_function("q5_variable_path_1_3_uid0", |b| {
        b.iter(|| {
            let r = db
                .execute(
                    "MATCH (a:User {uid:0})-[:KNOWS*1..3]->(b:User) \
                     RETURN b.uid",
                )
                .expect("Q5 real");
            criterion::black_box(r);
        });
    });
    group.finish();
}

/// Q8 on real SNAP Facebook dataset.
fn bench_snap_real_q8(c: &mut Criterion) {
    let path = match std::env::var("SNAP_DATASET") {
        Ok(p) => p,
        Err(_) => return,
    };
    let (_dir, db, _anchor) = build_snap_graph_from_file(&path);

    let mut group = c.benchmark_group("snap_real");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.bench_function("q8_mutual_friends_uid0_uid1", |b| {
        b.iter(|| {
            let r = db
                .execute(
                    "MATCH (a:User {uid:0})-[:KNOWS]->(m:User)<-[:KNOWS]-(b:User {uid:1}) \
                     RETURN m.uid",
                )
                .expect("Q8 real");
            criterion::black_box(r);
        });
    });
    group.finish();
}

// ── register ──────────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_q3_single_hop,
    bench_q8_mutual_friends,
    bench_q4_distinct_two_hop,
    bench_q5_variable_path,
    bench_snap_real_q3,
    bench_snap_real_q4,
    bench_snap_real_q5,
    bench_snap_real_q8,
);
criterion_main!(benches);
