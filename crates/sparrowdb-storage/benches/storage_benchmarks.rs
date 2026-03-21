use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use sparrowdb_common::TxnId;
use sparrowdb_storage::{
    crc32_of, crc32_zeroed_at,
    csr::CsrForward,
    metapage::Metapage,
    wal::{WalPayload, WalRecordKind, WalWriter},
};
use tempfile::TempDir;

// ── WAL benchmarks ─────────────────────────────────────────────────────────

fn bench_wal_append_single_commit(c: &mut Criterion) {
    c.bench_function("wal_append_single_commit", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let writer = WalWriter::open(dir.path()).unwrap();
                (dir, writer)
            },
            |(_dir, mut writer)| {
                // Single transaction: BEGIN + COMMIT (no page writes).
                writer
                    .append(WalRecordKind::Begin, TxnId(1), WalPayload::Empty)
                    .unwrap();
                writer
                    .append(WalRecordKind::Commit, TxnId(1), WalPayload::Empty)
                    .unwrap();
                writer.fsync().unwrap();
            },
        )
    });
}

fn bench_wal_append_batch_100(c: &mut Criterion) {
    c.bench_function("wal_append_batch_100", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let writer = WalWriter::open(dir.path()).unwrap();
                (dir, writer)
            },
            |(_dir, mut writer)| {
                // 100 transactions, each with one page write.
                let page = vec![0xABu8; 512];
                for i in 0u64..100 {
                    writer
                        .commit_transaction(TxnId(i), &[(i, page.clone())])
                        .unwrap();
                }
            },
        )
    });
}

// ── Metapage benchmarks ────────────────────────────────────────────────────

fn sample_metapage() -> Metapage {
    Metapage {
        txn_id: 42,
        catalog_root_page_id: 1,
        node_root_page_id: 2,
        edge_root_page_id: 3,
        wal_checkpoint_lsn: 100,
        global_node_count: 10_000,
        global_edge_count: 50_000,
        next_edge_id: 50_001,
    }
}

fn bench_metapage_encode(c: &mut Criterion) {
    let mp = sample_metapage();
    c.bench_function("metapage_encode", |b| {
        b.iter(|| {
            let encoded = black_box(&mp).encode();
            black_box(encoded)
        })
    });
}

fn bench_metapage_decode(c: &mut Criterion) {
    let encoded = sample_metapage().encode();
    c.bench_function("metapage_decode", |b| {
        b.iter(|| {
            let decoded = Metapage::decode(black_box(&encoded)).unwrap();
            black_box(decoded)
        })
    });
}

// ── CSR benchmarks ─────────────────────────────────────────────────────────

fn make_edges(n_nodes: u64, avg_degree: u64) -> Vec<(u64, u64)> {
    let mut edges = Vec::new();
    // Simple deterministic edge generation: node i connects to (i+1..i+avg_degree+1) % n_nodes
    for src in 0..n_nodes {
        for k in 1..=avg_degree {
            let dst = (src + k) % n_nodes;
            if src != dst {
                edges.push((src, dst));
            }
        }
    }
    edges
}

fn bench_csr_neighbors_small(c: &mut Criterion) {
    // 100 nodes, 10 neighbors each
    let n_nodes = 100u64;
    let edges = make_edges(n_nodes, 10);
    let csr = CsrForward::build(n_nodes, &edges);

    c.bench_function("csr_neighbors_small_1hop", |b| {
        b.iter(|| {
            // Perform 1-hop lookup for node 0
            let neighbors = csr.neighbors(black_box(0));
            black_box(neighbors.len())
        })
    });
}

fn bench_csr_neighbors_medium(c: &mut Criterion) {
    // 1000 nodes, 100 neighbors each
    let n_nodes = 1_000u64;
    let edges = make_edges(n_nodes, 100);
    let csr = CsrForward::build(n_nodes, &edges);

    c.bench_function("csr_neighbors_medium_1hop", |b| {
        b.iter(|| {
            // Perform 1-hop lookup for node 0
            let neighbors = csr.neighbors(black_box(0));
            black_box(neighbors.len())
        })
    });
}

// ── CRC32C benchmarks ──────────────────────────────────────────────────────

fn bench_crc32_4kb_page(c: &mut Criterion) {
    let page = vec![0x42u8; 4096];
    let mut group = c.benchmark_group("crc32c");
    group.throughput(Throughput::Bytes(4096));
    group.bench_function("4kb_page", |b| {
        b.iter(|| {
            let crc = crc32_of(black_box(&page));
            black_box(crc)
        })
    });
    group.finish();
}

fn bench_crc32_zeroed_at(c: &mut Criterion) {
    // Simulate metapage CRC: 512-byte buffer, zeroed at [4..8].
    let mut buf = vec![0xABu8; 512];
    buf[0..4].copy_from_slice(&0x4D455441u32.to_le_bytes()); // METAPAGE_MAGIC
    buf[8..12].copy_from_slice(&1u32.to_le_bytes()); // version

    c.bench_function("crc32_zeroed_at_metapage", |b| {
        b.iter(|| {
            let crc = crc32_zeroed_at(black_box(&buf), 4, 4).unwrap();
            black_box(crc)
        })
    });
}

// ── Register all benchmarks ────────────────────────────────────────────────

criterion_group!(
    wal_benches,
    bench_wal_append_single_commit,
    bench_wal_append_batch_100,
);

criterion_group!(
    metapage_benches,
    bench_metapage_encode,
    bench_metapage_decode,
);

criterion_group!(
    csr_benches,
    bench_csr_neighbors_small,
    bench_csr_neighbors_medium,
);

criterion_group!(crc32_benches, bench_crc32_4kb_page, bench_crc32_zeroed_at,);

criterion_main!(wal_benches, metapage_benches, csr_benches, crc32_benches);
