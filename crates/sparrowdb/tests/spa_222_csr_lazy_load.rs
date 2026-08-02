//! SPA-222: CSR lazy-load via memory-map.
//!
//! Verifies that opening a database whose edges have been folded into CSR base
//! files goes through the mmap-backed path, and that traversal through that
//! mapping still returns correct results.
//!
//! ## Fixture sizing (issue #434)
//!
//! `CsrForward::open` / `CsrBackward::open` in `sparrowdb-storage::csr` memory-map
//! **unconditionally** — there is no size threshold anywhere that chooses between
//! a heap read and an mmap, so a smaller fixture cannot silently fall off the
//! lazy-load path.  The only size that matters is the *page* granularity of the
//! mapping: the fixture must produce a CSR file spanning many OS pages, so that
//! reading a neighbour list genuinely faults a page in rather than being served
//! by the single page that already holds the header.
//!
//! With `N` nodes and `E` edges the on-disk CSR is exactly
//!
//! ```text
//! 8 + (N + 1) * 8 + E * 8   bytes
//! [n_nodes][offsets x (N+1)][neighbours x E]
//! ```
//!
//! For `N = 10_000`, `E = 20_000` that is 240_016 bytes — 15 pages at the 16 KiB
//! page size used on Apple Silicon, 59 pages at the 4 KiB size used by the Linux
//! CI runners.  The neighbours array alone starts at byte 80_016, i.e. page 4
//! (16 KiB) or page 19 (4 KiB), so **no** neighbour lookup can be satisfied by
//! the header page.  The test traverses both the first and the last node, and
//! the last node's neighbour entries are the final two `u64`s in the file — the
//! last page of the mapping.
//!
//! The previous 50K/100K fixture crossed exactly the same boundaries; it was 5x
//! larger for no additional coverage, and seeding it one Cypher statement at a
//! time took ~80 minutes.  Seeding now goes through `WriteTx` in chunked
//! transactions: the traversal is the subject of this test, the ingest path is
//! not.

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::types::Value;
use sparrowdb_storage::node_store::Value as StoreValue;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Number of `Person` nodes in the fixture.
const N: u64 = 10_000;

/// Each node gets two out-edges: `next` (`i+1`) and `skip-7` (`i+7`).
///
/// The two coincide only if `N` divides 6, which 10_000 does not — so the edge
/// set contains no duplicates and `E` below is exact after CSR de-duplication.
const EDGES_PER_NODE: u64 = 2;

/// Total edges in the fixture.
const E: u64 = N * EDGES_PER_NODE;

/// Nodes (or edges) staged per write transaction.
///
/// `WriteTx::create_edge` scans the transaction's own pending-op buffer to
/// derive the next edge id, so staging every edge in one transaction would be
/// quadratic.  Chunking bounds that scan while keeping the number of WAL fsyncs
/// small.
const CHUNK: usize = 2_000;

/// Locate the single rel table's forward CSR base file under `<db_root>/edges/`.
fn csr_forward_path(db_root: &Path) -> PathBuf {
    let edges_dir = db_root.join("edges");
    let mut found: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(&edges_dir).expect("edges/ must exist after checkpoint") {
        let candidate = entry.expect("read_dir entry").path().join("base.fwd.csr");
        if candidate.is_file() {
            found.push(candidate);
        }
    }
    assert_eq!(
        found.len(),
        1,
        "fixture defines exactly one rel table (KNOWS), found {found:?}"
    );
    found.pop().unwrap()
}

/// Decode `(n_nodes, n_edges)` from a CSR file using the documented layout:
/// `[n_nodes: u64][offsets: u64 x (n_nodes + 1)][neighbours: u64 x n_edges]`,
/// where `n_edges` is the sentinel entry `offsets[n_nodes]`.
fn read_csr_header(path: &Path) -> (u64, u64) {
    let bytes = std::fs::read(path).expect("read CSR base file");
    assert!(bytes.len() >= 8, "CSR file too short for n_nodes");
    let n_nodes = u64::from_le_bytes(bytes[0..8].try_into().unwrap());

    let sentinel = 8 + n_nodes as usize * 8;
    assert!(
        bytes.len() >= sentinel + 8,
        "CSR file too short for the offsets array"
    );
    let n_edges = u64::from_le_bytes(bytes[sentinel..sentinel + 8].try_into().unwrap());
    (n_nodes, n_edges)
}

/// Seed `N` `Person` nodes and `E` `KNOWS` edges through `WriteTx`.
///
/// Returns the packed node ids in `id` order, so `node_ids[i]` is the node whose
/// `id` property is `i`.
fn seed_graph(db: &GraphDb) -> Vec<sparrowdb_common::NodeId> {
    let mut node_ids = Vec::with_capacity(N as usize);

    for chunk_start in (0..N).step_by(CHUNK) {
        let chunk_end = (chunk_start + CHUNK as u64).min(N);
        let mut tx = db.begin_write().expect("begin_write (nodes)");
        let label_id = tx
            .get_or_create_label_id("Person")
            .expect("get_or_create_label_id");
        for i in chunk_start..chunk_end {
            let node_id = tx
                .create_node_named(label_id, &[("id".to_string(), StoreValue::Int64(i as i64))])
                .expect("create_node_named");
            node_ids.push(node_id);
        }
        tx.commit().expect("commit (nodes)");
    }
    assert_eq!(node_ids.len(), N as usize, "seeded node count");

    // Two edges per node, so halve the chunk to keep transactions the same size.
    let edge_chunk = CHUNK / 2;
    for chunk_start in (0..N).step_by(edge_chunk) {
        let chunk_end = (chunk_start + edge_chunk as u64).min(N);
        let mut tx = db.begin_write().expect("begin_write (edges)");
        for i in chunk_start..chunk_end {
            let next = (i + 1) % N;
            let skip = (i + 7) % N;
            tx.create_edge(
                node_ids[i as usize],
                node_ids[next as usize],
                "KNOWS",
                HashMap::new(),
            )
            .expect("create_edge (next)");
            tx.create_edge(
                node_ids[i as usize],
                node_ids[skip as usize],
                "KNOWS",
                HashMap::new(),
            )
            .expect("create_edge (skip-7)");
        }
        tx.commit().expect("commit (edges)");
    }

    node_ids
}

/// Run a one-hop traversal from the `Person` whose `id` property is `id`, and
/// return the neighbours' `id` values in ascending order.
fn neighbour_ids(db: &GraphDb, id: u64) -> Vec<i64> {
    let cypher = format!("MATCH (a:Person {{id: {id}}})-[:KNOWS]->(b) RETURN b.id ORDER BY b.id");
    db.execute(&cypher)
        .expect("traversal query")
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect()
}

/// Build the graph, checkpoint it into CSR base files, reopen the database, and
/// verify that traversal through the mmap-backed CSR returns correct results.
#[test]
fn spa222_mmap_open_and_traverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();

    seed_graph(&db);

    // Fold the delta log into the CSR base files on real disk.
    db.checkpoint().unwrap();
    drop(db);

    // ── The on-disk CSR must be a real, multi-page mapping target ────────────
    let csr_path = csr_forward_path(dir.path());

    // The checkpoint truncates the delta log, so every edge the traversal below
    // returns has to come out of the CSR base file.  Without this the test could
    // pass while reading edges from the delta log and never touch the mapping.
    let delta_path = csr_path.with_file_name("delta.log");
    let delta_len = std::fs::metadata(&delta_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        delta_len, 0,
        "delta log must be empty after CHECKPOINT so traversal reads the CSR"
    );

    let file_len = std::fs::metadata(&csr_path)
        .expect("stat CSR base file")
        .len() as usize;
    let (n_nodes, n_edges) = read_csr_header(&csr_path);

    assert_eq!(n_nodes, N, "CSR should span every seeded Person slot");
    assert_eq!(
        n_edges, E,
        "each of the {N} nodes contributes exactly {EDGES_PER_NODE} distinct edges"
    );

    // Layout is fully determined by the fixture: 8 + (N+1)*8 + E*8 = 240_016.
    let expected_len = 8 + (N as usize + 1) * 8 + E as usize * 8;
    assert_eq!(
        file_len, expected_len,
        "CSR file must match the documented [n_nodes][offsets][neighbours] layout"
    );

    // The largest page size in play is 16 KiB (Apple Silicon); Linux CI uses
    // 4 KiB.  Requiring >10 pages at the *larger* size proves the mapping is
    // comfortably multi-page on every platform we build on, so neighbour reads
    // cannot be served by the page holding the header.
    const LARGEST_PAGE: usize = 16 * 1024;
    assert!(
        file_len > 10 * LARGEST_PAGE,
        "CSR file is {file_len} bytes — too small to exercise lazy paging \
         (need > {} bytes, i.e. >10 pages of {LARGEST_PAGE})",
        10 * LARGEST_PAGE
    );

    // The neighbours array starts after the header and the offsets array, so no
    // neighbour lookup can land on the header page.
    let neighbours_start = 8 + (n_nodes as usize + 1) * 8;
    assert!(
        neighbours_start > LARGEST_PAGE,
        "neighbours array starts at byte {neighbours_start}, inside the header page"
    );

    // ── Reopen: this goes through CsrForward::open, i.e. mmap ────────────────
    let start = Instant::now();
    let db2 = open(dir.path()).unwrap();
    let open_ms = start.elapsed().as_millis();
    eprintln!("SPA-222: reopen with {N} nodes + {E} edges took {open_ms}ms");

    // ── Traversal through the mapping, at both ends of the neighbours array ──

    // Node 0 is the first CSR slot: its neighbours are (0+1)=1 and (0+7)=7.
    assert_eq!(
        neighbour_ids(&db2, 0),
        vec![1i64, 7],
        "node 0 must reach exactly its next and skip-7 neighbours"
    );

    // Node N-1 is the last CSR slot, so its two neighbour entries are the final
    // two u64s in the file — reading them faults in the last page of the
    // mapping.  Its neighbours wrap: (9999+1) % 10000 = 0 and (9999+7) % 10000 = 6.
    assert_eq!(
        neighbour_ids(&db2, N - 1),
        vec![0i64, 6],
        "last node must reach exactly its wrapped next and skip-7 neighbours"
    );
}
