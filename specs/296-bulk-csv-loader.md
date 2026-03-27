# Spec: Bulk CSV/Edge-List Loader

**Issue:** #296
**Status:** Draft
**Date:** 2026-03-27

## Problem

SparrowDB's only ingestion path is individual Cypher CREATE statements, which tops out at ~76-333 edges/sec. Importing real-world datasets is impractical:

| Dataset | Edges | Estimated Time |
|---------|-------|----------------|
| Facebook | 88K | ~5 min |
| Pokec | 30.6M | **~5 days** |
| LiveJournal | 69M | **~9 days** |
| LDBC SF1 | 17M | **~3 days** |

Neo4j's `neo4j-admin import` does Pokec in **30 seconds** (1M+ edges/sec). Target: 100K-1M+ edges/sec.

## Current Import Path

The CLI has a Neo4j APOC CSV importer (`sparrowdb import --neo4j-csv`):

```
crates/sparrowdb-cli/src/main.rs (lines 200-405)
```

**Why it's slow:**
1. Each node/edge committed in a **separate transaction** (individual fsync).
2. Properties parsed through the `WriteTx` API one-at-a-time.
3. No batching of column writes or CSR construction.
4. Edge delta log appended per-edge, then checkpointed separately.

## Design: Direct Storage-Layer Bulk Loader

Bypass `WriteTx` and Cypher entirely. Write directly to the storage layer files in a single pass.

### API

**Rust:**
```rust
use sparrowdb::bulk::{BulkLoader, BulkOptions, Format};

let options = BulkOptions {
    format: Format::Csv { delimiter: ',', has_header: true },
    batch_size: 100_000,
    skip_wal: true,          // No WAL for bulk load (db must be offline)
    parallel_sort: true,     // Use Rayon for edge sorting
};

let loader = BulkLoader::new("/path/to/db", options)?;
loader.load_nodes("nodes.csv")?;
loader.load_edges("edges.csv")?;
loader.finalize()?;           // Build CSR, write checkpoint, fsync
```

**CLI:**
```bash
# Generic CSV
sparrowdb bulk-import --nodes nodes.csv --edges edges.csv --db /path/to/db

# SNAP edge list (tab-separated src\tdst, no header)
sparrowdb bulk-import --edges soc-pokec.txt --format snap --db /path/to/db

# LDBC pipe-delimited
sparrowdb bulk-import --nodes person.csv --edges knows.csv --format ldbc --db /path/to/db
```

**Node.js binding:**
```javascript
db.bulkImport({ nodes: "nodes.csv", edges: "edges.csv" });
```

### Input Formats

**Generic CSV (default):**
```
_id,_label,name,age
1,Person,Alice,30
2,Person,Bob,25
3,City,Seattle,
```

Nodes require `_id` (external ID) and `_label` columns. All other columns become properties.

**Edge CSV:**
```
_start,_end,_type,weight
1,2,KNOWS,0.9
1,3,LIVES_IN,
```

Edges require `_start`, `_end` (referencing node `_id` values), and `_type` columns.

**SNAP edge list:**
```
# Comment lines start with #
0	1
0	2
1	3
```

Tab-separated `src_id\tdst_id`. No header. Nodes auto-created with a default label (configurable). No properties.

**LDBC pipe-delimited:**
```
id|firstName|lastName|creationDate
933|Mahinda|Perera|2010-02-14
```

Pipe `|` delimiter with header row. LDBC-specific column mappings.

### Algorithm

**Phase 1: Node ingestion**

```
1. Open CSV reader (streaming, not fully in memory)
2. For each row:
   a. Resolve label -> label_id (create in catalog if new)
   b. Allocate slot = next HWM for that label
   c. Compute NodeId = (label_id << 32) | slot
   d. Map external _id -> NodeId in hash map
   e. Buffer property values per (label_id, col_id)
3. For each (label_id, col_id) buffer:
   a. Write entire column file in one sequential write
   b. Write null bitmap
4. Update HWM files
5. Flush catalog
```

**Key optimization:** Collect all property values for a column before writing. This turns N random writes into 1 sequential write per column file.

**Phase 2: Edge ingestion**

```
1. Open edge CSV reader (streaming)
2. For each row:
   a. Resolve _start and _end via id_map -> (src_NodeId, dst_NodeId)
   b. Resolve _type -> rel_table_id (create in catalog if new)
   c. Append (src_slot, dst_slot) to per-rel-type edge buffer
3. For each rel_table:
   a. Sort edges by (src_slot, dst_slot) — parallel sort via Rayon
   b. Deduplicate
   c. Build CsrForward directly from sorted edges
   d. Build CsrBackward (sort by dst, build offsets)
   e. Write base.fwd.csr and base.bwd.csr atomically
   f. Write empty delta.log (no delta needed — CSR is complete)
4. Handle edge properties if present (write to edge_props.bin)
```

**Key optimization:** Skip the delta log entirely. Build the sorted CSR in memory from the complete edge set and write it once. This avoids the append-per-edge + checkpoint overhead.

**Phase 3: Finalize**

```
1. Fsync all written files
2. Write catalog.tlv with all new labels and rel types
3. Write WAL checkpoint marker (so recovery knows the bulk load is committed)
4. Fsync WAL
```

### Memory Management

For large datasets that exceed available RAM:

**Node properties:** Stream rows and write columns incrementally. Keep only the current batch in memory. Each batch writes its portion of the column file at the correct offset.

**Edge sorting:** If edge count exceeds a threshold (e.g., 50M), use external sort:
1. Read edges into chunks of `batch_size`.
2. Sort each chunk in memory.
3. Write sorted chunks to temp files.
4. Merge-sort temp files into final CSR.

**Id map:** The `HashMap<String, NodeId>` must fit in memory. For 100M nodes with 8-byte average ID strings, this is ~2.4 GB. Acceptable for datasets up to ~500M nodes on a 16 GB machine.

### WAL Handling

**Option A (recommended): Skip WAL for offline bulk load.**
The database must be closed (no concurrent readers/writers). The bulk loader writes directly to storage files. On completion, it writes a single WAL checkpoint record as a commit marker.

**Option B: WAL-backed bulk load.**
Emit batched WAL records (e.g., 10K nodes per record). Slower but allows online loading with crash recovery. Deferred to Phase 2.

### Error Handling

- **Missing `_id` or `_label` column:** Fatal error before any writes.
- **Duplicate `_id` values:** Last-write-wins (within same label) or error (configurable).
- **Missing edge endpoints:** Skip edge with warning, or error (configurable).
- **Malformed CSV row:** Skip with warning, or error (configurable via `--strict`).
- **Crash during bulk load:** Database is in inconsistent state. Recovery: delete `nodes/` and `edges/` directories and re-run. WAL checkpoint marker is only written on success.

### Progress Reporting

```
Loading nodes...  [████████████░░░░] 75%  750,000 / 1,000,000  (125K nodes/sec)
Loading edges...  [██████░░░░░░░░░░] 37%  11.3M / 30.6M  (890K edges/sec)
Building CSR...   [████████████████] 100% Forward + Backward
Finalizing...     Done. 1,000,000 nodes, 30,600,000 edges in 42.3s
```

### Performance Targets

| Operation | Target | Bottleneck |
|-----------|--------|------------|
| Node ingestion | 500K+ nodes/sec | Sequential column writes + CSV parsing |
| Edge ingestion | 1M+ edges/sec | In-memory sort + sequential CSR write |
| CSR build | O(E log E) | Parallel sort (Rayon) |
| Total (Pokec 30.6M) | < 60 seconds | Edge sort dominates |

## Phasing

| Phase | Scope | Size |
|---|---|---|
| **1** | Core `BulkLoader` struct, generic CSV format, offline node + edge loading, direct CSR build | L |
| **2** | SNAP and LDBC format support, edge properties, external sort for very large datasets | M |
| **3** | CLI `bulk-import` command, progress reporting, Node.js/Python bindings | M |
| **4** | Online bulk load (WAL-backed), concurrent reader support during import | L |

**Overall: L (Large)**

## Risks & Open Questions

| # | Item | Decision |
|---|---|---|
| 1 | Offline-only requirement for Phase 1 | Accept. Document that db must be closed. Online loading in Phase 4. |
| 2 | Memory for id_map on very large datasets | Accept up to ~500M nodes. Add disk-backed id map if needed. |
| 3 | External sort complexity | Defer to Phase 2. Phase 1 assumes edges fit in memory. |
| 4 | Multi-label nodes during bulk load | Defer until #289 is implemented. Phase 1 supports single-label only. |
| 5 | Reuse existing `csv` crate (in workspace deps) vs hand-rolled parser | Use `csv` crate for robustness. Hand-rolled parser in current CLI has edge cases. |
| 6 | Edge property storage format | Reuse existing `edge_props.bin` append format from `edge_store.rs`. |
| 7 | Should `BulkLoader` be in `sparrowdb` or a new crate? | Put in `sparrowdb` crate (`src/bulk.rs`) — it needs direct access to `NodeStore`, `EdgeStore`, `Catalog`. |
| 8 | Interaction with encryption | If db uses encryption, bulk loader must encrypt column/CSR files. Use same `XChaCha20-Poly1305` path. |

## Files to Create/Modify

| File | Change |
|---|---|
| `crates/sparrowdb/src/bulk.rs` | New: `BulkLoader`, `BulkOptions`, `Format`, core loading logic |
| `crates/sparrowdb/src/lib.rs` | Export `bulk` module |
| `crates/sparrowdb-cli/src/main.rs` | New `bulk-import` subcommand |
| `crates/sparrowdb-storage/src/node_store.rs` | Add `write_column_batch()` for bulk column writes |
| `crates/sparrowdb-storage/src/csr.rs` | Add `build_and_write()` that builds + writes CSR in one step |
| `crates/sparrowdb-storage/src/edge_store.rs` | Add `write_csr_from_sorted()` for direct CSR creation |
| `crates/sparrowdb-node/src/lib.rs` | Expose `bulkImport()` binding |
| `crates/sparrowdb-python/src/lib.rs` | Expose `bulk_import()` binding |
