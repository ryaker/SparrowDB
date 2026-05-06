# SparrowDB Architecture

## Overview

SparrowDB is an embedded, single-process graph database written in Rust. It stores property graphs with labeled nodes and typed directed edges, and accepts a subset of the Cypher query language. It is designed for use cases where a lightweight, dependency-free graph store is preferable to running a separate server process. There is no network server, no distributed mode, and no WAL replication. All state lives in a directory on the local filesystem.

---

## Crate Structure

The workspace contains eleven crates. The five that form the core are:

| Crate | Responsibility |
|---|---|
| `sparrowdb-common` | Primitive types (`NodeId`, `EdgeId`, `PageId`, `Lsn`, `TxnId`), the `Error` enum, and the canonical `col_id_of` function (FNV-1a hash of a property key name). |
| `sparrowdb-catalog` | Label and relationship-type registry. Backed by an append-only TLV file (`catalog.tlv`). Maps label names to `u16` IDs and relationship type names to `u64` IDs. |
| `sparrowdb-storage` | All on-disk data structures: the node property column files, edge delta log, CSR adjacency files, WAL, page store (with optional encryption), property index, text index, full-text index, and the dual-metapage. |
| `sparrowdb-cypher` | Lexer, parser, AST, and binder. Converts a Cypher string to a `BoundStatement`. |
| `sparrowdb-execution` | The `Engine` struct. Walks the bound AST and drives reads and writes against the storage layer. Returns a `QueryResult`. |

The top-level `sparrowdb` crate re-exports the public API (`GraphDb`, `ReadTx`, `WriteTx`, `Value`, `NodeId`, `EdgeId`, `Error`, `QueryResult`) and owns the SWMR transaction machinery and the in-memory MVCC version store.

The remaining crates are language bindings (`sparrowdb-python`, `sparrowdb-node`, `sparrowdb-ruby`), a CLI (`sparrowdb-cli`), an MCP server (`sparrowdb-mcp`), a metrics exporter (`sparrowdb-metrics`), and two network transports — the Bolt-protocol server (`sparrowdb-bolt`, port 7687 by default) for Neo4j-compatible drivers, and an HTTP REST server (`sparrowdb-server`, port 7480 by default) that exposes JSON-over-HTTP for browsers and language-agnostic clients.

---

## Storage Layer

### Column-per-property layout

Each node label gets its own directory (`nodes/{label_id}/`). Within that directory, every property key maps to a flat binary column file (`col_{col_id}.bin`). A column file is a fixed-width array of `u64` values, one per slot. A high-water mark file (`hwm.bin`) records the number of allocated slots.

The `col_id` for a property name is derived by `col_id_of`, which applies FNV-1a 32-bit hashing to the UTF-8 bytes of the key name. This mapping is computed independently by both the storage layer and the execution engine; no catalog lookup is required at query time for property access.

Each stored `u64` uses its top byte as a type tag:

| Tag | Meaning |
|---|---|
| `0x00` | `Int64` — lower 56 bits are two's-complement signed integer. |
| `0x01` | `Bytes` (inline) — up to 7 bytes of string data packed into the lower 7 bytes. |
| `0x02` | `BytesOverflow` — lower 5 bytes are heap offset (u40 LE), next 2 bytes are length (u16 LE), pointing into `strings.bin`. |
| `0x03` | `Float` — same overflow pointer layout, pointing to 8 raw IEEE-754 bytes in `strings.bin`. |

Slot zero of `col_0` is the node's existence sentinel. A value of `u64::MAX` in `col_0` is a tombstone (node deleted). A raw zero in any column means "never written" (absent). This means `Int64(0)` cannot be distinguished from an absent property using the raw column alone; the version chain must be consulted when this matters.

### CSR adjacency store

Edges are stored per relationship type in a subdirectory (`edges/{rel_table_id}/`). Each relationship type has three files:

- `delta.log` — append-only log of edge records, 20 bytes each (`src: u64 LE`, `dst: u64 LE`, `rel_id: u32 LE`).
- `base.fwd.csr` — Compressed Sparse Row forward adjacency (src → list of dst).
- `base.bwd.csr` — CSR backward adjacency (dst → list of src).

The CSR binary layout is:

```
[n_nodes: u64 LE]
[offsets: (n_nodes+1) × u64 LE]   — offsets[i] = start index of node i's neighbor list
[neighbors: n_edges × u64 LE]     — packed neighbor node IDs
```

New edges are appended to `delta.log`. The CSR files are rebuilt from the full delta log at checkpoint time (see below). At query time, the execution engine reads both the CSR base files and the delta log so that edges created since the last checkpoint are visible.

### WAL

The write-ahead log lives in a `wal/` subdirectory. Each record has the layout:

```
[length: u32 LE]    — covers everything after this field
[kind:   u8]        — record type
[lsn:    u64 LE]    — log sequence number
[txn_id: u64 LE]
[payload: variable]
[crc32c: u32 LE]    — over bytes [kind..payload_end]
```

Record kinds used for mutations are `NodeCreate` (0x10), `NodeUpdate` (0x11), `NodeDelete` (0x12), and `EdgeCreate` (0x13). Checkpoint markers are `CheckpointBegin` (0x05) and `CheckpointEnd` (0x06).

When encryption is enabled, the WAL payload bytes are encrypted with XChaCha20-Poly1305 before the CRC is appended. The LSN is used as AEAD associated data, binding each encrypted payload to its position in the log.

### Checkpoint

A checkpoint folds the edge delta log into the CSR base files. The sequence is:

1. Emit and fsync a `CheckpointBegin` WAL record.
2. For each relationship table, read the full delta log, build a new CSR (forward and backward), and write it to a temporary file, then atomically rename it over the existing base file.
3. Emit and fsync a `CheckpointEnd` WAL record.
4. Write the new `wal_checkpoint_lsn` (the LSN of `CheckpointEnd`) to the dual metapage.

Crash before step 3 completes: recovery finds `CheckpointBegin` without a matching `CheckpointEnd` and replays from the prior checkpoint LSN, ignoring the incomplete checkpoint.

Crash after step 3 but before step 4: recovery locates the `CheckpointEnd` LSN by scanning the WAL and treats it as the new replay horizon.

`OPTIMIZE` runs the same sequence but additionally sorts each source node's neighbor list by destination node ID, which improves sequential scan locality on hop queries.

### Dual metapage

`catalog.bin` begins with two 512-byte metapages at fixed offsets (128 and 640). Each metapage carries a `txn_id`, root page pointers, the `wal_checkpoint_lsn`, and global counts. The winning metapage on open is the valid one (magic and CRC32C match) with the higher `txn_id`. This gives atomic, crash-safe metapage updates without journaling the metapage itself.

### Page store

`PageStore` is the lowest-level I/O primitive. It maps logical page IDs to byte offsets in a flat file. Without encryption, page `n` is at `n * page_size`. With encryption enabled, each page occupies `page_size + 40` bytes on disk:

```
[nonce: 24 bytes][ciphertext + AEAD tag: page_size + 16 bytes]
```

---

## Transaction Model

SparrowDB uses Single-Writer Multiple-Reader (SWMR) concurrency.

**Single writer.** `GraphDb::begin_write` acquires an `AtomicBool` flag with a compare-exchange (`false → true`). If the flag is already `true`, `Error::WriterBusy` is returned immediately — there is no blocking wait. The flag is released via `Drop` on the `WriteGuard` RAII wrapper, so it is always reset even on panic.

**Multiple readers.** `GraphDb::begin_read` takes a snapshot of the current `txn_id` (an `AtomicU64`) at open time. Any number of `ReadTx` handles may coexist with an active writer. Each reader sees only data committed at or before its snapshot.

**Snapshot isolation for property updates.** Property updates (`set_node_col`) are recorded in an in-memory version chain keyed by `(NodeId, col_id)`. Each entry is a `(committed_at_txn_id, Value)` pair. When a `ReadTx` reads a property, it binary-searches the version chain for the most recent version with `committed_at <= snapshot_txn_id`, falling back to the on-disk column value if no chain entry exists within the snapshot window.

**Write-write conflict detection.** A `NodeVersions` map tracks the `txn_id` of the last committed write to each node. On commit, if any node in the write set has a `last_committed_txn_id` greater than the writer's `snapshot_txn_id`, the commit is rejected with `Error::WriteWriteConflict`.

**Structural buffering.** Node creates, node deletes, and edge creates are staged in a `PendingOp` buffer within the `WriteTx`. They are applied to disk only at commit time. Property updates are staged separately in a `WriteBuffer`. On commit, the WAL is appended, disk is updated, and then the in-memory version chain is updated — in that order.

**Version chain growth.** The version chain is never pruned in the current implementation. It grows proportionally to the total number of committed property updates across the lifetime of the process. A garbage-collection pass keyed to the oldest open `ReadTx` snapshot is a known gap.

---

## Execution Engine

A Cypher query follows four steps from string to result.

### 1. Parse

`sparrowdb_cypher::parse` runs a hand-written lexer and recursive-descent parser over the input string and produces a `Statement` AST. The AST covers `MATCH`, `CREATE`, `MERGE`, `SET`, `DELETE`, `RETURN`, `WITH`, `UNWIND`, `UNION`, `OPTIONAL MATCH`, `CALL`, and pipeline (`|>`) forms.

### 2. Bind

`sparrowdb_cypher::bind` walks the AST and validates label names and relationship types against the catalog. `MATCH` patterns must reference labels and relationship types that already exist in the catalog; `CREATE` and `MERGE` patterns skip this check because those operations auto-register labels. The binder returns a `BoundStatement` wrapping the same AST — structural AST rewriting is not done in this phase.

### 3. Execute

`sparrowdb_execution::Engine` holds a `NodeStore`, a `Catalog`, a per-relationship-type map of `CsrForward` files, and lazy-built indexes. Dispatch is by `Statement` variant.

**Read path — full scan.** For a `MATCH (n:Label) RETURN ...` with no property filter, the engine iterates slots `0..hwm` of the label's `col_0` column, skips tombstoned slots (`col_0 == u64::MAX`), reads the requested property columns, applies any `WHERE` clause, and collects result rows.

**Read path — index lookup.** When the `WHERE` clause (or inline property map) filters on a specific property, the engine calls `PropertyIndex::build_for(label_id, col_id)` if the index for that column has not been built yet. The property index is an in-memory `BTreeMap<sort_key, Vec<slot>>`. An equality lookup resolves directly to a slot list; a range predicate uses `BTreeMap::range`. Text predicates (`CONTAINS`, `STARTS WITH`) use a parallel `TextIndex` that stores `(decoded_string, slot)` pairs and supports binary-search prefix scans.

**One-hop traversal.** For `(a:L1)-[:R]->(b:L2)`, the engine iterates source slots, looks up each source node's forward neighbors in the CSR base file, merges in edges from the delta log for that relationship type, and for each neighbor reads the destination node's properties.

**Multi-hop traversal.** Two-hop and fixed N-hop patterns are unrolled iteratively. Variable-length paths (`-[*min..max]->`) use a BFS capped at `max_hops` (default cap: 10 if unbounded). The BFS maintains a `visited` set of `(slot, label_id)` pairs to prevent revisiting nodes and handles self-loops. Results from depths `>= min_hops` are collected; zero-hop (`*0..`) includes the source node itself.

**Aggregation.** `COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`, and `COUNT(DISTINCT ...)` are detected in the return clause and handled by an aggregation pass over the collected rows. Grouped aggregation (with non-aggregate return items) uses a hash map keyed by the group columns.

**Write path.** Mutation statements (`CREATE`, `MERGE`, `SET`, `DELETE`, `MATCH...CREATE`, `MATCH...SET`, etc.) open a `WriteTx` via `GraphDb::begin_write`, apply mutations through the write buffer and pending-ops queue, then commit. The engine's `execute_write` path handles this; `execute` also opens a write transaction internally for mutation statements.

---

## Property Index

The `PropertyIndex` in `sparrowdb-storage` is an in-memory `BTreeMap<u64, Vec<u32>>` per `(label_id, col_id)` pair, mapping sort keys to slot lists.

**Lazy build.** The index for a given `(label_id, col_id)` is built only when the first query that filters on that column is executed. Building scans the column file once, skips tombstoned slots and absent values (raw zero), and inserts `sort_key(raw) → slot` into the map. Subsequent queries within the same engine instance reuse the cached map.

**Sort-key encoding for Int64.** Raw `u64` order would sort negative integers above positive ones (e.g., `-1` encodes as `0x00FF_FFFF_FFFF_FFFF`). The `sort_key` function flips bit 55 of the 56-bit payload for `TAG_INT64` values: `raw ^ 0x0080_0000_0000_0000`. The same transform is applied to query values in `lookup` and `lookup_range`, so the corrected ordering is transparent to callers.

**Inline vs. overflow string limitation.** For inline strings (≤ 7 bytes), two equal strings produce the same `u64` encoding, so equality lookup on the raw value is equivalent to equality on the decoded string. For overflow strings (> 7 bytes), the `u64` encodes a heap offset and length, not the string content. Two distinct overflow strings with the same content but written at different heap offsets will have different raw `u64` values. The property index therefore does not support equality lookup on overflow strings — those queries fall back to a full scan.

**No persistence.** The index is built from the column file at engine startup and is not written to disk. It must be rebuilt on each new engine instance.

---

## Encryption

When a database is opened with `GraphDb::open_encrypted(path, key)`, a 32-byte master key is stored in `DbInner`. All WAL payloads and all `PageStore` pages are encrypted with XChaCha20-Poly1305.

**Per-page encryption.** Each page write generates a fresh 24-byte nonce from the OS CSPRNG. The nonce and ciphertext are written contiguously, making each page slot `page_size + 40` bytes on disk. The `page_id` is passed as AEAD associated data, binding the ciphertext to its logical position. Swapping the encrypted blob of page A into slot B causes the AEAD tag to fail at read time.

**Per-WAL-record encryption.** WAL payload bytes are encrypted before the CRC is appended. The LSN is the AEAD associated data for WAL records.

**Key handling.** The key is passed in at open time as a `[u8; 32]` and held in memory for the lifetime of `DbInner`. There is no key derivation, key rotation, or key storage mechanism in the database itself — those are the caller's responsibility. Opening a database with the wrong key causes WAL replay to fail with `Error::EncryptionAuthFailed` on the first record read.

---

## NodeId Packing

A `NodeId` is a `u64` packed as:

```
upper 32 bits: label_id (u32)
lower 32 bits: slot index within the label (u32)
```

Packing: `node_id = (label_id as u64) << 32 | slot as u64`.

This layout means `NodeId` values are not globally sequential. Two nodes with the same slot index but different label IDs have different `NodeId` values. The execution engine unpacks the label_id and slot separately when accessing column files or CSR adjacency data.

Note: the doc comment in `sparrowdb-common` describes the layout as "upper 16 bits = label_id, lower 48 bits = slot_id". This is incorrect. The actual code throughout the execution engine and node store uses a 32/32 split. The practical effect is that while 32 bits are reserved for the label ID, the catalog currently generates `u16` IDs, so the effective cap is `u16::MAX`. Slot counts per label are capped at `u32::MAX` (roughly 4 billion nodes per label).

---

## Known Limitations

**Multi-hop traversal performance.** Variable-length BFS is bounded at 10 hops by default. For deep or wide graphs, each BFS level requires reading the delta log in full for every frontier node. A dedicated neighbor-expansion index (equivalent to Neo4j's relationship store) would eliminate this. The current design works well for graphs with low average degree and shallow path depths.

**No distributed mode.** SparrowDB is a single-process, single-machine embedded store. There is no replication, no sharding, and no network protocol. All consumers must be in-process or use the language bindings.

**Overflow string index limitation.** The property index supports equality and range queries on inline strings (≤ 7 bytes) and integers. Strings longer than 7 bytes are stored as heap pointers; two strings with the same content but different heap addresses produce different index keys. Equality queries on long strings fall back to full column scans.

**Int64(0) is the absent sentinel.** A raw zero in a column file means "property not set." `Value::Int64(0)` written to a column will be indistinguishable from an absent property at the raw file level. The version chain records the actual write, so reads within the same process session see the correct value, but a query on a cold open that reads the column file directly will treat slot zero as absent if the version chain has been cleared.

**In-memory version chain unbounded growth.** The MVCC version chain is never garbage collected. Long-running processes with heavy write workloads accumulate all historical property versions in memory for the lifetime of the process.

**Schema changes are not atomic.** Label registration (in `sparrowdb-catalog`) is written immediately and is not part of the write transaction. A crash after label creation but before the node data is written leaves the label registered with no nodes. This is consistent but may surprise callers expecting full schema-change atomicity.
