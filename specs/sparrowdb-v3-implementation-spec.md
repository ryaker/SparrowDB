# SparrowDB v3 Implementation Spec

**Version:** v3.0-draft  
**Date:** March 20, 2026  
**Owner:** Rich Yaker  
**Prepared by:** Codex PM synthesis over the v2 spec plus Gemini, Grok, and Opus edit passes  
**Purpose:** Implementation-ready spec for Claude Code after the second external review round.

## 1. Document Role

This document supersedes the workspace v1.0 implementation draft.

It keeps the March 20 architecture baseline as the source of truth for product thesis and v1 scope, and it tightens the implementation layer with:

- byte-exact file-format tables for the critical durable structures
- explicit multi-label and undirected-edge semantics
- exact query-result contracts for Rust and Python
- a recovery-idempotency mechanism that does not alter the baseline page header
- golden-fixture and crash-failpoint gates
- a concrete fresh-database bootstrap contract
- a concrete catalog serialization contract

If this document conflicts with the architecture baseline, the baseline wins unless this document explicitly marks the change as a **v1 clarification** or a **proposed scope cut requiring approval**.

## 2. Review-Round Synthesis Decisions

These decisions are now fixed for v3 of the spec:

1. **Adopted:** byte-exact layouts for superblock, metapage, page header, WAL framing, delta records, and CSR metadata.
2. **Adopted:** fresh-database initial state, directory creation rules, and a pre-WAL bootstrap path for Phases 1 and 2.
3. **Adopted:** secondary-label scans use owner-table membership bitmaps plus a catalog reverse index, not a global sequence-ID indirection layer.
4. **Adopted:** a concrete TLV catalog payload format so Phase 2 is codeable without invention.
5. **Adopted:** `QueryResult` shape is exact in both Rust and Python.
6. **Adopted:** golden binary fixtures are mandatory for each durable file family before that family is considered stable.
7. **Adopted:** direct maintenance APIs are added alongside Cypher maintenance statements so `CHECKPOINT` and `OPTIMIZE` do not depend on parser completion.
8. **Modified:** replay idempotency is still implemented via a `last_applied_lsn` prefix in mutable page payloads, but torn-page recovery now treats that field as untrusted on CRC failure.
9. **Modified:** encryption stays in v1 and now binds file identity in AAD, defines logical versus physical page sizing, and defines WAL payload encryption.
10. **Rejected:** demoting binary ASP-Join, encryption, `OPTIMIZE`, `VARIANT`, or basic spill-to-disk out of v1 without owner approval.
11. **Rejected:** shared `edge_id` for symmetric undirected writes in v1. v1 undirected syntax remains sugar that creates two directed physical edges with distinct immutable `edge_id` values.

## 3. Product Goal

Ship an embedded Rust graph database that proves four things:

1. SparrowDB creates and reopens a stable, versioned on-disk database.
2. SparrowDB runs a narrow but real Cypher subset over factorized operators.
3. SparrowDB mutates nodes and edges safely with WAL-backed crash recovery.
4. SparrowDB exposes the engine through Rust and Python for the full hello-graph workflow.

v1 is done when the 14 acceptance checks in Section 21 pass.

## 4. Release Thesis

v1 is not "build most of a graph database."
v1 is "prove the durable storage contract and factorized execution model are real enough to justify the broader roadmap."

That means v1 optimizes for:

- format stability over peak performance
- correctness over planner sophistication
- explicit testability over breadth
- a narrow but dependable public API

## 5. v1 Scope Guardrails

### 5.1 Must Ship in v1

- stable on-disk format with superblock, dual metapages, per-page CRC32, WAL, and scrub-on-open
- SWMR transactions with snapshot readers
- typed node column storage with nullable columns
- `STRING` overflow storage
- `VARIANT` as opaque blob storage
- multi-label nodes
- undirected edge syntax sugar
- base plus delta edge mutability
- factorized execution
- S-Join and binary ASP-Join
- manual `CHECKPOINT` and manual `OPTIMIZE`
- opt-in at-rest encryption
- basic external spill for memory-heavy operators
- Rust and Python APIs

### 5.2 Not in v1

- multi-writer MVCC
- background compaction
- multiway WCO joins
- full openCypher support
- variable-length paths
- OPTIONAL MATCH
- vector indexes
- full-text indexes
- GIN JSON indexing
- WASM, Node.js, C, or Go bindings
- user-facing repair tooling

### 5.3 Post-Hello-Graph v1 Work

These remain in v1 scope but are not allowed to block initial storage bring-up:

- PK hash index
- encryption hardening beyond the minimum round-trip and wrong-key checks
- `OPTIMIZE` sort-path performance tuning
- spill-path performance tuning
- richer `VARIANT` coverage beyond round-trip semantics

## 6. Normative Clarifications

These are no longer soft defaults; they are the v3 implementation contract.

1. A node has one **primary storage label** and zero or more **secondary labels**.
2. The packed `node_id` stores the primary storage label in the upper `u16` label field and the label-local `slot_id` in the lower `u48`.
3. `NodeId` is represented in Rust as `pub struct NodeId(pub u64);` and `EdgeId` as `pub struct EdgeId(pub u64);`.
4. `NodeId` packing is `(label_id as u64) << 48 | (slot_id & 0x0000_FFFF_FFFF_FFFF)`.
5. `edge_id` values are monotonic `u64` allocations sourced from `next_edge_id` in the active metapage.
6. `CREATE (n:LabelA:LabelB)` uses the first declared label as the primary storage label.
7. Secondary labels are membership metadata only; no logical node may be duplicated across multiple physical primary-label tables.
8. Relationship tables are keyed by `(src_primary_label, rel_type, dst_primary_label)`.
9. Queries on secondary labels are implemented as node-label filters layered on primary-label-owned rows.
10. `CHECKPOINT` folds base plus delta into a new base and preserves insertion-order adjacency with `sorted = false`.
11. `OPTIMIZE` performs checkpoint work and then sorts neighbor lists by `(neighbor_node_id ASC, edge_id ASC)`, rewriting aligned edge-property columns and producing `sorted = true`.
12. `edge_id` storage is a parallel column aligned with CSR neighbor order in base files and aligned with delta insert order in delta files.
13. Undirected create syntax writes a symmetric pair of directed physical edges with **distinct** immutable `edge_id` values in v1.
14. `DELETE` and `SET` affect only the matched physical edge whose `edge_id` is bound by the query; the symmetric pair created by undirected sugar is not automatically kept in sync after creation.

## 7. Workspace Layout

```text
/
  Cargo.toml
  rust-toolchain.toml
  crates/
    sparrowdb/
    sparrowdb-common/
    sparrowdb-storage/
    sparrowdb-catalog/
    sparrowdb-cypher/
    sparrowdb-execution/
    sparrowdb-python/
  specs/
  fixtures/
    hello_graph/
    recovery/
    compatibility/
    golden/
  benches/
  scripts/
```

## 8. Crate Responsibilities

### 8.1 `sparrowdb-common`

- packed ID types
- error taxonomy
- CRC32 helpers
- binary encoding helpers
- shared enums for types, page families, and WAL kinds

### 8.2 `sparrowdb-storage`

- superblock, metapage, and page codecs
- `pread` and `pwrite` wrappers
- buffer pool and GClock
- WAL write and replay
- node column files, validity files, overflow files
- CSR base files, edge ID column files, delta files
- encryption layer
- scrub-on-open

### 8.3 `sparrowdb-catalog`

- labels
- relationship-table metadata
- immutable `field_id`
- defaults and nullability
- reverse index for secondary labels
- file root pointers

### 8.4 `sparrowdb-cypher`

- parser seam
- AST
- binder
- logical and physical plans
- Cypher feature gating

### 8.5 `sparrowdb-execution`

- typed vectors
- vector groups
- factorized chunks
- operators
- join implementations
- memory tracking and spill

### 8.6 `sparrowdb`

- public Rust API
- transaction entry points
- maintenance APIs
- schema introspection APIs

### 8.7 `sparrowdb-python`

- PyO3 binding surface
- Python result objects
- context manager support

## 9. Public API

### 9.1 Rust API

```rust
pub struct GraphDb;
pub struct ReadTx;
pub struct WriteTx;

pub struct OpenOptions {
    pub create_if_missing: bool,
    pub page_size: u32,
    pub memory_limit_bytes: u64,
    pub encryption_key: Option<[u8; 32]>,
}

impl GraphDb {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self>;
    pub fn open_with_options(path: impl AsRef<std::path::Path>, options: OpenOptions) -> Result<Self>;
    pub fn begin_read(&self) -> Result<ReadTx>;
    pub fn begin_write(&self) -> Result<WriteTx>;
    pub fn checkpoint(&self) -> Result<()>;
    pub fn optimize(&self) -> Result<()>;
    pub fn close(self) -> Result<()>;
}

impl ReadTx {
    pub fn query(&self, cypher: &str) -> Result<QueryResult>;
}

impl WriteTx {
    pub fn execute(&mut self, cypher: &str) -> Result<QueryResult>;
    pub fn commit(self) -> Result<()>;
    pub fn rollback(self) -> Result<()>;
}
```

Constraints:

- `begin_write` blocks while another writer is active
- `begin_read` never blocks on the writer
- `checkpoint()` and `optimize()` acquire the writer path internally
- Cypher maintenance statements and direct maintenance APIs must route to the same engine implementation

### 9.2 Rust Result Shape

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub enum Value {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Variant(Vec<u8>),
    NodeRef(NodeId),
    EdgeRef(EdgeId),
}
```

### 9.3 Python Result Shape

Python result shape is exact:

```python
result.columns  # list[str] in binder order
result.rows     # list[list[object]]
```

Rules:

- each row is a positional list aligned with `result.columns`
- `NULL` maps to `None`
- `NodeRef` maps to a wrapper with `.id: int`
- `EdgeRef` maps to a wrapper with `.id: int`
- wrapper objects are opaque in v1; they do not expose lazy property fetch

## 10. Storage Format Contracts

### 10.1 Database Directory

The engine must create and expect:

```text
db/
  catalog.bin
  nodes/
  edges/
  wal/
```

Fresh database creation in v1 must always create:

- `catalog.bin`
- `nodes/`
- `edges/`
- `wal/`

The engine must also tolerate the presence of empty reserved directories from the architecture baseline:

```text
db/stats/
db/vector/
db/fts/
db/encryption/
```

v1 does not need to create those reserved directories proactively.

### 10.2 `catalog.bin`

`catalog.bin` contains, in order:

1. Superblock at byte `0`
2. Metapage A at byte `128`
3. Metapage B at byte `640`
4. Fixed zero padding from byte `1152` up to the first aligned catalog payload page
5. Catalog payload pages starting at `catalog_payload_start`

Where:

```text
catalog_payload_start = round_up(1152, superblock.page_size)
```

For the default page size of 4096 bytes:

```text
catalog_payload_start = 4096
```

Open order:

1. validate superblock
2. validate both metapages
3. choose the highest valid `txn_id`
4. hydrate catalog from the winner
5. replay WAL newer than the winner's checkpoint horizon

#### 10.2.1 Fresh Database Initial State

When `create_if_missing` creates a new database:

1. write the superblock at byte `0`
2. write Metapage A at byte `128`
3. write Metapage B at byte `640` as invalid all-zero bytes
4. zero-fill bytes `1152..catalog_payload_start-1`
5. write one empty catalog payload page at `catalog_payload_start` with `last_applied_lsn = 0`, zero TLV entries, and `next_page_id = u64::MAX`
6. `fsync` `catalog.bin`
7. create empty `nodes/`, `edges/`, and `wal/` directories

Initial Metapage A values:

```text
txn_id = 1
catalog_root_page_id = 0
node_root_page_id = u64::MAX
edge_root_page_id = u64::MAX
wal_checkpoint_lsn = 0
global_node_count = 0
global_edge_count = 0
next_edge_id = 1
```

Root sentinel rule:

- `u64::MAX` means "root not yet allocated"
- page ID `0` is a valid catalog payload page ID and refers to the first payload page at `catalog_payload_start`

#### 10.2.2 Catalog Payload Format

Catalog payload pages use a tagged TLV format.

Page family:

- page header magic = `Catalog Payload`
- mutable page payload prefix applies

Each page stores:

```text
[16-byte page header]
[8-byte last_applied_lsn]
[catalog TLV bytes...]
[optional zero padding]
[8-byte next_page_id footer at page end]
```

Footer rule:

- the final 8 bytes of each catalog payload page store `next_page_id: u64`
- `u64::MAX` means "no next page"

TLV entry format:

```text
[tag: u16][length: u32][payload: length bytes]
```

Tag values:

| Tag | Meaning |
|---|---|
| `0x0001` | Label definition |
| `0x0002` | Relationship table definition |
| `0x0003` | Column definition |
| `0x0004` | Secondary-label reverse-index entry |
| `0x0005` | Format metadata |

Label definition payload:

```text
label_id: u16
name_len: u16
name: [u8; name_len]
```

Relationship table definition payload:

```text
rel_table_id: u64
src_label_id: u16
dst_label_id: u16
rel_type_len: u16
rel_type: [u8; rel_type_len]
```

Column definition payload:

```text
owner_kind: u8        // 0 = node label, 1 = relationship table
owner_id: u64
field_id: u32
name_len: u16
name: [u8; name_len]
type_tag: u8
nullable: u8
has_default: u8
default_len: u32
default_bytes: [u8; default_len]
```

Secondary-label reverse-index entry payload:

```text
secondary_label_id: u16
owner_count: u16
owner_label_ids: [u16; owner_count]
```

Golden fixture required:

- `fixtures/golden/catalog-payload-v1.bin`

### 10.3 Superblock

The superblock is exactly 128 bytes.

```text
Offset  Size  Field
------  ----  -----
  0       8   magic = "GRAPHDB1"
  8       4   format_version: u32 = 1
 12       4   page_size: u32
 16       8   flags: u64 (must be 0 in v1)
 24       8   created_at_unix_millis: i64
 32      16   db_uuid: [u8; 16]
 48       4   crc32c: u32 (field zeroed during calculation)
 52      76   reserved: zero-filled
```

Validation rules:

- `magic` must match exactly
- `format_version` must be `1`
- `page_size` must be a power of two and at least `4096`
- CRC mismatch is fatal

Golden fixture required:

- `fixtures/golden/superblock-v1.bin`

### 10.4 Universal Page Header

Every persisted page begins with the same 16-byte header.

```text
Offset  Size  Field
------  ----  -----
  0       4   magic: u32
  4       4   crc32c: u32 (field zeroed during calculation)
  8       4   version: u32 (must be 1 in v1)
 12       4   flags: u32
```

Flag bits:

- bit `0`: encrypted
- bit `1`: compressed
- bit `2`: sorted
- bits `3..31`: reserved zero in v1

Page-family magic constants:

| Page Family | Magic |
|---|---|
| Metapage | `0x4D455441` |
| Catalog Payload | `0x43415401` |
| Node Column | `0x4E4F4430` |
| Node Validity | `0x4E4F4431` |
| String Overflow | `0x53545230` |
| Variant Overflow | `0x56415230` |
| Membership Bitmap | `0x4C424D50` |
| CSR Metadata | `0x4353524D` |
| CSR Offsets | `0x4353524F` |
| CSR Neighbors | `0x4353524E` |
| Edge ID Column | `0x45494430` |
| Edge Property Column | `0x45505250` |
| Delta Page | `0x44454C54` |
| WAL Segment Header | `0x57414C30` |

Golden fixture required:

- `fixtures/golden/page-header-v1.bin`

### 10.5 Mutable Page Payload Prefix

The architecture baseline fixes the universal page header at 16 bytes.
To support replay idempotency without changing that contract, every **mutable** page family must reserve the first 8 bytes of its payload for:

```text
u64 last_applied_lsn
```

Mutable page families:

- Catalog Payload
- Node Column
- Node Validity
- String Overflow
- Variant Overflow
- Membership Bitmap
- CSR Offsets
- CSR Neighbors
- Edge ID Column
- Edge Property Column
- Delta Page

Immutable page families:

- Superblock
- Metapage
- CSR Metadata
- WAL segment framing

Rules:

- mutable-page readers must treat usable payload as `superblock.page_size - 16 - 8`
- recovery compares WAL record LSN to `last_applied_lsn` before applying a page-local mutation
- once a record is applied to a page, the page must be rewritten with `last_applied_lsn = record.lsn`
- if a page fails CRC, `last_applied_lsn` is untrusted and must be ignored during recovery

### 10.6 Metapage

Each metapage is exactly 512 bytes and must fit in a single sector.

```text
Offset  Size  Field
------  ----  -----
  0      16   page_header
 16       8   txn_id: u64
 24       8   catalog_root_page_id: u64
 32       8   node_root_page_id: u64
 40       8   edge_root_page_id: u64
 48       8   wal_checkpoint_lsn: u64
 56       8   global_node_count: u64
 64       8   global_edge_count: u64
 72       8   next_edge_id: u64
 80     432   reserved: zero-filled
```

Rules:

- the winning metapage is the valid metapage with the highest `txn_id`
- `wal_checkpoint_lsn` is the recovery horizon; only WAL records with greater LSN are considered for replay

Golden fixture required:

- `fixtures/golden/metapage-v1.bin`

### 10.7 Node Column Files

Each primary label owns:

```text
nodes/{label_id}/
  membership/
    secondary_{member_label_id}.roaring
  col_{field_id}.bin
  col_{field_id}.validity.bin
  col_{field_id}.overflow.bin
```

Rules:

- fixed-width values are page-organized arrays
- nullable fixed-width columns use a dedicated validity file
- `STRING` and `VARIANT` store an 8-byte ref in the main column and variable bytes in overflow
- `StringRef` and `VariantRef` layout is `u32 offset | u32 length`
- overflow files are append-only in v1; no space reclamation is required

Slot allocation:

- `slot_id` is assigned by append within each primary-label table
- the next assignable `slot_id` equals the current high-water mark for that label
- deleted slots are tombstoned via validity state and are never reused in v1
- live row count requires scanning the validity structure; high-water mark is not live count

### 10.8 Multi-Label Membership

Canonical representation:

- each logical node exists once in its primary-label table
- each owner table may carry zero or more secondary-label membership bitmaps keyed by `(owner_label_id, member_label_id)`
- the catalog keeps a reverse index:

```text
secondary_label_id -> Vec<owner_label_id>
```

Scan semantics:

- scanning primary label `A` reads table `A`
- scanning secondary label `B` reads owner tables from the reverse index plus rows whose primary label is `B`

This avoids both duplicate row storage and full-table scatter across all labels.

### 10.9 Edge Files

Each relationship table maps to `(src_primary_label, rel_type, dst_primary_label)`.

```text
edges/{rel_table_id}/
  base.fwd.csr
  base.bwd.csr
  edge_ids.fwd.bin
  edge_ids.bwd.bin
  prop_{field_id}.fwd.bin
  prop_{field_id}.bwd.bin
  delta.log
```

`metadata.bin` from earlier drafts is superseded in v3 by the metadata header embedded as page 0 of each `.csr` file.

#### 10.9.1 CSR Metadata Header Page

The first page of each `base.{dir}.csr` file is a metadata page.

```text
Offset  Size  Field
------  ----  -----
  0      16   page_header (magic = CSR Metadata)
 16       8   rel_table_id: u64
 24       1   direction: u8 (0 = forward, 1 = backward)
 25       1   sorted: u8 (0 or 1)
 26       6   reserved
 32       8   source_node_count: u64
 40       8   edge_count: u64
 48       8   offsets_root_page_id: u64
 56       8   neighbors_root_page_id: u64
 64       8   edge_ids_root_page_id: u64
 72       8   prop_root_page_id: u64
 80      16   relation_signature: [u8; 16] (reserved zero in v1)
 96     400   reserved
```

Rules:

- `sorted` must mirror page-header flag bit `2`
- `edge_ids_root_page_id` points into the parallel `edge_ids.{dir}.bin` family, not the CSR file itself
- `prop_root_page_id` points into the direction-matched `prop_{field_id}.{dir}.bin` family
- `relation_signature` must be all zero bytes in v1

Golden fixture required:

- `fixtures/golden/csr-metadata-v1.bin`

### 10.10 Delta Record Formats

`delta.log` is a paged append-only file of delta pages.

Page rules:

- every delta page uses page family magic `Delta Page`
- mutable page payload prefix applies
- delta records are packed into the page payload region after `last_applied_lsn`
- a delta record must not cross page boundaries; if the remaining bytes in the current page are insufficient, the writer pads the rest of the page payload with zero and starts a fresh delta page

#### Insert

```text
Offset  Size  Field
------  ----  -----
  0       1   kind = 0x01
  1       8   txn_id: u64
  9       8   edge_id: u64
 17       8   src_node_id: u64
 25       8   dst_node_id: u64
 33       4   prop_payload_len: u32
 37       *   prop_payload
 37+n     4   crc32c: u32
```

#### Delete

```text
Offset  Size  Field
------  ----  -----
  0       1   kind = 0x02
  1       8   txn_id: u64
  9       8   edge_id: u64
 17       4   crc32c: u32
```

#### Update

```text
Offset  Size  Field
------  ----  -----
  0       1   kind = 0x03
  1       8   txn_id: u64
  9       8   edge_id: u64
 17       4   prop_payload_len: u32
 21       *   prop_payload
 21+n     4   crc32c: u32
```

Visibility rule:

- a reader pinned to metapage `txn_id = T` must only consider delta records with `txn_id <= T`

Golden fixture required:

- `fixtures/golden/delta-insert-v1.bin`
- `fixtures/golden/delta-delete-v1.bin`
- `fixtures/golden/delta-update-v1.bin`

### 10.11 Encryption Layout

Encryption remains in v1.

Algorithm:

- XChaCha20-Poly1305

Logical versus physical sizing:

- `superblock.page_size` is the logical plaintext page size used by the buffer pool, page math, and page-family codecs
- encrypted paged files use a physical storage stride of `page_size + 40`
- unencrypted paged files use a physical storage stride of `page_size`

Encryption applies to:

- catalog payload pages
- node column, validity, overflow, and membership pages
- CSR offsets and neighbors pages
- edge ID and edge property pages
- delta pages

Encryption does **not** apply to:

- the superblock
- Metapage A or Metapage B
- the fixed zero padding area before `catalog_payload_start`

Paged file encoding:

```text
[0..15]                    plaintext 16-byte page header
[16..39]                   24-byte nonce
[40..page_size+23]         ciphertext of logical payload bytes
[page_size+24..page_size+39] 16-byte authentication tag
```

Rules:

- flag bit `0` must be set on encrypted pages
- nonce must be freshly generated from OS CSPRNG for every page write
- file-local logical page number is 0-based and computed from the first logical page in that file family
- authenticated data must include:
  - the 16-byte plaintext page header
  - the file-local logical page number
  - the page-family magic
  - the stable owner ID for the file family
- owner ID is:
  - `label_id` for node-owned files
  - `rel_table_id` for relationship-owned files
  - `0` for catalog payload pages
- CRC32-C is computed over the full stored encrypted page image with the CRC field zeroed
- wrong-key open or tag mismatch must return a distinct authentication failure error
- physical offset of logical page `n` in a paged encrypted file is:

```text
catalog payload pages: catalog_payload_start + n * (page_size + 40)
standalone paged files: n * (page_size + 40)
```

Threat-model limit:

- v1 detects corruption and wrong-key use but does **not** claim rollback-attack protection across whole-database page swaps

Golden fixture required:

- `fixtures/golden/encrypted-page-v1.bin`

### 10.12 Golden Fixture Contract

Before a durable file family is considered stable, the repo must contain:

1. a golden binary fixture for that family
2. a round-trip test
3. a corruption test that flips one byte and verifies detection

Minimum mandatory fixtures before Phase 3 exit:

- superblock
- page header
- metapage
- CSR metadata header
- WAL record
- delta insert, delete, and update

Additional fixture timing:

- `fixtures/golden/catalog-payload-v1.bin` is required before Phase 2 exit
- `fixtures/golden/encrypted-page-v1.bin` is required before Phase 6 exit, not Phase 3 exit

## 11. Transactions and Recovery

### 11.1 Concurrency

v1 is SWMR:

- one writer at a time
- many readers
- readers pin the winning metapage at transaction start
- readers never wait for the writer lock

### 11.2 Commit Sequence

The commit sequence is fixed:

1. append WAL records
2. `fsync` WAL
3. write data pages and delta pages
4. `fsync` data files
5. write the alternate metapage with incremented `txn_id`
6. `fsync` `catalog.bin`

Bootstrap note:

- Phases 1 and 2 may persist `catalog.bin` directly without WAL, using only direct writes plus the dual-metapage publish protocol
- WAL-backed commit becomes mandatory starting in Phase 3
- tests before Phase 3 must not depend on WAL replay

### 11.3 WAL Framing

WAL segments live in `wal/seg_{number}.wal`.

```text
Offset  Size  Field
------  ----  -----
  0       8   lsn: u64
  8       8   txn_id: u64
 16       1   kind: u8
 17       4   payload_len: u32
 21       *   payload
 21+n     4   crc32c: u32
```

WAL rules:

- `lsn` is a global monotonic `u64`
- record start offset within a segment is part of the WAL record's identity
- records must not straddle segments; if a record would overflow the active segment, the writer pads the remaining bytes with zero and starts the next segment
- WAL segment framing is always plaintext
- when database encryption is enabled, WAL payload bytes are encrypted but framing stays plaintext

WAL record kinds:

| Kind | Value | Payload |
|---|---|---|
| `TxnBegin` | `0x01` | empty |
| `TxnCommit` | `0x02` | empty |
| `CatalogMutation` | `0x10` | page-image payload |
| `NodeAppend` | `0x20` | exact node-mutation payload |
| `NodeDelete` | `0x21` | exact node-delete payload |
| `NodeUpdate` | `0x22` | exact node-mutation payload |
| `EdgeDeltaInsert` | `0x30` | exact edge-insert payload |
| `EdgeDeltaDelete` | `0x31` | exact edge-delete payload |
| `EdgeDeltaUpdate` | `0x32` | exact edge-update payload |
| `CheckpointBegin` | `0x40` | empty |
| `CheckpointEnd` | `0x41` | empty |
| `OptimizeBegin` | `0x42` | empty |
| `OptimizeEnd` | `0x43` | empty |

#### 11.3.1 WAL Payload Layouts

`CatalogMutation` payload:

```text
Offset  Size  Field
------  ----  -----
  0       8   target_page_id: u64
  8       4   image_len: u32
 12       *   page_image
```

When database encryption is enabled for WAL:

- `page_image` or logical payload bytes are stored as:

```text
[24-byte nonce][ciphertext][16-byte tag]
```

- `payload_len` counts the stored encrypted payload bytes
- authenticated data for encrypted WAL payloads must include:
  - `lsn`
  - `txn_id`
  - `kind`
  - WAL segment number
  - record start offset within the segment

`NodeAppend` and `NodeUpdate` payload:

```text
Offset  Size  Field
------  ----  -----
  0       2   label_id: u16
  2       8   slot_id: u64
 10       2   field_count: u16
 12       *   repeated field entries
```

Field entry layout:

```text
Offset  Size  Field
------  ----  -----
  0       4   field_id: u32
  4       1   type_tag: u8
  5       1   is_null: u8
  6       2   reserved
  8       4   value_len: u32
 12       *   value_bytes
```

Type tags:

| Type | `type_tag` |
|---|---|
| `INT64` | `0x01` |
| `FLOAT64` | `0x02` |
| `BOOL` | `0x03` |
| `STRING` | `0x04` |
| `VARIANT` | `0x05` |

`NodeDelete` payload:

```text
Offset  Size  Field
------  ----  -----
  0       2   label_id: u16
  2       8   slot_id: u64
```

`EdgeDeltaInsert` payload:

```text
Offset  Size  Field
------  ----  -----
  0       8   rel_table_id: u64
  8       *   delta-insert-record
```

`EdgeDeltaDelete` payload:

```text
Offset  Size  Field
------  ----  -----
  0       8   rel_table_id: u64
  8       8   edge_id: u64
```

`EdgeDeltaUpdate` payload:

```text
Offset  Size  Field
------  ----  -----
  0       8   rel_table_id: u64
  8       8   edge_id: u64
 16       4   prop_payload_len: u32
 20       *   prop_payload
```

Golden fixture required:

- `fixtures/golden/wal-record-v1.bin`

### 11.4 Replay Rules

Recovery algorithm:

1. validate superblock
2. choose winning metapage
3. scrub referenced files
4. scan WAL records with `lsn > winning_metapage.wal_checkpoint_lsn`
5. identify committed transactions by `TxnBegin` plus `TxnCommit`
6. replay committed records in LSN order

Replay idempotency contract:

- each mutable target page stores `last_applied_lsn` in its payload prefix
- if `record.lsn <= page.last_applied_lsn`, that page-local mutation is skipped
- page write and `last_applied_lsn` update must occur in the same physical page image

Torn-page recovery contract:

- if a page fails CRC during recovery, `last_applied_lsn` on that page is untrusted and must be ignored
- recovery must treat the torn page as missing and reconstruct it from the last durable checkpoint state plus committed WAL records newer than `wal_checkpoint_lsn`
- if the implementation cannot reconstruct a torn page from committed WAL history, open must fail with `CorruptionDetected`

Multi-page mutation rule:

- if a single logical WAL record affects multiple pages, replay must re-drive every target page needed to make that logical mutation consistent
- recovery must not assume that successful skip on one target page proves the full logical mutation was durably applied everywhere

Maintenance replay rule:

- `CheckpointBegin`, `CheckpointEnd`, `OptimizeBegin`, and `OptimizeEnd` are control records
- recovery must not re-execute checkpoint or optimize as logical operations
- freshly rewritten pages become visible only through alternate-metapage publish
- if crash occurs before metapage publish, unreachable rewritten pages are ignored as orphaned work

### 11.5 Delta Replay Correctness

Delta replay must satisfy:

- reapplying an insert record for the same `edge_id` must not create duplicate visible logical edges
- delete and update replay are keyed by `edge_id`
- when checkpoint or optimize rewrites delta into a fresh base, recovery must ignore WAL records at or below the metapage checkpoint horizon
- checkpoint and optimize must allocate fresh base pages rather than rewriting the active base in place
- every page rewritten by checkpoint or optimize must have `last_applied_lsn` set to the corresponding `CheckpointEnd` or `OptimizeEnd` LSN before metapage publish

### 11.6 Scrub-on-Open

On open, the engine must verify:

- superblock
- both metapages
- the entire fixed `catalog.bin` preamble through byte `catalog_payload_start - 1`
- every catalog payload page reachable from the winning `catalog_root_page_id`
- first page of every active node, edge, and WAL file referenced by the winning metapage
- last up to 256 pages of each active mutable file family

If scrub finds corruption:

- attempt WAL-based recovery if the corruption is on a mutable page newer than the checkpoint horizon
- otherwise fail closed with corruption error

## 12. Query Pipeline

Stages:

1. parse
2. bind
3. logical plan
4. physical plan
5. execute

### 12.1 Parser Seam

```rust
pub trait CypherParser {
    fn parse_statement(&self, input: &str) -> Result<AstStatement>;
}
```

The repo must also contain one documented minimal AST example for each statement family in Section 15.

### 12.2 Binder Duties

- resolve labels, relationship tables, and fields against catalog
- distinguish primary-label scans from secondary-label filters
- reject unsupported features with stable error codes
- route maintenance statements to maintenance plan nodes
- reject `DELETE` on a node with incident edges in v1

## 13. Factorized Execution

### 13.1 Core Structures

```rust
pub struct TypedVector;

pub struct VectorGroup {
    pub cur_idx: Option<usize>,
    pub vectors: Vec<TypedVector>,
}

pub struct FactorizedChunk {
    pub groups: Vec<VectorGroup>,
    pub multiplicity: u64,
}
```

### 13.2 Semantics

- a flat group has `cur_idx = Some(i)`
- an unflat group has `cur_idx = None` and represents a set
- multiplicity multiplies logical row count without forcing materialization

#### Example: `COUNT(*)`

If a chunk has:

- `multiplicity = 2`
- one flat group
- one unflat group with 3 values

Then `COUNT(*)` contributes `2 * 3 = 6`.

#### Example: 2-hop expansion

If `a = Alice` is flat, `b = {Bob, Carol}` is unflat, and each `b` expands to one `c`, then:

- `a` stays flat
- `b` remains unflat
- `c` is produced as a group aligned to `b`
- the executor must not materialize separate `(Alice, Bob, X)` and `(Alice, Carol, Y)` rows until result projection requires it

#### Double-Unflat Gate

If a join combines:

- left chunk multiplicity `m`
- one unflat left group of size `N`
- one unflat right group of size `M`

Then the implementation must prove either:

- the output chunk preserves factorized structure with effective logical multiplicity `m * N * M`, or
- any materialization step is explicitly isolated to the final projection boundary

The implementation must not silently materialize `N * M` rows during join-state construction when factorized representation can preserve them.

### 13.3 Binary ASP-Join Contract

Binary ASP-Join is required in v1.

Pseudocode contract:

```text
1. Accumulate probe-side factorized chunks and collect probe join keys.
2. Build a roaring semijoin filter from those keys.
3. Scan the build side using only keys admitted by the filter.
4. Build hash state for surviving build tuples.
5. Re-probe accumulated factorized chunks without flattening them unnecessarily.
6. Propagate multiplicity by multiplication, never by duplicated row emission alone.
```

The repo must include at least two golden operator examples:

- one where the semijoin filter eliminates non-matching build rows
- one where `COUNT(*)` over ASP-Join output is validated against multiplicity math
- one "double-unflat" example validating multiplicity-first behavior without accidental Cartesian blowup

## 14. Join Strategy

v1 join inventory:

- S-Join
- binary ASP-Join

Planner guidance:

- prefer the most selective filtered starting point
- prefer ASP-Join for the release-gate 2-hop patterns
- keep heuristics in planner code, not in operator internals

## 15. Cypher v1 Scope

### 15.1 Required Statements

- `CREATE (n:Label {prop: value})`
- `CREATE (n:LabelA:LabelB {prop: value})`
- `CREATE (a)-[:REL]->(b)`
- `CREATE (a)-[:REL]-(b)` as undirected sugar
- `MATCH (n:Label) RETURN ...`
- `MATCH (n:Label) WHERE n.prop = value RETURN ...`
- `MATCH (a)-[:REL]->(b) RETURN ...`
- `MATCH (a)-[:REL]->(b)-[:REL]->(c) RETURN ...`
- `DELETE`
- `SET`
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `ADD COLUMN`
- `SET` on `VARIANT`
- `CHECKPOINT`
- `OPTIMIZE`

Syntax clarifications:

- `DELETE` must be preceded by `MATCH`
- deleting a node with incident edges is an error in v1; `DETACH DELETE` is unsupported
- `SET` uses `MATCH ... SET variable.property = expression`
- `SET` on `VARIANT` accepts opaque byte input expressed as `0x...` hex or a string-cast literal
- `ADD COLUMN` syntax is `ALTER :Label ADD COLUMN name TYPE [DEFAULT expr]`
- `CHECKPOINT` and `OPTIMIZE` are bare keywords with no parentheses
- `RETURN ... AS alias` is supported
- `ORDER BY` and `LIMIT` are not in v1 and must be rejected
- multiple `CREATE` clauses may appear in one statement and may share variables across those clauses

### 15.2 Explicit Rejections

The parser or binder must reject with stable errors:

- `OPTIONAL MATCH`
- variable-length paths
- `UNION`
- `UNWIND`
- subqueries
- `DETACH DELETE`
- `ORDER BY`
- `LIMIT`
- unsupported expressions and functions

### 15.3 Maintenance Semantics

Both Cypher statements and direct APIs are required for:

- `CHECKPOINT`
- `OPTIMIZE`

Rules:

- both require the writer path
- both publish through the standard WAL plus metapage commit protocol
- `CHECKPOINT` produces a fresh base and resets delta
- `CHECKPOINT` never rewrites the active base in place
- `OPTIMIZE` performs checkpoint work and additionally sorts neighbor lists and aligned edge-property columns
- `OPTIMIZE` sort order is `(neighbor_node_id ASC, edge_id ASC)`

## 16. Schema and Catalog

The catalog must store:

- labels
- reverse index for secondary labels
- relationship tables
- columns with immutable `field_id`
- nullability and defaults
- file roots
- v1 format metadata

`ADD COLUMN` rules:

- only allowed if nullable or defaulted
- old rows read as `NULL` or default without rewrite

v1 type system:

| Type | Storage |
|---|---|
| `INT64` | 8-byte direct |
| `FLOAT64` | 8-byte direct |
| `BOOL` | 1-byte direct |
| `STRING` | 8-byte ref plus overflow |
| `VARIANT` | 8-byte ref plus overflow |

`type_tag` byte values used in catalog payloads, WAL payloads, and property payloads must follow the table defined in Section 11.3.1.

## 17. Buffer Manager and Spill

### 17.1 Buffer Pool

- fixed-size frames
- pin and unpin
- dirty tracking
- GClock eviction
- higher weight for catalog roots, CSR offsets, and hash metadata pages

### 17.2 Basic Spill

v1 spill applies to:

- sort
- binary ASP-Join hash state when needed

Rules:

- spilling may write temporary partition or run files under OS temp namespaced by `db_uuid`
- spill files are not part of the durable database format
- a spill-capable operator must complete rather than OOM when intermediate state exceeds memory limit

## 18. Error Model

Stable categories:

- `InvalidFormat`
- `UnsupportedVersion`
- `ChecksumFailure`
- `CorruptionDetected`
- `EncryptionAuthFailed`
- `UnsupportedFeature`
- `SchemaError`
- `WriterBusy`
- `MemoryLimitExceeded`
- `IoError`
- `InternalError`

## 19. Testing Strategy

### 19.1 Test Layers

1. unit tests
2. integration tests
3. crash-recovery failpoint tests
4. compatibility fixtures
5. golden binary fixtures

### 19.2 Mandatory Golden Tests

- superblock
- page header
- metapage
- CSR metadata
- WAL record
- delta insert
- delta delete
- delta update
- encrypted page

### 19.3 Mandatory Parser and Binder Must-Fail Cases

- `OPTIONAL MATCH`
- variable-length path syntax
- `UNION`
- `UNWIND`
- subquery syntax
- unsupported aggregate or scalar function
- reference to unknown label
- reference to unknown relationship type
- reference to unknown property field
- use of unbound variable in `RETURN` or `WHERE`
- `DELETE` on a node with incident edges
- `DETACH DELETE`
- `ORDER BY`
- `LIMIT`

### 19.4 Crash Failpoint Matrix

CI must inject crashes at least at:

1. after WAL append, before WAL fsync
2. after WAL fsync, before first data-page write
3. after partial data-page write, before data fsync
4. after data fsync, before alternate metapage write
5. after alternate metapage write, before `catalog.bin` fsync
6. during checkpoint rewrite before new base fsync
7. after checkpoint new base fsync, before metapage publish
8. during optimize sort rewrite before metapage publish

### 19.5 Compatibility Fixture Policy

At first v1 release, freeze:

```text
fixtures/compatibility/v1.0/
```

Future builds must open and query that fixture successfully.

## 20. Milestone Order

### Phase 0: Repo Bootstrap

- workspace `Cargo.toml`
- `rust-toolchain.toml` on stable, edition 2021
- crate skeletons
- CI:
  - `cargo fmt --check`
  - `cargo clippy -- -D warnings`
  - `cargo test --workspace`
- spec check-in

### Phase 1: Durable Core Codecs

- superblock
- page header
- metapage
- fresh-database bootstrap contract
- golden fixtures for superblock, page header, and metapage
- create and reopen empty DB

### Phase 2: Catalog and Nodes

- catalog TLV pages
- catalog payload golden fixture
- primary and secondary labels
- reverse index
- fixed-width columns
- validity bitmaps
- string and variant overflow
- append-only slot allocation

### Phase 3: Edges, WAL, and Recovery

- CSR metadata and base files
- directional edge ID columns
- delta records
- WAL framing and encrypted payload handling
- replay
- crash failpoints

### Phase 4a: Parser and Binder

- lexer and parser for all supported statement families
- AST examples
- binder
- must-fail coverage
- gate: every supported statement family parses and binds against a test catalog

### Phase 4b: Scan, Filter, Project

- primary-label scan
- secondary-label scan via reverse index plus membership filters
- property filter
- projection
- gate: simple `MATCH ... RETURN ...` works end-to-end against persisted data

### Phase 4c: Expand, Joins, Aggregates

- expand
- S-Join
- binary ASP-Join
- aggregate operators
- gate: release-gate 2-hop query and ASP-Join multiplicity examples pass

### Phase 5: Transactions and Maintenance

- snapshot pinning
- writer serialization
- `CHECKPOINT`
- `OPTIMIZE`

### Phase 6: Encryption, Spill, Python, Release Gate

- encrypted page path
- wrong-key failure test
- basic spill
- Python bindings
- compatibility fixtures
- full acceptance suite

## 21. Acceptance Checks

Each check must exist as one named test.

1. `create_db_directory`
2. `single_writer_create_two_nodes_one_edge_commit`
3. `single_reader_match_person_names`
4. `two_hop_match_via_binary_asp_join`
5. `restart_and_reopen_persisted_data`
6. `wal_replay_atomicity_mid_commit_crash`
7. `python_binding_core_roundtrip`
8. `superblock_format_version_matches_expected`
9. `multi_label_node_roundtrip`
10. `undirected_edge_and_encryption_roundtrip`
11. `snapshot_reader_survives_writer_commit`
12. `checkpoint_and_optimize_rewrite_clean_base`
13. `format_compatibility_fixture_opens`
14. `node_and_edge_update_delete_survive_restart`

## 22. Definition of Done

SparrowDB v1 is done when:

- all 14 acceptance checks pass
- all mandatory golden tests pass
- crash failpoint matrix passes
- unsupported Cypher features fail cleanly
- Python bindings pass the required workflow
- the team can explain the byte layout and recovery flow without hand-waving

## 23. Open Questions for Next Review Round

These are the highest-value remaining stress points:

1. Is the `last_applied_lsn` mutable-payload prefix still the right replay strategy, or should v1 eventually move to more page-image-heavy WAL for mutable page families?
2. Should WAL payload encryption stay record-level in v1, or is there a materially simpler secure alternative?
3. Is distinct `edge_id` for undirected sugar still the best v1 trade-off after implementation prototyping?
4. Should PK hash index remain post-hello-graph-but-pre-GA, or should it become a hard GA gate?
