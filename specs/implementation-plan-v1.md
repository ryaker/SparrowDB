# SparrowDB v1 — Agent Implementation Plan

**Date:** 2026-03-20
**Method:** WSJF (Weighted Shortest Job First) + parallel agent tracks
**Goal:** All 14 acceptance checks pass, all golden fixtures pass, crash failpoint matrix passes

---

## WSJF Scoring Guide

```
WSJF = (User/Business Value + Time Criticality + Risk Reduction) / Job Size
```

Scores use 1–10 scale. Higher WSJF = schedule first.

| Component | Meaning |
|-----------|---------|
| Value | How much correctness/functionality this unlocks downstream |
| Time Crit | How much delay cost increases if deferred |
| Risk | How much implementation risk is retired |
| Size | Relative effort (inverse — smaller = higher WSJF) |

---

## Phase 0: Repo Bootstrap

**Gate:** `cargo build --workspace` clean, CI green, spec checked in.
**Can parallelize:** Minimal — foundational.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P0-1 | Workspace Cargo.toml + rust-toolchain.toml | 10 | 10 | 8 | 1 | 28.0 | none |
| P0-2 | Crate skeletons (7 crates, empty lib.rs) | 9 | 9 | 5 | 2 | 11.5 | P0-1 |
| P0-3 | CI: fmt + clippy + test on push | 8 | 8 | 7 | 2 | 11.5 | P0-2 |
| P0-4 | .gitignore, LICENSE, README, CONTRIBUTING | 5 | 7 | 2 | 1 | 14.0 | P0-1 |
| P0-5 | Spec committed to `specs/` | 10 | 10 | 10 | 1 | 30.0 | P0-1 |

**Parallel opportunity:** P0-4 and P0-5 can run in parallel with P0-2/P0-3 once P0-1 lands.

---

## Phase 1: Durable Core Codecs

**Gate:** `create_db_directory` acceptance check passes. Golden fixtures for superblock, page header, metapage.
**Why first:** Every other component depends on the byte layout being correct. Format bugs discovered late are catastrophic.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P1-1 | `sparrowdb-common`: NodeId/EdgeId packed types | 10 | 10 | 9 | 2 | 14.5 | P0 |
| P1-2 | `sparrowdb-common`: error taxonomy (all 10 categories) | 9 | 9 | 8 | 2 | 13.0 | P0 |
| P1-3 | `sparrowdb-common`: CRC32c helpers + binary encoding | 9 | 10 | 8 | 2 | 13.5 | P0 |
| P1-4 | `sparrowdb-common`: shared enums (TypeTag, PageFamily, WalKind) | 9 | 10 | 7 | 2 | 13.0 | P0 |
| P1-5 | Superblock codec: encode/decode, validation rules | 10 | 10 | 9 | 3 | 9.67 | P1-1..4 |
| P1-6 | Universal page header codec | 10 | 10 | 9 | 2 | 14.5 | P1-1..4 |
| P1-7 | Metapage codec: dual-metapage read, winner selection | 10 | 10 | 10 | 3 | 10.0 | P1-5,6 |
| P1-8 | Fresh database bootstrap: directory + file creation | 10 | 10 | 8 | 3 | 9.33 | P1-5..7 |
| P1-9 | Golden fixture: superblock-v1.bin + round-trip + corruption test | 9 | 9 | 9 | 2 | 13.5 | P1-5 |
| P1-10 | Golden fixture: page-header-v1.bin + tests | 9 | 9 | 9 | 2 | 13.5 | P1-6 |
| P1-11 | Golden fixture: metapage-v1.bin + tests | 9 | 9 | 9 | 2 | 13.5 | P1-7 |
| P1-12 | Integration test: create and reopen empty DB | 10 | 10 | 10 | 2 | 15.0 | P1-8..11 |

**Parallel tracks within Phase 1:**
- Track A: P1-1 → P1-2 → P1-3 → P1-4 (common primitives — serialize)
- Track B: P1-5 + P1-6 can start once P1-1..4 land, run in parallel
- Track C: P1-9, P1-10, P1-11 fixtures can be written in parallel against each other

---

## Phase 2: Catalog and Nodes

**Gate:** Create + reopen DB with labels and typed node columns. `catalog-payload-v1.bin` golden fixture.
**Critical risk:** TLV format bugs discovered post-Phase 3 require file format rev. Lock it down here.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P2-1 | Catalog TLV page codec: label def, rel table def, col def | 10 | 10 | 10 | 4 | 7.5 | P1 |
| P2-2 | Catalog TLV: secondary-label reverse-index entry | 9 | 9 | 8 | 3 | 8.67 | P2-1 |
| P2-3 | Catalog TLV: format-metadata entry | 7 | 8 | 7 | 2 | 11.0 | P2-1 |
| P2-4 | CatalogWriter/CatalogReader: load/save catalog from catalog.bin | 10 | 10 | 9 | 4 | 7.25 | P2-1..3 |
| P2-5 | Golden fixture: catalog-payload-v1.bin + round-trip + corruption | 10 | 10 | 10 | 2 | 15.0 | P2-4 |
| P2-6 | Primary-label table: append-only slot allocation | 9 | 9 | 7 | 3 | 8.33 | P2-4 |
| P2-7 | Fixed-width column files: INT64, FLOAT64, BOOL storage | 9 | 9 | 7 | 3 | 8.33 | P2-6 |
| P2-8 | Validity bitmap file: null tracking per slot | 9 | 9 | 8 | 3 | 8.67 | P2-6 |
| P2-9 | STRING overflow: 8-byte StringRef, append-only overflow file | 8 | 8 | 7 | 3 | 7.67 | P2-7,8 |
| P2-10 | VARIANT overflow: 8-byte VariantRef, append-only overflow file | 7 | 7 | 6 | 3 | 6.67 | P2-9 |
| P2-11 | Multi-label: secondary-label membership bitmaps (roaring) | 8 | 8 | 8 | 4 | 6.0 | P2-6 |
| P2-12 | Reverse index: secondary_label_id → Vec<owner_label_id> | 8 | 8 | 8 | 3 | 8.0 | P2-11 |
| P2-13 | Buffer pool: fixed-size frames, pin/unpin, dirty tracking, GClock | 9 | 9 | 9 | 5 | 5.4 | P1 |
| P2-14 | Integration test: create labels, insert nodes, reopen, verify columns | 10 | 10 | 10 | 3 | 10.0 | P2-6..10 |
| P2-15 | Integration test: multi-label node round-trip | 9 | 9 | 9 | 2 | 13.5 | P2-11,12 |

**Parallel tracks within Phase 2:**
- Track A: P2-1 → P2-2 → P2-3 → P2-4 → P2-5 (catalog codec — must be sequential, highest risk)
- Track B: P2-13 (buffer pool) can start in parallel with Track A immediately after Phase 1
- Track C: P2-6 → P2-7 → P2-8 → P2-9 → P2-10 (node storage) starts after P2-4
- Track D: P2-11 → P2-12 can run in parallel with Track C after P2-6

---

## Phase 3: Edges, WAL, and Recovery

**Gate:** WAL replay passes, all 8 crash failpoints pass, CSR/delta golden fixtures.
**Critical:** WAL correctness is the hardest subsystem. Crash failpoints must be wired before Phase 4. Do not let query work start until recovery is provably correct.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P3-1 | CSR metadata header page codec | 10 | 10 | 10 | 2 | 15.0 | P2 |
| P3-2 | CSR base files: offsets + neighbors pages | 10 | 10 | 9 | 4 | 7.25 | P3-1 |
| P3-3 | Edge ID column files (parallel to CSR) | 9 | 9 | 8 | 3 | 8.67 | P3-1 |
| P3-4 | Directional CSR: forward + backward | 9 | 9 | 8 | 3 | 8.67 | P3-2,3 |
| P3-5 | Delta page codec: insert/delete/update records | 10 | 10 | 10 | 3 | 10.0 | P2 |
| P3-6 | Golden fixture: delta-insert/delete/update-v1.bin + tests | 10 | 10 | 10 | 2 | 15.0 | P3-5 |
| P3-7 | Golden fixture: csr-metadata-v1.bin + tests | 9 | 9 | 9 | 2 | 13.5 | P3-1 |
| P3-8 | WAL framing codec: segment header, record framing, LSN | 10 | 10 | 10 | 4 | 7.5 | P2 |
| P3-9 | WAL writer: append, fsync, segment rotation | 10 | 10 | 10 | 4 | 7.5 | P3-8 |
| P3-10 | WAL record kinds: all 13 payload layouts | 10 | 10 | 9 | 4 | 7.25 | P3-8 |
| P3-11 | Golden fixture: wal-record-v1.bin + tests | 10 | 10 | 10 | 2 | 15.0 | P3-8,10 |
| P3-12 | WAL replay: committed txn identification, LSN ordering | 10 | 10 | 10 | 5 | 6.0 | P3-9,10 |
| P3-13 | Replay idempotency: last_applied_lsn prefix, skip logic | 10 | 10 | 10 | 4 | 7.5 | P3-12 |
| P3-14 | Torn-page recovery: CRC failure → untrust LSN → reconstruct | 10 | 10 | 10 | 5 | 6.0 | P3-13 |
| P3-15 | Crash failpoint infra: fsync injection hooks | 10 | 10 | 10 | 3 | 10.0 | P3-9 |
| P3-16 | Crash failpoints 1-5: WAL append → data fsync | 10 | 10 | 10 | 4 | 7.5 | P3-15 |
| P3-17 | Crash failpoints 6-8: checkpoint/optimize rewrite paths | 9 | 9 | 9 | 4 | 6.75 | P3-16 |
| P3-18 | Commit sequence: full 6-step write path | 10 | 10 | 10 | 4 | 7.5 | P3-9..14 |
| P3-19 | Integration test: WAL replay atomicity mid-commit crash (acceptance #6) | 10 | 10 | 10 | 3 | 10.0 | P3-16 |
| P3-20 | Scrub-on-open: verify catalog, node, edge, WAL on open | 8 | 8 | 9 | 3 | 8.33 | P3-18 |

**Parallel tracks within Phase 3:**
- Track A: P3-1 → P3-2 → P3-3 → P3-4 (CSR structure)
- Track B: P3-5 → P3-6 (delta codec) — fully parallel with Track A
- Track C: P3-8 → P3-9 → P3-10 → P3-11 (WAL codec) — parallel with Tracks A+B
- Track D: P3-12 → P3-13 → P3-14 (replay) after WAL codec
- Track E: P3-15 → P3-16 → P3-17 (failpoints) after WAL writer

---

## Phase 4a: Parser and Binder

**Gate:** Every supported Cypher statement family parses and binds against a test catalog. All must-fail cases rejected.
**Note:** Parser and binder can start in parallel with Phase 3 storage work — they have no storage dependency until 4b.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P4a-1 | Lexer for Cypher v1 token set | 9 | 8 | 7 | 3 | 8.0 | P0 |
| P4a-2 | Parser: CREATE node/edge statements | 9 | 9 | 7 | 4 | 6.25 | P4a-1 |
| P4a-3 | Parser: MATCH patterns (1-hop, 2-hop) | 9 | 9 | 7 | 4 | 6.25 | P4a-1 |
| P4a-4 | Parser: WHERE, RETURN, AS alias | 8 | 8 | 6 | 3 | 7.33 | P4a-2,3 |
| P4a-5 | Parser: DELETE, SET, ADD COLUMN | 8 | 8 | 6 | 3 | 7.33 | P4a-2 |
| P4a-6 | Parser: CHECKPOINT, OPTIMIZE (bare keywords) | 7 | 7 | 5 | 2 | 9.5 | P4a-1 |
| P4a-7 | Parser: aggregates COUNT/SUM/AVG/MIN/MAX | 8 | 8 | 6 | 3 | 7.33 | P4a-4 |
| P4a-8 | AST types + one documented example per statement family | 9 | 9 | 8 | 3 | 8.67 | P4a-1..7 |
| P4a-9 | Binder: resolve labels/rel tables/fields against catalog | 10 | 10 | 9 | 4 | 7.25 | P4a-8 |
| P4a-10 | Binder: primary-label scan vs secondary-label filter distinction | 9 | 9 | 9 | 3 | 9.0 | P4a-9 |
| P4a-11 | Binder: route CHECKPOINT/OPTIMIZE to maintenance plan nodes | 8 | 8 | 7 | 2 | 11.5 | P4a-9 |
| P4a-12 | Binder: reject unsupported features with stable error codes | 9 | 9 | 9 | 3 | 9.0 | P4a-9 |
| P4a-13 | Must-fail test suite: all 14 rejection cases (Section 19.3) | 10 | 10 | 10 | 3 | 10.0 | P4a-12 |
| P4a-14 | CypherParser trait implementation wired to sparrowdb public API | 9 | 9 | 8 | 2 | 13.0 | P4a-8 |

**Parallel tracks:**
- Track A: P4a-1 → P4a-2 → P4a-3 → P4a-4 → P4a-5 → P4a-7 (parser)
- Track B: P4a-6 (maintenance keywords) — very short, run in parallel
- Track C: P4a-8 → P4a-9 → P4a-10..14 (binder) after parser stabilizes

**Can start Phase 4a in parallel with Phase 3 on a separate agent.**

---

## Phase 4b: Scan, Filter, Project

**Gate:** Simple `MATCH (n:Label) RETURN n.name` works end-to-end against persisted data.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P4b-1 | TypedVector: INT64, FLOAT64, BOOL, STRING, VARIANT, NodeRef, EdgeRef | 10 | 10 | 9 | 3 | 9.67 | P3 |
| P4b-2 | VectorGroup + FactorizedChunk structures | 10 | 10 | 10 | 3 | 10.0 | P4b-1 |
| P4b-3 | Primary-label scan operator | 10 | 10 | 9 | 3 | 9.67 | P4b-2, P3 |
| P4b-4 | Secondary-label scan via reverse index + membership bitmaps | 9 | 9 | 9 | 4 | 6.75 | P4b-3 |
| P4b-5 | Property filter operator | 9 | 9 | 7 | 3 | 8.33 | P4b-3 |
| P4b-6 | Projection operator: RETURN, aliases | 9 | 9 | 7 | 2 | 12.5 | P4b-5 |
| P4b-7 | QueryResult materialization (Rust + Python shapes) | 9 | 9 | 8 | 2 | 13.0 | P4b-6 |
| P4b-8 | Memory tracker: byte-level accounting for operators | 7 | 7 | 6 | 3 | 6.67 | P4b-2 |
| P4b-9 | Integration test: MATCH person names (acceptance #3) | 10 | 10 | 10 | 2 | 15.0 | P4b-3..7 |

---

## Phase 4c: Expand, Joins, Aggregates

**Gate:** 2-hop query and ASP-Join multiplicity examples pass (acceptance check #4).

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P4c-1 | Expand operator: CSR neighbor traversal | 10 | 10 | 9 | 4 | 7.25 | P4b, P3 |
| P4c-2 | Delta merge during expand: live edge visibility | 10 | 10 | 10 | 4 | 7.5 | P4c-1 |
| P4c-3 | S-Join implementation | 9 | 9 | 8 | 4 | 6.5 | P4c-1 |
| P4c-4 | Binary ASP-Join: semijoin filter (roaring) build phase | 10 | 10 | 10 | 5 | 6.0 | P4c-1 |
| P4c-5 | Binary ASP-Join: hash state build + re-probe (factorized) | 10 | 10 | 10 | 5 | 6.0 | P4c-4 |
| P4c-6 | Double-unflat gate: multiplicity-first, no Cartesian blowup | 10 | 10 | 10 | 4 | 7.5 | P4c-5 |
| P4c-7 | Aggregate operators: COUNT, SUM, AVG, MIN, MAX | 8 | 8 | 7 | 3 | 7.67 | P4b |
| P4c-8 | Golden operator examples: semijoin elimination + COUNT multiplicity + double-unflat | 9 | 9 | 9 | 3 | 9.0 | P4c-5,6 |
| P4c-9 | Integration test: 2-hop via binary ASP-Join (acceptance #4) | 10 | 10 | 10 | 3 | 10.0 | P4c-4..6 |

**Critical path:** P4c-4 → P4c-5 → P4c-6. These are the highest-risk items in the execution layer. Do not parallelize them — sequential review is mandatory.

---

## Phase 5: Transactions and Maintenance

**Gate:** Acceptance checks #11 (snapshot survives writer commit) and #12 (checkpoint/optimize clean base).

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P5-1 | Snapshot pinning: metapage pin at ReadTx start | 10 | 10 | 10 | 3 | 10.0 | P3, P4 |
| P5-2 | Writer serialization: begin_write blocks on active writer | 9 | 9 | 8 | 2 | 13.0 | P3 |
| P5-3 | CHECKPOINT: fold base+delta → fresh base, reset delta | 10 | 10 | 10 | 5 | 6.0 | P3 |
| P5-4 | CHECKPOINT WAL: CheckpointBegin/End records, metapage publish | 10 | 10 | 10 | 4 | 7.5 | P5-3 |
| P5-5 | OPTIMIZE: checkpoint work + sort neighbor lists by (node_id, edge_id) | 9 | 9 | 9 | 5 | 5.4 | P5-4 |
| P5-6 | OPTIMIZE: rewrite edge-property columns aligned to sort order | 8 | 8 | 8 | 4 | 6.0 | P5-5 |
| P5-7 | Direct maintenance APIs: db.checkpoint() + db.optimize() | 9 | 9 | 9 | 2 | 13.5 | P5-4,5 |
| P5-8 | Cypher CHECKPOINT/OPTIMIZE route to same engine path as direct APIs | 9 | 9 | 9 | 2 | 13.5 | P5-7, P4a-11 |
| P5-9 | Integration test: snapshot reader survives writer commit (acceptance #11) | 10 | 10 | 10 | 2 | 15.0 | P5-1,2 |
| P5-10 | Integration test: checkpoint + optimize clean base (acceptance #12) | 10 | 10 | 10 | 3 | 10.0 | P5-5,6 |

---

## Phase 6: Encryption, Spill, Python, Release Gate

**Gate:** All 14 acceptance checks pass. Compatibility fixture frozen.

### Tickets

| ID | Title | Value | Time Crit | Risk | Size | WSJF | Dependencies |
|----|-------|-------|-----------|------|------|------|--------------|
| P6-1 | XChaCha20-Poly1305 page encryption: physical stride logic | 10 | 10 | 10 | 4 | 7.5 | P1 |
| P6-2 | Encrypted paged file read/write: nonce generation, AAD binding | 10 | 10 | 10 | 4 | 7.5 | P6-1 |
| P6-3 | WAL payload encryption at record level | 9 | 9 | 9 | 4 | 6.75 | P6-1, P3-8 |
| P6-4 | Wrong-key open: distinct EncryptionAuthFailed error | 9 | 9 | 9 | 2 | 13.5 | P6-2 |
| P6-5 | Golden fixture: encrypted-page-v1.bin + round-trip + corruption | 10 | 10 | 10 | 2 | 15.0 | P6-2 |
| P6-6 | Integration test: undirected edge + encryption round-trip (acceptance #10) | 10 | 10 | 10 | 3 | 10.0 | P6-2, P4 |
| P6-7 | Basic spill: sort operator spill-to-disk via OS temp | 7 | 7 | 6 | 4 | 5.0 | P4b |
| P6-8 | Basic spill: ASP-Join hash state spill when over memory limit | 7 | 7 | 6 | 5 | 4.0 | P4c |
| P6-9 | PyO3 bindings: GraphDb, ReadTx, WriteTx, QueryResult, Python result objects | 10 | 10 | 9 | 4 | 7.25 | P5 |
| P6-10 | Python context manager support | 8 | 8 | 7 | 2 | 11.5 | P6-9 |
| P6-11 | Integration test: Python binding core round-trip (acceptance #7) | 10 | 10 | 10 | 2 | 15.0 | P6-9,10 |
| P6-12 | Compatibility fixture freeze: fixtures/compatibility/v1.0/ | 10 | 10 | 10 | 2 | 15.0 | all phases |
| P6-13 | Integration test: format compatibility fixture opens (acceptance #13) | 10 | 10 | 10 | 1 | 30.0 | P6-12 |
| P6-14 | Full acceptance suite run: all 14 checks | 10 | 10 | 10 | 2 | 15.0 | all |

**Parallel tracks in Phase 6:**
- Track A: P6-1 → P6-2 → P6-3 → P6-4 → P6-5 (encryption — start immediately, no Phase 5 dependency)
- Track B: P6-7 → P6-8 (spill — can run parallel with encryption after Phase 4c)
- Track C: P6-9 → P6-10 → P6-11 (Python — after Phase 5)
- Track D: P6-12 → P6-13 (compatibility fixture — after everything)

---

## Full Parallel Agent Strategy

### Agent Team Model

```
Orchestrator (plans, reviews, integrates, runs acceptance gates)
    │
    ├── Agent: common-storage (P0 → P1 → P2-storage → P3)
    │       Owns: sparrowdb-common, sparrowdb-storage
    │
    ├── Agent: catalog-schema (P2-catalog, P2-multi-label)
    │       Owns: sparrowdb-catalog
    │       Starts after P1 finishes
    │
    ├── Agent: parser-binder (P4a — fully parallel with P3)
    │       Owns: sparrowdb-cypher
    │       Can start from P0, no storage dependency until 4b
    │
    ├── Agent: execution (P4b → P4c → P5-maintenance)
    │       Owns: sparrowdb-execution
    │       Starts after P3 + P4a complete
    │
    ├── Agent: encryption (P6-encryption — can start after P1)
    │       Owns: encryption layer in sparrowdb-storage
    │       Start early: encryption touches page stride math
    │
    └── Agent: python-bindings (P6-python)
            Owns: sparrowdb-python
            Starts after P5 complete
```

### Critical Path (cannot parallelize)

```
P0 → P1 → P2 → P3 → P4b → P4c → P5 → P6-acceptance
```

### Parallel Acceleration Points

| Point | What runs in parallel |
|-------|----------------------|
| After P1 | Catalog agent (P2) + Parser agent (P4a) + Encryption start (P6-1,2) |
| After P3 | Execution agent (P4b) unblocked; Parser must be done too |
| After P4c | Transactions agent (P5) + Spill (P6-7,8) + Python starts (P6-9) |

---

## Acceptance Check → Ticket Mapping

| Acceptance Check | Tickets Required |
|-----------------|-----------------|
| #1 create_db_directory | P1-8, P1-12 |
| #2 single_writer_create_two_nodes_one_edge_commit | P2-14, P3-18 |
| #3 single_reader_match_person_names | P4b-9 |
| #4 two_hop_match_via_binary_asp_join | P4c-9 |
| #5 restart_and_reopen_persisted_data | P2-14, P3-18 |
| #6 wal_replay_atomicity_mid_commit_crash | P3-19 |
| #7 python_binding_core_roundtrip | P6-11 |
| #8 superblock_format_version_matches_expected | P1-9 |
| #9 multi_label_node_roundtrip | P2-15 |
| #10 undirected_edge_and_encryption_roundtrip | P6-6 |
| #11 snapshot_reader_survives_writer_commit | P5-9 |
| #12 checkpoint_and_optimize_rewrite_clean_base | P5-10 |
| #13 format_compatibility_fixture_opens | P6-13 |
| #14 node_and_edge_update_delete_survive_restart | P3-19, P4 |

---

## Anti-Patterns to Enforce

1. **No dead modules.** Every crate added must be reachable from a test or the public API before it merges.
2. **No skipping golden fixtures.** A phase cannot be marked done if any mandatory golden fixture is missing.
3. **Integration test before unit-test-only merge.** Unit tests alone do not prove a phase gate.
4. **Crash failpoints before query work.** Phase 4 cannot start without Phase 3 crash failpoints passing.
5. **Cypher maintenance and direct APIs must route to the same implementation.** Test both paths.
6. **Parser must-fail cases are not optional.** They're part of the Phase 4a gate.

---

## Linear Ticket Structure

Each ticket above maps to one Linear issue. Suggested labels:

- `phase:0` through `phase:6`
- `type:feature`, `type:test`, `type:fixture`, `type:integration-test`, `type:ci`
- `critical-path` for any item on the critical path
- `acceptance-gate` for the 14 named checks

Milestones:
- `Phase 0 — Bootstrap`
- `Phase 1 — Durable Codecs`
- `Phase 2 — Catalog + Nodes`
- `Phase 3 — Edges + WAL + Recovery`
- `Phase 4 — Query Engine`
- `Phase 5 — Transactions + Maintenance`
- `Phase 6 — Encryption + Python + Release`
