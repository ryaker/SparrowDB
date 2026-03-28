# Spec: Chunked Vectorized Pipeline — Phases 2, 3, 4

**Parent issue:** #299
**Child issues:** #338 (Phase 2), #339 (Phase 3), #340 (Phase 4)
**Status:** Draft — for LLM review before splitting into per-phase tickets
**Date:** 2026-03-28
**Author:** Based on #299 Gemini spec + Phase 1 implementation (PR #335)

---

## 0. What Phase 1 Already Built (Do Not Redo)

Phase 1 (PR #335, `feat/chunked-exec-299`) landed the following in
`crates/sparrowdb-execution/src/`:

| Symbol | File | Purpose |
|--------|------|---------|
| `CHUNK_CAPACITY = 2048` | `chunk.rs` | Fixed batch size |
| `NullBitmap` | `chunk.rs` | Presence bits for a column vector |
| `ColumnVector { data: Vec<u64>, nulls: NullBitmap, col_id: u32 }` | `chunk.rs` | One column in a chunk |
| `DataChunk { columns, len, sel }` | `chunk.rs` | A batch of tuples, columnar |
| `DataChunk::filter_sel(pred)` | `chunk.rs` | Apply selection vector |
| `COL_ID_SLOT`, `COL_ID_SRC_SLOT`, `COL_ID_DST_SLOT` | `chunk.rs` | Reserved column IDs |
| `PipelineOperator` trait | `pipeline.rs` | `next_chunk() -> Result<Option<DataChunk>>` |
| `ScanByLabel { slots, cursor }` | `pipeline.rs` | Yields slot numbers 0..hwm in chunks |
| `GetNeighbors<C>` | `pipeline.rs` | CSR + delta lookup per src_slot chunk |
| `Filter<C>` | `pipeline.rs` | Applies `FilterPredicate` to selection vector |
| `Engine::can_use_chunked_pipeline()` | `engine/pipeline_exec.rs` | Guard: single-node label scan only |
| `Engine::execute_scan_chunked()` | `engine/pipeline_exec.rs` | Phase 1 entry point (scan, no hops) |
| `Engine::with_chunked_pipeline(bool)` | `engine/mod.rs` | Builder flag |

**Phase 1 delivers zero user-visible performance improvement.** It is
infrastructure. All benchmark wins start in Phase 2.

---

## 1. Problem Statement (Repeated for Completeness)

The row-at-a-time engine allocates one `HashMap<String, Value>` per matched row
for WHERE evaluation, one per output row for projection, and one `Vec<u64>` per
source node for neighbor expansion. For LDBC IC4 (2-hop + aggregate):

```
22 avg_degree^2 = 484 Vec allocations per source node
× ~3 000 source nodes in SF-01 Person label
= ~1.5 M allocations per query
```

The chunked pipeline eliminates these allocations: columns are `Vec<u64>` that
live for the duration of the query, and selection vectors replace per-row
copies.

**v0.1.12 baselines** (SF-01, single-threaded, captured 2026-03-27):

| Query | Current (ms) | TuringDB-class (ms) | Gap |
|-------|-------------|---------------------|-----|
| IC3 1-hop + filter | ~42 | ~0.6 | 70× |
| IC4 2-hop + aggregate | ~87 | ~0.4 | 220× |
| IC8 mutual friends | ~160 | ~0.35 | 450× |

Phase 2 target: close IC3/IC5/IC7 gap to ≤ 10×.
Phase 3 target: close IC4/IC6 gap to ≤ 20×.
Phase 4 target: close IC8 gap to ≤ 5×.

---

## 2. Phase 2 — GetNeighbors + ReadProperties + 1-Hop Pipeline

### 2.1 New Operators

#### `ReadProperties<C: PipelineOperator>`

Bulk-reads requested column IDs for all live slots in an input chunk.

```rust
/// Appends property columns to each chunk from the child operator.
///
/// For each live slot in `input.column(COL_ID_SLOT)`, reads the requested
/// `col_ids` from the NodeStore column files via `batch_read_node_props`.
/// Appends one `ColumnVector` per col_id to the chunk.
pub struct ReadProperties<C: PipelineOperator> {
    child: C,
    store: Arc<NodeStore>,
    label_id: u32,
    col_ids: Vec<u32>,
}

impl<C: PipelineOperator> ReadProperties<C> {
    pub fn new(child: C, store: Arc<NodeStore>, label_id: u32, col_ids: Vec<u32>) -> Self;
}

impl<C: PipelineOperator> PipelineOperator for ReadProperties<C> {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>>;
}
```

**Implementation notes:**
- Call `store.batch_read_node_props(label_id, slots, &col_ids)` where `slots`
  is the live-slot list extracted from `chunk.live_rows()`.
- For slots with no value for a given col_id, set `NullBitmap::clear(i)` on that
  column.
- The slot column (COL_ID_SLOT) passes through unchanged.
- Do NOT call `read_node_props` per-row — that defeats the purpose.

#### `Project<C: PipelineOperator>`

Maps columns from the input chunk to an output schema.

```rust
pub struct Project<C: PipelineOperator> {
    child: C,
    /// (output_col_id, input_col_id) pairs.
    mappings: Vec<(u32, u32)>,
}
```

For Phase 2 this is thin — it selects which columns to include in the output
chunk for RETURN projection. No expression evaluation yet (that's Phase 4).

### 2.2 Planner Changes

Add `execute_one_hop_chunked` to `engine/pipeline_exec.rs`:

```rust
pub(crate) fn execute_one_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>
```

**Eligibility guard** — extend `can_use_chunked_pipeline` to return `true` when
the MatchStatement is a simple 1-hop with:
- Exactly 2 nodes, 1 relationship in the pattern.
- Both nodes have a single label (no multi-label patterns for now).
- Relationship is directed outgoing only (undirected deferred to Phase 3).
- No `WHERE` subquery, no `OPTIONAL MATCH`, no `UNION`.
- `RETURN` columns are node properties only (no `id()`, `type()`, `labels()`
  expressions — defer to Phase 4).

If the guard returns false, fall through to `execute_one_hop` (row-at-a-time).
Never remove the row-at-a-time fallback.

**Pipeline shape for `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name`:**

```
Project(a.name, b.name)
  └── ReadProperties(b: [name_col_id])
        └── Filter(b inline props if any)
              └── GetNeighbors(:KNOWS, src_label_id=person_label_id)
                    └── ReadProperties(a: [name_col_id])
                          └── ScanByLabel(Person, hwm)
```

### 2.3 GetNeighbors Wiring (Phase 1 Struct, Phase 2 Integration)

`GetNeighbors` was built in Phase 1 but only used in unit tests. Phase 2 wires
it into the planner. Key parameters to supply from the planner:

- `csr`: `CsrForward` loaded from `RelStore::csr_for_rel_type(rel_type_id)`
- `delta_records`: `edge_store.delta_for_rel_type(rel_type_id)`
- `src_label_id`: from `catalog.get_label(src_label)`
- `avg_degree_hint`: from `EdgeStore::avg_degree(rel_type_id)` or default 8

### 2.4 What NOT to Do in Phase 2

- **Do not** handle edge properties (`MATCH (a)-[r:R]->(b) RETURN r.weight`).
  The `r` variable with properties requires reading the edge property store,
  which is a separate code path. Defer to Phase 4.
- **Do not** handle `ORDER BY` or `LIMIT` in the chunked path (defer Phase 4
  `OrderByLimit` operator).
- **Do not** handle `RETURN COUNT(*)` aggregation (defer Phase 4 `HashAggregate`).
- **Do not** delete or modify any row-at-a-time code paths.
- **Do not** handle undirected relationships `(a)-[:R]-(b)` — defer to Phase 3.

### 2.5 Acceptance Criteria

- [ ] `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name` executes
      via `execute_one_hop_chunked` (add a tracing log line).
- [ ] Row count matches `execute_one_hop` output on LDBC SF-01 Person/KNOWS.
- [ ] Criterion bench `match_create_edges_indexed` shows ≥ 2× improvement over
      Phase 1 baseline (or document why not).
- [ ] New LDBC IC3 bench added; time < 20 ms on CI hardware (vs current ~42 ms).
- [ ] All existing acceptance tests pass (no regression).
- [ ] `cargo clippy --all-targets -D warnings` clean.
- [ ] No `unsafe` code introduced.

---

## 3. Phase 3 — Multi-Hop + Flat BFS Arena

### 3.1 Motivation

Phase 2's `GetNeighbors` is a single-hop operator: it expands src → dst once.
For 2-hop (`MATCH (a)-[:R]->(b)-[:R]->(c)`) and variable-length paths
(`MATCH (a)-[:R*2..4]->(b)`), the current engine allocates a fresh
`Vec<NodeId>` frontier per BFS level. Phase 3 replaces this with a flat
pre-allocated arena.

### 3.2 New: `BfsArena`

```rust
/// Flat-vector BFS frontier. Pre-allocates a single buffer for up to
/// `max_frontier` slots per level; never heap-allocates during traversal.
///
/// Lives outside the operator tree — passed into `GetNeighborsMultiHop`
/// as a reusable scratch buffer.
pub struct BfsArena {
    /// Current level's frontier (src_slots).
    current: Vec<u64>,
    /// Next level's frontier (dst_slots produced by this hop).
    next: Vec<u64>,
    /// Visited set (slot-only, deduplicated across hops).
    visited: roaring::RoaringBitmap,
    max_frontier: usize,
}

impl BfsArena {
    pub fn new(max_frontier: usize) -> Self;
    /// Swap current ↔ next; clear next. Call between BFS levels.
    pub fn advance(&mut self);
    pub fn current(&self) -> &[u64];
    pub fn next_mut(&mut self) -> &mut Vec<u64>;
    pub fn mark_visited(&mut self, slot: u64) -> bool; // returns false if already visited
}
```

`max_frontier` default: 64 × `CHUNK_CAPACITY` = 131 072 slots. Configurable
via `Engine::with_bfs_arena_capacity(usize)`.

### 3.3 New: `GetNeighborsMultiHop<C>`

```rust
/// Multi-hop variant of GetNeighbors.
///
/// Wraps a child that yields src-slot chunks. For each hop from `min_hops`
/// to `max_hops`, uses `BfsArena` to expand the frontier without allocating.
pub struct GetNeighborsMultiHop<C: PipelineOperator> {
    child: C,
    csr: CsrForward,
    delta_index: DeltaIndex,
    src_label_id: u32,
    min_hops: u32,
    max_hops: u32,
    arena: BfsArena,
    // buffered output pairs (src_slot, dst_slot) pending emission
    buf_src: Vec<u64>,
    buf_dst: Vec<u64>,
    buf_cursor: usize,
    child_done: bool,
}
```

### 3.4 Planner Changes

Add `execute_two_hop_chunked` and `execute_n_hop_chunked` in `pipeline_exec.rs`.

**Eligibility for `execute_two_hop_chunked`:**
- Exactly 3 nodes, 2 relationships.
- All relationships have the same type (heterogeneous multi-hop deferred).
- All relationships directed outgoing.
- No `WHERE` subquery, no `OPTIONAL MATCH`.

**Lazy property materialization:** For multi-hop, do NOT read intermediate node
properties until after all hops complete. Only read properties for the final
destination nodes (the `c` variable in a 2-hop). This avoids reading properties
for nodes that are filtered out by hop constraints.

### 3.5 `RoaringBitmap` for Visited Set

The existing workspace already depends on `roaring = "0.10"` (see
`Cargo.toml`). Use `RoaringBitmap` for the visited set in `BfsArena` — it is
significantly faster than `HashSet<u64>` for slot numbers (dense u32-range
integers).

The slot portion of a NodeId fits in u32 (low 32 bits). Cast `slot as u32` for
the bitmap. If slot values ever exceed u32::MAX the cast is lossy — add a debug
assertion.

### 3.6 What NOT to Do in Phase 3

- **Do not** implement `MATCH (a)-[:R]-(b)` undirected without testing both
  CSR forward and CSR backward directions. Both must be checked to avoid
  duplicate pairs.
- **Do not** use `HashSet<NodeId>` for the visited set — use `BfsArena` with
  `RoaringBitmap`.
- **Do not** read properties for intermediate hop nodes — only the final layer.
- **Do not** attempt heterogeneous multi-hop (`-[:R1]->()-[:R2]->()`) — that
  requires two separate CSR/delta lookups. Defer to Phase 4.

### 3.7 Acceptance Criteria

- [ ] `MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name`
      uses `execute_two_hop_chunked`.
- [ ] Row count matches `execute_two_hop` for LDBC SF-01.
- [ ] LDBC IC4 bench added; time < 30 ms (vs current ~87 ms).
- [ ] Variable-length `MATCH (a)-[:R*2..3]->(b)` uses `execute_n_hop_chunked`.
- [ ] Visited deduplication: no duplicate `(src, dst)` pairs in output.
- [ ] `BfsArena::advance()` called correctly between levels (no stale frontier).
- [ ] All existing acceptance tests pass.

---

## 4. Phase 4 — HashJoin + Full Pipeline Compiler

### 4.1 Motivation

Mutual friends (`MATCH (a)-[:R]->(x)<-[:R]-(b)`) and triangle patterns require
joining two neighbor sets. The current engine materializes both sets into
`HashSet<u64>` and intersects. Phase 4 introduces a proper `HashJoin` operator
and a pipeline compiler that replaces the hand-written `execute_*` dispatch.

### 4.2 New: `HashJoin<Build, Probe>`

```rust
/// Hash join on a join key column.
///
/// Build side: fully materialized into a HashMap<u64, Vec<DataChunk>> keyed
/// by the join column value.
/// Probe side: streamed chunk-by-chunk; for each live row, looks up matching
/// build rows and emits joined tuples.
pub struct HashJoin<Build: PipelineOperator, Probe: PipelineOperator> {
    build: Build,
    probe: Probe,
    build_key_col: u32,   // col_id to join on (build side)
    probe_key_col: u32,   // col_id to join on (probe side)
    // Internal state
    hash_table: HashMap<u64, Vec<Vec<u64>>>,  // key → list of build-side rows
    table_built: bool,
    pending_output: VecDeque<DataChunk>,
}
```

For mutual friends (`MATCH (a)-[:R]->(x)<-[:R]-(b)`):
- Build side: `GetNeighbors(a, :R, OUT)` → set of `x` slots reachable from `a`
- Probe side: `GetNeighbors(b, :R, IN)` → set of `x` slots reachable from `b`
  (backward CSR)
- Join key: `COL_ID_DST_SLOT` on build, `COL_ID_DST_SLOT` on probe

**Spill to disk**: If the build side exceeds `MAX_HASH_JOIN_MEMORY` (default
256 MiB), spill overflow buckets to a temp file and perform a hybrid
hash join. This mirrors the ASP-Join spill-to-disk already in the engine
(see SPA-101 / PR #278). Reuse `sparrowdb_storage::spill` infrastructure.

### 4.3 New: Pipeline Compiler

Replace the `match` dispatch in `Engine::execute()` that routes to
`execute_scan`, `execute_one_hop`, `execute_two_hop`, etc. with a compiler
that:

1. Reads the `MatchStatement` AST.
2. Builds an operator DAG (a tree of `Box<dyn PipelineOperator>`).
3. Drives the DAG to completion, collecting rows.

**Operator DAG construction rules:**

| Pattern | Root operator chain |
|---------|-------------------|
| `MATCH (n:L) RETURN n.p` | `Project → ReadProperties → ScanByLabel` |
| `MATCH (n:L) WHERE n.p > X RETURN n.p` | `Project → Filter → ReadProperties → ScanByLabel` |
| `MATCH (a:L1)-[:R]->(b:L2) RETURN b.p` | `Project → ReadProperties(b) → GetNeighbors → ScanByLabel(L1)` |
| `MATCH (a)-[:R]->(b)-[:R]->(c) RETURN c.p` | `Project → ReadProperties(c) → GetNeighborsMultiHop → ScanByLabel` |
| `MATCH ... RETURN COUNT(*)` | `HashAggregate(COUNT) → ...` |
| `MATCH ... RETURN ... ORDER BY x LIMIT n` | `OrderByLimit(n) → ...` |

### 4.4 New: `HashAggregate<C>`

```rust
pub struct HashAggregate<C: PipelineOperator> {
    child: C,
    group_by_cols: Vec<u32>,
    aggregates: Vec<AggregateExpr>,
    /// In-progress hash table: group key → accumulator state.
    table: HashMap<Vec<u64>, Vec<AccState>>,
    /// Whether we've fully consumed the child and are now emitting results.
    emitting: bool,
    emit_cursor: usize,
    emit_keys: Vec<Vec<u64>>,
}

pub enum AggregateExpr {
    Count,
    CountDistinct(u32),  // col_id
    Sum(u32),
    Min(u32),
    Max(u32),
    Collect(u32),        // LIST aggregation
}
```

### 4.5 New: `OrderByLimit<C>`

```rust
/// Top-K heap for ORDER BY … LIMIT n.
///
/// Maintains a min-heap of at most `limit` rows sorted by `sort_cols`.
/// When the child is exhausted, emits the heap contents in sort order.
/// Short-circuits: stops pulling from child once `limit` rows are confirmed.
pub struct OrderByLimit<C: PipelineOperator> {
    child: C,
    sort_cols: Vec<(u32, SortDir)>,
    limit: usize,
    heap: BinaryHeap<SortRow>,
}
```

### 4.6 Row Engine Retirement

After Phase 4, the following functions in `lib.rs` (or its submodules, post
refactor) become dead code and should be removed in a follow-up PR:
`execute_scan`, `execute_one_hop`, `execute_two_hop`, `execute_n_hop`,
`execute_match_mutate`, `execute_full_scan`.

**Do not remove them in the Phase 4 PR itself.** Remove only after all
acceptance tests pass on the new compiler exclusively, with the row engine
disabled via a feature flag `row_engine_fallback = false` for one release cycle.

### 4.7 What NOT to Do in Phase 4

- **Do not** remove row-at-a-time code until a full acceptance run confirms
  parity (use a feature flag for one release cycle).
- **Do not** implement SIMD inner loops — that is Phase 5 (not specced here).
- **Do not** implement morsel-driven parallelism — Phase 6.
- **Do not** spill `HashAggregate` to disk in Phase 4 — bound group count by
  returning an error if the table exceeds 256 MiB.

### 4.8 Acceptance Criteria

- [ ] `MATCH (a:Person)-[:KNOWS]->(x:Person)<-[:KNOWS]-(b:Person) RETURN b.name`
      (LDBC IC8 mutual friends) uses `HashJoin`.
- [ ] LDBC IC8 bench < 20 ms (vs current ~160 ms).
- [ ] `RETURN COUNT(*)` uses `HashAggregate` in chunked path.
- [ ] `RETURN ... ORDER BY x.p LIMIT 10` uses `OrderByLimit`.
- [ ] All 14 acceptance checks pass with row engine fallback disabled.
- [ ] Spill-to-disk fires for `HashJoin` when build side > 256 MiB (stress test).
- [ ] All existing golden fixtures round-trip correctly.

---

## 5. Cross-Phase Concerns

### 5.1 Column ID Conventions

All operators use `u32` col_ids derived from `sparrowdb_common::col_id_of(name)`.
The reserved IDs used as pipeline metadata columns:

| Constant | Value | Meaning |
|----------|-------|---------|
| `COL_ID_SLOT` | `0` | Node slot number (output of ScanByLabel) |
| `COL_ID_SRC_SLOT` | `u32::MAX - 1` | Source slot (output of GetNeighbors) |
| `COL_ID_DST_SLOT` | `u32::MAX - 2` | Destination slot (output of GetNeighbors) |

Property col_ids must not collide with these reserved values. Add a
debug assertion in `col_id_of` that the returned hash is not in the reserved
range.

### 5.2 NodeId Encoding

The raw `u64` NodeId encoding used throughout the pipeline:

```
high 32 bits = label_id (u32)
low  32 bits = slot     (u64 truncated to u32)
```

Utility: `node_id_parts(raw: u64) -> (u32, u64)` in `engine/mod.rs`.

When passing slots through the pipeline, slots are always the low 32 bits
(plain slot index within the label's column files). The label_id is tracked
separately (e.g., as a field in `ReadProperties` or `GetNeighbors`).

### 5.3 Delta Index Key Convention

`DeltaIndex = HashMap<(src_label_id: u32, src_slot: u64), Vec<DeltaRecord>>`

Built once per query via `build_delta_index(delta_records)`. The `src_label_id`
is the label ID of the node that the edge originates from — **not** hardcoded
to 0. This was the CRITICAL-1 bug fixed in Phase 1 PR #335.

### 5.4 Null Handling

`ColumnVector::nulls: NullBitmap` tracks which slots have a value for a given
property. A slot with no value for col_id X has `nulls.is_null(i) == true`.

- `Filter` must skip null slots for the filtered column (null does not satisfy
  any comparison predicate — consistent with Cypher semantics).
- `ReadProperties` must set null bits for missing properties.
- `HashAggregate` COUNT excludes null values; COUNT(*) counts all live rows.

### 5.5 Chunk Ownership and Lifetime

`DataChunk` is owned by the operator that creates it. When an operator appends
columns (e.g., `ReadProperties`), it takes the chunk by value, appends columns,
and returns a new owned chunk. No sharing or reference-counting.

Chunks are stack-allocated by value through the operator tree. Do not `Box`
a `DataChunk`.

### 5.6 Error Handling

All `PipelineOperator::next_chunk()` calls return `Result<Option<DataChunk>>`.
On `Err`, propagation is immediate — no partial results. The caller
(`execute_*_chunked`) converts the error to `sparrowdb_common::Error`.

Storage-layer errors (`NodeStore`, `CsrForward`) use the existing
`sparrowdb_common::Error` type. Do not introduce new error types.

### 5.7 Tracing

Each `execute_*_chunked` function must emit a `tracing::debug!` line at entry:

```rust
tracing::debug!(
    pattern = %pattern_description,
    engine = "chunked",
    "executing via chunked pipeline"
);
```

This allows confirming in logs that the chunked path is active.

---

## 6. Testing Strategy

### Unit Tests (in `crates/sparrowdb-execution/tests/`)

Each new operator needs:
- Empty input → `Ok(None)`
- Single chunk, all rows live
- Multiple chunks with overflow
- Null handling: input chunk with null bits set
- Selection vector interaction: pre-filtered input

### Integration Parity Tests (in `crates/sparrowdb/tests/`)

For every new `execute_*_chunked` entry point, add a parity test file
`spa_299_phase{N}_parity.rs` that:

1. Creates a test graph with known structure.
2. Executes the same Cypher via both row engine and chunked engine.
3. Asserts identical row counts and identical sorted values.

### Benchmark Tests (in `crates/sparrowdb/benches/`)

Add per-phase criterion bench file `pipeline_phase{N}_benchmarks.rs`. Each
bench must:
- Report time, not just correctness.
- Include a comment with the v0.1.12 baseline so regressions are detectable.
- Run `--bench pipeline_phase2_benchmarks -- --test` in CI (compile-check only,
  no wall-clock assertion).

---

## 7. Implementation Order Within Each Phase

For each phase, the recommended implementation order:

1. New operator struct + unit tests (no planner wiring yet)
2. `cargo clippy` + `cargo fmt` clean
3. Planner eligibility guard
4. `execute_*_chunked` entry point
5. Wire planner to use new entry point
6. Integration parity test
7. Criterion bench
8. PR — adversarial review before merge

---

## 8. Open Questions for Reviewer LLMs

1. **`ReadProperties` bulk read API**: Does `NodeStore::batch_read_node_props`
   accept a slice of arbitrary slots, or does it require contiguous ranges?
   If the latter, the operator needs to sort slots before batching. Confirm
   the current signature in `crates/sparrowdb-storage/src/node_store.rs`.

2. **Backward CSR**: Phase 3 undirected and Phase 4 mutual friends require
   traversing edges in reverse. Does `CsrForward` expose a backward index,
   or does a `CsrBackward` need to be built? Check
   `crates/sparrowdb-storage/src/csr.rs`.

3. **`HashJoin` spill**: The existing ASP-Join spill infrastructure (PR #278,
   SPA-101) is in `engine/join_spill.rs` or similar. Phase 4 should reuse it.
   Confirm the spill API is accessible from `pipeline.rs`.

4. **Chunk capacity tuning**: 2048 is a guess. Before Phase 4, run benchmarks
   with 512 / 1024 / 2048 / 4096 and document the winner.

5. **`OrderByLimit` vs sort-then-limit**: For large result sets, a bounded heap
   (top-K) is better than full sort. For `ORDER BY` without `LIMIT`, a full
   sort is required. The planner should detect whether `LIMIT` is present and
   choose accordingly.

6. **Multi-label nodes** (#289, merged): `ScanByLabel` scans the primary label
   slot range. Nodes with secondary labels appear in the primary scan. Phase 2
   must not double-emit such nodes. Confirm that secondary label membership is
   enforced by the catalog, not by re-scanning secondary label column files.
