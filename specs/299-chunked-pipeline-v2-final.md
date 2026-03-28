# Spec: Chunked Vectorized Pipeline — Phases 2, 3, 4 (v2-final)

**Parent issue:** #299
**Child issues:** #338 (Phase 2), #339 (Phase 3), #340 (Phase 4)
**Status:** Final — Phase 1 merged (PR #335, 2026-03-28); ready to split into tickets
**Date:** 2026-03-28
**Inputs:** original #299 spec + Codex contrarian review + Gemini enhanced review

---

## 0. What Phase 1 Already Built (Do Not Redo)

Phase 1 (PR #335) merged to `main` at `f14ed3b`. The following exist in
`crates/sparrowdb-execution/src/`:

| Symbol | File | Purpose |
|--------|------|---------|
| `CHUNK_CAPACITY = 2048` | `chunk.rs` | Fixed batch size |
| `NullBitmap` | `chunk.rs` | Presence bits for a column vector |
| `ColumnVector { data: Vec<u64>, nulls: NullBitmap, col_id: u32 }` | `chunk.rs` | One column in a chunk |
| `DataChunk { columns, len, sel }` | `chunk.rs` | A batch of tuples, columnar |
| `DataChunk::filter_sel(pred)` / `live_rows()` | `chunk.rs` | Selection vector ops |
| `COL_ID_SLOT`, `COL_ID_SRC_SLOT`, `COL_ID_DST_SLOT` | `chunk.rs` | Reserved column IDs |
| `PipelineOperator` trait | `pipeline.rs` | `next_chunk() -> Result<Option<DataChunk>>` |
| `ScanByLabel { slots: Vec<u64>, cursor }` | `pipeline.rs` | Yields slot numbers 0..hwm in chunks |
| `GetNeighbors<C>` | `pipeline.rs` | CSR + delta lookup per src_slot chunk |
| `Filter<C>` | `pipeline.rs` | Applies `FilterPredicate` to selection vector |
| `Engine::can_use_chunked_pipeline()` | `engine/pipeline_exec.rs` | Boolean guard |
| `Engine::execute_scan_chunked()` | `engine/pipeline_exec.rs` | Phase 1 entry point |
| `Engine::with_chunked_pipeline(bool)` | `engine/mod.rs` | Builder flag |

**Integration tests:** `crates/sparrowdb/tests/spa_299_chunked_pipeline.rs` — 23 tests,
all green. Covers scan, WHERE, LIMIT, ORDER BY, 10K nodes, chunk boundaries, multi-prop
AND, 1-hop parity, Int64(0) regression.

**Known Phase 1 gap (fix in Phase 2):** `ScanByLabel::new(hwm)` currently
pre-materializes all of `0..hwm` into a `Vec<u64>` before emitting any chunk.
This is a startup allocation O(hwm) that must be replaced with a cursor
(`next_slot: u64, end_slot: u64`).

**CRITICAL-1 fix (in Phase 1):** Delta-index lookup uses `(self.src_label_id, src_slot)`.
Never hardcode `0u32` as `src_label_id` — that was the bug.

**Phase 1 delivers zero user-visible performance improvement.** All wins start in Phase 2.

---

## 1. Problem Statement

The row-at-a-time engine allocates one `HashMap<String, Value>` per matched row for
WHERE evaluation and per output row for projection. For LDBC IC4 (2-hop + aggregate):

```
22 avg_degree^2 = 484 Vec allocations per source node
× ~3 000 source nodes in SF-01 Person label
= ~1.5 M allocations per query
```

The chunked pipeline eliminates these. Columns are `Vec<u64>` for the query lifetime;
selection vectors replace per-row copies.

**v0.1.12 baselines** (SF-01, single-threaded, 2026-03-27):

| Query | Current (ms) | TuringDB-class (ms) | Gap |
|-------|-------------|---------------------|-----|
| IC3 1-hop + filter | ~42 | ~0.6 | 70× |
| IC4 2-hop + aggregate | ~87 | ~0.4 | 220× |
| IC8 mutual friends | ~160 | ~0.35 | 450× |

**Performance targets (relative, not ms-gated):** Phase 2 ≥ 3× on 1-hop queries;
Phase 3 ≥ 2× on 2-hop; Phase 4 ≥ 5× on mutual-friends. Record hardware and
dataset in every benchmark — do not use raw ms CI gates.

---

## 2. Design Stance (Synthesized from Contrarian Review)

### 2.1 Principles

1. **Reuse before inventing.** ASP-Join, `join_spill.rs`, `sort_spill.rs`, and the
   row-engine expression code already exist. Wrap or feed them rather than replace.
2. **Correctness before vectorization.** NULL semantics and path multiplicity are
   release blockers. No perf result is acceptable if these regress.
3. **Shape selector, not compiler.** Replace the boolean guard with a small
   `ChunkedPlan` selector. Do not build a generic operator-DAG compiler.
4. **Late materialization.** Read only properties needed for filters, RETURN, ORDER BY.
5. **Fallback is a feature.** Unsupported shapes silently fall back to the row engine.
6. **Row engine stays.** Do not retire it in Phases 2–4.

### 2.2 The `ChunkedPlan` Selector (replaces boolean guard in Phase 4)

```rust
pub enum ChunkedPlan {
    Scan(ScanChunkedPlan),
    OneHop(OneHopChunkedPlan),
    TwoHop(TwoHopChunkedPlan),
    MutualNeighbors(MutualNeighborsPlan),
}

pub fn try_plan_chunked_match(&self, m: &MatchStatement) -> Option<ChunkedPlan>;
```

`None` → row engine. This selector replaces `can_use_chunked_pipeline()` by Phase 4.
It is not a full compiler.

---

## 3. Phase 2 — Real 1-Hop Fast Path

### 3.1 Task List (Implementation Order)

1. Fix `ScanByLabel` startup allocation
2. Add `batch_read_node_props_nullable` storage API
3. Add `ReadNodeProps<C>` operator
4. Add `ChunkPredicate` narrow predicate representation
5. Add `execute_one_hop_chunked()`
6. Parity tests + NULL/LIMIT/tombstone coverage
7. 1-hop criterion benchmark

### 3.2 Task 1: Fix `ScanByLabel` Startup Allocation

**Current (wrong):**
```rust
pub struct ScanByLabel {
    slots: Vec<u64>,   // allocates entire 0..hwm upfront
    cursor: usize,
}
```

**Replace with:**
```rust
pub struct ScanByLabel {
    next_slot: u64,
    end_slot: u64,
}
```

Rules:
- `ScanByLabel::new(hwm)` must allocate at most one chunk at a time.
- `ScanByLabel::from_slots(Vec<u64>)` may remain for unit tests and special scans.
- Output semantics unchanged.

### 3.3 Task 2: Nullable Bulk Read Storage API

`NodeStore::batch_read_node_props(...)` collapses missing values to raw `0`. That
is semantically wrong for `Int64(0)`, `Bool(false)`, and `IS NULL` predicates.

Add to `crates/sparrowdb-storage/src/node_store.rs`:

```rust
pub fn batch_read_node_props_nullable(
    &self,
    label_id: u32,
    slots: &[u32],
    col_ids: &[u32],
) -> Result<Vec<Vec<Option<u64>>>>;
```

Requirements:
- Use the null-bitmap sidecar when present.
- Preserve backward compatibility when the sidecar file does not exist.
- Keep the sorted-slot vs full-column heuristic.
- Never treat raw `0` as "missing" when the bitmap says the slot is present.

### 3.4 Task 3: `ReadNodeProps<C>`

```rust
/// Appends property columns to each chunk for the live rows of a slot-bearing chunk.
///
/// Reads only live (selection-vector-passing) slots — no I/O for filtered rows.
pub struct ReadNodeProps<C: PipelineOperator> {
    child: C,
    store: Arc<NodeStore>,
    label_id: u32,
    slot_col_id: u32,   // which column contains the slot numbers
    col_ids: Vec<u32>,
}

impl<C: PipelineOperator> ReadNodeProps<C> {
    pub fn new(
        child: C,
        store: Arc<NodeStore>,
        label_id: u32,
        slot_col_id: u32,
        col_ids: Vec<u32>,
    ) -> Self;
}

impl<C: PipelineOperator> PipelineOperator for ReadNodeProps<C> {
    fn next_chunk(&mut self) -> Result<Option<DataChunk>>;
}
```

Implementation:
- Extract live slots from `chunk.live_rows()` — skip filtered rows.
- Call `store.batch_read_node_props_nullable(label_id, &live_slots, &col_ids)`.
- Build `NullBitmap` from `Option<u64>` results.
- Append one `ColumnVector` per col_id to the chunk.
- `slot_col_id` passes through unchanged.

### 3.5 Task 4: Narrow Predicate Representation

Do NOT introduce a generic `ExpressionEvaluator` framework in Phase 2.
Add a narrow enum for the subset needed to accelerate common 1-hop filters:

```rust
pub enum ChunkPredicate {
    Eq { col_id: u32, rhs_raw: u64 },
    Gt { col_id: u32, rhs_raw: u64 },
    Ge { col_id: u32, rhs_raw: u64 },
    Lt { col_id: u32, rhs_raw: u64 },
    Le { col_id: u32, rhs_raw: u64 },
    IsNull { col_id: u32 },
    IsNotNull { col_id: u32 },
    And(Vec<ChunkPredicate>),
}
```

Rules:
- Only compile simple conjunctive property predicates.
- Unsupported `WHERE` expressions (CONTAINS, function calls, subqueries,
  cross-variable expressions) fall back to the row engine.

### 3.6 Task 5: `execute_one_hop_chunked()`

```rust
pub(crate) fn execute_one_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>;
```

**Eligibility guard** (extend `can_use_chunked_pipeline`):
- Exactly 2 nodes, 1 relationship.
- Both nodes single-label.
- Directed (outgoing or incoming); undirected deferred to Phase 3.
- No `OPTIONAL MATCH`, no `UNION`, no subquery in `WHERE`.
- No aggregate (`COUNT`, `SUM`, etc.).
- No `ORDER BY`.
- `LIMIT` allowed when there is no `DISTINCT` — the pull model short-circuits naturally.
- Planner resolves exactly one relationship table.

**Pipeline shape:**

```
MaterializeRows(limit?)
  <- optional Filter(ChunkPredicate, dst)
  <- ReadNodeProps(dst)
  <- GetNeighbors(rel_type_id, src_label_id)
  <- optional Filter(ChunkPredicate, src)
  <- ReadNodeProps(src)          [only if src props referenced]
  <- ScanByLabel(hwm)
```

**Terminal projection** continues using existing row-engine helpers
(`project_row`, `build_row_vals`, expression evaluation). Do not duplicate
projection semantics — feed chunked output into the existing row materializer.

**Wiring `GetNeighbors`:**
- `csr`: `RelStore::csr_for_rel_type(rel_type_id)`
- `delta_records`: `edge_store.delta_for_rel_type(rel_type_id)`
- `src_label_id`: from `catalog.get_label(src_label)` — never hardcode 0
- `avg_degree_hint`: from `EdgeStore::avg_degree(rel_type_id)` or default 8

### 3.7 What NOT to Do in Phase 2

- No generic `Project` operator (use existing row helpers at the sink).
- No generic `ExpressionEvaluator` framework.
- No edge-property reads (`[r:R]` where `r.weight` referenced).
- No `ORDER BY` or aggregation in chunked path.
- No row-engine code deletion.
- No undirected relationship handling.

### 3.8 Acceptance Criteria

- [ ] `ScanByLabel::new(hwm)` allocates at most one chunk's worth of memory at start.
- [ ] `MATCH (a:L)-[:R]->(b:M) RETURN a.p, b.q` runs via `execute_one_hop_chunked`.
- [ ] Incoming-edge 1-hop works (swap roles, not duplicated logic).
- [ ] NULL semantics preserved: `IS NULL`, absent vs `Int64(0)`.
- [ ] `LIMIT n` short-circuits without reading more chunks than needed.
- [ ] Tombstoned nodes excluded (same as row engine).
- [ ] Row counts and projected values match row engine on parity suite including:
      missing properties, zero-valued properties, tombstoned nodes, LIMIT.
- [ ] Relative speedup ≥ 3× on 1-hop bench vs Phase 1 baseline (record hardware).
- [ ] `cargo clippy --all-targets -D warnings` clean.
- [ ] `cargo fmt --all` clean.
- [ ] No `unsafe` code.

---

## 4. Phase 3 — Two-Hop and Bounded Paths

### 4.1 Non-Negotiable Semantic Rule

For ordinary `MATCH` path expansion, duplicate destination nodes may represent
**distinct valid paths**. Duplicate elimination must happen only when the query
explicitly requires it (`DISTINCT`, `RETURN DISTINCT`, etc.).

**Phase 3 must NOT introduce a default global visited set that deduplicates
ordinary query results.** This would silently change `COUNT(*)` and path
multiplicity.

### 4.2 Task List (Implementation Order)

1. Add `execute_two_hop_chunked()` reusing ASP-Join
2. Late materialization for intermediate nodes
3. Add `FrontierScratch` (allocation reuse, no semantics change)
4. Optional: bounded variable-length path subset
5. Multiplicity-focused parity tests
6. 2-hop criterion benchmark

### 4.3 Task 1: `execute_two_hop_chunked()` via ASP-Join Reuse

The repo already has `AspJoin::two_hop(...)`, `AspJoin::two_hop_factorized(...)`,
and `SpillingHashJoin::two_hop(...)`. Use these before building a new operator.

```rust
pub(crate) fn execute_two_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>;
```

**Eligibility:**
- Exactly 3 nodes, 2 relationships.
- Both hops resolve to the **same relationship table** (heterogeneous deferred).
- Both hops same direction.
- No `OPTIONAL MATCH`, no subquery in `WHERE`.
- No edge-property predicates.

Use the existing 2-hop infrastructure for the traversal. Feed the src-slot chunks
from the chunked pipeline into the ASP-Join machinery where practical.

### 4.4 Task 2: Late Materialization for Intermediate Nodes

For a 2-hop query returning only `c`:
- Do NOT read `b` properties unless `WHERE` references them.
- If `WHERE b.status = 'active'`, read only `b.status` before the second hop.
- Read `c` properties only after the full 2-hop expansion.

This is more important than any arena type. Implement as a property-need
analysis pass before building the operator pipeline.

### 4.5 Task 3: `FrontierScratch` (Allocation Reduction)

Only add this if profiling shows frontier allocation is a measured bottleneck.

```rust
/// Reusable frontier buffers — reduces per-level Vec churn.
/// Does NOT change traversal semantics.
pub struct FrontierScratch {
    current: Vec<u64>,
    next: Vec<u64>,
}

impl FrontierScratch {
    pub fn new(capacity: usize) -> Self;
    /// Swap current ↔ next; clear next.
    pub fn advance(&mut self);
    pub fn current(&self) -> &[u64];
    pub fn next_mut(&mut self) -> &mut Vec<u64>;
}
```

**No `visited` set on this struct.** If a query truly requires reachability
semantics (distinct nodes), add a separate `reachability_mode: bool` flag with
an explicit proof that the query requires it.

### 4.6 Task 4: Variable-Length Path (Optional for Phase 3)

Only if the same-rel 2-hop case is validated first.

Allowed subset:
- One relationship type.
- Bounded range (`*m..n`, both finite).
- No edge-property filters.
- No `OPTIONAL MATCH`.
- Semantics match row engine exactly (no implicit dedup).

If proof is not ready, variable-length stays on the row engine.

### 4.7 What NOT to Do in Phase 3

- No global visited-set dedup for ordinary `MATCH`.
- No generic `GetNeighborsMultiHop` before the ASP-Join reuse path is validated.
- No heterogeneous multi-hop before same-rel 2-hop is proven.
- No new join framework that duplicates ASP-Join.

### 4.8 Acceptance Criteria

- [ ] `execute_two_hop_chunked()` exists and is planner-wired.
- [ ] 2-hop same-rel queries match row-engine for row count and values on LDBC SF-01.
- [ ] Multiplicity tests: `COUNT(*)`, `COUNT(DISTINCT x)`, duplicate-destination cases.
- [ ] Intermediate properties are not read when not referenced by `WHERE` or `RETURN`.
- [ ] If `FrontierScratch` added, allocation profiling confirms improvement.
- [ ] Benchmark reports relative speedup + correctness/parity status together.
- [ ] All existing acceptance tests pass.
- [ ] `cargo clippy --all-targets -D warnings` clean. `cargo fmt --all` clean.

---

## 5. Phase 4 — Shape Selector + Targeted High-Value Operators

### 5.1 Task List (Implementation Order)

1. `ChunkedPlan` selector (replace boolean guard)
2. `SlotIntersect` for mutual-neighbors patterns
3. Narrow `COUNT` aggregate sink
4. `ORDER BY ... LIMIT` via `sort_spill.rs` reuse
5. Optional: memory tracker / `PipelineContext`
6. Parity/trace coverage
7. Benchmark packet

### 5.2 Task 1: `ChunkedPlan` Selector

Replace `can_use_chunked_pipeline()` with:

```rust
pub enum ChunkedPlan {
    Scan(ScanChunkedPlan),
    OneHop(OneHopChunkedPlan),
    TwoHop(TwoHopChunkedPlan),
    MutualNeighbors(MutualNeighborsPlan),
}

impl Engine {
    pub(crate) fn try_plan_chunked_match(
        &self,
        m: &MatchStatement,
    ) -> Option<ChunkedPlan>;
}
```

`None` → row engine fallback. Each variant encodes the shape-specific parameters
(label_ids, rel_type_ids, col_ids, predicates). Not a full compiler.

Each `execute_*_chunked` function emits:
```rust
tracing::debug!(engine = "chunked", plan = %plan_name, "executing via chunked pipeline");
```

### 5.3 Task 2: `SlotIntersect` for Mutual Neighbors

Do NOT implement a generic `HashJoin<Build, Probe>`. Add a targeted operator
for the mutual-neighbors / fork-join slot intersection pattern:

```rust
/// Intersects two slot-column streams on a shared key column.
/// Used for mutual-neighbor patterns: (a)-[:R]->(x)<-[:R]-(b).
pub struct SlotIntersect {
    left_key_col: u32,
    right_key_col: u32,
    spill_threshold: usize,   // reuse join_spill.rs above this
}
```

Build side: slot set (or slot-to-row map). Join key is always a `u64` slot column.
Spill reuses `join_spill.rs` partitioning — do not write new spill infrastructure.

### 5.4 Task 3: Narrow `COUNT` Aggregate Sink

Do NOT implement a full generic `HashAggregate`. Add a narrow sink for the most
common aggregate shapes:

Supported in Phase 4:
- `COUNT(*)` — count all live rows
- `COUNT(var)` — count non-null values of a slot column
- `COUNT(DISTINCT slot_col)` — when planner proves a supported key shape

Not required: `SUM`, `MIN`, `MAX`, `collect()`, arbitrary grouped aggregates.

The aggregate can either be a dedicated `CountSink` operator or delegate to
existing aggregate helpers at the materializer boundary — what matters is
correctness and avoiding full-row materialization.

### 5.5 Task 4: Ordered Results via `sort_spill.rs` Reuse

Two cases:

1. **`LIMIT n` without `ORDER BY`** — already handled by pull-model short-circuit
   (introduced in Phase 2). No new operator needed.

2. **`ORDER BY ... LIMIT k`** — reuse `sort_spill.rs` before inventing a new
   `OrderByLimit` operator. Feed chunked rows into the existing spillable sort.
   A dedicated `TopK` heap can be added later if profiling proves the sort is
   the bottleneck.

### 5.6 Task 5: `PipelineContext` / Memory Tracker (Optional)

If Phase 3 shows queries hitting OOM, add:

```rust
pub struct PipelineContext {
    memory_tracker: Arc<MemoryTracker>,
    deadline: Option<Instant>,
}
```

Every `next_chunk()` checks the tracker. If exceeded, returns
`Err(Error::QueryMemoryExceeded)` rather than panicking.

This is optional for Phase 4 if no OOM incident has occurred.

### 5.7 Row Engine Retention

**The row engine is not retired in Phase 4.** Keep:
- row-engine fallback for all unsupported shapes,
- parity tests against row-engine results for every new chunked plan,
- traceability of which engine ran each query.

Retirement is a separate future decision requiring its own benchmark and
correctness analysis.

### 5.8 What NOT to Do in Phase 4

- No row-engine retirement.
- No generic pipeline compiler.
- No generic `HashJoin<Build, Probe>` — use `SlotIntersect`.
- No wide `HashAggregate` surface — narrow COUNT only.
- No SIMD inner loops (Phase 5+).
- No morsel-driven parallelism (Phase 6+).
- No disabling fallback for architectural cleanliness.

### 5.9 Acceptance Criteria

- [ ] `try_plan_chunked_match()` selects among scan/one-hop/two-hop/mutual-neighbor
      with trace logging.
- [ ] Mutual-neighbor queries run via `SlotIntersect`.
- [ ] `COUNT(*)` runs without flattening to full row maps.
- [ ] `ORDER BY ... LIMIT k` reuses spillable sort.
- [ ] Every chunked plan has parity tests against the row engine.
- [ ] Fallback is enabled and exercised in tests.
- [ ] All 14 acceptance checks pass with fallback enabled.
- [ ] Spill fires for `SlotIntersect` when build side exceeds threshold (stress test).
- [ ] LDBC IC8 bench: record relative speedup vs v0.1.12 baseline.
- [ ] `cargo clippy --all-targets -D warnings` clean. `cargo fmt --all` clean.

---

## 6. Cross-Phase Correctness Requirements

### 6.1 NULL Semantics — Hard Gate

Any chunked property read path must preserve:
- Absent property ≠ stored raw `0`.
- `IS NULL` / `IS NOT NULL` Cypher behavior.
- Backward compatibility for data written before null bitmaps existed.

No performance result is acceptable if this regresses.

### 6.2 Path Multiplicity — Hard Gate

For path queries:
- Duplicate rows may be correct (distinct paths to same node).
- `COUNT(*)` parity must be checked separately from value parity.
- Duplicate elimination only when query explicitly requires it.

### 6.3 Tombstones and Visibility

Chunked plans must preserve tombstone filtering and snapshot visibility exactly
as the row engine does. Any reuse of row-engine helpers at the sink boundary
naturally satisfies this — do not bypass it.

### 6.4 Column ID Conventions

| Constant | Value | Meaning |
|----------|-------|---------|
| `COL_ID_SLOT` | `0` | Node slot number |
| `COL_ID_SRC_SLOT` | `u32::MAX - 1` | Source slot from GetNeighbors |
| `COL_ID_DST_SLOT` | `u32::MAX - 2` | Destination slot from GetNeighbors |

Property col_ids must not collide with reserved values. Add a debug assertion
in `col_id_of` that the hash is not in the reserved range.

### 6.5 NodeId Encoding

```
high 32 bits = label_id (u32)
low  32 bits = slot     (u32)
```

Utility: `node_id_parts(raw: u64) -> (u32, u64)` in `engine/mod.rs`.
Slots in the pipeline are always the low 32 bits. Label_id is a separate field.

### 6.6 Delta Index Key Convention

`DeltaIndex = HashMap<(src_label_id: u32, src_slot: u64), Vec<DeltaRecord>>`

Never hardcode `src_label_id = 0`. That was CRITICAL-1.

### 6.7 Chunk Ownership

`DataChunk` is owned by the operator that creates it. Passed by value, not
reference-counted. Do not `Box` a `DataChunk`.

### 6.8 Benchmark Discipline

Every performance acceptance report must include:
- Query shape and Cypher text
- Dataset and scale factor
- Benchmark command (`cargo bench -- <name>`)
- Hardware description
- Before/after numbers and relative speedup
- Correctness/parity status

Do not use bare ms gates unless CI hardware and dataset are locked and versioned.

---

## 7. Testing Strategy

### Unit Tests (`crates/sparrowdb-execution/tests/`)

Each new operator needs:
- Empty input → `Ok(None)`
- Single chunk, all rows live
- Multiple chunks with overflow
- Null handling: input chunk with null bits set
- Selection vector interaction: pre-filtered input (ReadNodeProps skips I/O)

### Integration Parity Tests (`crates/sparrowdb/tests/`)

For every new `execute_*_chunked` entry point, add
`spa_299_phase{N}_parity.rs` that:
1. Creates a test graph with known structure.
2. Executes the same Cypher via both row engine and chunked engine.
3. Asserts identical row counts AND identical sorted values.
4. Includes: missing properties, zero-valued properties, tombstoned nodes,
   `LIMIT`, multiplicity-sensitive cases.

### Benchmarks (`crates/sparrowdb/benches/`)

Add `pipeline_phase{N}_benchmarks.rs`. Each bench:
- Reports relative speedup with v0.1.12 baseline in a comment.
- CI runs `--bench -- --test` (compile-check only, no wall-clock gate).
- Full wall-clock run is manual with hardware/dataset documented.

---

## 8. Implementation Order Per Phase

For each phase:
1. Storage API additions (if any)
2. New operator struct + unit tests (no planner wiring)
3. `cargo clippy --all-targets -D warnings` + `cargo fmt --all` clean
4. Planner eligibility / selector update
5. `execute_*_chunked` entry point
6. Wire planner to use new entry point
7. Integration parity test (including multiplicity/NULL/tombstone cases)
8. Criterion bench
9. PR — adversarial review before merge

---

## 9. Resolved Design Questions

All 6 open questions from the v1 spec are resolved:

1. **`NodeStore` bulk read API** — Must add `batch_read_node_props_nullable`.
   The existing `batch_read_node_props` collapses missing to `0` which is wrong.

2. **Backward CSR for undirected/mutual-friends** — Reuse ASP-Join which already
   handles bidirectional traversal. Do not build `CsrBackward` separately in Phase 3.
   Phase 4 `SlotIntersect` feeds forward+backward GetNeighbors.

3. **Spill reuse** — `join_spill.rs` and `sort_spill.rs` are the reuse targets.
   Do not write new spill infrastructure.

4. **Chunk capacity tuning** — Benchmark 512/1024/2048/4096 before Phase 4 and
   document the winner. Current 2048 is untuned.

5. **OrderByLimit vs sort** — Reuse `sort_spill.rs` first. A `TopK` heap is an
   optimization to add only if sort is proven to be the bottleneck.

6. **Multi-label node dedup** — `ScanByLabel` emits every slot in the primary
   label range. Secondary label membership is enforced by the catalog. No dedup
   pass needed in Phase 2.
