# Spec: Chunked Vectorized Pipeline — Phases 2, 3, 4 (Final)

**Parent issue:** #299
**Child issues:** #338 (Phase 2), #339 (Phase 3), #340 (Phase 4)
**Status:** Implementation Ready — Phase 1 merged PR #335 (2026-03-28)
**Date:** 2026-03-28
**Inputs:** Original #299 spec · Codex contrarian review · Gemini enhanced · Gemini v3

---

## 0. Phase 1 Baseline (Merged, Do Not Redo)

Phase 1 (PR #335) landed in `crates/sparrowdb-execution/src/`:

| Symbol | File | Purpose |
|--------|------|---------|
| `CHUNK_CAPACITY = 2048` | `chunk.rs` | Fixed batch size (untuned — benchmark in Phase 4) |
| `NullBitmap` | `chunk.rs` | Presence bits per column vector |
| `ColumnVector { data: Vec<u64>, nulls: NullBitmap, col_id: u32 }` | `chunk.rs` | One column |
| `DataChunk { columns, len, sel }` | `chunk.rs` | A batch, columnar |
| `DataChunk::filter_sel(pred)` / `live_rows()` | `chunk.rs` | Selection vector ops |
| `COL_ID_SLOT` / `COL_ID_SRC_SLOT` / `COL_ID_DST_SLOT` | `chunk.rs` | Reserved col IDs |
| `PipelineOperator` trait | `pipeline.rs` | `next_chunk() -> Result<Option<DataChunk>>` |
| `ScanByLabel { slots: Vec<u64>, cursor }` | `pipeline.rs` | Slot scan (startup alloc bug — fix in Phase 2) |
| `GetNeighbors<C>` | `pipeline.rs` | CSR + delta lookup; `src_label_id` param (CRITICAL-1 fix) |
| `Filter<C>` | `pipeline.rs` | Applies `FilterPredicate` to selection vector |
| `Engine::can_use_chunked_pipeline()` | `engine/pipeline_exec.rs` | Boolean guard (replace in Phase 4) |
| `Engine::execute_scan_chunked()` | `engine/pipeline_exec.rs` | Phase 1 entry point |
| `Engine::with_chunked_pipeline(bool)` | `engine/mod.rs` | Builder flag |

**Integration tests:** `crates/sparrowdb/tests/spa_299_chunked_pipeline.rs` — 23 tests, all green.

**Phase 1 delivers zero user-visible performance improvement.** All benchmark wins start in Phase 2.

---

## 1. Problem Statement

The row-at-a-time engine allocates one `HashMap<String, Value>` per matched row for
WHERE evaluation and per output row for projection. For LDBC IC4 (2-hop + aggregate):

```
22 avg_degree^2 = 484 Vec allocations per source node
× ~3 000 source nodes in SF-01 Person label
= ~1.5 M allocations per query
```

**v0.1.12 baselines** (SF-01, single-threaded, 2026-03-27):

| Query | Current (ms) | TuringDB-class (ms) | Gap |
|-------|-------------|---------------------|-----|
| IC3 1-hop + filter | ~42 | ~0.6 | 70× |
| IC4 2-hop + aggregate | ~87 | ~0.4 | 220× |
| IC8 mutual friends | ~160 | ~0.35 | 450× |

**Performance targets (relative, not ms-gated):** ≥ 3× on 1-hop (Phase 2),
≥ 2× on 2-hop (Phase 3), ≥ 5× on mutual-friends (Phase 4).
Record hardware and dataset in every benchmark.

---

## 2. Design Stance (Synthesized)

### 2.1 Principles

1. **Reuse before inventing.** ASP-Join, `join_spill.rs`, `sort_spill.rs`, and
   row-engine expression code exist. Wrap or feed them rather than replace.
2. **Correctness before vectorization.** NULL semantics and path multiplicity
   are release blockers.
3. **Shape selector, not compiler.** Replace the boolean guard with a
   `ChunkedPlan` selector. Not a full operator-DAG compiler.
4. **Late materialization.** Read only properties needed for filters, RETURN,
   ORDER BY.
5. **Fallback is a feature.** Unsupported shapes silently use the row engine.
6. **Row engine stays.** Do not retire it in Phases 2–4.

### 2.2 What This Spec Explicitly Rejects

- A generic `ExpressionEvaluator` trait in Phase 2 — use `ChunkPredicate` (narrow enum).
- A generic `HashJoin<Build, Probe>` — use targeted `SlotIntersect`.
- A full pipeline compiler replacing `Engine::execute()`.
- Row engine retirement in these phases.
- SIMD inner loops (Phase 5+), morsel-driven parallelism (Phase 6+).
- Default global visited-set dedup in ordinary `MATCH` (path multiplicity risk).

### 2.3 The `ChunkedPlan` Selector (Phase 4 — replaces boolean guard)

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

`None` → row engine. Not a full compiler — a shape selector.

### 2.4 `EngineBuilder` Configurability (add in Phase 2)

```rust
impl EngineBuilder {
    pub fn with_chunk_capacity(self, n: usize) -> Self;
    pub fn with_chunked_pipeline(self, enabled: bool) -> Self;
    pub fn with_memory_limit(self, bytes: usize) -> Self;  // Phase 3 uses this
}
```

`with_chunk_capacity` and `with_memory_limit` should be no-ops until the relevant
operators use them — wire up as each operator is built.

---

## 3. Phase 2 — Real 1-Hop Fast Path

### 3.1 Task List (Implementation Order)

1. Fix `ScanByLabel` startup allocation
2. Add `batch_read_node_props_nullable` storage API
3. Add `ReadNodeProps<C>` operator
4. Add `ChunkPredicate` narrow predicate representation
5. Add `execute_one_hop_chunked()`
6. Parity tests (NULL / LIMIT / tombstone / zero-valued properties)
7. 1-hop criterion benchmark

### 3.2 Task 1: Fix `ScanByLabel` Startup Allocation

**Current Phase 1 (wrong long-term):**
```rust
pub struct ScanByLabel {
    slots: Vec<u64>,   // allocates entire 0..hwm before first chunk
    cursor: usize,
}
```

**Replace with cursor-based:**
```rust
pub struct ScanByLabel {
    next_slot: u64,
    end_slot: u64,
}
```

- `ScanByLabel::new(hwm)` allocates at most one chunk at a time.
- `ScanByLabel::from_slots(Vec<u64>)` may remain for unit tests and special scans.
- Output semantics unchanged.

### 3.3 Task 2: Nullable Bulk Read Storage API

`NodeStore::batch_read_node_props(...)` collapses missing values to raw `0`. This
is wrong for `Int64(0)`, `Bool(false)`, and `IS NULL` predicates (PR #333 regression
class). Add to `crates/sparrowdb-storage/src/node_store.rs`:

```rust
pub fn batch_read_node_props_nullable(
    &self,
    label_id: u32,
    slots: &[u32],
    col_ids: &[u32],
) -> Result<Vec<Vec<Option<u64>>>>;
```

Requirements:
- Use null-bitmap sidecar when present.
- Backward compatible when sidecar file does not exist.
- Keep sorted-slot vs full-column heuristic.
- Never treat raw `0` as "missing" when bitmap says slot is present.

### 3.4 Task 3: `ReadNodeProps<C>`

```rust
/// Appends property columns to a chunk for live (selection-vector-passing) rows only.
/// Skips I/O for rows already filtered out.
pub struct ReadNodeProps<C: PipelineOperator> {
    child: C,
    store: Arc<NodeStore>,
    label_id: u32,
    slot_col_id: u32,   // which column contains slot numbers
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
- Extract live slots from `chunk.live_rows()` — zero I/O for filtered rows.
- Call `store.batch_read_node_props_nullable(label_id, &live_slots, &col_ids)`.
- Build `NullBitmap` from `Option<u64>` results.
- Append one `ColumnVector` per col_id. `slot_col_id` passes through unchanged.

### 3.5 Task 4: Narrow Predicate Representation

Do NOT build a generic `ExpressionEvaluator` trait in Phase 2. Add a narrow
enum for simple conjunctive property predicates:

```rust
pub enum ChunkPredicate {
    Eq     { col_id: u32, rhs_raw: u64 },
    Ne     { col_id: u32, rhs_raw: u64 },
    Gt     { col_id: u32, rhs_raw: u64 },
    Ge     { col_id: u32, rhs_raw: u64 },
    Lt     { col_id: u32, rhs_raw: u64 },
    Le     { col_id: u32, rhs_raw: u64 },
    IsNull    { col_id: u32 },
    IsNotNull { col_id: u32 },
    And(Vec<ChunkPredicate>),
}
```

- Compile only simple conjunctive property predicates into `ChunkPredicate`.
- Unsupported `WHERE` (CONTAINS, function calls, subqueries, cross-variable)
  fall back to the row engine.

### 3.6 Task 5: `execute_one_hop_chunked()`

```rust
pub(crate) fn execute_one_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>;
```

**Eligibility guard:**
- Exactly 2 nodes, 1 relationship.
- Both nodes single-label.
- Directed (outgoing or incoming); undirected deferred to Phase 3.
- No `OPTIONAL MATCH`, no `UNION`, no subquery in `WHERE`.
- No aggregate, no `ORDER BY`.
- `LIMIT` allowed when no `DISTINCT` — pull model short-circuits naturally.
- Planner resolves exactly one relationship table.

**Pipeline shape:**
```
MaterializeRows(limit?)
  <- optional Filter(ChunkPredicate, dst)
  <- ReadNodeProps(dst)                      [only if dst props referenced]
  <- GetNeighbors(rel_type_id, src_label_id)
  <- optional Filter(ChunkPredicate, src)
  <- ReadNodeProps(src)                      [only if src props referenced]
  <- ScanByLabel(hwm)
```

**Terminal projection** uses existing row-engine helpers (`project_row`,
`build_row_vals`). Do not duplicate projection semantics — feed chunked output
into the existing row materializer at the sink.

**`GetNeighbors` wiring:**
- `src_label_id`: from `catalog.get_label(src_label)` — never hardcode 0
- `csr`: `RelStore::csr_for_rel_type(rel_type_id)`
- `delta_records`: `edge_store.delta_for_rel_type(rel_type_id)`
- `avg_degree_hint`: `EdgeStore::avg_degree(rel_type_id)` or default 8

### 3.7 What NOT to Do in Phase 2

- No generic `ExpressionEvaluator` framework.
- No generic `Project` operator (use existing row helpers at sink).
- No edge-property reads (`[r:R]` where `r.weight` referenced).
- No `ORDER BY` or aggregation in chunked path.
- No row-engine code deletion.
- No undirected relationship handling.

### 3.8 Acceptance Criteria

- [ ] `ScanByLabel::new(hwm)` allocates at most one chunk at startup.
- [ ] `MATCH (a:L)-[:R]->(b:M) RETURN a.p, b.q` runs via `execute_one_hop_chunked`.
- [ ] Incoming-edge 1-hop works (role swap, not duplicated logic).
- [ ] NULL semantics preserved: `IS NULL`, absent ≠ `Int64(0)`.
- [ ] `LIMIT n` short-circuits — no excess chunks pulled.
- [ ] Tombstoned nodes excluded (same as row engine).
- [ ] Parity suite passes: missing properties, zero-valued, tombstoned, LIMIT.
- [ ] Relative speedup ≥ 3× vs Phase 1 baseline (record hardware + dataset).
- [ ] `cargo clippy --all-targets -D warnings` clean.
- [ ] `cargo fmt --all` clean.
- [ ] No `unsafe`.

---

## 4. Phase 3 — Two-Hop and Bounded Paths

### 4.1 Non-Negotiable Semantic Rules

**Path multiplicity:** For ordinary `MATCH` path expansion, duplicate destination
nodes may represent distinct valid paths. Duplicate elimination must happen only
when explicitly required by the query (`DISTINCT`, set semantics).

**Do NOT introduce a default global visited set that deduplicates ordinary
query results.** This would silently corrupt `COUNT(*)` and path multiplicity.

**NULL and tombstones:** All Phase 3 operators must preserve the same null and
tombstone semantics as Phase 2.

### 4.2 Task List (Implementation Order)

1. `execute_two_hop_chunked()` reusing ASP-Join
2. Late materialization for intermediate nodes
3. `FrontierScratch` (allocation reuse buffer, no visited semantics)
4. Memory limit integration (using `EngineBuilder::with_memory_limit`)
5. Optional: bounded variable-length path subset
6. Multiplicity-focused parity tests
7. 2-hop criterion benchmark

### 4.3 Task 1: `execute_two_hop_chunked()` via ASP-Join Reuse

The repo already has `AspJoin::two_hop(...)`, `AspJoin::two_hop_factorized(...)`,
and `SpillingHashJoin::two_hop(...)`. Use these before building a new multi-hop
operator.

```rust
pub(crate) fn execute_two_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>;
```

**Eligibility:**
- Exactly 3 nodes, 2 relationships.
- Both hops resolve to the **same relationship table**.
- Both hops same direction.
- No `OPTIONAL MATCH`, no subquery in `WHERE`.
- No edge-property predicates.

Generalized heterogeneous multi-hop stays on the row path until same-rel
2-hop is validated.

### 4.4 Task 2: Late Materialization for Intermediate Nodes

Property-need analysis pass before building the operator pipeline:
- Do NOT read intermediate node properties unless `WHERE` references them.
- If `WHERE b.status = 'active'`, read only `b.status` before the second hop.
- If the query returns only `c`, do not read `b` properties at all.
- Read `c` properties only after the full 2-hop expansion.

This reduces I/O more than any arena optimization.

### 4.5 Task 3: `FrontierScratch`

Add only if profiling shows frontier allocation is a measured bottleneck.

```rust
/// Reusable frontier buffers — reduces per-level Vec churn.
/// Does NOT change traversal semantics. No visited set.
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

**No `visited` field on this struct.** If a future query shape truly requires
reachability dedup, add a separate `reachability_mode: bool` flag with an explicit
semantic proof. Do not attach it by default.

### 4.6 Task 4: Memory Limit Integration

Wire `EngineBuilder::with_memory_limit` into Phase 3 expansion. If the frontier
exceeds the configured limit during BFS expansion:

```rust
if frontier_bytes > self.memory_limit {
    return Err(Error::QueryMemoryExceeded);
}
```

This prevents unbounded fan-out from crashing the process.

### 4.7 Task 5: Variable-Length Path (Optional for Phase 3)

Only after same-rel 2-hop is validated.

Allowed subset:
- One relationship type.
- Bounded range (`*m..n`, both finite).
- No edge-property filters.
- No `OPTIONAL MATCH`.
- Semantics match row engine exactly.

If semantic proof is not ready, variable-length stays on the row engine.

### 4.8 What NOT to Do in Phase 3

- No global visited-set dedup for ordinary `MATCH`.
- No generic `GetNeighborsMultiHop` before ASP-Join reuse path is validated.
- No `HashSet<NodeId>` for visited — use `FrontierScratch` (no dedup) or add
  `RoaringBitmap` only to a proven reachability-mode variant.
- No heterogeneous multi-hop before same-rel 2-hop is proven.
- No new join framework duplicating ASP-Join.

### 4.9 Acceptance Criteria

- [ ] `execute_two_hop_chunked()` planner-wired.
- [ ] 2-hop same-rel queries match row-engine for row count and values (LDBC SF-01).
- [ ] Multiplicity tests: `COUNT(*)`, `COUNT(DISTINCT x)`, duplicate-destination cases.
- [ ] Intermediate properties not read when not referenced.
- [ ] Memory limit enforcement: query aborts gracefully at configured threshold.
- [ ] Relative speedup ≥ 2× vs Phase 1 on 2-hop benchmark (record hardware + dataset).
- [ ] All existing acceptance tests pass.
- [ ] `cargo clippy --all-targets -D warnings` clean. `cargo fmt --all` clean.

---

## 5. Phase 4 — Shape Selector + Targeted High-Value Operators

### 5.1 Task List (Implementation Order)

1. `ChunkedPlan` selector (replace boolean guard)
2. `SlotIntersect` for mutual-neighbors patterns
3. Narrow `COUNT` aggregate sink
4. Ordered results via `sort_spill.rs` reuse
5. Optional: `ChunkStringHeap` for string-heavy queries
6. Parity / trace coverage
7. Benchmark packet

### 5.2 Task 1: `ChunkedPlan` Selector

Replace `can_use_chunked_pipeline()` with `try_plan_chunked_match()` (see §2.3).
Each variant encodes shape-specific parameters (label_ids, rel_type_ids, col_ids,
predicates). Each `execute_*_chunked` emits:

```rust
tracing::debug!(engine = "chunked", plan = %plan_name,
    "executing via chunked pipeline");
```

### 5.3 Task 2: `SlotIntersect` for Mutual Neighbors

Do NOT implement generic `HashJoin<Build, Probe>`. Add a targeted operator
for mutual-neighbor / fork-join slot intersection:

```rust
/// Intersects two slot-column streams on a shared key column.
/// For: MATCH (a)-[:R]->(x)<-[:R]-(b)
pub struct SlotIntersect {
    left_key_col: u32,
    right_key_col: u32,
    spill_threshold: usize,  // reuse join_spill.rs above this
}
```

- Join key is always a `u64` slot column.
- Build side: slot set or slot-to-row map.
- Spill reuses `join_spill.rs` — do not write new spill infrastructure.

### 5.4 Task 3: Narrow `COUNT` Aggregate Sink

Do NOT build a full generic `HashAggregate`. Supported Phase 4 aggregates:

- `COUNT(*)` — count all live rows
- `COUNT(var)` — count non-null values of a slot column
- `COUNT(DISTINCT slot_col)` — when planner proves a supported key shape

Not required: `SUM`, `MIN`, `MAX`, `collect()`, arbitrary grouped aggregates.

Implementation: either a dedicated narrow `CountSink` operator, or delegate to
existing aggregate helpers at the materializer boundary. Correctness over elegance.

### 5.5 Task 4: Ordered Results via `sort_spill.rs`

Two cases:

1. **`LIMIT n` without `ORDER BY`** — already handled by Phase 2 pull-model
   short-circuit. No new operator needed.

2. **`ORDER BY ... LIMIT k`** — reuse `sort_spill.rs` before inventing
   `OrderByLimit`. A dedicated `TopK` heap is a later optimization if profiling
   proves sort is the bottleneck.

### 5.6 Task 5: `ChunkStringHeap` (Optional)

If Phase 3/4 benchmarks show `Vec<String>` allocation as a bottleneck, add:

```rust
pub struct ChunkStringHeap {
    buffer: Vec<u8>,
}
// ColumnVector stores (offset: u32, len: u32) into heap for string columns.
```

String comparisons in `Filter` become byte-slice comparisons with no allocation.
Defer until there is a measured need.

### 5.7 Row Engine Retention

**The row engine is not retired in Phase 4.** Keep:
- row-engine fallback for all unsupported shapes,
- parity tests for every new chunked plan,
- trace logging for which engine ran each query.

Row engine retirement is a separate future decision.

### 5.8 What NOT to Do in Phase 4

- No row-engine retirement.
- No generic pipeline compiler.
- No generic `HashJoin<Build, Probe>`.
- No wide `HashAggregate` surface.
- No SIMD inner loops (Phase 5+).
- No morsel-driven parallelism (Phase 6+).
- No disabling fallback for architectural cleanliness.

### 5.9 Acceptance Criteria

- [ ] `try_plan_chunked_match()` selects among scan/one-hop/two-hop/mutual-neighbor
      with trace logging.
- [ ] Mutual-neighbor queries run via `SlotIntersect`.
- [ ] `COUNT(*)` without full-row materialization.
- [ ] `ORDER BY ... LIMIT k` reuses spillable sort.
- [ ] Every chunked plan has parity tests against the row engine.
- [ ] Fallback enabled and exercised in tests.
- [ ] All 14 acceptance checks pass with fallback enabled.
- [ ] Spill fires for `SlotIntersect` when build side exceeds threshold (stress test).
- [ ] Relative speedup ≥ 5× vs Phase 1 on mutual-friends (record hardware + dataset).
- [ ] `cargo clippy --all-targets -D warnings` clean. `cargo fmt --all` clean.

---

## 6. Cross-Phase Correctness Requirements

### 6.1 NULL Semantics — Hard Gate

- Absent property ≠ stored raw `0`.
- `IS NULL` / `IS NOT NULL` Cypher behavior preserved.
- Backward compatibility for data written before null bitmaps.

No performance result is acceptable if this regresses.

### 6.2 Path Multiplicity — Hard Gate

- Duplicate rows may be correct (distinct paths to same node).
- `COUNT(*)` parity must be checked separately from value parity.
- Duplicate elimination only when query explicitly requires it.

### 6.3 Tombstones and Visibility

Chunked plans must preserve tombstone filtering and snapshot visibility exactly
as the row engine. Reusing row-engine helpers at the sink naturally satisfies this.

### 6.4 Column ID Conventions

| Constant | Value | Meaning |
|----------|-------|---------|
| `COL_ID_SLOT` | `0` | Node slot number |
| `COL_ID_SRC_SLOT` | `u32::MAX - 1` | Source slot (GetNeighbors output) |
| `COL_ID_DST_SLOT` | `u32::MAX - 2` | Destination slot (GetNeighbors output) |

Add debug assertion in `col_id_of` that returned hash is not in the reserved range.

### 6.5 NodeId Encoding

```
high 32 bits = label_id (u32)
low  32 bits = slot     (u32)
```

Utility: `node_id_parts(raw: u64) -> (u32, u64)` in `engine/mod.rs`.
Slots in pipeline are always low 32 bits. label_id is a separate operator field.
**Never hardcode `src_label_id = 0`** — that was CRITICAL-1 (fixed in Phase 1).

### 6.6 Chunk Ownership

`DataChunk` owned by the operator that creates it. Passed by value. Do not `Box`
a `DataChunk`.

### 6.7 Benchmark Discipline

Every performance result must include:
- Query shape and Cypher text
- Dataset and scale factor
- `cargo bench` command used
- Hardware description
- Before/after numbers and relative speedup
- Correctness/parity status

Do not use bare ms CI gates unless CI hardware and dataset are locked.

---

## 7. Testing Strategy

### Unit Tests (`crates/sparrowdb-execution/tests/`)

Each new operator:
- Empty input → `Ok(None)`
- Single chunk, all rows live
- Multiple chunks with overflow
- Null handling: input with null bits set
- Selection vector: pre-filtered input (ReadNodeProps skips I/O)

### Integration Parity Tests (`crates/sparrowdb/tests/`)

Per `execute_*_chunked` entry point, add `spa_299_phase{N}_parity.rs`:
1. Test graph with known structure.
2. Same Cypher via both row engine and chunked engine.
3. Identical row counts AND identical sorted values.
4. Must include: missing properties, zero-valued, tombstoned, LIMIT,
   multiplicity-sensitive (`COUNT(*)`, `COUNT(DISTINCT)`).

### Benchmarks (`crates/sparrowdb/benches/`)

`pipeline_phase{N}_benchmarks.rs`:
- Relative speedup comment with v0.1.12 baseline.
- CI: `--bench -- --test` (compile-check only, no wall-clock gate).
- Full run: manual with hardware/dataset documented.

---

## 8. Implementation Order Per Phase

For each phase:
1. Storage API additions (if any)
2. New operator struct + unit tests (no planner wiring)
3. `cargo clippy --all-targets -D warnings` + `cargo fmt --all` clean
4. Planner eligibility / selector update
5. `execute_*_chunked` entry point
6. Wire planner to use new entry point
7. Integration parity test (multiplicity/NULL/tombstone coverage)
8. Criterion bench
9. PR — adversarial review before merge

---

## 9. Resolved Design Questions

1. **Nullable bulk read** — `batch_read_node_props_nullable` must be added to
   `NodeStore`. Existing API collapses missing to `0` which is semantically wrong.

2. **Backward CSR** — Reuse ASP-Join (already handles bidirectional). Phase 4
   `SlotIntersect` feeds forward + backward `GetNeighbors` into the intersection.

3. **Spill reuse** — `join_spill.rs` and `sort_spill.rs` are the reuse targets.
   No new spill infrastructure needed.

4. **Chunk capacity tuning** — Benchmark 512/1024/2048/4096 before Phase 4 and
   document the winner. Current 2048 is untuned.

5. **ORDER BY strategy** — Reuse `sort_spill.rs` first. `TopK` heap is an
   optimization only if full sort is proven to be the bottleneck.

6. **Multi-label dedup** — Secondary label membership enforced by catalog.
   `ScanByLabel` emits every slot in primary label range; no dedup pass needed.
