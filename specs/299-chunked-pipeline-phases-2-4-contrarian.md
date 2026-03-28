# Spec: Chunked Pipeline — Phases 2, 3, 4 (Contrarian, Code-Aligned)

**Parent issue:** #299  
**Replaces:** planning assumptions in `specs/299-chunked-pipeline-phases-2-4.md`  
**Status:** Draft  
**Date:** 2026-03-27  
**Author:** Codex synthesis against merged Phase 1 code

---

## 1. Why This Version Exists

The previous draft is ambitious in ways that are not well aligned with the code
that Phase 1 actually landed.

That draft assumes we should:

1. build a second full execution stack with its own generic planner/compiler,
2. introduce generic `HashJoin`, `HashAggregate`, and `OrderByLimit` early,
3. use a global visited set for multi-hop traversal,
4. bulk-read properties without first nailing NULL semantics.

This spec takes the opposite view.

**Phases 2-4 should not create a second database engine inside SparrowDB.**
They should extend the merged Phase 1 chunked pipeline just enough to remove
measured hot-path allocations, while reusing the existing row engine,
factorized operators, ASP-Join implementation, spill infrastructure, and
expression/materialization code wherever possible.

The row-at-a-time engine stays. The factorized engine stays. The chunked
pipeline becomes a targeted fast path for the shapes where it clearly wins.

---

## 2. Phase 1 Reality Check

This spec is written against the merged Phase 1 code shape:

- `crates/sparrowdb-execution/src/chunk.rs`
- `crates/sparrowdb-execution/src/pipeline.rs`
- `crates/sparrowdb-execution/src/engine/pipeline_exec.rs`

### 2.1 What Phase 1 Actually Added

Phase 1 added these concrete building blocks:

- `CHUNK_CAPACITY = 2048`
- `NullBitmap`
- `ColumnVector { data: Vec<u64>, nulls: NullBitmap, col_id: u32 }`
- `DataChunk { columns, len, sel }`
- `DataChunk::filter_sel(...)`
- `DataChunk::live_rows()`
- `COL_ID_SLOT`, `COL_ID_SRC_SLOT`, `COL_ID_DST_SLOT`
- `PipelineOperator`
- `ScanByLabel`
- `GetNeighbors`
- `Filter`
- `Engine::can_use_chunked_pipeline()`
- `Engine::execute_scan_chunked()`
- `Engine::with_chunked_pipeline()`

### 2.2 What Phase 1 Did Not Do

Phase 1 is still intentionally narrow:

- only single-label scan queries use the chunked path,
- `WHERE` and projection inside `execute_scan_chunked()` are still row-at-a-time,
- `ScanByLabel::new(hwm)` pre-materializes `0..hwm` into a `Vec<u64>`,
- `GetNeighbors` exists but is not planner-wired for real hop queries,
- there is no property-reading operator in the pipeline,
- there is no chunked aggregation, join, or sort path,
- there is no chunked planner beyond a narrow boolean guard.

### 2.3 Existing Reuse Targets We Must Respect

The codebase already contains important infrastructure that this spec treats as
the preferred reuse path:

- the row engine query semantics in `engine/scan.rs` and `engine/hop.rs`,
- factorized execution types in `types.rs`,
- binary ASP-Join in `join.rs`,
- spill-to-disk ASP-Join support in `join_spill.rs`,
- spill-to-disk sorting in `sort_spill.rs`,
- nullable property reads in `NodeStore::get_node_raw_nullable(...)`.

That means the goal is not "replace everything with chunked operators."
The goal is "move the hot loops onto chunked batches without forking semantics."

---

## 3. Design Stance

### 3.1 Principles

1. **Reuse before inventing.** If the row engine, ASP-Join, or spill code
   already solves a problem, the chunked plan should wrap it or feed it rather
   than replace it.
2. **Correctness before vectorization.** `NULL` handling and path multiplicity
   are release blockers.
3. **Targeted planners before compilers.** We should add a small
   `ChunkedPlan` selector, not a whole new operator-DAG compiler.
4. **Late materialization by default.** Read only the properties required for
   filters, RETURN items, and ordering.
5. **Fallback is a feature, not a shame path.** Unsupported or risky shapes
   fall back to the row engine automatically.

### 3.2 What This Spec Explicitly Rejects

This spec rejects the following for Phases 2-4:

- retiring the row engine,
- replacing `Engine::execute()` with a generic pipeline compiler,
- introducing a generic `HashJoin<Build, Probe>` for all join shapes,
- introducing a generic `HashAggregate` for every aggregate in Phase 4,
- using a global visited set in ordinary `MATCH` path expansion,
- setting chunked-path performance gates in raw milliseconds without locking
  benchmark hardware and dataset state.

---

## 4. Execution Model for Phases 2-4

### 4.1 Planner Shape

Instead of a boolean "can/cannot" switch plus ad hoc executors, add a small
shape-aware planner:

```rust
pub enum ChunkedPlan {
    Scan(ScanChunkedPlan),
    OneHop(OneHopChunkedPlan),
    TwoHop(TwoHopChunkedPlan),
    MutualNeighbors(MutualNeighborsPlan),
}

pub fn try_plan_chunked_match(&self, m: &MatchStatement) -> Option<ChunkedPlan>;
```

`try_plan_chunked_match()` returns `None` for unsupported shapes, and the
existing row-at-a-time path remains the fallback.

This is intentionally not a full compiler. It is a shape selector.

### 4.2 Terminal Materialization

For Phases 2 and 3, terminal row materialization should continue using existing
row-engine helpers such as `project_row(...)`, `build_row_vals(...)`, and the
existing expression evaluation code where practical.

We do **not** introduce a generic `Project` operator in Phase 2 just to move
projection logic into the pipeline. That would duplicate semantics before we
have proved a performance need.

### 4.3 Late Materialization Rule

Every chunked plan must compute a per-variable property need set:

- properties referenced by inline pattern filters,
- properties referenced by `WHERE`,
- properties referenced by `RETURN`,
- properties referenced by `ORDER BY`.

Only those property columns may be read.

---

## 5. Phase 2 — Make 1-Hop Real

### 5.1 Goal

Phase 2 is not "add many operators."
Phase 2 is "turn the existing chunked pipeline into a real fast path for simple
1-hop traversals without regressing semantics."

Primary outcomes:

1. eliminate the up-front slot-vector allocation in `ScanByLabel`,
2. planner-wire `GetNeighbors` for actual 1-hop queries,
3. batch-read node properties with correct NULL handling,
4. support simple filter pushdown for the most common predicate forms,
5. preserve row-engine projection semantics at the sink.

### 5.2 Required Changes

#### 5.2.1 Replace `ScanByLabel` Full Slot Materialization

Current Phase 1 behavior:

```rust
pub struct ScanByLabel {
    slots: Vec<u64>,
    cursor: usize,
}
```

This is the wrong long-term shape because it allocates one `Vec<u64>` for the
entire label before the first chunk is emitted.

Replace it with:

```rust
pub struct ScanByLabel {
    next_slot: u64,
    end_slot: u64,
}
```

Rules:

- `ScanByLabel::new(hwm)` must allocate at most the current output chunk.
- `ScanByLabel::from_slots(Vec<u64>)` may remain for tests and special-case
  scans, but `new(hwm)` is the production path.
- output semantics do not change.

#### 5.2.2 Add `ReadNodeProps<C>`

Add a chunk operator that reads node properties in bulk for the live rows of a
slot-bearing input chunk.

```rust
pub struct ReadNodeProps<C: PipelineOperator> {
    child: C,
    store: Arc<NodeStore>,
    label_id: u32,
    slot_col_id: u32,
    col_ids: Vec<u32>,
}
```

Behavior:

- reads the slot column identified by `slot_col_id`,
- extracts live slots in current row order,
- bulk-reads the requested `col_ids`,
- appends one `ColumnVector` per property column,
- preserves input row order and selection vector.

#### 5.2.3 Add a Nullable Bulk Read API

`NodeStore::batch_read_node_props(...)` is not sufficient for Phase 2 because
it returns raw `u64` and collapses missing values to `0`.

That is semantically wrong for properties whose legitimate stored value is
`Int64(0)` or `Bool(false)` and for `IS NULL` / absent-property handling.

Phase 2 must add:

```rust
pub fn batch_read_node_props_nullable(
    &self,
    label_id: u32,
    slots: &[u32],
    col_ids: &[u32],
) -> Result<Vec<Vec<Option<u64>>>>;
```

Implementation requirements:

- use the null-bitmap sidecar when present,
- preserve backward compatibility when the null bitmap file does not exist,
- keep the existing sorted-slot vs full-column heuristic,
- never treat raw `0` as "missing" when the bitmap says the slot is present.

`ReadNodeProps` builds `NullBitmap` from these `Option<u64>` values.

#### 5.2.4 Add Narrow Predicate Pushdown, Not a Generic Expression Engine

Phase 2 should not introduce a full `ExpressionEvaluator` framework.

Instead, add a narrow predicate representation for the subset we need to
accelerate 1-hop filters:

```rust
pub enum ChunkPredicate {
    Eq { col_id: u32, rhs_raw: u64 },
    Gt { col_id: u32, rhs_raw: u64 },
    Ge { col_id: u32, rhs_raw: u64 },
    Lt { col_id: u32, rhs_raw: u64 },
    IsNull { col_id: u32 },
    IsNotNull { col_id: u32 },
    And(Vec<ChunkPredicate>),
}
```

Rules:

- compile only simple conjunctive property predicates into `ChunkPredicate`,
- unsupported `WHERE` expressions fall back to the row engine,
- string `CONTAINS`, arbitrary function calls, list predicates, subqueries,
  and cross-variable expressions remain on the row path in Phase 2.

This is deliberately narrower than a generic vectorized expression system.

#### 5.2.5 Add `execute_one_hop_chunked()`

Add:

```rust
pub(crate) fn execute_one_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>;
```

Initial supported query shape:

- single pattern,
- exactly 2 nodes and 1 relationship,
- planner resolves exactly one relationship table,
- directed outgoing or incoming,
- no `OPTIONAL MATCH`,
- no `UNION`,
- no subquery in `WHERE`,
- no aggregate,
- no `ORDER BY`,
- `LIMIT` allowed when there is no `DISTINCT`.

`LIMIT` is intentionally allowed here because the pull-based chunked path can
short-circuit at the sink without needing a new operator.

Pipeline shape:

```text
MaterializeRows(limit?)
  <- optional FilterChunk(dst)
  <- optional ReadNodeProps(dst)
  <- GetNeighbors
  <- optional FilterChunk(src)
  <- optional ReadNodeProps(src)
  <- ScanByLabel
```

Notes:

- src properties are read only if referenced,
- dst properties are read only if referenced,
- terminal projection still uses existing row-engine helpers,
- tombstone checks must remain consistent with the row engine.

### 5.3 What Phase 2 Must Not Do

- no generic `Project` operator,
- no generic vectorized expression engine,
- no edge-property reads,
- no `ORDER BY`,
- no aggregation,
- no row-engine deletion,
- no assumption that every 1-hop query maps to one CSR table; the planner must
  prove that before activating the chunked path.

### 5.4 Acceptance Criteria

- [ ] `ScanByLabel::new(hwm)` no longer allocates `Vec<u64>` for `0..hwm`.
- [ ] `MATCH (a:L)-[:R]->(b:M) RETURN ...` can run via `execute_one_hop_chunked`.
- [ ] Incoming-edge 1-hop can run by swapping logical roles rather than
      duplicating traversal logic.
- [ ] Phase 2 bulk property reads preserve `NULL` vs `Int64(0)` semantics.
- [ ] `LIMIT n` short-circuits after `n` output rows when no `ORDER BY`,
      `DISTINCT`, or aggregation is present.
- [ ] Row counts and projected values match the row engine across a parity suite
      that includes missing properties, zero-valued properties, and tombstoned
      nodes.
- [ ] A targeted 1-hop benchmark shows a clear improvement over Phase 1; record
      relative speedup and hardware details rather than a hard-coded millisecond
      gate.

---

## 6. Phase 3 — Two-Hop and Bounded Path Expansion Without Breaking Semantics

### 6.1 Goal

Phase 3 should extend the chunked path to 2-hop and selected bounded path
queries, but it must not silently change Cypher multiplicity semantics.

This is where the previous draft is riskiest.

A global `visited` set across ordinary `MATCH` path expansion is attractive for
speed, but it is not semantically safe for enumerative path queries. It can
collapse duplicate paths that the current engine would count separately.

### 6.2 Non-Negotiable Semantic Rule

For ordinary `MATCH` path expansion:

- fixed-length and bounded-length path queries are **path-enumerative** unless
  the existing row engine already proves otherwise,
- duplicate destination nodes may still represent distinct valid paths,
- duplicate elimination must happen only when the query explicitly requires it
  (`DISTINCT`, set semantics, or an equivalent proven optimization mode).

Therefore:

**Phase 3 must not introduce a default global visited set that deduplicates
ordinary query results.**

### 6.3 Preferred Implementation Strategy

#### 6.3.1 Reuse ASP-Join for 2-Hop Same-Rel Shapes

The repo already contains:

- `AspJoin::two_hop(...)`
- `AspJoin::two_hop_factorized(...)`
- `SpillingHashJoin::two_hop(...)`

Phase 3 should exploit that existing work rather than inventing a generic
`GetNeighborsMultiHop` first.

Supported first:

- same relationship table on both hops,
- same direction on both hops,
- no edge-property predicates,
- no `OPTIONAL MATCH`.

This is the high-value, low-risk 2-hop shape.

#### 6.3.2 Add a Small Scratch Buffer, Not a Semantic Visited Structure

If profiling shows frontier allocations are still hot for bounded path queries,
introduce:

```rust
pub struct FrontierScratch {
    current: Vec<u64>,
    next: Vec<u64>,
}
```

Purpose:

- reuse frontier buffers across levels,
- avoid per-level `Vec` churn,
- keep semantics unchanged.

Do **not** attach a default `visited` set to this structure.

If a future reachability-only optimization needs visited tracking, it must be
behind a separate proof step and separate plan variant.

#### 6.3.3 Late Materialize Intermediate Properties

For a 2-hop query:

- do not read intermediate node properties unless the query references them,
- if `WHERE` references `b`, read only the columns needed for `b`,
- if the query returns only `c`, do not read `b` properties at all.

This is more important than a fancy arena type.

#### 6.3.4 Add `execute_two_hop_chunked()`

Add:

```rust
pub(crate) fn execute_two_hop_chunked(
    &self,
    m: &MatchStatement,
    column_names: &[String],
) -> Result<QueryResult>;
```

Initial supported shape:

- single pattern,
- exactly 3 nodes and 2 relationships,
- both hops resolve to the same relationship table,
- no aggregate,
- no `ORDER BY`,
- no subquery in `WHERE`,
- no edge-property predicates.

Use the existing 2-hop infrastructure first. Generalized heterogeneous
multi-hop remains on the row path until the same-rel case is validated.

#### 6.3.5 Variable-Length Path Scope

Phase 3 may add a bounded variable-length chunked path only if it can match the
current row-engine semantics for the chosen subset.

Allowed initial subset:

- one relationship type,
- bounded range only (`*m..n`, both finite),
- no edge-property filters,
- no `OPTIONAL MATCH`,
- no claim of reachability-mode dedup unless the planner proves the query
  requires set semantics.

If that proof is not ready, variable-length stays on the row engine in Phase 3.

### 6.4 What Phase 3 Must Not Do

- no global visited-set dedup for ordinary `MATCH`,
- no generic `GetNeighborsMultiHop` just because it is aesthetically cleaner,
- no assumption that deduplicated `(src, dst)` pairs are always correct,
- no heterogeneous multi-hop support before the same-rel 2-hop path is proven,
- no new join framework that duplicates ASP-Join.

### 6.5 Acceptance Criteria

- [ ] `execute_two_hop_chunked()` exists and is planner-wired.
- [ ] 2-hop same-rel queries match row-engine results for row count and values.
- [ ] Multiplicity-sensitive tests pass:
      `COUNT(*)`, `COUNT(DISTINCT x)`, and duplicate-destination cases.
- [ ] Intermediate properties are not read when not referenced.
- [ ] If `FrontierScratch` is introduced, it reduces allocation churn without
      changing result semantics.
- [ ] Benchmark reporting includes both performance and multiplicity-parity
      checks; performance wins do not count if duplicate suppression changed
      the answer.

---

## 7. Phase 4 — Selective Generalization, Not a Rewrite

### 7.1 Goal

Phase 4 should broaden the chunked fast path to the highest-value non-linear
shapes and simple aggregates, but it should still avoid turning #299 into a
full engine rewrite.

This phase is about planner coverage and specialized operators, not about
building a generic database kernel inside the chunked path.

### 7.2 Replace the Boolean Guard With a Plan Selector

By Phase 4, `can_use_chunked_pipeline()` should become an implementation detail
of `try_plan_chunked_match()`.

The selector decides among:

- row engine,
- chunked scan,
- chunked one-hop,
- chunked two-hop,
- chunked mutual-neighbors/intersection.

This is enough to remove shape-specific ad hoc checks without replacing the
existing top-level execution structure.

### 7.3 Mutual Friends / Fork-Join Patterns

The prior draft proposed a generic `HashJoin<Build, Probe>`.
That is too broad for Phase 4.

What we actually need first is a specialized join on slot columns for patterns
like mutual neighbors.

Add a narrow operator or executor along these lines:

```rust
pub struct SlotIntersect {
    left_key_col: u32,
    right_key_col: u32,
    spill_threshold: usize,
}
```

Behavior:

- join key is always a slot-bearing `u64` column,
- build side is a slot set or slot-to-row map, not arbitrary row payload,
- spill reuses the partitioning ideas already implemented in `join_spill.rs`,
- output preserves the variable bindings needed by the query.

This should be enough for the chunked IC8-style path without committing us to a
generic hash-join framework we may not actually want.

### 7.4 Aggregation Scope

Phase 4 should add a **narrow aggregate sink**, not a full generic
`HashAggregate` for all functions.

Initial required scope:

- `COUNT(*)`
- `COUNT(var)`
- `COUNT(prop)`
- `COUNT(DISTINCT slot_or_prop)` only when the planner proves a supported key
  shape

The implementation may either:

- maintain a narrow chunked aggregate sink, or
- hand chunked rows into existing aggregate helpers at the sink boundary.

What matters is correctness and avoiding needless full-row materialization.

`SUM`, `MIN`, `MAX`, `collect()`, and arbitrary grouped aggregates are not
required for #299 unless a benchmark explicitly depends on them.

### 7.5 ORDER BY / LIMIT Scope

Phase 4 should distinguish two cases:

1. **Plain `LIMIT` without ordering**
   Already supported by the pull model and should be part of Phase 2/4 chunked
   plans.

2. **`ORDER BY ... LIMIT k`**
   Reuse existing spillable sort infrastructure in `sort_spill.rs` before
   inventing a new in-pipeline `OrderByLimit` operator.

A specialized `TopK` path can be added later if profiling proves that the full
sort is the bottleneck.

### 7.6 Row Engine Retention

The previous draft proposed retiring the row engine after Phase 4.

This spec does not.

Phase 4 must keep:

- row-engine fallback,
- parity tests against row-engine results,
- traceability of which engine executed which query shape.

Removing the row engine is a separate decision that requires its own benchmark,
correctness, and maintenance analysis.

### 7.7 What Phase 4 Must Not Do

- no row-engine retirement plan in this issue,
- no full pipeline compiler,
- no generic `HashJoin` unless the specialized slot-join proves insufficient,
- no generic `HashAggregate` with a wide surface area,
- no disabling fallback for the sake of a cleaner architecture diagram.

### 7.8 Acceptance Criteria

- [ ] planner selects among chunked scan / one-hop / two-hop / mutual-neighbor
      shapes with explicit trace logging,
- [ ] mutual-neighbor queries can use a chunked specialized slot join,
- [ ] `COUNT(*)` runs on supported chunked plans without flattening to full
      row maps first,
- [ ] `ORDER BY ... LIMIT k` on supported chunked plans reuses spillable sort
      infrastructure or clearly documents a narrower supported subset,
- [ ] every supported chunked plan has parity tests against the row engine,
- [ ] fallback remains enabled and exercised in tests.

---

## 8. Cross-Phase Correctness Requirements

### 8.1 NULL Semantics Are a Hard Gate

Any chunked property read path must preserve:

- absent property != stored raw `0`,
- `IS NULL` / `IS NOT NULL` behavior,
- backward compatibility for data written before null bitmaps existed.

No performance result is acceptable if this regresses.

### 8.2 Path Multiplicity Is a Hard Gate

For path queries:

- duplicate rows may be correct,
- duplicate destinations do not imply duplicate paths,
- `COUNT(*)` parity must be checked separately from value parity.

### 8.3 Tombstones and Visibility

Chunked plans must preserve:

- tombstone filtering,
- snapshot visibility,
- any existing read-time semantics enforced by the row engine.

### 8.4 Traceability

Every chunked executor must emit:

```rust
tracing::debug!(
    engine = "chunked",
    plan = "...",
    "executing via chunked pipeline"
);
```

This is required for debugging and benchmark attribution.

### 8.5 Benchmark Discipline

Performance acceptance for Phases 2-4 must be reported as:

- query shape,
- dataset,
- benchmark command,
- hardware,
- before/after numbers,
- relative speedup,
- correctness/parity status.

Do not use bare "must be under X ms on CI" gates unless CI hardware and dataset
state are locked and versioned.

---

## 9. Recommended Ticket Split

### 9.1 Phase 2

1. `ScanByLabel` cursor rewrite
2. nullable bulk-read storage API
3. `ReadNodeProps`
4. narrow predicate compilation
5. `execute_one_hop_chunked`
6. parity + NULL + LIMIT tests
7. 1-hop benchmarks

### 9.2 Phase 3

1. `execute_two_hop_chunked`
2. ASP-Join reuse path
3. late intermediate materialization
4. optional `FrontierScratch`
5. multiplicity-focused parity tests
6. 2-hop benchmarks

### 9.3 Phase 4

1. `ChunkedPlan` selector
2. specialized slot intersection for mutual neighbors
3. narrow aggregate sink for `COUNT`
4. spill-sort reuse for ordered chunked results
5. parity/trace coverage
6. benchmark packet

---

## 10. Summary

The core contrarian point is simple:

**#299 should make the chunked pipeline useful, not architecturally pure.**

If we:

- remove the obvious allocation mistakes,
- wire `GetNeighbors` into real plans,
- add NULL-correct bulk property reads,
- reuse ASP-Join and spill code,
- preserve path multiplicity,
- and keep the row engine as the oracle,

then Phases 2-4 can deliver real wins without creating a second unfinished
execution engine that SparrowDB then has to maintain forever.
