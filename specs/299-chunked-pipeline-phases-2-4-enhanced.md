# Spec: Chunked Vectorized Pipeline — Phases 2, 3, 4 (Enhanced)

**Parent issue:** #299
**Status:** Enhanced Draft — incorporates vectorized expression engine, string handling optimizations, and early filtering.
**Date:** 2026-03-25
**Author:** Gemini CLI (Enhanced from #299 Gemini spec)

---

## 0. Phase 1 Recap & Baseline

Phase 1 established the `PipelineOperator` trait and the `DataChunk` structure. 
Key components: `CHUNK_CAPACITY = 2048`, `ColumnVector` (tagged `u64`), and `NullBitmap`.

---

## 1. Enhancements & Missing Elements in Original Spec

The original spec provided a strong foundation but missed several critical optimizations for a production-grade vectorized engine:

1.  **Vectorized Expression Evaluation**: The original spec deferred expression evaluation to Phase 4 without defining the mechanism. This version introduces an `ExpressionEvaluator` that operates on chunks.
2.  **Intermediate Filtering (Early Exit)**: In multi-hop queries, filtering intermediate nodes (`WHERE b.age > 20`) is vital to prevent expanding "dead" frontiers. This spec adds intermediate predicates to `GetNeighborsMultiHop`.
3.  **Variable-Width Data (Strings)**: Storing `String` objects in a vector leads to pointer-chasing and frequent allocations. We propose a `ChunkStringHeap` for zero-allocation string handling within a chunk.
4.  **Memory Budgeting**: No mechanism was defined to prevent a single query from consuming all system memory. We introduce a `PipelineContext` for memory tracking.
5.  **Operator Fusion Readiness**: We identify candidates for fusion (e.g., `Filter` + `ReadProperties`) to reduce chunk passing overhead.

---

## 2. Phase 2 — Optimized 1-Hop & Property Reading

### 2.1 Enhanced `ReadProperties<C>`

Includes **Filter Push-down** to avoid materializing columns for rows that will be filtered anyway.

```rust
pub struct ReadProperties<C: PipelineOperator> {
    child: C,
    store: Arc<NodeStore>,
    label_id: u32,
    col_ids: Vec<u32>,
    /// Optional predicate to apply immediately after reading a column.
    /// If the predicate fails, the row is deselected in the chunk.
    pushdown_predicate: Option<Box<dyn ExpressionEvaluator>>,
}
```

### 2.2 Vectorized Expression Engine

A simple stack-based evaluator for Cypher expressions.

```rust
pub trait ExpressionEvaluator: Send + Sync {
    /// Evaluates the expression on the given chunk and returns a result ColumnVector.
    fn evaluate(&self, chunk: &DataChunk) -> Result<ColumnVector>;
}

pub enum ScalarExpr {
    ColumnRef(u32),
    Literal(Value),
    BinaryOp(BinaryOp, Box<ScalarExpr>, Box<ScalarExpr>),
    IsNull(Box<ScalarExpr>),
}
```

### 2.3 `Project<C>` Improvements

`Project` now handles expressions, not just column remapping.

```rust
pub struct Project<C: PipelineOperator> {
    child: C,
    /// List of (output_col_name, expression)
    projections: Vec<(String, Box<dyn ExpressionEvaluator>)>,
}
```

---

## 3. Phase 3 — Multi-Hop + BFS Arena + Early Filtering

### 3.1 `BfsArena` with Memory Tracking

```rust
pub struct BfsArena {
    current: Vec<u64>,
    next: Vec<u64>,
    visited: roaring::RoaringBitmap,
    /// Reference to query-wide memory tracker.
    mem_tracker: Arc<MemoryTracker>,
}
```

### 3.2 `GetNeighborsMultiHop<C>` with Intermediate Predicates

Supports queries like `MATCH (a)-[:R]->(b)-[:R]->(c) WHERE b.status = 'active'`.

```rust
pub struct GetNeighborsMultiHop<C: PipelineOperator> {
    child: C,
    hops: Vec<HopConfig>,
    arena: BfsArena,
    // ...
}

pub struct HopConfig {
    rel_type_id: u32,
    direction: Direction,
    /// Predicate applied to the node reached at THIS hop.
    predicate: Option<Box<dyn ExpressionEvaluator>>,
}
```

---

## 4. Phase 4 — Joins, Aggregates & String Handling

### 4.1 `ChunkStringHeap` (New)

To avoid `String` allocations, `ColumnVector` can represent strings as `(offset, len)` into a chunk-local heap.

```rust
pub struct ChunkStringHeap {
    buffer: Vec<u8>,
}

/// Updated ColumnVector to support string views.
pub enum VectorData {
    Fixed(Vec<u64>),
    Strings(ChunkStringHeap, Vec<StringView>),
}
```

### 4.2 `HashJoin` with Adaptive Strategy

If the build side is small (< 1024 rows), `HashJoin` can switch to a **SIMD-optimized nested loop join** to avoid hash table overhead.

### 4.3 Pipeline Compiler & Optimization Pass

The compiler should perform a simple optimization pass before execution:
1.  **Predicate Push-down**: Move `Filter` as close to `Scan` or `ReadProperties` as possible.
2.  **Column Pruning**: Remove properties that are never used in predicates or projections.
3.  **Join Reordering**: Smallest set becomes the "Build" side.

---

## 5. Implementation Enhancements (Technical Debt Mitigation)

*   **Zero-Copy Type Conversion**: Use the tagged `u64` from `NodeStore` directly in the pipeline. Only decode to `Value` when sending results to the user.
*   **Error Context**: Wrap pipeline errors with the operator name and chunk index for easier debugging of "unreachable" panics.
*   **Backpressure**: While pull-based, ensure `next_chunk` does not perform massive intermediate expansions that exceed the `MemoryTracker` limits.

---

## 6. Updated Acceptance Criteria

*   [ ] **String Benchmark**: IC3 with string filters shows no regression vs numeric filters.
*   [ ] **Memory Limit Test**: Query is aborted gracefully when `MemoryTracker` limit is hit.
*   [ ] **Filter Push-down Validation**: `ReadProperties` skips I/O for columns if a previous push-down filter already marked the row as dead.
*   [ ] **Expression Parity**: `1 + 1` and `n.age * 2` work correctly in `RETURN`.
