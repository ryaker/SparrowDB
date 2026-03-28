# Spec: Chunked Vectorized Pipeline — Phases 2, 3, 4 (v3 Final)

**Parent issue:** #299
**Status:** Implementation Ready (Enhanced via Deep Structural Audit & Contrarian Review)
**Date:** 2026-03-25
**Author:** Gemini CLI (Revised post-PR #335)

---

## 0. Phase 1 Implementation Baseline

Phase 1 (PR #335) landed the core infrastructure:
- `PipelineOperator` trait: `next_chunk() -> Result<Option<DataChunk>>`.
- `DataChunk`: Columnar storage with `CHUNK_CAPACITY` (default 2048), `NullBitmap`, and selection vectors (`sel`).
- `ScanByLabel`: Yields slot numbers in chunks.
- `GetNeighbors`: CSR lookup + delta merge with buffering for fan-out.
- `Filter`: Updates selection vector based on predicates.

---

## 1. Phase 2 — Selection-Aware I/O & Vectorized Expressions

### 1.1 `ReadProperties<C>` (Selection-Aware)
The operator must only process rows marked "active" in the input chunk's selection vector.

**Implementation Mandate:**
- Extract active slots from `chunk.column(COL_ID_SLOT)` using `chunk.live_rows()`.
- Call `store.batch_read_node_props(label_id, &active_slots, &col_ids)`.
- **Zero-Copy Optimization**: Map the resulting `u64` values directly into new `ColumnVector`s.
- **Null Handling**: Ensure `NullBitmap` is correctly populated for missing properties.

### 1.2 Vectorized Expression Engine
Move from row-at-a-time `Value` evaluation to columnar evaluation.

```rust
pub trait ExpressionEvaluator: Send + Sync {
    /// Evaluates the expression on the given chunk and returns a result ColumnVector.
    fn evaluate(&self, chunk: &DataChunk) -> Result<ColumnVector>;
}

pub enum ScalarExpr {
    ColumnRef(u32),
    Literal(Value),
    BinaryOp(BinaryOp, Box<ScalarExpr>, Box<ScalarExpr>),
}
```

- **Filter Push-down**: The planner should push simple predicates (e.g., `n.age > 20`) into `ReadProperties` or even `ScanByLabel` if possible.

---

## 2. Phase 3 — BFS Frontier Pruning & Memory Safety

### 2.1 `BfsArena` with Roaring Bitmap
Addressing the contrarian review finding: `HashSet<u64>` is a bottleneck for dense slot IDs.
- **Mandate**: Use `roaring::RoaringBitmap` for the visited set.
- **Memory Tracking**: Pass an `Arc<MemoryTracker>` to `BfsArena`. If the frontier exceeds `MAX_FRONTIER_MEMORY` (default 128 MiB), the query must abort.

### 2.2 `GetNeighborsMultiHop` with Intermediate Pruning
Supports `WHERE` predicates at each hop level to prevent frontier explosion.

```rust
pub struct HopConfig {
    rel_type_id: u32,
    direction: Direction,
    /// Predicate applied level-by-level. 
    /// Prunes the frontier BEFORE the next expansion.
    intermediate_filter: Option<Box<dyn ExpressionEvaluator>>,
}
```

---

## 3. Phase 4 — Adaptive Joins & Zero-Allocation Strings

### 3.1 `ChunkStringHeap`
Avoid `Vec<String>` allocations.
- **Implementation**: `ColumnVector` for strings stores `(offset: u32, len: u32)` into a chunk-local `Vec<u8>`.
- **Benefit**: String comparisons in `Filter` and `HashJoin` become memory-mapped byte slice comparisons.

### 3.2 Adaptive `HashJoin`
- **Small Build Strategy**: If the build side is < 1024 rows, use a **SIMD Nested Loop Join** (linear scan of build side for each probe row). This avoids hash table construction latency.
- **Spill-to-Disk**: Reuse `sparrowdb_execution::join_spill` if the build side exceeds `MAX_HASH_JOIN_MEMORY` (256 MiB).

---

## 4. Operational Requirements (from Contrarian Review)

### 4.1 Configurable Engine
- `EngineBuilder` must expose `with_chunk_capacity(usize)` and `with_memory_limit(usize)`.
- Default `CHUNK_CAPACITY` remains 2048 but must be validated via benchmarks.

### 4.2 Mandatory Testing
- **Integration Parity**: Every new operator requires a test in `crates/sparrowdb/tests/` asserting identical results to the row-engine.
- **Property Testing**: Use `proptest` to validate selection vector counts and null handling invariants.
- **Stress Test**: A specific test for "unbounded fan-out" that verifies memory limits are enforced.

### 4.3 Performance Baseline
| Query | Target Improvement |
|-------|--------------------|
| 1-hop + Filter | ≥ 5x |
| 2-hop + Pruning | ≥ 8x |
| Mutual Friends | ≥ 10x |

---

## 5. Implementation Roadmap

1.  **Phase 2**: `ReadProperties` (Selection-Aware) + Basic Expression Engine.
2.  **Phase 3**: `BfsArena` (Roaring) + `GetNeighborsMultiHop` + Memory Tracking.
3.  **Phase 4**: `HashJoin` (Adaptive) + `ChunkStringHeap` + Full Pipeline Compiler.
