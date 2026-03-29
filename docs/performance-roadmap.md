# Performance Roadmap

This document records known bottlenecks, measured improvements, and
prioritised ideas for closing the gap with server-based graph databases.
It is the authoritative source for performance work — update it when a
fix ships or when new data invalidates an assumption.

---

## Measured Baselines (v0.1.14, synthetic 1 K-node graph)

Criterion benchmarks in `crates/sparrowdb/benches/snap_benchmarks.rs`.
Graph: 1 000 User nodes (uid property), FAN_OUT=20 KNOWS edges each.

| Query | Before (main) | After fix | Notes |
|-------|--------------|-----------|-------|
| Q8 Mutual Friends | 146ms | 2.8ms | #357 merged; −97.5% |
| Q4 DISTINCT 2-hop | ~538ms | ~538ms | #358 in review; fix correct but BFS dominates at 1K nodes |
| Q5 Variable Path (single source) | ~31ms | ~28ms | #359 in review; within noise; BFS dominates |

**Why Q4 and Q5 fixes don't show on the 1K-node benchmark:**
The 1 000-node synthetic graph fits entirely in the OS page cache (~8KB
per column file). Property reads for source resolution (Q5) and dedup
over the post-traversal result set (Q4) are both negligible compared to
the BFS cost.  At SNAP scale (4 039 nodes, 88 234 edges) and beyond,
these become visible:

- Q4: ~88K pre-DISTINCT rows → dedup O(N²) with old code, O(N) with fix.
- Q5: 4 039 slot reads × cold column file = measurable overhead at first query.

---

## Improvement Ideas

Ranked by estimated impact relative to implementation effort.

### Tier 1 — High impact, existing infrastructure

#### 1. Rayon parallel source iteration

**Affects:** Q3 (1-hop), Q4 (2-hop), Q5 (variable-length), Q8 (mutual friends)
**Effort:** 1–2 days
**Expected gain:** 4–8× on M-series (8 performance cores); linear with core count

`execute_one_hop`, `execute_two_hop`, and `execute_variable_length` all
iterate `for src_slot in 0..hwm_src`.  Rayon is already a workspace
dependency and is wired into parts of the engine (commit `1f8cc0d`).
The source loop is embarrassingly parallel — each source node's
traversal is independent.

```rust
use rayon::prelude::*;

let rows: Vec<Vec<Value>> = (0..hwm_src)
    .into_par_iter()
    .filter_map(|src_slot| {
        // per-source traversal, returns Option<Vec<Value>>
    })
    .flatten()
    .collect();
```

Caveats: `Engine` must be `Sync`; check that `prop_index` borrow
(`RefCell`) is not shared across threads.  May need to pass
`Arc<PropertyIndex>` instead.

#### 2. LIMIT pushdown into 2-hop and mutual-neighbor traversals

**Affects:** Any `MATCH ... RETURN ... LIMIT k` query
**Effort:** 0.5 days
**Expected gain:** Near-instant for TOP-N queries regardless of graph size

LIMIT is already pushed into node scans (commit `4372f0a`).  Extending
it to 2-hop and mutual-neighbor traversals means stopping as soon as `k`
rows are collected rather than materialising the full result set first.
An `AtomicUsize` row counter shared across Rayon tasks can enforce the
limit even in parallel execution.

#### 3. Chunked DISTINCT path via `SlotDedup` operator

**Affects:** Q4 DISTINCT 2-hop (and all other DISTINCT traversals)
**Effort:** 1 day
**Expected gain:** Unlocks vectorised execution for DISTINCT queries;
currently the chunked pipeline rejects DISTINCT and falls to row-at-a-time

Add a `SlotDedup` pipeline operator backed by a Roaring bitmap.  The
bitmap tracks seen destination slots and filters duplicates during chunk
production, eliminating the post-hoc `deduplicate_rows` call entirely.

```
ChunkedPlan:  ScanByLabel → GetNeighbors → GetNeighbors → SlotDedup → ReadNodeProps
```

`SlotDedup` is cheap: a single Roaring bitmap `insert` per slot, O(1)
amortised.

### Tier 2 — Medium effort, medium impact

#### 4. Pre-sorted CSR adjacency lists for merge-join intersection

**Affects:** Q8 mutual friends (SlotIntersect), any multi-hop intersection
**Effort:** 1–2 days
**Expected gain:** O(M+N) merge-join replaces O(M) HashSet build for dense graphs

Currently `SlotIntersect` hashes one side and probes the other.  If CSR
adjacency lists are kept sorted by destination slot (enforce sort on
insert), intersection becomes a linear merge:

```
left: [2, 5, 7, 12, ...]
right: [3, 5, 9, 12, ...]
→ advance two pointers, emit matches: [5, 12, ...]
```

No allocations, cache-friendly sequential access.  For mutual-neighbor
queries on high-fanout graphs this is faster than hashing when both
sides are large.

#### 5. Persistent query plan cache

**Affects:** All repeated queries (agent workloads making 1 000s of identical queries)
**Effort:** 0.5 days
**Expected gain:** Eliminates re-planning overhead; matters for agents

Cache `query_string → ChunkedPlan + bound_ast` in a `HashMap` on
`GraphDb`.  Key on the raw query string (or a FNV hash of it).
Invalidate on schema change (label/rel-type creation).

#### 6. Vectorised WHERE evaluation on column vectors

**Affects:** Q2 (range filter), any scan with a WHERE predicate
**Effort:** 2–3 days
**Expected gain:** Eliminates per-row decode; enables SIMD on integer comparisons

Instead of evaluating `WHERE n.age > 30` row-by-row after decoding,
read the entire `age` column into a `Vec<u64>` (already possible via
`read_col_all`) and evaluate the predicate as a bitwise pass, producing
a Roaring bitmap of matching slots.  Then use the bitmap to filter
subsequent pipeline stages.

#### 7. Bidirectional BFS for `shortestPath`

**Affects:** `shortestPath((a)-[:R*]->(b))`
**Effort:** 1–2 days
**Expected gain:** Exponentially fewer nodes visited on high-fanout graphs

Current implementation is unidirectional BFS from `a`.  Bidirectional
BFS expands alternately from both `a` and `b`, meeting in the middle.
For a graph with fanout `f` and distance `d`, unidirectional visits
`O(f^d)` nodes; bidirectional visits `O(2 × f^(d/2))`.

### Tier 3 — Architectural changes (v2.0 scope)

| Idea | Gain | Cost |
|------|------|------|
| Column-oriented storage (Arrow layout) | SIMD, batch decode | Full storage rewrite |
| Explicit LRU buffer pool | Predictable memory, eviction control | Replaces OS page cache reliance |
| Parallel WAL commit (group commit) | Higher write throughput | WAL protocol change |
| Adaptive join ordering | Better plans on skewed graphs | Requires cardinality statistics |

---

## Embedded vs Server — When SparrowDB Wins

Documenting the cases where SparrowDB's embedded model has inherent
advantages regardless of traversal speed, to guide how we frame
performance results to users.

### Latency composition for AI agent workloads

A typical agent session making 50 graph queries:

```
Neo4j (cloud/Docker)             SparrowDB (embedded)
─────────────────────────────────────────────────────
Open:    ~5 000ms (server start) Open:    ~27ms (file open)
Query 1: traversal + 2ms RTT     Query 1: traversal + 0ms RTT
Query 2: traversal + 2ms RTT     Query 2: traversal + 0ms RTT
...                              ...
Query 50: traversal + 2ms RTT    Query 50: traversal + 0ms RTT

RTT tax: 50 × 2ms = 100ms        RTT tax: 0ms
```

For 1-hop traversals where SparrowDB is 68× slower than Neo4j
(42ms vs 0.6ms), the embedded model still wins on per-session
total latency once the server cold-start and per-query round-trips
are factored in for short-lived processes.

### Operational advantages (non-benchmark)

| Property | Neo4j | SparrowDB |
|----------|-------|-----------|
| Process management | Daemon required | None |
| Cold start | 5–30s | 27ms |
| Network hop per query | Yes (~1–5ms) | No |
| Multi-tenancy | Shared server | Per-process isolation |
| Data leaves application | Yes (Bolt TCP) | No |
| Serverless / Lambda compatible | No (JVM startup) | Yes |
| Docker required | Yes (or JVM install) | No |
| Per-agent isolated graph state | Complex | Trivial (open separate path) |
| Works offline / air-gapped | Requires local server | Yes, always |

---

## How to Update This Document

When a fix ships:
1. Move the relevant row from "Improvement Ideas" to the baseline table.
2. Record the measured before/after numbers from `cargo bench --bench snap_benchmarks`.
3. Update the README performance table if the numbers affect a published SNAP benchmark row.
4. Note the version and PR number.

When new benchmark data is available:
1. Run `cargo bench --bench snap_benchmarks` on main after merge.
2. Run on the SNAP Facebook dataset if the query pattern corresponds to a published row.
3. Update both this file and `README.md` benchmark table together in a single PR.
