# Performance Roadmap

This document records known bottlenecks, measured improvements, and
prioritised ideas for closing the gap with server-based graph databases.
It is the authoritative source for performance work — update it when a
fix ships or when new data invalidates an assumption.

---

## Target Use Case

SparrowDB is an **embedded** database — the SQLite of graph databases. The
performance bar is set by that workload, not by server databases:

- Graphs: thousands to low millions of nodes
- Queries: point lookups, 1-hop traversals, shallow multi-hop, mutual friends
- Concurrency: single process, one query at a time
- What matters: cold start, zero infrastructure, correctness, query cache hit rate

Benchmarks vs Neo4j on deep traversal of large social graphs (Q3/Q4/Q5) are
informative but **not the primary target**. A 50ms traversal that requires
no server, no JVM, and no network hop is still the right answer for the
embedded use case.

---

## Measured Baselines (v0.1.15, SNAP Facebook dataset — 4,039 nodes / 88,234 edges)

| Query | SparrowDB | Neo4j | Notes |
|-------|-----------|-------|-------|
| Q1 Point Lookup (indexed) | 103µs | 321µs | 3× faster |
| Q3 1-Hop Traversal | 55.5ms | 0.63ms | traversal gap; acceptable for embedded |
| Q4 2-Hop DISTINCT | 613ms | 0.38ms | traversal gap |
| Q5 Variable Path 1..3 | 110ms | 0.50ms | traversal gap |
| Q6 Global COUNT(*) | 2.2µs | 202µs | 93× faster |
| Q7 Top-10 by Degree | 401µs | 17,588µs | 44× faster |
| Q8 Mutual Friends | 0.67ms | 0.35ms | ~2× faster |

Synthetic 1K-node baseline in `crates/sparrowdb/benches/snap_benchmarks.rs`.
SNAP numbers measured with `SNAP_DATASET=.../facebook_combined.txt cargo bench --bench snap_benchmarks`.

---

## Correctness First

Before any performance work, the open correctness bugs take priority:

| Issue | Description |
|-------|-------------|
| #364 | Bare variable projection returns Null (`RETURN n`) |
| #363 | CASE WHEN expression returns Null |
| #355 | WITH…MATCH property lookup returns 0 rows |
| #354 | Variable-length path with terminal label filter returns 0 rows |

---

## Embedded-Relevant Performance Work

These improvements target the actual embedded use case.

### 1. Query plan cache

**Affects:** All repeated queries — the dominant pattern for AI agents
**Effort:** 0.5 days
**Expected gain:** Eliminates re-planning for agents that issue the same
query thousands of times per session

Cache `query_string → ChunkedPlan + bound_ast` in a `HashMap` on `GraphDb`.
Key on the raw query string. Invalidate on schema change (label/rel-type creation).

### 2. LIMIT pushdown into traversals

**Affects:** Any `MATCH ... RETURN ... LIMIT k` query
**Effort:** 0.5 days
**Expected gain:** Near-instant TOP-N queries regardless of graph size;
agents frequently want the first few results, not the full traversal

LIMIT is already pushed into node scans. Extend it to 2-hop and
variable-length traversals: stop collecting rows as soon as `k` are found.

### 3. SlotDedup operator (DISTINCT in chunked pipeline)

**Affects:** Q4 DISTINCT 2-hop and all other DISTINCT traversals
**Effort:** 1 day
**Expected gain:** Keeps DISTINCT queries in the fast chunked pipeline;
currently falls back to row-at-a-time

Add a `SlotDedup` pipeline operator backed by a Roaring bitmap that filters
duplicates during chunk production, replacing the post-hoc `deduplicate_rows` call.

```
ChunkedPlan:  ScanByLabel → GetNeighbors → GetNeighbors → SlotDedup → ReadNodeProps
```

---

## Future: Cloud / Server Version

If SparrowDB ever grows a server mode or targets large-scale graph workloads,
the following become relevant. Not planned for the embedded product.

| Idea | Gain | Prerequisite |
|------|------|-------------|
| Rayon parallel source iteration | 4–8× traversal on multi-core | Engine must be `Sync`; `RefCell<PropertyIndex>` needs `Arc` refactor |
| Pre-sorted CSR adjacency lists (merge-join intersection) | O(M+N) SlotIntersect for dense graphs | Storage format change |
| Vectorised WHERE on column vectors | SIMD predicate evaluation | Arrow-compatible column layout |
| Bidirectional BFS for `shortestPath` | Exponentially fewer hops on high-fanout | BFS engine rewrite |
| Column-oriented storage (Arrow layout) | Batch decode, SIMD, compression | Full storage rewrite |
| Explicit LRU buffer pool | Predictable memory under pressure | Replaces OS page cache reliance |
| Parallel WAL group commit | Higher write throughput | WAL protocol change |
| Adaptive join ordering | Better plans on skewed graphs | Cardinality statistics infrastructure |

---

## Embedded vs Server — When SparrowDB Wins Regardless of Traversal Speed

A typical AI agent session making 50 graph queries:

```
Neo4j (cloud/Docker)             SparrowDB (embedded)
─────────────────────────────────────────────────────
Open:    ~5,000ms (server start) Open:    ~27ms (file open)
Query ×50: traversal + 2ms RTT   Query ×50: traversal + 0ms RTT

RTT tax: 50 × 2ms = 100ms        RTT tax: 0ms
```

Even at 68× slower traversal (Q3: 55ms vs 0.6ms), the embedded model wins
on total session latency for short-lived processes once server cold-start
and per-query round-trips are included.

| Property | Neo4j | SparrowDB |
|----------|-------|-----------|
| Process management | Daemon required | None |
| Cold start | 5–30s | 27ms |
| Network hop per query | Yes (~1–5ms) | No |
| Per-agent isolated graph state | Complex | Trivial (open separate path) |
| Data leaves application | Yes (Bolt TCP) | No |
| Serverless / Lambda compatible | No (JVM startup) | Yes |
| Works offline / air-gapped | Requires local server | Yes, always |

---

## How to Update This Document

When a fix ships:
1. Move the relevant row into the baseline table with measured before/after numbers.
2. Update the README performance table if a published SNAP row changes.
3. Note the version and PR number.

When new benchmark data is available:
1. Run `SNAP_DATASET=.../facebook_combined.txt cargo bench --bench snap_benchmarks`.
2. Update both this file and `README.md` together in a single commit.
