# SparrowDB KMS Replacement Spec

**Version:** 1.0
**Date:** March 22, 2026
**Author:** Empirical — derived from live KMS workload testing against SparrowDB
**Status:** Draft — ready for implementation planning

---

## 1. Context

The KMS (Knowledge Management System) currently uses Neo4j Aura as its graph store.
The goal is to replace it with a local SparrowDB instance, eliminating the cloud dependency,
subscription cost, and round-trip latency.

This spec was written after running the full KMS production graph (377 nodes, 482 edges,
37 relationship types, 15 node labels) against the current SparrowDB build and observing
every failure point. It is not theoretical — every gap listed here produced a real error
or incorrect result during the migration attempt.

### KMS Graph Profile

| Metric | Value |
|--------|-------|
| Nodes | 377 |
| Relationships | 482 |
| Node labels | 15 (Knowledge, Person, Technology, Project, Organization, Service, Concept, Document, Event, System, QueryType, MemoryTier, ContextTrigger, ToolRoute, ResourceMap) |
| Relationship types | 37 (ABOUT, USES, IMPLEMENTED_BY, OWNS, WORKED_AT, PARENT_OF, EXPERT_IN, EXPOSES, RELATED_TO, MARRIED_TO, EXPERIENCED, DEPENDS_ON, FRIENDS_WITH, STORES_IN, SIBLING_OF, WORKS_ON, COLLABORATES_WITH, FOUNDED, CO_FOUNDED, STORED_IN, DEPLOYED_VIA, ROUTES_TO, UNCLE_OF, ATTENDED_EVENT, WORKS_AT, LONGTIME_FRIEND, ATTENDED, COUSIN_OF, FALLS_BACK_TO, HAS_DISTINCTION, ASSOCIATED_WITH, LEARNED_FROM, WORKED_WITH, PARTNER_OF, CONCEIVED, SUBSIDIARY_OF, ALSO_CHECK) |
| Median node degree | ~2.6 |
| Max node degree | 19 (Richard Yaker skill hub) |

### KMS Query Patterns (from production code at `~/Dev/KMSmcp`)

```cypher
-- Ingestion: create-or-update a knowledge node
MERGE (n:Knowledge {id: $id})
SET n.content = $content, n.type = $type, n.source = $source,
    n.confidence = $confidence, n.created_at = $ts, n.updated_at = $ts
RETURN n

-- Ingestion: create typed relationship between entities
MATCH (a {id: $id1}), (b {id: $id2})
MERGE (a)-[:$relType]->(b)

-- Search: fulltext across knowledge nodes
CALL db.index.fulltext.queryNodes('knowledge_search', $query)
YIELD node, score
RETURN node, score ORDER BY score DESC LIMIT $limit

-- Retrieval: ego network for a node
MATCH (n {id: $id})-[r]-(m)
RETURN n, type(r), labels(m), m

-- Retrieval: nodes by type + source filter
MATCH (n:Knowledge)
WHERE n.type = $type AND n.source = $source
RETURN n ORDER BY n.confidence DESC LIMIT $limit

-- Retrieval: multi-hop traversal
MATCH (a:Person {name: $name})-[*1..2]-(b)
RETURN DISTINCT b, labels(b) LIMIT 50

-- Analytics: count by label
MATCH (n:Knowledge) RETURN n.type as type, count(n) as cnt

-- Dedup check: does this node exist?
MATCH (n:Knowledge {id: $id}) RETURN n LIMIT 1
```

---

## 2. Current State — What Works Today

These patterns were confirmed working in the current SparrowDB build during the migration run:

| Pattern | Status |
|---------|--------|
| `CREATE (n:Label {prop: "val"})` | ✅ Works |
| `MATCH (n:Label) RETURN n.prop` | ✅ Works |
| `MATCH (n:Label {prop: "val"}) RETURN n.prop` | ✅ Works |
| `MATCH (a:L1)-[:TYPE]->(b:L2) RETURN a.prop, b.prop` (both labeled) | ✅ Works |
| `MERGE (n:Label {prop: "val"})` | ✅ Phase 7, works |
| `SET n.prop = "val"` | ✅ Phase 7, works |
| `DELETE n` | ✅ Phase 7, works |
| `CREATE (a)-[:TYPE]->(b)` after MATCH | ✅ Works |
| WAL crash recovery, CHECKPOINT, OPTIMIZE | ✅ Phases 1–6 |
| SWMR snapshot isolation | ✅ Phase 5 |
| Encryption at rest | ✅ Phase 6 |
| Node bindings (sparrowdb-node .node file) | ✅ Builds and loads |
| MCP server (sparrowdb-mcp) | ✅ Builds |

---

## 3. Gaps — Blocking the KMS Replacement

Listed in priority order. Each entry maps to a filed Linear ticket.

---

### GAP-1: Relationship type names not persisted across sessions (CRITICAL)
**Linear:** SPA-191

**Symptom:** After checkpoint + reopen, all relationship type queries fail with
`invalid argument: unknown relationship type: KNOWS`. The edge data IS physically
stored in `base.fwd.csr` — only the catalog name→ID mapping is missing from `catalog.tlv`.

**KMS impact:** Every relationship query fails on server restart. The KMS server
starts cold and must be able to query all 37 relationship types immediately.

**Fix:** `checkpoint()` must flush the relationship type name→ID registry to `catalog.tlv`
alongside node label names, which already persist correctly.

**Verification test:**
```rust
let db = Engine::open("test.db")?;
db.execute("CREATE (a:Person {name: 'Alice'})")?;
db.execute("CREATE (b:Person {name: 'Bob'})")?;
db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")?;
db.checkpoint()?;
drop(db);

let db2 = Engine::open("test.db")?;
let r = db2.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")?;
assert_eq!(r.rows.len(), 1);  // must pass
```

---

### GAP-2: One-sided unlabeled relationship endpoint returns "not found" (CRITICAL)
**Linear:** SPA-198

**Symptom:** `MATCH (a:Person)-[:ABOUT]->(b) RETURN a.name, b.content` fails.
Both endpoints must have explicit labels or the query fails. Only `(a:L1)-[:TYPE]->(b:L2)`
with both labeled works.

**KMS impact:** The KMS ingestion pipeline creates edges between nodes of heterogeneous
label types. Retrieval queries like `MATCH (n {id: $id})-[r]-(m) RETURN m` — which are
the core ego-network query — require unlabeled endpoints.

**Fix:** The CSR lookup and join planner must support a "wildcard" label scan when no
label is specified on an endpoint. This is equivalent to scanning all label slots for
that endpoint position.

**Verification test:**
```cypher
MATCH (a:Person)-[:ABOUT]->(b) RETURN a.name, b.name     -- b has any label
MATCH (a)-[:ABOUT]->(b:Technology) RETURN a.name, b.name -- a has any label
```

---

### GAP-3: Undirected relationship pattern not implemented (HIGH)
**Linear:** SPA-193

**Symptom:** `MATCH (a:Person)-[:KNOWS]-(b:Person)` throws `not yet implemented`.
Only directed `->` patterns work.

**KMS impact:** The primary ego-network query is undirected — it retrieves all neighbors
regardless of edge direction. Without this, the KMS must run two queries per traversal
(outgoing + incoming) and merge results in application code, doubling query load.

**Fix:** According to the v3 spec (Section 2.11), undirected syntax creates two directed
physical edges at write time. If that invariant is already implemented, undirected MATCH
just needs to query both CSR directions. If not, the planner needs a backward-scan path.

**Verification test:**
```cypher
MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(b:Person) RETURN b.name
-- must return both nodes Alice KNOWS and nodes that KNOW Alice
```

---

### GAP-4: `MATCH (n) RETURN n` without label fails (HIGH)
**Linear:** SPA-192

**Symptom:** Unbounded node scan `MATCH (n) RETURN n` throws `not found`.
Label-qualified scans `MATCH (n:Label) RETURN n` work fine.

**KMS impact:** Graph export, full-database analytics, and admin tooling all need
label-agnostic scans. The KMS import bridge (Phase 9 of SparrowDB roadmap) will
need this for Neo4j → SparrowDB data migration.

**Fix:** Implement a sequential scan across all label slots in the node store, yielding
NodeRef values for each node regardless of label.

---

### GAP-5: Fulltext search index (HIGH — KMS-specific)
**Linear:** (not yet filed — new requirement)

**Symptom:** Neo4j's `CALL db.index.fulltext.queryNodes('knowledge_search', $query)`
is the KMS's primary search mechanism. SparrowDB has no equivalent.

**KMS impact:** The KMS MCP `unified_search` tool routes queries to Neo4j fulltext search
for Knowledge, Person, Organization, Project, Technology, Concept, Service, and Event nodes.
Without fulltext search, the KMS cannot find nodes by content substring or keyword — which
is its core function.

**Required behavior:**
```cypher
-- Create a fulltext index
CALL db.index.fulltext.create('knowledge_search',
  ['Knowledge', 'Person', 'Organization', 'Project', 'Technology', 'Concept', 'Service', 'Event'],
  ['content', 'name', 'type'])

-- Query it
CALL db.index.fulltext.queryNodes('knowledge_search', 'TypeScript')
YIELD node, score
RETURN node.name, node.content, score
ORDER BY score DESC
LIMIT 20
```

**Implementation options (in priority order):**
1. **Embedded Tantivy** — Rust fulltext search library, battle-tested, BM25 scoring.
   Index is stored alongside the graph files (e.g., `fulltext/` subdirectory).
   Most complete option; ~2 weeks implementation.
2. **Trigram index** — store 3-gram sets per string property, query by intersection.
   Simpler, no external dep, supports `CONTAINS` and prefix search. Lower recall than BM25.
3. **Property scan + CONTAINS predicate** — no index, linear scan with substring match.
   Acceptable for ≤10K nodes; KMS is 377 nodes today. Good enough for v1 KMS replacement.
   Implement as `WHERE n.content CONTAINS $query`.

**Recommendation:** Ship option 3 (`CONTAINS` predicate) immediately to unblock KMS.
Plan option 1 (Tantivy) as a follow-on for when the graph grows past ~10K nodes.

---

### GAP-6: `count()` aggregation function (HIGH)
**Linear:** SPA-194

**Symptom:** `MATCH (n:Knowledge) RETURN count(n) as cnt` throws `not found`.

**KMS impact:** The KMS analytics endpoint reports knowledge counts by type and source.
These are used for monitoring and routing decisions.

**Fix:** Implement `count(expr)` and `count(*)` as aggregating operators in the
execution engine. This is part of the Phase 8 "function library" work.

**Minimum required aggregations for KMS:**
- `count(n)` / `count(*)`
- `count(DISTINCT n.prop)`

---

### GAP-7: `type(r)` function on relationship variable (HIGH)
**Linear:** SPA-195

**Symptom:** `MATCH (a)-[r:KNOWS]->(b) RETURN type(r)` throws
`invalid argument: unknown relationship type: r`.

**KMS impact:** The KMS relationship retrieval query returns `type(r)` to let the
application understand the semantic meaning of each edge (WORKED_AT vs EXPERT_IN vs
OWNS, etc.). Without this, the KMS loses relationship type information in query results.

**Fix:** The `type()` function needs to resolve the bound relationship variable `r` to
its registered type name from the catalog, not interpret `r` as a literal type name.

---

### GAP-8: `id(n)` always returns null (MEDIUM)
**Linear:** SPA-196

**Symptom:** `MATCH (n:Person) RETURN id(n)` always returns null.

**KMS impact:** The KMS uses Neo4j's internal node IDs for caching and deduplication
in application code. Workaround is to store an explicit `id` property on every node
(which the KMS already does via `MERGE (n:Knowledge {id: $id})`), so this is lower
priority if property-based lookup works.

**Fix:** Return the packed node ID (label_slot | row_offset) as a string from `id(n)`.

---

### GAP-9: Missing property returns `0` instead of `null` (MEDIUM)
**Linear:** SPA-197

**Symptom:** Querying a string property that was never set on a node returns integer `0`
instead of `null`.

**KMS impact:** The KMS checks for property existence before rendering. JavaScript
`if (node.name)` is truthy for `"Alice"` and falsy for `null`, but evaluates `0` as
falsy — which is accidental correctness. However, `node.name === null` checks (used in
the KMS routing logic) would incorrectly pass `0`, corrupting routing decisions.

**Fix:** The nullable column storage should return JSON `null` (Rust `Value::Null`) for
unset string columns, not the integer default `0`.

---

### GAP-10: Parameterized queries (MEDIUM — not yet tested)
**Linear:** (not yet filed)

**Symptom:** Unknown. Neo4j driver uses `$param` syntax:
```js
session.run('MATCH (n:Knowledge {id: $id}) RETURN n', { id: 'abc-123' })
```

**KMS impact:** All KMS queries use parameters to prevent injection and enable
query plan caching. If SparrowDB only supports literal string interpolation, the
KMS MCP server would need to sanitize and interpolate all values itself — a security risk.

**Required:** `$name` parameter binding in Cypher, with values passed separately from
the query string. The Node.js binding API should accept:
```js
db.execute('MATCH (n:Knowledge {id: $id}) RETURN n', { id: 'abc-123' })
```

---

### GAP-11: `WHERE n.prop CONTAINS $substring` predicate (MEDIUM)
**Linear:** (not yet filed)

**Symptom:** Not tested, but required to implement fulltext search fallback (GAP-5 option 3).

**KMS impact:** Until a proper fulltext index exists, `CONTAINS` is the search primitive.

**Required syntax:**
```cypher
MATCH (n:Knowledge)
WHERE n.content CONTAINS $query OR n.name CONTAINS $query
RETURN n ORDER BY n.confidence DESC LIMIT 20
```

Also needed: `STARTS WITH`, `ENDS WITH`, case-insensitive variants.

---

### GAP-12: `labels(n)` function (MEDIUM)
**Linear:** (not yet filed)

**Symptom:** `MATCH (n:Person) RETURN labels(n)` — not tested, but required.

**KMS impact:** The KMS ego-network query returns `labels(m)` for each neighbor to
let the application render the correct node type in the UI and route to the right
storage system for property enrichment.

---

### GAP-13: `ORDER BY`, `SKIP`, `LIMIT` pipeline (LOW — partially works)
**Linear:** (not yet filed)

**Symptom:** `LIMIT` works. `ORDER BY` and `SKIP` status unknown — Phase 8 includes
"spill-to-disk ORDER BY" which implies basic ORDER BY should exist.

**KMS impact:** Ranked search results require `ORDER BY score DESC`. Pagination
requires `SKIP $offset LIMIT $pageSize`.

**Required:**
```cypher
MATCH (n:Knowledge) RETURN n ORDER BY n.confidence DESC LIMIT 20
MATCH (n:Knowledge) RETURN n ORDER BY n.confidence DESC SKIP 20 LIMIT 20
```

---

### GAP-14: `OPTIONAL MATCH` (LOW)
**Linear:** (not yet filed)

**Symptom:** Not tested.

**KMS impact:** Some KMS queries need to return a node even when a related node
doesn't exist (nullable join). Example:
```cypher
MATCH (n:Knowledge {id: $id})
OPTIONAL MATCH (n)-[:ABOUT]->(entity)
RETURN n, entity
```

---

### GAP-15: Per-transaction Cypher execution (LOW)
**Linear:** (SPA-99 already exists per the type annotations)

**Symptom:** `WriteTx.execute()` always throws "not yet implemented". Mutations go
through `SparrowDB.execute()` in auto-committed transactions only.

**KMS impact:** The KMS ingestion pipeline is not currently transactional at the
application level — it does one MERGE per node, one CREATE per edge. Auto-commit
is sufficient for the current KMS. Explicit transactions would be needed if the KMS
ever implements atomic "store node + all its relationships or nothing" semantics.

---

## 4. Implementation Priority Order

To replace Neo4j in the KMS, the gaps must be resolved in this order:

### Phase A — Must have before KMS can boot on SparrowDB

| Gap | Effort (est.) | Blocker? |
|-----|---------------|----------|
| GAP-1: Rel type catalog persistence | S (1–2 days) | ✅ Yes |
| GAP-2: One-sided unlabeled endpoint | M (3–5 days) | ✅ Yes |
| GAP-3: Undirected pattern | M (3–5 days) | ✅ Yes |
| GAP-9: Null vs 0 for missing string | S (1 day) | ✅ Yes |

### Phase B — Must have for KMS search to work

| Gap | Effort (est.) | Blocker? |
|-----|---------------|----------|
| GAP-5: CONTAINS predicate (fulltext fallback) | S (2–3 days) | ✅ Yes |
| GAP-7: type(r) function | S (1–2 days) | ✅ Yes |
| GAP-6: count() aggregation | M (3–5 days) | ✅ Yes |
| GAP-12: labels(n) function | S (1 day) | ✅ Yes |

### Phase C — Needed for production quality

| Gap | Effort (est.) | Blocker? |
|-----|---------------|----------|
| GAP-10: Parameterized queries | M (3–5 days) | 🔶 Security |
| GAP-4: MATCH (n) without label | M (3–5 days) | 🔶 Admin |
| GAP-8: id(n) returns value | S (1 day) | 🔶 Nice to have |
| GAP-11: ORDER BY / SKIP | S (already partial) | 🔶 UX |
| GAP-13: OPTIONAL MATCH | M (3–5 days) | 🔶 Future |

### Phase D — Future / nice to have

| Gap | Effort (est.) | Notes |
|-----|---------------|-------|
| GAP-5 option 1: Tantivy fulltext | L (2–3 weeks) | When graph > 10K nodes |
| GAP-14: Per-transaction Cypher | L (1–2 weeks) | Already tracked SPA-99 |

---

## 5. KMS-Specific Test Suite

Before declaring "SparrowDB replaces Neo4j in KMS," all of the following must pass
against a SparrowDB instance loaded with the production graph dump:

```
✅ Import: 377 nodes and 482 edges load without error
✅ Persist: after checkpoint + reopen, all relationship types queryable
✅ Ingest: MERGE (n:Knowledge {id: $id}) SET n.content = $content works
✅ Link: MERGE (a)-[:ABOUT]->(b) between any label combination works
✅ Search: WHERE n.content CONTAINS 'TypeScript' returns relevant nodes
✅ Ego-network: (n {id: $id})-[r]-(m) returns all neighbors both directions
✅ Type-aware: type(r) returns correct string for each relationship
✅ Labels: labels(m) returns correct label array for each neighbor
✅ Count: count(n) returns integer node count
✅ Order: ORDER BY n.confidence DESC LIMIT 20 returns sorted results
✅ Filter: WHERE n.type = 'fact' AND n.source = 'technical' filters correctly
✅ MCP: sparrowdb-mcp server accepts and responds to all KMS query patterns
✅ Node binding: Node.js SparrowDB class handles all patterns correctly
✅ Concurrent read: multiple simultaneous reads don't block each other
✅ Write-safety: concurrent writes are serialized without data loss
```

---

## 6. Architecture Notes for KMS Integration

### Drop-in replacement path

The KMS Neo4j calls are concentrated in one module (the graph storage layer in
`~/Dev/KMSmcp/src/`). The replacement strategy:

1. Implement a `SparrowDBAdapter` that speaks the same internal interface as the
   current `Neo4jAdapter`
2. Use the same Cypher queries where SparrowDB syntax is compatible
3. Adapt the fulltext search call to use `WHERE n.content CONTAINS $query` initially,
   with a Tantivy upgrade path
4. The KMS MCP server (`kms.yaker.org`) would run locally with SparrowDB embedded —
   no network hop to Neo4j Aura

### Data migration

The migration script is already built: `/Volumes/Dev/localDev/SparrowTest/kms-visualizer/import_graph.js`.
It imports from the `kms_graph.json` export (produced by direct Neo4j query).
Once GAP-1 (catalog persistence) is fixed, the imported data survives server restarts
and the migration is complete.

### Connection model change

Neo4j Aura: TCP bolt connection, ~50–100ms round trip, cloud availability dependency.
SparrowDB: in-process call, sub-millisecond, always available.

The KMS performance would improve from ~100ms per graph query to <1ms with SparrowDB
embedded in the same process.

---

## 7. Out of Scope for KMS Replacement

These Neo4j features are used by other applications but NOT by the KMS, so they are
not required for the KMS replacement target:

- LDBC SNB benchmark compatibility (Phase 9 of SparrowDB roadmap)
- Causal clustering / replication
- APOC procedures
- GDS (Graph Data Science) library
- Bloom visualization
- Aura console / browser query interface (replaced by the SparrowDB visualizer built
  in this session: `kms-visualizer/`)
