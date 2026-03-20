# SparrowDB — Real-World Use Cases

These are the concrete scenarios that define "does this actually work."
Every use case has:
- A dataset (real or synthetic with real structure)
- Specific Cypher queries that must execute correctly
- A performance target (latency at scale)
- An integration test that encodes the scenario

---

## UC-1: Social Graph — Friend-of-Friend Recommendations

**Motivation:** The canonical graph database workload. Validates traversal,
factorized execution (no Cartesian blowup), and 2-hop joins.

**Dataset:** 10,000 Person nodes, 50,000 KNOWS edges (average degree 10),
synthetic but with realistic degree distribution (power law).

**Queries:**
```cypher
// Who are Alice's friends?
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(f:Person)
RETURN f.name

// Friends-of-friends (2-hop)
MATCH (a:Person {name: "Alice"})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
WHERE NOT (a)-[:KNOWS]->(fof)
RETURN DISTINCT fof.name

// Mutual friends between Alice and Bob
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(m:Person)<-[:KNOWS]-(b:Person {name: "Bob"})
RETURN m.name
```

**Performance targets:**
- 1-hop: < 1ms at 10k nodes
- 2-hop: < 10ms at 10k nodes (factorized, not materialized)
- No OOM on 100k node graph with 2-hop query

**Integration test:** `tests/integration/uc1_social_graph.rs`

---

## UC-2: Dependency Graph — Transitive Closure

**Motivation:** Package manager / build system workload. Validates DAG
traversal, cycle detection (the graph must handle it gracefully), and
correctness on deep chains.

**Dataset:** 500 Package nodes, 2,000 DEPENDS_ON edges, structured like
a real npm/cargo dependency tree with ~6 levels deep.

**Queries:**
```cypher
// Direct dependencies of "sparrowdb"
MATCH (p:Package {name: "sparrowdb"})-[:DEPENDS_ON]->(d:Package)
RETURN d.name, d.version

// All transitive dependencies (2 hops — proxy for reachability)
MATCH (p:Package {name: "sparrowdb"})-[:DEPENDS_ON*1..3]->(t:Package)
RETURN DISTINCT t.name

// Which packages depend on "tokio"?
MATCH (p:Package)-[:DEPENDS_ON]->(t:Package {name: "tokio"})
RETURN p.name
```

**Integration test:** `tests/integration/uc2_dependency_graph.rs`

---

## UC-3: KMS — Replace Neo4j Aura with SparrowDB (PRIMARY TARGET)

**Motivation:** This is the first real production deployment. The Personal
Knowledge Management System (KMS) at `~/Dev/KMSmcp` currently uses Neo4j
Aura (cloud) as its graph store. SparrowDB replaces it — same Cypher, local
on Mac Mini, zero cloud latency, no subscription cost.

**Current Neo4j schema in KMS:**
```
Node labels: Knowledge | Person | Organization | Project |
             Technology | Concept | Service | Event
Relationships: ABOUT, MENTIONS, RELATED_TO (typed, created by EntityLinker)
Fulltext index: 'knowledge_search' on all node types (name, content fields)
```

**Real queries KMS runs today (from Neo4jStorage.ts):**
```cypher
// Store a knowledge node
CREATE (k:Knowledge {
  id: $id,
  content: $content,
  contentType: $contentType,
  source: $source,
  confidence: $confidence,
  timestamp: $timestamp
})

// Search by property match (replaces fulltext for v0.1)
MATCH (k:Knowledge)
WHERE k.content CONTAINS $query OR k.source = $source
RETURN k ORDER BY k.confidence DESC LIMIT 20

// Entity relationship traversal (EntityLinker output)
MATCH (k:Knowledge)-[:ABOUT]->(t:Technology {name: $tech})
RETURN k.content, k.confidence, k.timestamp
ORDER BY k.confidence DESC

// 2-hop: what knowledge is related to this project?
MATCH (p:Project {name: $project})<-[:ABOUT]-(k:Knowledge)-[:MENTIONS]->(t:Technology)
RETURN k.content, t.name

// Get all knowledge for a session
MATCH (k:Knowledge)
WHERE k.timestamp > $since
RETURN k ORDER BY k.timestamp DESC
```

**Migration path:**
1. `src/storage/SparrowDBStorage.ts` replaces `Neo4jStorage.ts` (same interface)
2. Router updated: `neo4j` → `sparrowdb` in routing config
3. Existing data migrated via export/import script
4. KMS runs with zero downtime (Mem0 + MongoDB unchanged)

**Gap — fulltext index:** KMS uses `knowledge_search` fulltext index.
SparrowDB v0.1 does not have fulltext search. Mitigation for v0.1:
- Property `CONTAINS` matching (already in Cypher spec)
- Mem0 continues to handle semantic search
- Fulltext is tracked as a post-v0.1 enhancement

**Integration test:** `tests/integration/uc3_kms_storage.rs`

**Dataset:** Snapshot of real KMS data (anonymized) — ~2,000 Knowledge nodes,
~500 entity nodes, ~5,000 typed relationships. Generated from a KMS export.

---

## UC-4: Durability — Crash and Recovery

**Motivation:** Proves the database is actually durable, not just fast.
Validates WAL replay, torn-page detection, and dual-metapage recovery.

**Scenario:**
1. Write 1,000 nodes across 10 transactions
2. Inject crash at each of the 8 WAL failpoints
3. Reopen DB
4. Assert: all committed transactions visible, no partial writes

**This is not a benchmark — it is a correctness gate.**
All 8 crash scenarios must pass before any Phase 2 code ships.

**Integration test:** `tests/integration/uc4_crash_recovery.rs`
(maps directly to acceptance checks #5, #6, #7, #10)

---

## UC-5: Encryption at Rest — Confidential Graph

**Motivation:** Embedded use case where the graph contains sensitive data
(medical records, financial relationships). Validates encryption round-trip
and wrong-key rejection.

**Scenario:**
1. Create encrypted DB (32-byte key)
2. Write a social graph (UC-1 dataset)
3. Run all UC-1 queries — results must match unencrypted version exactly
4. Attempt to open with wrong key → `Error::DecryptionFailed`
5. Verify file bytes are not plaintext (no node names visible in hex dump)

**Integration test:** `tests/integration/uc5_encryption.rs`
(maps to acceptance check #13)

---

## UC-6: Python — Embedded Graph for AI Pipelines

**Motivation:** The target embedding use case. A Python script builds a
knowledge graph, queries it, and uses results in an LLM pipeline.

**Scenario (pytest):**
```python
import sparrowdb

with sparrowdb.open("/tmp/ai-graph.db") as db:
    # Build knowledge graph
    db.query('CREATE (:Concept {name: "neural network", domain: "AI"})')
    db.query('CREATE (:Concept {name: "backpropagation", domain: "AI"})')
    db.query('''
        MATCH (a:Concept {name: "neural network"}),
              (b:Concept {name: "backpropagation"})
        CREATE (a)-[:REQUIRES]->(b)
    ''')

    # Query for LLM context
    result = db.query('''
        MATCH (c:Concept {domain: "AI"})-[:REQUIRES]->(r:Concept)
        RETURN c.name, r.name
    ''')
    for row in result:
        print(f"{row['c.name']} requires {row['r.name']}")
```

**Integration test:** `tests/python/test_uc6_ai_pipeline.py`
(maps to acceptance check #14)

---

## Performance Targets Summary

| Use Case | Dataset | Query | Target Latency |
|----------|---------|-------|---------------|
| UC-1 1-hop | 10k nodes / 50k edges | friend lookup | < 1ms |
| UC-1 2-hop | 10k nodes / 50k edges | friend-of-friend | < 10ms |
| UC-1 2-hop | 100k nodes / 500k edges | friend-of-friend | < 100ms |
| UC-2 transitive | 500 nodes / 2k edges | dep chain 3-hop | < 5ms |
| UC-3 strength filter | 1k nodes / 3k edges | edge property scan | < 2ms |
| WAL append | — | single commit | < 500µs |
| Page read (encrypted) | — | single page decrypt | < 50µs |
| CHECKPOINT | 10k nodes merged | fold base+delta | < 1s |

These targets are encoded in `benches/` and measured on every PR.

---

## Test Dataset Generation

Datasets are generated deterministically (seeded RNG) so tests are
reproducible. Generator lives in `tests/fixtures/gen/`:

```bash
cargo run --bin gen-fixtures -- --seed 42 --out tests/fixtures/
```

Generates:
- `social_10k.json` — UC-1 dataset (10k persons, 50k edges)
- `social_100k.json` — UC-1 scale test
- `deps_500.json` — UC-2 dataset
- `concepts_1k.json` — UC-3 dataset

Fixtures are committed (small JSON, ~2MB total). The generator is for
regeneration and scaling experiments only.
