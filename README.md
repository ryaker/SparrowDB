<p align="center">
  <img src="docs/logo.png" alt="SparrowDB" width="260" />
</p>

<h1 align="center">SparrowDB</h1>

<p align="center"><strong>The SQLite of graph databases. Embedded, Cypher-native, zero infrastructure.</strong></p>

<p align="center">
  <a href="https://github.com/ryaker/SparrowDB/actions"><img src="https://github.com/ryaker/SparrowDB/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://crates.io/crates/sparrowdb"><img src="https://img.shields.io/crates/v/sparrowdb.svg" alt="crates.io" /></a>
  <a href="https://docs.rs/sparrowdb"><img src="https://docs.rs/sparrowdb/badge.svg" alt="docs.rs" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
  <img src="https://img.shields.io/badge/status-pre--1.0%20%7C%20building%20in%20public-orange.svg" alt="Status" />
  <img src="https://img.shields.io/badge/bindings-Python%20%7C%20Node.js%20%7C%20Ruby-blue.svg" alt="Bindings" />
  <a href="https://deepwiki.com/ryaker/SparrowDB"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki" /></a>
</p>

---

**SparrowDB is an embedded graph database.** It links directly into your process — Rust, Python, Node.js, or Ruby — and gives you a real Cypher query interface backed by a WAL-durable store on disk. No server. No JVM. No cloud subscription. No daemon to babysit.

If your data is fundamentally relational — recommendations, social graphs, dependency trees, fraud rings, knowledge graphs — and you want to query it with multi-hop traversals instead of JOIN chains, SparrowDB is the drop-in answer.

---

## Quick Start

```rust,no_run
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    let db = GraphDb::open(std::path::Path::new("social.db"))?;

    db.execute("CREATE (alice:Person {name: 'Alice', age: 30})")?;
    db.execute("CREATE (bob:Person   {name: 'Bob',   age: 25})")?;
    db.execute("MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")?;

    // Who does Alice know? Who do *they* know?
    let fof = db.execute("MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(f) RETURN DISTINCT f.name")?;
    // -> [["Bob"], ["Carol"]]  (Carol is a friend-of-friend)
    let _ = fof;
    Ok(())
}
```

That's it. The database is a directory on disk. Ship it.

---

## Performance: Faster Than Neo4j Where It Counts

Benchmarked against Neo4j 5.x on the SNAP Facebook dataset (4,039 nodes, 88,234 edges). All figures are p50 latency, v0.1.15.

| Query | SparrowDB | Neo4j | vs Neo4j |
|-------|-----------|-------|---------|
| **Point Lookup (indexed)** | **103µs** | 321µs | **3x faster** |
| **Global COUNT(\*)** | **2.2µs** | 202µs | **93x faster** |
| **Top-10 by Degree** | **401µs** | 17,588µs | **44x faster** |
| **Mutual Friends (Q8)** | **0.72ms** | 352µs | **2x faster** |

**Point lookups, aggregations, and mutual-neighbor queries beat a running Neo4j server — with no JVM, no server process, no network hop.**

Q8 (mutual friends) dropped from 153ms → **0.72ms** (−99.5%) in v0.1.15 via SlotIntersect + BfsArena + bitvector optimizations. Multi-hop traversal numbers (Q3/Q4/Q5) are being re-baselined with consistent query shapes — see [Performance](#performance-characteristics).

**Cold start: ~27ms** — viable for serverless and short-lived processes where Neo4j's server startup is disqualifying.

---

## Built for AI Agents and MCP

SparrowDB ships with a first-class **MCP server** (`sparrowdb-mcp`) — the only embedded graph database with native MCP support. It speaks JSON-RPC 2.0 over stdio and plugs directly into Claude Desktop and any MCP-compatible AI client.

```bash
cargo install sparrowdb --bin sparrowdb-mcp --locked
```

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sparrowdb": {
      "command": "/absolute/path/to/sparrowdb-mcp",
      "args": []
    }
  }
}
```

Your AI assistant can now query and write to your graph database using natural tool calls:

| Tool | Description |
|------|-------------|
| `execute_cypher` | Execute any Cypher statement; returns result rows |
| `create_entity` | Create a node with a label and properties |
| `add_property` | Set a property on nodes matching a filter |
| `checkpoint` | Flush WAL and compact |
| `info` | Database metadata |

Full setup: [docs/mcp-setup.md](docs/mcp-setup.md)

**Why this matters for agent builders:** Multi-agent systems need shared, persistent graph state. SparrowDB gives your agents a knowledge graph they can read and write without spinning up a server. Pair it with [SparrowOntology](https://github.com/ryaker/SparrowOntology) for schema-enforced agent memory and governance.

---

## Why SparrowDB

**The graph database landscape has a gap.**

Neo4j is powerful, but it requires a running server, a JVM, and a license the moment you need production features. DGraph is horizontally scalable, but you don't need horizontal scale — you need to ship your app. Every existing option assumes you want to operate a database cluster, not embed a graph engine.

SparrowDB fills the same role SQLite fills for relational data: *zero infrastructure, full capability, open source, MIT licensed.*

| Question | Answer |
|---|---|
| Does it need a server? | No. It's a library. |
| Does it need a cloud account? | No. It's a file on disk. |
| Can it survive `kill -9`? | Yes. WAL + crash recovery. |
| Can multiple threads read at once? | Yes. SWMR — readers never block writers. |
| Does the Python binding release the GIL? | Yes. Every call into the engine releases it. |
| Can I use it from an AI assistant? | Yes. Built-in MCP server. |

---

## When to Use SparrowDB

SparrowDB is the right choice when:

- **Your data has structure that's hard to flatten.** Social follows, product recommendations, dependency graphs, org charts, bill-of-materials, knowledge graphs — these are terrible in SQL and natural in graphs.
- **You're building an application, not operating a database.** You want to `cargo add sparrowdb` and ship, not provision instances.
- **You need multi-hop queries.** `MATCH (a)-[:FOLLOWS*1..3]->(b)` is one query. In SQL it's recursive CTEs all the way down.
- **You're embedding into a CLI, desktop app, agent, or edge service.** SparrowDB opens in milliseconds and has no runtime overhead when idle.

SparrowDB is *not* the right choice when:

- **Deep multi-hop traversal is your primary workload.** 1-hop to 5-hop queries on high-fanout graphs (social networks, web graphs) are where Neo4j's battle-hardened CSR layout and parallel execution show. SparrowDB is currently slower on anchor-based 1-hop and 2-hop traversals (Q3/Q4). That gap is actively narrowing — Q8 mutual friends is now 2x faster than Neo4j in v0.1.15 — but if deep traversal at scale is your core workload today, use Neo4j.
- **You need distributed writes across many nodes**, or your graph has billions of edges and requires horizontal sharding. Use Neo4j Aura or DGraph for that.

---

## Install

### Node.js

```bash
npm install sparrowdb
```

### Rust

```toml
[dependencies]
sparrowdb = "0.1"
```

### Python

```bash
# Build from source (requires Rust toolchain):
cd crates/sparrowdb-python && maturin develop
```

> PyPI package coming soon. Pre-built wheels are on the roadmap.

### Ruby

```bash
# Build from source (requires Rust toolchain):
cd crates/sparrowdb-ruby && bundle install && rake compile
```

> RubyGems package coming soon.

### CLI

```bash
cargo install sparrowdb --bin sparrowdb
```

### MCP Server (Claude Desktop integration)

```bash
cargo install sparrowdb --bin sparrowdb-mcp --locked
```

---

## Features

### Cypher Support

| Feature | Status |
|---------|--------|
| `CREATE`, `MATCH`, `SET`, `DELETE` | ✅ |
| `WHERE` — `=`, `<>`, `<`, `<=`, `>`, `>=` | ✅ |
| `WHERE n.prop CONTAINS str` / `STARTS WITH str` | ✅ |
| `WHERE n.prop IS NULL` / `IS NOT NULL` | ✅ |
| 1-hop and multi-hop edges `(a)-[:R]->()-[:R]->(c)` | ✅ |
| Undirected edges `(a)-[:R]-(b)` | ✅ |
| Reverse-arrow pattern `(a)->()<-(c)` | ✅ |
| Variable-length paths `[:R*1..N]` | ✅ |
| Multi-label nodes `(n:A:B)` | ✅ |
| `RETURN DISTINCT`, `ORDER BY`, `LIMIT`, `SKIP` | ✅ |
| `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)` | ✅ |
| `SUM`, `AVG`, `MIN`, `MAX` | ✅ |
| `collect()` — aggregate into list | ✅ |
| `coalesce(expr1, expr2, …)` — first non-null | ✅ |
| `WITH … WHERE` pipeline (filter mid-query) | ✅ |
| `WITH … MATCH` pipeline (chain traversals) | ✅ |
| `WITH … UNWIND` pipeline | ✅ |
| `UNWIND list AS var MATCH (n {id: var})` | ✅ |
| `OPTIONAL MATCH` | ✅ |
| `UNION` / `UNION ALL` | ✅ |
| `MERGE` — upsert node with `ON CREATE SET` / `ON MATCH SET` | ✅ |
| `MATCH (a),(b) MERGE (a)-[:R]->(b)` — idempotent edge | ✅ |
| `CREATE (a)-[:REL]->(b)` — directed edge | ✅ |
| `CASE WHEN … THEN … ELSE … END` | ✅ |
| `EXISTS { (n)-[:REL]->(:Label) }` | ✅ |
| `EXISTS` in `WITH … WHERE` | ✅ |
| `shortestPath((a)-[:R*]->(b))` | ✅ |
| `ANY` / `ALL` / `NONE` / `SINGLE` list predicates | ✅ |
| `id(n)`, `labels(n)`, `type(r)` | ✅ |
| `size()`, `range()`, `toInteger()`, `toString()` | ✅ |
| `toUpper()`, `toLower()`, `trim()`, `replace()`, `substring()` | ✅ |
| `abs()`, `ceil()`, `floor()`, `sqrt()`, `sign()` | ✅ |
| Parameters `$param` | ✅ |
| `CALL db.index.fulltext.queryNodes` — scored full-text search | ✅ |
| `CALL db.schema()` | ✅ |
| Subqueries `CALL { … }` | ⚠️ Partial |

### Engine & Storage

- **WAL durability** — write-ahead log with crash recovery; survives hard kills
- **SWMR concurrency** — single-writer, multiple-reader; readers never block writers
- **Chunked vectorized pipeline** — 4-phase chunked execution engine for multi-hop traversals; FrontierScratch arena eliminates per-hop allocation; SlotIntersect for mutual-neighbor queries
- **Factorized execution** — multi-hop traversals avoid materializing O(N²) intermediate rows
- **B-tree property index** — equality lookups in O(log n), not full label scans; persisted to disk
- **Inverted text index** — `CONTAINS` / `STARTS WITH` routed through an index
- **Full-text search** — relevance-scored `queryNodes` without Elasticsearch
- **External merge sort** — `ORDER BY` on large results spills to disk; no unbounded heap
- **At-rest encryption** — optional XChaCha20-Poly1305 per WAL entry; wrong key errors immediately, never silently decrypts garbage
- **`execute_batch()`** — multiple writes in one `fsync` for bulk-load throughput
- **Bulk CSV loader** — `sparrowdb bulk-import` ingests node and edge CSVs with batched WriteTx for high-throughput imports
- **`execute_with_timeout()`** — cancel runaway traversals without killing the process
- **`export_dot()`** — export any graph to Graphviz DOT for visualization
- **APOC CSV import** — migrate existing Neo4j graphs in one command
- **MVCC write-write conflict detection** — two writers on the same node: the second is aborted

### Language Bindings

| Language | Mechanism | Status |
|---|---|---|
| Rust | Native `GraphDb` API | ✅ Stable |
| Python | PyO3 — releases GIL, context manager | ✅ Stable |
| Node.js | napi-rs — `SparrowDB` class | ✅ Stable |
| Ruby | Magnus extension | ✅ Stable |

All bindings open the same on-disk format. A graph written from Python can be read by Node.js.

---

## Language Examples

### Rust

```rust,no_run
use sparrowdb::GraphDb;
use std::time::Duration;

fn main() -> sparrowdb::Result<()> {
    let db = GraphDb::open(std::path::Path::new("my.db"))?;

    // Bulk load — one fsync for all five writes
    db.execute_batch(&[
        "CREATE (alice:Person {name:'Alice', role:'engineer', score:9.1})",
        "CREATE (bob:Person   {name:'Bob',   role:'designer', score:7.5})",
        "CREATE (carol:Person {name:'Carol', role:'engineer', score:8.8})",
        "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'})   CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {name:'Bob'}),  (b:Person {name:'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    ])?;

    // Multi-hop: friend-of-friend
    let fof = db.execute(
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*2]->(f) RETURN f.name"
    )?;
    println!("{:?}", fof.rows); // [["Carol"]]

    // WITH pipeline: count edges, filter, continue
    let _prolific = db.execute(
        "MATCH (p:Person)-[:KNOWS]->(f)
         WITH p, COUNT(f) AS connections
         WHERE connections >= 1
         RETURN p.name, connections
         ORDER BY connections DESC"
    )?;

    // Upsert — creates on first call, updates on subsequent calls
    db.execute("MERGE (u:User {email:'alice@example.com'})
                ON CREATE SET u.created = '2024-01-01', u.logins = 0
                ON MATCH  SET u.logins  = u.logins + 1")?;

    // Cancel runaway traversal after 5 seconds
    let _ = db.execute_with_timeout(
        "MATCH (a)-[:FOLLOWS*1..10]->(b) RETURN b.name, count(*)",
        Duration::from_secs(5),
    );

    Ok(())
}
```

### Python

```python
import sparrowdb

# Context manager — database closes cleanly on exit; execute() releases the GIL
with sparrowdb.GraphDb("/path/to/my.db") as db:
    db.execute("CREATE (n:Product {id: 1, name: 'Widget',   price: 9.99})")
    db.execute("CREATE (n:Product {id: 2, name: 'Gadget',   price: 24.99})")
    db.execute("CREATE (n:Product {id: 3, name: 'Doohickey',price: 4.99})")
    db.execute(
        "MATCH (a:Product {id:1}),(b:Product {id:2}) CREATE (a)-[:RELATED]->(b)"
    )

    rows = db.execute(
        "MATCH (p:Product {name:'Widget'})-[:RELATED*1..2]->(r) "
        "RETURN DISTINCT r.name, r.price ORDER BY r.price"
    )
    print(rows)

# Thread-safe: GIL is released inside execute()
import concurrent.futures

def query_worker(limit):
    with sparrowdb.GraphDb("/path/to/my.db") as db:
        return db.execute(f"MATCH (n:Product) RETURN n.name LIMIT {limit}")

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(query_worker, [1, 2, 3]))
```

### Node.js / TypeScript

```typescript
import SparrowDB from 'sparrowdb';

const db = new SparrowDB('/path/to/my.db');

db.execute("CREATE (n:Article {id: 'a1', title: 'Graph Databases 101', tags: 'graphs,rust'})");
db.execute("CREATE (n:Article {id: 'a2', title: 'Cypher Query Language', tags: 'cypher,graphs'})");
db.execute("MATCH (a:Article {id:'a1'}),(b:Article {id:'a2'}) CREATE (a)-[:RELATED]->(b)");

// Full-text search
db.execute("CALL db.index.fulltext.createNodeIndex('articles', ['Article'], ['title', 'tags'])");
const results = db.execute(
  "CALL db.index.fulltext.queryNodes('articles', 'rust') " +
  "YIELD node, score RETURN node.title, score ORDER BY score DESC"
);

db.close();
```

### Ruby

```ruby
require 'sparrowdb'

db = SparrowDB::GraphDb.new('/path/to/my.db')

db.execute("CREATE (n:Dependency {name: 'tokio',  version: '1.35'})")
db.execute("CREATE (n:Dependency {name: 'serde',  version: '1.0'})")
db.execute("MATCH (a:Dependency {name:'tokio'}),(b:Dependency {name:'serde'}) CREATE (a)-[:DEPENDS_ON]->(b)")

rows = db.execute(
  "MATCH (a:Dependency {name:'tokio'})-[:DEPENDS_ON*1..5]->(dep) RETURN DISTINCT dep.name"
)
puts rows.inspect  # [["serde"]]

db.close
```

---

## Real-World Use Cases

### Recommendation Engine

```cypher
MATCH (u:User {id: $user_id})-[:LIKED]->(item:Item)
WITH collect(item) AS liked_items
MATCH (other:User)-[:LIKED]->(item) WHERE item IN liked_items
WITH other, COUNT(item) AS overlap ORDER BY overlap DESC LIMIT 20
MATCH (other)-[:LIKED]->(candidate:Item)
WHERE NOT candidate IN liked_items
RETURN candidate.name, COUNT(other) AS score ORDER BY score DESC LIMIT 10
```

### Fraud Detection

```cypher
MATCH (flagged:Account {status:'fraudulent'})-[:USED]->(device:Device)
MATCH (device)<-[:USED]-(suspect:Account)
WHERE suspect.status <> 'fraudulent'
WITH suspect, COUNT(device) AS shared_devices
WHERE shared_devices >= 2
RETURN suspect.id, suspect.email, shared_devices
ORDER BY shared_devices DESC
```

### Dependency Graph

```cypher
-- What breaks if we remove this package?
MATCH (pkg:Package {name: $package_name})<-[:DEPENDS_ON*1..10]-(dependent)
RETURN DISTINCT dependent.name, dependent.version
ORDER BY dependent.name
```

### Knowledge Graph

```cypher
MATCH (a:Concept {name: 'machine learning'}), (b:Concept {name: 'linear algebra'})
MATCH path = shortestPath((a)-[:RELATED_TO|REQUIRES|FOUNDATION_OF*]->(b))
RETURN [n IN nodes(path) | n.name] AS connection_chain
```

### Org Chart Reporting

```cypher
MATCH (emp:Employee {name: $name})-[:REPORTS_TO*]->(mgr:Employee)
RETURN emp.name, [m IN collect(mgr) | m.name + ' (' + m.title + ')'] AS chain
```

---

## Advanced Features

### Encrypted Database

```rust,no_run
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(b"my-secret-phrase");

    let db = GraphDb::open_encrypted(std::path::Path::new("secure.db"), key)?;
    db.execute("CREATE (n:Secret {data: 'classified'})")?;
    // Every WAL entry is XChaCha20-Poly1305 encrypted before hitting disk
    Ok(())
}
```

### Graph Visualization

```bash
sparrowdb visualize --db my.db | dot -Tsvg -o graph.svg
```

### Full-Text Search

```rust,no_run
db.execute("CALL db.index.fulltext.createNodeIndex('docs', ['Document'], ['content', 'title'])")?;
db.execute("CREATE (n:Document {title: 'Rust graph databases', content: 'Embedded and fast'})")?;

let results = db.execute(
    "CALL db.index.fulltext.queryNodes('docs', 'embedded graph') \
     YIELD node, score \
     RETURN node.title, score ORDER BY score DESC"
)?;
```

### Per-Query Timeout

```rust,no_run
match db.execute_with_timeout(
    "MATCH (a)-[:FOLLOWS*1..10]->(b) RETURN b.name",
    Duration::from_secs(5),
) {
    Ok(rows) => println!("{} rows", rows.rows.len()),
    Err(e) if e.to_string().contains("timeout") => eprintln!("Query cancelled"),
    Err(e) => return Err(e),
}
```

### Bulk CSV Import

```bash
# High-throughput node + edge ingestion via CLI
sparrowdb bulk-import \
  --db my.db \
  --nodes nodes.csv --node-label Person \
  --edges edges.csv --rel-type KNOWS \
  --src-label Person --dst-label Person \
  --batch-size 10000
```

```rust,no_run
// Or from Rust
use sparrowdb::bulk::{BulkLoader, BulkOptions};
let loader = BulkLoader::new(&db, BulkOptions::default());
let n = loader.load_nodes(Path::new("nodes.csv"), "Person")?;
let e = loader.load_edges(Path::new("edges.csv"), "KNOWS", "Person", "Person")?;
```

### Neo4j Migration

```bash
# Export from Neo4j using APOC, then:
sparrowdb import --neo4j-csv nodes.csv,relationships.csv --db my.db
```

---

## Performance Characteristics

### Benchmark Results: SNAP Facebook Dataset (v0.1.15)

Measured against Neo4j 5.x (server, JVM warmed) and Kùzu (Shi et al. VLDB 2023). All figures are p50 latency in microseconds. Dataset: SNAP Facebook social graph (4,039 nodes, 88,234 edges), 50 warmup + 200 iterations.

| Query | SparrowDB v0.1.15 (µs) | Neo4j (µs) | Kùzu (µs) | vs Neo4j |
|-------|----------------------|-----------|-----------|---------|
| Q1 Point Lookup (indexed) | **103** | 321 | 280 | **3x faster** |
| Q2 Range Filter | 3,600 | 333 | n/a | 11x slower |
| Q3 1-Hop Traversal | — | 632 | 410 | re-baseline pending |
| Q4 2-Hop Traversal | — | 376 | 490 | re-baseline pending |
| Q5 Variable Path 1..3 | — | 501 | 620 | re-baseline pending |
| Q6 Global COUNT(*) | **2.2** | 202 | 150 | **93x faster** |
| Q7 Top-10 by Degree | **401** | 17,588 | n/a | **44x faster** |
| Q8 Mutual Friends | **720** ¹ | 352 | n/a | **2x faster** |

¹ **Q8: 153,300µs → 720µs (−99.5%, 200× improvement)** via SlotIntersect + BfsArena + bitvector set-intersection (v0.1.15, #357–#359).

Q3/Q4/Q5 query shapes changed between v0.1.13 and v0.1.15 — prior numbers are not comparable. Fresh baseline measurements are planned after Q4 (#358) and Q5 (#359) fixes are confirmed at SNAP scale.

Neo4j reference: measured locally, Neo4j Docker v5.x, Bolt TCP. Kùzu reference: Shi et al. VLDB 2023 Table 5, in-process.

**Where SparrowDB wins:**
- **Q1 (point lookup):** B-tree property index at 103µs — 3x faster than Neo4j's Bolt round-trip with JVM overhead.
- **Q6 (global COUNT):** 2.2µs — 93x faster. Catalog-level metadata lookup, no scan.
- **Q7 (top-10 by degree):** 401µs — 44x faster than Neo4j's 17.6ms. Pre-computed degree cache vs Neo4j's full adjacency scan.
- **Q8 (mutual friends):** 0.72ms — now 2x faster than Neo4j. SlotIntersect + BfsArena eliminated the O(N × disk_reads) bottleneck.
- **Cold start:** ~27ms on macOS SSD — viable for serverless and short-lived CLI processes.

**Where SparrowDB trails:** Multi-hop traversal (Q3, Q4, Q5) and range scans (Q2). The core structural gap is the single-threaded engine and under-exploited CSR layout on large social graphs. See Roadmap.

**What this means in practice:**
- Use SparrowDB for: embedded apps, CLIs, agents, edge services, recommendation engines, and workloads dominated by point lookups, writes, aggregations, and shallow traversals.
- Use Neo4j for: deep multi-hop traversal on large social or web graphs as the primary query pattern.

### Engine Design

| Technique | What it buys you |
|-----------|-----------------|
| **Chunked vectorized pipeline** | 4-phase execution: ScanByLabel → GetNeighbors chunks → SlotIntersect → ReadNodeProps; eliminates per-row allocation overhead |
| **FrontierScratch arena** | Reusable flat arena for BFS frontiers; no per-hop heap allocation |
| **Factorized execution** | Multi-hop traversals avoid materializing O(N²) intermediate rows |
| **B-tree property index** | Equality lookups: O(log n), not a full label scan; persisted across restarts |
| **Inverted text index** | `CONTAINS` / `STARTS WITH` without scanning every node |
| **External merge sort** | `ORDER BY` on results larger than RAM — sorted runs spill to disk |
| **`execute_batch()`** | Bulk loads committed in one `fsync` |
| **SWMR concurrency** | Concurrent readers at zero extra cost; readers never block writers |
| **Zero-copy open** | Opens in under 1ms — suitable for serverless and short-lived processes |
| **GIL-released Python** | Python threads can issue parallel reads without contention |

---

## Comparison

| | SparrowDB | Neo4j | DGraph | SQLite + JSON |
|---|---|---|---|---|
| **Deployment** | Embedded (in-process) | Server required | Server required | Embedded |
| **Query language** | Cypher | Cypher | GraphQL+DQL | SQL |
| **Primary language** | Rust | JVM | Go | C |
| **Python binding** | PyO3 native (releases GIL) | Bolt driver | Bolt driver | Adapter |
| **Node.js binding** | napi-rs native | Bolt driver | Bolt driver | Adapter |
| **Ruby binding** | Magnus native | Bolt driver | None | Adapter |
| **At-rest encryption** | XChaCha20 built-in | Enterprise only | No | No |
| **WAL crash recovery** | Yes | Yes | Yes | Yes |
| **Full-text search** | Built-in | Built-in | Built-in | No |
| **MCP server** | Built-in | No | No | No |
| **Bulk CSV import** | Built-in | Via neo4j-admin | Via bulk loader | No |
| **License** | MIT | GPL / Commercial | Apache 2 | Public domain |
| **Runtime dependencies** | Zero | JVM + server | Server process | Zero |

**TL;DR:** If you need embedded + Cypher + zero infrastructure, there's nothing else. SparrowDB is the only option in that row.

---

## CLI Reference

```bash
sparrowdb query --db my.db "MATCH (n:Person) RETURN n.name LIMIT 10"
sparrowdb checkpoint --db my.db
sparrowdb info --db my.db
sparrowdb visualize --db my.db | dot -Tsvg -o graph.svg
sparrowdb import --neo4j-csv nodes.csv,relationships.csv --db my.db
sparrowdb bulk-import --db my.db --nodes nodes.csv --edges edges.csv --node-label Person --rel-type KNOWS

# NDJSON server mode
sparrowdb serve --db my.db
# stdin:  {"id":"q1","cypher":"MATCH (n) RETURN n LIMIT 5"}
# stdout: {"id":"q1","columns":["n"],"rows":[...],"error":null}
```

---

## Project Status

**SparrowDB is pre-1.0. We are building in public.**

The API is stable enough to build on, but the on-disk format may change before 1.0. Pin your version.

**What's done:** Full Cypher subset · Multi-label nodes `(n:A:B)` · WAL durability + crash recovery · MVCC · At-rest encryption · 4-phase chunked vectorized pipeline · Factorized multi-hop engine · B-tree + full-text indexes · External merge sort · Per-query timeouts · Bulk CSV loader · Python / Node.js / Ruby bindings · MCP server · CLI tools · Neo4j APOC import

---

## Roadmap

| Issue | Work | Why it matters |
|-------|------|----------------|
| #300 | Flat BFS arena — replace FrontierScratch with a single flat arena | Eliminates remaining allocation churn in Q8 mutual-neighbor traversals |
| #248 | WHERE predicate on edge properties returns 0 rows | Blocks rating/weight-based queries |
| SPA-253 | WAL CRC32C integrity checksums | Required before 1.0 |
| SPA-231 | HTTP/SSE transport layer | Remote access without embedding |
| — | PyPI pre-built wheels | No Rust toolchain required for Python users |
| — | RubyGems package | No Rust toolchain required for Ruby users |

**Recently shipped:**
- **Q8: 153ms → 0.72ms (−99.5%, 200×)** — SlotIntersect + BfsArena + bitvector set-intersection shipped in v0.1.15 (#357–#359); mutual-friend queries now 2x faster than Neo4j on SNAP Facebook
- **Q8 anchor-node bulk read** (#357) — `find_slot_by_props` replaced O(N × file_reads) with a single bulk column read; eliminates N disk reads for each inline-prop anchor lookup in mutual-neighbor queries
- **4-phase chunked vectorized pipeline** (#299) — FrontierScratch arena + SlotIntersect; Q4 −7%, Q8 p99 −37% in v0.1.13
- **Multi-label nodes `(n:A:B)`** (#289) — standard Cypher multi-label semantics
- **Bulk CSV loader** (#296) — `sparrowdb bulk-import` for high-throughput node/edge ingestion
- **Reverse-arrow pattern fix** (#294) — `(a)->()<-(c)` now returns correct results
- **B-tree property index persisted to disk** (#286) — index survives restart; no re-build on open
- **Delta log O(1) index** (#283) — un-checkpointed writes no longer degrade traversal
- **Degree cache** — Q7 (Top-10 Degree): 1,279ms → 401µs, now 44x faster than Neo4j
- **B-tree property index** — Q1 (Point Lookup): ~444µs → 103µs, now 3x faster than Neo4j
- **Global COUNT optimization** — Q6: 24µs → 2.2µs, now 93x faster than Neo4j
- **2-hop aggregate fix** (#282) — COUNT(*) on multi-hop queries no longer returns null

---

## Architecture

```text
+------------------------------------------------------------------------+
|  Language Bindings                                                     |
|  Rust - Python (PyO3) - Node.js (napi-rs) - Ruby (Magnus)            |
|  CLI (sparrowdb) - MCP Server (sparrowdb-mcp)                         |
+------------------------------------------------------------------------+
|  Cypher Frontend  (sparrowdb-cypher)                                   |
|  Lexer -> AST -> Binder (name resolution, type checking)              |
+------------------------------------------------------------------------+
|  Execution Engine  (sparrowdb-execution)                               |
|  Chunked vectorized pipeline - ChunkedPlan selector                   |
|  FrontierScratch arena - SlotIntersect - factorized aggregation       |
|  External merge sort - EXISTS evaluation - deadline checks             |
+------------------------------------------------------------------------+
|  Catalog  (sparrowdb-catalog)                                          |
|  Label registry - B-tree property index - Inverted text index         |
+------------------------------------------------------------------------+
|  Storage  (sparrowdb-storage)                                          |
|  Write-Ahead Log - CSR adjacency store - Delta log index              |
|  XChaCha20-Poly1305 encryption (optional) - Crash recovery - SWMR    |
+------------------------------------------------------------------------+
```

| Crate | Role |
|-------|------|
| `sparrowdb` | Public API — `GraphDb`, `QueryResult`, `Value`, `BulkLoader` |
| `sparrowdb-common` | Shared types and error definitions |
| `sparrowdb-storage` | WAL, CSR store, encryption, crash recovery |
| `sparrowdb-catalog` | Label/property schema, B-tree index, text index |
| `sparrowdb-cypher` | Lexer, parser, AST, binder |
| `sparrowdb-execution` | Chunked vectorized pipeline, sort, aggregation |
| `sparrowdb-cli` | `sparrowdb` command-line binary |
| `sparrowdb-mcp` | JSON-RPC 2.0 MCP server binary |
| `sparrowdb-python` | PyO3 extension module |
| `sparrowdb-node` | napi-rs Node.js addon |
| `sparrowdb-ruby` | Magnus Ruby extension |

---

## Documentation

| Guide | |
|-------|--|
| [docs/quickstart.md](docs/quickstart.md) | Build your first graph from zero |
| [docs/cypher-reference.md](docs/cypher-reference.md) | Full Cypher support with examples |
| [docs/bindings.md](docs/bindings.md) | Rust, Python, Node.js, Ruby API details |
| [docs/mcp-setup.md](docs/mcp-setup.md) | MCP server and Claude Desktop config |
| [docs/use-cases.md](docs/use-cases.md) | Real-world usage patterns |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Contributor workflow and architecture |

---

## Contributing

Open an issue before submitting a large PR so we can discuss the design first.

```bash
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB
cargo build
cargo test
cargo test -p sparrowdb  # integration tests — the signal
cargo clippy
cargo fmt --check
```

---

## License

MIT — see [LICENSE](LICENSE).
