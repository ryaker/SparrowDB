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

- **Deep multi-hop traversal is your primary workload.** 1-hop to 5-hop queries on high-fanout graphs (social networks, web graphs) are where Neo4j's battle-hardened CSR layout and parallel execution show. SparrowDB is currently 25–453x behind on those queries (1-hop: 67x, 2-hop: 233x, mutual friends: 453x). That gap narrows over time, but if deep traversal is your core workload, use Neo4j.
- **You need distributed writes across many nodes**, or your graph has billions of edges and requires horizontal sharding. Use Neo4j Aura or DGraph for that.

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
| Variable-length paths `[:R*1..N]` | ✅ |
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
| Multi-label nodes `(n:A:B)` | ⚠️ Planned |
| Subqueries `CALL { … }` | ⚠️ Partial |

### Engine & Storage

- **WAL durability** — write-ahead log with crash recovery; survives hard kills
- **SWMR concurrency** — single-writer, multiple-reader; readers never block writers
- **Factorized execution** — multi-hop traversals avoid materializing O(N²) intermediate rows
- **B-tree property index** — equality lookups in O(log n), not full label scans
- **Inverted text index** — `CONTAINS` / `STARTS WITH` routed through an index
- **Full-text search** — relevance-scored `queryNodes` without Elasticsearch
- **External merge sort** — `ORDER BY` on large results spills to disk; no unbounded heap
- **At-rest encryption** — optional XChaCha20-Poly1305 per WAL entry; wrong key errors immediately, never silently decrypts garbage
- **`execute_batch()`** — multiple writes in one `fsync` for bulk-load throughput
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

## Install

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

### Node.js

```bash
npm install sparrowdb
```

### Ruby

```bash
# Build from source (requires Rust toolchain):
cd crates/sparrowdb-ruby && bundle install && rake compile
```

### CLI

```bash
cargo install sparrowdb --bin sparrowdb
```

### MCP Server (Claude Desktop integration)

```bash
cargo install sparrowdb --bin sparrowdb-mcp --locked
```

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
    let prolific = db.execute(
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

    # Traverse: what's related to Widget?
    rows = db.execute(
        "MATCH (p:Product {name:'Widget'})-[:RELATED*1..2]->(r) "
        "RETURN DISTINCT r.name, r.price ORDER BY r.price"
    )
    print(rows)  # [{'r.name': 'Doohickey', 'r.price': 4.99}, {'r.name': 'Gadget', 'r.price': 24.99}]

    # UNWIND + MATCH: bulk lookup by ID
    rows = db.execute(
        "UNWIND [1, 3] AS item_id "
        "MATCH (n:Product {id: item_id}) "
        "RETURN n.name, n.price"
    )
    print(rows)  # [{'n.name': 'Widget', 'n.price': 9.99}, {'n.name': 'Doohickey', 'n.price': 4.99}]

# Thread-safe: GIL is released inside execute(), checkpoint(), and optimize()
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
db.execute("CREATE (n:Article {id: 'a3', title: 'Embedded Rust', tags: 'rust,embedded'})");
db.execute("MATCH (a:Article {id:'a1'}),(b:Article {id:'a2'}) CREATE (a)-[:RELATED]->(b)");

// Find related articles, 2 hops
const related = db.execute(
  "MATCH (a:Article {id:'a1'})-[:RELATED*1..2]->(r) RETURN DISTINCT r.title"
);
console.log(related); // [['Cypher Query Language']]

// Full-text search (after indexing)
db.execute("CALL db.index.fulltext.createNodeIndex('articles', ['Article'], ['title', 'tags'])");
const results = db.execute(
  "CALL db.index.fulltext.queryNodes('articles', 'rust') " +
  "YIELD node, score RETURN node.title, score ORDER BY score DESC"
);
console.log(results);

db.close();
```

### Ruby

```ruby
require 'sparrowdb'

db = SparrowDB::GraphDb.new('/path/to/my.db')

db.execute("CREATE (n:Dependency {name: 'tokio',  version: '1.35'})")
db.execute("CREATE (n:Dependency {name: 'serde',  version: '1.0'})")
db.execute("CREATE (n:Dependency {name: 'rand',   version: '0.8'})")
db.execute("MATCH (a:Dependency {name:'tokio'}),(b:Dependency {name:'serde'}) CREATE (a)-[:DEPENDS_ON]->(b)")

# Who does tokio depend on transitively?
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
-- "Users who liked X also liked Y"
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
-- Find accounts that share a device with a flagged account
MATCH (flagged:Account {status:'fraudulent'})-[:USED]->(device:Device)
MATCH (device)<-[:USED]-(suspect:Account)
WHERE suspect.status <> 'fraudulent'
WITH suspect, COUNT(device) AS shared_devices
WHERE shared_devices >= 2
RETURN suspect.id, suspect.email, shared_devices
ORDER BY shared_devices DESC
```

### Dependency Graph (software, supply chain)

```cypher
-- What breaks if we remove this package?
MATCH (pkg:Package {name: $package_name})<-[:DEPENDS_ON*1..10]-(dependent)
RETURN DISTINCT dependent.name, dependent.version
ORDER BY dependent.name
```

### Knowledge Graph

```cypher
-- How are these two concepts connected?
MATCH (a:Concept {name: 'machine learning'}), (b:Concept {name: 'linear algebra'})
MATCH path = shortestPath((a)-[:RELATED_TO|REQUIRES|FOUNDATION_OF*]->(b))
RETURN [n IN nodes(path) | n.name] AS connection_chain
```

### Org Chart Reporting

```cypher
-- Full reporting chain from an IC to the top
MATCH (emp:Employee {name: $name})-[:REPORTS_TO*]->(mgr:Employee)
RETURN emp.name, [m IN collect(mgr) | m.name + ' (' + m.title + ')'] AS chain
```

---

## Advanced Features

### Encrypted Database

Protect data at rest. The key must be exactly 32 bytes. Wrong key = immediate error, never silently decrypted garbage.

```rust,no_run
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    let mut key = [0u8; 32];
    // Use argon2 / scrypt in production to derive from a passphrase
    key[..16].copy_from_slice(b"my-secret-phrase");

    let db = GraphDb::open_encrypted(std::path::Path::new("secure.db"), key)?;
    db.execute("CREATE (n:Secret {data: 'classified'})")?;
    // Every WAL entry is XChaCha20-Poly1305 encrypted before hitting disk
    Ok(())
}
```

### Graph Visualization

```rust,no_run
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    let db = GraphDb::open(std::path::Path::new("my.db"))?;
    let dot = db.export_dot()?;
    std::fs::write("graph.dot", &dot)?;
    // Render:
    //   dot -Tsvg graph.dot -o graph.svg
    //   dot -Tpng graph.dot -o graph.png
    Ok(())
}
```

```bash
sparrowdb visualize --db my.db | dot -Tsvg -o graph.svg
```

### Full-Text Search

```rust,no_run
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    let db = GraphDb::open(std::path::Path::new("my.db"))?;

    // Index once
    db.execute(
        "CALL db.index.fulltext.createNodeIndex('docs', ['Document'], ['content', 'title'])"
    )?;

    // Index is maintained automatically on writes
    db.execute("CREATE (n:Document {title: 'Rust graph databases', content: 'Embedded and fast'})")?;

    // Query -- relevance ranked
    let results = db.execute(
        "CALL db.index.fulltext.queryNodes('docs', 'embedded graph') \
         YIELD node, score \
         RETURN node.title, score ORDER BY score DESC"
    )?;
    let _ = results;
    Ok(())
}
```

### Per-Query Timeout

```rust,no_run
use sparrowdb::GraphDb;
use std::time::Duration;

fn main() -> sparrowdb::Result<()> {
    let db = GraphDb::open(std::path::Path::new("my.db"))?;

    match db.execute_with_timeout(
        "MATCH (a)-[:FOLLOWS*1..10]->(b) RETURN b.name",
        Duration::from_secs(5),
    ) {
        Ok(rows) => println!("{} rows", rows.rows.len()),
        Err(e) if e.to_string().contains("timeout") => eprintln!("Query cancelled"),
        Err(e) => return Err(e),
    }
    Ok(())
}
```

### Bulk Load (single fsync)

```rust,no_run
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    let db = GraphDb::open(std::path::Path::new("my.db"))?;
    db.execute_batch(&[
        "CREATE (n:Product {id: 1, name: 'Widget',   price: 9.99})",
        "CREATE (n:Product {id: 2, name: 'Gadget',   price: 24.99})",
        "CREATE (n:Product {id: 3, name: 'Doohickey',price: 4.99})",
        "MATCH (a:Product {id:1}),(b:Product {id:2}) CREATE (a)-[:RELATED]->(b)",
    ])?;
    // All four statements committed in one WAL fsync
    Ok(())
}
```

### Neo4j Migration

```bash
# Export from Neo4j using APOC:
#   CALL apoc.export.csv.all("export", {})
# Produces nodes.csv + relationships.csv

sparrowdb import --neo4j-csv nodes.csv,relationships.csv --db my.db
```

---

## Performance Characteristics

### Benchmark Results: SNAP Facebook Dataset (v0.1.10)

Measured against Neo4j 5.x (server, JVM warmed) and Kùzu (Shi et al. VLDB 2023). All figures are p50 latency in microseconds. Dataset: SNAP Facebook social graph (4,039 nodes, 88,234 edges), 100 warmup + 500 iterations.

**The headline:** Point lookups and global aggregation beat a running Neo4j server handily — with no JVM, no server process, and no network hop.

| Query | SparrowDB (µs) | Neo4j (µs) | Kùzu (µs) | vs Neo4j |
|-------|---------------|-----------|-----------|---------|
| Q1 Point Lookup (indexed) | **105** | 321 | 280 | **3x faster** |
| Q2 Range Filter | 3,400 | 333 | n/a | 10x slower |
| Q3 1-Hop Traversal | 42,300 | 632 | 410 | 67x slower |
| Q4 2-Hop Traversal | 87,500 | 376 | 490 | 233x slower |
| Q5 Variable Path 1..3 | 12,300 | 501 | 620 | 25x slower |
| Q6 Global COUNT(*) | **2.2** | 202 | 150 | **91x faster** |
| Q7 Top-10 by Degree | **401** | 17,588 | n/a | **44x faster** |
| Q8 Mutual Friends | 159,600 | 352 | n/a | 453x slower |

Neo4j reference: measured locally, Neo4j Docker v5.x, Bolt TCP. Kùzu reference: Shi et al. VLDB 2023 Table 5, in-process.

**Where SparrowDB wins:**
- **Q1 (point lookup):** B-tree property index delivers O(log n) lookup at 105µs. 3x faster than Neo4j's Bolt round-trip with JVM overhead.
- **Q6 (global COUNT):** 2.2µs — 91x faster. Catalog-level metadata lookup, no scan.
- **Q7 (top-10 by degree):** 401µs — 44x faster than Neo4j's 17.6ms. Pre-computed degree cache delivers O(1) lookup vs Neo4j's full adjacency scan.
- **Cold start:** opens in ~27ms on macOS SSD — viable for serverless functions and short-lived CLI processes where Neo4j's server startup is disqualifying.

**Where SparrowDB trails:** Multi-hop traversal (Q3, Q4, Q5, Q8) and range scans (Q2). Neo4j's CSR layout is battle-hardened for high-fanout graph walks with parallel execution. The gap on deep traversals (67x–453x) is structural, not a tuning issue. It narrows over time as the engine matures (see Roadmap), but it is real today.

**What this means in practice:**
- Use SparrowDB for: embedded apps, CLIs, agents, edge services, recommendation engines, and workloads dominated by point lookups, writes, aggregations, and shallow traversals.
- Use Neo4j for: deep multi-hop traversal on large social or web graphs as the primary query pattern.

### Engine Design

| Technique | What it buys you |
|-----------|-----------------|
| **Factorized execution** | Multi-hop traversals avoid materializing O(N²) intermediate rows |
| **B-tree property index** | Equality lookups: O(log n), not a full label scan |
| **Inverted text index** | `CONTAINS` / `STARTS WITH` without scanning every node |
| **External merge sort** | `ORDER BY` on results larger than RAM — sorted runs spill to disk |
| **`execute_batch()`** | Bulk loads committed in one `fsync` |
| **SWMR concurrency** | Concurrent readers at zero extra cost; readers never block writers |
| **Zero-copy open** | Opens in under 1ms — suitable for serverless and short-lived processes |
| **GIL-released Python** | Python threads can issue parallel reads without contention |

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
|  Factorized Execution Engine  (sparrowdb-execution)                    |
|  Physical plan - iterator model - aggregation                          |
|  External merge sort - EXISTS evaluation - deadline checks             |
+------------------------------------------------------------------------+
|  Catalog  (sparrowdb-catalog)                                          |
|  Label registry - B-tree property index - Inverted text index         |
+------------------------------------------------------------------------+
|  Storage  (sparrowdb-storage)                                          |
|  Write-Ahead Log - CSR adjacency store - Delta log                    |
|  XChaCha20-Poly1305 encryption (optional) - Crash recovery - SWMR    |
+------------------------------------------------------------------------+
```

**Crate layout:**

| Crate | Role |
|-------|------|
| `sparrowdb` | Public API — `GraphDb`, `QueryResult`, `Value` |
| `sparrowdb-common` | Shared types and error definitions |
| `sparrowdb-storage` | WAL, CSR store, encryption, crash recovery |
| `sparrowdb-catalog` | Label/property schema, B-tree index, text index |
| `sparrowdb-cypher` | Lexer, parser, AST, binder |
| `sparrowdb-execution` | Physical query executor, sort, aggregation |
| `sparrowdb-cli` | `sparrowdb` command-line binary |
| `sparrowdb-mcp` | JSON-RPC 2.0 MCP server binary |
| `sparrowdb-python` | PyO3 extension module |
| `sparrowdb-node` | napi-rs Node.js addon |
| `sparrowdb-ruby` | Magnus Ruby extension |

---

## MCP Server — AI Assistant Integration

`sparrowdb-mcp` speaks JSON-RPC 2.0 over stdio. It plugs into Claude Desktop and any MCP-compatible AI client, letting the assistant query and write to your graph database using natural tool calls.

```bash
cargo build --release --bin sparrowdb-mcp
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

**Available tools:**

| Tool | Description |
|------|-------------|
| `execute_cypher` | Execute any Cypher statement; returns result rows |
| `create_entity` | Create a node with a label and properties |
| `add_property` | Set a property on nodes matching a filter |
| `checkpoint` | Flush WAL and compact |
| `info` | Database metadata |

Full setup: [docs/mcp-setup.md](docs/mcp-setup.md)

---

## CLI Reference

```bash
# Execute a query — results as JSON
sparrowdb query --db my.db "MATCH (n:Person) RETURN n.name LIMIT 10"

# Flush WAL and compact
sparrowdb checkpoint --db my.db

# Database metadata
sparrowdb info --db my.db

# Export graph as DOT
sparrowdb visualize --db my.db --output graph.dot
sparrowdb visualize --db my.db | dot -Tsvg -o graph.svg

# Import Neo4j APOC CSV export
sparrowdb import --neo4j-csv nodes.csv,relationships.csv --db my.db

# NDJSON line-oriented server mode
sparrowdb serve --db my.db
# stdin:  {"id":"q1","cypher":"MATCH (n) RETURN n LIMIT 5"}
# stdout: {"id":"q1","columns":["n"],"rows":[...],"error":null}
```

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
| **License** | MIT | GPL / Commercial | Apache 2 | Public domain |
| **Runtime dependencies** | Zero | JVM + server | Server process | Zero |

**TL;DR:** If you need embedded + Cypher + zero infrastructure, there's nothing else. SparrowDB is the only option in that row.

---

## Project Status

**SparrowDB is pre-1.0. We are building in public.**

We ship fast. The API is stable enough to build on, but we're still adding features and the on-disk format may change before 1.0. Pin your version.

**What's done:**
- Full Cypher subset (see table above)
- WAL durability + crash recovery
- At-rest encryption
- Factorized multi-hop engine
- B-tree + full-text indexes
- External merge sort
- Per-query timeouts
- Bulk batch writes
- Python / Node.js / Ruby bindings
- MCP server
- CLI tools
- Neo4j APOC import

Follow along: [github.com/ryaker/SparrowDB](https://github.com/ryaker/SparrowDB)

---

## Roadmap

These are the active workstreams that will close the most meaningful gaps, ordered by priority:

| Issue | Work | Why it matters |
|-------|------|----------------|
| #242 | MATCH on large WAL silently returns empty — causes phantom write successes | Affects import correctness at scale. Twitter import loses ~27% of edges silently. |
| #248 | WHERE predicate on edge properties returns 0 rows | Edge property reads work; filter evaluation does not. Blocks rating/weight-based queries. |
| #247 | `MERGE` upsert — find-by-property-or-create | Enables idempotent imports and fixes the root cause of #242's phantom creates. PR #250 open. |
| SPA-253 | WAL CRC32C integrity checksums | Detects bit-rot and incomplete writes on crash. Required before 1.0. |
| SPA-231 | HTTP/SSE transport layer | Enables remote access without embedding. |
| SPA-200 | Multi-label nodes `(n:A:B)` | Matches standard Cypher semantics. |
| SPA-226 | Publish SparrowOntology to crates.io | Makes the ontology layer reusable as a standalone dependency. |

**Recently shipped** (reflected in benchmark numbers above):
- **Degree cache** — Q7 (Top-10 Degree) went from 1,279ms → 401µs, now 44x *faster* than Neo4j
- **B-tree property index** — Q1 (Point Lookup) went from ~444µs → 105µs, now 3x faster than Neo4j
- **Global COUNT optimization** — Q6 went from 24µs → 2.2µs, now 91x *faster* than Neo4j
- **ReadSnapshot label_row_counts cache** — eliminated ~116µs per-query overhead on Q1
- **Edge property reads** (#240, #243) — edge properties now correctly returned after CHECKPOINT
- **Two-hop intermediate node prop reads** (#241, #244) — multi-hop traversal returns correct intermediate node values
- **edge_props.bin skip** (#245) — queries without edge variables no longer pay the edge-props read cost
- **2-hop aggregate fix** (#263, #281, #282) — COUNT(*) on multi-hop queries no longer returns null

The traversal gap (Q3, Q4, Q8) remains the largest open challenge. SparrowDB uses a CSR adjacency store on disk (see Architecture), but the current execution engine is single-threaded and does not exploit that layout for in-memory traversal walks. Parallel traversal and a tighter runtime adjacency representation are the two structural changes that will move those numbers.

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

cargo build           # build everything
cargo test            # full test suite
cargo test -p sparrowdb  # integration tests only (these are the signal — not unit tests)
cargo clippy          # lints
cargo fmt --check     # format check

cargo build --release --bin sparrowdb    # CLI
cargo build --release --bin sparrowdb-mcp  # MCP server
```

The workspace is structured so each crate has one job. Adding a Cypher feature typically means touching `sparrowdb-cypher` (parser + AST) and `sparrowdb-execution` (executor), with an integration test in `crates/sparrowdb/tests/`. See [DEVELOPMENT.md](DEVELOPMENT.md).

---

## License

MIT — see [LICENSE](LICENSE).
