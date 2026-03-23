<p align="center">
  <img src="docs/logo.png" alt="SparrowDB — graph-based sparrow's nest" width="320" />
</p>

<h1 align="center">SparrowDB</h1>

<p align="center"><strong>Embedded graph database with Cypher queries — no server, no subscription, no cloud.</strong></p>

<p align="center">
  <a href="https://github.com/ryaker/SparrowDB/actions"><img src="https://github.com/ryaker/SparrowDB/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://crates.io/crates/sparrowdb"><img src="https://img.shields.io/crates/v/sparrowdb.svg" alt="crates.io" /></a>
  <a href="https://docs.rs/sparrowdb"><img src="https://docs.rs/sparrowdb/badge.svg" alt="docs.rs" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" /></a>
  <img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Rust" />
  <img src="https://img.shields.io/badge/bindings-Python%20%7C%20Node.js%20%7C%20Ruby-blue.svg" alt="Bindings" />
</p>

---

## Why SparrowDB

Most graph databases require a running server, a cloud account, or a license fee. SparrowDB does none of that. It links directly into your process — Rust, Python, Node.js, or Ruby — and exposes a full Cypher query interface backed by a WAL-durable, crash-safe store on disk. You get multi-hop graph traversals, aggregations, MERGE upserts, full-text search, optional at-rest encryption, and an MCP server for AI assistant integration, all without standing up any infrastructure. Drop it in like SQLite and query like Neo4j.

---

## Feature Highlights

- **Full Cypher subset** — CREATE, MATCH, WITH pipelines, UNWIND+MATCH, MERGE (nodes and relationships), SET, DELETE, OPTIONAL MATCH, UNION/UNION ALL, variable-length paths `[:R*1..N]`, EXISTS, shortestPath, CASE WHEN, `coalesce()`, `collect()`, ALL/ANY/NONE/SINGLE predicates, ORDER BY / LIMIT / SKIP, COUNT DISTINCT, and more
- **Multi-language** — native Rust API; Python via PyO3; Node.js via napi-rs; Ruby via Magnus — all open the same on-disk format
- **WAL durability** — write-ahead log with crash recovery; survives `kill -9`
- **At-rest encryption** — optional XChaCha20-Poly1305 encryption of WAL payloads; wrong key returns an error, never silently decrypts garbage
- **Factorized execution engine** — multi-hop traversals avoid materialising O(N**2) intermediate rows; friend-of-friend queries stay fast at scale
- **B-tree property index** — equality lookups are `O(log n)`, not full label scans
- **Inverted text index** — `CONTAINS`, `STARTS WITH`, and `CALL db.index.fulltext.queryNodes` without an external search engine
- **External merge sort** — `ORDER BY` on result sets larger than RAM spills sorted runs to disk and merges them; no unbounded heap allocation
- **`execute_batch()`** — group multiple writes into one WAL `fsync` for bulk-load throughput
- **`execute_with_timeout()`** — cancel runaway traversals without killing the process
- **`export_dot()`** — export any graph to Graphviz DOT format for visualisation
- **Neo4j APOC CSV import** — migrate existing Neo4j graphs in one command
- **MCP server** (`sparrowdb-mcp`) — JSON-RPC 2.0 over stdio; plugs into Claude Desktop and any MCP-compatible AI client
- **Zero runtime dependencies** — fully embedded; no daemon, no JVM, no network required

---

## Install

### Rust

```toml
[dependencies]
sparrowdb = "0.1"
```

Or from git (latest development):

```toml
[dependencies]
sparrowdb = { git = "https://github.com/ryaker/SparrowDB" }
```

### Python (maturin / pip)

```bash
# When published to PyPI:
pip install sparrowdb

# Or build from source:
cd crates/sparrowdb-python
maturin develop --features python
```

Requires Python 3.9+ and a Rust toolchain for source builds.

### Node.js

```bash
npm install sparrowdb
```

Or build from source:

```bash
cd crates/sparrowdb-node
npm install
npm run build
```

### Ruby

```bash
# Build the native extension:
cd crates/sparrowdb-ruby
bundle install
rake compile
```

### CLI

```bash
cargo build --release --bin sparrowdb
# Binary at: target/release/sparrowdb
```

### MCP Server

```bash
cargo build --release --bin sparrowdb-mcp
# Binary at: target/release/sparrowdb-mcp
```

---

## Quick Start

### Rust

```rust
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    // Open (or create) a database directory
    let db = GraphDb::open(std::path::Path::new("my.db"))?;

    // Create nodes
    db.execute("CREATE (alice:Person {name: 'Alice', age: 30})")?;
    db.execute("CREATE (bob:Person   {name: 'Bob',   age: 25})")?;
    db.execute("CREATE (carol:Person {name: 'Carol', age: 35})")?;

    // Create relationships
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
         CREATE (a)-[:KNOWS]->(b)",
    )?;
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'})
         CREATE (a)-[:KNOWS]->(b)",
    )?;

    // 1-hop: who does Alice know?
    let friends = db.execute(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(f) RETURN f.name",
    )?;
    for row in &friends.rows {
        println!("{:?}", row); // ["Bob"]
    }

    // 2-hop: friends of friends
    let fof = db.execute(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(f) RETURN f.name",
    )?;
    for row in &fof.rows {
        println!("{:?}", row); // ["Carol"]
    }

    // Aggregations
    let stats = db.execute(
        "MATCH (p:Person) RETURN COUNT(*), AVG(p.age), MIN(p.age), MAX(p.age)",
    )?;
    println!("{:?}", stats.rows[0]); // [3, 30.0, 25, 35]

    // Paginate with ORDER BY + SKIP + LIMIT
    let _page = db.execute(
        "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 0 LIMIT 10",
    )?;

    // MERGE — upsert a node (creates if absent, updates if present)
    db.execute(
        "MERGE (n:Person {name: 'Alice'}) SET n.verified = true",
    )?;

    // WITH pipeline — filter mid-query
    let _seniors = db.execute(
        "MATCH (p:Person) WITH p WHERE p.age >= 30 RETURN p.name ORDER BY p.age",
    )?;

    // CASE WHEN — conditional expressions
    let _tiers = db.execute(
        "MATCH (p:Person)
         RETURN p.name,
                CASE WHEN p.age >= 30 THEN 'senior' ELSE 'junior' END AS tier",
    )?;

    Ok(())
}
```

### Python

```python
import sparrowdb

# Context manager — database closes cleanly on exit
with sparrowdb.GraphDb("/path/to/my.db") as db:
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
    db.execute("CREATE (n:Person {name: 'Bob',   age: 25})")
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS]->(b)"
    )

    # Query — returns a list of dicts
    rows = db.execute("MATCH (a:Person)-[:KNOWS]->(f) RETURN f.name")
    print(rows)  # [{'f.name': 'Bob'}]

    # Aggregation
    stats = db.execute(
        "MATCH (p:Person) RETURN COUNT(*) AS total, AVG(p.age) AS avg_age"
    )
    print(stats)  # [{'total': 2, 'avg_age': 27.5}]

    # ORDER BY with LIMIT
    page = db.execute(
        "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 10"
    )

    # MERGE upsert
    db.execute("MERGE (n:Person {name: 'Alice'}) SET n.active = true")

# execute() releases the GIL — safe to use from threads
import concurrent.futures

def run_query(limit):
    with sparrowdb.GraphDb("/path/to/my.db") as db:
        return db.execute(f"MATCH (n:Person) RETURN n.name LIMIT {limit}")

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    futures = [pool.submit(run_query, i) for i in range(1, 6)]
    results = [f.result() for f in futures]
```

### Node.js / TypeScript

```typescript
import SparrowDB from 'sparrowdb';

const db = new SparrowDB('/path/to/my.db');

// Create data
db.execute("CREATE (n:Person {name: 'Alice', age: 30})");
db.execute("CREATE (n:Person {name: 'Bob',   age: 25})");
db.execute(
  "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
  "CREATE (a)-[:KNOWS]->(b)"
);

// Query — returns array of row arrays
const rows = db.execute("MATCH (a:Person)-[:KNOWS]->(f) RETURN f.name");
console.log(rows); // [['Bob']]

// Aggregation
const stats = db.execute(
  "MATCH (p:Person) RETURN COUNT(*) AS total, AVG(p.age) AS avg_age"
);
console.log(stats);

// Order, paginate
const page = db.execute(
  "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 10"
);

db.close();
```

### Ruby

```ruby
require 'sparrowdb'

db = SparrowDB::GraphDb.new('/path/to/my.db')

db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
db.execute("CREATE (n:Person {name: 'Bob',   age: 25})")
db.execute(
  "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " \
  "CREATE (a)-[:KNOWS]->(b)"
)

rows = db.execute("MATCH (a:Person)-[:KNOWS]->(f) RETURN f.name")
puts rows.inspect  # [["Bob"]]

db.close
```

---

## Cypher Reference

| Feature | Status |
|---------|--------|
| `CREATE (n:Label {prop: val})` | yes |
| `MATCH (n:Label) RETURN n.prop` | yes |
| `MATCH (n)` — scan all nodes | yes |
| `WHERE` with `=`, `<>`, `<`, `<=`, `>`, `>=` | yes |
| `WHERE n.prop CONTAINS str` | yes |
| `WHERE n.prop STARTS WITH str` | yes |
| `WHERE n.prop IS NULL` / `IS NOT NULL` | yes |
| 1-hop `(a)-[:REL]->(b)` | yes |
| Multi-hop `(a)-[:R]->()-[:R]->(c)` | yes |
| Undirected `(a)-[:REL]-(b)` | yes |
| Variable-length paths `[:R*1..N]` | yes |
| `RETURN DISTINCT` | yes |
| `ORDER BY`, `LIMIT`, `SKIP` | yes |
| `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)` | yes |
| `SUM`, `AVG`, `MIN`, `MAX` | yes |
| `collect()` — aggregate into list | yes |
| `coalesce(expr1, expr2, ...)` — first non-null | yes |
| `WITH ... AS ... WHERE` pipeline | yes |
| `WITH ... MATCH`, `WITH ... UNWIND` pipelines | yes |
| `UNWIND list AS var` | yes |
| `UNWIND list AS var MATCH (n {id: var})` | yes |
| `OPTIONAL MATCH` | yes |
| `UNION` / `UNION ALL` | yes |
| `MERGE` (upsert node) | yes |
| `MERGE (a)-[:REL]->(b)` — idempotent edge creation | yes |
| `SET`, `DELETE` | yes |
| `CREATE (a)-[:REL]->(b)` — edge creation | yes |
| `CASE WHEN ... THEN ... ELSE ... END` | yes |
| `EXISTS { (n)-[:REL]->(:Label) }` | yes |
| `EXISTS` in `WITH ... WHERE` | yes |
| `shortestPath((a)-[:R*]->(b))` | yes |
| `ANY` / `ALL` / `NONE` / `SINGLE` list predicates | yes |
| `id(n)`, `labels(n)`, `type(r)` | yes |
| `size()`, `range()`, `toInteger()`, `toString()` | yes |
| `toUpper()`, `toLower()`, `trim()`, `replace()`, `substring()` | yes |
| `abs()`, `ceil()`, `floor()`, `sqrt()`, `sign()` | yes |
| Parameters `$param` | yes |
| MVCC write-write conflict detection | yes |
| `CALL db.index.fulltext.queryNodes` | yes |
| `CALL db.schema()` | yes |
| Multi-label nodes `(n:A:B)` | not yet |
| Subqueries `CALL { ... }` | partial |

Full reference: [docs/cypher-reference.md](docs/cypher-reference.md)

---

## Advanced Features

### Batch Writes

Group multiple statements into a single WAL `fsync`. This is substantially faster than issuing separate `execute()` calls during bulk loads:

```rust
db.execute_batch(&[
    "CREATE (n:Product {id: 1, name: 'Widget', price: 9.99})",
    "CREATE (n:Product {id: 2, name: 'Gadget', price: 24.99})",
    "CREATE (n:Product {id: 3, name: 'Doohickey', price: 4.99})",
    "MATCH (a:Product {id: 1}), (b:Product {id: 2}) CREATE (a)-[:RELATED_TO]->(b)",
    "MATCH (a:Product {id: 2}), (b:Product {id: 3}) CREATE (a)-[:RELATED_TO]->(b)",
])?;
// All five statements commit in one WAL fsync
```

### Per-Query Timeout

Cancel runaway graph traversals without killing the process:

```rust
use std::time::Duration;

let result = db.execute_with_timeout(
    "MATCH (a)-[:FOLLOWS*1..10]->(b) RETURN b.name, count(*) AS reach",
    Duration::from_secs(5),
);

match result {
    Ok(rows) => println!("{} rows returned", rows.rows.len()),
    Err(e) if e.to_string().contains("timeout") => {
        eprintln!("Query cancelled after 5 seconds");
    }
    Err(e) => return Err(e.into()),
}
```

### Encrypted Database

Protect data at rest with XChaCha20-Poly1305 encryption. The key must be exactly 32 bytes. An incorrect key returns an error immediately — the database never silently decrypts garbage:

```rust
use sparrowdb::GraphDb;

// Use a proper KDF (argon2, scrypt) in production to derive the key
let mut key = [0u8; 32];
let password_bytes = b"my-secret-passphrase";
let len = password_bytes.len().min(32);
key[..len].copy_from_slice(&password_bytes[..len]);

let db = GraphDb::open_encrypted(std::path::Path::new("secure.db"), key)?;
db.execute("CREATE (n:Secret {data: 'classified'})")?;
// WAL entries are encrypted with XChaCha20-Poly1305 before hitting disk
```

### Graph Export (Graphviz)

Export the full graph as a DOT file and render it with Graphviz:

```rust
let db = GraphDb::open(std::path::Path::new("my.db"))?;
let dot = db.export_dot()?;
std::fs::write("graph.dot", &dot)?;
// Render:
//   dot -Tsvg graph.dot -o graph.svg
//   dot -Tpng graph.dot -o graph.png
```

Or directly from the CLI:

```bash
sparrowdb visualize --db my.db --output graph.dot
dot -Tsvg graph.dot -o graph.svg
```

### Full-Text Search

Create an inverted index over node properties and query it with natural language search terms — no external search engine required:

```rust
// Create the index once
db.execute(
    "CALL db.index.fulltext.createNodeIndex('docs', ['Document'], ['content', 'title'])"
)?;

// Index is maintained automatically on subsequent writes
db.execute("CREATE (n:Document {title: 'Rust graph databases', content: 'Embedded and fast'})")?;
db.execute("CREATE (n:Document {title: 'Python data tools', content: 'For analytics workloads'})")?;

// Query — returns nodes ranked by relevance score
let results = db.execute(
    "CALL db.index.fulltext.queryNodes('docs', 'rust graph') \
     YIELD node, score \
     RETURN node.title, score \
     ORDER BY score DESC"
)?;
```

### Neo4j Migration (APOC CSV Import)

Migrate an existing Neo4j graph using APOC's `exportGraph` format:

```bash
# Export from Neo4j using APOC:
#   CALL apoc.export.csv.all("export", {})
# This produces nodes.csv and relationships.csv in your Neo4j import directory.

sparrowdb import --neo4j-csv nodes.csv,relationships.csv --db my.db
```

The importer reads the two-file APOC format:
- Nodes CSV: `_id,_labels,prop1,prop2,...`
- Relationships CSV: `_start,_end,_type,prop1,...`

### UNWIND + MATCH Pipelines

Perform set-based lookups — match multiple nodes by value from a list in a single query:

```cypher
UNWIND [1, 2, 3, 42, 99] AS item_id
MATCH (n:Product {id: item_id})
RETURN n.name, n.price
ORDER BY n.price DESC
```

### WITH Pipeline Filtering

Chain MATCH + filter + MATCH in a single query without multiple round-trips:

```cypher
MATCH (author:Person)-[:WROTE]->(book:Book)
WITH author, COUNT(book) AS book_count
WHERE book_count >= 3
MATCH (author)-[:LIVES_IN]->(city:City)
RETURN author.name, book_count, city.name
ORDER BY book_count DESC
LIMIT 10
```

### Variable-Length Paths and shortestPath

```cypher
MATCH (start:Person {name: 'Alice'})-[:KNOWS*1..4]->(reachable)
RETURN DISTINCT reachable.name

MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Charlie'})
MATCH path = shortestPath((a)-[:KNOWS*]->(b))
RETURN length(path) AS hops
```

### List Predicates

```cypher
MATCH (p:Person)
WHERE ANY(score IN p.scores WHERE score > 90)
RETURN p.name

MATCH (t:Team)
WHERE ALL(member IN t.members WHERE member.active = true)
RETURN t.name
```

### MERGE — Idempotent Writes

```cypher
MERGE (u:User {email: 'alice@example.com'})
ON CREATE SET u.created_at = '2024-01-01', u.login_count = 0
ON MATCH  SET u.login_count = u.login_count + 1

MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
MERGE (a)-[:KNOWS]->(b)
```

---

## Performance

SparrowDB is built for low-overhead, in-process graph workloads:

| Technique | What it means |
|-----------|---------------|
| **Factorized execution** | Multi-hop traversals avoid materialising O(N^2) intermediate rows — friend-of-friend queries stay fast at scale |
| **B-tree property index** | Equality lookups use an in-memory B-tree (O(log n)) rather than full label scans |
| **Inverted text index** | `CONTAINS` and `STARTS WITH` predicates are routed through an inverted index, not a full scan |
| **Full-text index** | `CALL db.index.fulltext.queryNodes` with relevance scoring — no Elasticsearch or Solr required |
| **External merge sort** | `ORDER BY` on large result sets spills sorted runs to disk and merges them — no unbounded heap allocation |
| **`execute_batch()`** | Group multiple writes into one WAL `fsync` — orders-of-magnitude speedup for bulk loads |
| **WAL-backed durability** | Writes commit to the write-ahead log; no `fsync` storm on every property update |
| **WAL encryption** | Optional XChaCha20-Poly1305 encryption is applied per-entry; the cipher is streaming and adds minimal overhead |
| **SWMR concurrency** | Single-writer, multiple-reader — readers never block writers; snapshot isolation at no extra cost |
| **Per-query timeout** | `execute_with_timeout(cypher, duration)` cancels runaway queries without killing the process |
| **Zero-copy open** | Database directories open with minimal startup cost — suitable for serverless and short-lived processes |

---

## Architecture

```
+---------------------------------------------------------------------+
|  Language Bindings: Rust . Python (PyO3) . Node.js (napi-rs)       |
|                     Ruby (Magnus) . CLI . MCP Server                |
+---------------------------------------------------------------------+
|  Cypher Parser  (sparrowdb-cypher)                                  |
|    Lexer -> AST -> Binder (name resolution, type checking)          |
+---------------------------------------------------------------------+
|  Factorized Execution Engine  (sparrowdb-execution)                 |
|    Physical plan . Iterator model . Aggregation                     |
|    External merge sort . EXISTS / subpath evaluation                |
+---------------------------------------------------------------------+
|  Catalog  (sparrowdb-catalog)                                       |
|    Label registry . B-tree property index . Inverted text index     |
+---------------------------------------------------------------------+
|  Storage  (sparrowdb-storage)                                       |
|    Write-Ahead Log (WAL) . CSR adjacency store . Delta store        |
|    XChaCha20-Poly1305 encryption (optional)                         |
|    Crash recovery . SWMR concurrency . Checkpoint                   |
+---------------------------------------------------------------------+
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
| `sparrowdb-metrics` | Prometheus metrics exporter |

---

## CLI Reference

Build the CLI binary:

```bash
cargo build --release --bin sparrowdb
```

### Commands

```bash
# Execute a Cypher query — results printed as JSON
sparrowdb query --db my.db "MATCH (n:Person) RETURN n.name, n.age LIMIT 10"

# Flush the WAL and compact the database
sparrowdb checkpoint --db my.db

# Print database metadata (transaction ID, path)
sparrowdb info --db my.db

# Export the full graph as a DOT file (to file or stdout)
sparrowdb visualize --db my.db --output graph.dot
sparrowdb visualize --db my.db | dot -Tsvg -o graph.svg

# Import a Neo4j APOC CSV export
sparrowdb import --neo4j-csv nodes.csv,relationships.csv --db my.db

# NDJSON server mode — queries arrive on stdin, results go to stdout
sparrowdb serve --db my.db
# Request:   {"id": "q1", "cypher": "MATCH (n) RETURN n LIMIT 5"}
# Response:  {"id": "q1", "columns": ["n"], "rows": [...], "error": null}
```

---

## MCP Server — Claude Desktop Integration

`sparrowdb-mcp` speaks JSON-RPC 2.0 over stdio. It lets Claude Desktop — or any MCP-compatible AI client — query and write to a SparrowDB database using natural tool calls.

### Build

```bash
cargo build --release --bin sparrowdb-mcp
realpath target/release/sparrowdb-mcp  # copy the absolute path for the config below
```

### Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "sparrowdb": {
      "command": "/absolute/path/to/target/release/sparrowdb-mcp",
      "args": [],
      "env": {}
    }
  }
}
```

Restart Claude Desktop after editing. SparrowDB will appear in the integrations panel.

### Available MCP Tools

All tools accept a `db_path` parameter — the path to the SparrowDB database directory. The directory is created automatically on first open.

| Tool | Description |
|------|-------------|
| `execute_cypher` | Execute any Cypher statement; returns result rows or a diagnostic error |
| `create_entity` | Create a node with a label and structured properties (validated, Claude-friendly interface) |
| `add_property` | Add or update a scalar property on nodes matched by label and a filter property; reports the number of nodes updated |
| `checkpoint` | Flush the WAL and compact the database |
| `info` | Return database metadata (transaction ID, path) |

### Example MCP Session

```
initialize:
  -> {"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}
  <- {"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2024-11-05","serverInfo":{"name":"sparrowdb-mcp","version":"0.1.0"}}}

create a node:
  -> {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_entity","arguments":{"db_path":"/tmp/mydb","class_name":"Person","properties":{"name":"Alice","age":30}}}}
  <- {"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Entity created successfully (class: 'Person')"}]}}

query:
  -> {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"execute_cypher","arguments":{"db_path":"/tmp/mydb","query":"MATCH (n:Person) RETURN n.name, n.age"}}}
  <- {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"QueryResult { columns: [\"n.name\", \"n.age\"], rows: [[String(\"Alice\"), Int64(30)]] }"}]}}

add a property:
  -> {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"add_property","arguments":{"db_path":"/tmp/mydb","label":"Person","match_prop":"name","match_val":"Alice","set_prop":"email","set_val":"alice@example.com"}}}
  <- {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"add_property: set 'Person.email' on 1 node(s) matching name='Alice'."}],"updated":1}}
```

Full MCP setup guide: [docs/mcp-setup.md](docs/mcp-setup.md)

---

## Comparison

| | SparrowDB | Neo4j | DGraph | SQLite + JSON |
|---|---|---|---|---|
| **Deployment** | Embedded (in-process) | Server required | Server required | Embedded |
| **Cypher** | Full subset | Full | Partial | No |
| **Primary language** | Rust | JVM | Go | C |
| **Python binding** | PyO3 native | Driver (bolt) | Driver | Driver |
| **Node.js binding** | napi-rs native | Driver (bolt) | Driver | Driver |
| **Ruby binding** | Magnus native | Driver (bolt) | No | Driver |
| **At-rest encryption** | XChaCha20 built-in | Enterprise only | No | No |
| **WAL crash recovery** | Yes | Yes | Yes | Yes |
| **Full-text search** | Built-in | Built-in | Built-in | No |
| **MCP server** | Built-in | No | No | No |
| **License** | MIT | GPL / Commercial | Apache 2 | Public domain |
| **Runtime dependencies** | None | JVM + server process | Server process | None |

---

## Documentation

| Guide | Description |
|-------|-------------|
| [docs/quickstart.md](docs/quickstart.md) | Step-by-step: build your first graph from zero |
| [docs/cypher-reference.md](docs/cypher-reference.md) | Full Cypher support reference with examples |
| [docs/bindings.md](docs/bindings.md) | Rust, Python, Node.js, Ruby API details |
| [docs/mcp-setup.md](docs/mcp-setup.md) | MCP server setup and Claude Desktop config |
| [docs/use-cases.md](docs/use-cases.md) | Real-world usage patterns |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Contributor workflow, architecture deep-dive |

---

## Contributing

Contributions are welcome. Please open an issue before submitting a large PR so we can discuss the design first.

```bash
# Clone and build
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB
cargo build

# Run the full test suite
cargo test

# Run a specific crate's tests
cargo test -p sparrowdb

# Run integration tests (requires the MCP binary to be built first)
cargo build -p sparrowdb-mcp
cargo test -p sparrowdb spa_228

# Benchmarks
cargo bench -p sparrowdb
```

The workspace is structured so that each crate has a focused responsibility. When adding a Cypher feature, the change typically touches `sparrowdb-cypher` (parser/AST) and `sparrowdb-execution` (executor), with an integration test added to `crates/sparrowdb/tests/`. See [DEVELOPMENT.md](DEVELOPMENT.md) for the full contributor guide.

---

## License

MIT — see [LICENSE](LICENSE).

Copyright 2024 Rich Yaker
