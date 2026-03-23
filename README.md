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

SparrowDB runs inside your process. Drop it into a Rust app, a Python script, or a Node.js service and query your graph with Cypher. Data lives on disk, survives crashes, and never phones home.

---

## Install

**Rust (crates.io)**

```toml
# Cargo.toml
[dependencies]
sparrowdb = "0.1"
```

**Or from git (latest)**

```toml
[dependencies]
sparrowdb = { git = "https://github.com/ryaker/SparrowDB" }
```

Python and Node.js packages are available — see [docs/bindings.md](docs/bindings.md).

---

## 5-minute quickstart

```rust
use sparrowdb::GraphDb;

fn main() -> sparrowdb::Result<()> {
    // Open (or create) a database directory
    let db = GraphDb::open(std::path::Path::new("my.db"))?;

    // Create nodes
    db.execute("CREATE (alice:Person {name: 'Alice', age: 30})")?;
    db.execute("CREATE (bob:Person   {name: 'Bob',   age: 25})")?;
    db.execute("CREATE (carol:Person {name: 'Carol', age: 35})")?;

    // Create a relationship
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
         CREATE (a)-[:KNOWS]->(b)",
    )?;

    // Query Alice's friends
    let result = db.execute(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(f:Person)
         RETURN f.name",
    )?;
    for row in &result.rows {
        println!("{:?}", row); // ["Bob"]
    }

    // Aggregate
    let stats = db.execute(
        "MATCH (p:Person) RETURN COUNT(*), AVG(p.age), MAX(p.age)",
    )?;
    println!("{:?}", stats.rows[0]); // [3, 30.0, 35]

    // Paginate
    let page = db.execute(
        "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 0 LIMIT 10",
    )?;

    Ok(())
}
```

Full walkthrough: [docs/quickstart.md](docs/quickstart.md)

---

## What Cypher is supported

| Feature | Status |
|---------|--------|
| `CREATE (n:Label {prop: val})` | ✅ |
| `MATCH (n:Label) RETURN n.prop` | ✅ |
| `MATCH (n)` — scan all labels | ✅ |
| `WHERE` with `=`, `<>`, `<`, `<=`, `>`, `>=` | ✅ |
| `WHERE n.prop CONTAINS str` | ✅ |
| `WHERE n.prop IS NULL` / `IS NOT NULL` | ✅ |
| 1-hop `(a)-[:REL]->(b)` | ✅ |
| Multi-hop `(a)-[:R]->()-[:R]->(c)` | ✅ |
| Undirected `(a)-[:REL]-(b)` | ✅ |
| Variable-length paths `[:R*1..3]` | ✅ |
| `RETURN DISTINCT` | ✅ |
| `ORDER BY`, `LIMIT`, `SKIP` | ✅ |
| `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)` | ✅ |
| `SUM`, `AVG`, `MIN`, `MAX`, `collect()` | ✅ |
| `WITH … AS … WHERE` pipeline | ✅ |
| `UNWIND list AS var` | ✅ |
| `OPTIONAL MATCH` | ✅ |
| `UNION` / `UNION ALL` | ✅ |
| `MERGE` (upsert node) | ✅ |
| `SET`, `DELETE` | ✅ |
| `CREATE (a)-[:REL]->(b)` — edge creation | ✅ |
| `CASE WHEN … THEN … ELSE … END` | ✅ |
| `EXISTS { (n)-[:REL]->(:Label) }` | ✅ |
| `shortestPath((a)-[:R*]->(b))` | ✅ |
| `ANY / ALL / NONE / SINGLE` list predicates | ✅ |
| `id(n)`, `labels(n)`, `type(r)` | ✅ |
| `size()`, `range()`, `toInteger()`, `toString()` | ✅ |
| `toUpper()`, `toLower()`, `trim()`, `replace()`, `substring()` | ✅ |
| `abs()`, `ceil()`, `floor()`, `sqrt()`, `sign()` | ✅ |
| `CALL db.index.fulltext.queryNodes` | ✅ |
| `CALL db.schema()` | ✅ |
| Parameters `$param` | ✅ |
| MVCC write-write conflict detection | ✅ |
| `coalesce()` | ⚠️ Not yet implemented |
| Multi-label nodes `(n:A:B)` | ⚠️ Not yet implemented |
| Subqueries `CALL { … }` | ⚠️ Partial |

Full reference: [docs/cypher-reference.md](docs/cypher-reference.md)

---

## Bindings

| Language | Install | Docs |
|----------|---------|------|
| **Rust** | `sparrowdb = "0.1"` (crates.io) | [docs/bindings.md#rust](docs/bindings.md#rust) |
| **Python** | `pip install sparrowdb` (or maturin) | [docs/bindings.md#python](docs/bindings.md#python) |
| **Node.js** | `npm install sparrowdb` | [docs/bindings.md#nodejs](docs/bindings.md#nodejs) |
| **Ruby** | `gem install sparrowdb` (or rake compile) | [docs/bindings.md#ruby](docs/bindings.md#ruby) |

---

## CLI

```bash
# One-shot query
sparrowdb query --db my.db "MATCH (n:Person) RETURN n.name LIMIT 5"

# Compact the WAL
sparrowdb checkpoint --db my.db

# Database info
sparrowdb info --db my.db
```

Build the CLI:
```bash
cargo build --release --bin sparrowdb
```

## MCP server

`sparrowdb-mcp` speaks JSON-RPC 2.0 over stdio — plug it into Claude Code or any MCP client:

```bash
cargo build --release --bin sparrowdb-mcp
```

Tools exposed: `execute_cypher`, `checkpoint`, `info`.

---

## Guides

| Guide | Description |
|-------|-------------|
| [docs/quickstart.md](docs/quickstart.md) | Step-by-step first graph |
| [docs/cypher-reference.md](docs/cypher-reference.md) | Full Cypher support reference |
| [docs/bindings.md](docs/bindings.md) | Rust, Python, Node.js, Ruby |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Contributor workflow, architecture |

---

## Performance

SparrowDB is built for low-overhead, in-process graph workloads:

| Technique | What it means |
|-----------|---------------|
| **Factorized execution** | Multi-hop traversals avoid materialising O(N²) intermediate rows — friend-of-friend queries stay fast at scale |
| **B-tree property index** | Range predicates (`WHERE age > 30`) use a sorted B-tree, not full scans |
| **Inverted text index** | Full-text search via `CALL db.index.fulltext.queryNodes` without an external search engine |
| **WAL-backed durability** | Writes commit to the write-ahead log; no `fsync` storm on every property update |
| **SWMR concurrency** | Single-writer, multiple-reader — readers never block writers; snapshot isolation at no extra cost |

---

## Why SparrowDB

- **Embedded** — links into your binary; zero infrastructure overhead
- **Durable** — WAL-backed crash recovery; survives `kill -9`
- **Encrypted** — optional XChaCha20-Poly1305 per-page encryption; wrong key returns an error, never silently decrypts garbage
- **Factorized execution** — 2-hop friend-of-friend queries don't materialise O(N²) intermediate rows
- **Multi-binding** — same database file, same Cypher, from Rust/Python/Node.js/Ruby

---

## License

MIT — see [LICENSE](LICENSE).
