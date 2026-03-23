# Changelog

All notable changes to SparrowDB are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased] — v0.1.1

### Added

#### Cypher language
- `coalesce(expr1, expr2, …)` — returns the first non-null expression (SPA-240)
- `WITH … MATCH` and `WITH … UNWIND` multi-clause pipelines (SPA-134)
- `EXISTS { … }` predicate inside `WITH … WHERE` clauses (SPA-134)
- `UNWIND list AS var MATCH (n {id: var})` — UNWIND then MATCH lookup pipeline (SPA-237)
- `MERGE (a)-[:REL]->(b)` — idempotent relationship creation (SPA-172)

#### Engine & performance
- `execute_with_timeout(cypher, duration)` — per-query cancellation without killing the process (SPA-254)
- `ORDER BY` external merge sort — large result sets spill sorted runs to disk and merge them, avoiding unbounded heap allocation (SPA-100)
- `TextIndex` acceleration — `CONTAINS` and `STARTS WITH` predicates routed through an inverted index rather than full label scans (SPA-251)
- `PropertyIndex` in-memory B-tree — equality lookups now `O(log n)` instead of full scans (SPA-249)
- `execute_batch()` — group multiple write statements into a single WAL `fsync` (SPA-250)

#### API
- `GraphDb::export_dot()` — export the graph in DOT format for Graphviz rendering (SPA-149)
- `sparrowdb visualize --db <path>` CLI subcommand (SPA-149)
- WAL payload encryption with XChaCha20-Poly1305 when a database key is set (SPA-98)

#### Python bindings
- `with sparrowdb.GraphDb(path) as db:` context manager (SPA-103)
- GIL released during `execute()`, `checkpoint()`, and `optimize()` — safe to use from multiple threads simultaneously (SPA-256)

### Fixed
- Undirected `MATCH (a)-[:R]-(b)` now returns traversals in both directions (SPA-257)
- Undirected `MATCH` with `GROUP BY` correctly includes target-only nodes (SPA-241)
- `MATCH` on a nonexistent label returns 0 rows instead of an error (SPA-245)
- Variable-length path engine now applies destination property filters correctly (SPA-262)
- `CALL db.index.fulltext.queryNodes … YIELD score` returns a numeric relevance score
- Boolean property filters now evaluate correctly in all contexts
- Float64 property roundtrip preserves exponent bits

---

## [0.1.0] — Initial release

### Added

#### Core graph engine
- Embedded graph database — links directly into Rust, Python, Node.js, or Ruby processes
- WAL-backed durability with crash recovery (`kill -9` safe)
- SWMR (single-writer, multiple-reader) concurrency with snapshot isolation
- Optional XChaCha20-Poly1305 per-page encryption; wrong key returns an error

#### Cypher support
- `CREATE (n:Label {prop: val})` — node creation with properties
- `MATCH (n:Label) RETURN n.prop` — label scan with property projection
- `WHERE` with comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`)
- `WHERE n.prop CONTAINS str` / `STARTS WITH str` / `IS NULL` / `IS NOT NULL`
- 1-hop and multi-hop relationship traversal: `(a)-[:REL]->(b)-[:REL2]->(c)`
- Undirected match: `(a)-[:REL]-(b)`
- Variable-length paths: `[:R*1..3]`
- `RETURN DISTINCT`, `ORDER BY`, `LIMIT`, `SKIP`
- Aggregation: `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)`, `SUM`, `AVG`, `MIN`, `MAX`, `collect()`
- `WITH … AS … WHERE` pipeline
- `UNWIND list AS var`
- `OPTIONAL MATCH`
- `UNION` / `UNION ALL`
- `MERGE` — upsert node by label + property
- `SET`, `DELETE`
- `CREATE (a)-[:REL]->(b)` — relationship creation
- `CASE WHEN … THEN … ELSE … END`
- `EXISTS { (n)-[:REL]->(:Label) }` subquery predicate
- `shortestPath((a)-[:R*]->(b))`
- `ANY / ALL / NONE / SINGLE` list predicates
- `id(n)`, `labels(n)`, `type(r)` introspection functions
- Scalar functions: `size()`, `range()`, `toInteger()`, `toString()`, `toUpper()`, `toLower()`, `trim()`, `replace()`, `substring()`
- Math functions: `abs()`, `ceil()`, `floor()`, `sqrt()`, `sign()`
- `CALL db.index.fulltext.queryNodes` — full-text search without an external engine
- `CALL db.schema()` — introspect labels, relationship types, and properties
- Named query parameters (`$param`)
- MVCC write-write conflict detection

#### API
- `GraphDb::open(path)` — open or create a database
- `GraphDb::execute(cypher)` → `QueryResult` with named columns and typed rows
- `GraphDb::checkpoint()` — compact the WAL
- `GraphDb::optimize()` — rebuild indexes
- Criterion benchmark suite covering CREATE, MATCH, traversal, and aggregation workloads
- Prometheus metrics exporter

#### CLI (`sparrowdb` binary)
- `query` — one-shot Cypher query
- `checkpoint` — WAL compaction
- `info` — database statistics
- Neo4j CSV import bridge (`sparrowdb import-csv`) for bulk loading APOC exports

#### MCP server (`sparrowdb-mcp`)
- JSON-RPC 2.0 over stdio; compatible with Claude Code and any MCP client
- Tools: `execute_cypher`, `checkpoint`, `info`, `create_entity`, `add_property`

#### Language bindings
- **Rust** — native crate (`sparrowdb = "0.1"` on crates.io)
- **Python** — PyO3/maturin wheel (`pip install sparrowdb`)
- **Node.js** — napi-rs native addon (`npm install sparrowdb`)
- **Ruby** — Magnus/rb-sys extension (`gem install sparrowdb`)

#### Compatibility
- v1.0 on-disk format frozen; forward-compatibility fixture test suite included
- MVCC snapshot isolation guarantees identical read results across binding languages
