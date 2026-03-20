# SparrowDB

> An embedded Rust graph database with a durable storage format, factorized execution, and Cypher query support.

[![CI](https://github.com/ryaker/SparrowDB/actions/workflows/ci.yml/badge.svg)](https://github.com/ryaker/SparrowDB/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What It Is

SparrowDB is an embedded graph database written in Rust. It proves four things in v1:

1. **Durable storage** — creates and reopens a stable, versioned on-disk database
2. **Real queries** — runs a narrow but real Cypher subset over factorized operators
3. **Safe mutations** — mutates nodes and edges with WAL-backed crash recovery
4. **Accessible** — exposes the engine through both Rust and Python APIs

SparrowDB is **not** a general-purpose distributed database. v1 is a proof that the durable storage contract and factorized execution model are real enough to justify a broader roadmap.

---

## Status: Building in Public

This project is under active development. The v1 implementation has not shipped yet. We are working from a finalized spec and building phase by phase with integration gates.

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Repo bootstrap, CI, crate skeletons | 🔄 In progress |
| 1 | Durable core codecs (superblock, metapage) | ⏳ Pending |
| 2 | Catalog and nodes | ⏳ Pending |
| 3 | Edges, WAL, and recovery | ⏳ Pending |
| 4a | Parser and binder | ⏳ Pending |
| 4b | Scan, filter, project | ⏳ Pending |
| 4c | Expand, joins, aggregates | ⏳ Pending |
| 5 | Transactions and maintenance | ⏳ Pending |
| 6 | Encryption, spill, Python bindings | ⏳ Pending |

v1 is done when the 14 named acceptance checks pass (see `specs/`).

---

## Design Principles

- **Format stability over peak performance** — byte layouts are nailed down before code is written
- **Correctness over planner sophistication** — the execution model is explicit, not magic
- **Explicit testability** — golden binary fixtures, crash failpoints, and integration gates
- **Narrow but dependable public API** — small surface, zero dead modules

---

## Architecture at a Glance

```
crates/
  sparrowdb-common/      # ID types, error taxonomy, CRC32, encoding helpers
  sparrowdb-storage/     # Page codecs, buffer pool, WAL, node/edge file families
  sparrowdb-catalog/     # Labels, rel tables, schema, reverse index
  sparrowdb-cypher/      # Parser, AST, binder, logical/physical plans
  sparrowdb-execution/   # Typed vectors, factorized chunks, operators, joins, spill
  sparrowdb/             # Public Rust API — GraphDb, ReadTx, WriteTx
  sparrowdb-python/      # PyO3 bindings

specs/                   # Implementation spec (source of truth)
fixtures/
  golden/                # Binary reference fixtures for every durable format
  recovery/              # Crash-recovery test databases
  compatibility/         # Frozen v1.0 fixture for forward-compat tests
```

### Storage Layout

Each database is a directory:

```
db/
  catalog.bin            # Superblock + dual metapages + catalog payload pages
  nodes/{label_id}/      # Per-label column files, validity bitmaps, overflow
  edges/{rel_table_id}/  # CSR base files, delta log, edge ID columns
  wal/                   # WAL segments
```

### Factorized Execution

Queries execute over `FactorizedChunk` — a structure that delays materialization until the projection boundary. A chunk with `multiplicity = 2`, one flat group, and one unflat group of size 3 represents 6 logical rows without emitting 6 physical rows. This is the core of the execution model.

---

## Quickstart (once v1 ships)

```rust
use sparrowdb::{GraphDb, OpenOptions};

let db = GraphDb::open("my.db")?;
let mut tx = db.begin_write()?;

tx.execute("CREATE (a:Person {name: 'Alice'})")?;
tx.execute("CREATE (b:Person {name: 'Bob'})")?;
tx.execute("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)")?;
tx.commit()?;

let tx = db.begin_read()?;
let result = tx.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")?;
```

```python
import sparrowdb

db = sparrowdb.open("my.db")
with db.write() as tx:
    tx.execute("CREATE (a:Person {name: 'Alice'})")
    tx.execute("CREATE (b:Person {name: 'Bob'})")

with db.read() as tx:
    result = tx.query("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name")
    for row in result.rows:
        print(row)
```

---

## Cypher Support (v1)

Supported:

- `CREATE (n:Label {prop: value})`
- `CREATE (n:LabelA:LabelB)` — multi-label nodes
- `CREATE (a)-[:REL]->(b)` and `CREATE (a)-[:REL]-(b)` (undirected sugar)
- `MATCH (n:Label) RETURN ...`
- `MATCH ... WHERE n.prop = value RETURN ...`
- 2-hop patterns: `MATCH (a)-[:R]->(b)-[:R]->(c) RETURN ...`
- `DELETE`, `SET`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- Schema: `ALTER :Label ADD COLUMN name TYPE`
- Maintenance: `CHECKPOINT`, `OPTIMIZE`

Not in v1: `OPTIONAL MATCH`, variable-length paths, `ORDER BY`, `LIMIT`, `UNION`, `UNWIND`, subqueries.

---

## Spec

The implementation spec lives in `specs/sparrowdb-v3-implementation-spec.md`. It defines:

- byte-exact layout tables for every durable structure
- golden fixture requirements
- crash failpoint matrix
- 14 acceptance checks that constitute "done"

---

## Development

```bash
# Build
cargo build --workspace

# Test
cargo test --workspace

# Lint
cargo clippy -- -D warnings
cargo fmt --check
```

See [DEVELOPMENT.md](DEVELOPMENT.md) for full setup instructions.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues and discussions welcome. PRs require passing CI and golden fixtures for any new durable format.

---

## License

MIT — see [LICENSE](LICENSE).
