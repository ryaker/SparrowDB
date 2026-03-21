# SparrowDB

> An embedded Rust graph database with Cypher queries, WAL-backed crash recovery, and factorized execution.

[![CI](https://github.com/ryaker/SparrowDB/actions/workflows/ci.yml/badge.svg)](https://github.com/ryaker/SparrowDB/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What It Is

SparrowDB is an embedded graph database written in Rust. Drop it into a process — no server, no daemon, no cloud dependency.

It runs Cypher queries over a durable on-disk graph using a factorized execution engine that avoids Cartesian blowup on multi-hop traversals. The storage format is byte-stable and versioned.

**First production target:** replace Neo4j Aura in a local Knowledge Management System — same Cypher, local on-device, zero latency, no subscription.

---

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Repo, CI, 7-crate workspace scaffold | ✅ Done |
| 1 | TLV catalog codec + dual metapage | ✅ Done |
| 2 | WAL write-ahead log + 8 crash failpoints | ✅ Done |
| 3 | CSR edge storage + node property columns | ✅ Done |
| 4 | Cypher parser, binder, factorized execution engine | ✅ Done |
| 5 | SWMR transactions, snapshot isolation, CHECKPOINT/OPTIMIZE | ✅ Done |
| 6 | XChaCha20-Poly1305 encryption, WAL payload encryption, golden fixtures | ✅ Done |
| infra | Criterion benchmarks + deterministic fixture generator | ✅ Done |
| 6 (cont.) | Spill-to-disk (ORDER BY + ASP-Join), PyO3 Python bindings, CLI + MCP server | ✅ Done |
| 7 | Mutation Cypher: MERGE, SET, DELETE, CREATE edge, WAL records, MVCC | 🔄 In Review |
| 8–11 | UNWIND, variable paths, function library, LDBC SNB, Neo4j import, publication | ⏳ Planned |

Acceptance checks passing: 1-hop scan, 2-hop ASP-Join, WAL crash recovery, CHECKPOINT/OPTIMIZE, snapshot isolation, encryption auth, spill-to-disk sort + join, Python binding round-trip.

---

## Quickstart

```rust
use sparrowdb::Engine;

// Open or create a database directory
let engine = Engine::open("my.db")?;

// Create nodes
engine.execute("CREATE (a:Person {name: \"Alice\"})")?;
engine.execute("CREATE (b:Person {name: \"Bob\"})")?;

// Create an edge
engine.execute(
    "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"})
     CREATE (a)-[:KNOWS]->(b)"
)?;

// 1-hop query
let result = engine.execute(
    "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(f:Person) RETURN f.name"
)?;
for row in &result.rows {
    println!("{:?}", row);
}

// 2-hop query (factorized, no Cartesian blowup)
let fof = engine.execute(
    "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
     RETURN DISTINCT fof.name"
)?;
```

Full guide: [docs/getting-started.md](docs/getting-started.md)

---

## Cypher Support (v0.1)

| Feature | Supported |
|---------|-----------|
| `CREATE (n:Label {prop: val})` | ✅ |
| `MATCH (n:Label) RETURN n.prop` | ✅ |
| `WHERE n.prop = val` | ✅ |
| `WHERE n.prop CONTAINS str` | ✅ |
| 1-hop `MATCH (a)-[:REL]->(b)` | ✅ |
| 2-hop `MATCH (a)-[:R]->()-[:R]->(c)` | ✅ |
| Relationship variables `[r:REL]` | ✅ |
| `RETURN DISTINCT` | ✅ |
| `ORDER BY`, `LIMIT` | ✅ |
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | ✅ |
| Parameters `$param` | ✅ |
| `DELETE`, `SET` | ✅ |
| `OPTIONAL MATCH` | ❌ |
| Variable-length paths `[:R*1..3]` | ❌ |
| `UNION`, `UNWIND`, subqueries | ❌ |

Full reference: [docs/cypher-reference.md](docs/cypher-reference.md)

---

## Architecture

```
crates/
  sparrowdb-common/     # ID types, Error enum, CRC32C helpers
  sparrowdb-storage/    # WAL, CSR, node/edge stores, metapage, encryption
  sparrowdb-catalog/    # Label and rel-table registry (TLV on-disk format)
  sparrowdb-cypher/     # Lexer, recursive-descent parser, AST, binder
  sparrowdb-execution/  # TypedVector, FactorizedChunk, operators, ASP-Join, spill
  sparrowdb/            # Public Rust API (GraphDb entry point)
  sparrowdb-python/     # PyO3 bindings (maturin wheel)
  sparrowdb-cli/        # `sparrowdb` CLI binary (query/checkpoint/info)
  sparrowdb-mcp/        # JSON-RPC 2.0 MCP server over stdio

tests/
  fixtures/             # Golden binary fixtures + small seeded JSON datasets
  integration/          # End-to-end use-case tests (UC-1 through UC-6)

benches/                # Criterion benchmarks (WAL, CSR, metapage, CRC32C)
specs/                  # Byte-exact implementation spec (source of truth)
docs/                   # User-facing guides and API reference
```

### Database directory layout

```
my.db/
  catalog.bin           # TLV catalog entries + dual metapages
  nodes/{label_id}/     # Per-label property column files
  edges/{rel_id}/       # CSR forward + backward + delta.log
  wal/                  # WAL segments (000000.wal, ...)
```

### Factorized execution

Queries return `FactorizedChunk` — a structure that tracks `multiplicity` rather than materializing repeated rows. A 2-hop friend-of-friend query over 10 k nodes never emits O(N²) intermediate rows; multiplicity is resolved only at `RETURN`.

### Encryption

Pass a 32-byte key to get XChaCha20-Poly1305 encryption of every page. Physical stride is `page_size + 40` bytes (`24` nonce + `16` AEAD tag). Wrong key → `Error::EncryptionAuthFailed`. No key → transparent passthrough. WAL records are also encrypted per-record with LSN as AAD.

---

## Installation

### From source (Rust stable 1.75+)

```bash
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB
cargo build --release
```

### As a library dependency

```toml
# Cargo.toml
[dependencies]
sparrowdb = { git = "https://github.com/ryaker/SparrowDB" }
```

Crates.io publication planned for v0.1 release.

Full install guide: [docs/getting-started.md](docs/getting-started.md)

---

## Python Bindings

Install via maturin (requires Rust):

```bash
cd crates/sparrowdb-python
pip install maturin
maturin develop
```

Usage:

```python
import sparrowdb

# Open or create a database
db = sparrowdb.GraphDb("/tmp/my.db")

# Execute Cypher — returns list of dicts
rows = db.execute("MATCH (n:Person) RETURN n.name LIMIT 5")
for row in rows:
    print(row)

# Transactions
with db.begin_write() as tx:
    tx.commit()

txn_id = db.begin_read().snapshot_txn_id

# Maintenance
db.checkpoint()
db.optimize()
```

Python 3.9+ supported via stable ABI wheel (`abi3`).

---

## Development

```bash
cargo build --workspace                          # build everything
cargo test --workspace                           # all tests
cargo bench -p sparrowdb-storage                 # benchmarks
cargo run --bin gen-fixtures -- --seed 42 \
  --out tests/fixtures/                          # regenerate datasets
cargo clippy --all-targets -- -D warnings        # lint
cargo fmt --all                                  # format
```

See [DEVELOPMENT.md](DEVELOPMENT.md) for worktree workflow, TDD conventions, and CI pipeline details.

---

## Documentation

| | |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Install, first run, compile from source |
| [docs/api-reference.md](docs/api-reference.md) | `Engine` API, `QueryResult`, error handling |
| [docs/cypher-reference.md](docs/cypher-reference.md) | Full Cypher support table with examples |
| [docs/use-cases.md](docs/use-cases.md) | Concrete workloads and perf targets |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Contributor workflow, TDD, CI |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs require passing CI, clippy, and fmt.
New durable formats require a committed golden binary fixture.

---

## License

MIT — see [LICENSE](LICENSE).
