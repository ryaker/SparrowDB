# Getting Started with SparrowDB

## Prerequisites

- Rust stable 1.75 or later ([rustup.rs](https://rustup.rs))
- Git

## Install from Source

```bash
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB
cargo build --release
```

The compiled library is at `target/release/libsparrowdb.rlib`. The fixture
generator binary is at `target/release/gen-fixtures`.

## Add as a Library Dependency

```toml
# In your project's Cargo.toml
[dependencies]
sparrowdb = { git = "https://github.com/ryaker/SparrowDB" }
```

Crates.io publication is planned for the v0.1 release.

## Your First Graph

Create a new Rust project and add sparrowdb as a dependency, then:

```rust
use sparrowdb::Engine;

fn main() -> sparrowdb::Result<()> {
    // Open (or create) a database directory
    let engine = Engine::open("my_graph.db")?;

    // Create nodes
    engine.execute("CREATE (alice:Person {name: \"Alice\", age: 30})")?;
    engine.execute("CREATE (bob:Person {name: \"Bob\", age: 25})")?;
    engine.execute("CREATE (carol:Person {name: \"Carol\", age: 35})")?;

    // Create edges
    engine.execute(
        "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"})
         CREATE (a)-[:KNOWS]->(b)"
    )?;
    engine.execute(
        "MATCH (a:Person {name: \"Bob\"}), (c:Person {name: \"Carol\"})
         CREATE (a)-[:KNOWS]->(c)"
    )?;

    // Query: Alice's friends
    let result = engine.execute(
        "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(f:Person)
         RETURN f.name"
    )?;
    println!("Alice's friends:");
    for row in &result.rows {
        println!("  {:?}", row);
    }

    // Query: friends-of-friends
    let fof = engine.execute(
        "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
         RETURN DISTINCT fof.name"
    )?;
    println!("Friends of Alice's friends:");
    for row in &fof.rows {
        println!("  {:?}", row);
    }

    Ok(())
}
```

Run it:

```bash
cargo run
```

The database is persisted to `my_graph.db/` in the current directory. Reopen
the same path to continue using the same data.

## Encrypted Database

Pass a 32-byte key to enable XChaCha20-Poly1305 page encryption:

```rust
use sparrowdb::Engine;

let key = [0x42u8; 32]; // use a real key in production
let engine = Engine::open_encrypted("secure.db", key)?;

engine.execute("CREATE (k:Knowledge {content: \"secret\"})")?;

// Opening with the wrong key returns Err(Error::DecryptionFailed)
```

## Running Tests

```bash
# All tests (unit + integration + acceptance)
cargo test --workspace

# Just the acceptance suite
cargo test --workspace --test acceptance -- --nocapture

# Specific use-case integration test
cargo test --workspace --test uc1_social_graph
```

## Generating Test Fixtures

The fixture generator produces seeded, deterministic datasets:

```bash
cargo run --bin gen-fixtures -- --seed 42 --out tests/fixtures/
```

This creates:
- `social_10k.json` — 10,000 Person nodes, 50,000 KNOWS edges (power-law degree)
- `social_100k.json` — scale test dataset
- `deps_500.json` — 500 Package nodes, 2,000 DEPENDS_ON edges (DAG)
- `concepts_1k.json` — 1,000 Knowledge nodes for KMS workloads

## Benchmarks

```bash
# Run all storage benchmarks
cargo bench -p sparrowdb-storage

# Run with bencher output (matches CI format)
cargo bench -p sparrowdb-storage -- --output-format bencher
```

Benchmarks cover WAL append, metapage encode/decode, CSR neighbor lookup, and
CRC32C throughput.

## What's Next

- [API Reference](api-reference.md) — full `Engine` API and query result types
- [Cypher Reference](cypher-reference.md) — all supported Cypher with examples
- [DEVELOPMENT.md](../DEVELOPMENT.md) — contributor workflow and architecture deep-dive
