# Quickstart

Everything you need to go from zero to a running graph in under five minutes.

---

## 1. Add the dependency

```toml
# Cargo.toml
[dependencies]
sparrowdb = { git = "https://github.com/ryaker/SparrowDB" }
```

Crates.io publication is planned for v0.1. Python and Node.js — see [bindings.md](bindings.md).

---

## 2. Open a database

```rust
use sparrowdb::GraphDb;

let db = GraphDb::open(std::path::Path::new("my.db"))?;
```

`GraphDb::open` creates the directory if it doesn't exist, or reopens an existing one. The database is fully durable — a clean process exit or `kill -9` will not lose committed data.

There is also a convenience free function:

```rust
let db = sparrowdb::open(std::path::Path::new("my.db"))?;
```

---

## 3. Create nodes

Property values can be strings, integers, floats, or booleans.

```rust
db.execute("CREATE (n:Person {name: 'Alice', age: 30})")?;
db.execute("CREATE (n:Person {name: 'Bob',   age: 25})")?;
db.execute("CREATE (n:Person {name: 'Carol', age: 35})")?;
```

```cypher
-- Multiple labels are not yet supported; use one label per node
CREATE (n:Article {title: 'Getting started', published: true, score: 4.5})
```

**Supported property types:**

| Cypher literal | Rust `Value` |
|----------------|-------------|
| `'hello'` | `Value::String` |
| `42` | `Value::Int64` |
| `3.14` | `Value::Float64` |
| `true` / `false` | `Value::Bool` |
| `null` | `Value::Null` |

---

## 4. Create relationships

Match the endpoints first, then create the edge:

```rust
db.execute(
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
     CREATE (a)-[:KNOWS]->(b)",
)?;

db.execute(
    "MATCH (a:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
     CREATE (a)-[:KNOWS]->(c)",
)?;
```

Relationships can also carry properties:

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020}]->(b)
```

---

## 5. Query with MATCH

```rust
// All people
let result = db.execute("MATCH (n:Person) RETURN n.name, n.age")?;
println!("{:?}", result.columns); // ["n.name", "n.age"]

for row in &result.rows {
    println!("{:?}", row); // [String("Alice"), Int64(30)]
}
```

### Filter with WHERE

```cypher
MATCH (n:Person) WHERE n.age > 28 RETURN n.name
MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n.name
MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name
```

Supported operators: `=`, `<>`, `<`, `<=`, `>`, `>=`, `CONTAINS`, `IN`, `IS NULL`, `IS NOT NULL`.

### Inline property filter (shorthand)

```cypher
MATCH (n:Person {name: 'Alice'}) RETURN n.age
```

Equivalent to `WHERE n.name = 'Alice'`. Multiple properties are ANDed.

### Aliases

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name AS person, f.name AS friend
```

---

## 6. Traverse relationships

### 1-hop

```rust
let result = db.execute(
    "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(f:Person) RETURN f.name",
)?;
```

### Multi-hop

```cypher
-- 2-hop friend-of-friend
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
RETURN DISTINCT fof.name

-- Variable-length: 1 to 3 hops
MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(f:Person)
RETURN DISTINCT f.name

-- Undirected (either direction)
MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(b:Person)
RETURN b.name
```

SparrowDB uses a factorized execution engine — multi-hop traversals never materialise O(N²) intermediate rows.

### Capture the relationship

```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a.name, type(r), b.name
```

### Shortest path

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'})
RETURN shortestPath((a)-[:KNOWS*]->(b))
-- Returns the path length as an integer
```

---

## 7. Aggregation

```rust
let stats = db.execute(
    "MATCH (p:Person) RETURN COUNT(*), AVG(p.age), MIN(p.age), MAX(p.age)",
)?;
println!("{:?}", stats.rows[0]); // [Int64(3), Float64(30.0), Int64(25), Int64(35)]
```

```cypher
-- Count matched rows
MATCH (p:Person) RETURN COUNT(*)
MATCH (p:Person) RETURN COUNT(p)
MATCH (p:Person) RETURN COUNT(DISTINCT p.age)

-- Sum and average
MATCH (p:Person) RETURN SUM(p.age), AVG(p.age)

-- Collect into a list
MATCH (p:Person) RETURN collect(p.name)
-- Returns Value::List(["Alice", "Bob", "Carol"])
```

### GROUP BY (implicit)

In Cypher, non-aggregate columns automatically become grouping keys:

```cypher
-- Count friends per person
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name, COUNT(f) AS friend_count
ORDER BY friend_count DESC
```

---

## 8. Sort, skip, limit

```cypher
-- Top 5 by age, descending
MATCH (n:Person) RETURN n.name, n.age
ORDER BY n.age DESC
LIMIT 5

-- Page 2 (items 11-20)
MATCH (n:Person) RETURN n.name
ORDER BY n.name
SKIP 10 LIMIT 10
```

---

## 9. WITH pipelines

`WITH` materialises an intermediate result and lets you filter or rename before `RETURN`:

```cypher
-- Rename a column
MATCH (n:Person) WITH n.name AS name RETURN name

-- Filter mid-query
MATCH (n:Person)
WITH n.name AS name, n.age AS age
WHERE age > 28
RETURN name, age ORDER BY age DESC
```

---

## 10. UNWIND

Expand a list into rows:

```cypher
UNWIND [1, 2, 3] AS x RETURN x

-- range() produces a list
UNWIND range(1, 5) AS n RETURN n
```

---

## 11. Mutations

### MERGE (upsert)

```cypher
-- Create if not exists, match if it does
MERGE (n:Person {name: 'Alice'})
RETURN n.name
```

### SET (update properties)

```cypher
MATCH (n:Person {name: 'Alice'})
SET n.age = 31
```

### DELETE

```cypher
-- Delete a node (delete its edges first)
MATCH (n:Person {name: 'Alice'})
DELETE n
```

---

## 12. Parameters

Avoid string concatenation — use `$param` placeholders:

```rust
use std::collections::HashMap;
use sparrowdb::{GraphDb, Value};

let mut params = HashMap::new();
params.insert("name".to_string(), Value::String("Alice".to_string()));
params.insert("age".to_string(), Value::Int64(30));

db.execute_with_params(
    "CREATE (n:Person {name: $name, age: $age})",
    params.clone(),
)?;

let result = db.execute_with_params(
    "MATCH (n:Person {name: $name}) RETURN n.age",
    params,
)?;
```

---

## 13. Encryption

Pass a 32-byte key to enable XChaCha20-Poly1305 page encryption:

```rust
let key: [u8; 32] = /* load from keychain / env */ [0x42; 32];
let db = sparrowdb::GraphDb::open_encrypted(
    std::path::Path::new("secure.db"),
    key,
)?;
```

Opening with the wrong key returns `Err(Error::DecryptionFailed)`. No key means transparent passthrough.

---

## 14. Maintenance

Call `checkpoint()` periodically in long-running processes to prevent WAL growth:

```rust
db.checkpoint()?;  // flush WAL into base storage
db.optimize()?;    // checkpoint + sort adjacency lists (heavier; maintenance window)
```

Or from the CLI:

```bash
sparrowdb checkpoint --db my.db
```

---

## Error handling

All operations return `sparrowdb::Result<T>`:

```rust
match db.execute("MATCH (n:Ghost) RETURN n") {
    Ok(result) => { /* ... */ }
    Err(sparrowdb::Error::InvalidArgument(msg)) => eprintln!("Schema error: {msg}"),
    Err(sparrowdb::Error::WriterBusy) => eprintln!("Concurrent write in progress"),
    Err(e) => return Err(e),
}
```

Unsupported Cypher features return `Err(Error::Unimplemented)` — never a panic.

---

## Next steps

- [cypher-reference.md](cypher-reference.md) — complete Cypher reference with all supported syntax
- [bindings.md](bindings.md) — use SparrowDB from Python, Node.js, or Ruby
- [DEVELOPMENT.md](../DEVELOPMENT.md) — architecture deep-dive, contributor workflow
