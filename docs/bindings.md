# Language Bindings

SparrowDB provides native bindings for Rust, Python, Node.js, and Ruby. All bindings open the same database format — you can write from Python and read from Rust against the same files.

---

## Rust

Rust is the primary interface. Everything else is built on top of it.

### Install

```toml
# Cargo.toml
[dependencies]
sparrowdb = { git = "https://github.com/ryaker/SparrowDB" }
```

Crates.io publication is planned for v0.1.

### Open a database

```rust
use sparrowdb::GraphDb;

let db = GraphDb::open(std::path::Path::new("my.db"))?;

// Convenience free function (equivalent)
let db = sparrowdb::open(std::path::Path::new("my.db"))?;
```

### Execute Cypher

```rust
// Write (auto-commit)
db.execute("CREATE (n:Person {name: 'Alice', age: 30})")?;

// Read
let result = db.execute("MATCH (n:Person) RETURN n.name, n.age")?;
println!("{:?}", result.columns); // ["n.name", "n.age"]
for row in &result.rows {
    println!("{:?}", row); // [String("Alice"), Int64(30)]
}
```

### QueryResult

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}
```

### Value type

```rust
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    NodeRef(NodeId),
    EdgeRef(EdgeId),
    List(Vec<Value>),
}
```

Pattern match to extract:

```rust
for row in &result.rows {
    match &row[0] {
        Value::String(s) => println!("{s}"),
        Value::Int64(n)  => println!("{n}"),
        Value::Null      => println!("null"),
        _                => {}
    }
}
```

### Parameters

```rust
use std::collections::HashMap;
use sparrowdb::Value;

let mut params = HashMap::new();
params.insert("name".to_string(), Value::String("Alice".to_string()));
params.insert("min_age".to_string(), Value::Int64(25));

let result = db.execute_with_params(
    "MATCH (n:Person {name: $name}) WHERE n.age >= $min_age RETURN n.age",
    params,
)?;
```

### Transactions

```rust
// Read-only snapshot (pinned to current committed state)
let tx = db.begin_read()?;
println!("snapshot txn_id: {}", tx.snapshot_txn_id);

// Write transaction (at most one writer at a time)
let mut tx = db.begin_write()?;
tx.create_node(label_id, &[(col_id, Value::String("Alice".into()))])?;
let txn_id = tx.commit()?; // dropping without commit = rollback
```

`begin_write()` returns `Err(Error::WriterBusy)` if another writer is already open.

### Encryption

```rust
let key: [u8; 32] = /* load from keychain/env */  [0x42; 32];
let db = sparrowdb::GraphDb::open_encrypted(
    std::path::Path::new("secure.db"),
    key,
)?;
// Wrong key => Err(Error::DecryptionFailed)
```

### Maintenance

```rust
db.checkpoint()?; // flush WAL → base storage (call periodically)
db.optimize()?;   // checkpoint + sort adjacency lists (heavier)
```

### Error handling

```rust
use sparrowdb::Error;

match db.execute("...") {
    Ok(result) => { /* ... */ }
    Err(Error::InvalidArgument(msg)) => eprintln!("bad query: {msg}"),
    Err(Error::WriterBusy)           => eprintln!("concurrent write"),
    Err(Error::DecryptionFailed)     => eprintln!("wrong encryption key"),
    Err(Error::Unimplemented)        => eprintln!("feature not yet supported"),
    Err(e)                           => return Err(e),
}
```

---

## Python

The Python binding is a native extension built with [maturin](https://www.maturin.rs/) and [PyO3](https://pyo3.rs/). Requires Python 3.9+.

### Install

```bash
pip install sparrowdb
```

If the PyPI wheel is not available for your platform, build from source:

```bash
# Requires Rust stable 1.75+
pip install maturin
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB/crates/sparrowdb-python
maturin develop --release
```

### Open a database

```python
import sparrowdb

db = sparrowdb.GraphDb("/tmp/my.db")
```

### Execute Cypher

`execute()` returns a `list[dict]` — each dict maps column name to value.

```python
# Write
db.execute("CREATE (:Person {name: 'Alice', age: 30})")
db.execute("CREATE (:Person {name: 'Bob',   age: 25})")

# Connect them
db.execute(
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
    "CREATE (a)-[:KNOWS]->(b)"
)

# Read
rows = db.execute("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age")
for row in rows:
    print(row["n.name"], row["n.age"])
# Bob 25
# Alice 30

# Aggregate
rows = db.execute("MATCH (n:Person) RETURN COUNT(*), AVG(n.age)")
print(rows[0])  # {'count(*)': 2, 'avg(n.age)': 27.5}
```

**Python value mapping:**

| SparrowDB `Value` | Python type |
|-------------------|-------------|
| `Null` | `None` |
| `Bool` | `bool` |
| `Int64` | `int` |
| `Float64` | `float` |
| `String` | `str` |
| `List` | `list` |
| `NodeRef` / `EdgeRef` | `int` (packed ID) |

### Transactions

```python
# Read-only snapshot
tx = db.begin_read()
print(tx.snapshot_txn_id)  # int

# Write transaction
tx = db.begin_write()
txn_id = tx.commit()  # int; dropping without commit = rollback
```

### Maintenance

```python
db.checkpoint()
db.optimize()
```

### Full example

```python
import sparrowdb

db = sparrowdb.GraphDb("/tmp/people.db")

db.execute("CREATE (:Person {name: 'Alice', age: 30})")
db.execute("CREATE (:Person {name: 'Bob',   age: 25})")
db.execute("CREATE (:Person {name: 'Carol', age: 35})")

db.execute(
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
    "CREATE (a)-[:KNOWS]->(b)"
)
db.execute(
    "MATCH (a:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) "
    "CREATE (a)-[:KNOWS]->(c)"
)

# 2-hop friend-of-friend
fof = db.execute(
    "MATCH (:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(f:Person) "
    "RETURN DISTINCT f.name"
)
print([r["f.name"] for r in fof])  # ['Carol']

db.checkpoint()
```

### Parameters (Python)

Native `$param` binding via `execute_with_params` is not yet exposed in the Python binding. Use typed Python values to avoid injection — numeric and boolean literals carry no injection risk:

```python
age_threshold = 25
rows = db.execute(f"MATCH (n:Person) WHERE n.age > {int(age_threshold)} RETURN n.name")
```

String values should be escaped or handled at the application layer until `execute_with_params` is available.

---

## Node.js

The Node.js binding is a native addon built with [napi-rs](https://napi.rs/). TypeScript types are included. Requires Node.js 16+.

### Install

```bash
npm install sparrowdb
```

If building from source:

```bash
# Requires Rust stable 1.75+ and napi-rs CLI
npm install -g @napi-rs/cli
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB/crates/sparrowdb-node
napi build --platform --release
```

### Open a database

```typescript
import { SparrowDB } from 'sparrowdb'
// or:
const { SparrowDB } = require('sparrowdb')

const db = SparrowDB.open('/path/to/my.db')
```

### Execute Cypher

`execute()` returns `{ columns: string[], rows: object[] }`.

```typescript
// Write
db.execute("CREATE (:Person {name: 'Alice', age: 30})")
db.execute("CREATE (:Person {name: 'Bob',   age: 25})")

// Connect
db.execute(
  "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
  "CREATE (a)-[:KNOWS]->(b)"
)

// Read
const result = db.execute("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age")
console.log(result.columns)      // ["n.name", "n.age"]
for (const row of result.rows) {
  console.log(row['n.name'], row['n.age'])
}

// Aggregate
const stats = db.execute("MATCH (n:Person) RETURN COUNT(*), AVG(n.age)")
console.log(stats.rows[0])  // { 'count(*)': 2, 'avg(n.age)': 27.5 }
```

**JavaScript value mapping:**

| SparrowDB `Value` | JavaScript type | Notes |
|-------------------|-----------------|-------|
| `Null` | `null` | |
| `Bool` | `boolean` | |
| `Int64` | `number` | Values outside ±(2^53−1) returned as `string` |
| `Float64` | `number` | |
| `String` | `string` | |
| `NodeRef` | `{ $type: "node", id: string }` | |
| `EdgeRef` | `{ $type: "edge", id: string }` | |

### Transactions

```typescript
// Read-only snapshot (snapshotTxnId is a decimal string to preserve u64 range)
const tx = db.beginRead()
console.log(tx.snapshotTxnId)  // "42"

// Write transaction
const tx = db.beginWrite()
const txnId = tx.commit()  // string; dropping without commit = rollback
tx.rollback()              // explicit rollback
```

> `ReadTx.execute()` and `WriteTx.execute()` are planned but not yet implemented. Use `SparrowDB.execute()` for all queries in the meantime.

### Maintenance

```typescript
db.checkpoint()
db.optimize()
```

### Full TypeScript example

```typescript
import { SparrowDB } from 'sparrowdb'

const db = SparrowDB.open('/tmp/people.db')

db.execute("CREATE (:Person {name: 'Alice', age: 30})")
db.execute("CREATE (:Person {name: 'Bob',   age: 25})")
db.execute("CREATE (:Person {name: 'Carol', age: 35})")

db.execute(
  "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
  "CREATE (a)-[:KNOWS]->(b)"
)
db.execute(
  "MATCH (a:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) " +
  "CREATE (a)-[:KNOWS]->(c)"
)

// 2-hop friend-of-friend
const fof = db.execute(
  "MATCH (:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(f:Person) " +
  "RETURN DISTINCT f.name"
)
console.log(fof.rows.map(r => r['f.name']))  // ['Carol']

db.checkpoint()
```

### Parameters (Node.js)

Native `$param` binding is planned. Until then, use typed values:

```typescript
// Safe: numeric values carry no injection risk
const minAge = 25
const result = db.execute(
  `MATCH (n:Person) WHERE n.age > ${Number(minAge)} RETURN n.name`
)
```

---

## Ruby

The Ruby binding is a native gem built with [Magnus](https://github.com/matsadler/magnus). Requires Ruby 3.0+.

### Install

```bash
gem install sparrowdb
```

If building from source:

```bash
# Requires Rust stable 1.75+
git clone https://github.com/ryaker/SparrowDB
cd SparrowDB/crates/sparrowdb-ruby
bundle install
bundle exec rake compile
```

### Open a database

```ruby
require 'sparrowdb'

db = SparrowDB::GraphDb.new('/tmp/my.db')
```

### Execute Cypher

`execute` returns an array of hashes, each mapping column name to value.

```ruby
# Write
db.execute("CREATE (:Person {name: 'Alice', age: 30})")
db.execute("CREATE (:Person {name: 'Bob',   age: 25})")

# Connect
db.execute(
  "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " \
  "CREATE (a)-[:KNOWS]->(b)"
)

# Read
rows = db.execute("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age")
rows.each { |row| puts "#{row['n.name']} — age #{row['n.age']}" }
# Bob — age 25
# Alice — age 30

# Aggregate
rows = db.execute("MATCH (n:Person) RETURN COUNT(*), AVG(n.age)")
puts rows.first.inspect
# {"count(*)"=>2, "avg(n.age)"=>27.5}
```

### Full example

```ruby
require 'sparrowdb'

db = SparrowDB::GraphDb.new('/tmp/people.db')

db.execute("CREATE (:Person {name: 'Alice', age: 30})")
db.execute("CREATE (:Person {name: 'Bob',   age: 25})")
db.execute("CREATE (:Person {name: 'Carol', age: 35})")

db.execute(
  "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " \
  "CREATE (a)-[:KNOWS]->(b)"
)
db.execute(
  "MATCH (a:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) " \
  "CREATE (a)-[:KNOWS]->(c)"
)

fof = db.execute(
  "MATCH (:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(f:Person) " \
  "RETURN DISTINCT f.name"
)
puts fof.map { |r| r['f.name'] }.inspect  # ["Carol"]

db.checkpoint
```

### Maintenance

```ruby
db.checkpoint
db.optimize
```

---

## Comparing across bindings

| Capability | Rust | Python | Node.js | Ruby |
|------------|------|--------|---------|------|
| `execute(cypher)` | ✅ | ✅ | ✅ | ✅ |
| `execute_with_params` | ✅ | Planned | Planned | Planned |
| `begin_read()` | ✅ | ✅ | ✅ | — |
| `begin_write()` | ✅ | ✅ | ✅ | — |
| `ReadTx.execute()` | N/A | — | Planned | — |
| `WriteTx.execute()` | N/A | — | Planned | — |
| `checkpoint()` | ✅ | ✅ | ✅ | ✅ |
| `optimize()` | ✅ | ✅ | ✅ | ✅ |
| Encryption | ✅ | Planned | Planned | Planned |
| TypeScript types | N/A | N/A | ✅ | N/A |
