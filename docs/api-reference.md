# SparrowDB API Reference

## Rust API

### `GraphDb`

The primary entry point. Opens or creates a database directory.

```rust
use sparrowdb::GraphDb;

// Open (or create) a database
let db = GraphDb::open(std::path::Path::new("path/to/db"))?;
```

There is also a convenience free function:

```rust
let db = sparrowdb::open(std::path::Path::new("path/to/db"))?;
```

#### `GraphDb::execute`

Execute a Cypher statement. Returns a `QueryResult`.

```rust
pub fn execute(&self, cypher: &str) -> Result<QueryResult>
```

**Write operations** (CREATE, MERGE, DELETE, SET) commit immediately.

**Read operations** (MATCH ... RETURN) return rows in the result.

```rust
// Write
db.execute("CREATE (n:Person {name: \"Alice\"})")?;

// Read
let result = db.execute("MATCH (n:Person) RETURN n.name")?;
```

#### `GraphDb::checkpoint`

Flush the WAL and compact the database. Call periodically in long-running
processes to bound WAL growth.

```rust
pub fn checkpoint(&self) -> Result<()>
```

#### `GraphDb::optimize`

Checkpoint + sort adjacency lists for faster traversal. Heavier than
`checkpoint`; best run during a maintenance window.

```rust
pub fn optimize(&self) -> Result<()>
```

#### `GraphDb::begin_read`

Open a read-only snapshot transaction pinned to the current committed state.
Multiple readers may coexist with an active writer.

```rust
pub fn begin_read(&self) -> Result<ReadTx>
```

#### `GraphDb::begin_write`

Open a write transaction. Only one writer may be active at a time; returns
`Error::WriterBusy` if another writer is already open.

```rust
pub fn begin_write(&self) -> Result<WriteTx<'_>>
```

---

### `ReadTx`

Read-only snapshot transaction. Sees only data committed at or before the
snapshot point; immune to concurrent writes.

```rust
pub struct ReadTx {
    pub snapshot_txn_id: u64,
    // ...
}
```

```rust
let tx = db.begin_read()?;
println!("Pinned to txn_id {}", tx.snapshot_txn_id);
```

---

### `WriteTx<'db>`

Write transaction. Commit explicitly; dropping without committing rolls back
all staged changes.

```rust
let mut tx = db.begin_write()?;
tx.create_node(label_id, &[(col_id, Value::String("Alice".into()))])?;
let txn_id = tx.commit()?;
```

Key methods:

| Method | Description |
|--------|-------------|
| `create_node(label_id, props)` | Stage a new node |
| `set_node_col(node_id, col_id, value)` | Stage a property update |
| `set_property(node_id, key, value)` | Stage a property update by name |
| `create_edge(src, rel_type, dst, props)` | Stage a new edge |
| `delete_node(node_id)` | Stage a node deletion |
| `create_label(name)` | Create a new label in the catalog |
| `commit(self)` | Commit — returns the new `TxnId` |

---

### `QueryResult`

```rust
pub struct QueryResult {
    /// Column names in RETURN order
    pub columns: Vec<String>,
    /// Materialized rows — each row has one Value per column
    pub rows: Vec<Vec<Value>>,
}
```

```rust
let result = db.execute("MATCH (n:Person) RETURN n.name, n.age")?;

println!("Columns: {:?}", result.columns); // ["n.name", "n.age"]
for row in &result.rows {
    let name = &row[0]; // Value::String("Alice")
    let age  = &row[1]; // Value::Int64(30)
    println!("{:?} is {:?}", name, age);
}
```

---

### `Value`

Property values returned from queries:

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

Pattern match to extract typed values:

```rust
for row in &result.rows {
    if let Value::String(name) = &row[0] {
        println!("Name: {}", name);
    }
}
```

---

### Cypher Parameter Binding

Use `$param` placeholders instead of string interpolation to safely pass
user-supplied values. Parameters are passed as a `HashMap<String, Value>`.

```rust
use std::collections::HashMap;
use sparrowdb::{GraphDb, Value};

let db = GraphDb::open(std::path::Path::new("my.db"))?;

let mut params = HashMap::new();
params.insert("name".to_string(), Value::String("Alice".to_string()));
params.insert("age".to_string(),  Value::Int64(30));

// Write with parameters
db.execute_with_params(
    "CREATE (n:Person {name: $name, age: $age})",
    params.clone(),
)?;

// Read with parameters
let result = db.execute_with_params(
    "MATCH (n:Person {name: $name}) RETURN n.name, n.age",
    params,
)?;
```

The `execute_with_params` signature:

```rust
pub fn execute_with_params(
    &self,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<QueryResult>
```

---

### Error Handling

All fallible operations return `sparrowdb::Result<T>` (`type Result<T> = std::result::Result<T, Error>`).

```rust
pub enum Error {
    Io(std::io::Error),
    Corruption(String),       // on-disk data is damaged
    InvalidMagic,             // wrong file magic bytes
    ChecksumMismatch,         // CRC32C verification failed
    VersionMismatch,          // format version not supported
    DecryptionFailed,         // wrong encryption key or tampered page
    InvalidArgument(String),  // bad caller input (unknown label, etc.)
    WriterBusy,               // begin_write() called while writer is active
    Unimplemented,            // feature not yet implemented
}
```

```rust
match db.execute("MATCH (n:Ghost) RETURN n") {
    Ok(result) => { /* ... */ }
    Err(sparrowdb::Error::InvalidArgument(msg)) => {
        eprintln!("Schema error: {}", msg);
    }
    Err(sparrowdb::Error::WriterBusy) => {
        eprintln!("Another write transaction is active");
    }
    Err(e) => return Err(e),
}
```

---

### Encryption API

```rust
use sparrowdb::encryption::EncryptionContext;

// Create context with key
let ctx = EncryptionContext::with_key([0x42u8; 32]);

// Encrypt a page (returns page_size + 40 bytes)
let encrypted = ctx.encrypt_page(page_id, &plaintext)?;

// Decrypt — validates page_id matches stored nonce (prevents page-swap attacks)
let decrypted = ctx.decrypt_page(page_id, &encrypted)?;

// Passthrough mode (no encryption)
let ctx = EncryptionContext::none();
```

---

## Python API

The Python extension module is named `sparrowdb`. Build it with
[maturin](https://www.maturin.rs/):

```bash
pip install maturin
cd crates/sparrowdb-python && maturin develop
```

Or install from PyPI once published:

```bash
pip install sparrowdb
```

### `GraphDb`

```python
import sparrowdb

db = sparrowdb.GraphDb("path/to/my.db")
```

#### `GraphDb.execute(cypher)` → `list[dict]`

Execute a Cypher statement. Returns a list of dicts where each dict maps
column name to value.

Supported Python value types: `None`, `int`, `float`, `bool`, `str`.
`NodeRef` and `EdgeRef` are returned as `int` (their packed id).
`List` values from `collect()` aggregation are returned as Python `list`.

```python
db.execute('CREATE (:Person {name: "Alice", age: 30})')

results = db.execute("MATCH (n:Person) RETURN n.name, n.age")
# [{"n.name": "Alice", "n.age": 30}]

for row in results:
    print(row["n.name"], row["n.age"])
```

#### `GraphDb.checkpoint()` → `None`

Flush the WAL and compact the database.

```python
db.checkpoint()
```

#### `GraphDb.optimize()` → `None`

Checkpoint + sort adjacency lists for faster traversal.

```python
db.optimize()
```

#### `GraphDb.begin_read()` → `ReadTx`

Open a read-only snapshot transaction.

```python
tx = db.begin_read()
print(tx.snapshot_txn_id)  # int — the committed txn_id this reader is pinned to
```

#### `GraphDb.begin_write()` → `WriteTx`

Open a write transaction. Raises `RuntimeError` if another writer is active.

```python
tx = db.begin_write()
txn_id = tx.commit()  # int
```

Dropping a `WriteTx` without calling `commit()` silently rolls it back.

### `ReadTx`

#### `tx.snapshot_txn_id` → `int`

The committed transaction ID this reader is pinned to.

### `WriteTx`

#### `tx.commit()` → `int`

Commit all staged changes. Returns the new transaction ID as an `int`.
Raises `RuntimeError` if already committed or rolled back.

### Python Example

```python
import sparrowdb

db = sparrowdb.GraphDb("/tmp/people.db")

# Write nodes
db.execute('CREATE (:Person {name: "Alice", age: 30})')
db.execute('CREATE (:Person {name: "Bob",   age: 25})')
db.execute(
    'MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) '
    'CREATE (a)-[:KNOWS]->(b)'
)

# Read
people = db.execute("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age")
for row in people:
    print(f"{row['n.name']} is {row['n.age']} years old")

# Explicit transaction (for txn_id tracking)
tx = db.begin_write()
txn_id = tx.commit()
print(f"committed at txn_id {txn_id}")

# Maintenance
db.checkpoint()
```

### Cypher Parameter Binding (Python)

Native `$param` binding via `execute_with_params` is not yet exposed in the
Python API. Until then, use typed Python values directly in f-strings to avoid
injection risk (numeric and boolean values cannot be injected):

```python
# Safe: integer/boolean values carry no injection risk
age_threshold = 25
results = db.execute(f"MATCH (n:Person) WHERE n.age > {int(age_threshold)} RETURN n.name")
```

---

## Node.js Bindings

The Node.js native addon (`sparrowdb.node`) is built with
[napi-rs](https://napi.rs/). It exposes `SparrowDB`, `ReadTx`, and `WriteTx`
classes.

### Building

```bash
cargo build --release -p sparrowdb-node
# or via napi-cli:
napi build --platform --release
```

The resulting `sparrowdb.node` binary is loaded at runtime:

```js
const { SparrowDB } = require('./sparrowdb.node')
```

### `SparrowDB`

Top-level database handle. Open with the static factory method `SparrowDB.open()`.

#### `SparrowDB.open(path)` → `SparrowDB`

Open (or create) a SparrowDB database at `path`. Throws if the directory
cannot be created or the database files are corrupt.

```typescript
const db = SparrowDB.open('/path/to/my.db')
```

#### `db.execute(cypher)` → `{ columns: string[], rows: object[] }`

Execute a Cypher query. Returns an object with:

- `columns` — array of column name strings in RETURN order
- `rows` — array of plain objects, each mapping column name to value

Supported value types:

| JS type | Notes |
|---------|-------|
| `null` | SparrowDB `Null` |
| `number` | `Int64` and `Float64`; `Int64` values outside ±(2^53−1) are returned as `string` |
| `boolean` | `Bool` |
| `string` | `String`, and large integers |
| `{ $type: "node", id: string }` | Node reference |
| `{ $type: "edge", id: string }` | Edge reference |

```typescript
db.execute('CREATE (:Person {name: "Alice", age: 30})')

const result = db.execute('MATCH (n:Person) RETURN n.name, n.age')
// result.columns  => ["n.name", "n.age"]
// result.rows     => [{ "n.name": "Alice", "n.age": 30 }]

for (const row of result.rows) {
  console.log(row['n.name'], row['n.age'])
}
```

#### `db.checkpoint()` → `void`

Flush the WAL and compact the database. Throws on I/O error.

```typescript
db.checkpoint()
```

#### `db.optimize()` → `void`

Checkpoint + sort adjacency lists for faster traversal.

```typescript
db.optimize()
```

#### `db.beginRead()` → `ReadTx`

Open a read-only snapshot transaction pinned to the current committed state.
Multiple readers may coexist with an active writer.

```typescript
const tx = db.beginRead()
console.log(tx.snapshotTxnId) // string (decimal u64)
```

#### `db.beginWrite()` → `WriteTx`

Open a write transaction. Throws `WriterBusy` if another writer is already open.

```typescript
const tx = db.beginWrite()
const txnId = tx.commit() // string (decimal u64)
```

---

### `ReadTx`

Read-only snapshot transaction. Obtained via `db.beginRead()`.

#### `tx.snapshotTxnId` → `string`

The committed transaction ID this reader is pinned to. Returned as a decimal
string to preserve the full `u64` range (JavaScript `Number` only safely
represents integers up to 2^53−1).

```typescript
const tx = db.beginRead()
console.log(`pinned to txn ${tx.snapshotTxnId}`)
```

> **Note:** `ReadTx.execute()` is not yet implemented. Snapshot-pinned query
> execution is tracked in SPA-100. Use `SparrowDB.execute()` to query the
> latest committed state.

---

### `WriteTx`

Write transaction. Obtained via `db.beginWrite()`. Commit explicitly;
dropping without committing rolls back all staged changes.

#### `tx.execute(cypher)` → `{ columns: string[], rows: object[] }`

Execute a Cypher mutation inside this transaction.

> **Note:** `WriteTx.execute()` is not yet implemented. Use
> `SparrowDB.execute()` for mutations (auto-commit mode). Explicit
> transactional mutations are planned.

#### `tx.commit()` → `string`

Commit all staged changes. Returns the new transaction ID as a decimal string.
Throws if the transaction was already committed or rolled back.

```typescript
const tx = db.beginWrite()
const txnId = tx.commit()
console.log(`committed at txn ${txnId}`)
```

#### `tx.rollback()` → `void`

Roll back all staged changes explicitly. Equivalent to dropping the
transaction without committing.

```typescript
const tx = db.beginWrite()
tx.rollback() // discard changes
```

---

### TypeScript Example

```typescript
import { SparrowDB } from 'sparrowdb'

// Open the database
const db = SparrowDB.open('/tmp/people.db')

// Write nodes (auto-commit)
db.execute('CREATE (:Person {name: "Alice", age: 30})')
db.execute('CREATE (:Person {name: "Bob",   age: 25})')
db.execute(
  'MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) ' +
  'CREATE (a)-[:KNOWS]->(b)'
)

// Read all people
const result = db.execute('MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age')
console.log('Columns:', result.columns)
// Columns: ["n.name", "n.age"]

for (const row of result.rows) {
  console.log(`${row['n.name']} is ${row['n.age']} years old`)
}
// Bob is 25 years old
// Alice is 30 years old

// Read a snapshot
const snap = db.beginRead()
console.log('snapshot txn:', snap.snapshotTxnId)

// Write transaction (for txn_id tracking / future transactional mutations)
const tx = db.beginWrite()
const txnId = tx.commit()
console.log('committed at txn:', txnId)

// Maintenance
db.checkpoint()
db.optimize()
```

### Cypher Parameter Binding (Node.js)

The `SparrowDB.execute()` method takes a raw Cypher string. Native `$param`
binding in the Node.js API is planned. Until then, use typed values to avoid
injection risks:

```typescript
// Safe: numeric values carry no injection risk
const minAge = 25
const result = db.execute(`MATCH (n:Person) WHERE n.age > ${Number(minAge)} RETURN n.name`)
```

---

## Internal Crate APIs

These are for contributors building on the storage layer.

### `sparrowdb_catalog::Catalog`

```rust
let mut catalog = Catalog::open(db_path)?;

// Labels
let label_id = catalog.create_label("Person")?;
let label = catalog.get_label("Person")?; // Option<LabelEntry>

// Relationship tables
let rel_id = catalog.create_rel_table(src_label_id, dst_label_id, "KNOWS")?;
```

### `sparrowdb_storage::node_store::NodeStore`

```rust
let mut store = NodeStore::open(db_path)?;

// Write a node (columns as (col_index, Value) pairs)
let node_id = store.create_node(label_id, &[
    (0, Value::Int64(42)),
    (1, Value::Int64(30)),
])?;

// Read a property
let value = store.read_col_slot(label_id, col_index, slot)?;
```

### `sparrowdb_storage::wal`

```rust
use sparrowdb_storage::wal::{WalWriter, WalReplayer};

// Write
let mut writer = WalWriter::open(wal_dir)?;
let txn_id = writer.begin_transaction()?;
writer.write_page(txn_id, page_id, &page_bytes)?;
writer.commit_transaction(txn_id)?;

// Replay after crash
WalReplayer::replay(wal_dir, last_applied_lsn, |page_id, data, lsn| {
    // apply page write
    Ok(())
})?;
```

---

## HTTP API (`sparrowdb-server`, Phase A)

The `sparrowdb-server` crate exposes a small JSON-over-HTTP surface so
language-agnostic clients (browsers, `curl`, Go, Java, etc.) can run Cypher
queries against an embedded SparrowDB instance without speaking Bolt.

The server is synchronous (`tiny_http` worker pool) and routes every request
to a cloned `GraphDb` handle, matching the SWMR engine model.  Phase A
buffers full result sets into a JSON response.  Streaming (SSE / NDJSON) is
Phase B.

### Binary

```bash
sparrowdb-server \
  --db /path/to/sparrowdb \
  --bind 127.0.0.1 \
  --port 7480 \
  --token-file /etc/sparrowdb/http.token
```

| Flag           | Default       | Notes                                                          |
|----------------|---------------|----------------------------------------------------------------|
| `--db`         | (required)    | Database directory.  Also reads `SPARROWDB_PATH`.              |
| `--bind`       | `127.0.0.1`   | IP to bind to.  Use `0.0.0.0` to expose the server externally. |
| `--port`       | `7480`        | TCP port.                                                      |
| `--token-file` | (none)        | File containing the bearer token (single line, trimmed).       |
| `--no-auth`    | `false`       | Disable auth.  **Refused** on any non-loopback bind address.   |

The bearer token may also be supplied via `SPARROWDB_HTTP_TOKEN`.

### Authentication

Authenticated routes require:

```
Authorization: Bearer <token>
```

`/health` is intentionally unauthenticated so liveness probes work without
secrets.  `--no-auth` is only allowed on loopback (`127.0.0.0/8`, `::1`) —
attempts to combine `--no-auth` with a non-loopback `--bind` are rejected
at startup.

### Routes

#### `GET /health`

No auth.  Liveness probe.

```json
{ "status": "ok", "version": "0.1.22" }
```

#### `GET /info`

Auth required.  Returns DB metadata.

```json
{
  "version": "0.1.22",
  "labels": ["Person", "Movie"],
  "relationship_types": ["ACTED_IN", "DIRECTED"],
  "counts": { "nodes": 8400, "edges": 16002 }
}
```

#### `POST /cypher`

Auth required.  Executes a single Cypher statement.

Request:
```json
{
  "query": "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
  "params": { "name": "alice" }
}
```

`params` is optional; omit or set to `null`/`{}` for no parameters.  Values
are coerced as follows:

| JSON type        | SparrowDB `Value`     |
|------------------|-----------------------|
| `null`           | `Null`                |
| `true`/`false`   | `Bool`                |
| integer number   | `Int64`               |
| fractional number| `Float64`             |
| string           | `String`              |
| array            | `List` (recursive)    |
| object           | `Map` (recursive)     |

Response (`200 OK`):

```json
{
  "columns": ["n.name"],
  "rows": [ { "n.name": "alice" } ]
}
```

`NodeRef` and `EdgeRef` values are encoded as
`{"$type":"node","id":"<u64-as-string>"}` to preserve precision across the
JSON boundary (see `sparrowdb_execution::json::value_to_json`).

#### Errors

All error responses are JSON `{ "error": "<message>" }`.

| Status | Cause                                                         |
|--------|---------------------------------------------------------------|
| `400`  | Malformed body, empty query, or non-coercible parameter type. |
| `401`  | Missing or invalid bearer token.                              |
| `404`  | Unknown route.                                                |
| `500`  | Engine error (parse, bind, execute).                          |

### CORS

Phase A serves a permissive CORS policy:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: POST, GET, OPTIONS
Access-Control-Allow-Headers: Authorization, Content-Type
```

`OPTIONS` preflight requests are answered with `204 No Content` and the
headers above.

### Library use

The crate also exposes `Server::new(addr, ServerConfig)` for embedding the
HTTP transport directly into a Rust application — useful for tests and for
applications that already own a `GraphDb`.
