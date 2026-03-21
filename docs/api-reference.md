# SparrowDB API Reference

## Rust API

### `Engine`

The primary entry point. Opens or creates a database directory.

```rust
use sparrowdb::Engine;

// Open without encryption
let engine = Engine::open("path/to/db")?;

// Open with XChaCha20-Poly1305 encryption
let key: [u8; 32] = /* your 32-byte key */;
let engine = Engine::open_encrypted("path/to/db", key)?;
```

#### `Engine::execute`

Execute a Cypher statement. Returns a `QueryResult`.

```rust
pub fn execute(&self, cypher: &str) -> Result<QueryResult>
```

**Write operations** (CREATE, DELETE, SET) commit immediately.

**Read operations** (MATCH ... RETURN) return rows in the result.

```rust
// Write
engine.execute("CREATE (n:Person {name: \"Alice\"})")?;

// Read
let result = engine.execute("MATCH (n:Person) RETURN n.name")?;
```

#### `Engine::execute_with_params`

Execute with named parameters (avoids string interpolation for user data):

```rust
pub fn execute_with_params(
    &self,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<QueryResult>
```

```rust
use std::collections::HashMap;
use sparrowdb::Value;

let mut params = HashMap::new();
params.insert("name".to_string(), Value::String("Alice".to_string()));

let result = engine.execute_with_params(
    "MATCH (n:Person {name: $name}) RETURN n.name",
    params,
)?;
```

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
let result = engine.execute("MATCH (n:Person) RETURN n.name, n.age")?;

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
    Unimplemented,            // feature not yet implemented
}
```

```rust
match engine.execute("MATCH (n:Ghost) RETURN n") {
    Ok(result) => { /* ... */ }
    Err(sparrowdb::Error::InvalidArgument(msg)) => {
        eprintln!("Schema error: {}", msg); // "unknown label: Ghost"
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

## Python API (Phase 6 — coming soon)

```python
import sparrowdb

# Open database
with sparrowdb.open("my.db") as db:
    db.query('CREATE (:Person {name: "Alice"})')
    result = db.query('MATCH (n:Person) RETURN n.name')
    for row in result:
        print(row['n.name'])

# With encryption
with sparrowdb.open("secure.db", key=bytes(32)) as db:
    db.query('CREATE (:Knowledge {content: "secret"})')
```

Install via pip (once published):

```bash
pip install sparrowdb
# or build from source:
pip install maturin
cd crates/sparrowdb-python && maturin develop
```
