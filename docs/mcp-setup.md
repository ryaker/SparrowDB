# SparrowDB MCP Setup Guide

`sparrowdb-mcp` is a JSON-RPC 2.0 MCP server that exposes SparrowDB as a tool
set for Claude Desktop (and any other MCP-compatible client).  Communication
happens over stdio — one JSON object per line.

---

## Building the Binary

From the workspace root:

```bash
# Debug build (faster, recommended for development)
cargo build -p sparrowdb-mcp

# Release build (recommended for production / Claude Desktop use)
cargo build -p sparrowdb-mcp --release
```

The binary is emitted to:

| Build profile | Path |
|---------------|------|
| debug | `target/debug/sparrowdb-mcp` |
| release | `target/release/sparrowdb-mcp` |

---

## Claude Desktop Configuration

Add the following snippet to your Claude Desktop config file.

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "sparrowdb": {
      "command": "/absolute/path/to/sparrowdb-mcp",
      "args": [],
      "env": {}
    }
  }
}
```

Replace `/absolute/path/to/sparrowdb-mcp` with the actual path returned by:

```bash
# macOS / Linux
realpath target/release/sparrowdb-mcp

# or hardcode, e.g.:
# /Users/you/Dev/SparrowDB/target/release/sparrowdb-mcp
```

After editing the config, restart Claude Desktop.  You should see
**SparrowDB** appear in the integrations panel.

---

## Available Tools

All tools accept a `db_path` parameter — the path to the directory that
contains (or will contain) a SparrowDB database.  The directory is created
automatically on first open.

### `execute_cypher`

Execute any Cypher statement directly against the database.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `db_path` | string | yes | Path to the SparrowDB database directory |
| `query`   | string | yes | Cypher statement to execute |

Returns the result rows on success.  Returns a JSON-RPC `error` on failure,
with a real diagnostic message (not a generic placeholder).

---

### `create_entity`

Create a node (entity) with a given label and structured properties.
Equivalent to `CREATE (n:ClassName {props})` but with a validated,
Claude-friendly interface.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `db_path`    | string | yes | Path to the SparrowDB database directory |
| `class_name` | string | yes | Node label (e.g. `"Person"`) |
| `properties` | object | no  | Key-value pairs; values must be strings, numbers, or booleans |

Returns a success message on creation.  Returns a descriptive `-32602` error
when `class_name` is empty or a property value is `null`, an array, or a
nested object.

---

### `add_property`

Add or update a property on existing nodes matched by label and a filter
property.  Equivalent to:

```cypher
MATCH (n:Label {match_prop: 'match_val'})
SET n.set_prop = set_val
```

| Parameter   | Type    | Required | Description |
|-------------|---------|----------|-------------|
| `db_path`   | string  | yes | Path to the SparrowDB database directory |
| `label`     | string  | yes | Node label to match (e.g. `"Person"`) |
| `match_prop`| string  | yes | Property name to filter on (e.g. `"id"`) |
| `match_val` | string  | yes | String value to filter on (e.g. `"alice"`) |
| `set_prop`  | string  | yes | Property name to set (e.g. `"email"`) |
| `set_val`   | any     | yes | New scalar value — string, number, or boolean |

Returns `{"updated": N}` where N is the number of nodes updated.  Returns 0
(not an error) when no nodes matched the filter.

---

### `checkpoint`

Flush the write-ahead log and compact the database.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `db_path` | string | yes | Path to the SparrowDB database directory |

---

### `info`

Return metadata about an open database (transaction ID, path).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `db_path` | string | yes | Path to the SparrowDB database directory |

---

## Example Session Transcript

Below is a minimal JSON-RPC session illustrating the protocol.
Each message is one line on stdin; each response is one line on stdout.

```
→ stdin (MCP client)
← stdout (sparrowdb-mcp)
```

### 1. Initialize

```jsonc
// → initialize handshake
{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"my-client","version":"1"}}}

// ← server acknowledges capabilities
{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"sparrowdb-mcp","version":"0.1.0"}}}
```

### 2. List Tools

```jsonc
// →
{"jsonrpc":"2.0","id":1,"method":"tools/list","params":null}

// ←
{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"execute_cypher",...},{"name":"create_entity",...},{"name":"add_property",...},{"name":"checkpoint",...},{"name":"info",...}]}}
```

### 3. Create a Node via `create_entity`

```jsonc
// →
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_entity","arguments":{"db_path":"/tmp/mydb","class_name":"Person","properties":{"name":"Alice","age":30}}}}

// ←
{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"Entity created successfully (class: 'Person')"}]}}
```

### 4. Query with `execute_cypher`

```jsonc
// →
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"execute_cypher","arguments":{"db_path":"/tmp/mydb","query":"MATCH (n:Person) RETURN n.name, n.age"}}}

// ←
{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"QueryResult { columns: [\"n.name\", \"n.age\"], rows: [[String(\"Alice\"), Int64(30)]] }"}]}}
```

### 5. Add a Property via `add_property`

```jsonc
// →
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"add_property","arguments":{"db_path":"/tmp/mydb","label":"Person","match_prop":"name","match_val":"Alice","set_prop":"email","set_val":"alice@example.com"}}}

// ←
{"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"add_property: set 'Person.email' on 1 node(s) matching name='Alice'."}],"updated":1}}
```

### 6. Error Response (invalid Cypher)

```jsonc
// →
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"execute_cypher","arguments":{"db_path":"/tmp/mydb","query":"NOT VALID @@@@"}}}

// ←
{"jsonrpc":"2.0","id":5,"error":{"code":-32000,"message":"parse error at position 0: unexpected token 'NOT'"}}
```

---

## Running the Integration Tests

```bash
# Ensure the binary is built first
cargo build -p sparrowdb-mcp

# Run only the SPA-228 integration tests
cargo test -p sparrowdb spa_228

# Run the full test suite
cargo test
```

Tests will skip gracefully (not fail) when the binary has not been built yet,
so they are safe to include in CI pipelines before the first artifact build.
