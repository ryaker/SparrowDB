# Bolt Protocol Server (`sparrowdb-bolt`)

SparrowDB ships a standalone Bolt v4.x server that lets any Neo4j-compatible
client — graph visualization tools, drivers, dashboards — connect to a
SparrowDB database over TCP.

## Quick Start

```bash
# Build
cargo build --release -p sparrowdb-bolt

# Run against an existing database
sparrowdb-bolt --db-path /path/to/my.db --port 7687
```

The server listens on `127.0.0.1:7687` by default.

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--db-path <PATH>` | *(required)* | Path to the SparrowDB database directory |
| `--port <PORT>` | `7687` | TCP port to listen on |
| `--host <HOST>` | `127.0.0.1` | Bind address |

## Connecting with gdotv

[gdotv](https://gdotv.app) is a graph visualization client for Neo4j-compatible
databases.

1. Start the bolt server: `sparrowdb-bolt --db-path my.db --port 7687`
2. Open gdotv → **New Connection**
3. Set **Protocol** to `bolt://`, **Host** to `localhost`, **Port** to `7687`
4. Leave credentials blank (authentication is not enforced in this release)
5. Click **Connect**

Once connected, run queries in the query panel. For example:

```cypher
MATCH (n) RETURN n LIMIT 50
```

```cypher
MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 100
```

```cypher
CALL db.labels()
```

### Path variable syntax

Path variable assignment is parsed and accepted:

```cypher
MATCH p = (a)-[r*1..3]->(b) RETURN a, b
```

The path variable `p` itself is currently ignored in the result — nodes and
relationships along the path are returned via their own bound variables.

## Connecting with the Neo4j Browser

The Neo4j Browser also works against `sparrowdb-bolt`:

```
bolt://localhost:7687
```

Leave username/password blank.

## Supported Bolt Messages

| Message | Status |
|---------|--------|
| `HELLO` | ✅ Accepted (any credentials) |
| `LOGON` | ✅ Accepted (Bolt 5.1+) |
| `RUN` | ✅ Executes Cypher |
| `PULL` | ✅ Streams result rows |
| `DISCARD` | ✅ Discards pending results |
| `BEGIN` | ✅ No-op (auto-commit only) |
| `COMMIT` | ✅ No-op |
| `ROLLBACK` | ✅ No-op |
| `RESET` | ✅ Returns to Ready state |
| `GOODBYE` | ✅ Closes connection |

### Procedure stubs

The following `CALL` procedures return stub data so clients that probe
database metadata on connect do not error:

- `db.labels()` — returns actual label names from the database
- `db.relationshipTypes()` — returns actual relationship type names
- `db.propertyKeys()` — returns `[]`
- `dbms.components()` — returns version info
- `dbms.procedures()` / `dbms.functions()` — returns `[]`

## Compatibility Notes

- The server identifies itself as `Neo4j/5.20.0` in the HELLO handshake.
  This is required for compatibility with clients that check the vendor string.
  SparrowDB is not affiliated with or endorsed by Neo4j.
- Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) are accepted but are no-ops;
  SparrowDB uses auto-commit semantics in this release.
- Parameterized queries (`{$param}`) are not yet supported over Bolt; include
  values inline in the query string.
- Authentication is not enforced. Bind to `127.0.0.1` (the default) and do not
  expose port 7687 to untrusted networks.

## Security

- **Default bind address is `127.0.0.1`** — the server is localhost-only unless
  you explicitly pass `--host 0.0.0.0`.
- There is no authentication in this release. Do not expose the port remotely.
- Query text is logged at `DEBUG` level only; sensitive literals are not emitted
  at `INFO`.
