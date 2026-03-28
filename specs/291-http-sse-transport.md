# Spec: HTTP/SSE Transport Layer — SPA-231

**Issue:** #291
**Status:** Draft
**Date:** 2026-03-27

## Problem

SparrowDB is embedded-only. All access requires in-process embedding (Rust, Python, Node.js, Ruby) or stdio piping (CLI serve mode, MCP server). There is no network protocol for remote access.

This blocks:
- Multi-client access to a shared database
- Web application backends querying SparrowDB
- Language-agnostic clients (Go, Java, etc.) without native bindings
- Cloud/remote deployment scenarios

## Current Transport State

| Transport | Protocol | Use Case |
|-----------|----------|----------|
| In-process | Direct Rust API (`GraphDb::execute()`) | Embedding in Rust apps |
| CLI serve | NDJSON over stdio | Inter-process communication |
| MCP server | JSON-RPC 2.0 over stdio | AI assistant integration |
| Python/Node/Ruby | FFI over in-process | Language bindings |

**No TCP/HTTP endpoint exists.**

## Design Decision: HTTP + SSE

After evaluating three options, HTTP + SSE is recommended for Phase 1:

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| **HTTP + SSE** | Simple, standard, works with any HTTP client, streaming for large results | No bidirectional streaming | **Recommended** |
| Bolt protocol (Neo4j compat) | Driver reuse | Complex binary protocol, licensing concerns, overkill for v1 | Deferred |
| WebSocket | Bidirectional, low overhead | More complex client code, connection management | Phase 2 option |

### Why HTTP + SSE

1. **Zero client-side dependencies** — `curl` can query the database.
2. **SSE for streaming** — large result sets stream row-by-row without buffering entire response.
3. **Stateless** — each request opens a fresh read/write transaction. No session management.
4. **Compatible with MCP HTTP transport** — the MCP spec defines an HTTP+SSE transport; this aligns.

## API Design

### Endpoints

```
POST /cypher                     Execute a Cypher query (JSON request/response)
POST /cypher/stream              Execute a Cypher query (SSE streaming response)
POST /batch                      Execute multiple queries in a single transaction
GET  /health                     Health check
GET  /info                       Database metadata (counts, labels, rel types)
POST /checkpoint                 Trigger WAL checkpoint
POST /optimize                   Trigger CSR optimization
```

### Authentication

Phase 1: **Bearer token** (static shared secret).

```
Authorization: Bearer <token>
```

- Token set via `--auth-token <token>` CLI flag or `SPARROWDB_AUTH_TOKEN` env var.
- If no token configured, server is **localhost-only** (binds `127.0.0.1`).
- If token configured, server may bind `0.0.0.0` (configurable via `--bind`).
- mTLS and OIDC deferred to Phase 2.

### Query Endpoint: `POST /cypher`

**Request:**
```json
{
  "query": "MATCH (n:Person) RETURN n.name, n.age",
  "params": {"name": "Alice"},
  "timeout_ms": 5000
}
```

- `query` (required): Cypher string.
- `params` (optional): Named parameters for parameterized queries (maps to `execute_with_params`).
- `timeout_ms` (optional): Query timeout in milliseconds (default: 30000).

**Response (success):**
```json
{
  "columns": ["n.name", "n.age"],
  "rows": [
    {"n.name": "Alice", "n.age": 30},
    {"n.name": "Bob", "n.age": 25}
  ],
  "stats": {
    "rows_returned": 2,
    "elapsed_ms": 12
  }
}
```

**Response (error):**
```json
{
  "error": {
    "code": "SYNTAX_ERROR",
    "message": "Unexpected token at line 1, column 7"
  }
}
```

**Error codes:**
- `SYNTAX_ERROR` — Cypher parse failure
- `EXECUTION_ERROR` — Runtime error (missing label, constraint violation, etc.)
- `TIMEOUT` — Query exceeded `timeout_ms`
- `WRITE_CONFLICT` — Another writer is active (SWMR)
- `AUTH_FAILED` — Invalid or missing token

**HTTP status mapping:**
- 200: Success
- 400: Syntax error or invalid request
- 401: Auth failed
- 408: Timeout
- 409: Write conflict
- 500: Internal error

### Streaming Endpoint: `POST /cypher/stream`

Same request format as `/cypher`. Response is `text/event-stream` (SSE):

```
Content-Type: text/event-stream

event: columns
data: {"columns": ["n.name", "n.age"]}

event: row
data: {"n.name": "Alice", "n.age": 30}

event: row
data: {"n.name": "Bob", "n.age": 25}

event: complete
data: {"rows_returned": 2, "elapsed_ms": 12}

```

**Error during streaming:**
```
event: error
data: {"code": "EXECUTION_ERROR", "message": "..."}

```

SSE allows clients to process rows as they arrive, reducing memory pressure for large result sets. The server iterates `QueryResult.rows` and sends each as an SSE event.

### Batch Endpoint: `POST /batch`

**Request:**
```json
{
  "queries": [
    "CREATE (a:Person {name: 'Alice'})",
    "CREATE (b:Person {name: 'Bob'})",
    "CREATE (a)-[:KNOWS]->(b)"
  ],
  "timeout_ms": 10000
}
```

**Response:**
```json
{
  "results": [
    {"columns": [], "rows": [], "stats": {"rows_returned": 0}},
    {"columns": [], "rows": [], "stats": {"rows_returned": 0}},
    {"columns": [], "rows": [], "stats": {"rows_returned": 0}}
  ],
  "stats": {
    "elapsed_ms": 45
  }
}
```

Maps to `GraphDb::execute_batch()`. All queries execute in a single transaction — if any fails, the entire batch is rolled back.

### Health Endpoint: `GET /health`

```json
{"status": "ok", "version": "0.1.12"}
```

### Info Endpoint: `GET /info`

```json
{
  "node_count": 1500,
  "edge_count": 4200,
  "labels": ["Person", "City", "Company"],
  "relationship_types": ["KNOWS", "LIVES_IN", "WORKS_AT"]
}
```

Maps to `GraphDb::db_counts()`, `GraphDb::labels()`, `GraphDb::relationship_types()`.

## Architecture

### New Crate: `sparrowdb-server`

```
crates/sparrowdb-server/
  Cargo.toml
  src/
    main.rs          — CLI entry point, arg parsing
    server.rs        — HTTP server loop
    handlers.rs      — Route handlers
    auth.rs          — Token validation middleware
    sse.rs           — SSE response formatting
```

**Dependencies:**
- `tiny_http` (already in workspace) — synchronous HTTP server
- `sparrowdb` — database engine
- `serde_json` — JSON serialization
- `sparrowdb-execution` — `QueryResult`, `value_to_json`, `query_result_to_json`

### Threading Model

`tiny_http` spawns a thread pool for handling requests. `GraphDb` is `Send + Sync` (Arc-wrapped), safe to share across handler threads.

```
main thread:
    GraphDb::open(path)
    tiny_http::Server::http(bind_addr)
    loop:
        request = server.recv()
        thread_pool.execute(|| handle_request(db.clone(), request))

handle_request:
    match route:
        POST /cypher        -> handle_cypher(db, request)
        POST /cypher/stream -> handle_cypher_stream(db, request)
        POST /batch         -> handle_batch(db, request)
        GET  /health        -> handle_health(request)
        GET  /info          -> handle_info(db, request)
        POST /checkpoint    -> handle_checkpoint(db, request)
        _                   -> 404
```

### Concurrency Semantics

SparrowDB uses SWMR (Single-Writer Multiple-Reader):
- Multiple simultaneous read queries: OK (each gets snapshot isolation).
- Only one write transaction at a time: second writer gets `WRITE_CONFLICT` (409).
- The HTTP layer does NOT serialize writes — it forwards the SWMR error to the client.

### CLI Interface

```bash
# Start server
sparrowdb serve --http --db /path/to/db --bind 127.0.0.1:7480

# With auth
sparrowdb serve --http --db /path/to/db --bind 0.0.0.0:7480 --auth-token mytoken

# Default port: 7480 (S=7, P=8, A=0 — "SPA" for SparrowDB API)
```

### Value Serialization

Reuse the existing `query_result_to_json()` and `value_to_json()` from `sparrowdb-execution/src/json.rs`:
- `Int64` -> JSON number (if within +-2^53) or string
- `NodeRef`/`EdgeRef` -> `{"$type": "node"|"edge", "id": "u64_string"}`
- `List` -> JSON array
- `Map` -> JSON object

## Phasing

| Phase | Scope | Size |
|---|---|---|
| **1** | `sparrowdb-server` crate, `/cypher` + `/health` + `/info` endpoints, bearer token auth, `tiny_http` | M |
| **2** | SSE streaming (`/cypher/stream`), `/batch` endpoint, `/checkpoint` + `/optimize` | M |
| **3** | WebSocket transport (bidirectional), connection keep-alive | L |
| **4** | mTLS, OIDC/JWT auth, rate limiting | M |

## Risks & Open Questions

| # | Item | Decision |
|---|---|---|
| 1 | Sync vs async HTTP | Phase 1 uses `tiny_http` (sync). If throughput demands it, migrate to `axum`/`tokio` in Phase 3. |
| 2 | Large result sets without streaming | `/cypher` buffers entire result. Clients concerned about memory should use `/cypher/stream`. |
| 3 | Transaction sessions | Phase 1 is stateless (one tx per request). Multi-statement sessions (BEGIN/COMMIT over HTTP) deferred. |
| 4 | CORS | Phase 1 adds permissive CORS headers for browser clients. |
| 5 | Request body size limit | Default 10 MB. Configurable via `--max-request-size`. |
| 6 | Default port | 7480. Configurable via `--bind`. |
| 7 | HTTPS/TLS termination | Phase 1: rely on reverse proxy (nginx, caddy). Phase 4: native TLS option. |
| 8 | Query cancellation | SSE connection close triggers query timeout. Phase 1 relies on `execute_with_timeout`. |
| 9 | Bolt protocol compatibility | Deferred. Significant complexity for limited v1 benefit. |

## Files to Create/Modify

| File | Change |
|---|---|
| `crates/sparrowdb-server/Cargo.toml` | New crate with `tiny_http`, `sparrowdb`, `serde_json` deps |
| `crates/sparrowdb-server/src/main.rs` | CLI arg parsing, server startup |
| `crates/sparrowdb-server/src/server.rs` | HTTP server loop, routing |
| `crates/sparrowdb-server/src/handlers.rs` | Request handlers for each endpoint |
| `crates/sparrowdb-server/src/auth.rs` | Bearer token validation |
| `crates/sparrowdb-server/src/sse.rs` | SSE response formatting |
| `Cargo.toml` | Add `sparrowdb-server` to workspace members |
