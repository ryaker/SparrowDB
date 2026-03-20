# Contributing to SparrowDB

## Before You Start

- Read `specs/sparrowdb-v3-implementation-spec.md` — it's the source of truth for all byte layouts, phase gates, and acceptance checks.
- Read `DEVELOPMENT.md` for build and test setup.
- Check open issues before starting work on a new feature.

## What We Accept

In v1 scope:
- Bug fixes against the spec
- Golden fixture additions for missing durable formats
- Integration test coverage for untested acceptance checks
- Crash failpoint tests from the matrix in Section 19.4

Not in v1 scope (will be closed):
- `ORDER BY`, `LIMIT`, `OPTIONAL MATCH`, variable-length paths
- Multi-writer MVCC
- Background compaction
- WASM, Node.js, C, or Go bindings
- Vector or full-text indexes

## Pull Request Requirements

1. **CI must pass** — `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test --workspace`
2. **Golden fixtures** — any new durable format requires a golden fixture, round-trip test, and corruption test
3. **Integration test** — new behavior requires an integration test, not just unit tests
4. **No dead modules** — do not add code that is not reachable from the public API or a test

## Spec Disagreements

If you believe the spec has a bug:
1. Open an issue describing the disagreement
2. Include the spec section number, the problematic constraint, and your proposed resolution
3. Do not work around the spec in code — spec changes come first

## Commit Style

```
<type>(<scope>): <short description>

<body if needed>
```

Types: `feat`, `fix`, `test`, `docs`, `chore`, `refactor`
Scopes: `storage`, `catalog`, `cypher`, `execution`, `python`, `ci`, `fixtures`

Examples:
```
feat(storage): implement superblock encode/decode with CRC32c
test(storage): add golden fixture for metapage v1
fix(catalog): correct TLV length field alignment for column definitions
```
