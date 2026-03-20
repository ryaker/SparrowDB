# Development Guide

## Prerequisites

- Rust stable (see `rust-toolchain.toml` once added in Phase 0)
- Python 3.9+ (for `sparrowdb-python` bindings)
- `maturin` for Python wheel builds

```bash
# Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Python tooling
pip install maturin pytest
```

## Building

```bash
# All crates
cargo build --workspace

# Release build
cargo build --workspace --release

# Python bindings (dev mode)
cd crates/sparrowdb-python
maturin develop
```

## Testing

```bash
# All tests
cargo test --workspace

# Single crate
cargo test -p sparrowdb-storage

# Specific test
cargo test -p sparrowdb -- acceptance::create_db_directory

# With output
cargo test --workspace -- --nocapture
```

### Test Layers

| Layer | What it tests |
|-------|---------------|
| Unit tests | Codec round-trips, individual operators |
| Integration tests | Full write → read → restart cycles |
| Golden fixture tests | Byte-exact binary format verification |
| Crash failpoint tests | Recovery after injected fsync failures |
| Compatibility tests | Opening the frozen `fixtures/compatibility/v1.0/` fixture |

**Integration tests are first-class.** Unit tests alone are not sufficient to prove a phase gate.

### Golden Fixtures

Golden fixtures are binary reference files for every durable format. They live in `fixtures/golden/`.

Each fixture must have:
1. A round-trip test (encode → decode → encode → compare)
2. A corruption test (flip one byte → verify detection)

Never delete or modify an existing golden fixture without a spec version bump.

## Lint

```bash
cargo fmt --check
cargo clippy -- -D warnings
```

## Phase Gates

Each phase has an explicit integration gate. **Do not move to the next phase until the gate for the current phase passes.**

| Phase | Gate |
|-------|------|
| 0 | `cargo build --workspace` clean, CI green |
| 1 | `create_db_directory` + golden fixtures for superblock/page-header/metapage |
| 2 | Create + reopen database with labels and node columns; catalog golden fixture |
| 3 | WAL replay passes; all crash failpoints pass; delta golden fixtures |
| 4a | Every supported statement parses and binds against test catalog |
| 4b | Simple `MATCH ... RETURN ...` works end-to-end |
| 4c | 2-hop query and ASP-Join multiplicity examples pass |
| 5 | Snapshot reader survives writer commit; CHECKPOINT/OPTIMIZE produce clean base |
| 6 | All 14 acceptance checks pass |

## Crate Dependency Order

```
sparrowdb-common
  └── sparrowdb-storage
        └── sparrowdb-catalog
              ├── sparrowdb-cypher
              └── sparrowdb-execution
                    └── sparrowdb (public API)
                          └── sparrowdb-python (PyO3 bindings)
```

## File Format Conventions

- All byte layouts are defined in `specs/sparrowdb-v3-implementation-spec.md` — that document is the source of truth.
- If you think the spec and code disagree, the spec wins. File an issue.
- Every durable page begins with a 16-byte page header (magic, CRC32c, version, flags).
- Every mutable page has an 8-byte `last_applied_lsn` prefix immediately after the page header.

## Encryption

At-rest encryption uses XChaCha20-Poly1305. Encrypted files have a physical page stride of `page_size + 40` (header + 24-byte nonce + ciphertext + 16-byte tag). The superblock and metapages are never encrypted.

## WAL Notes

WAL lives in `wal/seg_{n}.wal`. Records must not straddle segments. WAL framing is always plaintext; when encryption is enabled, only the record payload bytes are encrypted. WAL-backed commit is mandatory from Phase 3 onward.
