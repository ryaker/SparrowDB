# Spec: Publish SparrowOntology to crates.io — SPA-226

**Issue:** #292
**Status:** Draft
**Date:** 2026-03-27

## Problem

SparrowOntology is referenced throughout the codebase (MCP tools like `create_entity`, `add_property`; test fixtures using `OntologyClass` labels; `__SO_` reserved prefix) but does not yet exist as a standalone crate. It needs to be extracted, packaged, and published to crates.io so downstream users can depend on it independently.

## Current State

- **No `sparrowontology` crate exists** in the workspace. The 13 current workspace members do not include it.
- The `__SO_` prefix is reserved in the codebase for ontology system objects (`__SO_Property`, `__SO_Class`).
- MCP server tools (`create_entity`, `add_property`, `merge_node_by_property`) implement ontology-like operations inline.
- The CI/CD release workflow (`.github/workflows/release.yml`) publishes npm bindings but has **no `cargo publish` step** for crates.io.
- All crates use workspace-level version inheritance (`0.1.12`).

## Scope

This is primarily a packaging and CI task, not a feature implementation. The work is:

1. **Extract** ontology logic into a new `sparrow-ontology` crate.
2. **Verify** it compiles independently.
3. **Add** `cargo publish` to the CI release workflow.
4. **Publish** initial version.

## Design

### Crate Structure

```
crates/sparrow-ontology/
  Cargo.toml
  src/
    lib.rs           — Public API: OntologyDb, init(), create_class(), etc.
    schema.rs        — OntologyClass, OntologyProperty definitions
    validation.rs    — Schema validation, constraint checking
    error.rs         — OntologyError type
```

### Cargo.toml

```toml
[package]
name = "sparrow-ontology"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true
description = "Schema and ontology management layer for SparrowDB"
categories = ["database", "data-structures"]
keywords = ["graph-database", "ontology", "schema", "sparrowdb"]
readme = "README.md"

[dependencies]
sparrowdb = { path = "../sparrowdb", version = "0.1.12" }
serde = { workspace = true }
serde_json = { workspace = true }
```

**Key constraints for crates.io:**
- `description` is required.
- `license` or `license-file` is required (MIT — inherited from workspace).
- All path dependencies must also be published, with matching version constraints.
- `sparrowdb` itself must be published first (or simultaneously) since `sparrow-ontology` depends on it.

### Public API Surface

```rust
pub struct OntologyDb {
    db: GraphDb,
}

impl OntologyDb {
    /// Wrap an existing GraphDb with ontology management.
    pub fn new(db: GraphDb) -> Self;

    /// Initialize ontology schema (creates __SO_ system nodes if missing).
    pub fn init(&self) -> Result<()>;

    /// Define a new ontology class (node label with schema).
    pub fn create_class(&self, name: &str, properties: &[PropertyDef]) -> Result<()>;

    /// Create an entity (node) validated against its class schema.
    pub fn create_entity(&self, class: &str, props: HashMap<String, Value>) -> Result<NodeId>;

    /// Add a property definition to an existing class.
    pub fn add_property(&self, class: &str, prop: PropertyDef) -> Result<()>;

    /// List all defined classes.
    pub fn list_classes(&self) -> Result<Vec<String>>;

    /// Get schema for a class.
    pub fn get_class(&self, name: &str) -> Result<ClassSchema>;

    /// Validate that all nodes of a class conform to its schema.
    pub fn validate(&self, class: &str) -> Result<ValidationReport>;
}

pub struct PropertyDef {
    pub name: String,
    pub value_type: ValueType,    // String, Int64, Float64, Bool
    pub required: bool,
    pub unique: bool,
}
```

### Independence Check

The crate must compile with only its declared dependencies. Verify:

```bash
# From crates/sparrow-ontology/
cargo check
cargo test
cargo package --allow-dirty  # Dry run — checks crates.io readiness
```

Ensure no workspace-only imports leak through (e.g., dev-dependencies on internal test utilities).

### CI/CD: Add `cargo publish` to Release Workflow

Modify `.github/workflows/release.yml` to add a crates.io publish job:

```yaml
  publish-crates:
    runs-on: ubuntu-latest
    needs: [create-release]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Publish sparrowdb-common
        run: cargo publish -p sparrowdb-common --token ${{ secrets.CRATES_IO_TOKEN }}
      - name: Publish sparrowdb-storage
        run: cargo publish -p sparrowdb-storage --token ${{ secrets.CRATES_IO_TOKEN }}
      - name: Publish sparrowdb-catalog
        run: cargo publish -p sparrowdb-catalog --token ${{ secrets.CRATES_IO_TOKEN }}
      - name: Publish sparrowdb-cypher
        run: cargo publish -p sparrowdb-cypher --token ${{ secrets.CRATES_IO_TOKEN }}
      - name: Publish sparrowdb-execution
        run: cargo publish -p sparrowdb-execution --token ${{ secrets.CRATES_IO_TOKEN }}
      - name: Publish sparrowdb
        run: cargo publish -p sparrowdb --token ${{ secrets.CRATES_IO_TOKEN }}
      - name: Publish sparrow-ontology
        run: cargo publish -p sparrow-ontology --token ${{ secrets.CRATES_IO_TOKEN }}
```

**Publish order matters** — crates must be published in dependency order. Each `cargo publish` must wait for the previous crate to be indexed (~30s).

**Prerequisite:** Add `CRATES_IO_TOKEN` secret to the GitHub repository settings.

### Workspace Changes

Add to root `Cargo.toml`:

```toml
[workspace]
members = [
    # ... existing 13 members ...
    "crates/sparrow-ontology",
]
```

## Blockers

- **#289 (Multi-label nodes)** and **#290 (CALL subqueries)** should be stable first. The ontology layer may use multi-label semantics for class hierarchies. Rushing the publish before these stabilize risks breaking API changes.
- **`sparrowdb` must be published to crates.io first** (or in the same release). Currently no `cargo publish` step exists for any crate.

## Phasing

| Phase | Scope | Size |
|---|---|---|
| **1** | Create crate skeleton, extract `__SO_` logic, basic `create_class`/`create_entity` API | M |
| **2** | Schema validation, `PropertyDef` with type checking, `validate()` | S |
| **3** | CI: add `cargo publish` for all workspace crates in dependency order | S |
| **4** | Publish initial version (0.1.12 matching workspace) | S |

**Overall: M (Medium)**

## Risks & Open Questions

| # | Item | Decision |
|---|---|---|
| 1 | Versioning: same as workspace or independent? | Same version (workspace inheritance) for v1. Independent versioning once API stabilizes. |
| 2 | `sparrowdb` not yet on crates.io | Must publish `sparrowdb` (and its deps) first. This is the real blocker. |
| 3 | API stability | Pre-1.0, so breaking changes are acceptable. Document with `#[doc(hidden)]` for unstable APIs. |
| 4 | Crate name: `sparrow-ontology` vs `sparrowontology` | `sparrow-ontology` (hyphenated) is idiomatic Rust. |
| 5 | Feature flags | Consider `default-features = false` for minimal builds; `full` feature for validation + schema introspection. |
| 6 | `CRATES_IO_TOKEN` secret | Must be created by repo owner. |

## Files to Create/Modify

| File | Change |
|---|---|
| `crates/sparrow-ontology/Cargo.toml` | New crate |
| `crates/sparrow-ontology/src/lib.rs` | Public API |
| `crates/sparrow-ontology/src/schema.rs` | Class/property definitions |
| `crates/sparrow-ontology/src/validation.rs` | Schema validation |
| `crates/sparrow-ontology/src/error.rs` | Error types |
| `crates/sparrow-ontology/README.md` | Crate documentation |
| `Cargo.toml` | Add workspace member |
| `.github/workflows/release.yml` | Add `cargo publish` jobs |
