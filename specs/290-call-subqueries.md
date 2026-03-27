# Spec: General `CALL { }` Subquery Support

**Issue:** #290
**Status:** Draft
**Date:** 2026-03-27

## Problem

SparrowDB supports `CALL procedure(args) YIELD col` for built-in procedures and `EXISTS { pattern }` for existence predicates, but not general `CALL { ... }` subquery blocks that allow embedding arbitrary Cypher queries as composable building blocks.

Users cannot write:

```cypher
-- Uncorrelated subquery
MATCH (p:Person)
CALL { MATCH (c:City) RETURN count(c) AS cityCount }
RETURN p.name, cityCount

-- Correlated subquery
MATCH (p:Person)
CALL { WITH p MATCH (p)-[:LIVES_IN]->(c:City) RETURN c.name AS city }
RETURN p.name, city
```

## Current State

| Feature | Status |
|---------|--------|
| `CALL db.schema()` | Works — `parse_call()` parses dotted procedure name + `(args)` + `YIELD` |
| `EXISTS { pattern }` | Works — `parse_atom()` consumes `EXISTS { path_pattern }` |
| `CALL { MATCH ... RETURN ... }` | Broken — parser expects `CALL` followed by dotted name, not `{` |
| `CALL { WITH x ... }` | Broken — no correlated-subquery execution path |
| `CALL { }` as pipeline stage | Missing — `PipelineStage` has no `CallSubquery` variant |

**Root cause:** In `parser.rs:2257`, `parse_call()` unconditionally expects `CALL <dotted_ident>( args )`. When it encounters `CALL {`, the `{` fails `advance_as_prop_name()`.

## Design

### AST Changes

```rust
/// A general CALL { ... } subquery block.
pub struct CallSubquery {
    /// The inner query body (MATCH...RETURN, UNION, etc.)
    pub body: Box<Statement>,
    /// Variables imported from outer scope (empty = uncorrelated)
    pub imports: Vec<String>,
}
```

Extend `Statement` and `PipelineStage`:

```rust
pub enum Statement {
    // ... existing ...
    CallSubquery(CallSubquery),
}

pub enum PipelineStage {
    // ... existing ...
    CallSubquery(CallSubquery),
}
```

### Parser Changes

Modify `parse_call()` to branch on `{` vs dotted-ident:

```
parse_call():
    consume CALL
    if peek == '{':
        return parse_call_subquery()
    else:
        ... existing procedure-call logic ...

parse_call_subquery():
    consume '{'
    if peek == WITH:
        parse imports (WITH x, y, z — bare variable names only)
    parse inner statement (reuse parse_statement())
    consume '}'
    return Statement::CallSubquery(...)
```

**Key constraint:** The `WITH` at the top of a correlated subquery is NOT a regular WITH clause (no expressions, no WHERE, no ORDER BY). It is a variable import list. The parser must distinguish this from a regular WITH by checking for bare identifiers followed by a statement keyword.

### Scoping Rules (per openCypher spec)

**Uncorrelated** (`CALL { MATCH ... RETURN ... }`):
- Inner query has NO access to outer variables.
- Inner RETURN columns cross-producted with outer rows.
- If inner returns 0 rows, outer row is eliminated.

**Correlated** (`CALL { WITH x MATCH (x)-[...]->(y) RETURN y }`):
- Only variables in the leading `WITH` are visible inside.
- Inner query executes once per outer row with imported variables bound.
- Inner RETURN columns appended to outer row.
- 0 inner rows -> outer row eliminated. N inner rows -> outer row duplicated N times.

**Column naming:**
- Inner RETURN columns must not collide with outer columns (error at bind time).
- Aliases in inner RETURN become the added column names.

### Execution Model

**Uncorrelated:**
1. Execute inner query independently -> `Vec<HashMap<String, Value>>`
2. Cross-product: for each outer row x each inner row, merge column maps.
3. Inner returns 0 rows -> entire cross-product empty.

**Correlated:**
```
for each outer_row in outer_rows:
    bind imported variables from outer_row
    execute inner query with those bindings -> inner_rows
    for each inner_row in inner_rows:
        emit merge(outer_row, inner_row)
```

Nested-loop join. For N outer rows, inner query executes N times.

### Pipeline Integration

In `execute_pipeline()`, handle `PipelineStage::CallSubquery`:

```rust
PipelineStage::CallSubquery(sub) => {
    let mut next_rows = Vec::new();
    if sub.imports.is_empty() {
        // Uncorrelated: execute once, cross-product
        let inner_result = self.execute_statement(&sub.body)?;
        let inner_maps = result_to_maps(&inner_result);
        for outer in &current_rows {
            for inner in &inner_maps {
                let mut merged = outer.clone();
                merged.extend(inner.clone());
                next_rows.push(merged);
            }
        }
    } else {
        // Correlated: execute per outer row
        for outer in &current_rows {
            let bindings = extract_imports(outer, &sub.imports);
            let inner_result = self.execute_with_bindings(&sub.body, &bindings)?;
            let inner_maps = result_to_maps(&inner_result);
            for inner in inner_maps {
                let mut merged = outer.clone();
                merged.extend(inner);
                next_rows.push(merged);
            }
        }
    }
    current_rows = next_rows;
}
```

Main new infrastructure: `execute_with_bindings()` — run an inner statement with pre-bound variables by injecting bindings as a synthetic leading row set.

### UNION Inside CALL

```cypher
CALL {
    MATCH (p:Person) RETURN p.name AS name
    UNION
    MATCH (c:Company) RETURN c.name AS name
}
RETURN name
```

Already works at the AST level — `Statement::Union` can be the body of `CallSubquery`. No special handling needed.

### Binder Changes

`binder.rs` currently skips `Statement::Call(_)`. New validation needed:
- Column-collision check between inner RETURN and outer scope.
- For correlated subqueries: validate imported variable names exist in outer scope.

## Phasing

| Phase | Scope | Size |
|---|---|---|
| **1** | Uncorrelated subqueries: AST, parser branch, cross-product execution, binder collision check | M |
| **2** | Correlated subqueries: import-WITH parsing, per-row execution, `execute_with_bindings()` | L |
| **3** | Pipeline integration: `PipelineStage::CallSubquery`, mid-pipeline CALL { } | M |
| **4** | UNION inside CALL: parser + execution validation | S |

## Risks & Open Questions

| # | Risk | Mitigation |
|---|---|---|
| 1 | Variable injection depth — scan functions don't accept pre-bound vars | Modify `scan_label_nodes` and `execute_pipeline_match_stage` to accept bindings |
| 2 | Nested subqueries `CALL { CALL { ... } }` — stack overflow risk | Add max-depth limit (e.g., 8) |
| 3 | Write clauses inside CALL — no nested transaction support | Reject with error in Phase 1-4; add as future enhancement |
| 4 | Parser ambiguity: `CALL { WITH x ...` (import) vs `CALL { WITH x AS y ...` (projection) | Import-WITH has no expressions or aliases — just bare names followed by MATCH/RETURN/UNWIND |
| 5 | 0-row uncorrelated: eliminate outer row or produce NULLs? | Eliminate (matches Neo4j behavior) |
| 6 | `CALL { RETURN 42 AS x }` with no MATCH | Should work for free via `parse_standalone_return()` |
| 7 | Correlated performance on large outer sets | Log warning if outer row count exceeds configurable threshold |

## Files to Modify

| File | Change |
|---|---|
| `crates/sparrowdb-cypher/src/ast.rs` | `CallSubquery` struct, `Statement::CallSubquery`, `PipelineStage::CallSubquery` |
| `crates/sparrowdb-cypher/src/parser.rs` | Branch in `parse_call()` on `{` vs dotted-ident; `parse_call_subquery()` |
| `crates/sparrowdb-cypher/src/binder.rs` | Column-collision check, import variable validation |
| `crates/sparrowdb-execution/src/engine/mod.rs` | `execute()` dispatch for `Statement::CallSubquery` |
| `crates/sparrowdb-execution/src/engine/scan.rs` | `execute_pipeline()` stage for `CallSubquery`; `execute_with_bindings()` |
| `crates/sparrowdb-execution/src/engine/expr.rs` | May need binding-aware expression evaluation |
