# Spec: Multi-Label Nodes `(n:A:B)` — SPA-200

**Issue:** #289
**Status:** Draft
**Date:** 2026-03-27

## Problem

SparrowDB enforces a single label per node. Standard openCypher supports multi-label nodes: `CREATE (n:Person:Employee {name: "Alice"})` assigns both `:Person` and `:Employee` to the same node, and `MATCH (n:Person)` must return that node even though it also carries `:Employee`.

The parser already accepts `(n:A:B)` and populates `NodePattern.labels: Vec<String>`, but the engine silently drops all labels after the first.

Current single-label constraints:

| Layer | Constraint |
|---|---|
| **NodeId encoding** | `(label_id: u32) << 32 \| slot: u32` — label embedded in ID |
| **Storage** | Column files under `nodes/{label_id}/` — one dir per label |
| **Catalog** | `LabelId = u16`, simple name-to-id map |
| **Execution** | `node.labels.first()` everywhere |
| **Property index** | Keyed by `(label_id, col_id)` — assumes one label |

## Design: Primary Label + Secondary Label Set

Every node retains exactly **one primary label** (determines NodeId encoding and `nodes/{label_id}/` storage directory). Additional labels are tracked in a **label-set side table** that maps `NodeId -> Set<LabelId>`.

### Storage: Fixed-Width Bitset Column (Phase 1)

New file per primary label directory:

```
nodes/{primary_label_id}/secondary_labels.bin
```

A single `u64` per slot where each bit maps to a secondary label. Supports up to 64 total labels. Simple, cache-friendly, O(1) lookup.

Overflow (variable-length list of LabelIds) can be added in Phase 2 behind a feature flag if >64 labels are needed.

### Parser Changes

**None required.** The parser already handles `(n:A:B:C)` correctly:

```rust
// parser.rs:1451-1456
let mut labels = Vec::new();
while matches!(self.peek(), Token::Colon) {
    self.advance();
    let label = self.expect_label_or_type()?;
    labels.push(label);
}
```

### Catalog Changes

1. **No change to `LabelEntry` or `LabelId`** — each label name still maps to a unique `LabelId`.

2. **New TLV entry type: `NodeLabelSet`** — persists secondary label assignments:
   ```
   Tag: 0x03 (new)
   Fields: primary_label_id: u16, slot: u32, secondary_label_ids: Vec<u16>
   ```
   Appended to `catalog.tlv` on multi-label node creation or `SET n:NewLabel`.

3. **In-memory indexes** loaded on catalog open:
   - Forward: `HashMap<NodeId, HashSet<LabelId>>` — "which labels does this node have?"
   - Reverse: `HashMap<LabelId, HashSet<NodeId>>` — "which nodes have this label?" Used by MATCH to find nodes by secondary label without full scans.

### MATCH Semantics

`MATCH (n:A)` must return all nodes whose label set contains `:A`, regardless of primary vs secondary.

**Single-label query `MATCH (n:A)`:**
1. Primary-label scan (existing path, no change).
2. Reverse index lookup `secondary_label_index[A]`.
3. Union both sets, deduplicate by NodeId.

**Multi-label query `MATCH (n:A:B)`:**
1. For each label, collect the set of NodeIds that have that label.
2. Intersect all sets.
3. Return the intersection.

**Implementation in `scan.rs`:** Every call site that currently does `let label = node.labels.first()` must be updated to resolve all labels in the pattern and use intersection semantics.

The `__labels__` metadata injected for `labels(n)` must return the full label set, not just the primary label.

### CREATE Semantics

`CREATE (n:Person:Employee {name: "Alice"})`:

1. First label (`:Person`) = primary label — determines NodeId and storage.
2. Subsequent labels (`:Employee`) = secondary labels.
3. After `create_node(label_id, props)`:
   - Write `NodeLabelSet` TLV entry to catalog.
   - Update in-memory forward and reverse indexes.
4. `labels()` returns `["Person", "Employee"]`.

**Primary label selection:** Positional (first in syntax), matching Neo4j behavior.

### MERGE Semantics

`MERGE (n:Person:Employee {name: "Alice"})`:

1. `parse_merge` must accept multiple labels (currently errors after first).
2. Match phase: look up nodes with **all** specified labels plus matching properties.
3. Create phase: if no match, create with all labels (same as CREATE above).

### Property Index Impact

Property index is keyed by `(label_id, col_id)`. A node `(:Person:Employee {name: "Alice"})` stores columns under primary `label_id` (Person).

**Phase 1 limitation:** `CREATE INDEX ON :Employee(name)` will NOT automatically index nodes whose primary label is `:Person` but have `:Employee` as secondary. Document this. Phase 2 adds cross-label index support by scanning the reverse index during index builds.

### `labels()` Function

After this change, `labels(n)` returns the full ordered set: `[primary_label, secondary_1, secondary_2, ...]`. Primary label is always first.

### WAL Integration

New WAL op code for label-set writes. The WAL codec must learn to replay `NodeLabelSet` entries.

### Migration

**Zero migration required.** The design is purely additive:
- Nodes with no secondary labels have no entry in the side table.
- Existing queries work identically.
- `catalog.tlv` is append-only; new entry types are ignored by older readers.

## Risks & Open Questions

| # | Item | Decision |
|---|---|---|
| 1 | Primary label immutability | Accept: primary label baked into NodeId, changing it breaks edge references. |
| 2 | Label count limit (64 via bitset) | Sufficient for Phase 1. Add overflow if needed. |
| 3 | Secondary-label index gap | Document limitation. Phase 2 follow-up. |
| 4 | Edge storage | NodeId encoding unchanged — no edge migration. |
| 5 | WAL replay | Needs new WAL op code. |
| 6 | `REMOVE n:PrimaryLabel` | Error in Phase 1. |
| 7 | `labels()` ordering | Insertion order (primary first). |
| 8 | `WHERE n:A` label predicates | Out of scope — separate ticket. |

## Phasing

| Phase | Work | Size |
|---|---|---|
| **1a** | Catalog side table + in-memory indexes + CREATE multi-label | M |
| **1b** | MATCH resolution (primary + secondary scan, intersection) | M |
| **1c** | `labels()` function update + `__labels__` metadata | S |
| **1d** | MERGE multi-label support | S |
| **2** | Cross-label property indexes | M |
| **3** | SET/REMOVE label mutations | L |

**Overall: L (Large)**

Recommended: Phase 1a+1b+1c+1d as a single PR (end-to-end feature), Phase 2 and 3 as follow-ups.

## Files to Modify

| File | Change |
|---|---|
| `crates/sparrowdb-catalog/src/catalog.rs` | Label-set side table, reverse index, new TLV entry |
| `crates/sparrowdb-catalog/src/tlv.rs` | New `NodeLabelSet` TLV tag (0x03) |
| `crates/sparrowdb-storage/src/wal/codec.rs` | New WAL op for label-set writes |
| `crates/sparrowdb-storage/src/wal/replay.rs` | Replay handler for label-set ops |
| `crates/sparrowdb-execution/src/engine/scan.rs` | Multi-label MATCH resolution (~15 call sites) |
| `crates/sparrowdb-execution/src/engine/hop.rs` | Multi-label hop traversal |
| `crates/sparrowdb-execution/src/engine/mutation.rs` | CREATE + MERGE multi-label support |
| `crates/sparrowdb-execution/src/engine/expr.rs` | `labels()` function update |
| `crates/sparrowdb-cypher/src/parser.rs` | MERGE multi-label parsing (minor) |
