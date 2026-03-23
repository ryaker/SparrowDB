# SPA-106: Acceptance Check Status

All 14 acceptance checks defined in `crates/sparrowdb/tests/acceptance.rs` pass
on this branch.  Run the suite with:

```
cargo test --workspace --test acceptance -- --nocapture
```

## Check Matrix

| # | Name | Description | Status |
|---|------|-------------|--------|
| 1 | `check_1_one_hop_scan` | `(a:Person)-[:KNOWS]->(b:Person)` 1-hop MATCH | PASS |
| 2 | `check_2_two_hop_asp_join` | 2-hop friend-of-friend via ASP-Join | PASS |
| 3 | `check_3_wal_crash_recovery` | WriteTx dropped without commit; uncommitted data absent after reopen; committed data survives | PASS |
| 4 | `check_4_checkpoint_optimize_no_error` | CHECKPOINT and OPTIMIZE succeed; data readable afterward | PASS |
| 5 | `check_5_snapshot_isolation` | Old ReadTx sees pre-write snapshot; new ReadTx sees post-write value | PASS |
| 6 | `check_6_encryption_auth_wrong_key` | Wrong key returns `EncryptionAuthFailed`; correct key round-trips cleanly | PASS |
| 7 | `check_7_order_by_large_result` | ORDER BY ASC on 50-node result; output is in ascending order | PASS |
| 8 | `check_8_python_binding_skip_if_not_configured` | Python binding (skipped — requires maturin + Python) | SKIP |
| 9 | `check_9_nodejs_binding_skip_if_not_configured` | Node.js binding (skipped — requires napi-rs build + Node.js) | SKIP |
| 10 | `check_10_mutation_round_trip` | MERGE idempotent; MATCH…SET updates property; CREATE edge queryable | PASS |
| 11 | `check_11_mvcc_write_write_conflict` | Second `begin_write` returns `WriterBusy`; lock released on drop | PASS |
| 12 | `check_12_unwind_list_expansion` | `UNWIND [10,20,30]`; string list; empty list | PASS |
| 13 | `check_13_variable_length_paths` | `[:LINK*1]` and `[:LINK*1..2]` return correct hop counts | PASS |
| 14 | `check_14_fulltext_search` | `CALL db.index.fulltext.queryNodes(...)` returns matching node | PASS |

Checks 8 and 9 are intentional skips (external runtime required).
The CI `acceptance` job counts them as passing — `test result: ok. 14 passed`.

## Regressions Fixed in This Branch

Two pre-existing regressions were found and fixed during this sweep:

### 1. `spa_193_undirected_pattern::undirected_returns_both_directions`

**Root cause** (`crates/sparrowdb-execution/src/engine.rs`):
The backward pass of `(a)-[r]-(b)` matching checked
`seen_undirected.contains(&(a_slot, b_slot))` to avoid re-emitting pairs
already produced by the forward pass.  This was backwards: the backward
row about to be emitted is `(b_slot, a_slot)`, so the dedup check must be
`seen_undirected.contains(&(b_slot, a_slot))`.  With the wrong check, any
directed edge a→b suppressed its own backward traversal, returning only 1
row instead of 2.

**Fix**: Changed the contains() key from `(a_slot, b_slot)` to `(b_slot, a_slot)`.

### 2. `uc1_social_graph` — col_id mismatch

**Root cause** (`crates/sparrowdb/tests/uc1_social_graph.rs`):
The test stored node properties using raw integer col_ids `0` and `1`
(literal), but the execution engine looks up properties via
`col_id_of("col_0")` / `col_id_of("col_1")` (FNV-1a hash, values
2141575598 and 2158353217).  The mismatch caused property filters like
`{col_0: 1}` to never match, returning 0 rows.

**Fix**: Updated the test to call `col_id_of("col_0")` / `col_id_of("col_1")`
when building the node store, aligning stored keys with the engine's lookup.
