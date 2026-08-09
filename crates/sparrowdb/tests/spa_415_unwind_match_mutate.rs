//! SPA-415: `UNWIND … MATCH … SET/DELETE` and `DELETE … RETURN` e2e coverage.
//!
//! PR #411 added 576 lines across ast.rs/binder.rs/parser.rs/engine/mod.rs/db.rs
//! with zero tests. This file covers the two headline features end-to-end
//! against a real `GraphDb` on real disk (tempdir, real WAL, real catalog):
//!
//!   1. `UNWIND $list AS row MATCH (n {prop: row.field}) SET/DELETE ...`
//!   2. `MATCH (n) DELETE n RETURN count(n)` (and the UNWIND equivalent)
//!
//! Every UNWIND-mutate query in this file uses the `$param` + PropAccess
//! (`row.field`) form, not the literal-list form. That is deliberate, not an
//! oversight — see the "KNOWN BUG" test at the bottom and the PR report for
//! why the literal-list path (`execute_unwind_match_mutate`, no `$param`) is
//! currently unusable for any non-empty list.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;
use tempfile::tempdir;

fn open_db() -> (GraphDb, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    (db, dir)
}

/// Build a `$param` map-list value: `[{k1: v1, ...}, ...]` from row tuples.
fn map_list(rows: Vec<Vec<(&str, Value)>>) -> Value {
    Value::List(
        rows.into_iter()
            .map(|entries| {
                Value::Map(
                    entries
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                )
            })
            .collect(),
    )
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

// ── 1. UNWIND MATCH SET via $param — only targeted nodes change ────────────

/// Three nodes exist; the UNWIND list names two of them. Only those two may
/// change — the third is the control that would catch an implementation that
/// mutates every row of the label instead of filtering by the UNWIND value.
#[test]
fn unwind_param_set_updates_only_matched_nodes() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Memory {id: 'm1', score: 1})")
        .unwrap();
    db.execute("CREATE (n:Memory {id: 'm2', score: 1})")
        .unwrap();
    db.execute("CREATE (n:Memory {id: 'm3', score: 1})")
        .unwrap();

    let p = params(vec![(
        "updates",
        map_list(vec![
            vec![
                ("id", Value::String("m1".into())),
                ("score", Value::Int64(50)),
            ],
            vec![
                ("id", Value::String("m2".into())),
                ("score", Value::Int64(90)),
            ],
        ]),
    )]);

    let r = db
        .execute_with_params(
            "UNWIND $updates AS row MATCH (n:Memory {id: row.id}) SET n.score = row.score",
            p,
        )
        .unwrap();
    assert!(r.rows.is_empty(), "no RETURN clause => empty result");

    let check = db
        .execute("MATCH (n:Memory) RETURN n.id, n.score ORDER BY n.id")
        .unwrap();
    let got: Vec<(String, i64)> = check
        .rows
        .iter()
        .map(|row| {
            let id = match &row[0] {
                Value::String(s) => s.clone(),
                other => panic!("expected string id, got {other:?}"),
            };
            let score = match &row[1] {
                Value::Int64(n) => *n,
                other => panic!("expected int score, got {other:?}"),
            };
            (id, score)
        })
        .collect();

    // Hand-derived from the fixture: m1->50, m2->90, m3 untouched (still 1).
    assert_eq!(
        got,
        vec![
            ("m1".to_string(), 50),
            ("m2".to_string(), 90),
            ("m3".to_string(), 1),
        ],
        "m3 was never named in the UNWIND list and must be untouched"
    );
}

// ── 2. $param resolves in both WHERE and SET (the 5 Gemini findings) ───────

/// `$threshold` in WHERE and `$newval` in SET must both resolve against the
/// params map threaded through `execute_unwind_mutate_inner`. Two nodes exist
/// on either side of the threshold; only the one below it may change.
///
/// A implementation that forgets to call `engine.with_params()` (or forgets
/// to pass params into `resolve_set_value`) would either error on `$threshold`
/// / `$newval` or silently treat them as NULL — this test would catch either.
#[test]
fn unwind_param_where_and_set_value_both_resolve_dollar_param() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();
    db.execute("CREATE (n:Item {id: 'b', val: 100})").unwrap();

    let p = params(vec![
        (
            "updates",
            map_list(vec![
                vec![("id", Value::String("a".into()))],
                vec![("id", Value::String("b".into()))],
            ]),
        ),
        ("threshold", Value::Int64(50)),
        ("newval", Value::Int64(999)),
    ]);

    db.execute_with_params(
        "UNWIND $updates AS row MATCH (n:Item {id: row.id}) \
         WHERE n.val < $threshold SET n.val = $newval",
        p,
    )
    .unwrap();

    // Hand-derived: only 'a' (val=1 < 50) crosses the WHERE threshold.
    let a = db.execute("MATCH (n:Item {id: 'a'}) RETURN n.val").unwrap();
    assert_eq!(a.rows, vec![vec![Value::Int64(999)]]);
    let b = db.execute("MATCH (n:Item {id: 'b'}) RETURN n.val").unwrap();
    assert_eq!(
        b.rows,
        vec![vec![Value::Int64(100)]],
        "'b' (val=100) must not pass WHERE n.val < 50"
    );
}

// ── 3. SET n.prop = row.field (PropAccess in SET value, commit aa29e59) ────

/// Regression guard for commit aa29e59: the parser used to reject non-literal
/// SET values, which broke `SET n.score = row.score`. Each row carries a
/// *different* field value so a broken implementation that fell back to a
/// single shared literal (or NULL) would be caught immediately.
#[test]
fn unwind_set_value_from_row_prop_access() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Doc {id: 'd1', rank: 0})").unwrap();
    db.execute("CREATE (n:Doc {id: 'd2', rank: 0})").unwrap();

    let p = params(vec![(
        "rows",
        map_list(vec![
            vec![
                ("id", Value::String("d1".into())),
                ("rank", Value::Int64(7)),
            ],
            vec![
                ("id", Value::String("d2".into())),
                ("rank", Value::Int64(42)),
            ],
        ]),
    )]);

    db.execute_with_params(
        "UNWIND $rows AS row MATCH (n:Doc {id: row.id}) SET n.rank = row.rank",
        p,
    )
    .unwrap();

    let check = db
        .execute("MATCH (n:Doc) RETURN n.id, n.rank ORDER BY n.id")
        .unwrap();
    assert_eq!(
        check.rows,
        vec![
            vec![Value::String("d1".into()), Value::Int64(7)],
            vec![Value::String("d2".into()), Value::Int64(42)],
        ],
        "each node must receive its own row's rank, not a shared/last-row value"
    );
}

// ── 4. UNWIND MATCH DELETE (node) ───────────────────────────────────────────

#[test]
fn unwind_param_delete_removes_only_matched_nodes() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap();
    db.execute("CREATE (n:Item {id: 'b'})").unwrap();
    db.execute("CREATE (n:Item {id: 'c'})").unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![
            vec![("id", Value::String("a".into()))],
            vec![("id", Value::String("b".into()))],
        ]),
    )]);

    db.execute_with_params("UNWIND $ids AS row MATCH (n:Item {id: row.id}) DELETE n", p)
        .unwrap();

    let remaining = db.execute("MATCH (n:Item) RETURN n.id").unwrap();
    assert_eq!(
        remaining.rows,
        vec![vec![Value::String("c".into())]],
        "only 'c' (never named in the UNWIND list) should survive"
    );
}

/// DETACH DELETE via UNWIND must also remove incident edges (not just the
/// node), and the CSR cache must be invalidated so a later scan doesn't see
/// a stale edge. A plain (non-detach) DELETE on a node with edges would leave
/// dangling edge records — this test would fail if `has_detach_delete` /
/// `invalidate_csr_map` wiring were dropped.
#[test]
fn unwind_param_detach_delete_removes_node_and_edges() {
    let (db, _dir) = open_db();
    db.execute("CREATE (a:P {id: 'a'})").unwrap();
    db.execute("CREATE (b:P {id: 'b'})").unwrap();
    db.execute("MATCH (a:P {id: 'a'}), (b:P {id: 'b'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![vec![("id", Value::String("a".into()))]]),
    )]);

    db.execute_with_params(
        "UNWIND $ids AS row MATCH (n:P {id: row.id}) DETACH DELETE n",
        p,
    )
    .unwrap();

    let nodes = db.execute("MATCH (n:P) RETURN n.id").unwrap();
    assert_eq!(nodes.rows, vec![vec![Value::String("b".into())]]);

    let edges = db
        .execute("MATCH (x:P)-[r:KNOWS]->(y:P) RETURN x.id, y.id")
        .unwrap();
    assert!(
        edges.rows.is_empty(),
        "the KNOWS edge must be gone once its source node is detach-deleted, got {:?}",
        edges.rows
    );
}

// ── 5. UNWIND MATCH DELETE on an edge pattern: documented restriction ──────

/// `scan_match_mutate` (shared by both the plain and UNWIND mutate paths)
/// explicitly rejects any pattern with a relationship hop:
/// "MATCH...SET/DELETE currently supports only single-node patterns (no
/// relationships)". Unlike the plain `MATCH (a)-[r]->(b) DELETE r` path
/// (`execute_match_mutate`), the UNWIND path never calls
/// `is_edge_delete_mutation` / `scan_match_mutate_edges` at all — so
/// `UNWIND ... MATCH (a)-[r:REL]->(b) DELETE r` cannot reach the edge-delete
/// code path and always errors instead of silently misbehaving. This test
/// pins that current (safe, if incomplete) behaviour.
#[test]
fn unwind_match_delete_edge_pattern_errors_instead_of_silently_misbehaving() {
    let (db, _dir) = open_db();
    db.execute("CREATE (a:P {id: 'a'})").unwrap();
    db.execute("CREATE (b:P {id: 'b'})").unwrap();
    db.execute("MATCH (a:P {id: 'a'}), (b:P {id: 'b'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![vec![("id", Value::String("a".into()))]]),
    )]);

    let r = db.execute_with_params(
        "UNWIND $ids AS row MATCH (a:P {id: row.id})-[r:KNOWS]->(b:P) DELETE r",
        p,
    );
    let err = r.expect_err("edge-hop patterns are not supported on the UNWIND mutate path");
    let msg = err.to_string();
    assert!(
        msg.contains("single-node patterns"),
        "expected the scan_match_mutate 'single-node patterns' error, got: {msg}"
    );

    // The edge must still exist — the query must have failed before any write.
    let edges = db
        .execute("MATCH (x:P)-[r:KNOWS]->(y:P) RETURN x.id, y.id")
        .unwrap();
    assert_eq!(
        edges.rows,
        vec![vec![Value::String("a".into()), Value::String("b".into())]],
        "a rejected mutation must not have touched the edge"
    );
}

// ── 6. DELETE ... RETURN — the second headline feature ─────────────────────

/// `build_mutate_return` only special-cases `count(n)` / `count(*)`; any
/// other RETURN expression after a mutation falls back to an empty result
/// (the doc comment calls this "deferred to a future engine extension").
/// This is the plain (non-UNWIND) path.
#[test]
fn match_delete_return_count_reports_correct_count() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap();
    db.execute("CREATE (n:Item {id: 'b'})").unwrap();
    db.execute("CREATE (n:Item {id: 'c'})").unwrap();

    let r = db
        .execute("MATCH (n:Item) WHERE n.id <> 'c' DELETE n RETURN count(n)")
        .unwrap();
    assert_eq!(r.columns, vec!["count(n)".to_string()]);
    assert_eq!(r.rows, vec![vec![Value::Int64(2)]]);

    let remaining = db.execute("MATCH (n:Item) RETURN n.id").unwrap();
    assert_eq!(remaining.rows, vec![vec![Value::String("c".into())]]);
}

/// Same feature, but through the UNWIND pipeline, and with `total_mutated`
/// accumulated across multiple UNWIND rows (not just one MATCH scan). Also
/// exercises "element matching no node" (`zzz`) in the same list to confirm
/// it contributes 0 to the count rather than erroring the whole statement.
#[test]
fn unwind_delete_return_count_sums_across_rows() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap();
    db.execute("CREATE (n:Item {id: 'b'})").unwrap();
    db.execute("CREATE (n:Item {id: 'c'})").unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![
            vec![("id", Value::String("a".into()))],
            vec![("id", Value::String("b".into()))],
            vec![("id", Value::String("zzz".into()))], // matches nothing
        ]),
    )]);

    let r = db
        .execute_with_params(
            "UNWIND $ids AS row MATCH (n:Item {id: row.id}) DELETE n RETURN count(n)",
            p,
        )
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int64(2)]]);

    let remaining = db.execute("MATCH (n:Item) RETURN n.id").unwrap();
    assert_eq!(remaining.rows, vec![vec![Value::String("c".into())]]);
}

/// Non-count RETURN after an UNWIND DELETE: per `build_mutate_return`'s doc
/// comment, only `count(n)`/`count(*)` are honoured — `RETURN n` must not
/// error, but must return an empty projection rather than the deleted node.
#[test]
fn unwind_delete_return_non_count_expr_yields_empty_result() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![vec![("id", Value::String("a".into()))]]),
    )]);

    let r = db
        .execute_with_params(
            "UNWIND $ids AS row MATCH (n:Item {id: row.id}) DELETE n RETURN n",
            p,
        )
        .unwrap();
    assert!(
        r.rows.is_empty(),
        "RETURN n (non-count) after a mutation is documented as unsupported \
         and must fall back to empty, not error and not project the node"
    );

    // The delete itself must still have taken effect.
    let remaining = db.execute("MATCH (n:Item) RETURN n.id").unwrap();
    assert!(remaining.rows.is_empty());
}

// ── 7. Empty list / no-match element — must not error ───────────────────────

#[test]
fn unwind_empty_param_list_is_a_no_op() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();

    let p = params(vec![("ids", Value::List(vec![]))]);
    let r = db
        .execute_with_params(
            "UNWIND $ids AS row MATCH (n:Item {id: row.id}) SET n.val = 99",
            p,
        )
        .unwrap();
    assert!(r.rows.is_empty());

    let check = db.execute("MATCH (n:Item {id: 'a'}) RETURN n.val").unwrap();
    assert_eq!(
        check.rows,
        vec![vec![Value::Int64(1)]],
        "empty UNWIND list must leave existing data untouched"
    );
}

#[test]
fn unwind_element_matching_no_node_does_not_error() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'exists', val: 1})")
        .unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![
            vec![("id", Value::String("exists".into()))],
            vec![("id", Value::String("missing".into()))],
        ]),
    )]);
    let r = db
        .execute_with_params(
            "UNWIND $ids AS row MATCH (n:Item {id: row.id}) SET n.val = 5",
            p,
        )
        .unwrap();
    assert!(
        r.rows.is_empty(),
        "no RETURN clause => empty result, not an error"
    );

    let check = db
        .execute("MATCH (n:Item {id: 'exists'}) RETURN n.val")
        .unwrap();
    assert_eq!(check.rows, vec![vec![Value::Int64(5)]]);
}

// ── 8. Durability: mutation must survive a reopen ───────────────────────────

/// SET via the UNWIND pipeline must be durable: after `tx.commit()`, dropping
/// the `GraphDb` and reopening the same path must show the mutated value.
/// This is the check that catches a mutation applied only to an in-memory
/// snapshot that never reached the WAL.
#[test]
fn unwind_set_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = GraphDb::open(&path).unwrap();
        db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();
        let p = params(vec![(
            "ids",
            map_list(vec![vec![("id", Value::String("a".into()))]]),
        )]);
        db.execute_with_params(
            "UNWIND $ids AS row MATCH (n:Item {id: row.id}) SET n.val = 777",
            p,
        )
        .unwrap();
        // db dropped at end of this block.
    }

    let db2 = GraphDb::open(&path).unwrap();
    let check = db2
        .execute("MATCH (n:Item {id: 'a'}) RETURN n.val")
        .unwrap();
    assert_eq!(
        check.rows,
        vec![vec![Value::Int64(777)]],
        "SET via UNWIND MATCH must be durable across a reopen"
    );
}

/// Same durability check for DELETE via the UNWIND pipeline.
#[test]
fn unwind_delete_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = GraphDb::open(&path).unwrap();
        db.execute("CREATE (n:Item {id: 'a'})").unwrap();
        db.execute("CREATE (n:Item {id: 'b'})").unwrap();
        let p = params(vec![(
            "ids",
            map_list(vec![vec![("id", Value::String("a".into()))]]),
        )]);
        db.execute_with_params("UNWIND $ids AS row MATCH (n:Item {id: row.id}) DELETE n", p)
            .unwrap();
    }

    let db2 = GraphDb::open(&path).unwrap();
    let remaining = db2.execute("MATCH (n:Item) RETURN n.id").unwrap();
    assert_eq!(
        remaining.rows,
        vec![vec![Value::String("b".into())]],
        "DELETE via UNWIND MATCH must be durable across a reopen"
    );
}

// ── 9. Documented visibility limitation ─────────────────────────────────────

/// `execute_unwind_mutate_inner`'s doc comment states: "each UNWIND row is
/// scanned against committed storage — earlier rows' mutations within the
/// same transaction are NOT visible to later rows' MATCH scans." This test
/// pins exactly that semantic with a WHERE clause whose truth value would
/// flip if row-to-row visibility existed.
///
/// Setup: one node, val=1. Two UNWIND rows both target it, both guarded by
/// `WHERE n.val < 5`.
///   - If mutations WERE visible across rows: row 1 sets val=100, so row 2's
///     WHERE (100 < 5) would be false — only 1 node mutated, final val=100.
///   - Per the documented (actual) behaviour: both rows evaluate WHERE against
///     the same pre-transaction snapshot (val=1 < 5, true for both), so both
///     apply their SET — final val is row 2's value (last write wins), and
///     the mutation count is 2, not 1.
#[test]
fn unwind_rows_do_not_see_earlier_rows_mutations_within_same_statement() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();

    let p = params(vec![(
        "updates",
        map_list(vec![
            vec![
                ("id", Value::String("a".into())),
                ("out", Value::Int64(100)),
            ],
            vec![
                ("id", Value::String("a".into())),
                ("out", Value::Int64(200)),
            ],
        ]),
    )]);

    let r = db
        .execute_with_params(
            "UNWIND $updates AS row MATCH (n:Item {id: row.id}) \
             WHERE n.val < 5 SET n.val = row.out RETURN count(n)",
            p,
        )
        .unwrap();

    // Both rows matched (per the documented same-snapshot semantics), not just
    // the first one. If the engine ever gains row-to-row visibility, this
    // count would drop to 1 and this assertion would need updating alongside
    // the doc comment.
    assert_eq!(
        r.rows,
        vec![vec![Value::Int64(2)]],
        "both UNWIND rows must see the pre-statement snapshot (val=1), not each \
         other's writes — see execute_unwind_mutate_inner's visibility doc comment"
    );

    let check = db.execute("MATCH (n:Item {id: 'a'}) RETURN n.val").unwrap();
    assert_eq!(
        check.rows,
        vec![vec![Value::Int64(200)]],
        "last UNWIND row to write wins"
    );
}

// ── 10. KNOWN BUG (found while writing this suite, not one of the original
//         5 Gemini findings): a pattern property that references the UNWIND
//         alias directly (`{id: row}`) instead of via PropAccess (`{id:
//         row.id}`) silently matches and mutates EVERY node of the label,
//         instead of erroring or matching nothing. ────────────────────────
//
// Root cause (traced against source, not guessed):
//   - `resolve_pattern_props`/`row_expr_to_literal` (db.rs) only resolve
//     `Expr::PropAccess { var, prop }` against the UNWIND row. A bare
//     `Expr::Var("row")` pattern-prop value is left untouched.
//   - The unresolved `Expr::Var("row")` then reaches
//     `sparrowdb_execution::engine::mod::eval_expr`, which for `Expr::Var`
//     does `vals.get(v.as_str()).cloned().unwrap_or(Value::Null)` — "row" is
//     never a `$`-prefixed key in the params/dollar_params map, so this
//     always evaluates to `Value::Null`.
//   - `matches_prop_filter_static` (engine/mod.rs) then treats a `Value::Null`
//     filter as an automatic pass ("null filter passes (param-like
//     behaviour)"), for EVERY candidate node — the inline property filter
//     becomes a no-op and every live node of the label is returned as
//     "matching".
//
// Verified empirically (not asserted from a single anecdotal run): with 3
// nodes of label :Item and a `$ids` list naming only one of them, `UNWIND
// $ids AS row MATCH (n:Item {id: row}) SET n.val = 5` sets val=5 on ALL
// THREE nodes, not just the named one.
//
// This is a silent-data-corruption footgun in a *mutation* context (unlike a
// read-only MATCH misfire, this overwrites/deletes rows the caller never
// asked for) and is a very natural mistake to make — `UNWIND list AS x
// MATCH (n {prop: x})` is the standard Neo4j idiom when x is a scalar; here
// it silently requires `x.field` because the row is always a Map. Per the
// task's ground rule ("never bend a test to fit observed behaviour"), this
// test asserts the CORRECT Cypher semantics (only the named node changes)
// and is left `#[ignore]`, pointing at a bug rather than passing as if
// nothing were wrong. Team lead: please file this as its own issue —
// recommend either (a) rejecting non-PropAccess pattern-prop values that
// reference the UNWIND alias at parse/bind time, or (b) making
// `matches_prop_filter_static` treat an *unresolved variable* filter as "no
// match" rather than "always match".
#[test]
#[ignore = "SPA-415 follow-up: {id: row} (bare alias, no .field) in an UNWIND \
            MATCH pattern silently matches every node of the label instead of \
            erroring or matching none — see the comment above this test"]
fn unwind_pattern_prop_bare_alias_var_must_not_match_every_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();
    db.execute("CREATE (n:Item {id: 'b', val: 1})").unwrap();
    db.execute("CREATE (n:Item {id: 'c', val: 1})").unwrap();

    let p = params(vec![(
        "ids",
        map_list(vec![vec![("id", Value::String("a".into()))]]),
    )]);

    // NOTE: `{id: row}` — bare alias, not `row.id`. `row` is bound to a Map,
    // so this is nonsensical as written, but it must not silently degrade
    // into "no filter at all".
    db.execute_with_params(
        "UNWIND $ids AS row MATCH (n:Item {id: row}) SET n.val = 5",
        p,
    )
    .unwrap();

    let check = db
        .execute("MATCH (n:Item) RETURN n.id, n.val ORDER BY n.id")
        .unwrap();
    // Correct Cypher-derived expectation: either nothing matches (row is a
    // Map, not comparable to a string id) or, generously, only 'a' via some
    // future coercion — but 'b' and 'c' must never change.
    assert_ne!(
        check.rows,
        vec![
            vec![Value::String("a".into()), Value::Int64(5)],
            vec![Value::String("b".into()), Value::Int64(5)],
            vec![Value::String("c".into()), Value::Int64(5)],
        ],
        "BUG: the property filter degraded to match-all and mutated every node"
    );
}

// ── #468: the read pipeline must survive the SPA-415 dispatch ───────────────
//
// `parse_unwind` pre-consumes MATCH (and now WHERE) to look ahead for
// SET/DELETE. Two ways that broke the read path, both regressions against
// behaviour that worked on 6254f91:
//
//   1. The patterns were handed to `parse_pipeline_continuation` as
//      `leading_match` while `leading_unwind` was also set.
//      `execute_pipeline_inner` picks the leading clause with an `if / else if`
//      chain testing `leading_unwind` first, so the MATCH arm was unreachable
//      and the pattern was dropped silently — every row projected NULL.
//   2. Dispatching on `Token::Where` sent every read carrying a predicate into
//      the mutation tail, which rejected it outright.
//
// Both are asserted on values derived by hand from the fixture below, not from
// what the engine currently returns.

#[test]
fn unwind_match_return_binds_the_pattern_variable() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Item {id: 'a', v: 5})").unwrap();
    db.execute("CREATE (:Item {id: 'b', v: 1})").unwrap();
    db.execute("CREATE (:Item {id: 'c', v: 9})").unwrap();

    // 'c' is the control: never named, so it must not appear.
    let r = db
        .execute("UNWIND ['a', 'b'] AS x MATCH (n:Item {id: x}) RETURN n.id")
        .expect("UNWIND ... MATCH ... RETURN must parse and execute");

    assert_eq!(
        r.rows,
        vec![
            vec![Value::String("a".into())],
            vec![Value::String("b".into())],
        ],
        "#468: MATCH was dropped — rows project NULL when the pattern variable never binds"
    );
}

#[test]
fn unwind_match_where_return_applies_the_predicate() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Item {id: 'a', v: 5})").unwrap();
    db.execute("CREATE (:Item {id: 'b', v: 1})").unwrap();

    // Both ids are named; only 'a' has v > 3, so the predicate is what
    // distinguishes a working WHERE from one that was never applied.
    let r = db
        .execute("UNWIND ['a', 'b'] AS x MATCH (n:Item {id: x}) WHERE n.v > 3 RETURN n.id")
        .expect("#468: a read pipeline with WHERE must not be routed to the mutation tail");

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into())]],
        "#468: WHERE must filter the read pipeline; 'b' has v=1 and must not survive"
    );
}
