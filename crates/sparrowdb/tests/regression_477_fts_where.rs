//! Regression test for issue #477: `full_text_search()` and `bm25_score()`
//! were unreachable from the query shape anyone would actually write them in.
//!
//! # The defect
//!
//! `Engine::eval_where_graph` -> `Engine::eval_expr_graph` already had an arm
//! that dispatched a *bare* `full_text_search(...)` / `bm25_score(...)` /
//! `hybrid_search(...)` `FnCall` to the right evaluator. Two shapes still fell
//! through the catch-all `_ => eval_expr(expr, vals)`, which recurses with the
//! **generic**, non-graph `eval_expr` — and `functions.rs::dispatch_function`
//! does not know these three names, so they silently evaluated to
//! `Value::Null`/`false`:
//!
//!  1. `full_text_search(var.property, query)` — the 2-arg PropAccess form
//!     (mirroring `bm25_score`'s existing 2-arg form) was not implemented at
//!     all in `eval_full_text_search`, which only accepted the 3-arg
//!     `(label, property, query)` literal form.
//!  2. `bm25_score(var.property, query) > 0.5` — any threshold comparison,
//!     because `eval_expr_graph` had no `Expr::BinOp` arm, so the comparison
//!     (and both its operands) fell to the generic evaluator wholesale.
//!
//! The same fallthrough affected aggregate arguments
//! (`avg(bm25_score(...))`) and grouping keys in `aggregate_rows` /
//! `aggregate_with_items` (both in `crates/sparrowdb-execution/src/engine/`),
//! and `ORDER BY bm25_score(...)` in `aggregate.rs::execute_match_with`.
//!
//! # Fix
//!
//!  - `eval_full_text_search` (`engine/expr.rs`) now accepts both the 3-arg
//!    literal form and the 2-arg `PropAccess` form, resolving the node/label
//!    from the bound variable exactly as `eval_bm25_score` already did.
//!  - `eval_expr_graph` gained a `BinOp` arm mirroring the generic one but
//!    recursing through `eval_expr_graph` on both operands.
//!  - `aggregate_rows` (`engine/mod.rs`), `extract_collect_arg`, and
//!    `aggregate_with_items` (`engine/aggregate.rs`) now evaluate aggregate
//!    arguments and grouping keys via `eval_expr_graph`.
//!  - `execute_match_with`'s `ORDER BY` (`engine/aggregate.rs`) now sorts via
//!    `eval_expr_graph` so a non-aliased FTS call resolves.
//!
//! NOT fixed (reported as follow-up, not in scope for #477): `ORDER BY
//! bm25_score(...)` in a plain `MATCH ... RETURN ... ORDER BY` with **no**
//! `WITH` clause (`apply_order_by` in `engine/mod.rs`) only resolves
//! `PropAccess`/`Var` expressions to an existing output column by name — it
//! never evaluates an arbitrary expression, graph-aware or not. That is a
//! structural limitation shared by every non-column `ORDER BY` expression
//! (e.g. `ORDER BY abs(n.x)` has the same limitation), not specific to FTS
//! routing, and fixing it requires threading row-level `HashMap` access into
//! `apply_order_by`, which today only sees already-flattened `Vec<Value>`
//! rows plus column names.
//!
//! Every expected value below is derived by hand from the fixture built in
//! each test, never captured from a run of the code.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

fn exec(db: &GraphDb, cypher: &str) {
    db.execute(cypher)
        .unwrap_or_else(|e| panic!("exec failed for `{cypher}`: {e}"));
}

/// Shared fixture used by most tests below:
///
/// | id | text                                       | category | contains 'graph'? |
/// |----|--------------------------------------------|----------|--------------------|
/// | a  | "graph database indexing"                   | tech     | yes (1x)           |
/// | b  | "unrelated cooking recipe"                   | food     | no                 |
/// | c  | "graph theory and graph traversal basics"    | food     | yes (2x)           |
/// | d  | "category placeholder text"                  | tech     | no                 |
///
/// `category` lets AND/OR tests combine an FTS predicate with an ordinary
/// property predicate whose truth value is independent of it (a: both true,
/// b: both false, c: FTS-only, d: category-only).
fn build_fixture(db: &GraphDb) {
    exec(db, "CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)");
    exec(
        db,
        "CREATE (:Doc {id: 'a', text: 'graph database indexing', category: 'tech'})",
    );
    exec(
        db,
        "CREATE (:Doc {id: 'b', text: 'unrelated cooking recipe', category: 'food'})",
    );
    exec(
        db,
        "CREATE (:Doc {id: 'c', text: 'graph theory and graph traversal basics', category: 'food'})",
    );
    exec(
        db,
        "CREATE (:Doc {id: 'd', text: 'category placeholder text', category: 'tech'})",
    );
}

fn ids(result: &sparrowdb::QueryResult) -> Vec<String> {
    let mut v: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected n.id to be a String, got {other:?}"),
        })
        .collect();
    v.sort();
    v
}

/// Truncate the on-disk FTS index file to 3 bytes — too short to satisfy the
/// first `read_u64` in `FtsIndexData::load`, which needs 8 — so `open()`
/// hits a real decode failure. Same technique as
/// `regression_462_fts_open_swallow.rs::corrupt_fts_index_file`.
fn corrupt_fts_index_file(db_root: &std::path::Path, label: &str, property: &str) {
    let path = db_root.join("fts").join(format!("{label}__{property}.bin"));
    assert!(
        path.exists(),
        "expected {} to already exist from CREATE FULLTEXT INDEX before corrupting it",
        path.display()
    );
    std::fs::write(&path, [0xFFu8, 0xFF, 0xFF]).expect("truncate fts index file to 3 bytes");
}

// ── 1. Bare predicate, 2-arg PropAccess form ──────────────────────────────────

#[test]
fn full_text_search_two_arg_propaccess_bare_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    let r = db
        .execute("MATCH (n:Doc) WHERE full_text_search(n.text, 'graph') RETURN n.id")
        .expect("query failed");
    assert_eq!(
        ids(&r),
        vec!["a".to_string(), "c".to_string()],
        "a and c contain 'graph', b and d do not; got {:?}",
        r.rows
    );
}

// ── 2. Negated ─────────────────────────────────────────────────────────────

#[test]
fn full_text_search_negated_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    let r = db
        .execute("MATCH (n:Doc) WHERE NOT full_text_search(n.text, 'graph') RETURN n.id")
        .expect("query failed");
    assert_eq!(
        ids(&r),
        vec!["b".to_string(), "d".to_string()],
        "b and d do NOT contain 'graph'; got {:?}",
        r.rows
    );
}

// ── 3. Threshold comparison ───────────────────────────────────────────────────

#[test]
fn bm25_score_threshold_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    let r = db
        .execute("MATCH (n:Doc) WHERE bm25_score(n.text, 'graph') > 0.0 RETURN n.id")
        .expect("query failed");
    assert_eq!(
        ids(&r),
        vec!["a".to_string(), "c".to_string()],
        "only a and c can have a positive BM25 score for 'graph'; got {:?}",
        r.rows
    );
}

// ── 4. Combined with AND against an ordinary property predicate ──────────────

#[test]
fn bm25_score_threshold_and_ordinary_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    let r = db
        .execute(
            "MATCH (n:Doc) WHERE bm25_score(n.text, 'graph') > 0.0 AND n.category = 'tech' \
             RETURN n.id",
        )
        .expect("query failed");
    assert_eq!(
        ids(&r),
        vec!["a".to_string()],
        "only 'a' satisfies both the FTS predicate and category = 'tech' \
         (c matches FTS only, d matches category only, b matches neither); got {:?}",
        r.rows
    );
}

// ── 5. Combined with OR against an ordinary property predicate ───────────────

#[test]
fn full_text_search_or_ordinary_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    let r = db
        .execute(
            "MATCH (n:Doc) WHERE full_text_search(n.text, 'graph') OR n.category = 'tech' \
             RETURN n.id",
        )
        .expect("query failed");
    assert_eq!(
        ids(&r),
        vec!["a".to_string(), "c".to_string(), "d".to_string()],
        "a satisfies both, c satisfies FTS only, d satisfies category only; \
         b satisfies neither; got {:?}",
        r.rows
    );
}

// ── 6. Corrupt index must still fail closed in WHERE (interaction w/ #462) ───

#[test]
fn full_text_search_where_fails_closed_on_corrupt_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    corrupt_fts_index_file(dir.path(), "Doc", "text");

    // Bare predicate: eval_full_text_search returns Value::Null (not
    // Value::Bool) on a corrupt index, which is not a `Bool(true)`, so the
    // row must be rejected exactly like a genuine non-match.
    let r1 = db
        .execute("MATCH (n:Doc) WHERE full_text_search(n.text, 'graph') RETURN n.id")
        .expect("read-only MATCH must not fail even though the index is broken");
    assert!(
        r1.rows.is_empty(),
        "a corrupt index must fail closed (zero rows), not pass every row \
         through by treating Null as a pass; got {:?}",
        r1.rows
    );

    // Threshold comparison: the new BinOp arm must not turn a Null score into
    // a passing comparison either.
    let r2 = db
        .execute("MATCH (n:Doc) WHERE bm25_score(n.text, 'graph') > 0.0 RETURN n.id")
        .expect("read-only MATCH must not fail even though the index is broken");
    assert!(
        r2.rows.is_empty(),
        "Gt(Null, 0.0) must reject every row, not pass any; got {:?}",
        r2.rows
    );
}

// ── 7. Unknown function in WHERE must still fail closed (parity w/ #467) ─────

#[test]
fn unknown_function_in_where_matches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    // Bare predicate form: a genuinely unknown function must still reject
    // every row, exactly like regression_467's
    // `unknown_function_in_prop_filter_matches_nothing` does for the pattern-
    // property-filter shape of the same guard.
    let r1 = db
        .execute("MATCH (n:Doc) WHERE bogus_fn_477(n.text) RETURN n.id")
        .expect("should not error");
    assert!(
        r1.rows.is_empty(),
        "an unknown function must fail closed, not match every row; got {:?}",
        r1.rows
    );

    // Threshold/BinOp form: the new BinOp arm in eval_expr_graph must not
    // accidentally open a pass-through for unknown functions either.
    let r2 = db
        .execute("MATCH (n:Doc) WHERE bogus_fn_477(n.text) > 0 RETURN n.id")
        .expect("should not error");
    assert!(
        r2.rows.is_empty(),
        "Gt(Null, 0) from an unknown function must reject every row; got {:?}",
        r2.rows
    );

    // Combined with AND against an otherwise-true ordinary predicate: proves
    // the unknown-function side is not simply ignored by AND.
    let r3 = db
        .execute("MATCH (n:Doc) WHERE bogus_fn_477(n.text) AND n.category = 'tech' RETURN n.id")
        .expect("should not error");
    assert!(
        r3.rows.is_empty(),
        "AND must still reject when the unknown-function side rejects, even \
         though category = 'tech' matches a and d; got {:?}",
        r3.rows
    );
}

// ── 8. Aggregate argument routing ─────────────────────────────────────────────

#[test]
fn avg_bm25_score_aggregate_resolves() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    // Before the fix, `avg`'s argument was evaluated via the generic,
    // non-graph `eval_expr`, which does not know `bm25_score` and returns
    // Value::Null for every row. Aggregates ignore Null contributions, so the
    // accumulator was empty for every group and `finalize_aggregate(Avg, [])`
    // returns Value::Null (see `finalize_aggregate` in `engine/mod.rs`).
    let r = db
        .execute("MATCH (n:Doc) RETURN avg(bm25_score(n.text, 'graph'))")
        .expect("query failed");
    assert_eq!(r.rows.len(), 1);
    match &r.rows[0][0] {
        Value::Float64(v) => assert!(
            *v > 0.0,
            "a and c both have a positive score for 'graph', so the average \
             over all four rows (including b's and d's 0.0) must be positive; got {v}"
        ),
        other => panic!(
            "expected a resolved Float64 average, got {other:?} (Null means the \
             aggregate argument was not routed through the graph-aware evaluator)"
        ),
    }
}

/// Uses `collect()` rather than `avg()` for the aggregate: `aggregate_with_items`'s
/// grouped-finalization `match` (`engine/aggregate.rs`, after the per-row
/// accumulation loop this test otherwise exercises) has no `avg` arm and falls
/// through to `Value::Null` for every group regardless of #477 — a distinct,
/// pre-existing gap, unrelated to FTS routing, that this test deliberately
/// avoids so it isolates the thing #477 actually fixed: whether the
/// aggregate's *argument* (`bm25_score(n.text, 'graph')`) resolves per row via
/// `eval_expr_graph` instead of silently scoring every row `Null` and leaving
/// each group's accumulator empty.
#[test]
fn grouped_collect_bm25_score_resolves_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    build_fixture(&db);

    // tech = {a: score > 0, d: score = 0.0}
    // food = {b: score = 0.0, c: score > 0}
    let r = db
        .execute(
            "MATCH (n:Doc) WITH n.category AS cat, collect(bm25_score(n.text, 'graph')) AS scores \
             RETURN cat, scores ORDER BY cat",
        )
        .expect("query failed");
    assert_eq!(
        r.rows.len(),
        2,
        "expected one row per category, got {:?}",
        r.rows
    );

    // ORDER BY cat: 'food' < 'tech' lexicographically.
    let (food_row, tech_row) = (&r.rows[0], &r.rows[1]);
    assert_eq!(food_row[0], Value::String("food".into()));
    assert_eq!(tech_row[0], Value::String("tech".into()));

    for (label, row) in [("food", food_row), ("tech", tech_row)] {
        let scores: Vec<f64> = match &row[1] {
            Value::List(items) => items
                .iter()
                .map(|v| match v {
                    Value::Float64(f) => *f,
                    other => panic!("{label}: expected Float64 in collected list, got {other:?}"),
                })
                .collect(),
            other => panic!("{label}: expected a resolved List, got {other:?}"),
        };
        assert_eq!(
            scores.len(),
            2,
            "{label}: expected 2 collected scores (one per group member), got {scores:?}"
        );
        assert!(
            scores.iter().any(|s| *s > 0.0),
            "{label}: one member contains 'graph' and must contribute a positive \
             score; got {scores:?}"
        );
        assert!(
            scores.contains(&0.0),
            "{label}: one member does not contain 'graph' and must contribute a \
             0.0 score (not be dropped or Null); got {scores:?}"
        );
    }
}

// ── 9. ORDER BY within a WITH pipeline (aggregate.rs::execute_match_with) ────

#[test]
fn order_by_bare_bm25_score_after_with() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    // Two-node fixture: unambiguous ordering (one has a positive score, one
    // has zero — no need to reason about relative magnitude between two
    // positive scores). Deliberately created in the OPPOSITE order from the
    // expected DESC-by-score result: a no-op sort (the pre-fix bug, where
    // every row's score evaluates to Value::Null and every comparison is
    // therefore Ordering::Equal) would leave scan/insertion order ['b', 'a']
    // untouched and this test would still pass by coincidence. Creating 'b'
    // (score 0.0) first makes a no-op sort produce the wrong order, so this
    // only passes when ORDER BY actually reorders by score.
    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)");
    exec(
        &db,
        "CREATE (:Doc {id: 'b', text: 'unrelated cooking recipe'})",
    );
    exec(
        &db,
        "CREATE (:Doc {id: 'a', text: 'graph database indexing'})",
    );

    let r = db
        .execute("MATCH (n:Doc) WITH n ORDER BY bm25_score(n.text, 'graph') DESC RETURN n.id")
        .expect("query failed");
    let got: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {other:?}"),
        })
        .collect();
    assert_eq!(
        got,
        vec!["a".to_string(), "b".to_string()],
        "'a' has a positive score for 'graph' and must sort before 'b' \
         (score 0.0) under ORDER BY ... DESC; got {got:?}"
    );
}
