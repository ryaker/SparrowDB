//! Regression guards for issues #472 and #479 — two remaining directions of
//! the "absent / unresolved / null are three different things" conflation
//! family that #467/#471 partly closed.
//!
//! All expected values below are derived by hand from each fixture, never
//! captured from a prior run (repo rule — see CLAUDE.md).
//!
//! ── #472 ─────────────────────────────────────────────────────────────────
//! `matches_prop_filter_with_binding` (`engine/scan.rs`), used by the
//! pipeline `WITH … MATCH` re-traversal stage, had the same "unresolved
//! expression treated as `Value::Null`" conflation in its `Expr::Var` arm
//! that #467 fixed in `matches_prop_filter_static`. A bare variable not
//! present in the row `binding` (a typo, or a variable never carried
//! through from the preceding `WITH`) defaulted to `Value::Null`, which then
//! matched every node whose own property happened to be absent — a
//! symptom-free wrong answer, not the "match everything" #467 guarded
//! against, but wrong all the same.
//!
//! ── #479 ─────────────────────────────────────────────────────────────────
//! After #471, a pattern-property filter whose `$param` is genuinely bound
//! to null matches absent-property nodes on the read path, which combines
//! `get_node_raw_nullable` with `filter_map`, but it matches nothing on
//! three call sites that used the plain, zero-sentineled `get_node_raw`:
//! `mutation.rs::node_matches_prop_filter` (every SET/DELETE),
//! `expr.rs::find_node_by_props` (shortestPath endpoint resolution by
//! label+props), and the EXISTS-pattern dst-node check in
//! `expr.rs::eval_exists_subquery`.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;
use tempfile::tempdir;

fn open_db() -> (GraphDb, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    (db, dir)
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

// ═══════════════════════════════════════════════════════════════════════
// #472 — matches_prop_filter_with_binding (pipeline WITH … MATCH)
// ═══════════════════════════════════════════════════════════════════════

// ── 1. An unresolved bare var in the second MATCH's prop filter must fail
// closed, not match the node whose own property happens to be absent. ──────
//
// `undefined_var` is never projected by the WITH stage (only `dummy` is), so
// it cannot be resolved from `binding`. Fixture: 'a' has no `tag` (absent),
// 'b' has `tag: 'x'` (present). Pre-fix, the unresolved var defaulted to
// Value::Null, which matched 'a' via the `(None, Value::Null) => true` arm.
// Hand-derived correct answer: neither node may match an unresolvable filter.
#[test]
fn unresolved_var_in_pipeline_match_prop_filter_matches_nothing() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Trigger {x: 1})").unwrap(); // drives one WITH…MATCH pass
    db.execute("CREATE (:Item {id: 'a'})").unwrap(); // no `tag`
    db.execute("CREATE (:Item {id: 'b', tag: 'x'})").unwrap(); // `tag` present

    let r = db
        .execute(
            "MATCH (t:Trigger) \
             WITH t.x AS dummy \
             MATCH (m:Item {tag: undefined_var}) \
             RETURN m.id",
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "an unresolved bare var in a pipeline-stage prop filter must fail \
         closed, not match the node whose property is absent; got {:?}",
        r.rows
    );
}

// ── 2. Control: a var that IS resolvable via binding still matches by value. ──
//
// Confirms the resolvability gate doesn't also break the legitimate case.
// Hand-derived: `want` = "x", only 'b' has tag = "x".
#[test]
fn resolvable_binding_var_in_pipeline_match_still_matches_by_value() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Trigger {x: 1})").unwrap();
    db.execute("CREATE (:Item {id: 'a'})").unwrap();
    db.execute("CREATE (:Item {id: 'b', tag: 'x'})").unwrap();

    let r = db
        .execute(
            "MATCH (t:Trigger) \
             WITH 'x' AS want \
             MATCH (m:Item {tag: want}) \
             RETURN m.id",
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        vec![vec![Value::String("b".into())]],
        "a resolvable binding var must match by its actual value; got {:?}",
        r.rows
    );
}

// ── 3. Control: a resolvable var whose value matches no node returns empty,
// not the absent-prop node — proves the fix doesn't overcorrect into always
// matching 'a' regardless of resolvability. ─────────────────────────────────
#[test]
fn resolvable_binding_var_with_no_matching_value_matches_nothing() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Trigger {x: 1})").unwrap();
    db.execute("CREATE (:Item {id: 'a'})").unwrap(); // no `tag`
    db.execute("CREATE (:Item {id: 'b', tag: 'x'})").unwrap();

    let r = db
        .execute(
            "MATCH (t:Trigger) \
             WITH 'y' AS want \
             MATCH (m:Item {tag: want}) \
             RETURN m.id",
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "'y' matches neither node's tag ('a' absent, 'b' = 'x'); got {:?}",
        r.rows
    );
}

// ── 4. A binding var genuinely bound to null (via WITH) must still match the
// absent-prop node — the resolvability gate must not break this pre-existing,
// intentional case (mirrors regression_467's test 4 for the static function). ──
#[test]
fn genuinely_null_bound_binding_var_matches_only_absent_prop_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Trigger {x: 1})").unwrap();
    db.execute("CREATE (:Item {id: 'a'})").unwrap(); // no `tag`
    db.execute("CREATE (:Item {id: 'b', tag: 'x'})").unwrap();

    let r = db
        .execute(
            "MATCH (t:Trigger) \
             WITH null AS want \
             MATCH (m:Item {tag: want}) \
             RETURN m.id",
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into())]],
        "a binding var genuinely bound to null must match only the node \
         whose own property is likewise absent; got {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════
// #479 — absent conflated with zero (mutation-side call sites)
// ═══════════════════════════════════════════════════════════════════════

// ── 5. READ control: a $param genuinely bound to null matches the node with
// the absent property (already covered by regression_467, restated here as
// the read-side half of the read/mutation divergence #479 is about). ───────
#[test]
fn read_side_null_bound_param_matches_absent_prop_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Item {id: 'a', val: 1})").unwrap(); // no `tag`
    db.execute("CREATE (:Item {id: 'b', tag: 'x', val: 1})")
        .unwrap();

    let r = db
        .execute_with_params(
            "MATCH (n:Item {tag: $t}) RETURN n.id",
            params(vec![("t", Value::Null)]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into())]],
        "read path: only the absent-tag node must match; got {:?}",
        r.rows
    );
}

// ── 6. SET: the exact divergence from the issue. Same fixture and query
// shape as test 5, but SET instead of RETURN. Hand-derived: only 'a' (absent
// tag) matches, so only 'a' gets val=99; 'b' (tag present) is untouched. ───
#[test]
fn set_with_null_bound_param_updates_only_absent_prop_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Item {id: 'a', val: 1})").unwrap(); // no `tag`
    db.execute("CREATE (:Item {id: 'b', tag: 'x', val: 1})")
        .unwrap();

    db.execute_with_params(
        "MATCH (n:Item {tag: $t}) SET n.val = 99",
        params(vec![("t", Value::Null)]),
    )
    .unwrap();

    let check = db
        .execute("MATCH (n:Item) RETURN n.id, n.val ORDER BY n.id")
        .unwrap();

    assert_eq!(
        check.rows,
        vec![
            vec![Value::String("a".into()), Value::Int64(99)],
            vec![Value::String("b".into()), Value::Int64(1)],
        ],
        "SET must update only the node whose property is genuinely absent \
         (matching the read path), not leave every node untouched; got {:?}",
        check.rows
    );
}

// ── 7. DELETE: same shape, mutation.rs::node_matches_prop_filter also backs
// DELETE. Hand-derived: only 'a' (absent tag) is deleted; 'b' survives. ────
#[test]
fn delete_with_null_bound_param_removes_only_absent_prop_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Item {id: 'a'})").unwrap(); // no `tag`
    db.execute("CREATE (:Item {id: 'b', tag: 'x'})").unwrap();

    db.execute_with_params(
        "MATCH (n:Item {tag: $t}) DELETE n",
        params(vec![("t", Value::Null)]),
    )
    .unwrap();

    let check = db
        .execute("MATCH (n:Item) RETURN n.id ORDER BY n.id")
        .unwrap();

    assert_eq!(
        check.rows,
        vec![vec![Value::String("b".into())]],
        "DELETE must remove only the node whose property is genuinely \
         absent, leaving the present-tag node in place; got {:?}",
        check.rows
    );
}

// ── 8. EXISTS { }: the dst-node prop filter inside an EXISTS subquery must
// also see absence as absence. 'a1' has one edge to 'b1' (tag absent) and
// none to a tag-present node. Hand-derived: EXISTS { (a)-[:R]->(:B {tag:
// $t}) } with $t = null must be true for 'a1' via the b1 edge. ─────────────
#[test]
fn exists_subquery_dst_prop_filter_matches_absent_prop_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:A {id: 'a1'})").unwrap();
    db.execute("CREATE (:B {id: 'b1'})").unwrap(); // no `tag`
    db.execute("CREATE (:B {id: 'b2', tag: 'x'})").unwrap();
    db.execute("MATCH (a:A {id: 'a1'}), (b:B {id: 'b1'}) CREATE (a)-[:R]->(b)")
        .unwrap();

    let r = db
        .execute_with_params(
            "MATCH (a:A) WHERE EXISTS { (a)-[:R]->(:B {tag: $t}) } RETURN a.id",
            params(vec![("t", Value::Null)]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a1".into())]],
        "EXISTS {{ }} dst-node prop filter must match the neighbour whose \
         property is genuinely absent; got {:?}",
        r.rows
    );
}

// ── 9. EXISTS { } control: when the only neighbour HAS the property set,
// a null-bound filter must not match it (proves test 8 isn't vacuously true). ──
#[test]
fn exists_subquery_dst_prop_filter_does_not_match_present_prop_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:A {id: 'a1'})").unwrap();
    db.execute("CREATE (:B {id: 'b2', tag: 'x'})").unwrap();
    db.execute("MATCH (a:A {id: 'a1'}), (b:B {id: 'b2'}) CREATE (a)-[:R]->(b)")
        .unwrap();

    let r = db
        .execute_with_params(
            "MATCH (a:A) WHERE EXISTS { (a)-[:R]->(:B {tag: $t}) } RETURN a.id",
            params(vec![("t", Value::Null)]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "a null-bound filter must not match a neighbour whose property is \
         actually present; got {:?}",
        r.rows
    );
}
