//! Regression guard for issue #467 — pattern-property filters fail OPEN when
//! their value expression cannot be resolved.
//!
//! `matches_prop_filter_static` (`crates/sparrowdb-execution/src/engine/mod.rs`)
//! ended its match arms with `Value::Null => true, // null filter passes
//! (param-like behaviour)`. Every leaf of a pattern-property filter's value
//! expression is evaluated with `eval_expr`, which resolves an unknown
//! variable, an absent `.property` key, or a `$param` the caller never
//! supplied by falling back to `Value::Null` (see `eval_expr`'s `Expr::Var`
//! and `Literal::Param` arms). Before the fix, that made "I could not
//! resolve this filter" indistinguishable from "the filter is null", and
//! both were treated as "pass" — so an unresolvable filter degraded
//! `MATCH (n:Label {prop: <unresolvable>})` into "every live node of
//! `Label`". On a read that over-returns rows nobody asked for; on `SET` or
//! `DELETE` it silently mutates or removes nodes the query never named
//! (verified below).
//!
//! Two conditions were being conflated:
//!   1. The filter value is a `$param` the caller genuinely bound to null,
//!      or an explicit `null` literal written in the query — a real,
//!      resolvable value.
//!   2. The filter value could not be resolved at all (missing `$param`,
//!      an unbound variable, a property access on a variable not in scope).
//!
//! Only (1) has any claim to matching anything, and even then only by the
//! same `Null == Null` convention `values_equal` already uses for
//! WHERE-clause equality elsewhere in the same file (`engine/mod.rs`) — i.e.
//! it matches a node whose *own* property is likewise absent, never every
//! node regardless of its stored value. (2) now fails closed unconditionally
//! via `is_filter_expr_resolvable`, which is checked before the filter value
//! is ever evaluated.
//!
//! All expected values below are derived by hand from each fixture, not
//! captured from a prior run.

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

// ── 1. Read: a missing $param must not match every node of the label ───────
//
// Three nodes exist; `$missing` is never supplied to `execute_with_params`.
// A control node ('c') exists purely to prove the filter didn't degrade to
// "no filter at all" — the pre-fix code returned all three.

#[test]
fn missing_dollar_param_in_read_matches_nothing_not_everything() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap();
    db.execute("CREATE (n:Item {id: 'b'})").unwrap();
    db.execute("CREATE (n:Item {id: 'c'})").unwrap(); // control

    let r = db
        .execute_with_params("MATCH (n:Item {id: $missing}) RETURN n.id", HashMap::new())
        .unwrap();

    // Hand-derived: no node has an `id` equal to an unresolvable filter —
    // the correct answer is the empty set, not all three.
    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "an unresolvable $param must fail closed (no rows), not match \
         every node of the label; got {:?}",
        r.rows
    );
}

// ── 2. Mutation: the data-corruption case — SET must not touch every node ──
//
// This is the scenario the issue calls out explicitly: on a mutation, the
// fail-open bug doesn't just over-return rows, it overwrites/deletes rows
// the caller never named. 'c' is the control: it must retain its original
// value if the fix holds, and would read 99 (like 'a' and 'b') pre-fix.

#[test]
fn missing_dollar_param_in_set_leaves_untargeted_nodes_untouched() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();
    db.execute("CREATE (n:Item {id: 'b', val: 1})").unwrap();
    db.execute("CREATE (n:Item {id: 'c', val: 1})").unwrap(); // control

    db.execute_with_params(
        "MATCH (n:Item {id: $missing}) SET n.val = 99",
        HashMap::new(),
    )
    .unwrap();

    let check = db
        .execute("MATCH (n:Item) RETURN n.id, n.val ORDER BY n.id")
        .unwrap();

    // Hand-derived: nothing matched an unresolvable filter, so every node
    // must still read val=1 — none may read 99.
    assert_eq!(
        check.rows,
        vec![
            vec![Value::String("a".into()), Value::Int64(1)],
            vec![Value::String("b".into()), Value::Int64(1)],
            vec![Value::String("c".into()), Value::Int64(1)],
        ],
        "SET with an unresolvable $param must not write to any node \
         (data-corruption case), got {:?}",
        check.rows
    );
}

// ── 3. Read: a bare (unbound) variable in a sibling pattern's prop filter ──
//
// `a` is a bound node variable (a NodeRef), not a `$param`, referenced bare
// in a second pattern's inline prop filter. This is unresolvable through
// `matches_prop_filter_static` today (it only ever receives `$`-keyed
// params, never row-scope bindings) and must fail closed.

#[test]
fn bare_var_in_sibling_pattern_prop_filter_matches_nothing() {
    let (db, _dir) = open_db();
    db.execute("CREATE (a:A {tag: 'x'})").unwrap();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap();
    db.execute("CREATE (n:Item {id: 'b'})").unwrap();
    db.execute("CREATE (n:Item {id: 'c'})").unwrap(); // control

    let r = db
        .execute("MATCH (a:A), (n:Item {id: a}) RETURN n.id")
        .unwrap();

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "an unbound bare variable in a pattern-prop filter must fail closed, \
         got {:?}",
        r.rows
    );
}

// ── 4. A genuinely-null-bound $param pins the chosen semantic ──────────────
//
// `$t` is *explicitly* supplied and bound to `Value::Null` — this is the
// "genuinely null" case the fix must not silently break. It follows the
// same `Null == Null` convention `values_equal` already uses for WHERE
// equality: it matches only a node whose own property is likewise absent,
// never a node that has the property set to something else.
//
// 'a' has no `tag` property at all (absent); 'b' has `tag: 'x'` (present).
// Hand-derived: only 'a' should match.

#[test]
fn genuinely_null_bound_param_matches_only_nodes_with_absent_prop() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a'})").unwrap(); // no `tag`
    db.execute("CREATE (n:Item {id: 'b', tag: 'x'})").unwrap(); // `tag` present

    let r = db
        .execute_with_params(
            "MATCH (n:Item {tag: $t}) RETURN n.id ORDER BY n.id",
            params(vec![("t", Value::Null)]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into())]],
        "a $param genuinely bound to null must match only the node whose \
         own property is likewise absent, not every node and not none; \
         got {:?}",
        r.rows
    );
}

// ── 5. Same genuinely-null-bound $param, but where NO node qualifies ───────
//
// Both nodes have the property set, so a genuinely-null $param must match
// neither — confirming the fix doesn't silently widen when every candidate
// has a non-null value.

#[test]
fn genuinely_null_bound_param_matches_none_when_prop_always_present() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {id: 'a', val: 1})").unwrap();
    db.execute("CREATE (n:Item {id: 'b', val: 1})").unwrap();

    let r = db
        .execute_with_params(
            "MATCH (n:Item {id: $target}) RETURN n.id",
            params(vec![("target", Value::Null)]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "a genuinely-null $param must not match nodes whose property is \
         always present; got {:?}",
        r.rows
    );
}
