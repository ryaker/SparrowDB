//! Regression tests for #515 — every non-`PropAccess` expression in a
//! RETURN following a MATCH silently returned `Null` instead of its value.
//!
//! # Root cause
//!
//! The routing decision between the fast `project_row` column-lookup path
//! and the row-map eval path (`needs_node_ref_in_return`, `engine/mod.rs`)
//! was a deny-list: it OR'd together checks that each recursed looking for
//! one specific reason to bail out (a scalar `FnCall`, a graph-only
//! construct, `id(var)`, a bare `var`) and defaulted `_ => false` for
//! anything none of them recognized. A `BinOp` (`n.age + 1`), a bare
//! `Literal` (`42`), a `List` (`[1, 2]`), or an `InList` (`n.age IN
//! [10, 20]`) built purely from literals and `PropAccess` tripped none of
//! those checks, so it silently took the fast `project_row` path — which
//! only ever positively handles `PropAccess` and falls through to
//! `_ => Value::Null` for everything else. The result was a wrong answer
//! with no error, not a crash: the worst case is a *mixed* row where one
//! column resolves correctly (a `PropAccess`) and the next is silently
//! `Null` (anything else), which looks like real data.
//!
//! # The fix
//!
//! `needs_node_ref_in_return` is now built on an allow-list,
//! `expr_projectable_by_row`, whose only positive case is `PropAccess` —
//! matching the one positive case `project_row` itself handles. Anything
//! else routes to the eval path by construction, so a future `Expr` variant
//! cannot silently repeat this bug.
//!
//! Every expected value below is derived by hand from the fixture, never
//! captured from program output (see repo `feedback_derive_expected_from_source`).

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

/// One `Item` node: name="a", age=10. A second node, `Item` with name="b",
/// has no `age` at all — the control for "genuinely null" below.
fn setup() -> (GraphDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Item {name: 'a', age: 10})")
        .expect("CREATE a");
    db.execute("CREATE (n:Item {name: 'b'})")
        .expect("CREATE b (no age)");
    (db, dir)
}

/// Query against just the `age: 10` node so every test below has one row.
fn setup_single() -> (GraphDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Item {name: 'a', age: 10})")
        .expect("CREATE a");
    (db, dir)
}

// ═══════════════════════════════════════════════════════════════════════════
// Bare literals (issue's original repro table)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bare_int_literal_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN 42").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int64(42)]], "actual: {:?}", r.rows);
}

#[test]
fn bare_string_literal_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN 'hello'").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::String("hello".into())]],
        "actual: {:?}",
        r.rows
    );
}

#[test]
fn bare_bool_literal_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN true").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Bool(true)]],
        "actual: {:?}",
        r.rows
    );
}

#[test]
fn aliased_literal_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN 42 AS answer").unwrap();
    assert_eq!(r.columns, vec!["answer"]);
    assert_eq!(r.rows, vec![vec![Value::Int64(42)]], "actual: {:?}", r.rows);
}

// ═══════════════════════════════════════════════════════════════════════════
// Arithmetic / comparison / IS NULL / list construction / IN — the
// re-scoped issue's actual claim (every non-PropAccess shape, not just
// bare literals).
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn arithmetic_on_property_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN n.age + 1").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Int64(11)]],
        "n.age (10) + 1 = 11; actual: {:?}",
        r.rows
    );
}

#[test]
fn comparison_on_property_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN n.age > 5").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Bool(true)]],
        "10 > 5; actual: {:?}",
        r.rows
    );
}

#[test]
fn is_not_null_on_property_after_match() {
    let (db, _dir) = setup_single();
    let r = db
        .execute("MATCH (n:Item) RETURN n.age IS NOT NULL")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Bool(true)]],
        "n.age is present; actual: {:?}",
        r.rows
    );
}

#[test]
fn list_literal_after_match() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN [1, 2]").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::List(vec![Value::Int64(1), Value::Int64(2)])]],
        "actual: {:?}",
        r.rows
    );
}

#[test]
fn in_list_on_property_after_match() {
    let (db, _dir) = setup_single();
    let r = db
        .execute("MATCH (n:Item) RETURN n.age IN [10, 20]")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Bool(true)]],
        "10 is in [10, 20]; actual: {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// The worst case: a mixed row — one correct PropAccess column beside a
// computed one. Pre-fix, the PropAccess column was right and the computed
// one silently Null, which reads as real data.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn mixed_prop_and_literal_row() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN n.name, 42").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into()), Value::Int64(42)]],
        "actual: {:?}",
        r.rows
    );
}

#[test]
fn mixed_prop_and_arithmetic_row() {
    let (db, _dir) = setup_single();
    let r = db
        .execute("MATCH (n:Item) RETURN n.name, n.age + 1")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into()), Value::Int64(11)]],
        "actual: {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Controls: what must NOT change.
// ═══════════════════════════════════════════════════════════════════════════

/// A genuinely absent property must still project `Null` — the fix must not
/// turn a real null into an error or a fabricated value. `n.age` here is a
/// plain `PropAccess`, so it still takes the fast `project_row` path.
#[test]
fn genuinely_missing_property_still_projects_null() {
    let (db, _dir) = setup();
    let r = db
        .execute("MATCH (n:Item {name: 'b'}) RETURN n.age")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Null]],
        "Item 'b' has no age property; this Null is real, not a routing failure: {:?}",
        r.rows
    );
}

/// `labels(n)` is a `FnCall` — it already took the eval path before this
/// fix (#516 removed its `project_row` handling on the proven basis that
/// every non-aggregate `FnCall` reaches the eval path). Must stay green.
#[test]
fn labels_fn_call_still_works() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN labels(n)").unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::List(vec![Value::String("Item".into())])]],
        "actual: {:?}",
        r.rows
    );
}

/// `id(n)` must still resolve via the eval path (SPA-196 / #372).
#[test]
fn id_fn_call_still_works() {
    let (db, _dir) = setup_single();
    let r = db.execute("MATCH (n:Item) RETURN id(n)").unwrap();
    match &r.rows[0][0] {
        Value::Int64(_) => {}
        other => panic!("expected Int64 node id, got {other:?}"),
    }
}

/// Aliased `PropAccess` (#444) must still resolve correctly — this is the
/// fast path's one true positive case.
#[test]
fn aliased_prop_access_still_works() {
    let (db, _dir) = setup_single();
    let r = db
        .execute("MATCH (n:Item) RETURN n.name AS itemName")
        .unwrap();
    assert_eq!(r.columns, vec!["itemName"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into())]],
        "actual: {:?}",
        r.rows
    );
}

/// A mixed prop + fn-call row (#516's guarantee) must stay green: the
/// `PropAccess` column and the `FnCall` column must both resolve correctly
/// via the eval path.
#[test]
fn mixed_prop_and_fn_call_row_still_works() {
    let (db, _dir) = setup_single();
    let r = db
        .execute("MATCH (n:Item) RETURN n.name, labels(n)")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![
            Value::String("a".into()),
            Value::List(vec![Value::String("Item".into())])
        ]],
        "actual: {:?}",
        r.rows
    );
}
