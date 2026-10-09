//! Issue #482: `is_filter_expr_resolvable` must dispatch each function call
//! node once, so total work is linear (not quadratic) in nesting depth, and
//! the resolvability decision must be unchanged for every expression shape.

use super::*;
use crate::functions::DISPATCH_CALLS;
use sparrowdb_cypher::ast::PropEntry;

fn int(n: i64) -> Expr {
    Expr::Literal(Literal::Int(n))
}
fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FnCall {
        name: name.to_string(),
        args,
        distinct: false,
    }
}
/// `abs(abs(... abs(-1) ...))` with `depth` calls.
fn nested_abs(depth: usize) -> Expr {
    let mut e = int(-1);
    for _ in 0..depth {
        e = call("abs", vec![e]);
    }
    e
}

/// Dispatches performed by one `matches_prop_filter_static` call (the
/// per-candidate-node work on a label scan) for `{id: <expr>}` over a node
/// whose `id` is 1.
fn dispatches_for_one_node(expr: &Expr) -> (usize, bool) {
    let dir = tempfile::tempdir().unwrap();
    let store = NodeStore::open(dir.path()).unwrap();
    let col = prop_name_to_col_id("id");
    let props = [(col, StoreValue::Int64(1).to_u64())];
    let filters = [PropEntry {
        key: "id".to_string(),
        value: expr.clone(),
    }];
    DISPATCH_CALLS.with(|c| c.set(0));
    let matched = matches_prop_filter_static(&props, &filters, &HashMap::new(), &store);
    (DISPATCH_CALLS.with(|c| c.get()), matched)
}

#[test]
fn work_is_linear_in_nesting_depth() {
    let counts: Vec<(usize, usize)> = [1usize, 4, 8, 12, 16]
        .iter()
        .map(|&d| (d, dispatches_for_one_node(&nested_abs(d)).0))
        .collect();
    eprintln!("MEASURE (depth, dispatches): {counts:?}");
    for depth in [1usize, 4, 8, 12, 16] {
        let (calls, matched) = dispatches_for_one_node(&nested_abs(depth));
        eprintln!("depth={depth} dispatches={calls}");
        // abs^depth(-1) == 1 and the node's id is 1, so it must match.
        assert!(matched, "depth {depth} must still match");
        // One dispatch per FnCall node, in total, for check + evaluation.
        assert_eq!(
            calls, depth,
            "depth {depth}: expected one dispatch per node"
        );
    }
}

// ── Equivalence: resolvability decisions per expression shape ────────────────
//
// Expected values are hand-derived from the documented rules (and from
// `fn_abs`/`fn_id`/`fn_type`: `Null` is accepted, a non-null literal of the
// wrong type or the wrong arity is an `Err`), NOT captured from program
// output. These tests pass both before and after the #482 change: they pin
// that the decisions did not move.

fn var(n: &str) -> Expr {
    Expr::Var(n.to_string())
}
fn param(n: &str) -> Expr {
    Expr::Literal(Literal::Param(n.to_string()))
}
fn bin(l: Expr, op: BinOpKind, r: Expr) -> Expr {
    Expr::BinOp {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}
fn bogus() -> Expr {
    call("bogus_fn", vec![int(1)])
}
fn any_pred(variable: &str, list_expr: Expr, predicate: Expr) -> Expr {
    Expr::ListPredicate {
        kind: ListPredicateKind::Any,
        variable: variable.to_string(),
        list_expr: Box::new(list_expr),
        predicate: Box::new(predicate),
    }
}

fn resolvable(e: &Expr, locals: &[&str]) -> bool {
    let mut params = HashMap::new();
    params.insert("$p".to_string(), Value::Int64(5));
    resolve_filter_expr_scoped(e, &params, locals).is_some()
}

#[test]
fn resolvability_decisions_per_shape() {
    let t = true;
    let f = false;
    let null = Expr::Literal(Literal::Null);
    let cases: Vec<(&str, Expr, Vec<&str>, bool)> = vec![
        ("int literal", int(1), vec![], t),
        ("null literal", null.clone(), vec![], t),
        ("present $p", param("p"), vec![], t),
        ("missing $q", param("q"), vec![], f),
        ("abs(-1)", call("abs", vec![int(-1)]), vec![], t),
        (
            "ABS(-1) case-insensitive",
            call("ABS", vec![int(-1)]),
            vec![],
            t,
        ),
        ("unknown fn", bogus(), vec![], f),
        ("abs() arity 0", call("abs", vec![]), vec![], f),
        (
            "abs(1,2) arity 2",
            call("abs", vec![int(1), int(2)]),
            vec![],
            f,
        ),
        ("abs($missing)", call("abs", vec![param("q")]), vec![], f),
        ("abs($p)", call("abs", vec![param("p")]), vec![], t),
        ("abs(bogus(1))", call("abs", vec![bogus()]), vec![], f),
        ("abs(abs(-1))", nested_abs(2), vec![], t),
        (
            "abs(abs(abs(bogus(1))))",
            call("abs", vec![call("abs", vec![call("abs", vec![bogus()])])]),
            vec![],
            f,
        ),
        ("id(1) wrong arg type", call("id", vec![int(1)]), vec![], f),
        ("bare non-local var", var("x"), vec![], f),
        ("bare local var", var("x"), vec!["x"], t),
        ("abs(non-local x)", call("abs", vec![var("x")]), vec![], f),
        (
            "abs(local x) -> abs(Null) ok",
            call("abs", vec![var("x")]),
            vec!["x"],
            t,
        ),
        (
            "id(local x) -> id(Null) ok",
            call("id", vec![var("x")]),
            vec!["x"],
            t,
        ),
        (
            "type(local x) -> type(Null) ok",
            call("type", vec![var("x")]),
            vec!["x"],
            t,
        ),
        (
            "prop access",
            Expr::PropAccess {
                var: "n".into(),
                prop: "x".into(),
            },
            vec![],
            f,
        ),
        (
            "1 + abs(-1)",
            bin(int(1), BinOpKind::Add, call("abs", vec![int(-1)])),
            vec![],
            t,
        ),
        (
            "1 + bogus(1)",
            bin(int(1), BinOpKind::Add, bogus()),
            vec![],
            f,
        ),
        ("1 + var", bin(int(1), BinOpKind::Add, var("x")), vec![], f),
        (
            "abs(1 + abs(-2))",
            call(
                "abs",
                vec![bin(int(1), BinOpKind::Add, call("abs", vec![int(-2)]))],
            ),
            vec![],
            t,
        ),
        (
            "abs(1 + bogus(1))",
            call("abs", vec![bin(int(1), BinOpKind::Add, bogus())]),
            vec![],
            f,
        ),
        (
            "[1, abs(-1)]",
            Expr::List(vec![int(1), call("abs", vec![int(-1)])]),
            vec![],
            t,
        ),
        (
            "[1, bogus(1)]",
            Expr::List(vec![int(1), bogus()]),
            vec![],
            f,
        ),
        (
            "1 IN [1, abs(-2)]",
            Expr::InList {
                expr: Box::new(int(1)),
                list: vec![int(1), call("abs", vec![int(-2)])],
                negated: false,
            },
            vec![],
            t,
        ),
        (
            "bogus(1) IN [1]",
            Expr::InList {
                expr: Box::new(bogus()),
                list: vec![int(1)],
                negated: false,
            },
            vec![],
            f,
        ),
        (
            "1 NOT IN [bogus(1)]",
            Expr::InList {
                expr: Box::new(int(1)),
                list: vec![bogus()],
                negated: true,
            },
            vec![],
            f,
        ),
        (
            "NOT abs(-1)",
            Expr::Not(Box::new(call("abs", vec![int(-1)]))),
            vec![],
            t,
        ),
        ("NOT var", Expr::Not(Box::new(var("y"))), vec![], f),
        ("bogus IS NULL", Expr::IsNull(Box::new(bogus())), vec![], f),
        (
            "1 IS NOT NULL",
            Expr::IsNotNull(Box::new(int(1))),
            vec![],
            t,
        ),
        (
            "true AND bogus",
            Expr::And(
                Box::new(Expr::Literal(Literal::Bool(true))),
                Box::new(bogus()),
            ),
            vec![],
            f,
        ),
        (
            "true OR true",
            Expr::Or(
                Box::new(Expr::Literal(Literal::Bool(true))),
                Box::new(Expr::Literal(Literal::Bool(true))),
            ),
            vec![],
            t,
        ),
        (
            "CASE WHEN true THEN 1 ELSE 2",
            Expr::CaseWhen {
                branches: vec![(Expr::Literal(Literal::Bool(true)), int(1))],
                else_expr: Some(Box::new(int(2))),
            },
            vec![],
            t,
        ),
        (
            "CASE WHEN true THEN 1 (no else)",
            Expr::CaseWhen {
                branches: vec![(Expr::Literal(Literal::Bool(true)), int(1))],
                else_expr: None,
            },
            vec![],
            t,
        ),
        (
            "CASE WHEN true THEN 1 ELSE bogus (untaken branch still checked)",
            Expr::CaseWhen {
                branches: vec![(Expr::Literal(Literal::Bool(true)), int(1))],
                else_expr: Some(Box::new(bogus())),
            },
            vec![],
            f,
        ),
        (
            "CASE WHEN bogus THEN 1",
            Expr::CaseWhen {
                branches: vec![(bogus(), int(1))],
                else_expr: None,
            },
            vec![],
            f,
        ),
        (
            "ANY(x IN [1,2] WHERE x = 1)",
            any_pred(
                "x",
                Expr::List(vec![int(1), int(2)]),
                bin(var("x"), BinOpKind::Eq, int(1)),
            ),
            vec![],
            t,
        ),
        (
            "ANY(x IN [1] WHERE abs(x) = 1)",
            any_pred(
                "x",
                Expr::List(vec![int(1)]),
                bin(call("abs", vec![var("x")]), BinOpKind::Eq, int(1)),
            ),
            vec![],
            t,
        ),
        (
            "ANY(x IN [] WHERE bogus(x) = 1) (empty list, still checked)",
            any_pred(
                "x",
                Expr::List(vec![]),
                bin(call("bogus_fn", vec![var("x")]), BinOpKind::Eq, int(1)),
            ),
            vec![],
            f,
        ),
        (
            "ANY(x IN [1] WHERE y = 1) (y unbound)",
            any_pred(
                "x",
                Expr::List(vec![int(1)]),
                bin(var("y"), BinOpKind::Eq, int(1)),
            ),
            vec![],
            f,
        ),
        (
            "ANY(x IN z WHERE true) (list is non-local var)",
            any_pred("x", var("z"), Expr::Literal(Literal::Bool(true))),
            vec![],
            f,
        ),
        ("CountStar", Expr::CountStar, vec![], f),
    ];
    let mut failures = Vec::new();
    for (name, expr, locals, expected) in &cases {
        let got = resolvable(expr, locals);
        if got != *expected {
            failures.push(format!("{name}: expected {expected}, got {got}"));
        }
        // The value handed back must be what the plain evaluator produces
        // (compared against `eval_expr`, the unchanged reference path).
        if *expected {
            let mut params = HashMap::new();
            params.insert("$p".to_string(), Value::Int64(5));
            let resolved = resolve_filter_expr_scoped(expr, &params, locals);
            if resolved != Some(eval_expr(expr, &params)) {
                failures.push(format!("{name}: resolved value differs from eval_expr"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "decision changes:\n{}",
        failures.join("\n")
    );
}

// ── Equivalence: end-to-end match outcome against a node whose id == 1 ───────

fn matches_id(expr: Expr, props_present: bool) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let store = NodeStore::open(dir.path()).unwrap();
    let col = prop_name_to_col_id("id");
    let props: Vec<(u32, u64)> = if props_present {
        vec![(col, StoreValue::Int64(1).to_u64())]
    } else {
        vec![]
    };
    let filters = [PropEntry {
        key: "id".to_string(),
        value: expr,
    }];
    let mut params = HashMap::new();
    params.insert("$p".to_string(), Value::Int64(1));
    matches_prop_filter_static(&props, &filters, &params, &store)
}

#[test]
fn match_outcomes_per_shape() {
    let tr = Expr::Literal(Literal::Bool(true));
    let fl = Expr::Literal(Literal::Bool(false));
    // (name, expr, matches node {id:1}, matches node with no id)
    let cases: Vec<(&str, Expr, bool, bool)> = vec![
        ("abs(-1) = 1", call("abs", vec![int(-1)]), true, false),
        ("abs(-2) = 2", call("abs", vec![int(-2)]), false, false),
        ("abs^5(-1) = 1", nested_abs(5), true, false),
        ("$p = 1", param("p"), true, false),
        ("abs($p) = 1", call("abs", vec![param("p")]), true, false),
        (
            "-1 + abs(-2) = 1",
            bin(int(-1), BinOpKind::Add, call("abs", vec![int(-2)])),
            true,
            false,
        ),
        (
            "1 + abs(-2) = 3",
            bin(int(1), BinOpKind::Add, call("abs", vec![int(-2)])),
            false,
            false,
        ),
        (
            "abs(-1 + abs(-2)) = 1",
            call(
                "abs",
                vec![bin(int(-1), BinOpKind::Add, call("abs", vec![int(-2)]))],
            ),
            true,
            false,
        ),
        (
            "CASE WHEN true THEN abs(-1) ELSE 2 -> 1",
            Expr::CaseWhen {
                branches: vec![(tr.clone(), call("abs", vec![int(-1)]))],
                else_expr: Some(Box::new(int(2))),
            },
            true,
            false,
        ),
        (
            "CASE WHEN false THEN 1 ELSE abs(-2) -> 2",
            Expr::CaseWhen {
                branches: vec![(fl.clone(), int(1))],
                else_expr: Some(Box::new(call("abs", vec![int(-2)]))),
            },
            false,
            false,
        ),
        (
            "CASE WHEN false THEN 1 (no else) -> null",
            Expr::CaseWhen {
                branches: vec![(fl.clone(), int(1))],
                else_expr: None,
            },
            false,
            true,
        ),
        (
            "1 IN [abs(-1)] -> true -> stored as 1",
            Expr::InList {
                expr: Box::new(int(1)),
                list: vec![call("abs", vec![int(-1)])],
                negated: false,
            },
            true,
            false,
        ),
        (
            "1 IN [abs(-2)] -> false -> stored as 0",
            Expr::InList {
                expr: Box::new(int(1)),
                list: vec![call("abs", vec![int(-2)])],
                negated: false,
            },
            false,
            false,
        ),
        (
            "ANY(x IN [1,2] WHERE x = abs(-1)) -> true",
            any_pred(
                "x",
                Expr::List(vec![int(1), int(2)]),
                bin(var("x"), BinOpKind::Eq, call("abs", vec![int(-1)])),
            ),
            true,
            false,
        ),
        ("null literal", Expr::Literal(Literal::Null), false, true),
        (
            "abs(null) -> null",
            call("abs", vec![Expr::Literal(Literal::Null)]),
            false,
            true,
        ),
        ("bogus(1) fails closed", bogus(), false, false),
        (
            "abs(bogus(1)) fails closed",
            call("abs", vec![bogus()]),
            false,
            false,
        ),
        (
            "abs(1,2) fails closed",
            call("abs", vec![int(1), int(2)]),
            false,
            false,
        ),
        ("$missing fails closed", param("missing"), false, false),
        ("bare var fails closed", var("x"), false, false),
    ];
    let mut failures = Vec::new();
    for (name, expr, with_id, without_id) in &cases {
        let a = matches_id(expr.clone(), true);
        let b = matches_id(expr.clone(), false);
        if a != *with_id || b != *without_id {
            failures.push(format!(
                "{name}: with id expected {with_id} got {a}; without id expected {without_id} got {b}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "outcome changes:\n{}",
        failures.join("\n")
    );
}
