//! Regression guard for issue #523 — a mis-keyed parameter silently returns
//! zero rows instead of erroring.
//!
//! `GraphDb::execute_with_params` takes a `HashMap<String, Value>` keyed by
//! the *bare* parameter name. The Cypher lexer (`sparrowdb_cypher::lexer`)
//! strips the `$` sigil when tokenizing `$name` in query text, so
//! `Expr`/`Literal::Param` and the engine's lookup never see a leading `$`.
//! A caller who keys the map with the sigil — the natural mistake, since
//! that's how the parameter appears in the query text — was previously
//! silently mis-keyed: the lookup missed, the filter fell back to an
//! unresolvable value, and (post-#467) that fails *closed*: zero rows, no
//! error, indistinguishable from a query that legitimately matched nothing.
//!
//! The fix rejects any params-map key with a leading `$` outright, in
//! `GraphDb::execute_with_params` (`crates/sparrowdb/src/db.rs`) — the single
//! entry point shared by every binding (Node's `executeWithParams`, Python's
//! `execute_with_params`, the HTTP server's `/cypher` handler, and
//! `helpers.rs`), so the fix is inherited by all of them without any binding
//! changes.
//!
//! Deliberately unchanged (out of scope, #467/#471 territory):
//!   - a typo'd key (`tt` for `t`) — still returns silent-empty. Catching
//!     that requires walking the full bound AST for every `$param`
//!     reference and was not attempted here (see #523 discussion).
//!   - an *omitted* parameter (`params {}`) — still returns silent-empty,
//!     which is the correct fail-closed behavior `regression_467.rs` guards.
//!
//! All expected values below are derived by hand from each fixture, never
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

// ── 1. Control: the correct bare key must keep working ──────────────────
//
// Hand-derived: one `:Item` node has `tag: 'x'`, so `MATCH (n:Item {tag: $t})`
// with `$t` bound to `'x'` must return exactly that node's name.

#[test]
fn correct_bare_key_still_matches() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {name: 'a', tag: 'x'})").unwrap();

    let r = db
        .execute_with_params(
            "MATCH (n:Item {tag: $t}) RETURN n.name",
            params(vec![("t", Value::String("x".into()))]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a".into())]],
        "the correctly-keyed param must still match; got {:?}",
        r.rows
    );
}

// ── 2. The sigil form must now error, not silently return empty ─────────
//
// Pre-fix (confirmed against the pre-fix commit below): this returned
// `Ok(QueryResult { rows: [], .. })` — the mis-keyed "$t" entry in the map
// never matched the engine's bare-name lookup, so the filter fell back to
// unresolvable and failed closed with no error signal at all.

#[test]
fn sigil_prefixed_key_errors_instead_of_silently_empty() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {name: 'a', tag: 'x'})").unwrap();

    let r = db.execute_with_params(
        "MATCH (n:Item {tag: $t}) RETURN n.name",
        params(vec![("$t", Value::String("x".into()))]),
    );

    let err = r.expect_err(
        "a params-map key with a leading '$' must be rejected, not silently \
         return empty rows",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("$t") && msg.contains('t'),
        "error should name the offending key and the expected bare form; got: {msg}"
    );
}

// ── 3. Control: a typo'd key is explicitly out of scope — still empty ───
//
// Hand-derived: no node has `tag` matching an unresolvable filter value, so
// the correct answer remains the empty set (not an error) — this fix does
// not attempt to catch unreferenced/typo'd keys.

#[test]
fn typo_key_still_silently_empty_out_of_scope() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {name: 'a', tag: 'x'})").unwrap();

    let r = db
        .execute_with_params(
            "MATCH (n:Item {tag: $t}) RETURN n.name",
            params(vec![("tt", Value::String("x".into()))]),
        )
        .unwrap();

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "a typo'd key is out of scope for this fix and must remain \
         silent-empty, not error; got {:?}",
        r.rows
    );
}

// ── 4. Control: the omitted-parameter case must keep failing closed ─────
//
// This is the #467/#471 behavior explicitly out of scope for #523 — must
// not become an error as a side effect of this fix.

#[test]
fn omitted_param_still_silently_empty_control() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {name: 'a', tag: 'x'})").unwrap();

    let r = db
        .execute_with_params("MATCH (n:Item {tag: $t}) RETURN n.name", HashMap::new())
        .unwrap();

    assert_eq!(
        r.rows,
        Vec::<Vec<Value>>::new(),
        "omitting the param entirely must remain silent-empty (#467/#471 \
         territory, not this fix); got {:?}",
        r.rows
    );
}

// ── 5. A sigil-prefixed key must error even when other keys are correct ──
//
// Guards against a naive implementation that only checks a single key or
// bails after the first "good" key.

#[test]
fn sigil_key_errors_even_alongside_correct_keys() {
    let (db, _dir) = open_db();
    db.execute("CREATE (n:Item {name: 'a', tag: 'x', extra: 1})")
        .unwrap();

    let r = db.execute_with_params(
        "MATCH (n:Item {tag: $t, extra: $e}) RETURN n.name",
        params(vec![
            ("t", Value::String("x".into())),
            ("$e", Value::Int64(1)),
        ]),
    );

    assert!(
        r.is_err(),
        "a sigil-prefixed key must be rejected even when a sibling key in \
         the same map is correctly formed"
    );
}
