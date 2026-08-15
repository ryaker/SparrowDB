//! Regression tests for #475 and #473: a null (or other non-scalar) property
//! value silently written as `Int64(0)` instead of being rejected.
//!
//! ```rust,ignore
//! db.execute("CREATE (:Item {id: 'a', v: 7})")?;
//! let mut p = HashMap::new();
//! p.insert("nv".to_string(), Value::Null);
//! db.execute_with_params("MATCH (n:Item {id: 'a'}) SET n.v = $nv", p)?;
//! // MATCH (n:Item) RETURN n.id, n.v  ->  [["a", Int64(0)]]  (pre-fix)
//! ```
//!
//! `0` is a legal stored value, so a coerced null was indistinguishable on
//! read from a genuine stored zero: no error, no way to detect which rows
//! were silently corrupted, and the original value gone for good.
//!
//! ## Chosen semantic: reject, don't coerce and don't silently remove
//!
//! `SET n.prop = null` and `CREATE (n {prop: null})` both return
//! `Err(InvalidArgument)` rather than either (a) writing `0` or (b) treating
//! null as "remove the property". Removal was considered and rejected for
//! this fix: the storage layer has no primitive to delete a single property
//! column for an existing node (`upsert_node_col`/`set_null_bit` only ever
//! mark a column present, never clear it back to absent), so "null removes
//! the property" would require a new WAL-able storage operation, a
//! `WriteTx` enum change, and edits to every one of the ~8 `Mutation::Set`
//! match arms in `db.rs` — a real feature, not a bug fix, and out of scope
//! here. Rejecting is also consistent with the CREATE-path precedent PR
//! #492 already established (`resolve_create_prop_value`): "one null
//! convention for this call site, not two, rather than inventing 'null
//! means omit the property'". `List` / `Map` / `Vector` / `NodeRef` /
//! `EdgeRef` property values are rejected the same way — none of them has a
//! scalar storage representation either.
//!
//! ## Blast radius fixed here
//!
//! The catch-all-to-`Int64(0)` pattern existed in six functions, not just
//! the two named in the issues:
//! - `exec_value_to_storage` (#475's named function, helpers.rs)
//! - `value_to_store_value` (#473's named function, sparrowdb-execution)
//! - `literal_to_store_value` (same crate, dead code, same pattern)
//! - `literal_to_value` / `expr_to_value` (helpers.rs) — reachable via the
//!   *non-parameterized* `SET`/`MERGE` paths, a sibling of #475 that
//!   neither issue's repro exercises (`SET n.v = null` with no `$param`
//!   at all). Left unfixed, this would have been the same bug reachable
//!   through a one-token-simpler query.
//! - `literal_to_value_with_params` (helpers.rs)
//!
//! `resolve_create_prop_value`'s `$param` branch used to hand-duplicate
//! `exec_value_to_storage`'s match specifically because that function's
//! catch-all could not be trusted. Now that it can, the duplication is
//! gone — see `helpers.rs`.

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::types::Value;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn item_v(db: &GraphDb, id: &str) -> Value {
    let rows = db
        .execute(&format!("MATCH (n:Item {{id: '{id}'}}) RETURN n.v"))
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1, "exactly one Item with id='{id}'");
    rows[0][0].clone()
}

// ── #475: SET n.v = $param(null) ───────────────────────────────────────────

/// The exact reproduction from the issue. Pre-fix: `db.execute_with_params`
/// returns `Ok`, and the value is silently `Int64(0)` — the caller never
/// supplied a zero, and the original `7` is gone. Post-fix: the write is
/// rejected and `v` is still `7`, derived by hand from the CREATE statement
/// above, not captured from a prior run.
#[test]
fn set_null_via_param_rejected_old_value_preserved() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

    let err = db
        .execute_with_params(
            "MATCH (n:Item {id: 'a'}) SET n.v = $nv",
            params(&[("nv", Value::Null)]),
        )
        .expect_err("SET n.v = $nv (null) must be rejected, not coerced to 0");
    let msg = err.to_string();
    assert!(
        msg.contains("null"),
        "error '{msg}' should mention the value is null"
    );

    assert_eq!(
        item_v(&db, "a"),
        Value::Int64(7),
        "the pre-existing value must survive a rejected null SET, not become 0"
    );
}

/// Sibling of the above that neither #475 nor #473 named: a *literal* null
/// with no `$param` involved at all reaches a completely different function
/// (`literal_to_value`, not `exec_value_to_storage`) but had the exact same
/// catch-all-to-0 defect.
#[test]
fn set_null_literal_no_params_rejected_old_value_preserved() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

    let err = db
        .execute("MATCH (n:Item {id: 'a'}) SET n.v = null")
        .expect_err("SET n.v = null (literal, no params) must be rejected, not coerced to 0");
    assert!(err.to_string().contains("null"));

    assert_eq!(
        item_v(&db, "a"),
        Value::Int64(7),
        "the pre-existing value must survive a rejected literal-null SET"
    );
}

// ── #475 sibling: non-scalar $param values on SET ──────────────────────────

#[test]
fn set_list_param_rejected_old_value_preserved() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

    let err = db
        .execute_with_params(
            "MATCH (n:Item {id: 'a'}) SET n.v = $nv",
            params(&[("nv", Value::List(vec![Value::Int64(1), Value::Int64(2)]))]),
        )
        .expect_err("SET n.v = $nv (list) must be rejected, not coerced to 0");
    assert!(err.to_string().contains("list"), "error: {err}");
    assert_eq!(item_v(&db, "a"), Value::Int64(7));
}

#[test]
fn set_map_param_rejected_old_value_preserved() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

    let err = db
        .execute_with_params(
            "MATCH (n:Item {id: 'a'}) SET n.v = $nv",
            params(&[("nv", Value::Map(vec![("k".to_string(), Value::Int64(1))]))]),
        )
        .expect_err("SET n.v = $nv (map) must be rejected, not coerced to 0");
    assert!(err.to_string().contains("map"), "error: {err}");
    assert_eq!(item_v(&db, "a"), Value::Int64(7));
}

/// A vector `$param` written onto a plain (non-HNSW-indexed) property has no
/// storage representation either — it must error, not become `0` and
/// silently discard the vector data.
#[test]
fn set_vector_param_onto_plain_property_rejected() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

    let err = db
        .execute_with_params(
            "MATCH (n:Item {id: 'a'}) SET n.v = $nv",
            params(&[("nv", Value::Vector(vec![0.1, 0.2, 0.3]))]),
        )
        .expect_err("SET n.v = $nv (vector, non-indexed prop) must be rejected, not coerced to 0");
    assert!(err.to_string().contains("vector"), "error: {err}");
    assert_eq!(item_v(&db, "a"), Value::Int64(7));
}

// ── #473: CREATE with a null property ───────────────────────────────────────

/// `execute_create_standalone` (used by plain `db.execute`) already rejected
/// a literal null pre-fix (PR #492's `resolve_create_prop_value`) — this is
/// the pre-existing, still-correct behavior, re-asserted here as a control
/// so a future change to this file can tell "still works" from "newly
/// fixed" apart. See `create_batch_null_literal_rejected` below for the
/// arm that #473's fix actually touches.
#[test]
fn create_standalone_literal_null_rejected_no_node_created() {
    let (_dir, db) = make_db();
    let err = db
        .execute("CREATE (:Item {id: 'a', v: null})")
        .expect_err("CREATE with a literal null property must be rejected");
    assert!(err.to_string().contains("null"), "error: {err}");

    let rows = db.execute("MATCH (n:Item) RETURN n.id").unwrap().rows;
    assert_eq!(rows.len(), 0, "no partial node may be left behind");
}

/// #473's exact named function (`value_to_store_value` /
/// `crates/sparrowdb-execution/src/engine/mod.rs`) is reached via
/// `execute_batch_mutation`'s `Statement::Create` arm — this used to
/// hand-roll its own Null/Param rejection (already correct) but delegate to
/// the then-unsafe `literal_to_value` catch-all for every other literal.
/// This test exercises that arm directly via `execute_batch`, and doubles
/// as confirmation that simplifying it to call `resolve_create_prop_value`
/// (removing the duplicated hand-rolled check) didn't change behavior.
#[test]
fn create_batch_null_literal_rejected() {
    let (_dir, db) = make_db();
    let result = db.execute_batch(&["CREATE (:Item {id: 'a', v: null})"]);
    let err = result.expect_err("batch CREATE with null property must be rejected");
    assert!(err.to_string().contains("null"), "error: {err}");

    let rows = db.execute("MATCH (n:Item) RETURN n.id").unwrap().rows;
    assert_eq!(rows.len(), 0, "no partial node may be left behind");
}

#[test]
fn create_batch_param_reference_rejected_batch_is_non_parameterized() {
    let (_dir, db) = make_db();
    let result = db.execute_batch(&["CREATE (:Item {id: 'a', v: $nv})"]);
    let err = result.expect_err("batch CREATE cannot resolve $param — batch is non-parameterized");
    assert!(
        err.to_string().contains("execute_with_params") || err.to_string().contains("parameter"),
        "error: {err}"
    );
}

// ── MERGE: literal null in the merge-key props and in ON CREATE/MATCH SET ──

#[test]
fn merge_key_prop_null_rejected() {
    let (_dir, db) = make_db();
    let err = db
        .execute("MERGE (n:Item {id: 'a', v: null})")
        .expect_err("MERGE with a null key property must be rejected, not coerced to 0");
    assert!(err.to_string().contains("null"), "error: {err}");

    let rows = db.execute("MATCH (n:Item) RETURN n.id").unwrap().rows;
    assert_eq!(rows.len(), 0, "no partial node may be left behind");
}

#[test]
fn merge_on_create_set_null_rejected_no_node_left_behind() {
    let (_dir, db) = make_db();
    let err = db
        .execute("MERGE (n:Item {id: 'a'}) ON CREATE SET n.v = null")
        .expect_err("MERGE ... ON CREATE SET n.v = null must be rejected, not coerced to 0");
    assert!(err.to_string().contains("null"), "error: {err}");
}

// ── UNWIND ... MATCH ... SET: a null element in the list literal ───────────

/// `x` itself is never a valid `SET` right-hand side in this dialect (a bare
/// variable reference isn't a literal or `$param`, and is rejected on that
/// basis regardless of its value), so `DELETE` — which has no value
/// expression at all — is used here to isolate the list-literal-build step
/// (`db.rs`'s `expr_to_value(e)` over each `UNWIND [...]` item) from that
/// unrelated, pre-existing restriction. The mutation never runs: list
/// construction must fail before any row is scanned.
#[test]
fn unwind_list_literal_null_item_rejected_during_list_construction() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

    let err = db
        .execute("UNWIND [null] AS x MATCH (n:Item {id: 'a'}) DELETE n")
        .expect_err("UNWIND list literal containing null must be rejected, not coerced to 0");
    assert!(err.to_string().contains("null"), "error: {err}");

    let rows = db.execute("MATCH (n:Item) RETURN n.id").unwrap().rows;
    assert_eq!(
        rows.len(),
        1,
        "the node must survive: list construction must fail before the DELETE ever scans a row"
    );
}

// ── Durability: the rejection must not leave a partial write on disk ───────

#[test]
fn rejected_null_set_does_not_persist_across_checkpoint_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (:Item {id: 'a', v: 7})").unwrap();

        db.execute_with_params(
            "MATCH (n:Item {id: 'a'}) SET n.v = $nv",
            params(&[("nv", Value::Null)]),
        )
        .expect_err("null SET must be rejected");

        db.checkpoint().expect("checkpoint");
    }

    {
        let db = GraphDb::open(&db_path).expect("open session 2 (reopen)");
        assert_eq!(
            item_v(&db, "a"),
            Value::Int64(7),
            "a rejected null SET must not have persisted a 0 across checkpoint + reopen"
        );
    }
}

// ── Control: a genuine zero is unaffected by this fix ──────────────────────

/// Proves the fix didn't overcorrect: an intentional `0` — via literal SET,
/// via `$param` SET, and via CREATE — must still round-trip as `Int64(0)`,
/// including after a checkpoint + reopen. Expected values derived by hand:
/// this test creates the fixture and asserts on it, it does not assert
/// whatever `db.execute` currently happens to return.
#[test]
fn genuine_zero_round_trips_across_all_paths_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        // CREATE with a literal 0.
        db.execute("CREATE (:Item {id: 'lit', v: 0})").unwrap();
        // CREATE with a $param 0.
        db.execute_with_params(
            "CREATE (:Item {id: 'param', v: $v})",
            params(&[("v", Value::Int64(0))]),
        )
        .unwrap();
        // SET (literal) an existing nonzero value down to 0.
        db.execute("CREATE (:Item {id: 'set-lit', v: 9})").unwrap();
        db.execute("MATCH (n:Item {id: 'set-lit'}) SET n.v = 0")
            .unwrap();
        // SET ($param) an existing nonzero value down to 0.
        db.execute("CREATE (:Item {id: 'set-param', v: 9})")
            .unwrap();
        db.execute_with_params(
            "MATCH (n:Item {id: 'set-param'}) SET n.v = $v",
            params(&[("v", Value::Int64(0))]),
        )
        .unwrap();

        for id in ["lit", "param", "set-lit", "set-param"] {
            assert_eq!(
                item_v(&db, id),
                Value::Int64(0),
                "id='{id}' must be a genuine stored 0 before reopen"
            );
        }
        db.checkpoint().expect("checkpoint");
    }

    {
        let db = GraphDb::open(&db_path).expect("open session 2 (reopen)");
        for id in ["lit", "param", "set-lit", "set-param"] {
            assert_eq!(
                item_v(&db, id),
                Value::Int64(0),
                "id='{id}' must still be a genuine stored 0 after reopen"
            );
        }
    }
}
