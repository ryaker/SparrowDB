//! Regression test for #473 at the level it actually lives: `value_to_store_value`
//! (`crates/sparrowdb-execution/src/engine/mod.rs`), reached via
//! `Engine::execute_create` (`engine/mutation.rs`) when `Engine::execute_statement`
//! is called with a `Statement::Create` directly.
//!
//! `GraphDb::execute()` / `execute_with_params()` never reach this function for a
//! standalone `CREATE` — both intercept `Statement::Create` earlier and call
//! `execute_create_standalone`, which was already fixed by PR #492
//! (`resolve_create_prop_value`). `execute_batch_mutation`'s `Statement::Create`
//! arm (also `sparrowdb/src/db.rs`) is now fixed too (see
//! `regression_475_473_null_coercion.rs`) and never reaches this function either.
//! So #473's own text — "currently only reachable via execute_create's
//! empty-bindings path... exposure today is narrow" — is confirmed here: this
//! test exercises the Engine directly, the way `sparrowdb-execution`'s other
//! engine-level tests do (see `regression_421_varlen_plus_hop.rs`), because no
//! path through the public `GraphDb` API reaches it.
//!
//! Pre-fix: `value_to_store_value(val)` mapped `Value::Null` (and
//! `List`/`Map`/`Vector`/`NodeRef`/`EdgeRef`) to `StoreValue::Int64(0)`.
//! Post-fix: `value_to_store_value(key, val)` returns
//! `Err(InvalidArgument)` for all of those, naming the property key and the
//! offending kind.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::Engine;
use sparrowdb_storage::node_store::NodeStore;

fn build_engine() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let store = NodeStore::open(&path).expect("node store");
    let cat = Catalog::open(&path).expect("catalog");
    let engine = Engine::new(store, cat, std::collections::HashMap::new(), &path);
    (dir, engine)
}

/// A literal `null` property value reaching `Engine::execute_create` directly
/// (bypassing `GraphDb`, which never routes a standalone CREATE here) must be
/// rejected, not silently written as `StoreValue::Int64(0)`.
#[test]
fn engine_execute_create_null_property_rejected() {
    let (_dir, mut engine) = build_engine();

    let stmt = sparrowdb_cypher::parse("CREATE (:Item {v: null})").expect("parse");
    let err = engine
        .execute_statement(stmt)
        .expect_err("Engine::execute_create with a null property must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("null"), "error '{msg}' should mention null");
    assert!(
        msg.contains('v'),
        "error '{msg}' should name the offending property 'v'"
    );
}

/// Control: a genuine literal `0` through the same direct-Engine path must
/// still round-trip, proving the fix rejects only the unrepresentable
/// variants and not real zeros.
#[test]
fn engine_execute_create_genuine_zero_round_trips() {
    let (_dir, mut engine) = build_engine();

    let stmt = sparrowdb_cypher::parse("CREATE (:Item {v: 0})").expect("parse");
    engine
        .execute_statement(stmt)
        .expect("Engine::execute_create with a literal 0 must succeed");

    let stmt = sparrowdb_cypher::parse("MATCH (n:Item) RETURN n.v").expect("parse");
    let result = engine.execute_statement(stmt).expect("MATCH");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        sparrowdb_execution::types::Value::Int64(0)
    );
}
