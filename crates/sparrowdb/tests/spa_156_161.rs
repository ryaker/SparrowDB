//! Integration tests for SPA-156 and SPA-161.
//!
//! SPA-156: `CREATE` on a fresh DB must succeed — labels are auto-registered
//!          rather than rejected as "unknown".
//!
//! SPA-161: `WHERE` string-literal equality in property filters must correctly
//!          compare against stored values rather than always returning false.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;

/// Build a fresh engine backed by a temp directory (no pre-populated data).
fn fresh_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("node store");
    let cat = Catalog::open(dir).expect("catalog");
    let csr = CsrForward::build(0, &[]);
    Engine::new(store, cat, csr, dir)
}

// ── SPA-156: CREATE on fresh DB ───────────────────────────────────────────────

/// SPA-156: `CREATE (:Person {name: 'Alice'})` on a fresh DB must not error.
#[test]
fn spa156_create_on_fresh_db_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE on fresh DB must succeed");
}

/// SPA-156: After CREATE, MATCH returns the newly created node.
///
/// This verifies the full round-trip: label auto-registered, node persisted,
/// MATCH scan returns it.
#[test]
fn spa156_create_then_match_returns_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE must succeed");

    // Re-open the engine from the same directory so we read from disk,
    // confirming the node was actually persisted.
    let mut engine2 = fresh_engine(dir.path());
    let result = engine2
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH after CREATE must succeed");

    assert_eq!(result.rows.len(), 1, "expected exactly 1 Person node");
}

/// SPA-156: Multiple CREATEs on a fresh DB all succeed, and MATCH returns them all.
#[test]
fn spa156_multiple_creates_all_visible() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    engine
        .execute("CREATE (n:Person {name: 'Carol'})")
        .expect("CREATE Carol");

    let result = engine
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH");
    assert_eq!(result.rows.len(), 3, "expected 3 Person nodes");
}

// ── SPA-161: WHERE string literal equality ────────────────────────────────────

/// SPA-161: `WHERE n.name = 'Alice'` must match exactly the node whose `name`
/// property was stored as `'Alice'`, and exclude others.
#[test]
fn spa161_where_string_filter_returns_correct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Populate three Person nodes with distinct names.
    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    engine
        .execute("CREATE (n:Person {name: 'Carol'})")
        .expect("CREATE Carol");

    // Filter to only Alice.
    let result = engine
        .execute("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name")
        .expect("MATCH WHERE");

    assert_eq!(
        result.rows.len(),
        1,
        "WHERE n.name = 'Alice' must return exactly 1 row"
    );
}

/// SPA-161: A WHERE predicate that matches none of the nodes returns zero rows.
#[test]
fn spa161_where_string_no_match_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.name = 'Ghost' RETURN n.name")
        .expect("MATCH WHERE no match");

    assert_eq!(
        result.rows.len(),
        0,
        "WHERE n.name = 'Ghost' must return 0 rows when no Ghost exists"
    );
}

/// SPA-161: Inline property filter `{name: 'Alice'}` also works (same code path
/// as matches_prop_filter, not eval_where).
#[test]
fn spa161_inline_prop_filter_string_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = engine
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("MATCH with inline prop filter");

    assert_eq!(
        result.rows.len(),
        1,
        "Inline prop filter {{name: 'Alice'}} must return 1 row"
    );
}
