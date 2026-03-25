//! Regression tests for SPA-207: integer 0 silently treated as NULL.
//!
//! Previously `read_col_slot_nullable` used `raw == 0` as a NULL sentinel.
//! Any node property storing the integer value 0 was returned as NULL, which
//! is silent data corruption.
//!
//! Fix: per-column null-bitmap sidecar files (`col_{id}_null.bin`) explicitly
//! track which slots were written.  Backward compat: slots without a bitmap
//! are treated with the legacy zero-sentinel logic.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_execution::types::Value;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;

/// Build a fresh engine backed by a temp directory.
fn fresh_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("node store");
    let cat = Catalog::open(dir).expect("catalog");
    let csr = CsrForward::build(0, &[]);
    Engine::with_single_csr(store, cat, csr, dir)
}

// ── Test 1: integer 0 property roundtrips correctly ───────────────────────────

/// A node created with `score: 0` must return 0, not NULL.
#[test]
fn integer_zero_property_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Item {score: 0})")
        .expect("CREATE Item with score=0");

    // The property must NOT be treated as NULL.
    let not_null = engine
        .execute("MATCH (n:Item) WHERE n.score IS NOT NULL RETURN n.score")
        .expect("IS NOT NULL query");

    assert_eq!(
        not_null.rows.len(),
        1,
        "score=0 must not be treated as NULL; got rows: {:?}",
        not_null.rows
    );
    assert_eq!(
        not_null.rows[0][0],
        Value::Int64(0),
        "score must round-trip as 0"
    );

    // Conversely, IS NULL must return nothing.
    let is_null = engine
        .execute("MATCH (n:Item) WHERE n.score IS NULL RETURN n.score")
        .expect("IS NULL query");

    assert_eq!(
        is_null.rows.len(),
        0,
        "score=0 must NOT satisfy IS NULL; got rows: {:?}",
        is_null.rows
    );
}

// ── Test 2: unset property returns NULL ───────────────────────────────────────

/// A node created without a given property must return NULL for that property.
#[test]
fn null_property_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Create a node with name only — no score property.
    engine
        .execute("CREATE (n:Item {name: 'Alice'})")
        .expect("CREATE Item without score");

    let result = engine
        .execute("MATCH (n:Item) WHERE n.score IS NULL RETURN n.name")
        .expect("IS NULL on missing prop");

    assert_eq!(
        result.rows.len(),
        1,
        "node without score must satisfy IS NULL; got rows: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "expected Alice"
    );
}

// ── Test 3: nonzero property roundtrip ────────────────────────────────────────

/// A node with `score: 42` must return 42 (sanity check).
#[test]
fn nonzero_property_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Item {score: 42})")
        .expect("CREATE Item with score=42");

    let result = engine
        .execute("MATCH (n:Item) WHERE n.score IS NOT NULL RETURN n.score")
        .expect("IS NOT NULL for score=42");

    assert_eq!(result.rows.len(), 1, "score=42 must be present");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(42),
        "score must round-trip as 42"
    );
}

// ── Test 4: mix of 0 and non-zero values ──────────────────────────────────────

/// Multiple nodes with a mix of 0 and non-zero scores must all return non-NULL.
#[test]
fn multiple_nodes_zero_and_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Widget {name: 'A', score: 0})")
        .expect("CREATE Widget A score=0");
    engine
        .execute("CREATE (n:Widget {name: 'B', score: 99})")
        .expect("CREATE Widget B score=99");
    engine
        .execute("CREATE (n:Widget {name: 'C', score: 0})")
        .expect("CREATE Widget C score=0");

    // All 3 nodes have a score — none should satisfy IS NULL.
    let is_null = engine
        .execute("MATCH (n:Widget) WHERE n.score IS NULL RETURN n.name")
        .expect("IS NULL for widgets");

    assert_eq!(
        is_null.rows.len(),
        0,
        "no Widget should have a NULL score; got: {:?}",
        is_null.rows
    );

    // All 3 should satisfy IS NOT NULL.
    let not_null = engine
        .execute("MATCH (n:Widget) WHERE n.score IS NOT NULL RETURN n.name ORDER BY n.name")
        .expect("IS NOT NULL for widgets");

    assert_eq!(
        not_null.rows.len(),
        3,
        "all 3 Widgets must have non-NULL score; got: {:?}",
        not_null.rows
    );
}

// ── Test 5: zero vs missing — IS NULL only matches missing ────────────────────

/// A node with `score: 0` and a node without `score` must be distinguished.
/// IS NULL must match only the node without the property.
#[test]
fn zero_score_vs_missing_score() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Alice has score=0, Bob has no score.
    engine
        .execute("CREATE (n:Player {name: 'Alice', score: 0})")
        .expect("CREATE Alice score=0");
    engine
        .execute("CREATE (n:Player {name: 'Bob'})")
        .expect("CREATE Bob no score");

    // IS NULL must return only Bob.
    let null_result = engine
        .execute("MATCH (n:Player) WHERE n.score IS NULL RETURN n.name")
        .expect("IS NULL for Player");

    assert_eq!(
        null_result.rows.len(),
        1,
        "IS NULL should match only Bob (no score); got: {:?}",
        null_result.rows
    );
    assert_eq!(
        null_result.rows[0][0],
        Value::String("Bob".to_string()),
        "IS NULL should return Bob"
    );

    // IS NOT NULL must return only Alice.
    let not_null_result = engine
        .execute("MATCH (n:Player) WHERE n.score IS NOT NULL RETURN n.name")
        .expect("IS NOT NULL for Player");

    assert_eq!(
        not_null_result.rows.len(),
        1,
        "IS NOT NULL should match only Alice (score=0); got: {:?}",
        not_null_result.rows
    );
    assert_eq!(
        not_null_result.rows[0][0],
        Value::String("Alice".to_string()),
        "IS NOT NULL should return Alice"
    );
}
