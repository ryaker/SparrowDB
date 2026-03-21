//! End-to-end tests for IS NULL / IS NOT NULL predicates in Cypher WHERE clauses.
//!
//! Tests cover:
//!  1. `WHERE n.email IS NULL` — returns nodes that are missing the property
//!  2. `WHERE n.email IS NOT NULL` — returns nodes that have the property
//!  3. Mixed nodes: IS NULL filters correctly
//!  4. IS NOT NULL after OPTIONAL MATCH — filters out NULL rows

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_execution::types::Value;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;

/// Build a fresh engine backed by a temp directory (no pre-populated data).
fn fresh_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("node store");
    let cat = Catalog::open(dir).expect("catalog");
    let csr = CsrForward::build(0, &[]);
    Engine::new(store, cat, csr, dir)
}

// ── Test 1: IS NULL — node created without the property is returned ───────────

/// A node created without `email` should have that property as NULL.
/// `WHERE n.email IS NULL` must return it.
#[test]
fn is_null_missing_prop() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Create a node with no email property.
    engine
        .execute("CREATE (n:User {name: 'Alice'})")
        .expect("CREATE User without email");

    let result = engine
        .execute("MATCH (n:User) WHERE n.email IS NULL RETURN n.name")
        .expect("MATCH WHERE IS NULL");

    assert_eq!(result.rows.len(), 1, "expected 1 row — Alice has no email");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "expected Alice in result"
    );
}

// ── Test 2: IS NOT NULL — node with the property is returned ─────────────────

/// A node created with `email: 'a@b.com'` should satisfy `IS NOT NULL`.
#[test]
fn is_not_null_present_prop() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:User {name: 'Bob', email: 'bob@example.com'})")
        .expect("CREATE User with email");

    let result = engine
        .execute("MATCH (n:User) WHERE n.email IS NOT NULL RETURN n.name")
        .expect("MATCH WHERE IS NOT NULL");

    assert_eq!(result.rows.len(), 1, "expected 1 row — Bob has email");
    assert_eq!(
        result.rows[0][0],
        Value::String("Bob".to_string()),
        "expected Bob in result"
    );
}

// ── Test 3: Mixed nodes — IS NULL filters correctly ───────────────────────────

/// Create two nodes: one with email, one without.
/// `WHERE n.email IS NULL` must return only the one without email.
/// `WHERE n.email IS NOT NULL` must return only the one with email.
#[test]
fn is_null_filters_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Contact {name: 'Alice'})")
        .expect("CREATE Alice (no email)");
    engine
        .execute("CREATE (n:Contact {name: 'Bob', email: 'bob@example.com'})")
        .expect("CREATE Bob (with email)");

    // IS NULL should return only Alice.
    let null_result = engine
        .execute("MATCH (n:Contact) WHERE n.email IS NULL RETURN n.name")
        .expect("MATCH WHERE email IS NULL");

    assert_eq!(
        null_result.rows.len(),
        1,
        "IS NULL: expected 1 row (Alice), got {:?}",
        null_result.rows
    );
    assert_eq!(
        null_result.rows[0][0],
        Value::String("Alice".to_string()),
        "IS NULL result should be Alice"
    );

    // IS NOT NULL should return only Bob.
    let not_null_result = engine
        .execute("MATCH (n:Contact) WHERE n.email IS NOT NULL RETURN n.name")
        .expect("MATCH WHERE email IS NOT NULL");

    assert_eq!(
        not_null_result.rows.len(),
        1,
        "IS NOT NULL: expected 1 row (Bob), got {:?}",
        not_null_result.rows
    );
    assert_eq!(
        not_null_result.rows[0][0],
        Value::String("Bob".to_string()),
        "IS NOT NULL result should be Bob"
    );
}

// ── Test 4: IS NOT NULL on data from OPTIONAL MATCH ──────────────────────────

/// When nodes exist with a nullable property, IS NOT NULL correctly filters
/// them in a MATCH query.  This verifies IS NOT NULL in a realistic scenario
/// where some nodes have the property and others don't.
#[test]
fn is_not_null_after_optional_match_filters_null_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Create two nodes: one with phone (present), one without.
    engine
        .execute("CREATE (n:Contact2 {name: 'Alice'})")
        .expect("CREATE Alice (no phone)");
    engine
        .execute("CREATE (n:Contact2 {name: 'Bob', phone: '555-1234'})")
        .expect("CREATE Bob (with phone)");

    // IS NOT NULL should return only Bob (has phone).
    let result = engine
        .execute("MATCH (n:Contact2) WHERE n.phone IS NOT NULL RETURN n.name")
        .expect("MATCH WHERE phone IS NOT NULL must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "IS NOT NULL should return only Bob (has phone), got {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Bob".to_string()),
        "IS NOT NULL result should be Bob"
    );

    // IS NULL should return only Alice (no phone).
    let null_result = engine
        .execute("MATCH (n:Contact2) WHERE n.phone IS NULL RETURN n.name")
        .expect("MATCH WHERE phone IS NULL must not error");

    assert_eq!(
        null_result.rows.len(),
        1,
        "IS NULL should return only Alice (no phone), got {:?}",
        null_result.rows
    );
    assert_eq!(
        null_result.rows[0][0],
        Value::String("Alice".to_string()),
        "IS NULL result should be Alice"
    );
}

// ── Test 5: IS NULL works in AND expression ───────────────────────────────────

/// `WHERE n.email IS NULL AND n.name = 'Alice'` — compound predicate.
#[test]
fn is_null_in_and_expression() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Member {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Member {name: 'Bob', email: 'bob@example.com'})")
        .expect("CREATE Bob");

    let result = engine
        .execute("MATCH (n:Member) WHERE n.email IS NULL AND n.name = 'Alice' RETURN n.name")
        .expect("compound IS NULL AND predicate");

    assert_eq!(result.rows.len(), 1, "expected Alice only");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "result should be Alice"
    );
}
