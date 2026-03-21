//! End-to-end tests for IN list operator support in Cypher WHERE clauses.
//!
//! `WHERE x IN ['a', 'b', 'c']` — membership test against a literal list.
//!
//! Covers: string matching, integer matching, single-element list, empty list,
//! and no-match cases.

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
    Engine::new(store, cat, csr, dir)
}

// ── IN operator: string matching ─────────────────────────────────────────────

/// `WHERE n.name IN ['Alice', 'Bob']` — returns nodes whose name is in the list.
#[test]
fn in_operator_string_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    engine
        .execute("CREATE (n:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] RETURN n.name")
        .expect("MATCH WHERE IN");

    assert_eq!(result.rows.len(), 2, "expected 2 rows (Alice and Bob)");
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(
        names.contains(&&Value::String("Alice".to_string())),
        "Alice should be in results"
    );
    assert!(
        names.contains(&&Value::String("Bob".to_string())),
        "Bob should be in results"
    );
}

// ── IN operator: no match ────────────────────────────────────────────────────

/// `WHERE n.name IN ['Ghost', 'Nobody']` — returns empty when no node matches.
#[test]
fn in_operator_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.name IN ['Ghost', 'Nobody'] RETURN n.name")
        .expect("MATCH WHERE IN no match");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows — no node with those names"
    );
}

// ── IN operator: integer values ───────────────────────────────────────────────

/// `WHERE n.age IN [25, 30, 35]` — membership test with integers.
#[test]
fn in_operator_integer() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {age: 25})")
        .expect("CREATE age 25");
    engine
        .execute("CREATE (n:Person {age: 30})")
        .expect("CREATE age 30");
    engine
        .execute("CREATE (n:Person {age: 40})")
        .expect("CREATE age 40");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n.age")
        .expect("MATCH WHERE IN integers");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows (age 25 and age 30 match)"
    );
    let ages: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(
        ages.contains(&&Value::Int64(25)),
        "age 25 should be in results"
    );
    assert!(
        ages.contains(&&Value::Int64(30)),
        "age 30 should be in results"
    );
}

// ── IN operator: single-element list ─────────────────────────────────────────

/// `WHERE n.name IN ['Alice']` — single-element list works correctly.
#[test]
fn in_operator_single_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.name IN ['Alice'] RETURN n.name")
        .expect("MATCH WHERE IN single");

    assert_eq!(result.rows.len(), 1, "expected 1 row (Alice only)");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "result must be Alice"
    );
}

// ── IN operator: empty list ───────────────────────────────────────────────────

/// `WHERE n.name IN []` — empty list returns nothing, no error.
#[test]
fn in_operator_empty_list() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.name IN [] RETURN n.name")
        .expect("MATCH WHERE IN empty list — must not error");

    assert_eq!(result.rows.len(), 0, "empty IN list should match nothing");
}
