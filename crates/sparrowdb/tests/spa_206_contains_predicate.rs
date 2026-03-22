//! End-to-end tests for CONTAINS string predicate in Cypher WHERE clauses.
//!
//! `WHERE n.prop CONTAINS 'substring'` — case-sensitive substring test.
//!
//! ## Storage constraint (v1)
//!
//! The v1 inline encoding stores at most 7 bytes of string data per property
//! slot.  Strings longer than 7 characters are silently truncated.  All test
//! strings are kept within 7 bytes so that the storage layer round-trips them
//! faithfully.  Full overflow-string support is tracked separately.
//!
//! Covers: basic match, no-match, missing property (null → no match),
//! non-string property (no error), case-sensitivity, and exact-match.

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

// ── Basic CONTAINS match ──────────────────────────────────────────────────────

/// `WHERE n.body CONTAINS 'hi'` — returns nodes whose body contains the substring.
/// String values are kept ≤ 7 bytes to stay within the v1 inline limit.
#[test]
fn contains_basic_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // "hi mom" = 6 bytes  ✓
    engine
        .execute("CREATE (n:Doc {title: 'A', body: 'hi mom'})")
        .expect("CREATE A");
    // "say hi" = 6 bytes  ✓
    engine
        .execute("CREATE (n:Doc {title: 'B', body: 'say hi'})")
        .expect("CREATE B");
    // "bye" = 3 bytes, no 'hi'
    engine
        .execute("CREATE (n:Doc {title: 'C', body: 'bye'})")
        .expect("CREATE C");

    let result = engine
        .execute("MATCH (n:Doc) WHERE n.body CONTAINS 'hi' RETURN n.title")
        .expect("MATCH WHERE CONTAINS");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows — A and B contain 'hi'"
    );
    let titles: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(
        titles.contains(&&Value::String("A".to_string())),
        "node A should be in results"
    );
    assert!(
        titles.contains(&&Value::String("B".to_string())),
        "node B should be in results"
    );
}

// ── CONTAINS: no match ───────────────────────────────────────────────────────

/// `WHERE n.body CONTAINS 'zzz'` — returns empty when no node matches.
#[test]
fn contains_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Doc {title: 'A', body: 'hello'})")
        .expect("CREATE A");
    engine
        .execute("CREATE (n:Doc {title: 'B', body: 'world'})")
        .expect("CREATE B");

    let result = engine
        .execute("MATCH (n:Doc) WHERE n.body CONTAINS 'zzz' RETURN n.title")
        .expect("MATCH WHERE CONTAINS no match — must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows — no body contains 'zzz'"
    );
}

// ── CONTAINS: missing property → no match (not error) ────────────────────────

/// `WHERE n.body CONTAINS 'hi'` — nodes without the `body` property are silently excluded.
#[test]
fn contains_missing_property_no_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // This node has no 'body' property — must be silently excluded.
    engine
        .execute("CREATE (n:Doc {title: 'X'})")
        .expect("CREATE X — no body");
    engine
        .execute("CREATE (n:Doc {title: 'Y', body: 'hi'})")
        .expect("CREATE Y — has body");

    let result = engine
        .execute("MATCH (n:Doc) WHERE n.body CONTAINS 'hi' RETURN n.title")
        .expect("MATCH WHERE CONTAINS missing prop — must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row — only Y matches; missing property must not error"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Y".to_string()),
        "result must be node Y"
    );
}

// ── CONTAINS: case-sensitive ──────────────────────────────────────────────────

/// CONTAINS is case-sensitive — `'Hi'` does not match `'hi mom'`.
#[test]
fn contains_case_sensitive() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Doc {title: 'A', body: 'hi mom'})")
        .expect("CREATE A");

    let result = engine
        .execute("MATCH (n:Doc) WHERE n.body CONTAINS 'Hi' RETURN n.title")
        .expect("MATCH WHERE CONTAINS case sensitive — must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "CONTAINS is case-sensitive: 'Hi' must not match 'hi mom'"
    );
}

// ── CONTAINS: full string equals substring ───────────────────────────────────

/// When the substring equals the full string value, it still matches.
#[test]
fn contains_full_string_is_substring() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // "hi" = 2 bytes
    engine
        .execute("CREATE (n:Doc {title: 'A', body: 'hi'})")
        .expect("CREATE A");

    let result = engine
        .execute("MATCH (n:Doc) WHERE n.body CONTAINS 'hi' RETURN n.title")
        .expect("MATCH WHERE CONTAINS exact");

    assert_eq!(
        result.rows.len(),
        1,
        "full string 'hi' contains 'hi' — must match"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("A".to_string()),
        "result must be node A"
    );
}

// ── CONTAINS: non-string property → no match, no error ───────────────────────

/// `WHERE n.count CONTAINS 'x'` on an integer property returns no match without error.
#[test]
fn contains_non_string_property_no_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Doc {title: 'A', qty: 42})")
        .expect("CREATE A");

    let result = engine
        .execute("MATCH (n:Doc) WHERE n.qty CONTAINS 'x' RETURN n.title")
        .expect("MATCH WHERE CONTAINS non-string — must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "CONTAINS on non-string value must return no match, not error"
    );
}
