//! Integration tests for SPA-130: WITH clause support.
//!
//! `WITH <expr> AS <alias> [WHERE <pred>]` acts as a pipeline boundary
//! between MATCH and RETURN, materialising intermediate rows and optionally
//! filtering them before the final projection.
//!
//! All tests use a real tempdir-backed Engine so they exercise the full
//! parse → bind → execute path against an actual on-disk database.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;

// ── Test fixture ──────────────────────────────────────────────────────────────

/// Build a fresh engine with three Person nodes: Alice (age 30), Bob (age 25),
/// Carol (age 35).
fn setup_people(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("node store");
    let cat = Catalog::open(dir).expect("catalog");
    let csr = CsrForward::build(0, &[]);
    let mut engine = Engine::new(store, cat, csr, dir);

    engine
        .execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .expect("CREATE Bob");
    engine
        .execute("CREATE (n:Person {name: 'Carol', age: 35})")
        .expect("CREATE Carol");

    engine
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Basic: MATCH → WITH (rename) → RETURN.
///
/// `MATCH (n:Person) WITH n.name AS name RETURN name` must return all three
/// person names.
#[test]
fn spa130_with_basic_projection_rename() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_people(dir.path());

    let result = engine
        .execute("MATCH (n:Person) WITH n.name AS name RETURN name")
        .expect("MATCH … WITH … RETURN must succeed");

    assert_eq!(result.columns, vec!["name"]);
    assert_eq!(
        result.rows.len(),
        3,
        "expected 3 rows (Alice, Bob, Carol), got {}",
        result.rows.len()
    );
}

/// WITH + WHERE string filter: only Alice survives.
///
/// `MATCH (n:Person) WITH n.name AS name WHERE name = 'Alice' RETURN name`
/// must return exactly one row.
#[test]
fn spa130_with_where_string_filter() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_people(dir.path());

    let result = engine
        .execute(
            "MATCH (n:Person) WITH n.name AS name WHERE name = 'Alice' RETURN name",
        )
        .expect("MATCH … WITH … WHERE … RETURN must succeed");

    assert_eq!(result.columns, vec!["name"]);
    assert_eq!(
        result.rows.len(),
        1,
        "WHERE name = 'Alice' must keep exactly 1 row, got {}",
        result.rows.len()
    );

    // Verify the surviving row contains the Alice string (stored as raw u64
    // little-endian bytes in the current storage encoding).
    use sparrowdb_execution::types::Value;
    let alice_raw: u64 = {
        let b = b"Alice";
        let mut arr = [0u8; 8];
        arr[..b.len()].copy_from_slice(b);
        u64::from_le_bytes(arr)
    };
    assert_eq!(
        result.rows[0][0],
        Value::Int64(alice_raw as i64),
        "surviving row must encode 'Alice' as raw u64"
    );
}

/// WITH + WHERE numeric (greater-than) predicate.
///
/// `MATCH (n:Person) WITH n.age AS age WHERE age > 25 RETURN age` must
/// return Alice (30) and Carol (35) but not Bob (25).
#[test]
fn spa130_with_where_numeric_gt() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_people(dir.path());

    let result = engine
        .execute("MATCH (n:Person) WITH n.age AS age WHERE age > 25 RETURN age")
        .expect("MATCH … WITH … WHERE age > 25 … RETURN must succeed");

    assert_eq!(result.columns, vec!["age"]);
    assert_eq!(
        result.rows.len(),
        2,
        "WHERE age > 25 must keep 2 rows (Alice=30, Carol=35), got {}",
        result.rows.len()
    );

    // All surviving ages must be > 25.
    use sparrowdb_execution::types::Value;
    for row in &result.rows {
        match &row[0] {
            Value::Int64(age) => assert!(*age > 25, "age {age} is not > 25"),
            other => panic!("expected Int64 for age, got {:?}", other),
        }
    }
}

/// WITH without WHERE must pass all rows through (no filtering).
///
/// `MATCH (n:Person) WITH n.age AS age RETURN age` must return all 3 rows.
#[test]
fn spa130_with_no_where_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_people(dir.path());

    let result = engine
        .execute("MATCH (n:Person) WITH n.age AS age RETURN age")
        .expect("MATCH … WITH … RETURN must succeed without WHERE");

    assert_eq!(result.columns, vec!["age"]);
    assert_eq!(
        result.rows.len(),
        3,
        "all 3 persons must be returned when no WHERE predicate, got {}",
        result.rows.len()
    );
}

/// WITH on an empty label returns no rows and does not error.
#[test]
fn spa130_with_on_empty_result_set() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_people(dir.path());

    // Filter to a name that doesn't exist → intermediate result is empty.
    let result = engine
        .execute(
            "MATCH (n:Person) WITH n.name AS name WHERE name = 'Nobody' RETURN name",
        )
        .expect("empty result WITH must not error");

    assert_eq!(result.rows.len(), 0, "expected 0 rows for 'Nobody'");
}

/// WITH WHERE = numeric equality.
///
/// `MATCH (n:Person) WITH n.age AS age WHERE age = 30 RETURN age` must
/// return only Alice's row.
#[test]
fn spa130_with_where_numeric_eq() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_people(dir.path());

    let result = engine
        .execute("MATCH (n:Person) WITH n.age AS age WHERE age = 30 RETURN age")
        .expect("MATCH … WITH … WHERE age = 30 … RETURN must succeed");

    assert_eq!(result.columns, vec!["age"]);
    assert_eq!(
        result.rows.len(),
        1,
        "WHERE age = 30 must keep exactly 1 row (Alice), got {}",
        result.rows.len()
    );

    use sparrowdb_execution::types::Value;
    assert_eq!(result.rows[0][0], Value::Int64(30));
}
