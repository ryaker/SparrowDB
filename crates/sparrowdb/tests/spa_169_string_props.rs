//! End-to-end tests for SPA-169.
//!
//! Before this fix, `RETURN n.name` returned a raw `Int64` bit-pattern instead
//! of the actual string value.  The root cause was that `get_node_raw` returned
//! `u64` values and `build_row_vals`/`project_row` blindly reinterpreted them
//! as `Int64`, discarding the type information written by `literal_to_store_value`.
//!
//! The fix embeds a type tag in the top byte of every stored `u64`:
//!   - `0x00` → `Int64`  (7-byte signed payload)
//!   - `0x01` → `Bytes`  (up to 7 inline bytes of string data)
//!
//! `decode_raw_val` now reads the tag and returns `Value::String` or `Value::Int64`
//! accordingly, so `RETURN n.name` returns the correct string.

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

// ── SPA-169: string property round-trips ─────────────────────────────────────

/// CREATE a node with a string property, MATCH and RETURN it — must be a String,
/// not a garbage Int64.
#[test]
fn string_prop_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE");

    let result = engine
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH RETURN n.name");

    assert_eq!(result.rows.len(), 1, "expected exactly 1 Person node");
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "n.name must be String('Alice'), not a raw Int64 (SPA-169)"
    );
}

/// CREATE a node with an integer property, MATCH and RETURN it — must be Int64.
#[test]
fn int_prop_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {age: 30})")
        .expect("CREATE");

    let result = engine
        .execute("MATCH (n:Person) RETURN n.age")
        .expect("MATCH RETURN n.age");

    assert_eq!(result.rows.len(), 1, "expected exactly 1 Person node");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(30),
        "n.age must be Int64(30)"
    );
}

/// CREATE a node with both string and integer properties — both must round-trip
/// with the correct types.
#[test]
fn mixed_props_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .expect("CREATE");

    let name_result = engine
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("RETURN n.name");

    let age_result = engine
        .execute("MATCH (n:Person) RETURN n.age")
        .expect("RETURN n.age");

    assert_eq!(
        name_result.rows[0][0],
        Value::String("Bob".to_string()),
        "n.name must be String('Bob') (SPA-169)"
    );
    assert_eq!(
        age_result.rows[0][0],
        Value::Int64(25),
        "n.age must be Int64(25)"
    );
}

/// Multiple nodes with the same string property — all return correctly typed values.
#[test]
fn multiple_string_nodes_round_trip() {
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
        .expect("MATCH RETURN n.name");

    assert_eq!(result.rows.len(), 3, "expected 3 rows");

    // Every returned value must be a String, not Int64.
    for row in &result.rows {
        assert!(
            matches!(&row[0], Value::String(_)),
            "expected Value::String, got {:?} (SPA-169)",
            row[0]
        );
    }

    // The three names should all be present.
    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

// ── SPA-169: WHERE + RETURN end-to-end ───────────────────────────────────────

/// `WHERE n.name = 'Alice' RETURN n.name` must both filter correctly AND return
/// the name as a String.
#[test]
fn string_prop_where_and_return() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    engine
        .execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name")
        .expect("MATCH WHERE RETURN");

    assert_eq!(
        result.rows.len(),
        1,
        "WHERE n.name = 'Alice' must return exactly 1 row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "RETURN n.name must be String('Alice') after WHERE filter (SPA-169)"
    );
}

/// Confirm Int64 properties are unaffected — existing int storage must not regress.
#[test]
fn int_prop_where_and_return() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Person {age: 42})")
        .expect("CREATE age=42");
    engine
        .execute("CREATE (n:Person {age: 18})")
        .expect("CREATE age=18");

    let result = engine
        .execute("MATCH (n:Person) WHERE n.age = 42 RETURN n.age")
        .expect("MATCH WHERE age = 42 RETURN");

    assert_eq!(result.rows.len(), 1, "WHERE n.age = 42 must return 1 row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(42),
        "RETURN n.age must be Int64(42)"
    );
}

/// Verify string round-trip survives a DB reopen (persisted encoding is stable).
#[test]
fn string_prop_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: write.
    {
        let mut engine = fresh_engine(dir.path());
        engine
            .execute("CREATE (n:Person {name: 'Alice'})")
            .expect("CREATE");
    }

    // Session 2: read from disk.
    {
        let mut engine = fresh_engine(dir.path());
        let result = engine
            .execute("MATCH (n:Person) RETURN n.name")
            .expect("MATCH after reopen");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            Value::String("Alice".to_string()),
            "n.name must survive DB reopen as String('Alice') (SPA-169)"
        );
    }
}
