//! Regression tests for SPA-187: column slot misalignment when nodes have
//! different property sets.
//!
//! When nodes of the same label are created with heterogeneous property sets,
//! every known column file must be zero-padded for nodes that do not supply a
//! value.  Without padding, a later write would land at the wrong offset,
//! causing slot N to return data from a different node.
//!
//! The fix (in `NodeStore::create_node`) scans the on-disk column files for
//! the label and zero-pads any column not present in the supplied props.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Core regression: mixed property sets stay slot-aligned ───────────────────

/// Create three Person nodes with overlapping but non-identical properties:
///
/// ```cypher
/// CREATE (:Person {name: 'Alice'})           // slot 0: has name, no age
/// CREATE (:Person {age: 30})                 // slot 1: has age, no name
/// CREATE (:Person {name: 'Charlie', age: 25}) // slot 2: has both
/// ```
///
/// Before the fix, slot 1 of the `name` column would return Alice's name
/// (because the name column only had 8 bytes when slot 1 was inserted, making
/// the insertion land at offset 0 = slot 0 again — corrupting the data).
///
/// After the fix every column file has exactly `node_count * 8` bytes, so
/// slot N always refers to node N.
#[test]
fn mixed_props_return_correct_values_per_slot() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {age: 30})")
        .expect("CREATE age-only node");
    db.execute("CREATE (:Person {name: 'Charlie', age: 25})")
        .expect("CREATE Charlie");

    let result = db
        .execute("MATCH (p:Person) RETURN p.name, p.age")
        .expect("MATCH must succeed");

    assert_eq!(result.rows.len(), 3, "expected 3 Person nodes");

    // Collect results into a sorted set for order-independent comparison.
    let mut rows: Vec<(Value, Value)> = result
        .rows
        .into_iter()
        .map(|mut r| {
            assert_eq!(r.len(), 2, "each row must have 2 columns");
            let age = r.pop().unwrap();
            let name = r.pop().unwrap();
            (name, age)
        })
        .collect();

    // Sort by slot order: Alice (name present), age-only (name NULL), Charlie.
    // We identify rows by their non-null field.
    rows.sort_by_key(|(name, age)| match (name, age) {
        (Value::String(n), _) if n == "Alice" => 0,
        (Value::Null, Value::Int64(30)) => 1,
        (Value::String(n), _) if n == "Charlie" => 2,
        _ => 3,
    });

    // Alice: name='Alice', age=NULL
    assert_eq!(
        rows[0].0,
        Value::String("Alice".to_string()),
        "Alice: name must be 'Alice'"
    );
    assert_eq!(rows[0].1, Value::Null, "Alice: age must be NULL");

    // Node 2 (age-only): name=NULL, age=30
    assert_eq!(rows[1].0, Value::Null, "age-only node: name must be NULL");
    assert_eq!(
        rows[1].1,
        Value::Int64(30),
        "age-only node: age must be 30"
    );

    // Charlie: name='Charlie', age=25
    assert_eq!(
        rows[2].0,
        Value::String("Charlie".to_string()),
        "Charlie: name must be 'Charlie'"
    );
    assert_eq!(rows[2].1, Value::Int64(25), "Charlie: age must be 25");
}

// ── IS NULL correctly identifies missing properties after slot alignment ──────

/// `WHERE p.name IS NULL` must return only the age-only node after alignment.
#[test]
fn is_null_after_mixed_create() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person2 {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person2 {age: 30})").unwrap();
    db.execute("CREATE (:Person2 {name: 'Charlie', age: 25})")
        .unwrap();

    let result = db
        .execute("MATCH (p:Person2) WHERE p.name IS NULL RETURN p.age")
        .expect("IS NULL query must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "IS NULL on name should return exactly one row (the age-only node)"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(30),
        "the age-only node must have age=30"
    );
}

/// `WHERE p.age IS NULL` must return only Alice after alignment.
#[test]
fn is_null_age_returns_name_only_node() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person3 {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person3 {age: 30})").unwrap();
    db.execute("CREATE (:Person3 {name: 'Charlie', age: 25})")
        .unwrap();

    let result = db
        .execute("MATCH (p:Person3) WHERE p.age IS NULL RETURN p.name")
        .expect("IS NULL on age must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "IS NULL on age should return exactly one row (Alice)"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "must return Alice (the name-only node)"
    );
}

// ── Slot count matches node count ─────────────────────────────────────────────

/// After creating N nodes with varying properties, COUNT(*) must return N.
/// This verifies the HWM is not corrupted by the zero-padding writes.
#[test]
fn count_star_after_mixed_create() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Item {sku: 1})").unwrap();
    db.execute("CREATE (:Item {price: 9})").unwrap();
    db.execute("CREATE (:Item {sku: 2, price: 5})").unwrap();
    db.execute("CREATE (:Item {name: 'widget'})").unwrap();

    let result = db
        .execute("MATCH (i:Item) RETURN COUNT(*)")
        .expect("COUNT(*) must succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::Int64(4),
        "COUNT(*) must return 4 — one per created node"
    );
}
