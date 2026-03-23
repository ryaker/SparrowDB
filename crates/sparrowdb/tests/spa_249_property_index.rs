//! SPA-249: In-memory B-tree property index for O(log n) equality lookups.
//!
//! These tests verify correctness of the index — that equality lookup via
//! `MATCH (n:Label {prop: 'value'})` returns the same rows as a full scan,
//! and that the index stays consistent after CREATE.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── Test 1: index lookup returns correct nodes ────────────────────────────────

#[test]
fn index_lookup_returns_correct_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Carol', age: 30})")
        .unwrap();

    // Equality lookup on 'name' — should find only Alice.
    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("index lookup must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row for name='Alice', got {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".into()),
        "returned name must be Alice"
    );
}

// ── Test 2: index consistent after CREATE ─────────────────────────────────────

#[test]
fn index_consistent_after_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:City {name: 'London'})").unwrap();
    db.execute("CREATE (:City {name: 'Paris'})").unwrap();

    // The index was built at open time; now verify lookup after inserts.
    let result = db
        .execute("MATCH (c:City {name: 'Paris'}) RETURN c.name")
        .expect("must find Paris");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row for Paris, got {:?}",
        result.rows
    );
}

// ── Test 3: multiple nodes with same prop value all returned ──────────────────

#[test]
fn multiple_nodes_with_same_value_all_returned() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Product {category: 'electronics'})")
        .unwrap();
    db.execute("CREATE (:Product {category: 'electronics'})")
        .unwrap();
    db.execute("CREATE (:Product {category: 'clothing'})")
        .unwrap();
    db.execute("CREATE (:Product {category: 'electronics'})")
        .unwrap();

    let result = db
        .execute("MATCH (p:Product {category: 'electronics'}) RETURN p.category")
        .expect("multi-match lookup must succeed");

    assert_eq!(
        result.rows.len(),
        3,
        "expected 3 electronics products, got {:?}",
        result.rows
    );
}

// ── Test 4: non-existent value returns empty ──────────────────────────────────

#[test]
fn nonexistent_value_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Animal {species: 'cat'})").unwrap();
    db.execute("CREATE (:Animal {species: 'dog'})").unwrap();

    let result = db
        .execute("MATCH (a:Animal {species: 'parrot'}) RETURN a.species")
        .expect("lookup of absent value must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows for non-existent species, got {:?}",
        result.rows
    );
}

// ── Test 5: integer equality lookup via index ─────────────────────────────────

#[test]
fn integer_equality_lookup_via_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:User {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (:User {id: 2, name: 'Bob'})").unwrap();
    db.execute("CREATE (:User {id: 3, name: 'Carol'})").unwrap();

    let result = db
        .execute("MATCH (u:User {id: 2}) RETURN u.name")
        .expect("integer index lookup must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 user with id=2, got {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Bob".into()),
        "returned name must be Bob"
    );
}

// ── Test 6: index doesn't break WHERE clause filtering ────────────────────────

#[test]
fn index_coexists_with_where_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .unwrap();

    // Inline filter narrowed by index + additional WHERE clause.
    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) WHERE n.age = 30 RETURN n.name")
        .expect("index + WHERE must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 Alice aged 30, got {:?}",
        result.rows
    );
}

// ── Test 7: RETURN count correct when using index ────────────────────────────

#[test]
fn index_lookup_count_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    for _ in 0..10 {
        db.execute("CREATE (:Item {type: 'widget'})").unwrap();
    }
    for _ in 0..5 {
        db.execute("CREATE (:Item {type: 'gadget'})").unwrap();
    }

    let result = db
        .execute("MATCH (i:Item {type: 'widget'}) RETURN count(i)")
        .expect("count via index must succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::Int64(10),
        "expected count of 10 widgets, got {:?}",
        result.rows[0][0]
    );
}
