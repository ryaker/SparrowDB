//! SPA-235 / SPA-234: CREATE INDEX and UNIQUE constraint bodies.
//!
//! Verifies:
//! 1. `CREATE INDEX ON :Label(property)` builds the prop-index so subsequent
//!    equality lookups return the correct rows.
//! 2. `CREATE CONSTRAINT ON (n:Label) ASSERT n.property IS UNIQUE` allows the
//!    first insert but rejects a second insert with the same value.
//! 3. Constraint applies only to the constrained label — another label with the
//!    same property value is unaffected.
//! 4. Different property values on the same constrained label are each allowed.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── SPA-235 Test 1: CREATE INDEX builds index and supports equality lookup ────

#[test]
fn create_index_supports_equality_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Insert nodes before the index is created (index build scans existing data).
    db.execute("CREATE (:Product {sku: 'A001', price: 10})")
        .unwrap();
    db.execute("CREATE (:Product {sku: 'B002', price: 20})")
        .unwrap();

    // Build the index explicitly.
    db.execute("CREATE INDEX ON :Product(sku)").unwrap();

    // Equality lookup should find only the matching node.
    let result = db
        .execute("MATCH (n:Product {sku: 'A001'}) RETURN n.sku")
        .expect("lookup must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row for sku='A001', got {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("A001".into()),
        "returned sku must be A001"
    );
}

// ── SPA-235 Test 2: CREATE INDEX on non-existent label is a no-op ─────────────

#[test]
fn create_index_on_missing_label_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Label "Ghost" does not exist — should succeed without error.
    db.execute("CREATE INDEX ON :Ghost(name)")
        .expect("index on missing label must not error");
}

// ── SPA-234 Test 3: UNIQUE constraint allows first insert ─────────────────────

#[test]
fn unique_constraint_allows_first_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE")
        .unwrap();

    // First insert must succeed.
    db.execute("CREATE (:User {email: 'alice@example.com'})")
        .expect("first insert must succeed");
}

// ── SPA-234 Test 4: UNIQUE constraint rejects duplicate value ─────────────────

#[test]
fn unique_constraint_rejects_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE")
        .unwrap();

    db.execute("CREATE (:User {email: 'bob@example.com'})")
        .expect("first insert must succeed");

    let err = db
        .execute("CREATE (:User {email: 'bob@example.com'})")
        .expect_err("duplicate insert must fail");

    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("constraint") || msg.contains("violation"),
        "error message should mention constraint violation, got: {msg}"
    );
}

// ── SPA-234 Test 5: Constraint is label-scoped ────────────────────────────────

#[test]
fn unique_constraint_is_label_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE")
        .unwrap();

    db.execute("CREATE (:User {email: 'carol@example.com'})")
        .unwrap();

    // A different label with the same property value must be allowed.
    db.execute("CREATE (:Admin {email: 'carol@example.com'})")
        .expect("same value on a different label must not violate the constraint");
}

// ── SPA-234 Test 6: Different values on constrained label all pass ─────────────

#[test]
fn unique_constraint_allows_different_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE CONSTRAINT ON (n:Tag) ASSERT n.name IS UNIQUE")
        .unwrap();

    db.execute("CREATE (:Tag {name: 'rust'})").unwrap();
    db.execute("CREATE (:Tag {name: 'databases'})").unwrap();
    db.execute("CREATE (:Tag {name: 'graphs'})").unwrap();

    let result = db
        .execute("MATCH (t:Tag) RETURN t.name")
        .expect("query must succeed");

    assert_eq!(
        result.rows.len(),
        3,
        "all three distinct tags must be present, got {:?}",
        result.rows
    );
}
