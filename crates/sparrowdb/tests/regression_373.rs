//! Regression tests for issue #373: SET clause for property updates.
//!
//! Verifies that `MATCH (n:Label {filter}) SET n.prop = value` correctly
//! updates node properties, and that comma-separated multi-property SET
//! (`SET n.a = 1, n.b = 2`) is parsed and executed end-to-end.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Basic: SET updates an existing property ───────────────────────────────────

#[test]
fn set_updates_existing_property() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .expect("CREATE");

    db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 30")
        .expect("SET must succeed");

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.age")
        .expect("RETURN after SET");

    assert_eq!(result.columns, vec!["n.age"]);
    assert_eq!(result.rows.len(), 1, "must return exactly one row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(30),
        "age must be 30 after SET"
    );
}

// ── New property: SET adds a property that did not exist before ───────────────

#[test]
fn set_adds_new_property() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE");

    // city did not exist before SET
    db.execute("MATCH (n:Person {name: 'Bob'}) SET n.city = 'NYC'")
        .expect("SET new property must succeed");

    let result = db
        .execute("MATCH (n:Person {name: 'Bob'}) RETURN n.city")
        .expect("RETURN after SET new property");

    assert_eq!(result.columns, vec!["n.city"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("NYC".to_string()),
        "city must be 'NYC' after SET"
    );
}

// ── Multi-property: SET n.age = 30, n.city = "NYC" ───────────────────────────

#[test]
fn set_multiple_properties_comma_separated() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Carol', age: 20})")
        .expect("CREATE");

    db.execute("MATCH (n:Person {name: 'Carol'}) SET n.age = 35, n.city = 'LA'")
        .expect("multi-property SET must succeed");

    let age_result = db
        .execute("MATCH (n:Person {name: 'Carol'}) RETURN n.age")
        .expect("RETURN n.age");
    assert_eq!(age_result.rows.len(), 1);
    assert_eq!(
        age_result.rows[0][0],
        Value::Int64(35),
        "age must be 35 after multi SET"
    );

    let city_result = db
        .execute("MATCH (n:Person {name: 'Carol'}) RETURN n.city")
        .expect("RETURN n.city");
    assert_eq!(city_result.rows.len(), 1);
    assert_eq!(
        city_result.rows[0][0],
        Value::String("LA".to_string()),
        "city must be 'LA' after multi SET"
    );
}

// ── WHERE + SET: MATCH (n) WHERE n.name = 'Alice' SET n.age = 99 ─────────────

#[test]
fn set_with_where_clause() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {name: 'Dave', age: 40})")
        .expect("CREATE Dave");

    // Only Alice should be updated
    db.execute("MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 99")
        .expect("WHERE + SET must succeed");

    let alice_result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.age")
        .expect("RETURN Alice age");
    assert_eq!(alice_result.rows[0][0], Value::Int64(99), "Alice age must be 99");

    let dave_result = db
        .execute("MATCH (n:Person {name: 'Dave'}) RETURN n.age")
        .expect("RETURN Dave age");
    assert_eq!(dave_result.rows[0][0], Value::Int64(40), "Dave age must be unchanged");
}
