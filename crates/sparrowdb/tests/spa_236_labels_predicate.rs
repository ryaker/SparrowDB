//! E2E tests for SPA-236 / SPA-238: labels() list predicates in WHERE clause.
//!
//! `ANY(label IN labels(n) WHERE label IN ['Foo', 'Bar'])` should filter
//! nodes whose label is in the given list.
//!
//! SPA-238: compound predicates mixing `n.prop IN list` with
//! `ANY(label IN labels(n) WHERE ...)` using OR.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: ANY(label IN labels(n) WHERE label = 'Person') ──────────────────

/// Basic labels() predicate in WHERE — filter by single label.
#[test]
fn any_label_equals_person() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Animal {name: 'Rex'})").unwrap();

    let result = db
        .execute("MATCH (n) WHERE ANY(label IN labels(n) WHERE label = 'Person') RETURN n.name")
        .expect("ANY label predicate must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "expected Alice; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 2: ANY(label IN labels(n) WHERE label IN ['Person', 'Animal']) ─────

/// labels() predicate with IN list — match multiple labels.
#[test]
fn any_label_in_list() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Animal {name: 'Rex'})").unwrap();
    db.execute("CREATE (n:Place {name: 'Berlin'})").unwrap();

    let result = db
        .execute(
            "MATCH (n) WHERE ANY(label IN labels(n) WHERE label IN ['Person', 'Animal']) RETURN n.name ORDER BY n.name",
        )
        .expect("ANY label IN list must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows; got: {:?}",
        result.rows
    );
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(
        names.contains(&&Value::String("Alice".to_string())),
        "expected Alice in results; got: {:?}",
        names
    );
    assert!(
        names.contains(&&Value::String("Rex".to_string())),
        "expected Rex in results; got: {:?}",
        names
    );
}

// ── Test 3: NONE(label IN labels(n) WHERE label = 'Person') ─────────────────

/// NONE predicate — exclude nodes with a specific label.
#[test]
fn none_label_equals_person() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Animal {name: 'Rex'})").unwrap();
    db.execute("CREATE (n:Place {name: 'Berlin'})").unwrap();

    let result = db
        .execute(
            "MATCH (n) WHERE NONE(label IN labels(n) WHERE label = 'Person') RETURN n.name ORDER BY n.name",
        )
        .expect("NONE label predicate must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows; got: {:?}",
        result.rows
    );
}

// ── Test 4 (SPA-238): compound predicate with OR ────────────────────────────

/// Mix property predicate with labels() predicate using OR.
#[test]
fn compound_or_with_labels_predicate() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', type: 'admin'})")
        .unwrap();
    db.execute("CREATE (n:Animal {name: 'Rex', type: 'pet'})")
        .unwrap();
    db.execute("CREATE (n:Place {name: 'Berlin', type: 'city'})")
        .unwrap();

    // Should match Alice (Person label) and Berlin (type = 'city')
    let result = db
        .execute(
            "MATCH (n) WHERE ANY(label IN labels(n) WHERE label = 'Person') OR n.type = 'city' RETURN n.name ORDER BY n.name",
        )
        .expect("compound OR predicate must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows; got: {:?}",
        result.rows
    );
    let names: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(
        names.contains(&&Value::String("Alice".to_string())),
        "expected Alice; got: {:?}",
        names
    );
    assert!(
        names.contains(&&Value::String("Berlin".to_string())),
        "expected Berlin; got: {:?}",
        names
    );
}

// ── Test 5 (SPA-238): compound predicate with AND ───────────────────────────

/// Mix property predicate with labels() predicate using AND.
#[test]
fn compound_and_with_labels_predicate() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (n:Animal {name: 'Rex', age: 5})")
        .unwrap();

    // Should match only Alice (Person AND age > 28)
    let result = db
        .execute(
            "MATCH (n) WHERE ANY(label IN labels(n) WHERE label = 'Person') AND n.age > 28 RETURN n.name",
        )
        .expect("compound AND predicate must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("Alice".to_string()),);
}
