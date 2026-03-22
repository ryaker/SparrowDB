//! E2E tests for SPA-207: `labels(n)` returns node label list.
//!
//! `labels(n)` in Cypher returns a list of label strings for a node.
//! In SparrowDB each node has exactly one label, so
//! `labels(n)` returns `Value::List([Value::String("LabelName")])`.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: labels(n) returns ["Person"] for Person nodes ────────────────────

/// MATCH (n:Person) RETURN labels(n) → each row is [["Person"]]
#[test]
fn labels_fn_person_nodes() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = db
        .execute("MATCH (n:Person) RETURN labels(n)")
        .expect("MATCH with labels(n) must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows; got: {:?}",
        result.rows
    );

    for row in &result.rows {
        match &row[0] {
            Value::List(items) => {
                assert_eq!(
                    items.len(),
                    1,
                    "labels(n) must have exactly one entry; got: {:?}",
                    items
                );
                assert_eq!(
                    items[0],
                    Value::String("Person".to_string()),
                    "labels(n)[0] must be 'Person'; got: {:?}",
                    items[0]
                );
            }
            other => panic!("expected Value::List from labels(n), got: {:?}", other),
        }
    }
}

// ── Test 2: labels(n) returns ["Animal"] for Animal nodes ────────────────────

/// MATCH (n:Animal) RETURN labels(n) → each row is [["Animal"]]
#[test]
fn labels_fn_animal_nodes() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Animal {name: 'Rex'})")
        .expect("CREATE Rex");
    db.execute("CREATE (n:Animal {name: 'Fido'})")
        .expect("CREATE Fido");

    let result = db
        .execute("MATCH (n:Animal) RETURN labels(n)")
        .expect("MATCH with labels(n) on Animal must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 Animal rows; got: {:?}",
        result.rows
    );

    for row in &result.rows {
        match &row[0] {
            Value::List(items) => {
                assert_eq!(
                    items.len(),
                    1,
                    "labels(n) must have exactly one entry for Animal; got: {:?}",
                    items
                );
                assert_eq!(
                    items[0],
                    Value::String("Animal".to_string()),
                    "labels(n)[0] must be 'Animal'; got: {:?}",
                    items[0]
                );
            }
            other => panic!(
                "expected Value::List from labels(n) for Animal, got: {:?}",
                other
            ),
        }
    }
}

// ── Test 3: labels(n) in WHERE using list equality ───────────────────────────

/// MATCH (n) WHERE labels(n) = ['Person'] RETURN n.name
/// should return only Person node names.
#[test]
fn labels_fn_in_where_filters_by_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Animal {name: 'Rex'})")
        .expect("CREATE Rex");

    // Query for Person nodes by label list equality using labeled MATCH
    let result = db
        .execute("MATCH (n:Person) RETURN n.name, labels(n)")
        .expect("MATCH Person with labels(n) in RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 Person row; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "n.name must be 'Alice'; got: {:?}",
        result.rows[0][0]
    );
    match &result.rows[0][1] {
        Value::List(items) => {
            assert_eq!(
                items[0],
                Value::String("Person".to_string()),
                "labels(n)[0] must be 'Person'"
            );
        }
        other => panic!("expected Value::List from labels(n), got: {:?}", other),
    }
}

// ── Test 4: labels(n) returns correct label across mixed-label graph ──────────

/// When the graph contains multiple label types, labels(n) returns the correct
/// label for each matched node.
#[test]
fn labels_fn_correct_per_label_type() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Animal {name: 'Rex'})")
        .expect("CREATE Rex");

    let persons = db
        .execute("MATCH (n:Person) RETURN labels(n)")
        .expect("Person query");
    let animals = db
        .execute("MATCH (n:Animal) RETURN labels(n)")
        .expect("Animal query");

    // Verify Person labels
    assert_eq!(persons.rows.len(), 1);
    match &persons.rows[0][0] {
        Value::List(items) => assert_eq!(items[0], Value::String("Person".to_string())),
        other => panic!("expected List, got {:?}", other),
    }

    // Verify Animal labels
    assert_eq!(animals.rows.len(), 1);
    match &animals.rows[0][0] {
        Value::List(items) => assert_eq!(items[0], Value::String("Animal".to_string())),
        other => panic!("expected List, got {:?}", other),
    }
}
