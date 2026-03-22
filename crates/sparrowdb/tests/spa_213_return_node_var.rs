//! Regression tests for SPA-213: `MATCH (n:Label) RETURN n` (bare node variable)
//! must return a `Value::Map` of all stored properties instead of `Value::Null`.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// `MATCH (n:Person) RETURN n` must return a non-null map row for each node.
#[test]
fn return_bare_node_variable_yields_map() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN n")
        .expect("MATCH (n:Person) RETURN n must not error");

    // Must return one row per node.
    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows, got {:?}",
        result.rows
    );

    // Column header must be the variable name.
    assert_eq!(result.columns, vec!["n"]);

    // Each row must be a Value::Map (not Null or NodeRef).
    for row in &result.rows {
        assert_eq!(row.len(), 1);
        assert!(
            matches!(&row[0], Value::Map(_)),
            "expected Value::Map, got {:?}",
            row[0]
        );
    }

    // The maps must contain properties for the nodes.
    // Property values are stored under "col_{hash}" keys.  We verify that
    // each map is non-empty and contains at least two entries (name + age).
    for row in &result.rows {
        let Value::Map(ref entries) = row[0] else {
            panic!("expected Map");
        };
        assert!(
            entries.len() >= 2,
            "expected at least 2 properties in map, got {:?}",
            entries
        );
    }
}

/// The maps returned for two different nodes must have different values.
#[test]
fn return_bare_node_variable_distinct_per_node() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN n")
        .expect("MATCH (n:Person) RETURN n must not error");

    assert_eq!(result.rows.len(), 2);

    // The two maps must differ (they represent different nodes with different properties).
    let map0 = &result.rows[0][0];
    let map1 = &result.rows[1][0];
    assert_ne!(
        map0, map1,
        "two distinct nodes must produce distinct maps; got {:?} and {:?}",
        map0, map1
    );
}

/// `RETURN n` on an empty label must return zero rows (not one null row).
#[test]
fn return_bare_node_variable_empty_label() {
    let (_dir, db) = make_db();

    // Create a node to register the Person label, then delete it.
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN n")
        .expect("MATCH on empty label must not error");

    // Deleted node must not appear.
    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows after deletion, got {:?}",
        result.rows
    );
}
