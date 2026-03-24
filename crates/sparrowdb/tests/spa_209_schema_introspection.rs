//! SPA-209 acceptance tests — CALL db.schema() + named columns in QueryResult.
//!
//! Covers:
//!   1. CALL db.schema() returns node labels with their property names.
//!   2. CALL db.schema() returns relationship types with their property names.
//!   3. QueryResult.columns is non-empty and contains expected column names.
//!   4. Empty DB returns an empty schema (no rows, valid columns).
//!   5. Labels with no properties show an empty property list.

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::Value;

/// Open a fresh database in a temp directory.
fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: QueryResult.columns has the expected names
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_result_has_named_columns() {
    let (_dir, db) = make_db();

    // Create a node so there's something in the schema.
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    assert_eq!(
        result.columns,
        vec!["type", "name", "properties"],
        "QueryResult.columns must be [type, name, properties]"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: node labels appear with their property names
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_contains_node_labels_and_properties() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (m:Movie {title: 'Inception'})").unwrap();

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    // Find the Person row.
    let person_row = result
        .rows
        .iter()
        .find(|row| {
            row[0] == Value::String("node".to_owned())
                && row[1] == Value::String("Person".to_owned())
        })
        .expect("Person label must appear in schema");

    // Properties for Person should include 'age' and 'name'.
    match &person_row[2] {
        Value::List(props) => {
            let prop_names: Vec<&str> = props
                .iter()
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(
                prop_names.contains(&"name"),
                "Person properties must include 'name', got: {:?}",
                prop_names
            );
            assert!(
                prop_names.contains(&"age"),
                "Person properties must include 'age', got: {:?}",
                prop_names
            );
        }
        other => panic!("properties column must be a List, got: {:?}", other),
    }

    // Find the Movie row.
    let movie_row = result
        .rows
        .iter()
        .find(|row| {
            row[0] == Value::String("node".to_owned())
                && row[1] == Value::String("Movie".to_owned())
        })
        .expect("Movie label must appear in schema");

    match &movie_row[2] {
        Value::List(props) => {
            let prop_names: Vec<&str> = props
                .iter()
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(
                prop_names.contains(&"title"),
                "Movie properties must include 'title', got: {:?}",
                prop_names
            );
        }
        other => panic!("properties column must be a List, got: {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: relationship types appear in schema
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_contains_relationship_types() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .unwrap();
    db.execute("CREATE (p:Person {name: 'Charlie'})-[:ACTED_IN]->(m:Movie {title: 'The Matrix'})")
        .unwrap();

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    // Check KNOWS relationship appears.
    let knows_row = result
        .rows
        .iter()
        .find(|row| {
            row[0] == Value::String("relationship".to_owned())
                && row[1] == Value::String("KNOWS".to_owned())
        })
        .expect("KNOWS relationship type must appear in schema");

    assert_eq!(
        knows_row[0],
        Value::String("relationship".to_owned()),
        "type column must be 'relationship'"
    );

    // Check ACTED_IN appears.
    assert!(
        result.rows.iter().any(|row| {
            row[0] == Value::String("relationship".to_owned())
                && row[1] == Value::String("ACTED_IN".to_owned())
        }),
        "ACTED_IN relationship type must appear in schema"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: empty database returns empty rows, valid columns
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_empty_db_returns_no_rows() {
    let (_dir, db) = make_db();

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() on empty DB must succeed");

    assert!(
        result.rows.is_empty(),
        "empty DB must produce zero schema rows"
    );
    assert_eq!(
        result.columns,
        vec!["type", "name", "properties"],
        "columns must be populated even for empty DB"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: label with no properties shows empty list
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_label_with_no_properties() {
    let (_dir, db) = make_db();

    // Explicitly create a node with no properties via WriteTx so the label is
    // registered but no properties are stored.
    let mut tx = db.begin_write().expect("begin_write");
    let label_id = tx.create_label("Empty").expect("create Empty label") as u32;
    tx.create_node(label_id, &[]).expect("create node");
    tx.commit().expect("commit");

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    let empty_row = result
        .rows
        .iter()
        .find(|row| {
            row[0] == Value::String("node".to_owned())
                && row[1] == Value::String("Empty".to_owned())
        })
        .expect("Empty label must appear in schema");

    match &empty_row[2] {
        Value::List(props) => {
            assert!(
                props.is_empty(),
                "Empty label must have no properties, got: {:?}",
                props
            );
        }
        other => panic!("properties must be a List, got: {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6: row_as_map() convenience — columns accessible by name
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn query_result_row_as_map() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Widget {color: 'red', weight: 42})")
        .unwrap();

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    // Use row_as_map to access columns by name.
    let map = result.row_as_map(0).expect("row 0 must exist");
    assert!(map.contains_key("type"), "map must contain 'type'");
    assert!(map.contains_key("name"), "map must contain 'name'");
    assert!(
        map.contains_key("properties"),
        "map must contain 'properties'"
    );
}

#[test]
fn labels_empty_db_returns_empty_vec() {
    let (_dir, db) = make_db();
    let labels = db.labels().expect("labels() must not error");
    assert!(labels.is_empty());
}

#[test]
fn labels_returns_all_registered_labels() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (m:Movie {title: 'Inception'})").unwrap();
    let mut labels = db.labels().expect("labels() must succeed");
    labels.sort();
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Movie".to_string()));
    assert_eq!(labels.len(), 2);
}

#[test]
fn labels_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    { let db = sparrowdb::open(dir.path()).expect("open");
      db.execute("CREATE (n:Animal {name: 'Cat'})").unwrap(); }
    let db2 = sparrowdb::open(dir.path()).expect("reopen");
    let labels = db2.labels().expect("labels() after reopen");
    assert!(labels.contains(&"Animal".to_string()));
}

#[test]
fn relationship_types_empty_db_returns_empty_vec() {
    let (_dir, db) = make_db();
    let types = db.relationship_types().expect("relationship_types() must not error");
    assert!(types.is_empty());
}

#[test]
fn relationship_types_returns_registered_types() {
    let (_dir, db) = make_db();
    db.execute("CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})").unwrap();
    let types = db.relationship_types().expect("relationship_types() must succeed");
    assert!(types.contains(&"KNOWS".to_string()));
}

#[test]
fn relationship_types_deduplicates_same_type() {
    let (_dir, db) = make_db();
    db.execute("CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})").unwrap();
    db.execute("CREATE (x:Animal {name: 'X'})-[:KNOWS]->(y:Animal {name: 'Y'})").unwrap();
    let types = db.relationship_types().expect("relationship_types()");
    let count = types.iter().filter(|t| t.as_str() == "KNOWS").count();
    assert_eq!(count, 1);
}
