//! SPA-209 acceptance tests — CALL db.schema() + named columns in QueryResult.
//!
//! Covers:
//!   1. `call_db_schema_returns_labels` — create nodes with two labels,
//!      call db.schema(), verify both labels appear.
//!   2. `query_result_has_named_columns` — execute `MATCH (n:Person) RETURN n.name`,
//!      verify `result.columns == ["n.name"]`.
//!   3. `schema_includes_rel_types` — create nodes + edges, call db.schema(),
//!      verify the relationship type appears.

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::Value;

/// Open a fresh database in a temp directory.
fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: create nodes with two labels; CALL db.schema() returns both labels.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn call_db_schema_returns_labels() {
    let (_dir, db) = make_db();

    // Create nodes using Cypher (which registers properties via merge_node).
    db.execute("CREATE (p:Person {name: 'Alice'})")
        .expect("create person");
    db.execute("CREATE (c:Company {name: 'Acme'})")
        .expect("create company");

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    assert_eq!(
        result.columns,
        vec!["label", "properties", "relationship_types"],
        "schema columns should be [label, properties, relationship_types]"
    );

    // Extract the label strings from the rows.
    let mut label_names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| {
            if let Some(Value::String(name)) = row.first() {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();
    label_names.sort();

    assert!(
        label_names.contains(&"Person".to_owned()),
        "schema should include Person label; got: {label_names:?}"
    );
    assert!(
        label_names.contains(&"Company".to_owned()),
        "schema should include Company label; got: {label_names:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: MATCH … RETURN produces correctly named columns.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn query_result_has_named_columns() {
    let (_dir, db) = make_db();

    // Insert a Person node.
    db.execute("CREATE (p:Person {name: 'Bob', age: 30})")
        .expect("create person");

    // Query with a single RETURN projection.
    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query must succeed");

    assert_eq!(
        result.columns,
        vec!["n.name"],
        "column name for 'n.name' projection should be 'n.name'"
    );
    assert_eq!(result.rows.len(), 1, "should return one row");

    // Query with multiple projections.
    let result2 = db
        .execute("MATCH (n:Person) RETURN n.name, n.age")
        .expect("query must succeed");

    assert_eq!(
        result2.columns,
        vec!["n.name", "n.age"],
        "columns should match the RETURN projection order"
    );

    // Query with an AS alias.
    let result3 = db
        .execute("MATCH (n:Person) RETURN n.name AS fullName")
        .expect("query must succeed");

    assert_eq!(
        result3.columns,
        vec!["fullName"],
        "AS alias should be reflected in columns"
    );

    // Query with count(*).
    let result4 = db
        .execute("MATCH (n:Person) RETURN count(*)")
        .expect("query must succeed");

    assert_eq!(
        result4.columns,
        vec!["count(*)"],
        "count(*) column name should be 'count(*)'"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: create nodes + edges; CALL db.schema() reports the rel type.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_includes_rel_types() {
    let (_dir, db) = make_db();

    // Create two nodes and connect them with a KNOWS relationship.
    db.execute("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})")
        .expect("create nodes");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("create edge");

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    // Every row's third column should be a List of rel types.
    // We only need to find at least one row where KNOWS appears.
    let mut found_knows = false;
    for row in &result.rows {
        if let Some(Value::List(rel_types)) = row.get(2) {
            for rt in rel_types {
                if let Value::String(s) = rt {
                    if s == "KNOWS" {
                        found_knows = true;
                    }
                }
            }
        }
    }

    assert!(
        found_knows,
        "CALL db.schema() should report KNOWS relationship type; rows: {result:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: property keys registered via merge_node appear in schema.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_includes_property_keys() {
    let (_dir, db) = make_db();

    // Create a node with named properties via Cypher CREATE.
    db.execute("CREATE (p:Person {name: 'Carol', age: 42})")
        .expect("create person");

    let result = db
        .execute("CALL db.schema()")
        .expect("CALL db.schema() must succeed");

    // Find the Person row and check its properties list.
    let person_row = result
        .rows
        .iter()
        .find(|row| matches!(row.first(), Some(Value::String(s)) if s == "Person"));

    assert!(person_row.is_some(), "Person label should appear in schema");

    let person_row = person_row.unwrap();
    if let Some(Value::List(props)) = person_row.get(1) {
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
            "Person properties should include 'name'; got: {prop_names:?}"
        );
        assert!(
            prop_names.contains(&"age"),
            "Person properties should include 'age'; got: {prop_names:?}"
        );
    } else {
        panic!(
            "Expected Value::List for properties column, got: {:?}",
            person_row.get(1)
        );
    }
}
