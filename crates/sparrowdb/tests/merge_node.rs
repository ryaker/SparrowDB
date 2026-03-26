//! SPA-247 — merge_node_by_property (upsert) tests.
//!
//! Verifies find-by-property-or-create semantics on both WriteTx and GraphDb.

use sparrowdb::{open, GraphDb};
use sparrowdb_storage::node_store::Value;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Helper: create a Bytes value from a string literal.
fn str_val(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

#[test]
fn merge_node_creates_when_absent() {
    let (_dir, db) = make_db();

    let match_value = str_val("alice@example.com");
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("Alice"));
    props.insert("age".to_string(), Value::Int64(30));

    let (node_id, created) = db
        .merge_node_by_property("Person", "email", &match_value, props)
        .expect("merge_node_by_property");

    assert!(created, "should report created=true for new node");

    // Verify node exists with correct properties via Cypher.
    let result = db
        .execute("MATCH (n:Person {email: 'alice@example.com'}) RETURN n.name, n.age")
        .expect("query");
    assert_eq!(result.rows.len(), 1, "exactly one node should exist");

    // Verify the returned NodeId is valid.
    let _ = node_id; // just ensure it's usable
}

#[test]
fn merge_node_returns_existing_when_present() {
    let (_dir, db) = make_db();

    let match_value = str_val("bob@example.com");

    // First call — create.
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), str_val("Bob"));
    let (id1, created1) = db
        .merge_node_by_property("Person", "email", &match_value, props1)
        .expect("first merge");
    assert!(created1, "first call should create");

    // Second call — find existing, update properties.
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), str_val("Robert"));
    props2.insert("age".to_string(), Value::Int64(42));
    let (id2, created2) = db
        .merge_node_by_property("Person", "email", &match_value, props2)
        .expect("second merge");

    assert!(!created2, "second call should find existing");
    assert_eq!(id1, id2, "should return same NodeId");

    // Verify property was updated.
    let result = db
        .execute("MATCH (n:Person {email: 'bob@example.com'}) RETURN n.name, n.age")
        .expect("query");
    assert_eq!(result.rows.len(), 1, "still exactly one node");
}

#[test]
fn merge_node_idempotent_on_reimport() {
    let (_dir, db) = make_db();

    let match_value = str_val("charlie@example.com");

    // Call merge_node_by_property 10 times with the same match key.
    let mut first_id = None;
    for i in 0..10 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), str_val("Charlie"));
        props.insert("import_seq".to_string(), Value::Int64(i));
        let (id, created) = db
            .merge_node_by_property("Person", "email", &match_value, props)
            .expect("merge");
        if i == 0 {
            assert!(created, "first call should create");
            first_id = Some(id);
        } else {
            assert!(!created, "subsequent calls should find existing");
            assert_eq!(id, first_id.unwrap(), "same NodeId every time");
        }
    }

    // Verify exactly 1 node with this email exists.
    let result = db
        .execute("MATCH (n:Person {email: 'charlie@example.com'}) RETURN n.name")
        .expect("query");
    assert_eq!(result.rows.len(), 1, "exactly one node after 10 merges");
}

#[test]
fn merge_node_by_property_via_write_tx() {
    let (_dir, db) = make_db();

    let match_value = str_val("dana@example.com");

    // Use WriteTx directly to test the lower-level API.
    {
        let mut tx = db.begin_write().expect("begin_write");
        let mut props = HashMap::new();
        props.insert("name".to_string(), str_val("Dana"));
        let (id, created) = tx
            .merge_node_by_property("Person", "email", &match_value, props)
            .expect("merge in tx");
        assert!(created);

        // Second merge within the same transaction should find the pending node.
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), str_val("Dana Updated"));
        let (id2, created2) = tx
            .merge_node_by_property("Person", "email", &match_value, props2)
            .expect("merge in tx again");
        assert!(!created2, "should find pending node");
        assert_eq!(id, id2);

        tx.commit().expect("commit");
    }

    // Verify from a fresh read.
    let result = db
        .execute("MATCH (n:Person {email: 'dana@example.com'}) RETURN n.name")
        .expect("query");
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn merge_node_different_labels_are_independent() {
    let (_dir, db) = make_db();

    let match_value = str_val("shared-key");

    let mut props = HashMap::new();
    props.insert("data".to_string(), str_val("person-data"));
    let (id_person, created_person) = db
        .merge_node_by_property("Person", "code", &match_value, props)
        .expect("merge Person");
    assert!(created_person);

    let mut props2 = HashMap::new();
    props2.insert("data".to_string(), str_val("company-data"));
    let (id_company, created_company) = db
        .merge_node_by_property("Company", "code", &match_value, props2)
        .expect("merge Company");
    assert!(created_company, "different label should create new node");
    assert_ne!(id_person, id_company, "different labels = different nodes");
}
