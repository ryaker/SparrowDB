//! Minimal reproduction for issue #259.
//!
//! Demonstrates that inline prop filters miss WriteTx nodes when
//! the property index was built before those nodes were committed.

use sparrowdb::{open, GraphDb};
use sparrowdb_storage::node_store::Value;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn str_val(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

/// Accurate reproduction for issue #259:
/// The key is that the LABEL must already exist (so the catalog knows it)
/// when the prewarm query runs — otherwise the label-not-found early-return
/// prevents build_for from being called at all.
///
/// Sequence:
/// 1. Create a seed node to establish the label + col file
/// 2. Read query with inline prop filter → triggers build_for on (label, name_col)
///    → index built from disk (contains seed node only)
/// 3. WriteTx creates ClassA → invalidate_prop_index
/// 4. WriteTx creates ClassB → invalidate_prop_index
/// 5. MATCH {name:'ClassA'} → must find ClassA
#[test]
fn inline_prop_filter_after_index_built_on_seeded_label() {
    let (_dir, db) = make_db();

    // Step 1: seed the label so it exists in catalog
    db.execute("CREATE (n:OntologyClass {name: 'SeedNode'})")
        .expect("seed CREATE");

    // Step 2: read query that builds the index for OntologyClass/name
    // (at this point index has SeedNode only)
    let seed_check = db
        .execute("MATCH (n:OntologyClass {name: 'SeedNode'}) RETURN id(n)")
        .expect("seed check");
    assert_eq!(seed_check.rows.len(), 1, "seed node must be found");

    // Also run a non-matching query to ensure the index is cached with
    // the 'prewarm' key pointing to an empty slot list
    let miss = db
        .execute("MATCH (n:OntologyClass {name: 'ClassA'}) RETURN id(n)")
        .expect("miss before write");
    assert_eq!(miss.rows.len(), 0, "ClassA not yet created");

    // Step 3: WriteTx creates ClassA
    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassA"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    // Step 4: WriteTx creates ClassB
    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassB"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    // Baseline: full scan must return all 3 nodes
    let full = db
        .execute("MATCH (n:OntologyClass) RETURN n.name")
        .expect("full scan");
    assert_eq!(
        full.rows.len(),
        3,
        "full scan must return SeedNode + ClassA + ClassB"
    );

    // Step 5: inline prop filter must find ClassA
    let result = db
        .execute("MATCH (n:OntologyClass {name: 'ClassA'}) RETURN id(n)")
        .expect("inline prop filter for ClassA");
    assert_eq!(
        result.rows.len(),
        1,
        "inline prop filter must find ClassA after WriteTx (bug #259)"
    );
}

/// The exact reproduction from issue #259:
/// 1. A read query runs first (triggering lazy index build on OntologyClass/name)
/// 2. WriteTx creates ClassA
/// 3. WriteTx creates ClassB
/// 4. MATCH {name: 'ClassA'} → should return ClassA's id
#[test]
fn inline_prop_filter_after_prewarm_finds_node() {
    let (_dir, db) = make_db();

    // Step 1: read query that triggers index build on an empty label
    // (simulates the "init/seed" step that creates the label but has no nodes yet)
    let empty = db
        .execute("MATCH (n:OntologyClass {name: 'prewarm'}) RETURN id(n)")
        .expect("step 1: prewarm query");
    assert_eq!(empty.rows.len(), 0, "no nodes yet");

    // Step 2: WriteTx creates ClassA
    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassA"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    // Step 3: WriteTx creates ClassB
    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassB"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    // Step 4a: Full scan must work (baseline)
    let full = db
        .execute("MATCH (n:OntologyClass) RETURN n.name")
        .expect("step 4a: full scan");
    assert_eq!(full.rows.len(), 2, "full scan must return both nodes");

    // Step 4b: Inline prop filter must also work
    let result = db
        .execute("MATCH (n:OntologyClass {name: 'ClassA'}) RETURN id(n)")
        .expect("step 4b: inline prop filter");
    assert_eq!(
        result.rows.len(),
        1,
        "inline prop filter must find ClassA (bug #259)"
    );
}

/// Variant: no prewarm, just two WriteTx operations.
#[test]
fn inline_prop_filter_two_writetx_no_prewarm() {
    let (_dir, db) = make_db();

    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassA"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassB"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    let result = db
        .execute("MATCH (n:OntologyClass {name: 'ClassA'}) RETURN id(n)")
        .expect("prop filter");
    assert_eq!(result.rows.len(), 1, "must find ClassA");
}

/// Variant: single WriteTx (baseline — this should definitely work)
#[test]
fn inline_prop_filter_single_writetx() {
    let (_dir, db) = make_db();

    let mut tx = db.begin_write().unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), str_val("ClassA"));
    tx.merge_node("OntologyClass", props).unwrap();
    tx.commit().unwrap();

    let result = db
        .execute("MATCH (n:OntologyClass {name: 'ClassA'}) RETURN id(n)")
        .expect("prop filter");
    assert_eq!(
        result.rows.len(),
        1,
        "must find ClassA after single WriteTx"
    );
}
