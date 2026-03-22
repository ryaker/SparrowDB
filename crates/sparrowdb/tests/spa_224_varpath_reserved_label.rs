//! SPA-224: variable-length path queries must work when src/dst nodes use
//! `__SO_`-prefixed labels.
//!
//! ## Root cause
//!
//! `execute_variable_length` read dst node props using only the projection
//! column set (`col_ids_dst`).  When the dst variable was not referenced in
//! RETURN (e.g. `RETURN c.name` with dst var `a`), `col_ids_dst` defaulted
//! to `[col_0]` — the tombstone sentinel column — so the inline prop filter
//! on the dst node (e.g. `{name:'Person'}`) always evaluated against an empty
//! set and silently failed.
//!
//! The fix extends the dst read set to include every column referenced by the
//! dst inline prop filters and the WHERE clause, mirroring the 1-hop code.
//!
//! ## Test strategy
//!
//! Uses the low-level `WriteTx` API to create `__SO_`-prefixed label nodes
//! (bypassing the SPA-208 user-guard) and runs both 1-hop and var-path queries
//! to verify correctness.

use sparrowdb::fnv1a_col_id;
use sparrowdb::open;
use sparrowdb::{NodeId, Value};
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Helper ────────────────────────────────────────────────────────────────────

/// Build the __SO_ ontology graph:
///   Employee(:__SO_Class) -[:__SO_SUBCLASS_OF]-> Person(:__SO_Class)
///
/// Returns the db so the caller can run queries against it.
fn build_so_graph() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let (dir, db) = make_db();
    let name_col = fnv1a_col_id("name");

    {
        let mut tx = db.begin_write().expect("begin_write");

        let label_id = tx
            .create_label("__SO_Class")
            .expect("create __SO_Class label") as u32;

        // slot 0 = Person (Bytes stores string values)
        let person_node = NodeId((label_id as u64) << 32);
        tx.create_node(label_id, &[(name_col, Value::Bytes(b"Person".to_vec()))])
            .expect("create Person node");

        // slot 1 = Employee
        let employee_node = NodeId((label_id as u64) << 32 | 1);
        tx.create_node(label_id, &[(name_col, Value::Bytes(b"Employee".to_vec()))])
            .expect("create Employee node");

        // Employee -[:__SO_SUBCLASS_OF]-> Person
        tx.create_edge(
            employee_node,
            person_node,
            "__SO_SUBCLASS_OF",
            HashMap::new(),
        )
        .expect("create subclass edge");

        tx.commit().expect("commit");
    }

    (dir, db)
}

// ── Test 1: varpath_with_reserved_label_finds_nodes ───────────────────────────

/// A var-path `*1..20` query over `__SO_`-prefixed labels must return the
/// correct source node when the dst node satisfies the inline prop filter.
///
/// Before the fix this returned `[]`; after the fix it returns
/// `[[String("Employee")]]`.
#[test]
fn varpath_with_reserved_label_finds_nodes() {
    let (_dir, db) = build_so_graph();

    // 1-hop sanity check: this always worked.
    let r1 = db
        .execute(
            "MATCH (c:__SO_Class {name:'Employee'})-[:__SO_SUBCLASS_OF]->(a:__SO_Class {name:'Person'}) \
             RETURN c.name",
        )
        .expect("1-hop query must succeed");

    assert_eq!(
        r1.rows.len(),
        1,
        "1-hop must return 1 row; got: {:?}",
        r1.rows
    );
    assert_eq!(
        r1.rows[0][0],
        sparrowdb_execution::Value::String("Employee".into()),
        "1-hop row[0][0] must be 'Employee'"
    );

    // var-path: this was the broken case (SPA-224).
    let rv = db
        .execute(
            "MATCH (c:__SO_Class {name:'Employee'})-[:__SO_SUBCLASS_OF*1..20]->(a:__SO_Class {name:'Person'}) \
             RETURN c.name",
        )
        .expect("var-path query must succeed");

    assert_eq!(
        rv.rows.len(),
        1,
        "var-path must return 1 row; got: {:?}",
        rv.rows
    );
    assert_eq!(
        rv.rows[0][0],
        sparrowdb_execution::Value::String("Employee".into()),
        "var-path row[0][0] must be 'Employee'"
    );
}

// ── Test 2: varpath_normal_label_still_works ──────────────────────────────────

/// Regular (non-reserved) labels must continue to work correctly after the
/// fix.  This is a regression guard: the fix must not break existing behavior.
#[test]
fn varpath_normal_label_still_works() {
    let (_dir, db) = make_db();

    // Build a simple chain: Alice -[:KNOWS]-> Bob -[:KNOWS]-> Carol.
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // 1-hop with dst filter.
    let r1 = db
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a.name",
        )
        .expect("1-hop dst-filtered query");
    assert_eq!(r1.rows.len(), 1, "1-hop with dst filter: {:?}", r1.rows);

    // var-path with dst filter — this tests the same code path as SPA-224
    // but with a normal label.
    let rv = db
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..5]->(b:Person {name: 'Carol'}) \
             RETURN a.name",
        )
        .expect("var-path dst-filtered query must succeed");

    assert_eq!(
        rv.rows.len(),
        1,
        "var-path with dst filter must return 1 row; got: {:?}",
        rv.rows
    );
    assert_eq!(
        rv.rows[0][0],
        sparrowdb_execution::Value::String("Alice".into()),
        "var-path row[0][0] must be 'Alice'"
    );
}
