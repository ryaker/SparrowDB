//! SPA-224 debug: variable-length path query with __SO_-prefixed labels.
//!
//! Uses the low-level WriteTx API to bypass the SPA-208 reserved-label guard,
//! since __SO_ labels are for internal system use only and cannot be created
//! via user-facing Cypher CREATE.

use sparrowdb::fnv1a_col_id;
use sparrowdb::open;
use sparrowdb::{NodeId, Value};
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Build an ontology-like graph using the low-level WriteTx API:
///   Employee -[:__SO_SUBCLASS_OF]-> Person
///
/// Both nodes share the __SO_Class label (slot 0 = Person, slot 1 = Employee).
/// Then test both 1-hop and var-path MATCH queries.
#[test]
fn so_label_varpath_repro() {
    let (_dir, db) = make_db();

    let name_col = fnv1a_col_id("name");

    // ── Create nodes + edge via WriteTx (bypasses SPA-208 guard) ─────────
    let label_id = {
        let mut tx = db.begin_write().expect("begin_write");

        let label_id = tx.create_label("__SO_Class").expect("create label") as u32;

        // slot 0 = Person (Bytes stores strings)
        tx.create_node(label_id, &[(name_col, Value::Bytes(b"Person".to_vec()))])
            .expect("create Person");

        // slot 1 = Employee
        tx.create_node(label_id, &[(name_col, Value::Bytes(b"Employee".to_vec()))])
            .expect("create Employee");

        let person_node = NodeId((label_id as u64) << 32);
        let employee_node = NodeId((label_id as u64) << 32 | 1);

        // Employee -> Person
        tx.create_edge(
            employee_node,
            person_node,
            "__SO_SUBCLASS_OF",
            HashMap::new(),
        )
        .expect("create edge");

        tx.commit().expect("commit");
        label_id
    };
    let _ = label_id;

    // ── 1-hop: should work ─────────────────────────────────────────────────
    let result_1hop = db
        .execute(
            "MATCH (c:__SO_Class {name:'Employee'})-[:__SO_SUBCLASS_OF]->(a:__SO_Class {name:'Person'}) \
             RETURN c.name",
        )
        .expect("1-hop query");
    println!("1-hop: {:?}", result_1hop.rows);

    // ── var-path: this is the one that fails ──────────────────────────────
    let result_var = db
        .execute(
            "MATCH (c:__SO_Class {name:'Employee'})-[:__SO_SUBCLASS_OF*1..20]->(a:__SO_Class {name:'Person'}) \
             RETURN c.name",
        )
        .expect("var-path query");
    println!("var-path: {:?}", result_var.rows);

    assert_eq!(
        result_1hop.rows.len(),
        1,
        "1-hop must return 1 row; got: {:?}",
        result_1hop.rows
    );
    assert_eq!(
        result_var.rows.len(),
        1,
        "var-path must return 1 row; got: {:?}",
        result_var.rows
    );
}
