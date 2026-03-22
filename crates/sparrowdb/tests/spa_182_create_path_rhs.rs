//! SPA-182: Standalone CREATE path parser must capture both LHS and RHS nodes.
//!
//! Before this fix, `CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})`
//! silently dropped the right-hand-side node `(b)`.  The parser's `parse_create_body`
//! never added the source node (`node`) in the edge-pattern branch at all, and
//! `dst_node` was only added when it had labels or props.
//!
//! Additionally, the executor's `execute_create` never processed `create.edges`, so
//! even when both nodes were known, no relationship was written to the store.
//!
//! Fixes:
//! 1. `parse_create_body` (parser.rs): both src and dst nodes are always emitted so
//!    that the executor can create them and resolve variable bindings for edges.
//! 2. `Engine::is_mutation` (engine.rs): `Statement::Create` with edges is now
//!    classified as a mutation so it is routed to the write-transaction path.
//! 3. `GraphDb::execute_create_standalone` (lib.rs): pre-validates edge variable
//!    references, then creates all declared nodes via `WriteTx::create_node`
//!    (recording variable → NodeId), and finally calls `WriteTx::create_edge` for
//!    each relationship in the CREATE clause.

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── SPA-182 test 1: both nodes and the edge are created in one CREATE ─────────

/// `CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})`
/// must create two Person nodes and one KNOWS edge.  A subsequent
/// `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name`
/// must return exactly one row with the correct (Alice, Bob) pair.
#[test]
fn create_path_both_nodes_and_edge_persisted() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .expect("standalone CREATE path must succeed");

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH traversal must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 KNOWS edge after CREATE path; got rows: {:?}",
        result.rows
    );

    let row = &result.rows[0];
    assert_eq!(
        row[0],
        Value::String("Alice".to_string()),
        "a.name must be 'Alice'; got {:?}",
        row[0]
    );
    assert_eq!(
        row[1],
        Value::String("Bob".to_string()),
        "b.name must be 'Bob'; got {:?}",
        row[1]
    );
}

// ── SPA-182 test 2: RHS node (b) is independently queryable ──────────────────

/// After the standalone CREATE path both Alice and Bob must be reachable via
/// plain node scans — confirming the RHS node is not silently dropped.
#[test]
fn create_path_rhs_node_is_queryable() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .expect("standalone CREATE path must succeed");

    // Both nodes must be visible.
    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH Person must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 Person nodes (Alice and Bob); got rows: {:?}",
        result.rows
    );

    // Collect returned names to verify both Alice and Bob are present.
    let mut names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| {
            if let Value::String(s) = &row[0] {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    names.sort();

    assert_eq!(
        names,
        vec!["Alice".to_string(), "Bob".to_string()],
        "expected both Alice and Bob to be present; got {:?}",
        names
    );
}

// ── SPA-182 test 3: rel type is registered in catalog ─────────────────────────

/// The relationship type 'KNOWS' must appear in the catalog after the
/// standalone CREATE path so that subsequent bound queries can resolve it.
#[test]
fn create_path_rel_type_registered_in_catalog() {
    let (dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .expect("standalone CREATE path must succeed");

    let catalog = Catalog::open(dir.path()).expect("catalog re-open");
    let tables = catalog.list_rel_tables().expect("list_rel_tables");
    let knows_registered = tables.iter().any(|(_, _, rt)| rt == "KNOWS");

    assert!(
        knows_registered,
        "rel type 'KNOWS' must be persisted in catalog after standalone CREATE path; tables: {:?}",
        tables
    );
}
