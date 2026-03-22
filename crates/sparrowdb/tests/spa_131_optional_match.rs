//! SPA-131: OPTIONAL MATCH — integration tests.
//!
//! Tests left-outer-join semantics: when no pattern match is found, a single
//! row with NULL values is produced rather than dropping the row entirely.

use std::collections::HashMap;

use sparrowdb::GraphDb;
use sparrowdb_common::NodeId;
use sparrowdb_execution::types::Value;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── Test 1: OPTIONAL MATCH on existing label returns actual rows ──────────────

#[test]
fn optional_match_existing_label_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Create some Person nodes.
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("OPTIONAL MATCH (n:Person) RETURN n.name")
        .expect("optional match must succeed");

    // Two Person nodes exist — should return two rows, not NULLs.
    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows, got {:?}",
        result.rows
    );
    // All rows should be non-null (actual values).
    for row in &result.rows {
        assert_ne!(row[0], Value::Null, "expected non-null name, got {:?}", row);
    }
}

// ── Test 2: OPTIONAL MATCH on non-existent label returns one NULL row ─────────

#[test]
fn optional_match_missing_label_returns_null_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // No Ghost nodes — label doesn't exist.
    let result = db
        .execute("OPTIONAL MATCH (n:Ghost) RETURN n.name")
        .expect("optional match on missing label must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly one NULL row, got {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "expected NULL for missing label, got {:?}",
        result.rows[0]
    );
}

// ── Test 3: OPTIONAL MATCH on label that exists but zero nodes exist ──────────

#[test]
fn optional_match_label_with_no_nodes_returns_null_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // The label "Robot" is not in the catalog and has no nodes.
    // OPTIONAL MATCH should return one NULL row.
    let result = db
        .execute("OPTIONAL MATCH (n:Robot) RETURN n.name")
        .expect("optional match on missing label must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly one NULL row, got {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "expected NULL value, got {:?}",
        result.rows[0]
    );
}

// ── Test 4: NULL values propagate through RETURN without error ────────────────

#[test]
fn optional_match_null_propagates_through_return() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // OPTIONAL MATCH on non-existent label — result must be [NULL, NULL] not an error.
    let result = db
        .execute("OPTIONAL MATCH (n:NoSuchLabel) RETURN n.name, n.age")
        .expect("NULL propagation must not error");

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1, "expected one NULL row");
    assert_eq!(result.rows[0], vec![Value::Null, Value::Null]);
}

// ── Test 5: MATCH + OPTIONAL MATCH combined — nodes with edges ────────────────

/// Set up a graph with 3 Person nodes and one KNOWS edge (Alice→Bob) using the
/// low-level write API, then return the GraphDb handle.
///
/// Returns (db, alice_id, bob_id, carol_id).
fn setup_person_graph_with_edge(dir: &std::path::Path) -> (GraphDb, NodeId, NodeId, NodeId) {
    let db = GraphDb::open(dir).expect("open");

    // Create Person nodes via Cypher CREATE.
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();

    // Read back the node IDs from the MATCH result to get the packed NodeIds.
    let res = db.execute("MATCH (n:Person) RETURN n.name").unwrap();
    // We have 3 rows but need the NodeIds to create an edge.
    // Use a WriteTx to create the edge using known slot positions.
    // Slots are 0=Alice, 1=Bob, 2=Carol (creation order).
    //
    // To get the label_id we need to open the catalog.
    use sparrowdb_catalog::catalog::Catalog;
    let cat = Catalog::open(dir).expect("catalog");
    let person_label_id = cat.get_label("Person").unwrap().expect("Person label") as u32;

    let alice_id = NodeId((person_label_id as u64) << 32);
    let bob_id = NodeId(((person_label_id as u64) << 32) | 1);
    let carol_id = NodeId(((person_label_id as u64) << 32) | 2);

    // Create KNOWS edge Alice → Bob via WriteTx.
    let mut tx = db.begin_write().expect("begin write");
    tx.create_edge(alice_id, bob_id, "KNOWS", HashMap::new())
        .expect("create edge");
    tx.commit().expect("commit");

    let _ = res; // suppress unused warning
    (db, alice_id, bob_id, carol_id)
}

#[test]
fn match_optional_match_with_edges() {
    let dir = tempfile::tempdir().unwrap();
    let (db, alice_id, _bob_id, _carol_id) = setup_person_graph_with_edge(dir.path());

    // Confirm the edge exists via regular 1-hop MATCH.
    let edge_check = db
        .execute("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name")
        .expect("1-hop check");
    assert_eq!(
        edge_check.rows.len(),
        1,
        "expected 1 KNOWS edge (Alice→Bob), got {:?}",
        edge_check.rows
    );

    // Combined query: MATCH all persons, OPTIONAL MATCH their KNOWS neighbors.
    // Alice should have m.name=Bob's name; Bob and Carol should have m.name=NULL.
    let result = db
        .execute("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name")
        .expect("combined MATCH + OPTIONAL MATCH must succeed");

    // Should return 3 rows: one per Person.
    assert_eq!(
        result.rows.len(),
        3,
        "expected 3 rows (one per Person), got {:?}",
        result.rows
    );

    // Determine Alice's slot so we can find her row.
    let alice_slot = alice_id.0 & 0xFFFF_FFFF;
    let _ = alice_slot;

    // At least one row should have a non-null m.name (Alice→Bob edge).
    let non_null_m_rows: Vec<_> = result
        .rows
        .iter()
        .filter(|r| r.len() > 1 && r[1] != Value::Null)
        .collect();

    assert_eq!(
        non_null_m_rows.len(),
        1,
        "exactly 1 row should have non-null m.name (Alice→Bob): {:?}",
        result.rows
    );

    // The other 2 rows (Bob, Carol — no outgoing KNOWS) must have NULL m.name.
    let null_m_rows: Vec<_> = result
        .rows
        .iter()
        .filter(|r| r.len() > 1 && r[1] == Value::Null)
        .collect();

    assert_eq!(
        null_m_rows.len(),
        2,
        "exactly 2 rows should have NULL m.name (no outgoing KNOWS): {:?}",
        result.rows
    );
}

// ── Test 6: Column names in result are correct ────────────────────────────────

#[test]
fn optional_match_column_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let result = db
        .execute("OPTIONAL MATCH (n:Missing) RETURN n.name AS person_name")
        .expect("column name test must not error");

    assert_eq!(result.columns, vec!["person_name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Null);
}

// ── Test 7: node exists with no matching rel yields null for rel-side var ─────

#[test]
fn optional_match_no_rel_yields_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Alice exists but has no KNOWS relationships.
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();

    let result = db
        .execute("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("optional match must succeed");

    assert_eq!(result.rows.len(), 1, "one row for Alice: {:?}", result.rows);
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "a.name should be Alice"
    );
    assert_eq!(
        result.rows[0][1],
        Value::Null,
        "b.name should be null (no KNOWS rel)"
    );
}

// ── Test 8: node exists with matching rel yields real value ───────────────────

#[test]
fn optional_match_with_rel_yields_value() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _alice_id, _bob_id, _carol_id) = setup_person_graph_with_edge(dir.path());

    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        )
        .expect("optional match with rel must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "one row for Alice→Bob: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "a.name should be Alice"
    );
    assert_eq!(
        result.rows[0][1],
        Value::String("Bob".to_string()),
        "b.name should be Bob (KNOWS rel exists)"
    );
}

// ── Test 9: MATCH + OPTIONAL MATCH chained ────────────────────────────────────

#[test]
fn optional_match_chained() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    // MATCH all persons, OPTIONAL MATCH non-existent LIKES relationship.
    let result = db
        .execute("MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(b:Person) RETURN a.name, b.name")
        .expect("chained optional match must succeed");

    // Both Alice and Bob should appear; b.name is null for both.
    assert_eq!(result.rows.len(), 2, "expected 2 rows: {:?}", result.rows);
    for row in &result.rows {
        assert_ne!(row[0], Value::Null, "a.name should not be null");
        assert_eq!(row[1], Value::Null, "b.name should be null (no LIKES rel)");
    }
}

// ── Test 10: base MATCH finds nothing → 0 rows (not a null row) ──────────────

#[test]
fn optional_match_no_base_node() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // No Person nodes at all — the leading MATCH produces 0 rows.
    let result = db
        .execute("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("query must not error even with no base match");

    // With 0 leading rows the result should also be 0 rows — NOT one null row.
    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows when MATCH produces nothing, got {:?}",
        result.rows
    );
}
