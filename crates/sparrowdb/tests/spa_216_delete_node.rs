//! SPA-216 integration tests — MATCH...DELETE tombstones matched nodes.
//!
//! Verifies that `MATCH (n:Person {name: 'Alice'}) DELETE n` correctly
//! tombstones the matched node so that subsequent MATCH queries return 0 rows.

use sparrowdb::{open, GraphDb};

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// CREATE a Person node, verify it exists, DELETE it, verify it is gone.
#[test]
fn delete_node_with_prop_filter() {
    let (_dir, db) = make_db();

    // CREATE the node.
    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE must succeed");

    // Verify the node exists before delete.
    let before = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("MATCH before DELETE must succeed");
    assert_eq!(
        before.rows.len(),
        1,
        "expected 1 row before DELETE, got {}",
        before.rows.len()
    );

    // DELETE the node.
    db.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .expect("MATCH DELETE must succeed");

    // Verify the node is gone.
    let after = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("MATCH after DELETE must succeed");
    assert_eq!(
        after.rows.len(),
        0,
        "expected 0 rows after DELETE, got {}",
        after.rows.len()
    );
}

/// DELETE without a label-specific filter — delete ALL Person nodes.
///
/// Also verifies label isolation: a Company node created alongside the Person
/// nodes must survive the `MATCH (n:Person) DELETE n` operation.
#[test]
fn delete_all_nodes_of_label() {
    let (_dir, db) = make_db();

    // CREATE two Person nodes and one non-Person control node.
    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE Alice must succeed");
    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE Bob must succeed");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("CREATE Company must succeed");

    // Verify both Person nodes exist.
    let before = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH before DELETE must succeed");
    assert_eq!(before.rows.len(), 2, "expected 2 Person rows before DELETE");

    // DELETE all Person nodes.
    db.execute("MATCH (n:Person) DELETE n")
        .expect("DELETE all Person nodes must succeed");

    // Verify all Person nodes are gone.
    let after = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH after DELETE must succeed");
    assert_eq!(
        after.rows.len(),
        0,
        "expected 0 Person rows after DELETE all, got {}",
        after.rows.len()
    );

    // Verify the Company node was NOT deleted (label isolation).
    let company = db
        .execute("MATCH (n:Company {name: 'Acme'}) RETURN n.name")
        .expect("MATCH Company after Person DELETE must succeed");
    assert_eq!(
        company.rows.len(),
        1,
        "Company node must survive MATCH (n:Person) DELETE n"
    );
}

/// DELETE of a non-existent node is a no-op (no error).
#[test]
fn delete_nonexistent_node_is_noop() {
    let (_dir, db) = make_db();

    // CREATE a Person node (so the label exists).
    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE must succeed");

    // DELETE a node that doesn't exist — should succeed without error.
    db.execute("MATCH (n:Person {name: 'Nobody'}) DELETE n")
        .expect("DELETE of non-existent node must be a no-op (no error)");

    // The existing node (Bob) must still be there.
    let after = db
        .execute("MATCH (n:Person {name: 'Bob'}) RETURN n.name")
        .expect("MATCH Bob after no-op DELETE must succeed");
    assert_eq!(
        after.rows.len(),
        1,
        "Bob must still exist after deleting Nobody"
    );
}

/// Tombstoned nodes must not re-appear in subsequent MATCH queries.
#[test]
fn deleted_node_invisible_to_future_queries() {
    let (_dir, db) = make_db();

    // CREATE two nodes with different names.
    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {name: 'Charlie'})")
        .expect("CREATE Charlie");

    // DELETE Alice only.
    db.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .expect("DELETE Alice");

    // Verify only Charlie remains (by count).
    let remaining = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH after partial DELETE");
    assert_eq!(
        remaining.rows.len(),
        1,
        "expected 1 remaining node after deleting Alice, got {}",
        remaining.rows.len()
    );

    // Confirm the survivor is specifically Charlie, not a corrupted row.
    let charlie = db
        .execute("MATCH (n:Person {name: 'Charlie'}) RETURN n.name")
        .expect("MATCH Charlie after DELETE");
    assert_eq!(
        charlie.rows.len(),
        1,
        "Charlie must remain after deleting Alice"
    );

    // Also confirm Alice is not found by name.
    let alice = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .expect("MATCH Alice after DELETE");
    assert_eq!(alice.rows.len(), 0, "Alice must not appear after DELETE");
}
