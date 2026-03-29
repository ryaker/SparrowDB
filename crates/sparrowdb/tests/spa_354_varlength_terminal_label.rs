//! Regression tests for issue #354: variable-length path with terminal label filter returns 0 rows.
//!
//! Demonstrates two related failure modes:
//!
//! 1. Source and destination have **different** labels — `(a:Person)-[:R*1..3]->(b:Company)`.
//!    In a graph with Person→Company edges the query must return the Company nodes.
//!
//! 2. Source node is **unlabeled** — `(a)-[:R*1..3]->(b:Person)`.
//!    The engine must scan all nodes regardless of their label and filter destination by label.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: labeled source → different-label destination ───────────────────────

/// `(a:Person)-[:WORKS_AT*1..2]->(b:Company)` must return the reachable Company node.
///
/// This is the core regression: the terminal label filter (`b:Company`) must be
/// applied correctly even when the destination label differs from the source label.
#[test]
fn varpath_terminal_label_different_from_source() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Company {name: 'Acme'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[:WORKS_AT*1..2]->(b:Company) RETURN a.name, b.name")
        .expect("var-length path with different-label dst must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row (Alice -> Acme), got {:?}",
        result.rows
    );
}

// ── Test 2: labeled source → different-label destination, multi-hop ───────────

/// Person → Company → Product: `(a:Person)-[:LINK*1..3]->(b:Product)` must reach Product.
#[test]
fn varpath_terminal_label_multi_hop() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Company {name: 'TechCo'})").unwrap();
    db.execute("CREATE (:Product {name: 'Widget'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Company {name: 'TechCo'}) CREATE (a)-[:LINK]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Company {name: 'TechCo'}), (b:Product {name: 'Widget'}) CREATE (a)-[:LINK]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[:LINK*1..3]->(b:Product) RETURN b.name")
        .expect("multi-hop var-length path with different-label dst must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row (Widget), got {:?}",
        result.rows
    );

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match &row[0] {
            sparrowdb_execution::types::Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        names.contains(&"Widget".to_string()),
        "expected Widget in results, got {:?}",
        names
    );
}

// ── Test 3: terminal label filter excludes wrong-label nodes ──────────────────

/// Graph: Person→Company, Person→Person.
/// `(a:Person)-[:KNOWS*1..2]->(b:Company)` must only return Company nodes,
/// not Person nodes also reachable via KNOWS edges.
#[test]
fn varpath_terminal_label_excludes_wrong_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Company {name: 'Acme'})").unwrap();

    // Alice -> Bob (Person->Person)
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    // Alice -> Acme (Person->Company)
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Company {name: 'Acme'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Company) RETURN b.name")
        .expect("terminal label filter with mixed-label neighbors must not error");

    // Only Acme should be returned -- not Bob (who is a Person)
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row (Acme only), got {:?}",
        result.rows
    );

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match &row[0] {
            sparrowdb_execution::types::Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        names.contains(&"Acme".to_string()),
        "expected Acme in results, got {:?}",
        names
    );
    assert!(
        !names.contains(&"Bob".to_string()),
        "Bob (Person) must not appear in Company-filtered results, got {:?}",
        names
    );
}

// ── Test 4: unlabeled source with labeled destination ─────────────────────────

/// `(a)-[:KNOWS*1..2]->(b:Person)` with an unlabeled source must not error and
/// must return Person destination nodes.
///
/// Before the fix: `execute_variable_length` called `get_label("").ok_or(NotFound)?`
/// which returned Err(NotFound) for an empty label string, causing the query to
/// fail rather than scan all source nodes.
#[test]
fn varpath_unlabeled_source_labeled_destination() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a)-[:KNOWS*1..2]->(b:Person) RETURN b.name")
        .expect("var-length path with unlabeled source and labeled dst must not error");

    assert!(
        !result.rows.is_empty(),
        "expected at least 1 row, got {:?}",
        result.rows
    );

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match &row[0] {
            sparrowdb_execution::types::Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        names.contains(&"Bob".to_string()),
        "expected Bob in results, got {:?}",
        names
    );
}

// ── Test 5: regression — labeled src+dst same label still works ───────────────

/// Guard against the fix breaking the already-passing case where src and dst
/// share the same label (`(a:Person)-[:KNOWS*1..2]->(b:Person)`).
#[test]
fn varpath_same_label_src_dst_unaffected() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN b.name")
        .expect("same-label var-length path must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row (Alice -> Bob), got {:?}",
        result.rows
    );
}
