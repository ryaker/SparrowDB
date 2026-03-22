//! SPA-183: MATCH…CREATE must use matched variable bindings — not a Cartesian
//! product of all node candidates.
//!
//! Before this fix, `execute_match_create` called `scan_match_create` which
//! collected all matching NodeIds per variable independently and then took a
//! full Cartesian product.  With N nodes of the same label this created N²
//! edges instead of exactly one per matched pair.
//!
//! The fix introduces `scan_match_create_rows` which executes the MATCH clause
//! properly, yielding one correlated binding row per result.  `execute_match_create`
//! then iterates those rows — one `create_edge` call per row.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: exactly one edge created when props uniquely identify nodes ────────

/// `MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)`
/// must create exactly **one** KNOWS edge — not N² edges.
///
/// With the old Cartesian-product bug and two Person nodes, two separate
/// `MATCH…CREATE` calls (or even one with multiple label candidates) could
/// produce more than 1 edge. This test asserts the correct count of 1.
#[test]
fn match_create_exactly_one_edge_with_props() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE");

    // Count KNOWS edges via traversal.
    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH traversal");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 KNOWS edge; got {} rows: {:?}",
        result.rows.len(),
        result.rows
    );
}

// ── Test 2: the single edge connects the correct nodes (Alice→Bob) ─────────────

/// Verify that the edge created by MATCH…CREATE connects Alice to Bob
/// specifically — not any other pair.
#[test]
fn match_create_correct_endpoints() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE");

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH traversal");

    assert_eq!(result.rows.len(), 1, "expected 1 row");

    let row = &result.rows[0];
    assert_eq!(
        row.len(),
        2,
        "expected 2 columns (a.name, b.name); got: {:?}",
        row
    );

    use sparrowdb_execution::types::Value;
    assert_eq!(
        row[0],
        Value::String("Alice".to_string()),
        "src of KNOWS edge must be Alice; got: {:?}",
        row[0]
    );
    assert_eq!(
        row[1],
        Value::String("Bob".to_string()),
        "dst of KNOWS edge must be Bob; got: {:?}",
        row[1]
    );
}

// ── Test 3: with 3+ Person nodes, props still produce exactly 1 edge ──────────

/// Even when the DB contains more Person nodes (Carol, Dave), the property
/// filters on the MATCH clause must restrict the result to exactly the Alice→Bob
/// pair — not a Cartesian product involving all Person nodes.
#[test]
fn match_create_no_cartesian_product_with_extra_nodes() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol'})")
        .expect("CREATE Carol");
    db.execute("CREATE (n:Person {name: 'Dave'})")
        .expect("CREATE Dave");

    // Should create exactly ONE KNOWS edge: Alice → Bob.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE");

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH traversal");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 KNOWS edge with 4 Person nodes; got {} rows: {:?}",
        result.rows.len(),
        result.rows
    );

    use sparrowdb_execution::types::Value;
    assert_eq!(row(&result, 0, 0), Value::String("Alice".to_string()));
    assert_eq!(row(&result, 0, 1), Value::String("Bob".to_string()));
}

// ── Test 4: no-match → no edges (regression from SPA-168) ─────────────────────

/// When MATCH finds no nodes, CREATE must not produce any edges.
/// The KNOWS rel type must also not be registered in the catalog.
#[test]
fn match_create_no_match_no_edge_regression() {
    use sparrowdb_catalog::catalog::Catalog;

    let (dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");

    // 'Ghost' does not exist → MATCH yields no rows → no edges.
    db.execute(
        "MATCH (a:Person {name: 'Ghost'}), (b:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("must not error");

    // Verify KNOWS rel type was never registered (no edge was created).
    let catalog = Catalog::open(dir.path()).expect("catalog re-open");
    let tables = catalog.list_rel_tables().expect("list_rel_tables");
    let knows_registered = tables.iter().any(|(_, _, rt)| rt == "KNOWS");

    assert!(
        !knows_registered,
        "no rel type must be registered when MATCH found no rows; tables: {:?}",
        tables
    );
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn row(
    result: &sparrowdb_execution::types::QueryResult,
    r: usize,
    c: usize,
) -> sparrowdb_execution::types::Value {
    result.rows[r][c].clone()
}
