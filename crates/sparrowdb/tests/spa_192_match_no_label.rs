//! Regression tests for SPA-192: `MATCH (n) RETURN n` without a label filter
//! must scan ALL node labels and union the results instead of throwing "not found".

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Basic: label-less MATCH returns all nodes across all labels ───────────────

#[test]
fn match_without_label_returns_all_nodes() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Knowledge {title: 'GraphDB'})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n) RETURN n")
        .expect("MATCH (n) RETURN n must not throw");

    // 3 nodes total across 2 labels.
    assert_eq!(
        result.rows.len(),
        3,
        "expected 3 rows (2 Person + 1 Knowledge), got {}",
        result.rows.len()
    );
}

// ── LIMIT: label-less MATCH with LIMIT returns exactly that many rows ─────────

#[test]
fn match_without_label_with_limit() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Knowledge {title: 'GraphDB'})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Animal {species: 'Cat'})").unwrap();

    let result = db
        .execute("MATCH (n) RETURN n LIMIT 3")
        .expect("MATCH (n) RETURN n LIMIT 3 must not throw");

    assert_eq!(
        result.rows.len(),
        3,
        "LIMIT 3 must return exactly 3 rows, got {}",
        result.rows.len()
    );
}

// ── Empty DB: label-less MATCH on an empty database returns zero rows ─────────

#[test]
fn match_without_label_empty_db_returns_zero_rows() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MATCH (n) RETURN n")
        .expect("MATCH (n) RETURN n on empty db must not throw");

    assert_eq!(
        result.rows.len(),
        0,
        "empty db should return 0 rows, got {}",
        result.rows.len()
    );
}

// ── COUNT(*): label-less MATCH with COUNT(*) returns total node count ─────────

#[test]
fn match_without_label_count_star() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Movie {title: 'Inception'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n) RETURN COUNT(*)")
        .expect("MATCH (n) RETURN COUNT(*) must not throw");

    assert_eq!(
        result.rows.len(),
        1,
        "COUNT(*) must produce exactly one row"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "COUNT(*) must equal total node count (3)"
    );
}

// ── Single label in catalog: label-less MATCH still works ────────────────────

#[test]
fn match_without_label_single_label_in_catalog() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n) RETURN n")
        .expect("MATCH (n) on single-label catalog must not throw");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 Person rows, got {}",
        result.rows.len()
    );
}
