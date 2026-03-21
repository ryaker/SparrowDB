//! SPA-132: UNION / UNION ALL — integration tests.
//!
//! Exercises the full parse → bind → execute path on a real tempdir-backed
//! database (no mocks, real WAL and catalog).
//!
//! Syntax: `MATCH ... RETURN ... UNION [ALL] MATCH ... RETURN ...`
//! - `UNION ALL` — concatenates both row sets, keeping duplicates.
//! - `UNION`     — concatenates and deduplicates identical rows.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── Test 1: UNION ALL combines results from both sides ────────────────────────

#[test]
fn union_combines_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol', age: 35})").unwrap();

    // UNION ALL of two disjoint WHERE-filtered sides — combined they cover all 3 rows.
    let result = db
        .execute(
            "MATCH (n:Person) WHERE n.age > 28 RETURN n.age \
             UNION ALL \
             MATCH (n:Person) WHERE n.age < 28 RETURN n.age",
        )
        .expect("UNION ALL must succeed");

    // Left: Alice(30), Carol(35) = 2 rows; Right: Bob(25) = 1 row → total 3.
    assert_eq!(
        result.rows.len(),
        3,
        "UNION ALL of age>28 and age<28 must return 3 rows, got {:?}",
        result.rows
    );
    assert_eq!(result.columns, vec!["n.age"]);
}

// ── Test 2: UNION deduplicates identical rows ─────────────────────────────────

#[test]
fn union_deduplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})").unwrap();

    // Both sides return all Person ages — UNION must deduplicate so we get 2 unique rows.
    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.age \
             UNION \
             MATCH (n:Person) RETURN n.age",
        )
        .expect("UNION must succeed");

    // Without dedup we'd get 4 rows; with dedup we get 2 unique ages.
    assert_eq!(
        result.rows.len(),
        2,
        "UNION must deduplicate, expected 2 unique age rows, got {:?}",
        result.rows
    );
}

// ── Test 3: UNION ALL keeps duplicates ───────────────────────────────────────

#[test]
fn union_all_keeps_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})").unwrap();

    // Both sides return all Person ages — UNION ALL must keep all 4 rows (2+2).
    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.age \
             UNION ALL \
             MATCH (n:Person) RETURN n.age",
        )
        .expect("UNION ALL must succeed");

    assert_eq!(
        result.rows.len(),
        4,
        "UNION ALL must keep duplicates, expected 4 rows (2+2), got {:?}",
        result.rows
    );
}

// ── Test 4: UNION combining different label sets ──────────────────────────────

#[test]
fn union_different_label_sets() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Company {name: 'Acme'})").unwrap();

    // UNION names from Person label and Company label — both project n.name.
    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name \
             UNION ALL \
             MATCH (n:Company) RETURN n.name",
        )
        .expect("UNION ALL across label sets must succeed");

    // 2 Person rows + 1 Company row = 3 total.
    assert_eq!(
        result.rows.len(),
        3,
        "UNION ALL across Person+Company must return 3 rows, got {:?}",
        result.rows
    );
    assert_eq!(result.columns, vec!["n.name"]);

    // All values must be non-null.
    for row in &result.rows {
        assert_ne!(
            row[0],
            Value::Null,
            "expected non-null name in union result, got {:?}",
            row
        );
    }
}

// ── Test 5: UNION when left side produces no rows ────────────────────────────
//
// Both sides scan the same label but left uses a WHERE predicate that matches
// no nodes.  The engine must return only the right-side rows (no error).

#[test]
fn union_empty_left() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})").unwrap();

    // Left side: WHERE age > 100 → matches nobody (0 rows).
    // Right side: no predicate → 2 rows.
    let result = db
        .execute(
            "MATCH (n:Person) WHERE n.age > 100 RETURN n.age \
             UNION ALL \
             MATCH (n:Person) RETURN n.age",
        )
        .expect("UNION ALL with empty left must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "when left produces no rows, UNION ALL must return right-side rows, got {:?}",
        result.rows
    );

    // All returned values must be non-null integers.
    for row in &result.rows {
        assert!(
            matches!(row[0], Value::Int64(_)),
            "expected Int64 age values from right side, got {:?}",
            row[0]
        );
    }
}

// ── Test 6: UNION when right side produces no rows ───────────────────────────
//
// Both sides scan the same label but right uses a WHERE predicate that matches
// no nodes.  The engine must return only the left-side rows (no error).

#[test]
fn union_empty_right() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})").unwrap();

    // Left side: no predicate → 2 rows.
    // Right side: WHERE age > 100 → matches nobody (0 rows).
    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.age \
             UNION ALL \
             MATCH (n:Person) WHERE n.age > 100 RETURN n.age",
        )
        .expect("UNION ALL with empty right must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "when right produces no rows, UNION ALL must return left-side rows, got {:?}",
        result.rows
    );

    // All returned values must be non-null integers.
    for row in &result.rows {
        assert!(
            matches!(row[0], Value::Int64(_)),
            "expected Int64 age values from left side, got {:?}",
            row[0]
        );
    }
}
