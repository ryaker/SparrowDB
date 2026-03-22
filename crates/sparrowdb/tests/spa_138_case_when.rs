//! End-to-end tests for CASE WHEN expression (SPA-138).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// CASE WHEN returns the correct string based on age comparison.
#[test]
fn case_when_string_result() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 35})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name, \
             CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END AS category \
             ORDER BY n.name",
        )
        .expect("CASE WHEN query must not error");

    assert_eq!(result.rows.len(), 2, "expected two rows");
    // Alice (age 35) → senior
    assert_eq!(result.rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::String("senior".to_string()));
    // Bob (age 25) → junior
    assert_eq!(result.rows[1][0], Value::String("Bob".to_string()));
    assert_eq!(result.rows[1][1], Value::String("junior".to_string()));
}

/// CASE WHEN with multiple conditions picks the first matching branch.
#[test]
fn case_when_multiple_conditions() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 35})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Carol', age: 18})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name, \
             CASE WHEN n.age >= 30 THEN 'senior' \
                  WHEN n.age >= 21 THEN 'adult' \
                  ELSE 'young' END AS tier \
             ORDER BY n.name",
        )
        .expect("CASE WHEN multiple conditions must not error");

    assert_eq!(result.rows.len(), 3, "expected three rows");
    // Alice (35) → senior
    assert_eq!(result.rows[0][1], Value::String("senior".to_string()));
    // Bob (25) → adult
    assert_eq!(result.rows[1][1], Value::String("adult".to_string()));
    // Carol (18) → young
    assert_eq!(result.rows[2][1], Value::String("young".to_string()));
}

/// CASE WHEN with no matching branch and no ELSE returns NULL.
#[test]
fn case_when_no_else_returns_null() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 25})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name, \
             CASE WHEN n.age > 30 THEN 'senior' END AS category",
        )
        .expect("CASE WHEN no ELSE must not error");

    assert_eq!(result.rows.len(), 1, "expected one row");
    assert_eq!(result.rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(
        result.rows[0][1],
        Value::Null,
        "no matching branch and no ELSE should return NULL"
    );
}
