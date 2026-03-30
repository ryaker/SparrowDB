//! Regression tests for issue #363: CASE WHEN expression returns Null
//! instead of the evaluated branch result.
//!
//! The exact query from the bug report:
//! MATCH (n:Person) RETURN CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END AS tier

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Exact reproduction of issue #363: CASE WHEN with AS alias must not return Null.
#[test]
fn case_when_with_alias_returns_correct_branch() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice', age: 35})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END AS tier ORDER BY n.name",
        )
        .expect("CASE WHEN AS tier query must not error");

    assert_eq!(result.rows.len(), 2, "expected two rows");
    // Alice (age 35) → senior
    assert_ne!(
        result.rows[0][0],
        Value::Null,
        "tier for Alice (age 35) must not be Null — issue #363"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("senior".to_string()),
        "Alice (age 35) should be 'senior'"
    );
    // Bob (age 25) → junior
    assert_ne!(
        result.rows[1][0],
        Value::Null,
        "tier for Bob (age 25) must not be Null — issue #363"
    );
    assert_eq!(
        result.rows[1][0],
        Value::String("junior".to_string()),
        "Bob (age 25) should be 'junior'"
    );
}

/// CASE WHEN as the sole RETURN item (no other property columns, no ORDER BY).
#[test]
fn case_when_sole_return_item() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Charlie', age: 40})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END AS tier",
        )
        .expect("CASE WHEN sole return must not error");

    assert_eq!(result.rows.len(), 1, "expected one row");
    assert_eq!(
        result.rows[0][0],
        Value::String("senior".to_string()),
        "Charlie (age 40) should be 'senior' — issue #363"
    );
}

/// CASE WHEN with integer result values.
#[test]
fn case_when_integer_branch_result() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Dave', score: 90})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Eve', score: 50})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name, CASE WHEN n.score >= 75 THEN 1 ELSE 0 END AS passed ORDER BY n.name",
        )
        .expect("CASE WHEN integer branch must not error");

    assert_eq!(result.rows.len(), 2, "expected two rows");
    // Dave (score 90) → passed=1
    assert_eq!(
        result.rows[0][1],
        Value::Int64(1),
        "Dave (score 90) passed flag should be 1"
    );
    // Eve (score 50) → passed=0
    assert_eq!(
        result.rows[1][1],
        Value::Int64(0),
        "Eve (score 50) passed flag should be 0"
    );
}

/// CASE WHEN with ORDER BY on the CASE column — tests that ORDER BY is applied
/// when use_eval_path is true (SPA-138 eval path must not skip ORDER BY).
#[test]
fn case_when_order_by_applied() {
    let (_dir, db) = make_db();

    // Insert in intentionally reverse-alpha order to force ORDER BY to reorder.
    db.execute("CREATE (n:Person {name: 'Zara', age: 40})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Anna', age: 20})")
        .unwrap();
    db.execute("CREATE (n:Person {name: 'Mike', age: 35})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name, CASE WHEN n.age >= 30 THEN 'senior' ELSE 'junior' END AS tier ORDER BY n.name ASC",
        )
        .expect("CASE WHEN ORDER BY query must not error");

    assert_eq!(result.rows.len(), 3, "expected three rows");
    // ORDER BY n.name ASC should give Anna, Mike, Zara
    assert_eq!(
        result.rows[0][0],
        Value::String("Anna".to_string()),
        "first row should be Anna after ORDER BY n.name ASC"
    );
    assert_eq!(
        result.rows[0][1],
        Value::String("junior".to_string()),
        "Anna (age 20) should be junior"
    );
    assert_eq!(
        result.rows[1][0],
        Value::String("Mike".to_string()),
        "second row should be Mike after ORDER BY n.name ASC"
    );
    assert_eq!(
        result.rows[1][1],
        Value::String("senior".to_string()),
        "Mike (age 35) should be senior"
    );
    assert_eq!(
        result.rows[2][0],
        Value::String("Zara".to_string()),
        "third row should be Zara after ORDER BY n.name ASC"
    );
    assert_eq!(
        result.rows[2][1],
        Value::String("senior".to_string()),
        "Zara (age 40) should be senior"
    );
}

/// CASE WHEN with LIMIT — tests that LIMIT is applied when use_eval_path is true.
#[test]
fn case_when_with_limit() {
    let (_dir, db) = make_db();

    for i in 1..=5 {
        db.execute(&format!(
            "CREATE (n:Person {{name: 'Person{i}', age: {}}}) ",
            i * 10
        ))
        .unwrap();
    }

    let result = db
        .execute(
            "MATCH (n:Person) RETURN n.name, CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END AS tier LIMIT 2",
        )
        .expect("CASE WHEN LIMIT query must not error");

    assert_eq!(result.rows.len(), 2, "LIMIT 2 should return only 2 rows");
}
