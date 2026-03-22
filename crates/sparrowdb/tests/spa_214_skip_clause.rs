//! Regression tests for SKIP clause in RETURN statements (SPA-214).
//!
//! Cypher clause order: RETURN … [ORDER BY] [SKIP n] [LIMIT n]

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Helper to create 10 Person nodes with sequential age values 1..=10.
fn seed_persons(db: &sparrowdb::GraphDb) {
    for i in 1..=10i64 {
        db.execute(&format!(
            "CREATE (n:Person {{name: 'Person{i}', age: {i}}})"
        ))
        .unwrap_or_else(|e| panic!("seed failed for i={i}: {e}"));
    }
}

// ── SKIP 0 returns all rows ──────────────────────────────────────────────────

/// `SKIP 0` is a no-op: all rows must be returned.
#[test]
fn skip_zero_returns_all_rows() {
    let (_dir, db) = make_db();
    seed_persons(&db);

    let result = db
        .execute("MATCH (n:Person) RETURN n.age SKIP 0")
        .expect("SKIP 0 must not fail");

    assert_eq!(result.rows.len(), 10, "SKIP 0 should return all 10 rows");
}

// ── SKIP 2 LIMIT 3 returns rows 3,4,5 ───────────────────────────────────────

/// After ordering by age ascending, `SKIP 2 LIMIT 3` must return ages 3, 4, 5.
#[test]
fn skip_2_limit_3_returns_correct_slice() {
    let (_dir, db) = make_db();
    seed_persons(&db);

    let result = db
        .execute("MATCH (n:Person) RETURN n.age ORDER BY n.age ASC SKIP 2 LIMIT 3")
        .expect("SKIP 2 LIMIT 3 must succeed");

    assert_eq!(
        result.rows.len(),
        3,
        "SKIP 2 LIMIT 3 should return exactly 3 rows"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "first row should be age=3"
    );
    assert_eq!(
        result.rows[1][0],
        Value::Int64(4),
        "second row should be age=4"
    );
    assert_eq!(
        result.rows[2][0],
        Value::Int64(5),
        "third row should be age=5"
    );
}

// ── SKIP beyond result count returns 0 rows (no error) ───────────────────────

/// `SKIP 100` on a 5-row result must return an empty result without error.
#[test]
fn skip_beyond_result_count_returns_empty() {
    let (_dir, db) = make_db();

    for i in 1..=5i64 {
        db.execute(&format!("CREATE (n:Widget {{id: {i}}})"))
            .unwrap();
    }

    let result = db
        .execute("MATCH (n:Widget) RETURN n.id SKIP 100")
        .expect("SKIP beyond count must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "SKIP 100 on 5 rows should return 0 rows"
    );
}

// ── SKIP without LIMIT works ─────────────────────────────────────────────────

/// `SKIP n` without `LIMIT` must work on its own.
#[test]
fn skip_without_limit_works() {
    let (_dir, db) = make_db();
    seed_persons(&db);

    let result = db
        .execute("MATCH (n:Person) RETURN n.age ORDER BY n.age ASC SKIP 7")
        .expect("SKIP without LIMIT must succeed");

    // Ages 8, 9, 10 remain after skipping 7.
    assert_eq!(
        result.rows.len(),
        3,
        "SKIP 7 on 10 rows should leave 3 rows"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(8),
        "first remaining row should be age=8"
    );
}

// ── ORDER BY + SKIP + LIMIT combined ─────────────────────────────────────────

/// The full combination `ORDER BY x SKIP 2 LIMIT 3` must produce the
/// correct window from a sorted result set.
#[test]
fn order_by_skip_limit_combined() {
    let (_dir, db) = make_db();

    // Insert in scrambled order so ORDER BY is actually doing work.
    for &age in &[5i64, 2, 8, 1, 9, 3, 7, 4, 10, 6] {
        db.execute(&format!("CREATE (n:Item {{val: {age}}})"))
            .unwrap();
    }

    let result = db
        .execute("MATCH (n:Item) RETURN n.val ORDER BY n.val ASC SKIP 2 LIMIT 3")
        .expect("ORDER BY … SKIP … LIMIT must succeed");

    assert_eq!(result.rows.len(), 3);
    // Sorted ascending: 1, 2, 3, 4, 5, … — skip 2 → start at 3, take 3 → [3, 4, 5]
    assert_eq!(result.rows[0][0], Value::Int64(3));
    assert_eq!(result.rows[1][0], Value::Int64(4));
    assert_eq!(result.rows[2][0], Value::Int64(5));
}

// ── Exact reproduction of the bug report ─────────────────────────────────────

/// `MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 3` must not throw
/// "unexpected trailing token: Ident(\"SKIP\")".
#[test]
fn bug_report_query_does_not_error() {
    let (_dir, db) = make_db();
    seed_persons(&db);

    let result = db
        .execute("MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 3")
        .expect("bug-report query must not throw a parse error");

    assert_eq!(
        result.rows.len(),
        3,
        "SKIP 5 LIMIT 3 on 10 rows should return 3 rows"
    );
}
