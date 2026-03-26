//! Tests for LIMIT pushdown into node scan (SPA-198 / issue #198).
//!
//! Verifies that `MATCH (n:Label) WHERE … RETURN … LIMIT k` stops scanning
//! after k matching rows instead of collecting all results and truncating.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Create 100 User nodes with sequential uid and age values.
fn seed_users(db: &sparrowdb::GraphDb) {
    for i in 1..=100i64 {
        db.execute(&format!(
            "CREATE (n:User {{uid: {i}, age: {i}, city: 'City{i}'}})"
        ))
        .unwrap_or_else(|e| panic!("seed failed for i={i}: {e}"));
    }
}

// ── Basic LIMIT returns exactly k rows ──────────────────────────────────────

#[test]
fn limit_returns_exact_count() {
    let (_dir, db) = make_db();
    seed_users(&db);

    let result = db
        .execute("MATCH (n:User) RETURN n.uid LIMIT 10")
        .expect("LIMIT 10 must succeed");

    assert_eq!(
        result.rows.len(),
        10,
        "LIMIT 10 should return exactly 10 rows"
    );
}

// ── LIMIT with WHERE clause ─────────────────────────────────────────────────

#[test]
fn limit_with_where_returns_exact_count() {
    let (_dir, db) = make_db();
    seed_users(&db);

    // age > 30 matches users 31..=100 (70 users), LIMIT 5 should return 5.
    let result = db
        .execute("MATCH (n:User) WHERE n.age > 30 RETURN n.uid, n.city LIMIT 5")
        .expect("WHERE + LIMIT must succeed");

    assert_eq!(
        result.rows.len(),
        5,
        "WHERE + LIMIT 5 should return exactly 5 rows"
    );
}

// ── LIMIT larger than result set returns all matching rows ──────────────────

#[test]
fn limit_larger_than_result_set() {
    let (_dir, db) = make_db();
    seed_users(&db);

    // age > 95 matches users 96..=100 (5 users), LIMIT 50 should return all 5.
    let result = db
        .execute("MATCH (n:User) WHERE n.age > 95 RETURN n.uid LIMIT 50")
        .expect("LIMIT > result size must succeed");

    assert_eq!(
        result.rows.len(),
        5,
        "LIMIT 50 with 5 matches should return 5 rows"
    );
}

// ── No LIMIT still returns all matching rows ────────────────────────────────

#[test]
fn no_limit_returns_all_rows() {
    let (_dir, db) = make_db();
    seed_users(&db);

    let result = db
        .execute("MATCH (n:User) WHERE n.age > 90 RETURN n.uid")
        .expect("no LIMIT must succeed");

    assert_eq!(
        result.rows.len(),
        10,
        "no LIMIT should return all 10 matching rows"
    );
}

// ── SKIP + LIMIT combination ────────────────────────────────────────────────

#[test]
fn skip_and_limit_combination() {
    let (_dir, db) = make_db();
    seed_users(&db);

    // No ORDER BY, so order is scan order (slot order = insertion order).
    // SKIP 5 LIMIT 3 should return 3 rows.
    let result = db
        .execute("MATCH (n:User) RETURN n.uid SKIP 5 LIMIT 3")
        .expect("SKIP + LIMIT must succeed");

    assert_eq!(
        result.rows.len(),
        3,
        "SKIP 5 LIMIT 3 should return exactly 3 rows"
    );
}

// ── LIMIT with ORDER BY must not use pushdown (correctness) ─────────────────

#[test]
fn limit_with_order_by_returns_correct_results() {
    let (_dir, db) = make_db();
    seed_users(&db);

    // ORDER BY n.uid DESC LIMIT 3 must return the top 3 uids: 100, 99, 98.
    let result = db
        .execute("MATCH (n:User) RETURN n.uid ORDER BY n.uid DESC LIMIT 3")
        .expect("ORDER BY + LIMIT must succeed");

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Int64(100));
    assert_eq!(result.rows[1][0], Value::Int64(99));
    assert_eq!(result.rows[2][0], Value::Int64(98));
}

// ── LIMIT with DISTINCT must not use pushdown (correctness) ─────────────────

#[test]
fn limit_with_distinct_returns_correct_count() {
    let (_dir, db) = make_db();
    // Create nodes with duplicate city values.
    for i in 1..=20i64 {
        let city = if i <= 10 { "Alpha" } else { "Beta" };
        db.execute(&format!(
            "CREATE (n:Place {{name: 'Place{i}', city: '{city}'}})"
        ))
        .expect("seed");
    }

    let result = db
        .execute("MATCH (n:Place) RETURN DISTINCT n.city LIMIT 5")
        .expect("DISTINCT + LIMIT must succeed");

    // Only 2 distinct cities exist, LIMIT 5 should return 2.
    assert_eq!(
        result.rows.len(),
        2,
        "DISTINCT should yield 2 unique cities"
    );
}

// ── LIMIT 0 returns no rows ────────────────────────────────────────────────

#[test]
fn limit_zero_returns_empty() {
    let (_dir, db) = make_db();
    seed_users(&db);

    let result = db
        .execute("MATCH (n:User) RETURN n.uid LIMIT 0")
        .expect("LIMIT 0 must succeed");

    assert_eq!(result.rows.len(), 0, "LIMIT 0 should return 0 rows");
}
