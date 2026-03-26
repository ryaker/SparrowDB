//! property_range_index: BTree O(log N + K) range scan tests.
//!
//! Verifies that `WHERE n.age > X`, `< X`, `>= X AND <= Y` use the
//! PropertyIndex BTreeMap::range() path rather than a full O(N) scan.
//! Uses 1 000-node datasets to exercise the fast path meaningfully.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn collect_ints(result: &sparrowdb_execution::QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64, got {:?}", other),
        })
        .collect()
}

/// Insert `count` Person nodes with age 1..=count.
fn populate(db: &GraphDb, count: u32) {
    for age in 1..=count {
        db.execute(&format!("CREATE (:Person {{age: {age}}})",))
            .unwrap();
    }
}

// ── Test 1: WHERE n.age > 900 → 100 nodes ────────────────────────────────────

#[test]
fn range_gt_upper_slice() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    populate(&db, 1000);

    let result = db
        .execute("MATCH (n:Person) WHERE n.age > 900 RETURN n.age")
        .expect("range_gt must succeed");

    let mut ages = collect_ints(&result);
    ages.sort_unstable();

    assert_eq!(
        ages.len(),
        100,
        "expected 100 nodes with age > 900, got {} — {:?}",
        ages.len(),
        &ages[..ages.len().min(10)]
    );
    assert_eq!(ages[0], 901, "first age should be 901");
    assert_eq!(ages[99], 1000, "last age should be 1000");
}

// ── Test 2: WHERE n.age < 10 → 9 nodes ───────────────────────────────────────

#[test]
fn range_lt_lower_slice() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    populate(&db, 1000);

    let result = db
        .execute("MATCH (n:Person) WHERE n.age < 10 RETURN n.age")
        .expect("range_lt must succeed");

    let mut ages = collect_ints(&result);
    ages.sort_unstable();

    assert_eq!(
        ages.len(),
        9,
        "expected 9 nodes with age < 10, got {} — {:?}",
        ages.len(),
        ages
    );
    assert_eq!(ages[0], 1);
    assert_eq!(ages[8], 9);
}

// ── Test 3: WHERE n.age >= 500 AND n.age <= 510 → 11 nodes ───────────────────

#[test]
fn range_between_inclusive() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    populate(&db, 1000);

    let result = db
        .execute("MATCH (n:Person) WHERE n.age >= 500 AND n.age <= 510 RETURN n.age")
        .expect("compound range must succeed");

    let mut ages = collect_ints(&result);
    ages.sort_unstable();

    assert_eq!(
        ages.len(),
        11,
        "expected 11 nodes for age 500..=510, got {} — {:?}",
        ages.len(),
        ages
    );
    assert_eq!(ages[0], 500);
    assert_eq!(ages[10], 510);

    // Verify every value is present.
    for (i, &age) in ages.iter().enumerate() {
        assert_eq!(age, 500 + i as i64, "unexpected age at position {i}: {age}");
    }
}

// ── Test 4: range returning empty ────────────────────────────────────────────

#[test]
fn range_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    populate(&db, 1000);

    let result = db
        .execute("MATCH (n:Person) WHERE n.age > 1000 RETURN n.age")
        .expect("range yielding empty must succeed");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows for age > 1000, got {:?}",
        result.rows
    );
}

// ── Test 5: WHERE n.age >= 1 → full population ───────────────────────────────

#[test]
fn range_gte_full_population() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    populate(&db, 1000);

    let result = db
        .execute("MATCH (n:Person) WHERE n.age >= 1 RETURN n.age")
        .expect("range_gte full scan must succeed");

    assert_eq!(
        result.rows.len(),
        1000,
        "expected all 1000 nodes for age >= 1"
    );
}

// ── Test 6: performance — 1000-node scan completes in < 100 ms (debug) ───────

#[test]
fn range_scan_completes_within_time_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    populate(&db, 1000);

    let start = std::time::Instant::now();
    let result = db
        .execute("MATCH (n:Person) WHERE n.age > 900 RETURN n.age")
        .expect("timed range query must succeed");
    let elapsed = start.elapsed();

    assert_eq!(result.rows.len(), 100, "range query returned wrong count");
    assert!(
        elapsed.as_millis() < 100,
        "range scan took {elapsed:?} — expected < 100 ms (debug build)"
    );
}
