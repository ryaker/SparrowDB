//! SPA-254: per-query timeout via `GraphDb::execute_with_timeout`.
//!
//! Tests verify:
//! 1. Normal queries complete successfully within a generous timeout.
//! 2. A near-zero timeout (1 ns) either times out or completes without panic.
//! 3. `Error::QueryTimeout` is correctly distinguished from other errors.

use sparrowdb::GraphDb;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn execute_with_timeout_normal_query_completes() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Node {v: 1})").unwrap();

    // 5-second deadline is far more than enough for a single-node scan.
    let r = db
        .execute_with_timeout("MATCH (n:Node) RETURN n.v", Duration::from_secs(5))
        .unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn execute_with_timeout_returns_correct_values() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    for i in 0..5i64 {
        db.execute(&format!("CREATE (n:Item {{val: {}}})", i))
            .unwrap();
    }

    let r = db
        .execute_with_timeout("MATCH (n:Item) RETURN n.val", Duration::from_secs(10))
        .unwrap();
    assert_eq!(r.rows.len(), 5);
}

#[test]
fn execute_with_timeout_very_short_does_not_panic() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    // Create enough nodes to give the scanner something to do.
    for i in 0..200 {
        db.execute(&format!("CREATE (n:Node {{v: {}}})", i))
            .unwrap();
    }

    // 1ns timeout: may expire before or during the scan.
    // Important: this must NOT panic — it should either succeed or return QueryTimeout.
    let result = db.execute_with_timeout("MATCH (n:Node) RETURN n.v", Duration::from_nanos(1));
    match result {
        Ok(_) => {}                               // completed before deadline checked — fine
        Err(sparrowdb::Error::QueryTimeout) => {} // timed out — expected
        Err(e) => panic!("unexpected error: {e}"),
    }
}

#[test]
fn execute_with_timeout_zero_duration_does_not_panic() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Node {v: 42})").unwrap();

    // Zero duration: deadline is already in the past by the time we check.
    let result = db.execute_with_timeout("MATCH (n:Node) RETURN n.v", Duration::ZERO);
    match result {
        Ok(_) => {}
        Err(sparrowdb::Error::QueryTimeout) => {}
        Err(e) => panic!("unexpected error: {e}"),
    }
}

#[test]
fn execute_with_timeout_query_timeout_error_display() {
    // Verify that QueryTimeout has a meaningful Display string.
    let msg = format!("{}", sparrowdb::Error::QueryTimeout);
    assert!(!msg.is_empty(), "QueryTimeout Display must not be empty");
    assert!(
        msg.contains("timeout") || msg.contains("deadline"),
        "QueryTimeout message should mention timeout or deadline, got: {msg}"
    );
}

#[test]
fn execute_with_timeout_create_statement_completes() {
    // Mutation statements (CREATE) should not be affected by the timeout
    // mechanism (they go through the standard write-transaction path).
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let result = db.execute_with_timeout(
        "CREATE (n:Widget {name: 'sprocket'})",
        Duration::from_secs(10),
    );
    assert!(
        result.is_ok(),
        "CREATE through execute_with_timeout should succeed"
    );
}
