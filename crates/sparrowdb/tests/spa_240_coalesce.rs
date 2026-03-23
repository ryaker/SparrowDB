//! End-to-end tests for SPA-240: `coalesce(expr1, expr2, ...)`.
//!
//! `coalesce()` returns the first non-null value among its arguments.
//! Used heavily in LangChain schema queries and general Cypher applications.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// `coalesce(null_prop, non_null_prop, fallback)` returns the first non-null.
#[test]
fn coalesce_returns_first_non_null() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    let r = db
        .execute("MATCH (n:Person) RETURN coalesce(n.missing, n.name, 'default')")
        .expect("coalesce query must succeed");

    assert_eq!(r.rows.len(), 1, "expected one row");
    assert_eq!(
        r.rows[0][0],
        Value::String("Alice".into()),
        "coalesce should skip the missing property and return n.name"
    );
}

/// When all arguments are null, `coalesce()` returns null.
#[test]
fn coalesce_returns_null_when_all_null() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();

    let r = db
        .execute("MATCH (n:Person) RETURN coalesce(n.missing, n.also_missing)")
        .expect("coalesce with all-null args must succeed");

    assert_eq!(r.rows.len(), 1, "expected one row");
    assert_eq!(
        r.rows[0][0],
        Value::Null,
        "coalesce should return null when all arguments are null"
    );
}

/// `coalesce(null_prop, second_prop)` returns the second property when the first is missing.
#[test]
fn coalesce_with_two_props_returns_second() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {id: 'alice-123'})").unwrap();

    let r = db
        .execute("MATCH (n:Person) RETURN coalesce(n.name, n.id)")
        .expect("coalesce(n.name, n.id) must succeed");

    assert_eq!(r.rows.len(), 1, "expected one row");
    assert_eq!(
        r.rows[0][0],
        Value::String("alice-123".into()),
        "coalesce should fall through to n.id when n.name is missing"
    );
}

/// `coalesce()` with a literal string fallback returns that literal.
#[test]
fn coalesce_literal_fallback() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let r = db
        .execute("MATCH (n:Person) RETURN coalesce(n.nickname, 'no-nickname')")
        .expect("coalesce with literal fallback must succeed");

    assert_eq!(r.rows.len(), 1, "expected one row");
    assert_eq!(
        r.rows[0][0],
        Value::String("no-nickname".into()),
        "coalesce should return the literal fallback when the property is missing"
    );
}

/// `coalesce()` with a present first argument short-circuits immediately.
#[test]
fn coalesce_short_circuits_on_first_non_null() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Person {name: 'Carol', nickname: 'Caz'})")
        .unwrap();

    let r = db
        .execute("MATCH (n:Person) RETURN coalesce(n.name, n.nickname, 'fallback')")
        .expect("coalesce short-circuit must succeed");

    assert_eq!(r.rows.len(), 1, "expected one row");
    assert_eq!(
        r.rows[0][0],
        Value::String("Carol".into()),
        "coalesce should return the first non-null value without evaluating further args"
    );
}

/// `RETURN coalesce(...)` works without a MATCH clause (scalar context).
#[test]
fn coalesce_scalar_return() {
    let (_dir, db) = make_db();

    let r = db
        .execute("RETURN coalesce(null, 42)")
        .expect("RETURN coalesce(null, 42) must succeed");

    assert_eq!(r.rows.len(), 1, "expected one row");
    assert_eq!(
        r.rows[0][0],
        Value::Int64(42),
        "coalesce should return 42 when first arg is null"
    );
}
