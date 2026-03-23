//! SPA-266 — MATCH on nonexistent label returns [] instead of error.
//! SPA-265 — Property names matching aggregate/keyword tokens parse correctly.

use sparrowdb::{open, GraphDb};

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// SPA-266: MATCH on a label that has never been created must return 0 rows,
/// not an InvalidArgument error.
#[test]
fn match_nonexistent_label_returns_empty() {
    let (_dir, db) = make_db();
    let r = db.execute("MATCH (n:Ghost) RETURN n").unwrap();
    assert_eq!(r.rows.len(), 0, "expected 0 rows for unknown label Ghost");
}

/// SPA-266: The empty-result guarantee should hold for any unrecognised label,
/// not just during a plain MATCH scan.
#[test]
fn match_nonexistent_label_with_props_filter_returns_empty() {
    let (_dir, db) = make_db();
    let r = db
        .execute("MATCH (n:Ghost {name: \"Casper\"}) RETURN n")
        .unwrap();
    assert_eq!(r.rows.len(), 0);
}

/// SPA-265: A property named `count` must round-trip through CREATE → MATCH.
#[test]
fn property_named_count_roundtrips() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Metric {count: 42, sum: 100})")
        .unwrap();
    let r = db
        .execute("MATCH (n:Metric) RETURN n.count, n.sum")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "expected 1 row");
    // Verify the values are correct.
    use sparrowdb_execution::types::Value;
    let row = &r.rows[0];
    assert!(
        row.contains(&Value::Int64(42)),
        "expected count=42 in row, got {:?}",
        row
    );
    assert!(
        row.contains(&Value::Int64(100)),
        "expected sum=100 in row, got {:?}",
        row
    );
}

/// SPA-265: Using COUNT as a map key in a CREATE literal must parse correctly.
#[test]
fn create_with_count_property_parses() {
    let (_dir, db) = make_db();
    // This would previously fail at the lexer/parser because `count` was
    // tokenised as Token::Count rather than Token::Ident.
    db.execute("CREATE (n:Stats {count: 7})").unwrap();
    let r = db.execute("MATCH (n:Stats) RETURN n.count").unwrap();
    assert_eq!(r.rows.len(), 1);
}
