//! Regression test: SPA-224 dst_all_col_ids fix must survive future PRs.
//! Uses non-__SO__ labels and a dst prop filter (the case PR #100 broke).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// MATCH (a:Person)-[:KNOWS*1..2]->(b:Person {name: 'Bob'}) RETURN b.name
/// Without dst_all_col_ids, the dst prop filter {name: 'Bob'} silently rejects
/// all candidates (the column isn't in col_ids_dst when b isn't in RETURN).
#[test]
fn varpath_dst_prop_filter_non_so_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS*1..2]->(b:Person {name: 'Bob'}) RETURN b.name")
        .expect("var-path with dst prop filter must not error");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("Bob".to_string()));
}
