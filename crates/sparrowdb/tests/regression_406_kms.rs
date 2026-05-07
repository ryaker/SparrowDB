//! Debug test using KMS DB path to reproduce issue #406 against a real DB.

use sparrowdb::open;

/// Test against the actual KMS DB if it exists (read-only).
#[test]
fn kms_db_anonymous_rel_count() {
    let kms_path = std::path::Path::new("/Users/ryaker/.kms-sparrowdb-v2");
    if !kms_path.exists() {
        println!("KMS DB not found, skipping");
        return;
    }

    let db = open(kms_path).expect("open KMS DB");

    // Labeled should work:
    let r1 = db
        .execute("MATCH (n:Knowledge) RETURN count(n)")
        .expect("labeled count must work");
    println!("Knowledge count: {:?}", r1.rows);

    // Non-aggregating single-hop (no rel var):
    let r2 = db.execute("MATCH (n:Knowledge)-->() RETURN count(n)");
    println!("non-rel-var hop: {:?}", r2.map(|r| r.rows));

    // Simple rel with labeled src (non-aggregating):
    let r3 = db.execute("MATCH (n:Knowledge)-[r]->() RETURN r LIMIT 1");
    println!("labeled-src rel LIMIT 1: {:?}", r3.map(|r| r.rows));

    // Try with chunked disabled by using aggregate:
    let r4 = db.execute("MATCH (n:Knowledge)-[r]->() RETURN count(r) AS cnt");
    println!("labeled-src count(r): {:?}", r4.map(|r| r.rows));

    // The regression:
    let r5 = db.execute("MATCH ()-[r]->() RETURN count(r) AS cnt");
    match r5 {
        Ok(result) => println!("count(r): {:?}", result.rows),
        Err(e) => panic!("MATCH ()-[r]->() failed: {e}"),
    }
}

/// Test with multiple rel tables.
#[test]
fn multi_rel_table_anonymous_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");

    db.execute("CREATE (a:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (b:Document {title: 'Doc1'})").unwrap();
    db.execute("CREATE (c:Person {name: 'Bob'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Document {title: 'Doc1'}) CREATE (a)-[:WROTE]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .unwrap();

    let result = db
        .execute("MATCH ()-[r]->() RETURN count(r) AS cnt")
        .expect("multi-rel-table anonymous count must not throw");
    println!("multi-rel-table count(r): {:?}", result.rows);

    use sparrowdb_execution::types::Value;
    assert_eq!(result.rows[0][0], Value::Int64(2), "expected 2 edges");
}
