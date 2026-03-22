use sparrowdb::open;
use sparrowdb_execution::types::Value;

#[test]
fn debug_case_when() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    db.execute("CREATE (n:Person {name: 'Alice', age: 35})")
        .unwrap();

    let r1 = db.execute("MATCH (n:Person) RETURN n.age").unwrap();
    assert_eq!(r1.rows.len(), 1, "expected one row");
    assert_eq!(r1.rows[0][0], Value::Int64(35), "expected age 35");

    let r2 = db
        .execute("MATCH (n:Person) WHERE n.age > 30 RETURN n.name")
        .unwrap();
    assert_eq!(
        r2.rows.len(),
        1,
        "expected one row matching WHERE n.age > 30"
    );
    assert_eq!(
        r2.rows[0][0],
        Value::String("Alice".to_string()),
        "expected name Alice"
    );

    let r3 = db
        .execute("MATCH (n:Person) RETURN CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END")
        .unwrap();
    assert_eq!(r3.rows.len(), 1, "expected one row");
    assert_eq!(
        r3.rows[0][0],
        Value::String("senior".to_string()),
        "expected CASE WHEN result 'senior' for age 35"
    );
}
