use sparrowdb::open;

#[test]
fn debug_case_when() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    db.execute("CREATE (n:Person {name: 'Alice', age: 35})")
        .unwrap();

    let r1 = db.execute("MATCH (n:Person) RETURN n.age").unwrap();
    println!("n.age = {:?}", r1.rows);

    let r2 = db
        .execute("MATCH (n:Person) WHERE n.age > 30 RETURN n.name")
        .unwrap();
    println!("WHERE n.age > 30: {:?}", r2.rows);

    let r3 = db
        .execute("MATCH (n:Person) RETURN CASE WHEN n.age > 30 THEN 'senior' ELSE 'junior' END")
        .unwrap();
    println!("CASE WHEN: {:?}", r3.rows);
}
