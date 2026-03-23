/// SPA-233: MERGE relationship pattern `(a)-[r:TYPE]->(b)`.
///
/// Tests that `MATCH … MERGE (a)-[r:TYPE]->(b)` finds or creates a relationship
/// between matched nodes, and that running the same MERGE twice is idempotent.
use sparrowdb::GraphDb;
use tempfile::tempdir;

#[test]
fn merge_creates_relationship_when_absent() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (k:Knowledge {id: 'k1', title: 'Rust'})")
        .unwrap();
    db.execute("CREATE (e:Entity {id: 'e1', name: 'Rust lang'})")
        .unwrap();
    db.execute("MATCH (k:Knowledge {id:'k1'}),(e:Entity {id:'e1'}) MERGE (k)-[r:ABOUT]->(e)")
        .unwrap();
    let r = db
        .execute("MATCH (k:Knowledge)-[:ABOUT]->(e:Entity) RETURN e.name")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn merge_is_idempotent_for_existing_relationship() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (a:A {id: '1'})").unwrap();
    db.execute("CREATE (b:B {id: '2'})").unwrap();
    db.execute("MATCH (a:A),(b:B) MERGE (a)-[:LINKS]->(b)")
        .unwrap();
    db.execute("MATCH (a:A),(b:B) MERGE (a)-[:LINKS]->(b)")
        .unwrap(); // second MERGE
    let r = db.execute("MATCH ()-[:LINKS]->() RETURN 1").unwrap();
    assert_eq!(
        r.rows.len(),
        1,
        "Exactly one LINKS relationship — MERGE is idempotent"
    );
}
