/// SPA-229: Storing a float literal as an edge property panicked at runtime:
///   "Value::Float cannot be inline-encoded; use NodeStore::encode_value"
///
/// Root cause: WriteTx::create_edge called `val.to_u64()` which panics for
/// Value::Float.  The fix uses `NodeStore::encode_value()` (same path as node
/// properties) which writes float bits to the heap and returns a tagged u64.
use sparrowdb::GraphDb;
use tempfile::tempdir;

/// Basic float edge property round-trip (the exact scenario from the bug report).
#[test]
fn edge_float_prop_roundtrip() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:Knowledge {id: 'a'})").unwrap();
    db.execute("CREATE (b:Knowledge {id: 'b'})").unwrap();
    db.execute(
        "MATCH (a:Knowledge {id: 'a'}), (b:Knowledge {id: 'b'}) \
         CREATE (a)-[:RELATED_TO {strength: 0.85}]->(b)",
    )
    .unwrap();

    let r = db
        .execute("MATCH (a:Knowledge)-[r:RELATED_TO]->(b:Knowledge) RETURN r.strength")
        .unwrap();

    assert_eq!(r.rows.len(), 1, "expected one edge row");
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - 0.85_f64).abs() < 1e-10,
                "strength round-trip failed: got {f}"
            );
        }
        other => panic!("Expected Float64 for r.strength, got {:?}", other),
    }
}

/// Multiple float props on a single edge.
#[test]
fn edge_multiple_float_props() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:Person {name: 'alice'})").unwrap();
    db.execute("CREATE (b:Person {name: 'bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) \
         CREATE (a)-[:KNOWS {weight: 0.8, score: -1.5}]->(b)",
    )
    .unwrap();

    let r = db
        .execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.weight, r.score")
        .unwrap();

    assert_eq!(r.rows.len(), 1, "expected one row");
    let row = &r.rows[0];

    match &row[0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - 0.8_f64).abs() < 1e-10,
                "weight round-trip failed: got {f}"
            );
        }
        other => panic!("Expected Float64 for r.weight, got {:?}", other),
    }
    match &row[1] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - (-1.5_f64)).abs() < 1e-10,
                "score round-trip failed: got {f}"
            );
        }
        other => panic!("Expected Float64 for r.score, got {:?}", other),
    }
}

/// Storing 0.0 as an edge property must not panic.
#[test]
fn edge_float_zero_does_not_panic() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (x:Node {id: 'x'})").unwrap();
    db.execute("CREATE (y:Node {id: 'y'})").unwrap();
    // Must not panic — SPA-229 guard.
    db.execute(
        "MATCH (x:Node {id: 'x'}), (y:Node {id: 'y'}) \
         CREATE (x)-[:LINK {weight: 0.0}]->(y)",
    )
    .unwrap();
}

/// Mixed int and float edge properties on the same edge.
#[test]
fn edge_mixed_int_and_float_props() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (u:User {id: 'u1'})").unwrap();
    db.execute("CREATE (v:User {id: 'v1'})").unwrap();
    db.execute(
        "MATCH (u:User {id: 'u1'}), (v:User {id: 'v1'}) \
         CREATE (u)-[:FOLLOWS {since: 2020, strength: 0.75}]->(v)",
    )
    .unwrap();

    let r = db
        .execute("MATCH (u:User)-[r:FOLLOWS]->(v:User) RETURN r.since, r.strength")
        .unwrap();

    assert_eq!(r.rows.len(), 1);
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Int64(n) => assert_eq!(*n, 2020, "since mismatch"),
        other => panic!("Expected Int64 for r.since, got {:?}", other),
    }
    match &r.rows[0][1] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - 0.75_f64).abs() < 1e-10,
                "strength round-trip failed: got {f}"
            );
        }
        other => panic!("Expected Float64 for r.strength, got {:?}", other),
    }
}
