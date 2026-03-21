//! Variable-length path matching tests.
//!
//! Covers `[:R*]`, `[:R*N]`, and `[:R*M..N]` syntax in MATCH patterns.
//! Each test builds a real graph on disk via Cypher CREATE and MATCH…CREATE,
//! then verifies BFS traversal correctness.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Helper: collect a single column of strings from a query result.
fn col_strings(result: &sparrowdb_execution::types::QueryResult, col: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| match &row[col] {
            sparrowdb_execution::types::Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ── Test 1: [:KNOWS*1] behaves exactly like [:KNOWS] (direct neighbors) ───────

/// A→B: `[:KNOWS*1]` must return B (depth-1 only, same as plain `[:KNOWS]`).
#[test]
fn var_path_star_one_hop() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS*1]->(b:Person) RETURN a.name, b.name")
        .expect("[:KNOWS*1] query must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "[:KNOWS*1] must return exactly 1 row (direct hop); got: {:?}",
        result.rows
    );

    let src_names = col_strings(&result, 0);
    let dst_names = col_strings(&result, 1);
    assert!(src_names.contains(&"Alice".to_string()), "src should be Alice");
    assert!(dst_names.contains(&"Bob".to_string()), "dst should be Bob");
}

// ── Test 2: [:KNOWS*2] returns friends-of-friends ─────────────────────────────

/// Chain: A→B→C.  `[:KNOWS*2]` from A must return C (depth-2 only, not B).
#[test]
fn var_path_star_two_hops() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(b:Person) RETURN b.name")
        .expect("[:KNOWS*2] query must succeed");

    let dst_names = col_strings(&result, 0);
    assert!(
        dst_names.contains(&"Carol".to_string()),
        "[:KNOWS*2] must reach Carol (depth 2); got: {:?}",
        dst_names
    );
    assert!(
        !dst_names.contains(&"Bob".to_string()),
        "[:KNOWS*2] must NOT include Bob (depth 1); got: {:?}",
        dst_names
    );
}

// ── Test 3: [:KNOWS*1..2] returns both depth-1 and depth-2 nodes ───────────────

/// Chain: A→B→C.  `[:KNOWS*1..2]` must return both B and C.
#[test]
fn var_path_range() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name")
        .expect("[:KNOWS*1..2] query must succeed");

    let dst_names = col_strings(&result, 0);
    assert!(
        dst_names.contains(&"Bob".to_string()),
        "[:KNOWS*1..2] must include Bob (depth 1); got: {:?}",
        dst_names
    );
    assert!(
        dst_names.contains(&"Carol".to_string()),
        "[:KNOWS*1..2] must include Carol (depth 2); got: {:?}",
        dst_names
    );
}

// ── Test 4: [:KNOWS*2] does NOT return depth-1 nodes ─────────────────────────

/// Chain: A→B→C.  `[:KNOWS*2]` from A must include C but NOT B.
#[test]
fn var_path_exact() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(b:Person) RETURN b.name")
        .expect("[:KNOWS*2] query must succeed");

    let dst_names = col_strings(&result, 0);

    assert!(
        !dst_names.contains(&"Bob".to_string()),
        "[:KNOWS*2] (exact 2 hops) must NOT return Bob (depth 1); got: {:?}",
        dst_names
    );
    assert!(
        dst_names.contains(&"Carol".to_string()),
        "[:KNOWS*2] must return Carol (depth 2); got: {:?}",
        dst_names
    );
}

// ── Test 5: [:KNOWS*] traverses full chain ────────────────────────────────────

/// Chain: A→B→C→D.  `[:KNOWS*]` from A must return B, C, and D.
#[test]
fn var_path_unbounded() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Dave'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Carol'}), (b:Person {name: 'Dave'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person) RETURN b.name")
        .expect("[:KNOWS*] query must succeed");

    let dst_names = col_strings(&result, 0);

    assert!(
        dst_names.contains(&"Bob".to_string()),
        "[:KNOWS*] must include Bob (depth 1); got: {:?}",
        dst_names
    );
    assert!(
        dst_names.contains(&"Carol".to_string()),
        "[:KNOWS*] must include Carol (depth 2); got: {:?}",
        dst_names
    );
    assert!(
        dst_names.contains(&"Dave".to_string()),
        "[:KNOWS*] must include Dave (depth 3); got: {:?}",
        dst_names
    );
}
