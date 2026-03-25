//! Integration tests for SPA-272 / Q7: COUNT(f) AS deg + ORDER BY deg DESC LIMIT k
//! fastpath via DegreeCache.
//!
//! Validates that `MATCH (n:L)-[:R]->(f:L2) RETURN n.prop, COUNT(f) AS deg
//! ORDER BY deg DESC LIMIT k` activates the degree-cache fast-path and returns
//! correct results — no full edge scan + group + aggregate at query time.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: Basic Q7 shape — top-k by COUNT(f) DESC ─────────────────────────
//
// A → B, A → C, A → D  (degree 3)
// B → C, B → D          (degree 2)
// C → D                  (degree 1)
// D — no outgoing edges  (degree 0)
//
// MATCH (n:Person)-[:KNOWS]->(f:Person)
// RETURN n.name, COUNT(f) AS deg ORDER BY deg DESC LIMIT 2
// → [("A", 3), ("B", 2)]

#[test]
fn q7_count_f_order_by_alias_desc_limit() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'A'})").unwrap();
    db.execute("CREATE (n:Person {name: 'B'})").unwrap();
    db.execute("CREATE (n:Person {name: 'C'})").unwrap();
    db.execute("CREATE (n:Person {name: 'D'})").unwrap();

    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'B'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'C'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'D'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'B'}),(b:Person {name:'C'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'B'}),(b:Person {name:'D'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person {name:'C'}),(b:Person {name:'D'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person)-[:KNOWS]->(f:Person) \
             RETURN n.name, COUNT(f) AS deg \
             ORDER BY deg DESC LIMIT 2",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2, "LIMIT 2 must return exactly 2 rows");

    // Column 0 = n.name, Column 1 = deg (COUNT)
    assert_eq!(
        result.rows[0][1],
        sparrowdb_execution::Value::Int64(3),
        "first row should have degree 3"
    );
    assert_eq!(
        result.rows[1][1],
        sparrowdb_execution::Value::Int64(2),
        "second row should have degree 2"
    );

    // Verify the names correspond to the right degrees.
    assert_eq!(
        result.rows[0][0],
        sparrowdb_execution::Value::String("A".to_string()),
        "first row should be node A"
    );
    assert_eq!(
        result.rows[1][0],
        sparrowdb_execution::Value::String("B".to_string()),
        "second row should be node B"
    );
}

// ── Test 2: Exact Q7 query shape with uid property ──────────────────────────

#[test]
fn q7_exact_facebook_query_shape() {
    let (_dir, db) = make_db();

    for i in 0..5u32 {
        db.execute(&format!("CREATE (n:User {{uid: {i}}})"))
            .unwrap();
    }

    // uid=0 has 4 friends, uid=1 has 2, uid=2 has 1
    for i in 1..5u32 {
        db.execute(&format!(
            "MATCH (a:User {{uid:0}}),(b:User {{uid:{i}}}) CREATE (a)-[:FRIENDS]->(b)"
        ))
        .unwrap();
    }
    db.execute("MATCH (a:User {uid:1}),(b:User {uid:2}) CREATE (a)-[:FRIENDS]->(b)")
        .unwrap();
    db.execute("MATCH (a:User {uid:1}),(b:User {uid:3}) CREATE (a)-[:FRIENDS]->(b)")
        .unwrap();
    db.execute("MATCH (a:User {uid:2}),(b:User {uid:4}) CREATE (a)-[:FRIENDS]->(b)")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:User)-[:FRIENDS]->(f:User) \
             RETURN n.uid, COUNT(f) AS deg \
             ORDER BY deg DESC LIMIT 10",
        )
        .unwrap();

    // uid=0: 4 friends, uid=1: 2 friends, uid=2: 1 friend
    // uid=3, uid=4: 0 friends (not returned by 1-hop match)
    assert_eq!(result.rows.len(), 3, "3 nodes have outgoing FRIENDS edges");

    // Verify descending degree order.
    let degrees: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match &row[1] {
            sparrowdb_execution::Value::Int64(d) => *d,
            _ => -1,
        })
        .collect();
    assert_eq!(degrees, vec![4, 2, 1], "degrees must be [4, 2, 1]");
}

// ── Test 3: Unknown label returns empty ─────────────────────────────────────

#[test]
fn q7_count_fastpath_unknown_label_returns_empty() {
    let (_dir, db) = make_db();
    let result = db
        .execute(
            "MATCH (n:NoSuchLabel)-[:R]->(f:NoSuchLabel) \
             RETURN n.name, COUNT(f) AS deg \
             ORDER BY deg DESC LIMIT 10",
        )
        .unwrap();
    assert!(result.rows.is_empty(), "unknown label must produce 0 rows");
}

// ── Test 4: Fastpath not taken with WHERE clause ────────────────────────────

#[test]
fn q7_count_with_where_falls_through_to_normal_path() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'A'})").unwrap();
    db.execute("CREATE (n:Person {name: 'B'})").unwrap();
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'B'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    // WHERE clause should prevent fastpath — but still return correct results.
    let result = db
        .execute(
            "MATCH (n:Person)-[:KNOWS]->(f:Person) \
             WHERE n.name = 'A' \
             RETURN n.name, COUNT(f) AS deg \
             ORDER BY deg DESC LIMIT 10",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], sparrowdb_execution::Value::Int64(1),);
}

// ── Test 5: Column order preserved ──────────────────────────────────────────

#[test]
fn q7_count_column_names_correct() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'X'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Y'})").unwrap();
    db.execute("MATCH (a:Person {name:'X'}),(b:Person {name:'Y'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person)-[:KNOWS]->(f:Person) \
             RETURN n.name, COUNT(f) AS deg \
             ORDER BY deg DESC LIMIT 5",
        )
        .unwrap();

    assert_eq!(result.columns, vec!["n.name", "deg"]);
}
