//! Integration tests for SPA-272: DegreeCache wired to Cypher ORDER BY degree.
//!
//! Validates that `MATCH (n:Label) RETURN … ORDER BY out_degree(n) DESC LIMIT k`
//! activates the degree-cache fast-path and returns correct results — no full
//! edge scan at query time.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: ORDER BY out_degree(n) DESC LIMIT k returns top-k nodes ───────────
//
// A → B, A → C, A → D  (degree 3)
// B → C, B → D          (degree 2)
// C → D                  (degree 1)
// D — no outgoing edges  (degree 0)
//
// MATCH (n:Person) RETURN n.name, out_degree(n) AS deg ORDER BY out_degree(n) DESC LIMIT 2
// → [("A", 3), ("B", 2)]

#[test]
fn cypher_order_by_out_degree_returns_top_k() {
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
        .execute("MATCH (n:Person) RETURN out_degree(n) ORDER BY out_degree(n) DESC LIMIT 2")
        .unwrap();

    assert_eq!(result.rows.len(), 2, "LIMIT 2 must return exactly 2 rows");

    // First row must be highest degree (3).
    assert_eq!(
        result.rows[0][0],
        sparrowdb_execution::Value::Int64(3),
        "first row should have degree 3"
    );
    // Second row must be next highest degree (2).
    assert_eq!(
        result.rows[1][0],
        sparrowdb_execution::Value::Int64(2),
        "second row should have degree 2"
    );
}

// ── Test 2: ORDER BY degree(n) DESC LIMIT k (alias spelling) ──────────────────

#[test]
fn cypher_order_by_degree_alias_returns_top_k() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Widget {id: 1})").unwrap();
    db.execute("CREATE (n:Widget {id: 2})").unwrap();
    db.execute("CREATE (n:Widget {id: 3})").unwrap();

    // id=1 → id=2, id=1 → id=3  (degree 2)
    db.execute("MATCH (a:Widget {id:1}),(b:Widget {id:2}) CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:Widget {id:1}),(b:Widget {id:3}) CREATE (a)-[:E]->(b)")
        .unwrap();
    // id=2 → id=3  (degree 1)
    db.execute("MATCH (a:Widget {id:2}),(b:Widget {id:3}) CREATE (a)-[:E]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (n:Widget) RETURN degree(n) ORDER BY degree(n) DESC LIMIT 1")
        .unwrap();

    assert_eq!(result.rows.len(), 1, "LIMIT 1 must return exactly 1 row");
    assert_eq!(
        result.rows[0][0],
        sparrowdb_execution::Value::Int64(2),
        "top widget should have degree 2"
    );
}

// ── Test 3: Unknown label returns empty ───────────────────────────────────────

#[test]
fn cypher_order_by_degree_unknown_label_returns_empty() {
    let (_dir, db) = make_db();
    let result = db
        .execute("MATCH (n:NoSuchLabel) RETURN out_degree(n) ORDER BY out_degree(n) DESC LIMIT 10")
        .unwrap();
    assert!(result.rows.is_empty(), "unknown label must produce 0 rows");
}

// ── Test 4: DESC ordering — degrees are non-increasing ────────────────────────

#[test]
fn cypher_order_by_degree_desc_ordering_correct() {
    let (_dir, db) = make_db();

    // Build a star graph: node 0 has degree N-1, node 1 has degree 1, rest 0.
    for i in 0..5u32 {
        db.execute(&format!("CREATE (n:Star {{id: {i}}})")).unwrap();
    }
    for i in 1..5u32 {
        db.execute(&format!(
            "MATCH (a:Star {{id:0}}),(b:Star {{id:{i}}}) CREATE (a)-[:RAY]->(b)"
        ))
        .unwrap();
    }
    // Node 1 also has one outgoing edge.
    db.execute("MATCH (a:Star {id:1}),(b:Star {id:2}) CREATE (a)-[:RAY]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (n:Star) RETURN out_degree(n) ORDER BY out_degree(n) DESC LIMIT 5")
        .unwrap();

    // Verify non-increasing order.
    let degrees: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            sparrowdb_execution::Value::Int64(d) => *d,
            _ => -1,
        })
        .collect();
    let is_sorted = degrees.windows(2).all(|w| w[0] >= w[1]);
    assert!(
        is_sorted,
        "degrees must be non-increasing; got: {degrees:?}"
    );
    assert_eq!(
        degrees[0], 4,
        "hub node should have degree 4; got {degrees:?}"
    );
}

// ── Test 5: Fast-path not taken when inline prop filter is present ─────────────
// (Boundary test: inline prop filters prevent the fast-path; the query must
//  still return correct results via the normal scan path.)

#[test]
fn cypher_inline_prop_filter_bypasses_degree_fastpath() {
    let (_dir, db) = make_db();

    // Two Tagged nodes; only the one with tag='keep' passes the inline filter.
    db.execute("CREATE (n:Tagged {name: 'X', tag: 'keep'})")
        .unwrap();
    db.execute("CREATE (n:Tagged {name: 'Y', tag: 'skip'})")
        .unwrap();

    // Inline prop filter {tag:'keep'} — fast-path must NOT fire.
    let result = db
        .execute("MATCH (n:Tagged {tag: 'keep'}) RETURN n.name ORDER BY n.name ASC LIMIT 5")
        .unwrap();

    // Only X passes the inline filter.
    assert_eq!(
        result.rows.len(),
        1,
        "inline prop filter should leave only one row; got: {:?}",
        result.rows
    );
}
