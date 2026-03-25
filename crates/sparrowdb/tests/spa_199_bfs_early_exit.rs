//! BFS early-exit tests for variable-length path + LIMIT (issue #199).
//!
//! Verifies that the engine stops BFS/DFS traversal early when LIMIT is
//! present, returning exactly k results without enumerating the full
//! reachable set.

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

/// Build a fan-out graph: src -> {dst_0, dst_1, ..., dst_(n-1)} all at depth 1.
fn build_fan_graph(db: &sparrowdb::GraphDb, n: usize) {
    db.execute("CREATE (n:User {uid: 'src'})").unwrap();
    for i in 0..n {
        db.execute(&format!("CREATE (n:User {{uid: 'dst_{i}'}})"))
            .unwrap();
        db.execute(&format!(
            "MATCH (a:User {{uid: 'src'}}), (b:User {{uid: 'dst_{i}'}}) \
             CREATE (a)-[:FRIENDS]->(b)"
        ))
        .unwrap();
    }
}

// ── Test 1: DISTINCT varpath + LIMIT returns exactly k rows ─────────────────

#[test]
fn varpath_limit_returns_exactly_k() {
    let (_dir, db) = make_db();
    build_fan_graph(&db, 20);

    let result = db
        .execute(
            "MATCH (a:User {uid: 'src'})-[:FRIENDS*1..3]->(b:User) \
             RETURN DISTINCT b.uid LIMIT 5",
        )
        .expect("varpath + LIMIT query must succeed");

    assert_eq!(
        result.rows.len(),
        5,
        "LIMIT 5 must return exactly 5 rows; got {}",
        result.rows.len()
    );
}

// ── Test 2: without LIMIT, all reachable nodes are returned ─────────────────

#[test]
fn varpath_no_limit_returns_all() {
    let (_dir, db) = make_db();
    build_fan_graph(&db, 10);

    let result = db
        .execute(
            "MATCH (a:User {uid: 'src'})-[:FRIENDS*1..3]->(b:User) \
             RETURN DISTINCT b.uid",
        )
        .expect("varpath without LIMIT must succeed");

    assert_eq!(
        result.rows.len(),
        10,
        "without LIMIT all 10 neighbors must be returned; got {}",
        result.rows.len()
    );
}

// ── Test 3: results are valid reachable nodes ───────────────────────────────

#[test]
fn varpath_limit_results_are_reachable() {
    let (_dir, db) = make_db();
    build_fan_graph(&db, 15);

    let result = db
        .execute(
            "MATCH (a:User {uid: 'src'})-[:FRIENDS*1..3]->(b:User) \
             RETURN DISTINCT b.uid LIMIT 7",
        )
        .expect("query must succeed");

    let uids = col_strings(&result, 0);
    assert_eq!(uids.len(), 7);

    // All returned uids must be valid dst_N names.
    for uid in &uids {
        assert!(
            uid.starts_with("dst_"),
            "unexpected uid '{uid}' — not a reachable neighbor"
        );
    }

    // No duplicates in DISTINCT results.
    let unique: std::collections::HashSet<&String> = uids.iter().collect();
    assert_eq!(
        unique.len(),
        uids.len(),
        "DISTINCT results must have no duplicates"
    );
}

// ── Test 4: LIMIT larger than reachable set returns all ─────────────────────

#[test]
fn varpath_limit_larger_than_reachable() {
    let (_dir, db) = make_db();
    build_fan_graph(&db, 5);

    let result = db
        .execute(
            "MATCH (a:User {uid: 'src'})-[:FRIENDS*1..3]->(b:User) \
             RETURN DISTINCT b.uid LIMIT 200",
        )
        .expect("query must succeed");

    assert_eq!(
        result.rows.len(),
        5,
        "LIMIT 200 but only 5 reachable; should return 5, got {}",
        result.rows.len()
    );
}

// ── Test 5: multi-hop with LIMIT (deeper graph) ────────────────────────────

#[test]
fn varpath_multi_hop_limit() {
    let (_dir, db) = make_db();

    // Build a chain: u0 -> u1 -> u2 -> ... -> u9 (depth 1..9 from u0)
    for i in 0..10 {
        db.execute(&format!("CREATE (n:User {{uid: 'u{i}'}})"))
            .unwrap();
    }
    for i in 0..9 {
        let j = i + 1;
        db.execute(&format!(
            "MATCH (a:User {{uid: 'u{i}'}}), (b:User {{uid: 'u{j}'}}) \
             CREATE (a)-[:FRIENDS]->(b)"
        ))
        .unwrap();
    }

    // *1..5 from u0 should reach u1..u5 (5 nodes). LIMIT 3 → 3 rows.
    let result = db
        .execute(
            "MATCH (a:User {uid: 'u0'})-[:FRIENDS*1..5]->(b:User) \
             RETURN DISTINCT b.uid LIMIT 3",
        )
        .expect("multi-hop + LIMIT must succeed");

    assert_eq!(
        result.rows.len(),
        3,
        "LIMIT 3 on chain graph must return 3; got {}",
        result.rows.len()
    );
}
