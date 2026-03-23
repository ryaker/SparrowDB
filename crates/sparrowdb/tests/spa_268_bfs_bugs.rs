//! Regression tests for SPA-268: BFS variable-length path correctness bugs.
//!
//! Three bugs fixed in `execute_variable_hops`:
//!   Bug A — label filter always used src_label_id for all frontier depths
//!   Bug B — self-loop could insert src_slot back into results
//!   Bug C — min_hops == 0 never included the source node in results

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

// ── Bug A: heterogeneous graph traversal ──────────────────────────────────────

/// Graph: (a:A {name:'a1'})-[:R]->(b:B {name:'b1'})-[:R]->(c:C {name:'c1'})
///
/// `MATCH (a:A)-[:R*2]->(c:C) RETURN c.name` must find c1.
///
/// Before the fix, the depth-2 expansion of node b used A's label_id in the
/// delta-log filter, silently dropping all edges out of b (which has label B).
#[test]
fn bfs_bug_a_heterogeneous_graph() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:A {name: 'a1'})").unwrap();
    db.execute("CREATE (n:B {name: 'b1'})").unwrap();
    db.execute("CREATE (n:C {name: 'c1'})").unwrap();

    // A→B edge
    db.execute("MATCH (a:A {name: 'a1'}), (b:B {name: 'b1'}) CREATE (a)-[:R]->(b)")
        .unwrap();
    // B→C edge
    db.execute("MATCH (b:B {name: 'b1'}), (c:C {name: 'c1'}) CREATE (b)-[:R]->(c)")
        .unwrap();

    let result = db
        .execute("MATCH (a:A)-[:R*2]->(c:C) RETURN c.name")
        .expect("[:R*2] on heterogeneous graph must succeed");

    let names = col_strings(&result, 0);
    assert!(
        names.contains(&"c1".to_string()),
        "[:R*2] must reach c1 through heterogeneous labels A→B→C; got: {:?}",
        names
    );
}

/// Same graph but using *1..2 range: A should reach both b1 (depth 1) and c1 (depth 2).
#[test]
fn bfs_bug_a_heterogeneous_range() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:A {name: 'a1'})").unwrap();
    db.execute("CREATE (n:B {name: 'b1'})").unwrap();
    db.execute("CREATE (n:C {name: 'c1'})").unwrap();

    db.execute("MATCH (a:A {name: 'a1'}), (b:B {name: 'b1'}) CREATE (a)-[:R]->(b)")
        .unwrap();
    db.execute("MATCH (b:B {name: 'b1'}), (c:C {name: 'c1'}) CREATE (b)-[:R]->(c)")
        .unwrap();

    // Ask for depth 1..2 starting from A — must find b1 (depth 1) and c1 (depth 2).
    // (The dst label is not constrained in the query, so all dst nodes qualify.)
    let result = db
        .execute("MATCH (a:A {name: 'a1'})-[:R*1..2]->(x) RETURN x.name")
        .expect("[:R*1..2] on heterogeneous graph must succeed");

    let names = col_strings(&result, 0);
    assert!(
        names.contains(&"b1".to_string()),
        "[:R*1..2] must include b1 (depth 1); got: {:?}",
        names
    );
    assert!(
        names.contains(&"c1".to_string()),
        "[:R*1..2] must include c1 (depth 2) through heterogeneous A→B→C; got: {:?}",
        names
    );
}

// ── Bug B: self-loop must not return the source node ─────────────────────────

/// Graph: (a:A {name:'loop'})-[:SELF]->(a)   (self-loop)
///
/// `MATCH (a:A)-[:SELF*1]->(b:A) RETURN b.name` must NOT return 'loop' itself,
/// because a self-loop revisits the source which is already in `visited`.
#[test]
fn bfs_bug_b_self_loop_not_returned() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:A {name: 'loop'})").unwrap();

    // Create a self-loop: a→a
    db.execute("MATCH (a:A {name: 'loop'}) CREATE (a)-[:SELF]->(a)")
        .unwrap();

    let result = db
        .execute("MATCH (a:A {name: 'loop'})-[:SELF*1]->(b:A) RETURN b.name")
        .expect("[:SELF*1] self-loop query must succeed");

    let names = col_strings(&result, 0);
    assert!(
        !names.contains(&"loop".to_string()),
        "[:SELF*1] self-loop must NOT return the source node; got: {:?}",
        names
    );
}

// ── Bug C: zero-hop match must include the source node ───────────────────────

/// Graph: (a:A {name:'src'})-[:R]->(b:A {name:'dst'})
///
/// `MATCH (a:A {name:'src'})-[:R*0..2]->(b:A) RETURN b.name` must include
/// 'src' itself (zero hops) as well as 'dst' (one hop).
#[test]
fn bfs_bug_c_zero_hop_includes_source() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:A {name: 'src'})").unwrap();
    db.execute("CREATE (n:A {name: 'dst'})").unwrap();

    db.execute("MATCH (a:A {name: 'src'}), (b:A {name: 'dst'}) CREATE (a)-[:R]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (a:A {name: 'src'})-[:R*0..2]->(b:A) RETURN b.name")
        .expect("[:R*0..2] query must succeed");

    let names = col_strings(&result, 0);
    assert!(
        names.contains(&"src".to_string()),
        "[:R*0..2] must include 'src' (zero-hop match); got: {:?}",
        names
    );
    assert!(
        names.contains(&"dst".to_string()),
        "[:R*0..2] must include 'dst' (one-hop match); got: {:?}",
        names
    );
}

/// Isolated node with min_hops == 0: the node itself should appear.
#[test]
fn bfs_bug_c_zero_hop_isolated_node() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:A {name: 'alone'})").unwrap();

    let result = db
        .execute("MATCH (a:A {name: 'alone'})-[:R*0..1]->(b:A) RETURN b.name")
        .expect("[:R*0..1] on isolated node must succeed");

    let names = col_strings(&result, 0);
    assert!(
        names.contains(&"alone".to_string()),
        "[:R*0..1] on isolated node must include 'alone' (zero-hop); got: {:?}",
        names
    );
}
