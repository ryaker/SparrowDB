//! Path-semantics correctness tests for variable-length paths `*M..N` (SPA-PATH-SEMANTICS).
//!
//! OpenCypher requires **enumerative** (all simple paths) semantics, not
//! **existential** (reachable nodes) semantics.
//!
//! Key test cases:
//!   - Diamond:   A→B→D, A→C→D — `*2..2` from A must yield D **twice**.
//!   - Cycle:     A→B→C→A — `*1..4` must yield B and C, but NOT A again (no revisit).
//!   - Self-loop: A→A — `*1..2` must yield **nothing** (self-loop revisits A).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Count occurrences of a string value in column `col` of the result.
fn count_in_col(
    result: &sparrowdb_execution::types::QueryResult,
    col: usize,
    needle: &str,
) -> usize {
    result
        .rows
        .iter()
        .filter(|row| matches!(&row[col], Value::String(s) if s == needle))
        .count()
}

/// Collect all string values from column `col`.
fn col_strings(result: &sparrowdb_execution::types::QueryResult, col: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| match &row[col] {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ── 1. Diamond graph — D must appear TWICE (two distinct simple paths) ────────

/// Graph:
///   A -[:EDGE]-> B -[:EDGE]-> D
///   A -[:EDGE]-> C -[:EDGE]-> D
///
/// `MATCH (a:N {name:'A'})-[:EDGE*2..2]->(b:N) RETURN b.name`
/// Expected: 2 rows, both with name 'D' (one per simple path).
///
/// With global-visited BFS, D is only returned once (existential).
/// With per-path DFS, D appears twice (enumerative).
#[test]
fn diamond_path_d_appears_twice() {
    let (_dir, db) = make_db();

    for name in &["A", "B", "C", "D"] {
        db.execute(&format!("CREATE (n:N {{name: '{name}'}})"))
            .unwrap();
    }
    // A→B, A→C
    for (src, dst) in &[("A", "B"), ("A", "C")] {
        db.execute(&format!(
            "MATCH (a:N {{name: '{src}'}}), (b:N {{name: '{dst}'}}) CREATE (a)-[:EDGE]->(b)"
        ))
        .unwrap();
    }
    // B→D, C→D
    for (src, dst) in &[("B", "D"), ("C", "D")] {
        db.execute(&format!(
            "MATCH (a:N {{name: '{src}'}}), (b:N {{name: '{dst}'}}) CREATE (a)-[:EDGE]->(b)"
        ))
        .unwrap();
    }

    let result = db
        .execute("MATCH (a:N {name: 'A'})-[:EDGE*2..2]->(b:N) RETURN b.name")
        .expect("diamond *2..2 query must succeed");

    let d_count = count_in_col(&result, 0, "D");
    assert_eq!(
        d_count, 2,
        "D must appear exactly twice (one per simple path A→B→D and A→C→D); \
         got {} occurrences. Rows: {:?}",
        d_count, result.rows
    );
}

// ── 2. Cycle graph — must NOT revisit start node ──────────────────────────────

/// Graph:
///   A -[:HOP]-> B -[:HOP]-> C -[:HOP]-> A   (cycle)
///
/// `MATCH (a:Cyc {name:'A'})-[:HOP*1..4]->(b:Cyc) RETURN b.name`
/// Expected: B (depth 1) and C (depth 2). A must NOT appear (would revisit start).
///
/// A BFS without per-path visited allows the cycle to revisit A.
#[test]
fn cycle_does_not_revisit_start() {
    let (_dir, db) = make_db();

    for name in &["A", "B", "C"] {
        db.execute(&format!("CREATE (n:Cyc {{name: '{name}'}})"))
            .unwrap();
    }
    for (src, dst) in &[("A", "B"), ("B", "C"), ("C", "A")] {
        db.execute(&format!(
            "MATCH (a:Cyc {{name: '{src}'}}), (b:Cyc {{name: '{dst}'}}) CREATE (a)-[:HOP]->(b)"
        ))
        .unwrap();
    }

    let result = db
        .execute("MATCH (a:Cyc {name: 'A'})-[:HOP*1..4]->(b:Cyc) RETURN b.name")
        .expect("cycle *1..4 query must succeed");

    let mut names = col_strings(&result, 0);
    names.sort();

    // Exactly two results: B (depth 1) and C (depth 2). No duplicates, no A.
    assert_eq!(
        names,
        vec!["B".to_string(), "C".to_string()],
        "cycle *1..4 from A must yield exactly [B, C]; got: {names:?}"
    );
}

// ── 3. Self-loop — must return EMPTY ─────────────────────────────────────────

/// Graph:
///   A -[:SELF]-> A   (self-loop)
///
/// `MATCH (a:SL {name:'A'})-[:SELF*1..2]->(b:SL) RETURN b.name`
/// Expected: EMPTY — following the self-loop immediately revisits A (the source).
#[test]
fn self_loop_returns_empty() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:SL {name: 'A'})").unwrap();
    db.execute("MATCH (a:SL {name: 'A'}), (b:SL {name: 'A'}) CREATE (a)-[:SELF]->(b)")
        .unwrap();

    let result = db
        .execute("MATCH (a:SL {name: 'A'})-[:SELF*1..2]->(b:SL) RETURN b.name")
        .expect("self-loop *1..2 query must not error");

    assert!(
        result.rows.is_empty(),
        "self-loop must produce no results (A→A revisits A); got rows: {:?}",
        result.rows
    );
}
