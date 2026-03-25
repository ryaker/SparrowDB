//! SPA-201 — Incoming-edge support in execute_two_hop (mutual friends / Q8).
//!
//! Verifies that `MATCH (a:User)-[:R]->(m:User)<-[:R]-(b:User) RETURN m`
//! (the Q8 "mutual friends" pattern) returns correct results when the second
//! relationship hop uses `EdgeDir::Incoming`.
//!
//! Before the fix, `execute_two_hop` used `AspJoin::two_hop` (forward-forward)
//! for all two-hop patterns, ignoring `rels[1].dir`.  For incoming second hops
//! this produced wrong results on directed graphs.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Helper: collect string values from the first column of a result.
fn collect_strings(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .filter_map(|row| match row.first() {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ── Test 1: basic mutual friend ───────────────────────────────────────────────
//
// Graph:  A -[:FRIENDS]-> M <-[:FRIENDS]- B
//
// Query:  MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'})
//         RETURN m.name
//
// Expected: exactly ["M"]

#[test]
fn mutual_friends_basic() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'A'})").expect("CREATE A");
    db.execute("CREATE (:User {name: 'M'})").expect("CREATE M");
    db.execute("CREATE (:User {name: 'B'})").expect("CREATE B");

    db.execute(
        "MATCH (a:User {name:'A'}), (m:User {name:'M'}) CREATE (a)-[:FRIENDS]->(m)",
    )
    .expect("edge A->M");
    db.execute(
        "MATCH (b:User {name:'B'}), (m:User {name:'M'}) CREATE (b)-[:FRIENDS]->(m)",
    )
    .expect("edge B->M");

    let result = db
        .execute(
            "MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'}) \
             RETURN m.name",
        )
        .expect("mutual friends query");

    let names = collect_strings(&result.rows);
    assert_eq!(
        names,
        vec!["M"],
        "SPA-201: mutual friends basic — expected [M], got {:?}",
        names
    );
}

// ── Test 2: no common neighbor ────────────────────────────────────────────────
//
// Graph:  A -[:FRIENDS]-> M1
//         B -[:FRIENDS]-> M2
//
// Query: MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'})
//        RETURN m.name
//
// Expected: empty (no shared m)

#[test]
fn mutual_friends_no_common() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'A'})").expect("CREATE A");
    db.execute("CREATE (:User {name: 'B'})").expect("CREATE B");
    db.execute("CREATE (:User {name: 'M1'})").expect("CREATE M1");
    db.execute("CREATE (:User {name: 'M2'})").expect("CREATE M2");

    db.execute(
        "MATCH (a:User {name:'A'}), (m:User {name:'M1'}) CREATE (a)-[:FRIENDS]->(m)",
    )
    .expect("edge A->M1");
    db.execute(
        "MATCH (b:User {name:'B'}), (m:User {name:'M2'}) CREATE (b)-[:FRIENDS]->(m)",
    )
    .expect("edge B->M2");

    let result = db
        .execute(
            "MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'}) \
             RETURN m.name",
        )
        .expect("no common query");

    assert!(
        result.rows.is_empty(),
        "SPA-201: no common neighbor — expected empty, got {:?}",
        result.rows
    );
}

// ── Test 3: multiple common neighbors ────────────────────────────────────────
//
// Graph:  A -> M1 <- B
//         A -> M2 <- B
//         A -> M3 <- B
//
// Expected: 3 rows [M1, M2, M3] (order may vary)

#[test]
fn mutual_friends_multiple() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'A'})").expect("CREATE A");
    db.execute("CREATE (:User {name: 'B'})").expect("CREATE B");
    db.execute("CREATE (:User {name: 'M1'})").expect("CREATE M1");
    db.execute("CREATE (:User {name: 'M2'})").expect("CREATE M2");
    db.execute("CREATE (:User {name: 'M3'})").expect("CREATE M3");

    for m in ["M1", "M2", "M3"] {
        db.execute(&format!(
            "MATCH (a:User {{name:'A'}}), (m:User {{name:'{m}'}}) CREATE (a)-[:FRIENDS]->(m)"
        ))
        .expect("edge A->M");
        db.execute(&format!(
            "MATCH (b:User {{name:'B'}}), (m:User {{name:'{m}'}}) CREATE (b)-[:FRIENDS]->(m)"
        ))
        .expect("edge B->M");
    }

    let result = db
        .execute(
            "MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'}) \
             RETURN m.name",
        )
        .expect("multiple mutual friends query");

    let mut names = collect_strings(&result.rows);
    names.sort();
    assert_eq!(
        names,
        vec!["M1", "M2", "M3"],
        "SPA-201: multiple mutual friends — expected [M1,M2,M3], got {:?}",
        names
    );
}

// ── Test 4: directed vs undirected correctness ────────────────────────────────
//
// For bidirectional edges (A->M and M->A, B->M and M->B), the incoming second
// hop `<-[:FRIENDS]-` should find nodes that have an outgoing edge TO m.
// On this fully bidirectional graph, (a)->(m)<-(b) should still find M as the
// common neighbor when B has an edge TO M.

#[test]
fn mutual_friends_directed_vs_undirected() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'A'})").expect("CREATE A");
    db.execute("CREATE (:User {name: 'M'})").expect("CREATE M");
    db.execute("CREATE (:User {name: 'B'})").expect("CREATE B");

    // Bidirectional edges: A<->M and B<->M
    db.execute(
        "MATCH (a:User {name:'A'}), (m:User {name:'M'}) CREATE (a)-[:FRIENDS]->(m)",
    )
    .expect("A->M");
    db.execute(
        "MATCH (m:User {name:'M'}), (a:User {name:'A'}) CREATE (m)-[:FRIENDS]->(a)",
    )
    .expect("M->A");
    db.execute(
        "MATCH (b:User {name:'B'}), (m:User {name:'M'}) CREATE (b)-[:FRIENDS]->(m)",
    )
    .expect("B->M");
    db.execute(
        "MATCH (m:User {name:'M'}), (b:User {name:'B'}) CREATE (m)-[:FRIENDS]->(b)",
    )
    .expect("M->B");

    // Query: (a)-[:FRIENDS]->(m)<-[:FRIENDS]-(b) — second hop is Incoming
    // This looks for nodes that A points TO and that B also points TO.
    // B->M exists, so M should be in the result.
    let result = db
        .execute(
            "MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'}) \
             RETURN m.name",
        )
        .expect("bidirectional mutual friends query");

    let names = collect_strings(&result.rows);
    assert!(
        names.contains(&"M".to_string()),
        "SPA-201: bidirectional graph — M must be a mutual friend, got {:?}",
        names
    );
}

// ── Test 5: empty anchor — anchor has no outgoing edges ───────────────────────
//
// Graph: A (no outgoing edges), B -> M
//
// Expected: empty (A has no first-hop neighbors)

#[test]
fn mutual_friends_empty_anchor() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:User {name: 'A'})").expect("CREATE A");
    db.execute("CREATE (:User {name: 'B'})").expect("CREATE B");
    db.execute("CREATE (:User {name: 'M'})").expect("CREATE M");

    // Only B -> M; A has no outgoing edges.
    db.execute(
        "MATCH (b:User {name:'B'}), (m:User {name:'M'}) CREATE (b)-[:FRIENDS]->(m)",
    )
    .expect("edge B->M");

    let result = db
        .execute(
            "MATCH (a:User {name:'A'})-[:FRIENDS]->(m:User)<-[:FRIENDS]-(b:User {name:'B'}) \
             RETURN m.name",
        )
        .expect("empty anchor query");

    assert!(
        result.rows.is_empty(),
        "SPA-201: empty anchor — expected empty result, got {:?}",
        result.rows
    );
}
