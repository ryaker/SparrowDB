//! Regression tests for #543: ORDER BY / SKIP / LIMIT were silently ignored
//! when a relationship MATCH (1-hop and 2-hop) returned a grouping key plus an
//! aggregate, because those executors returned straight after aggregation
//! instead of running the shared post-processing step.
//!
//! All expected values are derived by hand from the fixture below.
//!
//! Fixture
//!   P1{g:x} P2{g:y} P3{g:y};  T A, B, C;  K X, Y
//!   (p)-[:HAS]->(t): (1,C) (1,B) (2,B) (3,B) (2,A) (3,C) (1,A)
//!   (t)-[:IN]->(k):  (A,Y) (B,X) (C,X)
//!
//! Hand-derived per-T: COUNT(*): A=2 (p2,p1)  B=3 (p1,p2,p3)  C=2 (p1,p3)
//!                     SUM(p.id): A=3  B=6  C=4
//!                     MIN(p.id): A=1  B=1  C=1
//!                     MAX(p.id): A=2  B=3  C=3
//! Per-K two-hop COUNT(*): X = B3 + C2 = 5,  Y = A2 = 2
//! Per-g single node COUNT(*): x=1, y=2

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

fn fixture() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for (i, g) in [(1, "x"), (2, "y"), (3, "y")] {
        db.execute(&format!("CREATE (:P {{id:{i}, g:'{g}'}})"))
            .unwrap();
    }
    for t in ["A", "B", "C"] {
        db.execute(&format!("CREATE (:T {{name:'{t}'}})")).unwrap();
    }
    for k in ["X", "Y"] {
        db.execute(&format!("CREATE (:K {{name:'{k}'}})")).unwrap();
    }
    for (p, t) in [
        (1, "C"),
        (1, "B"),
        (2, "B"),
        (3, "B"),
        (2, "A"),
        (3, "C"),
        (1, "A"),
    ] {
        db.execute(&format!(
            "MATCH (p:P {{id:{p}}}),(t:T {{name:'{t}'}}) CREATE (p)-[:HAS]->(t)"
        ))
        .unwrap();
    }
    for (t, k) in [("A", "Y"), ("B", "X"), ("C", "X")] {
        db.execute(&format!(
            "MATCH (t:T {{name:'{t}'}}),(k:K {{name:'{k}'}}) CREATE (t)-[:IN]->(k)"
        ))
        .unwrap();
    }
    (dir, db)
}

fn pairs(v: &[(&str, i64)]) -> Vec<Vec<Value>> {
    v.iter()
        .map(|(n, c)| vec![s(n), Value::Int64(*c)])
        .collect()
}

#[test]
fn order_by_aggregate_matrix_543() {
    let (_d, db) = fixture();
    let h = "MATCH (p:P)-[:HAS]->(t:T) ";
    let cases: Vec<(String, Vec<Vec<Value>>)> = vec![
        // ---- 1-hop: COUNT(*) --------------------------------------------
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY cnt DESC, t.name ASC"),
            pairs(&[("B", 3), ("A", 2), ("C", 2)]),
        ),
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY cnt ASC, t.name ASC"),
            pairs(&[("A", 2), ("C", 2), ("B", 3)]),
        ),
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY t.name DESC"),
            pairs(&[("C", 2), ("B", 3), ("A", 2)]),
        ),
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY cnt DESC, t.name ASC LIMIT 2"),
            pairs(&[("B", 3), ("A", 2)]),
        ),
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY cnt DESC, t.name ASC SKIP 1 LIMIT 1"),
            pairs(&[("A", 2)]),
        ),
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY cnt DESC, t.name ASC SKIP 1"),
            pairs(&[("A", 2), ("C", 2)]),
        ),
        // ORDER BY the aggregate expression rather than its alias
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY COUNT(*) DESC, t.name ASC"),
            pairs(&[("B", 3), ("A", 2), ("C", 2)]),
        ),
        (
            format!("{h}RETURN t.name, COUNT(*) AS cnt ORDER BY t.name ASC"),
            pairs(&[("A", 2), ("B", 3), ("C", 2)]),
        ),
        (
            format!("{h}RETURN t.name AS n, COUNT(*) AS cnt ORDER BY n ASC"),
            pairs(&[("A", 2), ("B", 3), ("C", 2)]),
        ),
        (
            format!("{h}RETURN t.name, MIN(p.id) AS m ORDER BY m ASC, t.name ASC"),
            pairs(&[("A", 1), ("B", 1), ("C", 1)]),
        ),
        // aliased grouping key
        (
            format!("{h}RETURN t.name AS n, COUNT(*) AS cnt ORDER BY n DESC"),
            pairs(&[("C", 2), ("B", 3), ("A", 2)]),
        ),
        // DISTINCT over grouped rows
        (
            format!("{h}RETURN DISTINCT t.name, COUNT(*) AS cnt ORDER BY cnt DESC, t.name ASC"),
            pairs(&[("B", 3), ("A", 2), ("C", 2)]),
        ),
        // ---- 1-hop: COUNT(x), SUM, MIN, MAX ------------------------------
        (
            format!("{h}RETURN t.name, COUNT(p.id) AS cnt ORDER BY cnt DESC, t.name ASC"),
            pairs(&[("B", 3), ("A", 2), ("C", 2)]),
        ),
        (
            format!("{h}RETURN t.name, SUM(p.id) AS tot ORDER BY tot DESC"),
            pairs(&[("B", 6), ("C", 4), ("A", 3)]),
        ),
        (
            format!("{h}RETURN t.name, MIN(p.id) AS m ORDER BY m ASC, t.name DESC"),
            pairs(&[("C", 1), ("B", 1), ("A", 1)]),
        ),
        (
            format!("{h}RETURN t.name, MAX(p.id) AS m ORDER BY m DESC, t.name ASC"),
            pairs(&[("B", 3), ("C", 3), ("A", 2)]),
        ),
        // ---- 2-hop -------------------------------------------------------
        (
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN k.name, COUNT(*) AS cnt ORDER BY cnt DESC"
                .into(),
            pairs(&[("X", 5), ("Y", 2)]),
        ),
        (
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN k.name, COUNT(*) AS cnt ORDER BY cnt ASC"
                .into(),
            pairs(&[("Y", 2), ("X", 5)]),
        ),
        (
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN k.name, COUNT(*) AS cnt ORDER BY cnt ASC LIMIT 1"
                .into(),
            pairs(&[("Y", 2)]),
        ),
        (
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN k.name, COUNT(*) AS cnt ORDER BY cnt DESC LIMIT 1"
                .into(),
            pairs(&[("X", 5)]),
        ),
        // ---- variable length ---------------------------------------------
        (
            "MATCH (p:P)-[:HAS*1..2]->(t:T) RETURN t.name, COUNT(*) AS cnt ORDER BY cnt DESC, t.name ASC LIMIT 2"
                .into(),
            pairs(&[("B", 3), ("A", 2)]),
        ),
        (
            "MATCH (p:P)-[:HAS*1..2]->(t:T) RETURN t.name, COUNT(*) AS cnt ORDER BY cnt ASC, t.name ASC"
                .into(),
            pairs(&[("A", 2), ("C", 2), ("B", 3)]),
        ),
        // ---- single-node scan (already correct before the fix) --------------------------------------------
        (
            "MATCH (p:P) RETURN p.g, COUNT(*) AS cnt ORDER BY cnt DESC".into(),
            pairs(&[("y", 2), ("x", 1)]),
        ),
        (
            "MATCH (p:P) RETURN p.g, COUNT(*) AS cnt ORDER BY p.g DESC".into(),
            pairs(&[("y", 2), ("x", 1)]),
        ),
    ];

    let mut failures = Vec::new();
    for (q, want) in &cases {
        match db.execute(q) {
            Ok(r) if &r.rows == want => {}
            Ok(r) => failures.push(format!(
                "WRONG  {q}\n   want {want:?}\n   got  {:?}",
                r.rows
            )),
            Err(e) => failures.push(format!("ERROR  {q}\n   {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} cells wrong:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}
