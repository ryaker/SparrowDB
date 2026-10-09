//! #557 / #552: `MATCH ... OPTIONAL MATCH <pattern>` must left-join the optional
//! pattern on EVERY variable the leading MATCH already bound, wherever that
//! variable sits in the optional path (source, destination, or middle), and for
//! optional paths of any length.
//!
//! Every expected value is derived BY HAND from the fixtures below.
//!
//! Fixture S (small, from #557): T A,B,C; P P1,P2; P1->A, P2->A, P1->B  (:HAS)
//!   in-degree of T: A=2 (P1,P2), B=1 (P1), C=0
//!   out-degree of P: P1=2 (A,B), P2=1 (A)
//!
//! Fixture L (from #552): P1,P2,P3; T A,B,C,D; K X,Y
//!   (:HAS) (P1,C)(P1,B)(P2,B)(P3,B)(P2,A)(P3,C)(P1,A)
//!   (:IN)  (A,Y)(B,X)(C,X)          D has no :IN, no :HAS
//!   HAS targets: P1={A,B,C}  P2={A,B}  P3={B,C}
//!   HAS sources: A={P1,P2}  B={P1,P2,P3}  C={P1,P3}  D={}

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn i(v: i64) -> Value {
    Value::Int64(v)
}
fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

fn run(db: &sparrowdb::GraphDb, q: &str) -> Vec<Vec<Value>> {
    db.execute(q)
        .unwrap_or_else(|e| panic!("query failed: {q}: {e}"))
        .rows
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

fn fixture_s() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for t in ["A", "B", "C"] {
        db.execute(&format!("CREATE (:T {{name:'{t}'}})")).unwrap();
    }
    for p in ["P1", "P2"] {
        db.execute(&format!("CREATE (:P {{name:'{p}'}})")).unwrap();
    }
    for (p, t) in [("P1", "A"), ("P2", "A"), ("P1", "B")] {
        db.execute(&format!(
            "MATCH (p:P {{name:'{p}'}}),(t:T {{name:'{t}'}}) CREATE (p)-[:HAS]->(t)"
        ))
        .unwrap();
    }
    (dir, db)
}

fn fixture_l() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for id in 1..=3 {
        db.execute(&format!("CREATE (:P {{id:{id}}})")).unwrap();
    }
    for t in ["A", "B", "C", "D"] {
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

#[test]
fn bound_dest_count_is_in_degree() {
    let (_d, db) = fixture_s();
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (p:P)-[:HAS]->(t) RETURN t.name, COUNT(p) ORDER BY t.name",
    );
    assert_eq!(
        rows,
        vec![vec![s("A"), i(2)], vec![s("B"), i(1)], vec![s("C"), i(0)]]
    );
}

#[test]
fn bound_dest_raw_rows_null_for_unmatched() {
    let (_d, db) = fixture_s();
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (p:P)-[:HAS]->(t) RETURN t.name, p.name",
    );
    assert_eq!(
        sorted(rows),
        sorted(vec![
            vec![s("A"), s("P1")],
            vec![s("A"), s("P2")],
            vec![s("B"), s("P1")],
            vec![s("C"), Value::Null],
        ])
    );
}

#[test]
fn bound_dest_collect_and_count_prop() {
    let (_d, db) = fixture_s();
    // collect(p.name) per t: A -> [P1,P2], B -> [P1], C -> [] (order inside a
    // list is unspecified, so sort it before comparing).
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (p:P)-[:HAS]->(t) RETURN t.name, collect(p.name) ORDER BY t.name",
    );
    let got: Vec<(Value, Vec<Value>)> = rows
        .into_iter()
        .map(|mut r| {
            let list = match r.pop().unwrap() {
                Value::List(mut l) => {
                    l.sort_by_key(|v| format!("{v:?}"));
                    l
                }
                other => panic!("expected list, got {other:?}"),
            };
            (r.pop().unwrap(), list)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (s("A"), vec![s("P1"), s("P2")]),
            (s("B"), vec![s("P1")]),
            (s("C"), vec![]),
        ]
    );
    // COUNT over a property: NULL (unmatched) is not counted.
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (p:P)-[:HAS]->(t) RETURN t.name, COUNT(p.name) AS c ORDER BY c DESC, t.name",
    );
    assert_eq!(
        rows,
        vec![vec![s("A"), i(2)], vec![s("B"), i(1)], vec![s("C"), i(0)]]
    );
}

#[test]
fn bound_src_still_works() {
    let (_d, db) = fixture_s();
    let rows = run(
        &db,
        "MATCH (p:P) OPTIONAL MATCH (p)-[:HAS]->(t:T) RETURN p.name, COUNT(t) ORDER BY p.name",
    );
    assert_eq!(rows, vec![vec![s("P1"), i(2)], vec![s("P2"), i(1)]]);
}

#[test]
fn bound_dest_with_inline_filter_on_other_end() {
    let (_d, db) = fixture_s();
    // Only P2 qualifies: A has 1 match, B and C have none.
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (p:P {name:'P2'})-[:HAS]->(t) RETURN t.name, COUNT(p) ORDER BY t.name",
    );
    assert_eq!(
        rows,
        vec![vec![s("A"), i(1)], vec![s("B"), i(0)], vec![s("C"), i(0)]]
    );
}

#[test]
fn both_ends_bound() {
    let (_d, db) = fixture_s();
    // MATCH (p),(t) gives 2x3 = 6 rows; the optional path joins on BOTH ends
    // and counts the other P nodes pointing at t, but only if p -> t exists.
    // (P1,A): edge exists, A's sources {P1,P2} => 2 ; (P1,B): edge, B's {P1} => 1
    // (P2,A): edge, {P1,P2} => 2 ; (P1,C),(P2,B),(P2,C): no p -> t edge => 0
    let rows = run(
        &db,
        "MATCH (p:P),(t:T) OPTIONAL MATCH (p)-[:HAS]->(t)<-[:HAS]-(q:P) RETURN p.name, t.name, COUNT(q) ORDER BY p.name, t.name",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("P1"), s("A"), i(2)],
            vec![s("P1"), s("B"), i(1)],
            vec![s("P1"), s("C"), i(0)],
            vec![s("P2"), s("A"), i(2)],
            vec![s("P2"), s("B"), i(0)],
            vec![s("P2"), s("C"), i(0)],
        ]
    );
}

#[test]
fn multi_hop_bound_src_issue_552() {
    let (_d, db) = fixture_l();
    // P1 -> A,B,C -> Y,X,X = 3 ; P2 -> B,A -> X,Y = 2 ; P3 -> B,C -> X,X = 2
    let rows = run(
        &db,
        "MATCH (p:P) OPTIONAL MATCH (p)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN p.id, COUNT(k) AS c ORDER BY c DESC, p.id ASC",
    );
    assert_eq!(
        rows,
        vec![vec![i(1), i(3)], vec![i(2), i(2)], vec![i(3), i(2)]]
    );
}

#[test]
fn multi_hop_bound_last_node() {
    let (_d, db) = fixture_l();
    // X <- B (P1,P2,P3) and C (P1,P3) = 5 ; Y <- A (P1,P2) = 2
    let rows = run(
        &db,
        "MATCH (k:K) OPTIONAL MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k) RETURN k.name, COUNT(p) ORDER BY k.name",
    );
    assert_eq!(rows, vec![vec![s("X"), i(5)], vec![s("Y"), i(2)]]);
}

#[test]
fn multi_hop_bound_middle_node() {
    let (_d, db) = fixture_l();
    // A: HAS-sources {P1,P2}, A -IN-> Y => 2 ; B: {P1,P2,P3} => 3 ;
    // C: {P1,P3} => 2 ; D: no sources => 0
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (p:P)-[:HAS]->(t)-[:IN]->(k:K) RETURN t.name, COUNT(p) ORDER BY t.name",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("A"), i(2)],
            vec![s("B"), i(3)],
            vec![s("C"), i(2)],
            vec![s("D"), i(0)]
        ]
    );
}

#[test]
fn multi_hop_unmatched_rows_are_null() {
    let (_d, db) = fixture_l();
    // D has no :IN edge: its k is NULL, but D still appears once.
    let rows = run(
        &db,
        "MATCH (t:T) OPTIONAL MATCH (t)-[:IN]->(k:K) RETURN t.name, k.name",
    );
    assert_eq!(
        sorted(rows),
        sorted(vec![
            vec![s("A"), s("Y")],
            vec![s("B"), s("X")],
            vec![s("C"), s("X")],
            vec![s("D"), Value::Null],
        ])
    );
}
