//! Matrix sweep: aggregate x relationship-pattern shape x ORDER BY / SKIP /
//! LIMIT / DISTINCT. Follow-up to #543, which lived for months because no test
//! crossed aggregates with relationship-pattern shapes.
//!
//! Every expected value is derived BY HAND from the fixture below and the
//! openCypher semantics; none was captured from engine output.
//!
//! Fixture
//!   P1{g:x} P2{g:y} P3{g:y};  T A, B, C, D (D is isolated);  K X, Y;  Z;
//!   Q1 Q2 Q3 (label Q, prop i)
//!   (p)-[:HAS]->(t): (1,C) (1,B) (2,B) (3,B) (2,A) (3,C) (1,A)   -- 7 edges
//!   (t)-[:IN]->(k):  (A,Y) (B,X) (C,X)
//!   (k)-[:OF]->(z):  (X,Z) (Y,Z)
//!   (q)-[:N]->(q):   (1,2) (2,3)
//!
//! Derived facts
//!   per-T COUNT(*):  A=2 (p2,p1)  B=3 (p1,p2,p3)  C=2 (p1,p3)   [D has none]
//!   per-T SUM(p.id): A=3 B=6 C=4;  MAX(p.id): A=2 B=3 C=3
//!   per-P distinct T: p1=3 (A,B,C)  p2=2 (A,B)  p3=2 (B,C)
//!   per-K 2/3-hop COUNT(*): X=5 (B3+C2)  Y=2 (A2);  total 7
//!   N*1..2 paths: 1->2, 2->3, 1->3(len 2)  => 3 paths
//!     by start: q1=2 q2=1;  by end: q2=1 q3=2
//!   N undirected 1-hop (each edge both ways): q1=1 q2=2 q3=1, total 4

use sparrowdb::open;
use sparrowdb_execution::types::Value;

type Rows = Vec<Vec<Value>>;

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}
fn i(v: i64) -> Value {
    Value::Int64(v)
}
fn n() -> Value {
    Value::Null
}
fn l(v: Vec<Value>) -> Value {
    Value::List(v)
}

fn fixture() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for (id, g) in [(1, "x"), (2, "y"), (3, "y")] {
        db.execute(&format!("CREATE (:P {{id:{id}, g:'{g}'}})"))
            .unwrap();
    }
    for t in ["A", "B", "C", "D"] {
        db.execute(&format!("CREATE (:T {{name:'{t}'}})")).unwrap();
    }
    for k in ["X", "Y"] {
        db.execute(&format!("CREATE (:K {{name:'{k}'}})")).unwrap();
    }
    db.execute("CREATE (:Z {name:'Z'})").unwrap();
    for q in 1..=3 {
        db.execute(&format!("CREATE (:Q {{i:{q}}})")).unwrap();
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
    for k in ["X", "Y"] {
        db.execute(&format!(
            "MATCH (k:K {{name:'{k}'}}),(z:Z) CREATE (k)-[:OF]->(z)"
        ))
        .unwrap();
    }
    for (a, b) in [(1, 2), (2, 3)] {
        db.execute(&format!(
            "MATCH (a:Q {{i:{a}}}),(b:Q {{i:{b}}}) CREATE (a)-[:N]->(b)"
        ))
        .unwrap();
    }
    (dir, db)
}

fn check(cells: Vec<(&str, String, Rows)>) {
    let (_d, db) = fixture();
    let mut failures = Vec::new();
    for (name, q, want) in &cells {
        match db.execute(q) {
            Ok(r) if &r.rows == want => {}
            Ok(r) => failures.push(format!(
                "WRONG  [{name}] {q}\n   want {want:?}\n   got  {:?}",
                r.rows
            )),
            Err(e) => failures.push(format!("ERROR  [{name}] {q}\n   {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} cells wrong:\n{}",
        failures.len(),
        cells.len(),
        failures.join("\n")
    );
}

fn c(name: &'static str, q: &str, want: Rows) -> (&'static str, String, Rows) {
    (name, q.to_string(), want)
}

const H: &str = "MATCH (p:P)-[:HAS]->(t:T) ";

/// Global (no group key) aggregates over an input that matches nothing must
/// still return exactly one row.
#[test]
fn empty_input_global_aggregates() {
    let shapes: Vec<(&str, &str)> = vec![
        ("node", "MATCH (p:P) WHERE p.id > 99 "),
        ("1hop", "MATCH (p:P)-[:HAS]->(t:T) WHERE t.name = 'D' "),
        (
            "2hop",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) WHERE t.name = 'D' ",
        ),
        (
            "3hop",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K)-[:OF]->(z:Z) WHERE t.name = 'D' ",
        ),
        ("varlen", "MATCH (a:Q)-[:N*1..2]->(b:Q) WHERE b.i > 99 "),
        ("varlen_in", "MATCH (a:Q)<-[:N*1..2]-(b:Q) WHERE b.i > 99 "),
        ("undirected", "MATCH (a:Q)-[:N]-(b:Q) WHERE b.i > 99 "),
    ];
    let mut cells = Vec::new();
    for (shape, m) in &shapes {
        let x = match *shape {
            "node" | "1hop" | "2hop" | "3hop" => "p.id",
            _ => "a.i",
        };
        let leak: &'static str = Box::leak(format!("empty/{shape}/COUNT(*)").into_boxed_str());
        cells.push((leak, format!("{m}RETURN COUNT(*) AS v"), vec![vec![i(0)]]));
        let leak: &'static str = Box::leak(format!("empty/{shape}/COUNT(x)").into_boxed_str());
        cells.push((leak, format!("{m}RETURN COUNT({x}) AS v"), vec![vec![i(0)]]));
        let leak: &'static str = Box::leak(format!("empty/{shape}/MIN").into_boxed_str());
        cells.push((leak, format!("{m}RETURN MIN({x}) AS v"), vec![vec![n()]]));
        let leak: &'static str = Box::leak(format!("empty/{shape}/MAX").into_boxed_str());
        cells.push((leak, format!("{m}RETURN MAX({x}) AS v"), vec![vec![n()]]));
        let leak: &'static str = Box::leak(format!("empty/{shape}/collect").into_boxed_str());
        cells.push((
            leak,
            format!("{m}RETURN collect({x}) AS v"),
            vec![vec![l(vec![])]],
        ));
        let leak: &'static str = Box::leak(format!("empty/{shape}/AVG").into_boxed_str());
        cells.push((leak, format!("{m}RETURN AVG({x}) AS v"), vec![vec![n()]]));
        // grouped aggregate over empty input: zero rows
        let leak: &'static str = Box::leak(format!("empty/{shape}/grouped").into_boxed_str());
        cells.push((leak, format!("{m}RETURN {x} AS k, COUNT(*) AS v"), vec![]));
    }
    check(cells);
}

/// openCypher / Neo4j: sum() over no rows is 0 (not null).
#[test]
fn empty_input_sum_is_zero() {
    check(vec![
        c(
            "empty/node/SUM",
            "MATCH (p:P) WHERE p.id > 99 RETURN SUM(p.id) AS v",
            vec![vec![i(0)]],
        ),
        c(
            "empty/1hop/SUM",
            "MATCH (p:P)-[:HAS]->(t:T) WHERE t.name = 'D' RETURN SUM(p.id) AS v",
            vec![vec![i(0)]],
        ),
        c(
            "empty/varlen/SUM",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) WHERE b.i > 99 RETURN SUM(a.i) AS v",
            vec![vec![i(0)]],
        ),
    ]);
}

#[test]
fn empty_input_global_with_order_limit() {
    check(vec![
        c(
            "empty/1hop/COUNT/ORDER",
            "MATCH (p:P)-[:HAS]->(t:T) WHERE t.name = 'D' RETURN COUNT(*) AS v ORDER BY v",
            vec![vec![i(0)]],
        ),
        c(
            "empty/varlen/COUNT/LIMIT",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) WHERE b.i > 99 RETURN COUNT(*) AS v LIMIT 1",
            vec![vec![i(0)]],
        ),
        c(
            "empty/1hop/COUNT/SKIP1",
            "MATCH (p:P)-[:HAS]->(t:T) WHERE t.name = 'D' RETURN COUNT(*) AS v SKIP 1",
            vec![],
        ),
    ]);
}

#[test]
fn global_aggregates_per_shape() {
    check(vec![
        c(
            "g/1hop/COUNT",
            &format!("{H}RETURN COUNT(*) AS v"),
            vec![vec![i(7)]],
        ),
        c(
            "g/2hop/COUNT",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN COUNT(*) AS v",
            vec![vec![i(7)]],
        ),
        c(
            "g/3hop/COUNT",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K)-[:OF]->(z:Z) RETURN COUNT(*) AS v",
            vec![vec![i(7)]],
        ),
        c(
            "g/varlen/COUNT",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) RETURN COUNT(*) AS v",
            vec![vec![i(3)]],
        ),
        c(
            "g/varlen_in/COUNT",
            "MATCH (a:Q)<-[:N*1..2]-(b:Q) RETURN COUNT(*) AS v",
            vec![vec![i(3)]],
        ),
        c(
            "g/varlen_exact2/COUNT",
            "MATCH (a:Q)-[:N*2..2]->(b:Q) RETURN COUNT(*) AS v",
            vec![vec![i(1)]],
        ),
        c(
            "g/undirected/COUNT",
            "MATCH (a:Q)-[:N]-(b:Q) RETURN COUNT(*) AS v",
            vec![vec![i(4)]],
        ),
        c(
            "g/1hop/COUNT/ORDER/LIMIT",
            &format!("{H}RETURN COUNT(*) AS v ORDER BY v LIMIT 1"),
            vec![vec![i(7)]],
        ),
        c(
            "g/1hop/COUNT/SKIP1",
            &format!("{H}RETURN COUNT(*) AS v SKIP 1"),
            vec![],
        ),
        c(
            "g/1hop/COUNT/LIMIT0",
            &format!("{H}RETURN COUNT(*) AS v LIMIT 0"),
            vec![],
        ),
        c(
            "g/1hop/SUM",
            &format!("{H}RETURN SUM(p.id) AS v"),
            vec![vec![i(13)]],
        ),
        c(
            "g/varlen/SUM",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) RETURN SUM(b.i) AS v",
            vec![vec![i(8)]],
        ),
    ]);
}

#[test]
fn grouped_one_hop_extras() {
    check(vec![
        c(
            "1hop/multi-agg",
            &format!("{H}RETURN t.name, COUNT(*) AS c, SUM(p.id) AS s ORDER BY s DESC"),
            vec![
                vec![s("B"), i(3), i(6)],
                vec![s("C"), i(2), i(4)],
                vec![s("A"), i(2), i(3)],
            ],
        ),
        c(
            "1hop/expr-key/LIMIT",
            &format!("{H}RETURN t.name, COUNT(*) AS c ORDER BY c * -1 ASC, t.name DESC LIMIT 2"),
            vec![vec![s("B"), i(3)], vec![s("C"), i(2)]],
        ),
        c(
            "1hop/expr-key-sum",
            &format!("{H}RETURN t.name, SUM(p.id) AS tot ORDER BY tot * -1 ASC"),
            vec![vec![s("B"), i(6)], vec![s("C"), i(4)], vec![s("A"), i(3)]],
        ),
        c(
            "1hop/DISTINCT-key",
            &format!("{H}RETURN DISTINCT t.name ORDER BY t.name DESC"),
            vec![vec![s("C")], vec![s("B")], vec![s("A")]],
        ),
        c(
            "1hop/DISTINCT-g",
            &format!("{H}RETURN DISTINCT p.g ORDER BY p.g ASC"),
            vec![vec![s("x")], vec![s("y")]],
        ),
        c(
            "1hop/DISTINCT-g/LIMIT",
            &format!("{H}RETURN DISTINCT p.g ORDER BY p.g DESC LIMIT 1"),
            vec![vec![s("y")]],
        ),
        c(
            "1hop/agg-order-by-nonreturned-key",
            &format!("{H}RETURN t.name, COUNT(*) AS c ORDER BY t.name DESC LIMIT 2"),
            vec![vec![s("C"), i(2)], vec![s("B"), i(3)]],
        ),
    ]);
}

#[test]
fn undirected_one_hop_aggregates() {
    let u = "MATCH (a:Q)-[:N]-(b:Q) ";
    check(vec![
        c(
            "undir/COUNT/ORDER-key",
            &format!("{u}RETURN a.i, COUNT(*) AS c ORDER BY a.i ASC"),
            vec![vec![i(1), i(1)], vec![i(2), i(2)], vec![i(3), i(1)]],
        ),
        c(
            "undir/COUNT/ORDER-agg",
            &format!("{u}RETURN a.i, COUNT(*) AS c ORDER BY c DESC, a.i ASC"),
            vec![vec![i(2), i(2)], vec![i(1), i(1)], vec![i(3), i(1)]],
        ),
        c(
            "undir/COUNT/LIMIT",
            &format!("{u}RETURN a.i, COUNT(*) AS c ORDER BY c DESC, a.i ASC LIMIT 1"),
            vec![vec![i(2), i(2)]],
        ),
        c(
            "undir/COUNT/SKIP",
            &format!("{u}RETURN a.i, COUNT(*) AS c ORDER BY c DESC, a.i ASC SKIP 2"),
            vec![vec![i(3), i(1)]],
        ),
        c(
            "undir/SUM",
            // neighbours: q1->{2}, q2->{1,3}, q3->{2}
            &format!("{u}RETURN a.i, SUM(b.i) AS s ORDER BY s DESC, a.i ASC"),
            vec![vec![i(2), i(4)], vec![i(1), i(2)], vec![i(3), i(2)]],
        ),
        c(
            "undir/T-P-COUNT",
            "MATCH (t:T)-[:HAS]-(p:P) RETURN t.name, COUNT(*) AS c ORDER BY c DESC, t.name ASC",
            vec![vec![s("B"), i(3)], vec![s("A"), i(2)], vec![s("C"), i(2)]],
        ),
        c(
            "undir/raw-rows/ORDER",
            &format!("{u}RETURN a.i, b.i ORDER BY a.i DESC, b.i DESC"),
            vec![
                vec![i(3), i(2)],
                vec![i(2), i(3)],
                vec![i(2), i(1)],
                vec![i(1), i(2)],
            ],
        ),
    ]);
}

#[test]
fn relationship_variable_aggregates() {
    check(vec![
        c(
            "relvar/1hop/COUNT(r)",
            "MATCH (p:P)-[r:HAS]->(t:T) RETURN t.name, COUNT(r) AS c ORDER BY c DESC, t.name ASC",
            vec![vec![s("B"), i(3)], vec![s("A"), i(2)], vec![s("C"), i(2)]],
        ),
        c(
            "relvar/1hop/type(r)",
            "MATCH (p:P)-[r:HAS]->(t:T) RETURN type(r) AS ty, COUNT(*) AS c",
            vec![vec![s("HAS"), i(7)]],
        ),
        c(
            "relvar/1hop/type(r)/ORDER",
            "MATCH (p:P)-[r:HAS]->(t:T) RETURN type(r) AS ty, t.name AS n, COUNT(*) AS c ORDER BY n DESC",
            vec![
                vec![s("HAS"), s("C"), i(2)],
                vec![s("HAS"), s("B"), i(3)],
                vec![s("HAS"), s("A"), i(2)],
            ],
        ),
        c(
            "relvar/undirected/COUNT(r)",
            "MATCH (a:Q)-[r:N]-(b:Q) RETURN a.i, COUNT(r) AS c ORDER BY a.i ASC",
            vec![vec![i(1), i(1)], vec![i(2), i(2)], vec![i(3), i(1)]],
        ),
        c(
            "relvar/2hop/COUNT(r2)",
            "MATCH (p:P)-[r1:HAS]->(t:T)-[r2:IN]->(k:K) RETURN k.name, COUNT(r2) AS c ORDER BY c DESC",
            vec![vec![s("X"), i(5)], vec![s("Y"), i(2)]],
        ),
    ]);
}

#[test]
fn optional_match_aggregates() {
    let o = "MATCH (t:T) OPTIONAL MATCH (t)<-[:HAS]-(p:P) ";
    check(vec![
        c(
            "opt/COUNT(p)",
            &format!("{o}RETURN t.name, COUNT(p) AS c ORDER BY c DESC, t.name ASC"),
            vec![
                vec![s("B"), i(3)],
                vec![s("A"), i(2)],
                vec![s("C"), i(2)],
                vec![s("D"), i(0)],
            ],
        ),
        c(
            "opt/COUNT(*)",
            &format!("{o}RETURN t.name, COUNT(*) AS c ORDER BY c DESC, t.name ASC"),
            vec![
                vec![s("B"), i(3)],
                vec![s("A"), i(2)],
                vec![s("C"), i(2)],
                vec![s("D"), i(1)],
            ],
        ),
        c(
            "opt/COUNT(p)/ASC/LIMIT",
            &format!("{o}RETURN t.name, COUNT(p) AS c ORDER BY c ASC, t.name ASC LIMIT 2"),
            vec![vec![s("D"), i(0)], vec![s("A"), i(2)]],
        ),
        c(
            "opt/MAX/by-name",
            &format!("{o}RETURN t.name, MAX(p.id) AS m ORDER BY t.name ASC"),
            vec![
                vec![s("A"), i(2)],
                vec![s("B"), i(3)],
                vec![s("C"), i(3)],
                vec![s("D"), n()],
            ],
        ),
        c(
            "opt/global-COUNT(*)",
            &format!("{o}RETURN COUNT(*) AS c"),
            vec![vec![i(8)]],
        ),
        c(
            "opt/global-COUNT(p)",
            &format!("{o}RETURN COUNT(p) AS c"),
            vec![vec![i(7)]],
        ),
        c(
            "opt/all-empty-COUNT(*)",
            "MATCH (t:T) OPTIONAL MATCH (t)-[:NOPE]->(k:K) RETURN COUNT(*) AS c",
            vec![vec![i(4)]],
        ),
    ]);
}

#[test]
fn raw_rows_order_by_on_relationship_patterns() {
    check(vec![
        c(
            "raw/1hop/ORDER",
            &format!("{H}RETURN p.id, t.name ORDER BY p.id DESC, t.name ASC"),
            vec![
                vec![i(3), s("B")],
                vec![i(3), s("C")],
                vec![i(2), s("A")],
                vec![i(2), s("B")],
                vec![i(1), s("A")],
                vec![i(1), s("B")],
                vec![i(1), s("C")],
            ],
        ),
        c(
            "raw/1hop/ORDER/SKIP/LIMIT",
            &format!("{H}RETURN p.id, t.name ORDER BY p.id DESC, t.name ASC SKIP 2 LIMIT 3"),
            vec![
                vec![i(2), s("A")],
                vec![i(2), s("B")],
                vec![i(1), s("A")],
            ],
        ),
        c(
            "raw/1hop/ORDER-expr",
            &format!("{H}RETURN p.id, t.name ORDER BY p.id * -1 ASC, t.name DESC LIMIT 2"),
            vec![vec![i(3), s("C")], vec![i(3), s("B")]],
        ),
        c(
            "raw/2hop/ORDER",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN k.name, p.id ORDER BY k.name DESC, p.id ASC LIMIT 4",
            vec![
                // Y: A -> p2, p1 ; X: B -> p1,p2,p3 and C -> p1,p3
                vec![s("Y"), i(1)],
                vec![s("Y"), i(2)],
                vec![s("X"), i(1)],
                vec![s("X"), i(1)],
            ],
        ),
        c(
            "raw/varlen/ORDER",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) RETURN a.i, b.i ORDER BY a.i DESC, b.i DESC",
            vec![
                vec![i(2), i(3)],
                vec![i(1), i(3)],
                vec![i(1), i(2)],
            ],
        ),
        c(
            "raw/varlen_in/ORDER/LIMIT",
            "MATCH (a:Q)<-[:N*1..2]-(b:Q) RETURN a.i, b.i ORDER BY a.i DESC, b.i ASC LIMIT 2",
            vec![vec![i(3), i(1)], vec![i(3), i(2)]],
        ),
        c(
            "raw/varlen/DISTINCT",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) RETURN DISTINCT b.i ORDER BY b.i DESC",
            vec![vec![i(3)], vec![i(2)]],
        ),
        c(
            "raw/3hop/ORDER",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K)-[:OF]->(z:Z) RETURN DISTINCT k.name ORDER BY k.name DESC",
            vec![vec![s("Y")], vec![s("X")]],
        ),
    ]);
}

#[test]
fn n_hop_and_varlen_chain_aggregates() {
    let h3 = "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K)-[:OF]->(z:Z) ";
    check(vec![
        c(
            "3hop/COUNT/key/DESC",
            &format!("{h3}RETURN k.name, COUNT(*) AS c ORDER BY c DESC"),
            vec![vec![s("X"), i(5)], vec![s("Y"), i(2)]],
        ),
        c(
            "3hop/COUNT/key/ASC/LIMIT",
            &format!("{h3}RETURN k.name, COUNT(*) AS c ORDER BY c ASC LIMIT 1"),
            vec![vec![s("Y"), i(2)]],
        ),
        c(
            "3hop/COUNT/end-group",
            &format!("{h3}RETURN z.name, COUNT(*) AS c"),
            vec![vec![s("Z"), i(7)]],
        ),
        c(
            "3hop/SUM",
            // X: B(1,2,3)=6 + C(1,3)=4 = 10 ; Y: A(2,1)=3
            &format!("{h3}RETURN k.name, SUM(p.id) AS s ORDER BY s DESC"),
            vec![vec![s("X"), i(10)], vec![s("Y"), i(3)]],
        ),
        c(
            "1hop-then-varlen/COUNT",
            "MATCH (t:T)-[:IN]->(k:K)-[:OF*1..2]->(z:Z) RETURN k.name, COUNT(*) AS c ORDER BY c DESC",
            vec![vec![s("X"), i(2)], vec![s("Y"), i(1)]],
        ),
        c(
            "1hop-then-varlen/COUNT/ASC",
            "MATCH (t:T)-[:IN]->(k:K)-[:OF*1..2]->(z:Z) RETURN k.name, COUNT(*) AS c ORDER BY c ASC",
            vec![vec![s("Y"), i(1)], vec![s("X"), i(2)]],
        ),
        c(
            "varlen-then-1hop/COUNT",
            // q1 -N*1..2-> q2 -N-> q3 : exactly one path
            "MATCH (a:Q)-[:N*1..2]->(b:Q)-[:N]->(c:Q) RETURN a.i, COUNT(*) AS n ORDER BY n DESC",
            vec![vec![i(1), i(1)]],
        ),
        c(
            "1hop-then-varlen/global-empty",
            "MATCH (t:T)-[:IN]->(k:K)-[:OF*1..2]->(z:Z) WHERE k.name = 'nope' RETURN COUNT(*) AS c",
            vec![vec![i(0)]],
        ),
    ]);
}

#[test]
fn varlen_and_incoming_varlen_aggregates() {
    let o = "MATCH (a:Q)-[:N*1..2]->(b:Q) ";
    let inc = "MATCH (a:Q)<-[:N*1..2]-(b:Q) ";
    check(vec![
        c(
            "varlen/by-start/DESC",
            &format!("{o}RETURN a.i, COUNT(*) AS c ORDER BY c DESC, a.i ASC"),
            vec![vec![i(1), i(2)], vec![i(2), i(1)]],
        ),
        c(
            "varlen/by-start/ASC",
            &format!("{o}RETURN a.i, COUNT(*) AS c ORDER BY c ASC, a.i ASC"),
            vec![vec![i(2), i(1)], vec![i(1), i(2)]],
        ),
        c(
            "varlen/by-start/SKIP",
            &format!("{o}RETURN a.i, COUNT(*) AS c ORDER BY c DESC, a.i ASC SKIP 1"),
            vec![vec![i(2), i(1)]],
        ),
        c(
            "varlen/by-end",
            &format!("{o}RETURN b.i, COUNT(*) AS c ORDER BY c DESC"),
            vec![vec![i(3), i(2)], vec![i(2), i(1)]],
        ),
        c(
            "varlen/SUM",
            &format!("{o}RETURN a.i, SUM(b.i) AS s ORDER BY s DESC"),
            vec![vec![i(1), i(5)], vec![i(2), i(3)]],
        ),
        c(
            "varlen/MIN",
            &format!("{o}RETURN a.i, MIN(b.i) AS m ORDER BY m DESC"),
            vec![vec![i(2), i(3)], vec![i(1), i(2)]],
        ),
        c(
            "varlen/MAX",
            &format!("{o}RETURN a.i, MAX(b.i) AS m ORDER BY m DESC, a.i ASC"),
            vec![vec![i(1), i(3)], vec![i(2), i(3)]],
        ),
        c(
            "varlen/collect(1-elem)",
            "MATCH (a:Q)-[:N]->(b:Q) RETURN a.i, collect(b.i) AS bs ORDER BY a.i ASC",
            vec![
                vec![i(1), l(vec![i(2)])],
                vec![i(2), l(vec![i(3)])],
            ],
        ),
        c(
            "varlen/expr-key/LIMIT",
            &format!("{o}RETURN a.i, COUNT(*) AS c ORDER BY a.i * -1 ASC LIMIT 1"),
            vec![vec![i(2), i(1)]],
        ),
        c(
            "varlen_in/by-a/DESC",
            &format!("{inc}RETURN a.i, COUNT(*) AS c ORDER BY c DESC"),
            vec![vec![i(3), i(2)], vec![i(2), i(1)]],
        ),
        c(
            "varlen_in/by-a/ASC",
            &format!("{inc}RETURN a.i, COUNT(*) AS c ORDER BY c ASC"),
            vec![vec![i(2), i(1)], vec![i(3), i(2)]],
        ),
        c(
            "varlen_in/by-b/key-DESC",
            &format!("{inc}RETURN b.i, COUNT(*) AS c ORDER BY b.i DESC"),
            vec![vec![i(2), i(1)], vec![i(1), i(2)]],
        ),
        c(
            "varlen_in/LIMIT",
            &format!("{inc}RETURN a.i, COUNT(*) AS c ORDER BY c DESC LIMIT 1"),
            vec![vec![i(3), i(2)]],
        ),
        c(
            "varlen_in/T-P",
            "MATCH (t:T)<-[:HAS*1..2]-(p:P) RETURN t.name, COUNT(*) AS c ORDER BY c DESC, t.name ASC LIMIT 2",
            vec![vec![s("B"), i(3)], vec![s("A"), i(2)]],
        ),
        c(
            "varlen_in/SUM",
            &format!("{inc}RETURN a.i, SUM(b.i) AS s ORDER BY s DESC"),
            vec![vec![i(3), i(3)], vec![i(2), i(1)]],
        ),
    ]);
}

/// Known bug #549: engine returns the wrong answer; expectations are hand-derived.
#[test]
#[ignore = "bug #549"]
fn count_distinct_unsupported() {
    check(vec![
        c(
            "g/1hop/COUNT_DISTINCT_t",
            &format!("{H}RETURN COUNT(DISTINCT t.name) AS v"),
            vec![vec![i(3)]],
        ),
        c(
            "g/1hop/COUNT_DISTINCT_g",
            &format!("{H}RETURN COUNT(DISTINCT p.g) AS v"),
            vec![vec![i(2)]],
        ),
        c(
            "g/2hop/COUNT_DISTINCT_k",
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN COUNT(DISTINCT k.name) AS v",
            vec![vec![i(2)]],
        ),
        c(
            "1hop/COUNT_DISTINCT/group",
            &format!("{H}RETURN p.id, COUNT(DISTINCT t.name) AS c ORDER BY c DESC, p.id ASC"),
            vec![vec![i(1), i(3)], vec![i(2), i(2)], vec![i(3), i(2)]],
        ),
        c(
            "varlen/COUNT_DISTINCT",
            "MATCH (a:Q)-[:N*1..2]->(b:Q) RETURN COUNT(DISTINCT b.i) AS c",
            vec![vec![i(2)]],
        ),
    ]);
}

/// Known bug #550: engine returns the wrong answer; expectations are hand-derived.
#[test]
#[ignore = "bug #550"]
fn order_by_non_returned_key() {
    check(vec![c(
        "raw/1hop/ORDER-id(p)",
        &format!("{H}RETURN p.id, t.name ORDER BY id(p) DESC, t.name ASC LIMIT 3"),
        vec![vec![i(3), s("B")], vec![i(3), s("C")], vec![i(2), s("A")]],
    )]);
}

/// Known bug #551: engine returns the wrong answer; expectations are hand-derived.
#[test]
#[ignore = "bug #551"]
fn varlen_relationship_variable() {
    check(vec![
        c(
            "relvar/varlen/COUNT(r)",
            "MATCH (p:P)-[r:HAS*1..2]->(t:T) RETURN t.name, COUNT(r) AS c ORDER BY c DESC, t.name ASC",
            vec![vec![s("B"), i(3)], vec![s("A"), i(2)], vec![s("C"), i(2)]],
        ),
        c(
            "relvar/varlen/COUNT(r)/global",
            "MATCH (a:Q)-[r:N*1..2]->(b:Q) RETURN COUNT(r) AS c",
            vec![vec![i(3)]],
        ),
        c(
            "relvar/varlen/size(r)",
            "MATCH (a:Q)-[r:N*1..2]->(b:Q) RETURN size(r) AS len, COUNT(*) AS c ORDER BY len ASC",
            vec![vec![i(1), i(2)], vec![i(2), i(1)]],
        ),
    ]);
}

/// Known bug #552: engine returns the wrong answer; expectations are hand-derived.
#[test]
#[ignore = "bug #552"]
fn optional_match_multi_hop() {
    check(vec![
        c(
            "opt/two-hop-optional/COUNT(k)",
            // p1 -> A,B,C -> Y,X,X = 3 ; p2 -> B,A -> X,Y = 2 ; p3 -> B,C -> X,X = 2
            "MATCH (p:P) OPTIONAL MATCH (p)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN p.id, COUNT(k) AS c ORDER BY c DESC, p.id ASC",
            vec![vec![i(1), i(3)], vec![i(2), i(2)], vec![i(3), i(2)]],
        ),
    ]);
}
