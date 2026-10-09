//! #558: an aggregate in the RETURN after a non-aggregating WITH must aggregate
//! the WITH output (one row per group), not be evaluated once per input row;
//! and a WITH-level AVG / float SUM / empty MIN-MAX must be computed with the
//! same semantics as the same aggregate in a RETURN.
//!
//! Every expected value is derived BY HAND from the fixture below; none was
//! captured from engine output.
//!
//!   W   id: 1, 2, 3                       (all present)
//!   D   v : 1, 1, 2                       (duplicates)
//!   X   k : 4, 6, absent                  (one node lacks k)
//!   Y   f : 1.5, 2.5                      (floats)
//!   P   n : 1, 2
//!   Q   v : 10, 20, 30
//!   R   w : 1, 2, 4
//!   P1-[:K]->Q10, P1-[:K]->Q20, P2-[:K]->Q30
//!   Q10-[:L]->R1, Q20-[:L]->R2, Q30-[:L]->R4

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn i(v: i64) -> Value {
    Value::Int64(v)
}
fn f(v: f64) -> Value {
    Value::Float64(v)
}

fn fixture() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for q in [
        "CREATE (:W {id:1})",
        "CREATE (:W {id:2})",
        "CREATE (:W {id:3})",
        "CREATE (:D {v:1})",
        "CREATE (:D {v:1})",
        "CREATE (:D {v:2})",
        "CREATE (:X {k:4})",
        "CREATE (:X {k:6})",
        "CREATE (:X {j:1})",
        "CREATE (:Y {f:1.5})",
        "CREATE (:Y {f:2.5})",
        "CREATE (:P {n:1})",
        "CREATE (:P {n:2})",
        "CREATE (:Q {v:10})",
        "CREATE (:Q {v:20})",
        "CREATE (:Q {v:30})",
        "CREATE (:R {w:1})",
        "CREATE (:R {w:2})",
        "CREATE (:R {w:4})",
        "MATCH (p:P {n:1}),(q:Q {v:10}) CREATE (p)-[:K]->(q)",
        "MATCH (p:P {n:1}),(q:Q {v:20}) CREATE (p)-[:K]->(q)",
        "MATCH (p:P {n:2}),(q:Q {v:30}) CREATE (p)-[:K]->(q)",
        "MATCH (q:Q {v:10}),(r:R {w:1}) CREATE (q)-[:L]->(r)",
        "MATCH (q:Q {v:20}),(r:R {w:2}) CREATE (q)-[:L]->(r)",
        "MATCH (q:Q {v:30}),(r:R {w:4}) CREATE (q)-[:L]->(r)",
    ] {
        db.execute(q).unwrap();
    }
    (dir, db)
}

fn check(cases: Vec<(&str, Vec<Vec<Value>>)>) {
    let (_dir, db) = fixture();
    let mut failures = Vec::new();
    for (q, want) in cases {
        match db.execute(q) {
            Ok(r) if r.rows == want => {}
            Ok(r) => failures.push(format!("{q}\n   want {want:?}\n   got  {:?}", r.rows)),
            Err(e) => failures.push(format!("{q}\n   want {want:?}\n   err  {e:?}")),
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[test]
fn issue_558_reported_queries() {
    check(vec![
        (
            "MATCH (w:W) WITH w.id AS x RETURN COUNT(x)",
            vec![vec![i(3)]],
        ),
        ("MATCH (w:W) WITH w RETURN COUNT(w.id)", vec![vec![i(3)]]),
        (
            "MATCH (w:W) WITH AVG(w.id) AS r RETURN r",
            vec![vec![f(2.0)]],
        ),
    ]);
}

#[test]
fn return_aggregate_after_with_projection() {
    check(vec![
        (
            "MATCH (w:W) WITH w.id AS x RETURN COUNT(x), SUM(x), AVG(x), MIN(x), MAX(x)",
            vec![vec![i(3), i(6), f(2.0), i(1), i(3)]],
        ),
        ("MATCH (w:W) WITH w.id AS x RETURN COUNT(*)", vec![vec![i(3)]]),
        (
            "MATCH (w:W) WITH w.id AS x RETURN collect(x)",
            vec![vec![Value::List(vec![i(1), i(2), i(3)])]],
        ),
        // Absent k reads as NULL and is skipped: k values are 4, 6.
        (
            "MATCH (x:X) WITH x.k AS k RETURN COUNT(k), SUM(k), AVG(k), MIN(k), MAX(k)",
            vec![vec![i(2), i(10), f(5.0), i(4), i(6)]],
        ),
        // Float input: 1.5 + 2.5.
        (
            "MATCH (y:Y) WITH y.f AS v RETURN SUM(v), AVG(v)",
            vec![vec![f(4.0), f(2.0)]],
        ),
        // Empty input, global aggregate: COUNT/SUM 0, AVG/MIN/MAX NULL.
        (
            "MATCH (w:W) WHERE w.id > 9 WITH w.id AS x RETURN COUNT(x), SUM(x), AVG(x), MIN(x), MAX(x)",
            vec![vec![i(0), i(0), Value::Null, Value::Null, Value::Null]],
        ),
    ]);
}

#[test]
fn return_aggregate_after_with_where() {
    check(vec![
        // Rows kept: 2, 3.
        (
            "MATCH (w:W) WITH w.id AS x WHERE x > 1 RETURN COUNT(x), SUM(x), AVG(x)",
            vec![vec![i(2), i(5), f(2.5)]],
        ),
        (
            "MATCH (w:W) WITH w.id AS x WHERE x > 9 RETURN COUNT(x)",
            vec![vec![i(0)]],
        ),
    ]);
}

#[test]
fn return_aggregate_after_with_order_limit() {
    check(vec![
        // WITH-level LIMIT is applied BEFORE the aggregate: rows 3, 2 -> 5.
        (
            "MATCH (w:W) WITH w.id AS x ORDER BY x DESC LIMIT 2 RETURN SUM(x)",
            vec![vec![i(5)]],
        ),
        // WITH-level SKIP 1 over 1,2,3 leaves 2, 3.
        (
            "MATCH (w:W) WITH w.id AS x ORDER BY x SKIP 1 RETURN COUNT(x), MIN(x)",
            vec![vec![i(2), i(2)]],
        ),
        // RETURN-level ORDER BY / LIMIT apply to the grouped rows.
        (
            "MATCH (w:W) WITH w.id AS x RETURN x, COUNT(*) ORDER BY x DESC LIMIT 2",
            vec![vec![i(3), i(1)], vec![i(2), i(1)]],
        ),
        (
            "MATCH (d:D) WITH d.v AS x RETURN x, COUNT(*) ORDER BY x",
            vec![vec![i(1), i(2)], vec![i(2), i(1)]],
        ),
        (
            "MATCH (d:D) WITH d.v AS x RETURN x, COUNT(*) ORDER BY x SKIP 1",
            vec![vec![i(2), i(1)]],
        ),
        // ORDER BY the aggregate itself: counts are x=1 -> 2, x=2 -> 1.
        (
            "MATCH (d:D) WITH d.v AS x RETURN x, COUNT(*) AS c ORDER BY c DESC",
            vec![vec![i(1), i(2)], vec![i(2), i(1)]],
        ),
    ]);
}

#[test]
fn return_aggregate_after_with_distinct() {
    check(vec![
        // v = 1, 1, 2.
        (
            "MATCH (d:D) WITH d.v AS x RETURN COUNT(x), COUNT(DISTINCT x), SUM(x), SUM(DISTINCT x)",
            vec![vec![i(3), i(2), i(4), i(3)]],
        ),
        (
            "MATCH (d:D) WITH d.v AS x RETURN DISTINCT COUNT(x)",
            vec![vec![i(3)]],
        ),
    ]);
}

#[test]
fn with_level_aggregates() {
    check(vec![
        ("MATCH (w:W) WITH SUM(w.id) AS r RETURN r", vec![vec![i(6)]]),
        ("MATCH (w:W) WITH MIN(w.id) AS r RETURN r", vec![vec![i(1)]]),
        ("MATCH (w:W) WITH MAX(w.id) AS r RETURN r", vec![vec![i(3)]]),
        (
            "MATCH (y:Y) WITH SUM(y.f) AS r RETURN r",
            vec![vec![f(4.0)]],
        ),
        (
            "MATCH (y:Y) WITH AVG(y.f) AS r RETURN r",
            vec![vec![f(2.0)]],
        ),
        (
            "MATCH (x:X) WITH AVG(x.k) AS r RETURN r",
            vec![vec![f(5.0)]],
        ),
        (
            "MATCH (d:D) WITH COUNT(DISTINCT d.v) AS r RETURN r",
            vec![vec![i(2)]],
        ),
        // Empty input: AVG / MIN / MAX are NULL (not 0); COUNT / SUM are 0.
        (
            "MATCH (w:W) WHERE w.id > 9 WITH AVG(w.id) AS r RETURN r",
            vec![vec![Value::Null]],
        ),
        (
            "MATCH (w:W) WHERE w.id > 9 WITH MIN(w.id) AS r RETURN r",
            vec![vec![Value::Null]],
        ),
        (
            "MATCH (w:W) WHERE w.id > 9 WITH MAX(w.id) AS r RETURN r",
            vec![vec![Value::Null]],
        ),
        (
            "MATCH (w:W) WHERE w.id > 9 WITH COUNT(w.id) AS r RETURN r",
            vec![vec![i(0)]],
        ),
        (
            "MATCH (w:W) WHERE w.id > 9 WITH SUM(w.id) AS r RETURN r",
            vec![vec![i(0)]],
        ),
        // Grouped WITH aggregate, then a RETURN aggregate over the groups:
        // groups x=1 (count 2), x=2 (count 1); SUM of counts = 3.
        (
            "MATCH (d:D) WITH d.v AS x, COUNT(*) AS c RETURN SUM(c), MAX(c)",
            vec![vec![i(3), i(2)]],
        ),
    ]);
}

/// Multi-stage pipelines are the path that reaches 1-hop and 2-hop shapes.
#[test]
fn aggregate_after_with_in_pipelines_over_hops() {
    check(vec![
        // 1-hop: q.v is 10, 20, 30.
        (
            "MATCH (p:P) WITH p MATCH (p)-[:K]->(q:Q) WITH q.v AS x RETURN SUM(x), AVG(x), COUNT(x)",
            vec![vec![i(60), f(20.0), i(3)]],
        ),
        // 1-hop, WITH-level AVG per group: p1 -> (10+20)/2, p2 -> 30.
        (
            "MATCH (p:P) WITH p MATCH (p)-[:K]->(q:Q) WITH p.n AS n, AVG(q.v) AS a RETURN n, a ORDER BY n",
            vec![vec![i(1), f(15.0)], vec![i(2), f(30.0)]],
        ),
        // 2-hop: r.w is 1, 2, 4.
        (
            "MATCH (p:P) WITH p MATCH (p)-[:K]->(q:Q)-[:L]->(r:R) WITH r.w AS x RETURN SUM(x), MAX(x)",
            vec![vec![i(7), i(4)]],
        ),
        (
            "MATCH (p:P) WITH p MATCH (p)-[:K]->(q:Q)-[:L]->(r:R) WITH AVG(r.w) AS a RETURN a",
            vec![vec![f(7.0 / 3.0)]],
        ),
        // Pipeline: WITH-level AVG after a non-aggregating WITH.
        (
            "MATCH (w:W) WITH w.id AS x WITH AVG(x) AS a RETURN a",
            vec![vec![f(2.0)]],
        ),
    ]);
}
