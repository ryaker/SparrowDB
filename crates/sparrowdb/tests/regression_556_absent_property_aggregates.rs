//! #556: a property ABSENT from a node must read as NULL in aggregate input
//! (COUNT/SUM/AVG/MIN/MAX/collect), not as Int64(0). A property legitimately
//! stored as 0 must still count as 0.
//!
//! Every expected value is derived BY HAND from the fixture below and the
//! openCypher semantics; none was captured from engine output.
//!
//! Fixture: W1, W2, W3 (label W); S -[:R]-> A; A -[:R]-> W1, W2, W3.
//!
//!   prop  | W1  | W2  | W3  | non-null values
//!   id    |  1  |  2  |  3  | [1,2,3]
//!   nope  |  -  |  -  |  -  | []              (absent everywhere)
//!   u     |  0  |  4  |  -  | [0,4]           (stored 0 next to absent)
//!   z     |  0  |  0  |  0  | [0,0,0]         (stored 0 everywhere)
//!   m     |  -  |  7  |  9  | [7,9]           (a spurious 0 would become MIN)
//!   n     | -3  |  -  | -5  | [-3,-5]         (a spurious 0 would become MAX)
//!
//! Every entry-point shape reaches each W exactly once, so the per-property
//! results below are identical for every shape.

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
        "CREATE (:W {id:1, u:0, z:0, n:-3})",
        "CREATE (:W {id:2, u:4, z:0, m:7})",
        "CREATE (:W {id:3, z:0, m:9, n:-5})",
        "CREATE (:A {name:'a'})",
        "CREATE (:S {name:'s'})",
        "MATCH (s:S),(a:A) CREATE (s)-[:R]->(a)",
        "MATCH (a:A),(w:W) CREATE (a)-[:R]->(w)",
    ] {
        db.execute(q).unwrap();
    }
    (dir, db)
}

/// (property, [COUNT, SUM, AVG, MIN, MAX, collect-as-sorted-list]).
fn expected() -> Vec<(&'static str, [Value; 6])> {
    let l = |v: Vec<i64>| Value::List(v.into_iter().map(Value::Int64).collect());
    vec![
        ("id", [i(3), i(6), f(2.0), i(1), i(3), l(vec![1, 2, 3])]),
        (
            "nope",
            [i(0), i(0), Value::Null, Value::Null, Value::Null, l(vec![])],
        ),
        ("u", [i(2), i(4), f(2.0), i(0), i(4), l(vec![0, 4])]),
        ("z", [i(3), i(0), f(0.0), i(0), i(0), l(vec![0, 0, 0])]),
        ("m", [i(2), i(16), f(8.0), i(7), i(9), l(vec![7, 9])]),
        ("n", [i(2), i(-8), f(-4.0), i(-5), i(-3), l(vec![-5, -3])]),
    ]
}

const AGGS: [&str; 6] = ["COUNT", "SUM", "AVG", "MIN", "MAX", "collect"];

fn norm(v: Value) -> Value {
    match v {
        Value::List(mut xs) => {
            xs.sort_by_key(|x| format!("{x:?}"));
            Value::List(xs)
        }
        o => o,
    }
}

/// `pat` binds `w`; `var` is the aggregated variable; `pre` is a WITH prefix.
fn sweep(shape: &str, pat: &str, tail: &str) {
    let (_d, db) = fixture();
    let mut bad = Vec::new();
    for (prop, want) in expected() {
        for (k, agg) in AGGS.iter().enumerate() {
            let q = format!("{pat} RETURN {agg}(w.{prop}){tail}");
            let want_cell = norm(want[k].clone());
            match db.execute(&q) {
                Ok(r) if r.rows.len() == 1 && norm(r.rows[0][0].clone()) == want_cell => {}
                Ok(r) => bad.push(format!(
                    "WRONG [{shape}] {q}\n   want {want_cell:?}\n   got  {:?}",
                    r.rows
                )),
                Err(e) => bad.push(format!("ERROR [{shape}] {q}\n   {e}")),
            }
        }
    }
    assert!(
        bad.is_empty(),
        "{} cells wrong:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
fn single_node_match() {
    sweep("single-node", "MATCH (w:W)", "");
}

#[test]
fn one_hop_dst() {
    sweep("1-hop dst", "MATCH (a:A)-[:R]->(w:W)", "");
}

#[test]
fn one_hop_src() {
    sweep("1-hop src", "MATCH (w:W)<-[:R]-(a:A)", "");
}

#[test]
fn two_hop() {
    sweep("2-hop", "MATCH (s:S)-[:R]->(a:A)-[:R]->(w:W)", "");
}

#[test]
fn varlen() {
    sweep("varlen", "MATCH (s:S)-[:R*2..2]->(w:W)", "");
}

#[test]
fn optional_match() {
    sweep("optional", "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(w:W)", "");
}

#[test]
fn with_clause_aggregate() {
    // Aggregate inside the WITH clause itself, then RETURN the alias.
    // (AVG is excluded: WITH-level AVG is unimplemented, see
    // `with_clause_avg_is_unimplemented`.)
    let (_d, db) = fixture();
    let mut bad = Vec::new();
    for (prop, want) in expected() {
        for (k, agg) in AGGS.iter().enumerate() {
            if *agg == "AVG" {
                continue;
            }
            let q = format!("MATCH (w:W) WITH {agg}(w.{prop}) AS r RETURN r");
            let want_cell = norm(want[k].clone());
            match db.execute(&q) {
                Ok(r) if r.rows.len() == 1 && norm(r.rows[0][0].clone()) == want_cell => {}
                Ok(r) => bad.push(format!(
                    "WRONG {q}\n   want {want_cell:?}\n   got  {:?}",
                    r.rows
                )),
                Err(e) => bad.push(format!("ERROR {q}\n   {e}")),
            }
        }
    }
    assert!(
        bad.is_empty(),
        "{} cells wrong:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
fn grouped_aggregate_over_absent() {
    // Group by id (each group has one W); per-group COUNT(w.u): W1 has u=0 -> 1,
    // W2 u=4 -> 1, W3 absent -> 0.
    let (_d, db) = fixture();
    let r = db
        .execute("MATCH (w:W) RETURN w.id, COUNT(w.u) ORDER BY w.id")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![i(1), i(1)], vec![i(2), i(1)], vec![i(3), i(0)]]
    );
}

#[test]
fn return_absent_property_is_null() {
    let (_d, db) = fixture();
    let r = db
        .execute("MATCH (w:W) RETURN w.id, w.nope, w.u ORDER BY w.id")
        .unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![i(1), Value::Null, i(0)],
            vec![i(2), Value::Null, i(4)],
            vec![i(3), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn where_is_null_agrees_with_count() {
    let (_d, db) = fixture();
    let r = db
        .execute("MATCH (w:W) WHERE w.nope IS NULL RETURN COUNT(*)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![i(3)]]);
    let r = db
        .execute("MATCH (w:W) WHERE w.u IS NOT NULL RETURN COUNT(*)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![i(2)]]);
}

#[test]
fn group_by_absent_key_is_one_null_group() {
    // u: W1=0, W2=4, W3 absent -> three groups, absent is its own NULL group.
    let (_d, db) = fixture();
    let r = db.execute("MATCH (w:W) RETURN w.nope, COUNT(*)").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Null, i(3)]]);
}

#[test]
fn distinct_over_absent_keeps_stored_zero_distinct_from_null() {
    let (_d, db) = fixture();
    let r = db.execute("MATCH (w:W) RETURN DISTINCT w.u").unwrap();
    let mut got: Vec<String> = r.rows.iter().map(|x| format!("{:?}", x[0])).collect();
    got.sort();
    let mut want = vec![
        format!("{:?}", i(0)),
        format!("{:?}", i(4)),
        format!("{:?}", Value::Null),
    ];
    want.sort();
    assert_eq!(got, want);
}

#[test]
#[ignore = "bug #558"]
fn with_then_return_aggregate_absent() {
    let (_d, db) = fixture();
    let r = db
        .execute("MATCH (w:W) WITH w.u AS x RETURN COUNT(x)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![i(2)]]);
}

#[test]
#[ignore = "bug #558"]
fn with_clause_avg_is_unimplemented() {
    let (_d, db) = fixture();
    let r = db
        .execute("MATCH (w:W) WITH AVG(w.u) AS r RETURN r")
        .unwrap();
    assert_eq!(r.rows, vec![vec![f(2.0)]]);
}

#[test]
#[ignore = "bug #559"]
fn degree_fastpath_count_matches_plain_count() {
    let (_d, db) = fixture();
    let r = db
        .execute("MATCH (a:A)-[:R]->(w:W) RETURN a.name, COUNT(w) AS c ORDER BY c DESC LIMIT 3")
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::String("a".into()), i(3)]]);
}

#[test]
fn multi_pattern_inline_zero_filter_does_not_match_absent() {
    let (_d, db) = fixture();
    // Only W1 stores u = 0; W3 lacks u.  One A, so 1 * 1 = 1.
    let r = db
        .execute("MATCH (a:A),(w:W {u:0}) RETURN COUNT(*)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![i(1)]]);
    let r = db
        .execute("MATCH (a:A),(w:W {nope:0}) RETURN COUNT(*)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![i(0)]]);
}
