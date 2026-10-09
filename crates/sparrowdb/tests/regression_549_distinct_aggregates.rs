//! #549: `DISTINCT` inside an aggregate — `COUNT/SUM/AVG/MIN/MAX/collect(DISTINCT x)`.
//!
//! Every expected value below is derived by hand from the fixture, never
//! captured from program output.
//!
//! Fixture (same as the aggregate shape sweep in the issue):
//!   P{id,g}: P1{g:x} P2{g:y} P3{g:y};  T{name}: A B C D;  K{name}: X Y;  Z;  Q{i}: 1 2 3
//!   (p)-[:HAS]->(t): (1,C) (1,B) (2,B) (3,B) (2,A) (3,C) (1,A)   (7 edges)
//!   (t)-[:IN]->(k):  (A,Y) (B,X) (C,X)
//!   (k)-[:OF]->(z):  (X,Z) (Y,Z)
//!   (q)-[:N]->(q):   (1,2) (2,3)
//! Hence per p: P1 -> {A,B,C}, P2 -> {A,B}, P3 -> {B,C}.
//!
//! Semantics under test: DISTINCT drops repeated non-null argument values per
//! group before aggregating; nulls are ignored; equality is the one RETURN
//! DISTINCT uses (type-tagged identity, so 1 and 1.0 are different values).

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::types::Value;

fn i(n: i64) -> Value {
    Value::Int64(n)
}
fn s(x: &str) -> Value {
    Value::String(x.to_string())
}

fn fixture() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    for (id, g) in [(1, "x"), (2, "y"), (3, "y")] {
        db.execute(&format!("CREATE (:P {{id: {id}, g: '{g}'}})"))
            .unwrap();
    }
    for t in ["A", "B", "C", "D"] {
        db.execute(&format!("CREATE (:T {{name: '{t}'}})")).unwrap();
    }
    for k in ["X", "Y"] {
        db.execute(&format!("CREATE (:K {{name: '{k}'}})")).unwrap();
    }
    db.execute("CREATE (:Z {name: 'Z'})").unwrap();
    for q in 1..=3 {
        db.execute(&format!("CREATE (:Q {{i: {q}}})")).unwrap();
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
            "MATCH (p:P {{id: {p}}}), (t:T {{name: '{t}'}}) CREATE (p)-[:HAS]->(t)"
        ))
        .unwrap();
    }
    for (t, k) in [("A", "Y"), ("B", "X"), ("C", "X")] {
        db.execute(&format!(
            "MATCH (t:T {{name: '{t}'}}), (k:K {{name: '{k}'}}) CREATE (t)-[:IN]->(k)"
        ))
        .unwrap();
    }
    for k in ["X", "Y"] {
        db.execute(&format!(
            "MATCH (k:K {{name: '{k}'}}), (z:Z) CREATE (k)-[:OF]->(z)"
        ))
        .unwrap();
    }
    for (a, b) in [(1, 2), (2, 3)] {
        db.execute(&format!(
            "MATCH (a:Q {{i: {a}}}), (b:Q {{i: {b}}}) CREATE (a)-[:N]->(b)"
        ))
        .unwrap();
    }
    (dir, db)
}

fn rows(db: &GraphDb, q: &str) -> Vec<Vec<Value>> {
    db.execute(q)
        .unwrap_or_else(|e| panic!("query must succeed: {q}\n  error: {e}"))
        .rows
}

fn single(db: &GraphDb, q: &str) -> Value {
    let r = rows(db, q);
    assert_eq!(r.len(), 1, "expected exactly one row for {q}: {r:?}");
    assert_eq!(r[0].len(), 1, "expected exactly one column for {q}: {r:?}");
    r[0][0].clone()
}

// ── the cells listed in the issue ────────────────────────────────────────────

#[test]
fn count_distinct_one_hop_by_t() {
    let (_d, db) = fixture();
    // 7 edges reach the 3 distinct targets A, B, C.
    assert_eq!(
        single(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN COUNT(DISTINCT t.name) AS v"
        ),
        i(3)
    );
    // Control: the non-distinct count is the edge count.
    assert_eq!(
        single(&db, "MATCH (p:P)-[:HAS]->(t:T) RETURN COUNT(t.name) AS v"),
        i(7)
    );
}

#[test]
fn count_distinct_one_hop_by_g() {
    let (_d, db) = fixture();
    // Every p has at least one edge: g in {x, y}.
    assert_eq!(
        single(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN COUNT(DISTINCT p.g) AS v"
        ),
        i(2)
    );
}

#[test]
fn count_distinct_two_hop() {
    let (_d, db) = fixture();
    // t in {A,B,C} -> k in {Y,X,X}: 3 paths, 2 distinct k.
    assert_eq!(
        single(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K) RETURN COUNT(DISTINCT k.name) AS v"
        ),
        i(2)
    );
}

#[test]
fn count_distinct_three_hop() {
    let (_d, db) = fixture();
    // Every p-t-k path ends in the single Z: many paths, one distinct z.
    assert_eq!(
        single(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T)-[:IN]->(k:K)-[:OF]->(z:Z) RETURN COUNT(DISTINCT z.name) AS v"
        ),
        i(1)
    );
}

#[test]
fn count_distinct_varlen() {
    let (_d, db) = fixture();
    // N*1..2 from each q: 1->2, 1->3, 2->3 => b.i = 2,3,3 => 2 distinct.
    assert_eq!(
        single(
            &db,
            "MATCH (a:Q)-[:N*1..2]->(b:Q) RETURN COUNT(DISTINCT b.i) AS v"
        ),
        i(2)
    );
}

#[test]
fn count_distinct_grouped_ordered_on_the_aggregate() {
    let (_d, db) = fixture();
    // P1 -> {A,B,C}=3, P2 -> {A,B}=2, P3 -> {B,C}=2; ordered by c DESC, p.id.
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN p.id, COUNT(DISTINCT t.name) AS c ORDER BY c DESC, p.id"
        ),
        vec![vec![i(1), i(3)], vec![i(2), i(2)], vec![i(3), i(2)]]
    );
}

#[test]
fn count_distinct_single_node_match() {
    let (_d, db) = fixture();
    // Not shape specific: P.g = x, y, y.
    assert_eq!(
        single(&db, "MATCH (p:P) RETURN COUNT(DISTINCT p.g) AS v"),
        i(2)
    );
}

// ── semantics ────────────────────────────────────────────────────────────────

#[test]
fn distinct_across_groups_is_per_group_not_global() {
    let (_d, db) = fixture();
    // g=x: P1 -> {A,B,C} = 3.  g=y: P2 {A,B} + P3 {B,C} -> {A,B,C} = 3 (4 if
    // it were not deduplicated: A,B,B,C).  A global dedup would give x=3, y=0.
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN p.g, COUNT(DISTINCT t.name) AS c ORDER BY p.g"
        ),
        vec![vec![s("x"), i(3)], vec![s("y"), i(3)]]
    );
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN p.g, COUNT(t.name) AS c ORDER BY p.g"
        ),
        vec![vec![s("x"), i(3)], vec![s("y"), i(4)]]
    );
}

#[test]
fn distinct_and_plain_aggregates_in_one_return() {
    let (_d, db) = fixture();
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) \
             RETURN COUNT(t.name) AS plain, COUNT(DISTINCT t.name) AS dist, COUNT(*) AS star"
        ),
        vec![vec![i(7), i(3), i(7)]]
    );
}

#[test]
fn count_distinct_on_node_variables() {
    let (_d, db) = fixture();
    // 3 distinct sources and 3 distinct targets over the 7 edges.
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN COUNT(DISTINCT p) AS ps, COUNT(DISTINCT t) AS ts"
        ),
        vec![vec![i(3), i(3)]]
    );
    assert_eq!(
        single(&db, "MATCH (p:P) RETURN COUNT(DISTINCT p) AS v"),
        i(3)
    );
}

#[test]
fn count_distinct_over_nodes_with_identical_properties_counts_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    // Three distinct nodes, two of which carry identical properties.
    db.execute("CREATE (:Dup {v: 1})").unwrap();
    db.execute("CREATE (:Dup {v: 1})").unwrap();
    db.execute("CREATE (:Dup {v: 2})").unwrap();
    assert_eq!(
        single(&db, "MATCH (n:Dup) RETURN COUNT(DISTINCT n) AS v"),
        i(3)
    );
}

fn w_fixture() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    // v = 1, 1, 2.
    db.execute("CREATE (:W {v: 1})").unwrap();
    db.execute("CREATE (:W {v: 1})").unwrap();
    db.execute("CREATE (:W {v: 2})").unwrap();
    (dir, db)
}

#[test]
fn nulls_are_ignored_by_distinct_aggregates() {
    let (_d, db) = w_fixture();
    // `CASE WHEN w.v = 1 THEN w.v END` is 1, 1, NULL.  (A property that is
    // merely absent from a node is not usable here: the aggregate input path
    // reads it as 0, not NULL — a separate, pre-existing bug.)
    assert_eq!(
        single(
            &db,
            "MATCH (w:W) RETURN COUNT(CASE WHEN w.v = 1 THEN w.v END) AS v"
        ),
        i(2),
        "control: plain COUNT skips only the NULL"
    );
    // distinct non-null values: {1}; the NULL is not a second value.
    assert_eq!(
        single(
            &db,
            "MATCH (w:W) RETURN COUNT(DISTINCT CASE WHEN w.v = 1 THEN w.v END) AS v"
        ),
        i(1)
    );
    assert_eq!(
        single(
            &db,
            "MATCH (w:W) RETURN SUM(DISTINCT CASE WHEN w.v = 2 THEN w.v END) AS v"
        ),
        i(2)
    );
    // Every input NULL: distinct count is 0, not 1.
    assert_eq!(
        single(
            &db,
            "MATCH (w:W) RETURN COUNT(DISTINCT CASE WHEN w.v = 99 THEN w.v END) AS v"
        ),
        i(0)
    );
}

#[test]
fn sum_avg_min_max_distinct() {
    let (_d, db) = w_fixture();
    // distinct values {1,2}: sum 3 (plain 1+1+2 = 4), avg 1.5 (plain 4/3).
    assert_eq!(
        single(&db, "MATCH (w:W) RETURN SUM(DISTINCT w.v) AS v"),
        i(3)
    );
    assert_eq!(single(&db, "MATCH (w:W) RETURN SUM(w.v) AS v"), i(4));
    assert_eq!(
        single(&db, "MATCH (w:W) RETURN AVG(DISTINCT w.v) AS v"),
        Value::Float64(1.5)
    );
    assert_eq!(
        single(&db, "MATCH (w:W) RETURN MIN(DISTINCT w.v) AS v"),
        i(1)
    );
    assert_eq!(
        single(&db, "MATCH (w:W) RETURN MAX(DISTINCT w.v) AS v"),
        i(2)
    );
}

fn sorted_strings(v: &Value) -> Vec<String> {
    let Value::List(items) = v else {
        panic!("expected a list, got {v:?}")
    };
    let mut out: Vec<String> = items
        .iter()
        .map(|x| match x {
            Value::String(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        })
        .collect();
    out.sort();
    out
}

#[test]
fn collect_distinct_is_a_duplicate_free_set() {
    let (_d, db) = fixture();
    // Compared as a sorted set: the engine does not define collect() order.
    let v = single(
        &db,
        "MATCH (p:P)-[:HAS]->(t:T) RETURN collect(DISTINCT t.name) AS v",
    );
    assert_eq!(sorted_strings(&v), vec!["A", "B", "C"]);
    // Control: plain collect keeps all 7.
    let v = single(&db, "MATCH (p:P)-[:HAS]->(t:T) RETURN collect(t.name) AS v");
    assert_eq!(sorted_strings(&v).len(), 7);
}

#[test]
fn collect_distinct_grouped() {
    let (_d, db) = fixture();
    let r = rows(
        &db,
        "MATCH (p:P)-[:HAS]->(t:T) RETURN p.g, collect(DISTINCT t.name) AS v ORDER BY p.g",
    );
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], s("x"));
    assert_eq!(sorted_strings(&r[0][1]), vec!["A", "B", "C"]);
    assert_eq!(r[1][0], s("y"));
    assert_eq!(sorted_strings(&r[1][1]), vec!["A", "B", "C"]);
}

#[test]
fn distinct_aggregate_over_empty_input() {
    let (_d, db) = fixture();
    // A global aggregate over zero rows still yields exactly one row.
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P) WHERE p.id = 99 RETURN COUNT(DISTINCT p.g) AS v"
        ),
        vec![vec![i(0)]]
    );
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P) WHERE p.id = 99 RETURN SUM(DISTINCT p.id) AS s, collect(DISTINCT p.g) AS c"
        ),
        vec![vec![i(0), Value::List(vec![])]]
    );
    // With a grouping key and no rows there are no output rows.
    assert!(rows(
        &db,
        "MATCH (p:P) WHERE p.id = 99 RETURN p.g, COUNT(DISTINCT p.id) AS v"
    )
    .is_empty());
}

#[test]
fn order_by_distinct_aggregate_and_limit() {
    let (_d, db) = fixture();
    // Per T: A <- {P1,P2}, B <- {P1,P2,P3}, C <- {P1,P3}; D has no edges and
    // is not matched.  Ordered by distinct-count ascending then name.
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN t.name, COUNT(DISTINCT p.g) AS c ORDER BY c, t.name"
        ),
        vec![vec![s("A"), i(2)], vec![s("B"), i(2)], vec![s("C"), i(2)]]
    );
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P)-[:HAS]->(t:T) RETURN p.id, COUNT(DISTINCT t.name) AS c ORDER BY c DESC, p.id LIMIT 1"
        ),
        vec![vec![i(1), i(3)]]
    );
}

#[test]
fn distinct_aggregate_in_optional_match() {
    let (_d, db) = fixture();
    // Grouped by p.g: x = P1 {A,B,C} = 3; y = P2 {A,B} + P3 {B,C} -> {A,B,C}
    // = 3 distinct (4 plain).
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P) OPTIONAL MATCH (p)-[:HAS]->(t:T) \
             RETURN p.g, COUNT(DISTINCT t.name) AS c ORDER BY p.g"
        ),
        vec![vec![s("x"), i(3)], vec![s("y"), i(3)]]
    );
    assert_eq!(
        rows(
            &db,
            "MATCH (p:P) OPTIONAL MATCH (p)-[:HAS]->(t:T) \
             RETURN p.g, COUNT(t.name) AS c ORDER BY p.g"
        ),
        vec![vec![s("x"), i(3)], vec![s("y"), i(4)]],
        "control: plain count"
    );
}

#[test]
fn distinct_aggregate_in_with_clause() {
    let (_d, db) = w_fixture();
    // WITH-aggregation (`aggregate_with_items`).  Group v=1 has 2 nodes but
    // its distinct w.v is {1}; v=2 has {2}.
    assert_eq!(
        rows(
            &db,
            "MATCH (w:W) WITH w.v AS v, COUNT(DISTINCT w.v) AS c RETURN v, c ORDER BY v"
        ),
        vec![vec![i(1), i(1)], vec![i(2), i(1)]]
    );
    // Global: 3 rows, 2 distinct values (plain COUNT would be 3).
    assert_eq!(
        single(&db, "MATCH (w:W) WITH COUNT(DISTINCT w.v) AS c RETURN c"),
        i(2)
    );
    assert_eq!(
        single(&db, "MATCH (w:W) WITH SUM(DISTINCT w.v) AS c RETURN c"),
        i(3)
    );
    let v = single(&db, "MATCH (w:W) WITH collect(DISTINCT w.v) AS c RETURN c");
    let Value::List(mut items) = v else {
        panic!("expected list")
    };
    items.sort_by_key(|x| match x {
        Value::Int64(n) => *n,
        _ => i64::MAX,
    });
    assert_eq!(items, vec![i(1), i(2)]);
}

#[test]
fn parallel_edges_are_deduplicated_not_degree_counted() {
    // The degree-cache fast path (MATCH (n:L)-[]->(f) RETURN n.prop, COUNT(f)
    // ORDER BY c DESC LIMIT k) counts edges; COUNT(DISTINCT f) must not use it.
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    db.execute("CREATE (:S {name: 'a'})").unwrap();
    db.execute("CREATE (:S {name: 'b'})").unwrap();
    db.execute("CREATE (:U {name: 'u'})").unwrap();
    db.execute("CREATE (:U {name: 'v'})").unwrap();
    for (s_, u) in [("a", "u"), ("a", "u"), ("a", "v"), ("b", "u")] {
        db.execute(&format!(
            "MATCH (s:S {{name: '{s_}'}}), (u:U {{name: '{u}'}}) CREATE (s)-[:R]->(u)"
        ))
        .unwrap();
    }
    // a: u,u,v -> 2 distinct (3 edges); b: u -> 1.
    assert_eq!(
        rows(
            &db,
            "MATCH (s:S)-[:R]->(f:U) RETURN s.name, COUNT(DISTINCT f) AS c ORDER BY c DESC LIMIT 2"
        ),
        vec![vec![s("a"), i(2)], vec![s("b"), i(1)]]
    );
    // And the plain count still reports edges (3 and 1).
    assert_eq!(
        rows(
            &db,
            "MATCH (s:S)-[:R]->(f:U) RETURN s.name, COUNT(f) AS c ORDER BY c DESC LIMIT 2"
        ),
        vec![vec![s("a"), i(3)], vec![s("b"), i(1)]]
    );
}

// ── parse-time rejections ────────────────────────────────────────────────────

#[test]
fn count_distinct_star_is_rejected() {
    let (_d, db) = fixture();
    assert!(db.execute("MATCH (p:P) RETURN COUNT(DISTINCT *)").is_err());
}

#[test]
fn distinct_inside_a_scalar_function_is_rejected() {
    let (_d, db) = fixture();
    assert!(db
        .execute("MATCH (p:P) RETURN toUpper(DISTINCT p.g)")
        .is_err());
}

#[test]
fn return_distinct_still_works_alongside() {
    let (_d, db) = fixture();
    assert_eq!(
        rows(&db, "MATCH (p:P) RETURN DISTINCT p.g ORDER BY p.g"),
        vec![vec![s("x")], vec![s("y")]]
    );
}
