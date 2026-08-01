//! Regression tests for #421 — variable-length path followed by further hops.
//!
//! `MATCH (a)-[:R*1..2]->(b)-[:S]->(c)` used to dispatch on relationship count
//! alone, so `execute_two_hop` demoted `*1..2` to a plain single hop and every
//! match at depth >= 2 was dropped without an error.
//!
//! # Fixture (built by `build_fixture`, all expectations derived from it by hand)
//!
//! Six `Person` nodes, slots 0..=5, `pid` = slot.
//! Two `City` nodes, slots 0..=1, `code` = 10 (slot 0) and 20 (slot 1).
//!
//! ```text
//! knows   (Person→Person):  0→1, 0→4, 1→2, 2→3, 4→5
//! livesIn (Person→City):    0→c0, 1→c0, 2→c1, 3→c1, 4→c1, 5→c0
//! ```
//!
//! Reachability from person 0 over `knows`:
//!   depth 1 → {1, 4}
//!   depth 2 → {2, 5}
//!   depth 3 → {3}
//!
//! So `-[:knows*1..2]->` from person 0 binds `{1, 4, 2, 5}` — the pre-fix code
//! returned only `{1, 4}`.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::types::Value;
use sparrowdb_execution::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};

const KNOWS: [(u64, u64); 5] = [(0, 1), (0, 4), (1, 2), (2, 3), (4, 5)];
const LIVES_IN: [(u64, u64); 6] = [(0, 0), (1, 0), (2, 1), (3, 1), (4, 1), (5, 0)];

/// City code for each city slot.
const CITY_CODES: [i64; 2] = [10, 20];

struct Fixture {
    _dir: tempfile::TempDir,
    engine: Engine,
}

fn build_fixture(chunked: bool) -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    let pid_col = sparrowdb_common::col_id_of("pid");
    let code_col = sparrowdb_common::col_id_of("code");

    let (knows_rel, lives_rel) = {
        let mut store = NodeStore::open(&path).expect("node store");
        let mut cat = Catalog::open(&path).expect("catalog");

        let person_label = cat.create_label("Person").expect("Person") as u32;
        let city_label = cat.create_label("City").expect("City") as u32;

        for pid in 0..6i64 {
            store
                .create_node(person_label, &[(pid_col, StoreValue::Int64(pid))])
                .expect("person");
        }
        for code in CITY_CODES {
            store
                .create_node(city_label, &[(code_col, StoreValue::Int64(code))])
                .expect("city");
        }

        let knows_rel = cat
            .create_rel_table(person_label as u16, person_label as u16, "knows")
            .expect("knows rel table") as u32;
        let lives_rel = cat
            .create_rel_table(person_label as u16, city_label as u16, "livesIn")
            .expect("livesIn rel table") as u32;

        (knows_rel, lives_rel)
    };

    // CSR per rel table. Source slots are Person slots in both cases.
    let mut csrs = std::collections::HashMap::new();
    csrs.insert(knows_rel, CsrForward::build(6, &KNOWS));
    csrs.insert(lives_rel, CsrForward::build(6, &LIVES_IN));

    let store = NodeStore::open(&path).expect("reopen store");
    let cat = Catalog::open(&path).expect("reopen catalog");
    let engine = Engine::new(store, cat, csrs, &path);
    let engine = if chunked {
        engine.with_chunked_pipeline()
    } else {
        engine
    };

    Fixture { _dir: dir, engine }
}

/// Collect `(int, int)` pairs from a two-column result, sorted for comparison.
fn int_pairs(result: &sparrowdb_execution::types::QueryResult) -> Vec<(i64, i64)> {
    let mut out: Vec<(i64, i64)> = result
        .rows
        .iter()
        .map(|row| {
            let a = match &row[0] {
                Value::Int64(v) => *v,
                other => panic!("expected Int64 in column 0, got {other:?}"),
            };
            let b = match &row[1] {
                Value::Int64(v) => *v,
                other => panic!("expected Int64 in column 1, got {other:?}"),
            };
            (a, b)
        })
        .collect();
    out.sort_unstable();
    out
}

fn ints(result: &sparrowdb_execution::types::QueryResult) -> Vec<i64> {
    let mut out: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

// ── Baseline: the varlen pattern on its own is (and was) correct ─────────────

#[test]
fn varlen_alone_reaches_depth_two() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute("MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person) RETURN DISTINCT b.pid")
        .expect("query ok");
    // depth 1 = {1, 4}; depth 2 = {2, 5}.
    assert_eq!(
        ints(&r),
        vec![1, 2, 4, 5],
        "knows*1..2 from person 0 reaches 1 and 4 at depth 1, 2 and 5 at depth 2"
    );
}

// ── #421: varlen + one trailing hop ──────────────────────────────────────────

#[test]
fn varlen_plus_trailing_hop_keeps_depth_two_matches() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City) \
             RETURN DISTINCT b.pid, c.code",
        )
        .expect("query ok");

    // b ∈ {1, 4} at depth 1 and {2, 5} at depth 2.
    // livesIn: 1→c0(10), 4→c1(20), 2→c1(20), 5→c0(10).
    assert_eq!(
        int_pairs(&r),
        vec![(1, 10), (2, 20), (4, 20), (5, 10)],
        "every depth-1 and depth-2 friend must be joined to its city; \
         before #421 was fixed only the depth-1 pair set {{(1,10),(4,20)}} came back"
    );
}

#[test]
fn varlen_plus_trailing_hop_enumerates_paths_without_distinct() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City) \
             RETURN b.pid, c.code",
        )
        .expect("query ok");

    // Without DISTINCT the traversal enumerates simple paths. From person 0
    // there is exactly one simple path to each of 1, 4, 2 and 5 within 2 hops
    // (0→1, 0→4, 0→1→2, 0→4→5), so each contributes exactly one row.
    assert_eq!(
        int_pairs(&r),
        vec![(1, 10), (2, 20), (4, 20), (5, 10)],
        "one row per simple path"
    );
}

#[test]
fn varlen_plus_trailing_hop_respects_where_on_final_node() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City) \
             WHERE c.code = 20 RETURN DISTINCT b.pid",
        )
        .expect("query ok");
    // City code 20 is city slot 1; residents among {1,4,2,5} are 4 and 2.
    assert_eq!(
        ints(&r),
        vec![2, 4],
        "WHERE on the trailing hop's node must filter the joined rows"
    );
}

#[test]
fn varlen_plus_trailing_hop_respects_inline_prop_on_final_node() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City {code: 10}) \
             RETURN DISTINCT b.pid",
        )
        .expect("query ok");
    // City code 10 is city slot 0; residents among {1,4,2,5} are 1 and 5.
    assert_eq!(
        ints(&r),
        vec![1, 5],
        "inline prop filter on the trailing hop's node must be applied"
    );
}

#[test]
fn varlen_min_two_plus_trailing_hop_excludes_depth_one() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*2..2]->(b:Person)-[:livesIn]->(c:City) \
             RETURN DISTINCT b.pid",
        )
        .expect("query ok");
    assert_eq!(
        ints(&r),
        vec![2, 5],
        "min_hops = 2 must exclude the direct friends 1 and 4"
    );
}

#[test]
fn varlen_zero_hop_plus_trailing_hop_includes_source() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*0..1]->(b:Person)-[:livesIn]->(c:City) \
             RETURN DISTINCT b.pid",
        )
        .expect("query ok");
    // *0..1 binds b to person 0 itself plus the depth-1 friends 1 and 4.
    assert_eq!(
        ints(&r),
        vec![0, 1, 4],
        "min_hops = 0 must bind the source node itself as well"
    );
}

// ── #421: varlen followed by two fixed hops (3-rel chain) ────────────────────

#[test]
fn varlen_plus_two_trailing_hops() {
    let mut f = build_fixture(false);
    // City has no outgoing edges, so route the third hop back through knows:
    // (a)-[:knows*1..2]->(b)-[:knows]->(d)-[:livesIn]->(c)
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:knows]->(d:Person) \
             -[:livesIn]->(c:City) RETURN DISTINCT d.pid, c.code",
        )
        .expect("query ok");
    // b ∈ {1, 4, 2, 5}. Outgoing knows: 1→2, 4→5, 2→3, 5→none.
    // d ∈ {2, 5, 3}. livesIn: 2→c1(20), 5→c0(10), 3→c1(20).
    assert_eq!(
        int_pairs(&r),
        vec![(2, 20), (3, 20), (5, 10)],
        "a varlen hop followed by two fixed hops must join all three levels"
    );
}

// ── #421: a leading fixed hop followed by a varlen hop ──────────────────────

#[test]
fn leading_fixed_hop_then_varlen() {
    let mut f = build_fixture(false);
    // The quantifier is on the *second* relationship, not the first.
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows]->(b:Person)-[:knows*1..2]->(d:Person) \
             RETURN DISTINCT d.pid",
        )
        .expect("query ok");
    // b ∈ {1, 4}. From 1: depth 1 = {2}, depth 2 = {3}. From 4: depth 1 = {5}.
    assert_eq!(
        ints(&r),
        vec![2, 3, 5],
        "a varlen quantifier must be honoured when it is not the first hop"
    );
}

// ── #421: shapes deliberately left unsupported must error, not mislead ──────

#[test]
fn incoming_varlen_in_a_chain_is_rejected() {
    let mut f = build_fixture(false);
    // `execute_variable_hops` walks outgoing edges only; answering an incoming
    // quantifier with outgoing edges would be silently wrong.
    let r = f.engine.execute(
        "MATCH (a:Person {pid: 3})<-[:knows*1..2]-(b:Person)-[:livesIn]->(c:City) \
         RETURN b.pid",
    );
    assert!(
        matches!(r, Err(sparrowdb_common::Error::Unimplemented)),
        "an incoming variable-length hop must be rejected, not answered; got {r:?}"
    );
}

// ── #421: the chunked pipeline must not bypass the new support ───────────────

#[test]
fn chunked_pipeline_does_not_bypass_varlen_plus_hop() {
    let mut f = build_fixture(true);
    let r = f
        .engine
        .execute(
            "MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City) \
             RETURN DISTINCT b.pid, c.code",
        )
        .expect("query ok");
    assert_eq!(
        int_pairs(&r),
        vec![(1, 10), (2, 20), (4, 20), (5, 10)],
        "the chunked pipeline must fall back to the varlen chain executor, \
         not answer the pattern with a plain two-hop plan"
    );
}

#[test]
fn chunked_pipeline_varlen_alone_still_correct() {
    let mut f = build_fixture(true);
    let r = f
        .engine
        .execute("MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person) RETURN DISTINCT b.pid")
        .expect("query ok");
    assert_eq!(ints(&r), vec![1, 2, 4, 5]);
}

// ── #421: OPTIONAL MATCH must not swallow the pattern into NULL rows ─────────

#[test]
fn optional_match_varlen_plus_hop_returns_real_rows() {
    let mut f = build_fixture(false);
    let r = f
        .engine
        .execute(
            "OPTIONAL MATCH (a:Person {pid: 0})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City) \
             RETURN DISTINCT b.pid, c.code",
        )
        .expect("query ok");
    assert_eq!(
        int_pairs(&r),
        vec![(1, 10), (2, 20), (4, 20), (5, 10)],
        "OPTIONAL MATCH absorbs InvalidArgument into a NULL row, so a rejected \
         pattern would surface here as nulls rather than an error"
    );
}

#[test]
fn optional_match_varlen_plus_hop_no_match_yields_null_row() {
    let mut f = build_fixture(false);
    // Person 3 has no outgoing knows edges, so the varlen expansion is empty.
    let r = f
        .engine
        .execute(
            "OPTIONAL MATCH (a:Person {pid: 3})-[:knows*1..2]->(b:Person)-[:livesIn]->(c:City) \
             RETURN b.pid, c.code",
        )
        .expect("query ok");
    assert_eq!(r.rows.len(), 1, "OPTIONAL MATCH must yield one NULL row");
    assert!(
        r.rows[0].iter().all(|v| matches!(v, Value::Null)),
        "all columns must be NULL; got {:?}",
        r.rows[0]
    );
}

// ── #421: a varlen hop in a pipeline MATCH stage is rejected, not truncated ──

#[test]
fn pipeline_match_stage_rejects_varlen() {
    let mut f = build_fixture(false);
    // `execute_pipeline_match_hop` only ever takes a single step along rels[0]
    // and ignores the quantifier — the same silent truncation as #421.
    let err = f.engine.execute(
        "MATCH (a:Person {pid: 0}) WITH a MATCH (a)-[:knows*1..2]->(b:Person) RETURN b.pid",
    );
    match err {
        Err(sparrowdb_common::Error::InvalidArgument(msg)) => {
            assert!(
                msg.contains("421"),
                "error should name the tracking issue; got {msg}"
            );
        }
        other => panic!("expected InvalidArgument rejecting the pattern, got {other:?}"),
    }
}
