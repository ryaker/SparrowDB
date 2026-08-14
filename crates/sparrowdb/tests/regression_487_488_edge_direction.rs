//! Regression tests for GitHub issues #487 and #488, filed while investigating
//! #486 and confirmed by the team lead as reproducing on top of #486's fix.
//!
//! #486 fixed `execute_one_hop` (single relationship) to honor a left-pointing
//! (`<-`) pattern. Two sibling functions shared the same defect class and were
//! left unfixed there, deliberately out of scope for that PR:
//!
//! - #487 — `execute_two_hop` (crates/sparrowdb-execution/src/engine/hop.rs,
//!   `execute_two_hop`) never read `rels[0].dir` (the *first* hop of a 2-rel
//!   pattern); only the second hop's direction was handled (the pre-existing
//!   SPA-201 / #294 fix). A left-pointing first hop was silently treated as
//!   if it had been written `->`.
//! - #488 — `execute_n_hop` (3+ rel chains) fail-closed correctly for a
//!   variable-length hop with non-Outgoing direction, but had no such guard
//!   for a *fixed* hop — the fixed-hop neighbour-gathering branch always
//!   walked the forward CSR/delta regardless of `RelPattern::dir`.
//!
//! Both bugs return an EMPTY result rather than a wrong-but-nonempty one
//! (unlike #486's single-hop case, which invented rows) — for a question
//! inbound traversal exists to answer ("what transitively depends on this"),
//! an empty result is arguably the more dangerous failure: it reads as
//! "nothing does".
//!
//! The fix threads a `first_hop_incoming` / per-hop `incoming` flag through
//! both functions, mirroring the existing `second_hop_incoming` (SPA-201)
//! pattern: a new `Engine::csr_predecessors_labeled` (the backward mirror of
//! `csr_neighbors_labeled`) plus a per-table `CsrBackward` built the same way
//! `execute_one_hop`'s `Both` backward pass and `execute_two_hop`'s existing
//! `merged_bwd_csr` already do, so checkpointed edges are covered; the delta
//! log is scanned in the opposite direction (dst-match instead of src-match)
//! for edges written since the last checkpoint.
//!
//! All expected values are derived by hand from each fixture's create calls,
//! never captured from program output. Tests are duplicated pre- and
//! post-CHECKPOINT where practical, because the delta-log path and the
//! persisted-CSR path (`csr_predecessors_labeled`, `hop1_bwd_csr`) are
//! genuinely different code and a bug in either would otherwise go unnoticed
//! — none of this repo's existing MATCH e2e tests call CHECKPOINT, so the
//! CSR-backed backward path was previously untested by any file in this repo.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn collect_strings(rows: &[Vec<Value>]) -> Vec<String> {
    let mut v: Vec<String> = rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

/// The team lead's fixture, reproduced exactly: `a1 -[:R]-> b1 -[:S]-> c1 -[:T]-> d1`.
fn setup_chain(db: &sparrowdb::GraphDb) {
    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:B {id: 'b1'})").expect("b1");
    db.execute("CREATE (:C {id: 'c1'})").expect("c1");
    db.execute("CREATE (:D {id: 'd1'})").expect("d1");
    db.execute("MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .expect("a1->b1");
    db.execute("MATCH (b:B {id:'b1'}), (c:C {id:'c1'}) CREATE (b)-[:S]->(c)")
        .expect("b1->c1");
    db.execute("MATCH (c:C {id:'c1'}), (d:D {id:'d1'}) CREATE (c)-[:T]->(d)")
        .expect("c1->d1");
}

// ── #487: execute_two_hop, first-hop direction ───────────────────────────────
//
// Fixture: a1 -[:R]-> b1 -[:S]-> c1
//
//   (a:A)-[:R]->(b:B)-[:S]->(c:C)       RETURN c.id  → ["c1"]  (sanity: forward-forward still works)
//   (c:C)<-[:S]-(b:B)<-[:R]-(a:A)       RETURN a.id  → ["a1"]  (#487 — both hops written right-to-left)

#[test]
fn issue_487_forward_forward_sanity_pre_checkpoint() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    let result = db
        .execute("MATCH (a:A)-[:R]->(b:B)-[:S]->(c:C) RETURN c.id")
        .expect("forward-forward two-hop");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["c1".to_string()],
        "forward-forward two-hop must still return c1"
    );
}

#[test]
fn issue_487_two_hop_first_hop_incoming_pre_checkpoint() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    let result = db
        .execute("MATCH (c:C)<-[:S]-(b:B)<-[:R]-(a:A) RETURN a.id")
        .expect("reverse two-hop (delta-log path)");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["a1".to_string()],
        "#487: MATCH (c:C)<-[:S]-(b:B)<-[:R]-(a:A) must return a1, not an \
         empty result"
    );
}

#[test]
fn issue_487_two_hop_first_hop_incoming_post_checkpoint() {
    let (_dir, db) = make_db();
    setup_chain(&db);
    db.execute("CHECKPOINT").expect("checkpoint");

    let result = db
        .execute("MATCH (c:C)<-[:S]-(b:B)<-[:R]-(a:A) RETURN a.id")
        .expect("reverse two-hop (persisted-CSR path)");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["a1".to_string()],
        "#487: same query must also return a1 after CHECKPOINT, exercising \
         Engine::csr_predecessors_labeled / hop1_bwd_csr rather than the \
         delta log"
    );
}

// ── #487: mixed-direction hops — per-hop independence ────────────────────────
//
// The bug (and a naive fix) could plausibly apply a single swap to the whole
// query instead of per-hop, which passes an all-inbound test and fails a
// mixed one. These two cases pin each hop's direction independently.

/// `(a)<-[:R]-(b)-[:S]->(c)`: first hop Incoming, second hop Outgoing.
/// This is the case the pre-#487 code could not express at all — hop1 was
/// always assumed Outgoing, so with distinct labels A/B the physical rel
/// table for R (stored as B->A, since the edge below is created b1->a1) never
/// matched the old (sid=A, did=B) filter, and the query silently returned 0
/// rows no matter what CREATE existed.
///
/// Fixture: b1 -[:R]-> a1 (so `(a)<-[:R]-(b)` holds), b1 -[:S]-> c1 (so
/// `(b)-[:S]->(c)` holds).
///
/// Expected: `(a:A)<-[:R]-(b:B)-[:S]->(c:C) RETURN c.id` → ["c1"].
#[test]
fn issue_487_mixed_incoming_then_outgoing() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:B {id: 'b1'})").expect("b1");
    db.execute("CREATE (:C {id: 'c1'})").expect("c1");
    db.execute("MATCH (b:B {id:'b1'}), (a:A {id:'a1'}) CREATE (b)-[:R]->(a)")
        .expect("b1->a1");
    db.execute("MATCH (b:B {id:'b1'}), (c:C {id:'c1'}) CREATE (b)-[:S]->(c)")
        .expect("b1->c1");

    let result = db
        .execute("MATCH (a:A)<-[:R]-(b:B)-[:S]->(c:C) RETURN c.id")
        .expect("incoming-then-outgoing two-hop");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["c1".to_string()],
        "#487: MATCH (a:A)<-[:R]-(b:B)-[:S]->(c:C) must return c1"
    );
}

/// `(a)-[:R]->(b)<-[:S]-(c)`: first hop Outgoing, second hop Incoming — the
/// pre-existing SPA-201/#294 "mutual friends" shape. Included as an explicit
/// regression guard: the #487 fix touches the same function and must not
/// disturb this already-correct case.
///
/// Fixture: a1 -[:R]-> m1, c1 -[:S]-> m1 (both point INTO m1).
///
/// Expected: `(a:A)-[:R]->(m:M)<-[:S]-(c:C) RETURN c.id` → ["c1"].
#[test]
fn issue_487_mixed_outgoing_then_incoming_regression_guard() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:M {id: 'm1'})").expect("m1");
    db.execute("CREATE (:C {id: 'c1'})").expect("c1");
    db.execute("MATCH (a:A {id:'a1'}), (m:M {id:'m1'}) CREATE (a)-[:R]->(m)")
        .expect("a1->m1");
    db.execute("MATCH (c:C {id:'c1'}), (m:M {id:'m1'}) CREATE (c)-[:S]->(m)")
        .expect("c1->m1");

    let result = db
        .execute("MATCH (a:A)-[:R]->(m:M)<-[:S]-(c:C) RETURN c.id")
        .expect("outgoing-then-incoming two-hop");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["c1".to_string()],
        "outgoing-then-incoming (SPA-201 shape) must still return c1"
    );
}

// ── #487: multiple predecessors on the (Incoming, Incoming) path ────────────
//
// Both hops Incoming, and TWO distinct nodes satisfy the final (leftmost)
// hop — proves the fix returns every match, not just the first.
//
// Fixture: a1 -[:R]-> m1, a2 -[:R]-> m1, m1 -[:S]-> c1.
//
// Expected: `(c:C)<-[:S]-(m:M)<-[:R]-(a:A) RETURN a.id` → ["a1", "a2"].
#[test]
fn issue_487_two_hop_double_incoming_multiple_matches() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:A {id: 'a2'})").expect("a2");
    db.execute("CREATE (:M {id: 'm1'})").expect("m1");
    db.execute("CREATE (:C {id: 'c1'})").expect("c1");
    db.execute("MATCH (a:A {id:'a1'}), (m:M {id:'m1'}) CREATE (a)-[:R]->(m)")
        .expect("a1->m1");
    db.execute("MATCH (a:A {id:'a2'}), (m:M {id:'m1'}) CREATE (a)-[:R]->(m)")
        .expect("a2->m1");
    db.execute("MATCH (m:M {id:'m1'}), (c:C {id:'c1'}) CREATE (m)-[:S]->(c)")
        .expect("m1->c1");

    let result = db
        .execute("MATCH (c:C)<-[:S]-(m:M)<-[:R]-(a:A) RETURN a.id")
        .expect("double-incoming two-hop, multiple matches");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["a1".to_string(), "a2".to_string()],
        "#487: double-incoming two-hop must return every matching \
         predecessor, not just one"
    );
}

// ── #488: execute_n_hop, fixed (non-variable-length) hop direction ──────────
//
// Fixture: a1 -[:R]-> b1 -[:S]-> c1 -[:T]-> d1  (3 fixed hops, 4 nodes)
//
//   (dd:D)<-[:T]-(c:C)<-[:S]-(b:B)<-[:R]-(a:A) RETURN a.id  → ["a1"]

#[test]
fn issue_488_three_hop_all_incoming_pre_checkpoint() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    let result = db
        .execute("MATCH (dd:D)<-[:T]-(c:C)<-[:S]-(b:B)<-[:R]-(a:A) RETURN a.id")
        .expect("3-hop all-incoming chain (delta-log path)");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["a1".to_string()],
        "#488: MATCH (dd:D)<-[:T]-(c:C)<-[:S]-(b:B)<-[:R]-(a:A) must return \
         a1, not an empty result"
    );
}

#[test]
fn issue_488_three_hop_all_incoming_post_checkpoint() {
    let (_dir, db) = make_db();
    setup_chain(&db);
    db.execute("CHECKPOINT").expect("checkpoint");

    let result = db
        .execute("MATCH (dd:D)<-[:T]-(c:C)<-[:S]-(b:B)<-[:R]-(a:A) RETURN a.id")
        .expect("3-hop all-incoming chain (persisted-CSR path)");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["a1".to_string()],
        "#488: same chain must also return a1 after CHECKPOINT, exercising \
         csr_predecessors_labeled in execute_n_hop's fixed-hop branch"
    );
}

/// Every intermediate binding, not just the final one, derived by hand:
/// dd=d1, c=c1, b=b1, a=a1 walking the chain backward from d1.
#[test]
fn issue_488_three_hop_all_incoming_intermediate_bindings() {
    let (_dir, db) = make_db();
    setup_chain(&db);

    let result = db
        .execute(
            "MATCH (dd:D)<-[:T]-(c:C)<-[:S]-(b:B)<-[:R]-(a:A) \
             RETURN dd.id, c.id, b.id, a.id",
        )
        .expect("3-hop all-incoming, all bindings");
    assert_eq!(result.rows.len(), 1, "expected exactly one path");
    assert_eq!(
        result.rows[0],
        vec![
            Value::String("d1".to_string()),
            Value::String("c1".to_string()),
            Value::String("b1".to_string()),
            Value::String("a1".to_string()),
        ],
        "#488: every intermediate node in the reversed chain must bind to \
         its correct value, not just the endpoint"
    );
}

/// Mixed 3-hop chain: first hop Outgoing, second hop Incoming, third hop
/// Outgoing — `(a)-[:R]->(b)<-[:S]-(c)-[:T]->(d)`. Exercises per-hop
/// independence in execute_n_hop the same way the two-hop mixed tests do.
///
/// Fixture: a1 -[:R]-> b1 (so a->b holds), c1 -[:S]-> b1 (so b<-S-c holds,
/// i.e. `(b)<-[:S]-(c)`), c1 -[:T]-> d1 (so c->d holds).
///
/// Expected: `(a:A)-[:R]->(b:B)<-[:S]-(c:C)-[:T]->(d:D) RETURN d.id` → ["d1"].
#[test]
fn issue_488_mixed_direction_three_hop_chain() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:B {id: 'b1'})").expect("b1");
    db.execute("CREATE (:C {id: 'c1'})").expect("c1");
    db.execute("CREATE (:D {id: 'd1'})").expect("d1");
    db.execute("MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .expect("a1->b1");
    db.execute("MATCH (c:C {id:'c1'}), (b:B {id:'b1'}) CREATE (c)-[:S]->(b)")
        .expect("c1->b1");
    db.execute("MATCH (c:C {id:'c1'}), (d:D {id:'d1'}) CREATE (c)-[:T]->(d)")
        .expect("c1->d1");

    let result = db
        .execute("MATCH (a:A)-[:R]->(b:B)<-[:S]-(c:C)-[:T]->(d:D) RETURN d.id")
        .expect("mixed-direction 3-hop chain");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["d1".to_string()],
        "#488: mixed-direction 3-hop chain must return d1"
    );
}
