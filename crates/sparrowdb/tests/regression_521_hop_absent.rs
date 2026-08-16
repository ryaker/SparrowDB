//! Regression guards for issue #521 — the same "absent conflated with zero"
//! defect #479/#522 fixed in mutation.rs/expr.rs, found on 4 remaining
//! call sites in hop.rs's `execute_one_hop`: `get_node_raw` zero-sentinels
//! an absent column to `Ok(0)` (decoding to `Value::Int64(0)`, never
//! `None`), so a genuinely null-bound `$param` prop filter could never
//! match the node whose property is actually absent on these traversal
//! paths — silent under-matching (same direction as #472/#479, on the
//! traversal path rather than the pipeline/mutation path).
//!
//! All expected values below are derived by hand from each fixture, never
//! captured from a prior run (repo rule — see CLAUDE.md). Each test's
//! pre-fix failure output is quoted in the PR description, captured by
//! running these tests against the parent commit.
//!
//! ── Site coverage ───────────────────────────────────────────────────────
//! - hop.rs:247 (src_props, forward pass)  → `src_prop_filter_forward_hop_matches_absent_node`
//! - hop.rs:349 (dst_props, batch-miss fallback) → NOT independently testable;
//!   see the doc comment on `dst_fallback_site_is_unreachable_note` below.
//! - hop.rs:612 (b_props, backward "a"-role)     → `undirected_backward_a_role_filter_matches_absent_node`
//! - hop.rs:683 (a_props, backward "b"-role)     → `undirected_backward_b_role_filter_matches_absent_node`
//!
//! Each undirected test is built so the *forward* pass contributes either
//! zero rows or only bug-independent rows (its dst-side filter always lands
//! on a genuinely-*present* property, which both the old zero-sentineled
//! `get_node_raw` and the fix read identically) — so a difference in the
//! final row count is attributable only to the backward-pass site under
//! test, not to the still-untouched `batch_read_node_props` path noted in
//! the PR description's findings section.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;
use tempfile::tempdir;

fn open_db() -> (GraphDb, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    (db, dir)
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

// ═══════════════════════════════════════════════════════════════════════
// Site hop.rs:247 — src_props, forward pass (directed, no backward pass)
// ═══════════════════════════════════════════════════════════════════════

// `MATCH (a:A {tag:$t})-[:R]->(b:B) RETURN a.id` reaches `execute_one_hop`
// (the inline node prop filter bails the chunked one-hop path — see
// `can_use_one_hop_chunked`'s "Inline prop filters on node patterns" guard).
// Direction is Outgoing, so the undirected backward pass never runs —
// this test exercises hop.rs:247 in isolation.
//
// Fixture: a1 has no `tag` (absent), a2 has `tag:'x'` (present); both have
// an edge to b1. Hand-derived: a null-bound $t matches only the node whose
// property is genuinely absent (mirrors the `Null == Null` convention
// `matches_prop_filter_static` already documents) — only a1.
#[test]
fn src_prop_filter_forward_hop_matches_absent_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:A {id: 'a1'})").unwrap(); // no `tag`
    db.execute("CREATE (:A {id: 'a2', tag: 'x'})").unwrap(); // `tag` present
    db.execute("CREATE (:B {id: 'b1'})").unwrap();
    db.execute("MATCH (a:A {id:'a1'}),(b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .unwrap();
    db.execute("MATCH (a:A {id:'a2'}),(b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .unwrap();

    let r = db
        .execute_with_params(
            "MATCH (a:A {tag: $t})-[:R]->(b:B) RETURN a.id ORDER BY a.id",
            params(vec![("t", Value::Null)]),
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        vec![vec![Value::String("a1".into())]],
        "one-hop forward src-side filter: only the absent-tag node (a1) may \
         match a null-bound $t; a2 (tag='x', present) must not; got {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Site hop.rs:612 — b_props, backward pass "a"-role filter
// ═══════════════════════════════════════════════════════════════════════

// `MATCH (a:Person {tag:$t})-[r]-(b:Person) RETURN a.name, b.name` — dir is
// Both (undirected), so the backward pass (SPA-193) runs, re-scanning the
// same rel table's dst-label pool as candidate "a" values via `b_props`.
// The `b` side has no inline filter, so `dst_node_pat.props` is empty and
// hop.rs:683 (a_props) is exercised only vacuously here — this isolates
// hop.rs:612.
//
// Fixture: Alice (no tag) --KNOWS--> Bob (tag='x'); Carol (tag='y')
// --KNOWS--> Alice. Bob and Carol both have their (irrelevant) tag column
// genuinely *present*, so their exclusion from the forward src-role and
// from the backward "a"-role is correct regardless of the fix — it does
// not depend on absent-vs-zero at all, keeping this test's pass/fail tied
// only to Alice's absent-tag read.
//
// Hand-derived:
// - Forward pass: only Alice's own filter passes (absent tag) → row
//   (Alice, Bob) via her one outgoing edge.
// - Backward pass: scanning Bob/Carol/Alice as candidate "a" via b_props —
//   Bob and Carol are excluded (tag present, fails `stored_val.is_none()`
//   regardless of fix). Alice's absent tag must pass via a fixed hop.rs:612,
//   surfacing her one predecessor, Carol, as "b" → row (Alice, Carol).
// - Total: {(Alice,Bob), (Alice,Carol)}. Pre-fix, hop.rs:247 (forward
//   src_props) and hop.rs:612 (backward b_props) share the same bug, so
//   Alice's absent tag is misread as `Some(0)` (never `None`) on *both*
//   passes — she is wrongly excluded as forward src too, not just from the
//   backward branch, so the pre-fix result is empty, not `[(Alice,Bob)]`.
//   Confirmed by running this test against the pre-fix commit: got `[]`.
#[test]
fn undirected_backward_a_role_filter_matches_absent_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap(); // no `tag`
    db.execute("CREATE (:Person {name: 'Bob', tag: 'x'})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Carol', tag: 'y'})")
        .unwrap();
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute(
        "MATCH (a:Person {name:'Carol'}),(b:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let r = db
        .execute_with_params(
            "MATCH (a:Person {tag: $t})-[r]-(b:Person) RETURN a.name, b.name \
             ORDER BY a.name, b.name",
            params(vec![("t", Value::Null)]),
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        vec![
            vec![Value::String("Alice".into()), Value::String("Bob".into())],
            vec![
                Value::String("Alice".into()),
                Value::String("Carol".into())
            ],
        ],
        "undirected backward pass: Alice (absent tag) must surface via both \
         the forward edge to Bob and the backward edge from Carol; Bob and \
         Carol (tag present) must never appear as 'a'; got {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Site hop.rs:683 — a_props, backward pass "b"-role filter
// ═══════════════════════════════════════════════════════════════════════

// Mirrors the previous test with the filter moved to the "b" side instead:
// `MATCH (a:Person)-[r]-(b:Person {tag:$t}) RETURN a.name, b.name`. `a` is
// unfiltered, so hop.rs:612 (b_props) is only exercised vacuously here —
// this isolates hop.rs:683.
//
// Same fixture as above (Alice absent tag; Bob, Carol present).
//
// Hand-derived:
// - Forward pass: `a` unfiltered so every node is tried as src, but the
//   `b`-side filter is evaluated through the *unmodified* batch-read path
//   (`batch_read_node_props`, hop.rs:308 — flagged as a separate, broader
//   finding in the PR description, not fixed here). That path reads
//   present-tag columns correctly regardless: Alice→Bob is excluded because
//   Bob's tag is genuinely present ('x'). Carol→Alice is also excluded, but
//   for the *wrong* reason (the untouched batch path zero-sentinels Alice's
//   absent tag) — this happens to make the forward pass contribute zero
//   rows in both the pre-fix and post-fix runs, so it does not affect this
//   test's ability to isolate hop.rs:683.
// - Backward pass: scanning Alice/Bob/Carol as candidate "b" via a_props —
//   at b_slot=Bob, predecessor Alice is tested via a_props against the
//   `b`-filter (hop.rs:683). Alice's absent tag must pass under the fix.
//   At b_slot=Alice, predecessor Carol is tested the same way; Carol's
//   present tag correctly fails regardless of the fix.
// - Total: exactly one row, (Bob, Alice), present only post-fix.
#[test]
fn undirected_backward_b_role_filter_matches_absent_node() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap(); // no `tag`
    db.execute("CREATE (:Person {name: 'Bob', tag: 'x'})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Carol', tag: 'y'})")
        .unwrap();
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute(
        "MATCH (a:Person {name:'Carol'}),(b:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let r = db
        .execute_with_params(
            "MATCH (a:Person)-[r]-(b:Person {tag: $t}) RETURN a.name, b.name \
             ORDER BY a.name, b.name",
            params(vec![("t", Value::Null)]),
        )
        .expect("should not error");

    assert_eq!(
        r.rows,
        vec![vec![Value::String("Bob".into()), Value::String("Alice".into())]],
        "undirected backward pass: Alice (absent tag) must surface as 'b' \
         via Bob's backward-scanned predecessor edge; Carol (tag present) \
         must never appear as 'b'; got {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Site hop.rs:349 — dst_props batch-miss fallback: NOT independently
// testable; documented here rather than covered by a misleading test.
// ═══════════════════════════════════════════════════════════════════════
//
// `unique_dst_slots` (hop.rs:297) is built by deduplicating `all_neighbors`
// — the exact same collection the outer `for &dst_slot in &all_neighbors`
// loop (hop.rs:324) iterates. `dst_slot_to_idx` (hop.rs:317) indexes every
// entry of `unique_dst_slots`, and `batch_read_node_props` always returns
// exactly one row per requested slot (verified in
// crates/sparrowdb-storage/src/node_store.rs:1595-1645). So
// `dst_slot_to_idx.get(&dst_slot)` at hop.rs:338 can never miss for any
// `dst_slot` reached by that loop — the fallback branch at hop.rs:349 is
// unreachable through the public query API as currently structured. No
// fixture can force a pre-fix/post-fix behavioral difference there because
// the branch never executes either way. The site was still converted to
// `read_node_props` for consistency with the other 3 sites and in case a
// future change (e.g. a partial/streamed batch read) makes it reachable.
