//! Regression guards for issue #521 — the "absent conflated with zero"
//! defect #479/#522 fixed in mutation.rs/expr.rs, found on two families of
//! call sites in hop.rs:
//!
//! 1. **4 `get_node_raw` sites** in `execute_one_hop` (the individual-read
//!    accessor): `read_col_slot` zero-sentinels an absent column to `Ok(0)`
//!    (decoding to `Value::Int64(0)`, never `None`), so a genuinely
//!    null-bound `$param` prop filter could never match the node whose
//!    property is actually absent — silent under-matching (same direction
//!    as #472/#479, on the traversal path rather than pipeline/mutation).
//! 2. **2 `batch_read_node_props` sites** (the batch-read accessor used by
//!    `execute_one_hop`'s dst-side and `execute_two_hop`'s friend-of-friend
//!    side) — found while building this PR, not in the original issue text
//!    (which only grepped for `get_node_raw(` and missed the differently
//!    named batch function entirely). These are the *primary* read path for
//!    those two prop filters, not a rare fallback, so they matter more in
//!    practice than family 1. One of the two sites additionally carried a
//!    manual `.filter(|&(_, v)| v != 0)` workaround that fixed the null case
//!    by accident while breaking the opposite case: a genuinely stored
//!    `Int64(0)` was misread as absent. Both sites now use
//!    `batch_read_node_props_nullable`, which distinguishes absence from
//!    stored-zero via the null-bitmap sidecar.
//!
//! All expected values below are derived by hand from each fixture, never
//! captured from a prior run (repo rule — see CLAUDE.md). Each test's
//! pre-fix failure output is quoted in the PR description, captured by
//! running these tests against the relevant parent commit.
//!
//! ── Site coverage ───────────────────────────────────────────────────────
//! - hop.rs `execute_one_hop` src_props (individual read, forward pass)
//!   → `src_prop_filter_forward_hop_matches_absent_node`
//! - hop.rs `execute_one_hop` dst_props (individual-read batch-miss
//!   fallback) → NOT independently testable; see the doc comment on
//!   `dst_fallback_site_is_unreachable_note` below.
//! - hop.rs `execute_one_hop` dst_props (**batch read, primary path**)
//!   → `one_hop_dst_batch_filter_matches_absent_and_preserves_real_zero`
//! - hop.rs `execute_one_hop` b_props (individual read, backward "a"-role)
//!   → `undirected_backward_a_role_filter_matches_absent_node`
//! - hop.rs `execute_one_hop` a_props (individual read, backward "b"-role)
//!   → `undirected_backward_b_role_filter_matches_absent_node`
//! - hop.rs `execute_two_hop` fof_props (**batch read, primary path**,
//!   forward-forward `(a)-[:R]->(m)-[:R]->(f)` branch)
//!   → `two_hop_fof_batch_filter_matches_absent_and_preserves_real_zero`
//!
//! Each undirected (`execute_one_hop`, backward-pass) test is built so its
//! forward-pass contribution is fully accounted for by hand, including
//! through the (now also fixed) batch path — see each test's derivation
//! comment for the exact row-by-row trace.

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
            vec![Value::String("Alice".into()), Value::String("Carol".into())],
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
// unfiltered, so the backward pass's "a"-role filter (b_props) is only
// exercised vacuously here — this isolates the backward pass's "b"-role
// filter (a_props).
//
// Same fixture as above (Alice absent tag; Bob, Carol present).
//
// Hand-derived, accounting for the dst-side filter also being evaluated via
// the now-fixed `execute_one_hop` dst batch path (this test predates that
// fix being in scope — its expected value changed once the batch path was
// also corrected; see the PR description for the "before expansion" value):
// - Forward pass: `a` unfiltered so every node is tried as src.
//   src=Alice → neighbor Bob → dst filter (b.tag=null) on Bob via the fixed
//   batch path: Bob's tag is genuinely present ('x') → excluded correctly.
//   src=Bob → no outgoing edges → nothing.
//   src=Carol → neighbor Alice → dst filter on Alice via the fixed batch
//   path: Alice's tag is genuinely absent → now correctly matches → row
//   (a=Carol, b=Alice).
// - Backward pass: scanning Alice/Bob/Carol as candidate "b" via a_props —
//   at b_slot=Bob, predecessor Alice is tested via a_props against the
//   `b`-filter. Alice's absent tag passes under the fix → row (a=Bob,
//   b=Alice). At b_slot=Alice, predecessor Carol is tested the same way;
//   Carol's present tag correctly fails.
// - Total, ORDER BY a.name: [(Bob,Alice), (Carol,Alice)].
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
        vec![
            vec![Value::String("Bob".into()), Value::String("Alice".into())],
            vec![Value::String("Carol".into()), Value::String("Alice".into())],
        ],
        "undirected backward pass + fixed forward dst-batch path: Alice \
         (absent tag) must surface as 'b' via both the backward edge from \
         Bob and the forward edge from Carol; Bob and Carol must never \
         appear as 'b' themselves since neither's tag is absent; got {:?}",
        r.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════
// execute_one_hop dst_props — BATCH read, primary path (not the fallback)
// ═══════════════════════════════════════════════════════════════════════

// Directed one-hop with a dst-side filter and 4 neighbours off a single src
// node, so `unique_dst_slots.len() == 4` and every one of them is found in
// `dst_slot_to_idx` — i.e. this exercises the batch read
// (`batch_read_node_props_nullable` as of this fix), not the individual-read
// fallback a few lines below it (which the earlier test in this file already
// established is unreachable through the public API regardless). Direction
// is Outgoing, so no backward pass runs — isolates the forward dst-batch
// path cleanly.
//
// Fixture: single `a1` with 4 outgoing edges to `f_absent` (no tag),
// `f_absent2` (no tag), `f_present` (tag='x'), `f_zero` (tag=0, a genuinely
// stored zero — encoded via `StoreValue::Int64(0)`, same tag byte as any
// other Int64, not a raw all-zero sentinel).
//
// Two queries against the same fixture, both hand-derived:
// - Null filter (`tag: $t` with `$t = null`): must match only the two
//   genuinely-absent nodes. `f_zero` must NOT match — it has a real stored
//   value, just one that happens to be zero.
// - Zero filter (`tag: $t` with `$t = Value::Int64(0)`): must match only
//   `f_zero`. The two absent nodes must NOT match a literal-0 filter — this
//   is the direction the pre-fix batch code got backwards: with no
//   null-bitmap check, an absent column and a genuinely-stored 0 have the
//   identical raw encoding (0u64), so pre-fix, `f_absent`/`f_absent2` were
//   indistinguishable from `f_zero` to this filter.
#[test]
fn one_hop_dst_batch_filter_matches_absent_and_preserves_real_zero() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:A {id: 'a1'})").unwrap();
    db.execute("CREATE (:F {id: 'f_absent'})").unwrap(); // no `tag`
    db.execute("CREATE (:F {id: 'f_absent2'})").unwrap(); // no `tag`
    db.execute("CREATE (:F {id: 'f_present', tag: 'x'})")
        .unwrap();
    db.execute("CREATE (:F {id: 'f_zero', tag: 0})").unwrap(); // genuinely stored 0
    for fof in ["f_absent", "f_absent2", "f_present", "f_zero"] {
        db.execute(&format!(
            "MATCH (a:A {{id:'a1'}}),(f:F {{id:'{fof}'}}) CREATE (a)-[:R]->(f)"
        ))
        .unwrap();
    }

    let null_result = db
        .execute_with_params(
            "MATCH (a:A)-[:R]->(f:F {tag: $t}) RETURN f.id ORDER BY f.id",
            params(vec![("t", Value::Null)]),
        )
        .expect("should not error");
    assert_eq!(
        null_result.rows,
        vec![
            vec![Value::String("f_absent".into())],
            vec![Value::String("f_absent2".into())],
        ],
        "one-hop dst batch path: a null-bound filter must match only the \
         genuinely absent-tag nodes, never f_zero (real stored 0) or \
         f_present; got {:?}",
        null_result.rows
    );

    let zero_result = db
        .execute_with_params(
            "MATCH (a:A)-[:R]->(f:F {tag: $t}) RETURN f.id",
            params(vec![("t", Value::Int64(0))]),
        )
        .expect("should not error");
    assert_eq!(
        zero_result.rows,
        vec![vec![Value::String("f_zero".into())]],
        "one-hop dst batch path: a literal-0 filter must find the \
         genuinely-stored-0 node and only that node — absent columns must \
         not be misread as a stored 0; got {:?}",
        zero_result.rows
    );
}

// ═══════════════════════════════════════════════════════════════════════
// execute_two_hop fof_props — BATCH read, primary path (forward-forward
// branch, `(a)-[:R]->(m)-[:R]->(f)`)
// ═══════════════════════════════════════════════════════════════════════

// Mirrors the previous test one hop further out: `a1 -> m1 -> {4 fof
// candidates}`, forcing `all_fof_slots.len() == 4` through the single batch
// read in the forward-forward branch of `execute_two_hop` (hop.rs, the
// "#487: this branch runs whenever the second hop is Outgoing" comment).
// This is the site that carried the pre-existing `.filter(|&(_, v)| v != 0)`
// workaround — the fix here is not just "match null correctly" but "stop
// silently misreading a genuinely-stored 0 as absent".
#[test]
fn two_hop_fof_batch_filter_matches_absent_and_preserves_real_zero() {
    let (db, _dir) = open_db();
    db.execute("CREATE (:A {id: 'a1'})").unwrap();
    db.execute("CREATE (:M {id: 'm1'})").unwrap();
    db.execute("CREATE (:F {id: 'f_absent'})").unwrap(); // no `tag`
    db.execute("CREATE (:F {id: 'f_absent2'})").unwrap(); // no `tag`
    db.execute("CREATE (:F {id: 'f_present', tag: 'x'})")
        .unwrap();
    db.execute("CREATE (:F {id: 'f_zero', tag: 0})").unwrap(); // genuinely stored 0
    db.execute("MATCH (a:A {id:'a1'}),(m:M {id:'m1'}) CREATE (a)-[:R]->(m)")
        .unwrap();
    for fof in ["f_absent", "f_absent2", "f_present", "f_zero"] {
        db.execute(&format!(
            "MATCH (m:M {{id:'m1'}}),(f:F {{id:'{fof}'}}) CREATE (m)-[:R]->(f)"
        ))
        .unwrap();
    }

    let null_result = db
        .execute_with_params(
            "MATCH (a:A)-[:R]->(m:M)-[:R]->(f:F {tag: $t}) RETURN f.id ORDER BY f.id",
            params(vec![("t", Value::Null)]),
        )
        .expect("should not error");
    assert_eq!(
        null_result.rows,
        vec![
            vec![Value::String("f_absent".into())],
            vec![Value::String("f_absent2".into())],
        ],
        "two-hop fof batch path: a null-bound filter must match only the \
         genuinely absent-tag nodes, never f_zero (real stored 0) or \
         f_present; got {:?}",
        null_result.rows
    );

    let zero_result = db
        .execute_with_params(
            "MATCH (a:A)-[:R]->(m:M)-[:R]->(f:F {tag: $t}) RETURN f.id",
            params(vec![("t", Value::Int64(0))]),
        )
        .expect("should not error");
    assert_eq!(
        zero_result.rows,
        vec![vec![Value::String("f_zero".into())]],
        "two-hop fof batch path: a literal-0 filter must find the \
         genuinely-stored-0 node and only that node — the old \
         `.filter(v != 0)` workaround dropped it as if absent; got {:?}",
        zero_result.rows
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
