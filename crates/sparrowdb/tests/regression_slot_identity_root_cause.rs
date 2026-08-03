//! Regression tests for the shared root cause behind #415, #427, #429 and #431:
//! **slot identity treated as node identity**.
//!
//! A `NodeId` is `(label_id << 32) | slot`, so slot 2 exists independently in
//! `Person`, `Place`, `Org` and every other label.  The neighbour lookups in
//! `engine/mod.rs` used to take a bare slot with no label
//! (`csr_neighbors_all` / `csr_neighbors_filtered`) and return bare slots, so
//! every caller inherited two defects at once:
//!
//! 1. **Reading another label's edges.** A `CsrForward` is indexed by the source
//!    slot of *one* relationship table.  Probing every table with a bare slot
//!    lets a `Person` inherit the edges of the `Org` in the same slot number
//!    (#429).
//! 2. **Losing the neighbour's label.** Callers then had to reconstruct it,
//!    which `get_node_neighbors_labeled` did from delta-log hints, falling back
//!    to the *source's* label.  `checkpoint()` truncates the delta log, so after
//!    a checkpoint every hint vanishes and every CSR neighbour silently takes
//!    the source's label (#431).
//!
//! The fix threads `(slot, label_id)` through the neighbour APIs and reads both
//! labels off the catalog, which registers every relationship table as
//! `(id, src_label_id, dst_label_id, rel_type)`.  For a CSR hit the destination
//! label is therefore *known*, and a table whose `src_label_id` does not match
//! is skipped before its CSR is ever touched.
//!
//! **Every expected value below is derived by hand from [`build_graph`].**  None
//! of it was captured from program output; each test spells out the derivation.
//!
//! The fixture has four node labels, three relationship *types* across four
//! relationship *tables*, and every assertion is made both before and after
//! `checkpoint()` — #431 only exists after a checkpoint, and #429's CSR
//! contamination only exists after a checkpoint too, because the delta log
//! carries full `NodeId`s and was already filtered by source label.

use sparrowdb::open;
use sparrowdb_execution::types::{QueryResult, Value};

// ── Fixture ─────────────────────────────────────────────────────────────────
//
// Four node labels (Person, Place, Org, Post) and three relationship types
// (KNOWS, LOCATED_IN, LIKES).  LOCATED_IN deliberately spans two label pairs,
// so the catalog registers **four** relationship tables:
//
//   T1  (Person) -[:KNOWS]->      (Person)
//   T2  (Person) -[:LOCATED_IN]-> (Place)
//   T3  (Org)    -[:LOCATED_IN]-> (Place)
//   T4  (Person) -[:LIKES]->      (Post)
//
// Slots are assigned per label in creation order, so the collisions below are
// deliberate and unavoidable — they are the whole point of the fixture:
//
//   Person slots: 0 Alice        1 Bob        2 Carol
//   Place  slots: 0 Springfield  1 Berlin     2 Kyoto
//   Org    slots: 0 OrgZero      1 OrgOne     2 TechStart
//   Post   slots: 0 PostZero
//
//   * Person 2 (Carol) and Org 2 (TechStart) both exist and are connected
//     differently by the *same* relationship type — Carol to Kyoto, TechStart
//     to Berlin.  That is the #429 collision.
//   * Slot 0 is occupied in all four labels at once.
//
// Edges:
//
//   e1  Alice     -[:KNOWS]->      Bob          Person 0 -> Person 1
//   e2  Bob       -[:KNOWS]->      Carol        Person 1 -> Person 2
//   e3  Alice     -[:LOCATED_IN]-> Springfield  Person 0 -> Place  0
//   e4  Bob       -[:LOCATED_IN]-> Springfield  Person 1 -> Place  0
//   e5  Carol     -[:LOCATED_IN]-> Kyoto        Person 2 -> Place  2
//   e6  TechStart -[:LOCATED_IN]-> Berlin       Org    2 -> Place  1   <-- slot 2
//   e7  Alice     -[:LIKES]->      PostZero     Person 0 -> Post   0
//
// Nothing in the graph has an outgoing edge from any Place or Post, and OrgZero
// and OrgOne have no edges at all.

fn build_graph() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");

    // Nodes — creation order fixes the per-label slot numbers listed above.
    for name in ["Alice", "Bob", "Carol"] {
        db.execute(&format!("CREATE (p:Person {{name: '{name}'}})"))
            .expect("CREATE Person");
    }
    for name in ["Springfield", "Berlin", "Kyoto"] {
        db.execute(&format!("CREATE (c:Place {{name: '{name}'}})"))
            .expect("CREATE Place");
    }
    for name in ["OrgZero", "OrgOne", "TechStart"] {
        db.execute(&format!("CREATE (o:Org {{name: '{name}'}})"))
            .expect("CREATE Org");
    }
    db.execute("CREATE (t:Post {name: 'PostZero'})")
        .expect("CREATE Post");

    let edges: [(&str, &str, &str, &str, &str); 7] = [
        ("Person", "Alice", "KNOWS", "Person", "Bob"),
        ("Person", "Bob", "KNOWS", "Person", "Carol"),
        ("Person", "Alice", "LOCATED_IN", "Place", "Springfield"),
        ("Person", "Bob", "LOCATED_IN", "Place", "Springfield"),
        ("Person", "Carol", "LOCATED_IN", "Place", "Kyoto"),
        ("Org", "TechStart", "LOCATED_IN", "Place", "Berlin"),
        ("Person", "Alice", "LIKES", "Post", "PostZero"),
    ];
    for (src_label, src_name, rel, dst_label, dst_name) in edges {
        db.execute(&format!(
            "MATCH (s:{src_label} {{name: '{src_name}'}}), \
                   (d:{dst_label} {{name: '{dst_name}'}}) \
             CREATE (s)-[:{rel}]->(d)"
        ))
        .expect("CREATE edge");
    }

    (dir, db)
}

/// Collect column `col` of a result as a sorted `Vec<String>`.
///
/// Sorted because none of these queries specifies an ORDER BY, so row order is
/// not part of the contract; the *set* of rows is what the derivations pin down.
fn sorted_col(result: &QueryResult, col: usize) -> Vec<String> {
    let mut v: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[col] {
            Value::String(s) => s.clone(),
            other => panic!("expected a string in column {col}, got {other:?}"),
        })
        .collect();
    v.sort();
    v
}

fn query(db: &sparrowdb::GraphDb, q: &str) -> QueryResult {
    db.execute(q)
        .unwrap_or_else(|e| panic!("query failed: {q}\n{e:?}"))
}

// ── #429: a 3-hop chain must not inherit another label's edges ──────────────

/// The exact #429 shape: three inline fixed hops, the last one over a
/// relationship type that exists for two different source labels.
///
/// Derivation:
///   * hop 1 — Alice (Person 0) `-[:KNOWS]->` Bob (Person 1), via e1.  Alice's
///     only other edges are e3 (LOCATED_IN) and e7 (LIKES), neither of which is
///     KNOWS, so Bob is the only hop-1 node.
///   * hop 2 — Bob (Person 1) `-[:KNOWS]->` Carol (Person 2), via e2.
///   * hop 3 — Carol (Person 2) `-[:LOCATED_IN]->` Kyoto (Place 2), via e5.
///     That is Carol's only edge of any type.
///
/// So exactly **one** row: `(Carol, Kyoto)`.
///
/// Berlin must not appear.  Berlin is reachable in this graph only from
/// TechStart, which is **Org** slot 2 — the same slot number as Carol, who is
/// **Person** slot 2.  Before the fix hop 3 resolved LOCATED_IN by type name
/// alone, giving both T2 and T3, then probed both CSRs with the bare slot `2`,
/// so Carol collected TechStart's edge and the query returned two rows.
///
/// The contamination is CSR-only, hence the checkpoint: the delta log stores
/// full `NodeId`s and was already filtered by source label, so the same query
/// was correct before `checkpoint()` and wrong after it.
#[test]
fn three_hop_chain_does_not_inherit_another_labels_edges() {
    let (_dir, db) = build_graph();
    let q = "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person)\
             -[:KNOWS]->(c:Person)-[:LOCATED_IN]->(pl:Place) \
             RETURN c.name, pl.name";

    // Uncheckpointed (delta-backed).
    let before = query(&db, q);
    assert_eq!(
        sorted_col(&before, 1),
        vec!["Kyoto".to_string()],
        "Carol lives in Kyoto (e5); Berlin belongs to TechStart (Org slot 2)"
    );
    assert_eq!(sorted_col(&before, 0), vec!["Carol".to_string()]);

    // Checkpointed (CSR-backed) — the answer must not change.
    db.checkpoint().expect("checkpoint");
    let after = query(&db, q);
    assert_eq!(
        sorted_col(&after, 1),
        vec!["Kyoto".to_string()],
        "after checkpoint the edges live in per-table CSRs indexed by source \
         slot; Person slot 2 must not read Org slot 2's row"
    );
    assert_eq!(sorted_col(&after, 0), vec!["Carol".to_string()]);
}

/// The half of #429 that narrowing at the call site could not reach: an
/// **unlabeled** intermediate node.  With `(b)` and `(c)` carrying no label
/// there is nothing to narrow the relationship-table set against, so a fix that
/// resolves `(src_label, dst_label, rel_type)` from the *pattern* has no
/// information to work with.  Deriving the label from the catalog does.
///
/// Same derivation as above — the pattern matches the same single path
/// Alice -> Bob -> Carol -> Kyoto — so the answer is the same one row, `Kyoto`.
#[test]
fn unlabeled_intermediates_do_not_inherit_another_labels_edges() {
    let (_dir, db) = build_graph();
    db.checkpoint().expect("checkpoint");

    let result = query(
        &db,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)\
         -[:LOCATED_IN]->(pl:Place) RETURN pl.name",
    );
    assert_eq!(
        sorted_col(&result, 0),
        vec!["Kyoto".to_string()],
        "the traversal reaches Carol (Person 2), whose only LOCATED_IN edge is \
         to Kyoto; TechStart (Org 2) is a different node"
    );
}

// ── #431: cross-label variable-length traversal after a checkpoint ──────────

/// A variable-length hop that crosses labels, asserted on both sides of
/// `checkpoint()`.
///
/// Derivation: Alice is Person 0.  Her only LOCATED_IN edge is e3, to
/// Springfield (Place 0).  Springfield has no outgoing edges, so depth 2 adds
/// nothing.  `[:LOCATED_IN*1..2]` therefore yields exactly `["Springfield"]`.
///
/// Before the fix this returned one row before the checkpoint and **zero** after
/// it.  `get_node_neighbors_labeled` recovered a CSR neighbour's label from
/// hints built out of `read_delta_all()`; `checkpoint()` empties the delta log,
/// so the hint set was empty and every neighbour fell back to the *source's*
/// label — Person.  `execute_variable_length` then dropped it, because the
/// destination pattern requires Place.  Silent, and empty in the safe-looking
/// direction.
#[test]
fn cross_label_varlen_survives_checkpoint() {
    let (_dir, db) = build_graph();
    let q = "MATCH (p:Person {name: 'Alice'})-[:LOCATED_IN*1..2]->(pl:Place) \
             RETURN pl.name";

    let before = query(&db, q);
    assert_eq!(
        sorted_col(&before, 0),
        vec!["Springfield".to_string()],
        "Alice's only LOCATED_IN edge is e3, to Springfield; Springfield has no \
         outgoing edges so depth 2 adds nothing"
    );

    db.checkpoint().expect("checkpoint");
    let after = query(&db, q);
    assert_eq!(
        sorted_col(&after, 0),
        vec!["Springfield".to_string()],
        "checkpointing moves the edge into a CSR; the destination's label comes \
         from the catalog, so the answer must be identical"
    );
}

/// Both faults at once, in one variable-length query.
///
/// Derivation: Carol is Person 2.  Her only LOCATED_IN edge is e5, to Kyoto
/// (Place 2).  So the answer is exactly `["Kyoto"]`.
///
/// Berlin is the trap: it is reached only by e6, from TechStart, which is **Org**
/// slot 2 — Carol's slot number in a different label.  Before the fix, after a
/// checkpoint, the traversal probed both LOCATED_IN CSRs with the bare slot `2`
/// (picking up Berlin) and then relabelled every neighbour as Person (dropping
/// both), so the query returned nothing at all.
#[test]
fn cross_label_varlen_does_not_read_a_colliding_slot() {
    let (_dir, db) = build_graph();
    db.checkpoint().expect("checkpoint");

    let result = query(
        &db,
        "MATCH (p:Person {name: 'Carol'})-[:LOCATED_IN*1..1]->(pl:Place) \
         RETURN pl.name",
    );
    assert_eq!(
        sorted_col(&result, 0),
        vec!["Kyoto".to_string()],
        "Carol (Person 2) lives in Kyoto; Berlin belongs to TechStart (Org 2)"
    );
}

/// Control: the homogeneous case that hid all of this for so long.
///
/// Derivation: the KNOWS chain is Alice -> Bob -> Carol (e1, e2).  From Alice,
/// `[:KNOWS*1..2]` reaches Bob at depth 1 and Carol at depth 2, and nothing
/// else — Carol has no outgoing KNOWS edge.  So `["Bob", "Carol"]`.
///
/// A Person->Person traversal is unaffected by either fault, because the source
/// label *is* the correct destination label.  This asserts the fix does not
/// over-filter and lose genuine edges.
#[test]
fn homogeneous_varlen_is_unchanged() {
    let (_dir, db) = build_graph();
    db.checkpoint().expect("checkpoint");

    let result = query(
        &db,
        "MATCH (p:Person {name: 'Alice'})-[:KNOWS*1..2]->(f:Person) RETURN f.name",
    );
    assert_eq!(
        sorted_col(&result, 0),
        vec!["Bob".to_string(), "Carol".to_string()],
        "Alice -> Bob is 1 hop and Alice -> Bob -> Carol is 2"
    );
}

// ── #421 × #429/#431: varlen hop + trailing hop must not inherit a colliding
// slot's edges ───────────────────────────────────────────────────────────

/// Collect two string columns as a sorted `Vec<(String, String)>`.
fn sorted_pairs(result: &QueryResult, col_a: usize, col_b: usize) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = result
        .rows
        .iter()
        .map(|row| {
            let a = match &row[col_a] {
                Value::String(s) => s.clone(),
                other => panic!("expected a string in column {col_a}, got {other:?}"),
            };
            let b = match &row[col_b] {
                Value::String(s) => s.clone(),
                other => panic!("expected a string in column {col_b}, got {other:?}"),
            };
            (a, b)
        })
        .collect();
    v.sort();
    v
}

/// #421 taught `execute_n_hop` to answer a variable-length hop followed by a
/// further hop instead of rejecting it. The root fix in this file made every
/// fixed-hop neighbour lookup label-aware. Neither guards the intersection on
/// its own: #421's tests predate this fixture, and before #421 this exact
/// shape was rejected outright rather than executed (see
/// `regression_421_varlen_plus_hop::pipeline_match_stage_rejects_varlen`), so
/// nothing exercises the trailing hop of a varlen chain landing on a
/// colliding slot except this.
///
/// `b` is deliberately **unlabeled**. `rel_ids_per_hop` narrows a fixed hop to
/// a single catalog table when *both* its endpoint labels are declared — see
/// the comment above that code, which names this exact fixture (Carol /
/// TechStart) as the case it exists to head off. With `(b:Person)` the
/// narrowing hides the bug before the neighbour lookup ever runs; with `(b)`
/// unlabeled there is nothing to narrow against, so both LOCATED_IN tables
/// (T2 Person->Place, T3 Org->Place) stay live and the neighbour lookup
/// itself has to be the thing that filters by source label.
///
/// Derivation: Alice's KNOWS chain is Alice(P0) -> Bob(P1) -> Carol(P2) (e1,
/// e2), so `[:KNOWS*1..2]` binds `b` to {Bob, Carol} — Bob at depth 1, Carol
/// at depth 2. Bob's only LOCATED_IN edge is e4, to Springfield. Carol's only
/// LOCATED_IN edge is e5, to Kyoto. So exactly two rows:
/// `(Bob, Springfield)`, `(Carol, Kyoto)`.
///
/// The trap is Carol: she is Person slot 2, the same slot number as TechStart
/// (Org slot 2), and both have an outgoing LOCATED_IN edge registered under
/// different catalog tables (T2 via e5, T3 via e6, TechStart -> Berlin). A
/// trailing-hop neighbour lookup that filtered by bare slot alone, without
/// checking which table's `src_label_id` actually matches Carol, would pick
/// up e6 too and add a spurious third row, `(Carol, Berlin)` — after
/// `checkpoint()` moves the edges into per-table CSRs, exactly where
/// #429/#431 lived.
#[test]
fn varlen_plus_trailing_hop_does_not_read_a_colliding_slot() {
    let (_dir, db) = build_graph();
    let q = "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b)\
             -[:LOCATED_IN]->(pl:Place) RETURN DISTINCT b.name, pl.name";

    // Uncheckpointed (delta-backed) — the delta log stores full NodeIds and
    // was never subject to the bare-slot bug, so this must already be right.
    let before = query(&db, q);
    assert_eq!(
        sorted_pairs(&before, 0, 1),
        vec![
            ("Bob".to_string(), "Springfield".to_string()),
            ("Carol".to_string(), "Kyoto".to_string()),
        ],
        "Bob lives in Springfield (e4); Carol lives in Kyoto (e5); Berlin \
         belongs to TechStart (Org slot 2), not to Carol (Person slot 2)"
    );

    // Checkpointed (CSR-backed) — the answer must not change.
    db.checkpoint().expect("checkpoint");
    let after = query(&db, q);
    assert_eq!(
        sorted_pairs(&after, 0, 1),
        vec![
            ("Bob".to_string(), "Springfield".to_string()),
            ("Carol".to_string(), "Kyoto".to_string()),
        ],
        "after checkpoint the edges live in per-table CSRs indexed by source \
         slot; Carol (Person slot 2) must not read TechStart's (Org slot 2) \
         row just because the numeric slot matches"
    );
}

/// CodeRabbit (#432 review): the fixed-hop branch used to encode an unlabeled
/// destination's neighbour label as `next_label_id_opt.unwrap_or(0)`. Label id
/// `0` is a real label — the catalog hands out ids starting at 0, and in this
/// fixture `Person` (the *first* label `build_graph` creates) **is** label 0
/// — so `unwrap_or(0)` is not "no label", it is "silently relabel this
/// neighbour as Person". `read_node_props` then reads whatever Person happens
/// to occupy the destination's numeric slot instead of the real destination.
///
/// Every existing test above resolves its unlabeled positions to Person
/// nodes, so the wrong default and the right answer coincide by accident and
/// the bug never surfaces. This test forces the unlabeled destination to
/// resolve to `Place` (label 1, not 0) so `unwrap_or(0)` and reality diverge.
///
/// Derivation, same KNOWS chain as above: `b` ∈ {Bob (P1), Carol (P2)}. Both
/// `b` and `pl` are unlabeled, so LOCATED_IN stays type-wide (T2 and T3).
/// Bob's only LOCATED_IN edge is e4, to Springfield (**Place slot 0**).
/// Carol's only LOCATED_IN edge is e5, to Kyoto (**Place slot 2**). So
/// exactly two rows: `(Bob, Springfield)`, `(Carol, Kyoto)`.
///
/// Under the old code every candidate slot — from either table, delta or CSR
/// — got relabelled Person (`unwrap_or(0)`), so `pl` would resolve to
/// whichever Person occupies that slot number instead of the real Place:
/// Bob's candidate slot is 0 (Springfield's slot), which as a *Person* is
/// Alice; Carol's candidate slots are 2 from T2 (Kyoto's slot, which as a
/// Person is Carol herself) and 1 from T3 (Berlin's slot, which as a Person
/// is Bob). `pl.name` would read "Alice", "Carol" or "Bob" — never a Place at
/// all. This is not checkpoint-specific: the delta path threw the label away
/// exactly the same way (`r.dst.0 & 0xFFFF_FFFF` keeps only the slot), so the
/// bug is present before `checkpoint()` too.
#[test]
fn varlen_plus_trailing_hop_unlabeled_destination_does_not_default_to_label_zero() {
    let (_dir, db) = build_graph();
    let q = "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b)\
             -[:LOCATED_IN]->(pl) RETURN DISTINCT b.name, pl.name";

    // Uncheckpointed (delta-backed) — the old bug lived here too: the delta
    // branch discarded `r.dst`'s label just as thoroughly as the CSR branch.
    let before = query(&db, q);
    assert_eq!(
        sorted_pairs(&before, 0, 1),
        vec![
            ("Bob".to_string(), "Springfield".to_string()),
            ("Carol".to_string(), "Kyoto".to_string()),
        ],
        "pl must resolve to the real Place destination, not to whichever \
         Person happens to occupy the same numeric slot"
    );

    // Checkpointed (CSR-backed) — the answer must not change.
    db.checkpoint().expect("checkpoint");
    let after = query(&db, q);
    assert_eq!(
        sorted_pairs(&after, 0, 1),
        vec![
            ("Bob".to_string(), "Springfield".to_string()),
            ("Carol".to_string(), "Kyoto".to_string()),
        ],
        "an unlabeled destination must never default to label 0; it must \
         carry the neighbour's real discovered label"
    );
}

// ── #427: shortestPath, re-asserted against the shared neighbour lookup ─────

/// `bfs_shortest_path` grew its own catalog-driven neighbour loop in the #427
/// fix; the root fix replaces it with the shared `get_node_neighbors_labeled`.
/// These assertions guard that swap — they must keep holding.
///
/// Derivations, all after `checkpoint()`:
///   * `Alice -[:KNOWS*]-> Carol` = **2** (e1 then e2).
///   * `Carol -[:KNOWS*]-> Alice` = **NULL**.  The KNOWS chain is
///     one-directional and Carol's only edge is e5, a LOCATED_IN to Kyoto.
///     Kyoto is Place slot 2 and Alice is Person slot 0, so no slot coincidence
///     can rescue it either.
///   * `Alice -[:LOCATED_IN*]-> Springfield` = **1** (e3) — a cross-label
///     destination, which a slot-only comparison cannot express.
///   * `Carol -[:LOCATED_IN*]-> Berlin` = **NULL**.  Berlin is reachable only
///     from TechStart (Org 2); Carol is Person 2.  Reading the colliding slot
///     would report 1.
#[test]
fn shortest_path_still_correct_through_the_shared_lookup() {
    let (_dir, db) = build_graph();
    db.checkpoint().expect("checkpoint");

    let sp = |src_label: &str, src: &str, rel: &str, dst_label: &str, dst: &str| -> Value {
        let q = format!(
            "MATCH (s:{src_label} {{name: '{src}'}}), (d:{dst_label} {{name: '{dst}'}}) \
             RETURN shortestPath((s)-[:{rel}*]->(d))"
        );
        let r = query(&db, &q);
        assert_eq!(r.rows.len(), 1, "expected one row for: {q}");
        r.rows[0][0].clone()
    };

    assert_eq!(
        sp("Person", "Alice", "KNOWS", "Person", "Carol"),
        Value::Int64(2),
        "Alice -> Bob -> Carol is 2 KNOWS hops"
    );
    assert_eq!(
        sp("Person", "Carol", "KNOWS", "Person", "Alice"),
        Value::Null,
        "the KNOWS chain is one-directional"
    );
    assert_eq!(
        sp("Person", "Alice", "LOCATED_IN", "Place", "Springfield"),
        Value::Int64(1),
        "Alice -[:LOCATED_IN]-> Springfield is a direct cross-label edge"
    );
    assert_eq!(
        sp("Person", "Carol", "LOCATED_IN", "Place", "Berlin"),
        Value::Null,
        "Berlin belongs to TechStart (Org 2), not to Carol (Person 2)"
    );
}

// ── One-hop control ─────────────────────────────────────────────────────────

/// The single-hop path resolves `(src_label, dst_label, rel_type)` to one table
/// already, so it was never exposed to #429.  Asserted anyway so that a
/// regression in the shared helper cannot pass unnoticed.
///
/// Derivation: Carol's only LOCATED_IN edge is e5, to Kyoto.  One row.
#[test]
fn one_hop_control_is_unchanged() {
    let (_dir, db) = build_graph();
    db.checkpoint().expect("checkpoint");

    let result = query(
        &db,
        "MATCH (p:Person {name: 'Carol'})-[:LOCATED_IN]->(pl:Place) RETURN pl.name",
    );
    assert_eq!(
        sorted_col(&result, 0),
        vec!["Kyoto".to_string()],
        "Carol -[:LOCATED_IN]-> Kyoto (e5) is her only edge"
    );
}
