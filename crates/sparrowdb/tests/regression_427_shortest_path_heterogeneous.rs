//! Regression tests for issue #427 — `shortestPath()` on heterogeneous graphs.
//!
//! Two independent faults lived in `Engine::bfs_shortest_path`:
//!
//! 1. **The relationship-type filter was discarded.** The BFS called
//!    `get_node_neighbors_by_slot(..., &[])`; the empty `rel_ids` slice means
//!    "no type filter", so a BFS for `[:KNOWS*]` walked every edge type in the
//!    graph.
//! 2. **Nodes were identified by storage slot alone, without their label.**
//!    A `NodeId` is `(label_id << 32) | slot`, so slot 0 exists once per label.
//!    The destination test (`nb_slot == dst_slot`), the zero-hop test
//!    (`src_slot == dst_slot`) and the `visited` set all compared bare slots,
//!    so a `City` and a `Person` that happen to occupy the same slot aliased
//!    each other. That produced both false positives (reporting a path that
//!    does not exist) and silent pruning (dropping the only real path).
//!
//! Every expected value below is derived **by hand** from the fixture defined
//! in [`build_graph`] — none of it was captured from program output. The
//! derivation is spelled out in each test's comment.
//!
//! No-path contract: `shortestPath()` evaluates to `Value::Null` when no path
//! exists (openCypher semantics, and what `spa_136_shortest_path.rs` and
//! `spa_139_phase9_path_acceptance.rs` already assert).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

// ── Fixture ─────────────────────────────────────────────────────────────────
//
// Three node labels, four relationship types, and a deliberately isolated node.
//
// Slots are assigned per label in creation order, so the fixture is written so
// that slot collisions ACROSS labels are unavoidable — that is the whole point.
//
//   Person slots: 0 Alice  1 Bob  2 Carol  3 Dave  4 Erin  5 Frank  6 Grace
//   City   slots: 0 Metro   1 Junction
//   Post   slots: 0 Hello
//
// Edges (creation order matters for the pruning test — see `Post`/`Person`
// collision on slot 0 below):
//
//   e1  Alice -[:LIVES_IN]-> Junction     Person 0 -> City 1
//   e2  Alice -[:KNOWS]->    Bob          Person 0 -> Person 1
//   e3  Bob   -[:KNOWS]->    Carol        Person 1 -> Person 2
//   e4  Carol -[:KNOWS]->    Dave         Person 2 -> Person 3
//   e5  Dave  -[:LIVES_IN]-> Metro        Person 3 -> City 0
//   e6  Dave  -[:LIKES]->    Hello        Person 3 -> Post 0
//   e7  Erin  -[:LIVES_IN]-> Metro        Person 4 -> City 0
//   e8  Alice -[:FOLLOWS]->  Erin         Person 0 -> Person 4
//   e9  Bob   -[:MENTIONS]-> Hello        Person 1 -> Post 0     <-- created first
//   e10 Bob   -[:MENTIONS]-> Alice        Person 1 -> Person 0   <-- same slot (0)
//   e11 Alice -[:MENTIONS]-> Carol        Person 0 -> Person 2
//   e12 Alice -[:MENTIONS]-> Frank        Person 0 -> Person 5
//
// Grace (Person 6) has no edges at all.
//
// The complete KNOWS sub-graph is the one-directional chain
//   Alice -> Bob -> Carol -> Dave
// so under `[:KNOWS*]`:
//   * Erin, Frank, every City and every Post are unreachable from anywhere;
//   * Dave and Erin have no outgoing KNOWS edge at all;
//   * there is no route back from Carol or Dave to Alice.

fn build_graph() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");

    // Nodes — creation order fixes the per-label slot numbers listed above.
    for name in ["Alice", "Bob", "Carol", "Dave", "Erin", "Frank", "Grace"] {
        db.execute(&format!("CREATE (p:Person {{name: '{name}'}})"))
            .expect("CREATE Person");
    }
    for name in ["Metro", "Junction"] {
        db.execute(&format!("CREATE (c:City {{name: '{name}'}})"))
            .expect("CREATE City");
    }
    db.execute("CREATE (p:Post {name: 'Hello'})")
        .expect("CREATE Post");

    // Edges, in the order documented above.
    let edges: [(&str, &str, &str, &str, &str); 12] = [
        ("Person", "Alice", "LIVES_IN", "City", "Junction"),
        ("Person", "Alice", "KNOWS", "Person", "Bob"),
        ("Person", "Bob", "KNOWS", "Person", "Carol"),
        ("Person", "Carol", "KNOWS", "Person", "Dave"),
        ("Person", "Dave", "LIVES_IN", "City", "Metro"),
        ("Person", "Dave", "LIKES", "Post", "Hello"),
        ("Person", "Erin", "LIVES_IN", "City", "Metro"),
        ("Person", "Alice", "FOLLOWS", "Person", "Erin"),
        ("Person", "Bob", "MENTIONS", "Post", "Hello"),
        ("Person", "Bob", "MENTIONS", "Person", "Alice"),
        ("Person", "Alice", "MENTIONS", "Person", "Carol"),
        ("Person", "Alice", "MENTIONS", "Person", "Frank"),
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

/// Run `shortestPath((s)-[:REL*]->(d))` between two named nodes and return the
/// single scalar it produced.
fn sp(
    db: &sparrowdb::GraphDb,
    src_label: &str,
    src_name: &str,
    rel: &str,
    dst_label: &str,
    dst_name: &str,
) -> Value {
    let q = format!(
        "MATCH (s:{src_label} {{name: '{src_name}'}}), \
               (d:{dst_label} {{name: '{dst_name}'}}) \
         RETURN shortestPath((s)-[:{rel}*]->(d))"
    );
    let result = db.execute(&q).expect("shortestPath query must not error");
    assert_eq!(result.rows.len(), 1, "expected exactly one row for: {q}");
    assert_eq!(
        result.rows[0].len(),
        1,
        "expected exactly one column for: {q}"
    );
    result.rows[0][0].clone()
}

// ── Fault 1: the relationship-type filter must be honoured ──────────────────

/// Alice reaches Erin in one hop — but only over `FOLLOWS` (e8), never over
/// `KNOWS`. The KNOWS closure of Alice is exactly {Bob, Carol, Dave} (e2, e3,
/// e4), so Erin is not in it and `shortestPath((Alice)-[:KNOWS*]->(Erin))` has
/// no path: NULL.
///
/// Both endpoints are `Person`, so label aliasing cannot explain a wrong
/// answer here — this isolates fault 1. Before the fix the BFS ignored the
/// type and crossed the FOLLOWS edge, reporting 1.
#[test]
fn rel_type_filter_is_applied_between_same_label_nodes() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Alice", "KNOWS", "Person", "Erin"),
        Value::Null,
        "Erin is reachable from Alice only via FOLLOWS; [:KNOWS*] must find no path"
    );
}

/// The exact reproduction from issue #427.
///
/// Dave's only outgoing edges are `LIVES_IN` -> Metro (e5) and `LIKES` ->
/// Hello (e6). He has no outgoing `KNOWS` edge at all, so under `[:KNOWS*]`
/// his frontier is empty at depth 1 and nothing is reachable: NULL.
///
/// Before the fix both faults combined: the type filter was dropped, so Dave's
/// neighbours were City slot 0 and Post slot 0; the destination Alice is
/// Person slot 0; the bare-slot comparison matched and the query returned 1.
#[test]
fn source_with_no_edge_of_queried_type_has_no_path() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Dave", "KNOWS", "Person", "Alice"),
        Value::Null,
        "Dave has no outgoing KNOWS edge, so no KNOWS path to Alice exists"
    );
}

// ── Fault 2: nodes must be identified by (slot, label), not slot ────────────

/// Erin's only outgoing edge is `LIVES_IN` -> Metro (e7), and Metro has no
/// outgoing edges at all. So `shortestPath((Erin)-[:LIVES_IN*]->(Alice))` has
/// no path: NULL.
///
/// This isolates fault 2. Erin has exactly one outgoing edge and it is of the
/// queried type, so discarding the type filter changes nothing — the only way
/// to get a wrong answer is the destination test. Metro is City slot 0 and
/// Alice is Person slot 0; before the fix `nb_slot == dst_slot` matched and the
/// query returned 1.
#[test]
fn destination_match_requires_the_label_to_agree() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Erin", "LIVES_IN", "Person", "Alice"),
        Value::Null,
        "Erin's LIVES_IN neighbour is Metro (City), not Alice (Person)"
    );
}

/// Alice is Person slot 0; Metro is City slot 0. They are different nodes and
/// there is no edge between them: Alice's only `LIVES_IN` edge is e1, to
/// Junction, and Junction has no outgoing edges. So
/// `shortestPath((Alice)-[:LIVES_IN*]->(Metro))` has no path: NULL.
///
/// Before the fix the zero-hop shortcut `if src_slot == dst_slot { Some(0) }`
/// fired on the bare slot and returned 0 — claiming the source and the
/// destination were the same node.
#[test]
fn zero_hop_shortcut_requires_the_label_to_agree() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Alice", "LIVES_IN", "City", "Metro"),
        Value::Null,
        "Person slot 0 (Alice) and City slot 0 (Metro) are different nodes"
    );
}

/// The silent half of fault 2: a real path being pruned away.
///
/// Bob has two outgoing `MENTIONS` edges: e9 to Hello (Post slot 0) and e10 to
/// Alice (Person slot 0). Alice in turn mentions Frank (e12). Bob has no
/// `MENTIONS` edge to Frank, and Hello has no outgoing edges at all, so
/// `shortestPath((Bob)-[:MENTIONS*]->(Frank))` = **2**: Bob -> Alice -> Frank.
///
/// Before the fix the two hop-1 neighbours collapsed into one:
/// `get_node_neighbors_by_slot` returns a `HashSet<u64>` of bare slots, so
/// `{Post 0, Person 0}` became the single entry `0`. The neighbour's label was
/// then recovered by taking the first delta record with that destination slot
/// — e9, created first, i.e. the Post. Alice was therefore never expanded, the
/// frontier died at the dead-end Post, and the query returned NULL for a path
/// that plainly exists. Frank sits at Person slot 5, which no node of any other
/// label occupies in this fixture, so the destination test cannot rescue it.
#[test]
fn slot_aliasing_does_not_prune_the_only_real_path() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Bob", "MENTIONS", "Person", "Frank"),
        Value::Int64(2),
        "Bob -[:MENTIONS]-> Alice -[:MENTIONS]-> Frank is 2 hops"
    );
}

// ── Distances must not be understated by cross-type shortcuts ───────────────

/// The KNOWS chain is Alice -> Bob -> Carol -> Dave (e2, e3, e4), so
/// `shortestPath((Alice)-[:KNOWS*]->(Dave))` = **3**, and
/// `shortestPath((Alice)-[:KNOWS*]->(Bob))` = **1**.
///
/// Before the fix the 3-hop answer came back as 2: with the type filter gone,
/// Alice's `MENTIONS` edge to Carol (e11) was treated as a KNOWS hop, giving
/// Alice -> Carol -> Dave. That is the "understates real distances" half of
/// the issue. The 1-hop assertion is the control that the fix does not
/// over-filter and lose genuine KNOWS edges.
#[test]
fn distance_is_not_understated_by_other_edge_types() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Alice", "KNOWS", "Person", "Bob"),
        Value::Int64(1),
        "Alice -[:KNOWS]-> Bob is a direct edge"
    );
    assert_eq!(
        sp(&db, "Person", "Alice", "KNOWS", "Person", "Dave"),
        Value::Int64(3),
        "Alice -> Bob -> Carol -> Dave is 3 KNOWS hops; the MENTIONS shortcut \
         Alice -> Carol must not count"
    );
}

// ── Unreachable pairs ───────────────────────────────────────────────────────

/// Grace (Person slot 6) has no edges whatsoever, so nothing reaches her:
/// `shortestPath((Alice)-[:KNOWS*]->(Grace))` is NULL.
///
/// The KNOWS chain is one-directional, so there is also no route back from
/// Carol to Alice: Carol's only outgoing KNOWS edge is e4 to Dave, and Dave has
/// no outgoing KNOWS edge. `shortestPath((Carol)-[:KNOWS*]->(Alice))` is NULL.
///
/// Before the fix the Grace assertion already passed on its own (no node of any
/// label occupies slot 6 in the reachable set) — it is the honest control here.
/// The Carol assertion returned 2: with the type filter gone,
/// Carol -> Dave -> Metro (City slot 0) matched destination Alice (Person slot
/// 0) at depth 2, so the test as a whole failed before the fix.
#[test]
fn unreachable_pairs_return_null() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Alice", "KNOWS", "Person", "Grace"),
        Value::Null,
        "Grace has no edges; no path can reach her"
    );
    assert_eq!(
        sp(&db, "Person", "Carol", "KNOWS", "Person", "Alice"),
        Value::Null,
        "the KNOWS chain is one-directional; there is no route back to Alice"
    );
}

// ── A relationship type that does not exist at all ──────────────────────────

/// `SPEAKS_TO` is not in the catalog, so no edge of that type can exist and
/// `shortestPath((Alice)-[:SPEAKS_TO*]->(Bob))` is NULL — even though Alice and
/// Bob are one KNOWS hop apart.
///
/// Before the fix the type name was ignored entirely, so this walked the KNOWS
/// edge and returned 1.
#[test]
fn unknown_relationship_type_yields_no_path() {
    let (_dir, db) = build_graph();
    assert_eq!(
        sp(&db, "Person", "Alice", "SPEAKS_TO", "Person", "Bob"),
        Value::Null,
        "no SPEAKS_TO edges exist in the graph"
    );
}

// ── The same answers must hold once the graph is checkpointed ───────────────

/// `checkpoint()` moves every edge out of the delta log and into the per-rel-type
/// CSR files. The delta log carries full `NodeId`s, so a neighbour's label can be
/// read straight off it; a CSR entry is a bare slot, so the label has to come
/// from somewhere else. The answers must not change.
///
/// Every expectation below is the same hand derivation as the tests above,
/// re-asserted against CSR-backed storage:
///   * Alice -[:LIVES_IN]-> Junction (e1) is one hop, and Junction is a `City`
///     while Alice is a `Person` — a cross-label destination, which is precisely
///     the case a slot-only comparison cannot express.
///   * Dave has no outgoing KNOWS edge, so no KNOWS path reaches Alice.
///   * Alice -> Bob -> Carol -> Dave is 3 KNOWS hops.
///   * Bob -> Alice -> Frank is 2 MENTIONS hops. `MENTIONS` spans two label
///     pairs here (Person->Post via e9 and Person->Person via e10/e11/e12), so
///     it is registered as two separate relationship tables — the traversal has
///     to merge both and keep each one's destination label straight.
#[test]
fn answers_are_unchanged_after_checkpoint() {
    let (_dir, db) = build_graph();
    db.checkpoint().expect("checkpoint");

    assert_eq!(
        sp(&db, "Person", "Alice", "LIVES_IN", "City", "Junction"),
        Value::Int64(1),
        "Alice -[:LIVES_IN]-> Junction is a direct cross-label edge"
    );
    assert_eq!(
        sp(&db, "Person", "Dave", "KNOWS", "Person", "Alice"),
        Value::Null,
        "Dave has no outgoing KNOWS edge"
    );
    assert_eq!(
        sp(&db, "Person", "Alice", "KNOWS", "Person", "Dave"),
        Value::Int64(3),
        "Alice -> Bob -> Carol -> Dave is 3 KNOWS hops"
    );
    assert_eq!(
        sp(&db, "Person", "Bob", "MENTIONS", "Person", "Frank"),
        Value::Int64(2),
        "Bob -[:MENTIONS]-> Alice -[:MENTIONS]-> Frank is 2 hops"
    );
}
