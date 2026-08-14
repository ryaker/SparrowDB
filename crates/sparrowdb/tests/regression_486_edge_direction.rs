//! Regression test for GitHub issue #486:
//! Left-pointing relationship patterns (`<-[:R]-`) did not traverse inbound.
//! `execute_one_hop` (crates/sparrowdb-execution/src/engine/hop.rs) always
//! walked the pattern's first node as the physical CSR/delta source and the
//! second node as the physical destination, regardless of `RelPattern::dir`.
//! So `(a)<-[:R]-(x)` was silently executed as if it had been written
//! `(a)-[:R]->(x)`: it invented inbound relationships that did not exist and
//! hid the ones that did, both without ever surfacing an error.
//!
//! Root cause: `execute_one_hop` extracted `src_node_pat = pat.nodes[0]` /
//! `dst_node_pat = pat.nodes[1]` unconditionally. The comment directly above
//! the extraction claimed a swap happened "below" for the incoming case, but
//! no such swap existed anywhere in the function — `EdgeDir::Incoming` was
//! never checked. The fix swaps which pattern node plays the physical-source
//! role before any label resolution, table filtering, or CSR/delta lookup
//! happens, mirroring the swap `execute_one_hop_chunked` already performed
//! correctly in the Phase-2 chunked pipeline (pipeline_exec.rs) for the
//! narrower case where both endpoints carry exactly one label.
//!
//! All expected values below are derived by hand from each fixture's create
//! calls, never captured from program output.

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

// ── Test 1: the exact table from the issue report ───────────────────────────
//
// Graph: a1 --R--> b1  (single directed edge, A -> B)
//
//   MATCH (a:A)-[r:R]->(x) RETURN x.id        → ["b1"]   (real outbound edge)
//   MATCH (a:A)<-[r:R]-(x) RETURN x.id        → []       (nothing points at a1)
//   MATCH (b:B)<-[r:R]-(x) RETURN x.id        → ["a1"]   (a1 points at b1)

#[test]
fn issue_486_reproduction_table() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:B {id: 'b1'})").expect("b1");
    db.execute("MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .expect("a1->b1");

    let outbound_from_a = db
        .execute("MATCH (a:A)-[r:R]->(x) RETURN x.id")
        .expect("outbound from a");
    assert_eq!(
        collect_strings(&outbound_from_a.rows),
        vec!["b1".to_string()],
        "outbound MATCH (a:A)-[r:R]->(x) must return b1"
    );

    let inbound_to_a = db
        .execute("MATCH (a:A)<-[r:R]-(x) RETURN x.id")
        .expect("inbound to a");
    assert!(
        inbound_to_a.rows.is_empty(),
        "#486: MATCH (a:A)<-[r:R]-(x) invented a relationship that does not \
         exist; expected [], got {:?}",
        collect_strings(&inbound_to_a.rows)
    );

    let inbound_to_b = db
        .execute("MATCH (b:B)<-[r:R]-(x) RETURN x.id")
        .expect("inbound to b");
    assert_eq!(
        collect_strings(&inbound_to_b.rows),
        vec!["a1".to_string()],
        "#486: MATCH (b:B)<-[r:R]-(x) hid the real a1->b1 edge; expected [\"a1\"]"
    );
}

// ── Test 2: anchor-vs-direction isolation ────────────────────────────────────
//
// Same inbound question, written the other way: arrow points right, the
// labelled node is on the right, the anchor variable is unlabeled. If this
// works while `<-` fails, the bug is specifically in `<-` token handling
// rather than in anchor selection.
//
//   MATCH (x)-[r:R]->(b:B) RETURN x.id        → ["a1"]

#[test]
fn issue_486_anchor_vs_direction_isolation() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:A {id: 'a1'})").expect("a1");
    db.execute("CREATE (:B {id: 'b1'})").expect("b1");
    db.execute("MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R]->(b)")
        .expect("a1->b1");

    let result = db
        .execute("MATCH (x)-[r:R]->(b:B) RETURN x.id")
        .expect("outbound anchored on the right");
    assert_eq!(
        collect_strings(&result.rows),
        vec!["a1".to_string()],
        "MATCH (x)-[r:R]->(b:B) must return a1 regardless of which side is labelled"
    );
}

// ── Test 3: multiple inbound sources ─────────────────────────────────────────
//
// Graph: X --FOLLOWS--> Z,  Y --FOLLOWS--> Z   (two nodes point at one target)
//
//   MATCH (z:Person {name:'Z'})<-[:FOLLOWS]-(follower) RETURN follower.name
//     → ["X", "Y"]  (both followers, not just one)

#[test]
fn issue_486_multiple_inbound_sources() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'X'})").expect("X");
    db.execute("CREATE (:Person {name: 'Y'})").expect("Y");
    db.execute("CREATE (:Person {name: 'Z'})").expect("Z");

    db.execute("MATCH (a:Person {name:'X'}), (b:Person {name:'Z'}) CREATE (a)-[:FOLLOWS]->(b)")
        .expect("X->Z");
    db.execute("MATCH (a:Person {name:'Y'}), (b:Person {name:'Z'}) CREATE (a)-[:FOLLOWS]->(b)")
        .expect("Y->Z");

    let result = db
        .execute("MATCH (z:Person {name:'Z'})<-[:FOLLOWS]-(follower) RETURN follower.name")
        .expect("followers of Z");

    assert_eq!(
        collect_strings(&result.rows),
        vec!["X".to_string(), "Y".to_string()],
        "#486: inbound traversal must return ALL sources pointing at the \
         target, not just one"
    );
}

// ── Test 4: a node with both inbound and outbound edges ─────────────────────
//
// Graph: A --KNOWS--> B,  C --KNOWS--> A
//
// From A's perspective:
//   outbound (->): A knows B            → ["B"]
//   inbound  (<-): C knows A            → ["C"]
//
// These must be different, correct sets. A version of the bug that ignores
// direction entirely (rather than inverting it) would return the same set —
// or the wrong set — for both queries; this test catches that failure mode
// too, not just simple inversion.

#[test]
fn issue_486_node_with_both_directions() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'A'})").expect("A");
    db.execute("CREATE (:Person {name: 'B'})").expect("B");
    db.execute("CREATE (:Person {name: 'C'})").expect("C");

    db.execute("MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("A->B");
    db.execute("MATCH (c:Person {name:'C'}), (a:Person {name:'A'}) CREATE (c)-[:KNOWS]->(a)")
        .expect("C->A");

    let outbound = db
        .execute("MATCH (a:Person {name:'A'})-[:KNOWS]->(x) RETURN x.name")
        .expect("outbound from A");
    assert_eq!(
        collect_strings(&outbound.rows),
        vec!["B".to_string()],
        "outbound from A must be exactly [B]"
    );

    let inbound = db
        .execute("MATCH (a:Person {name:'A'})<-[:KNOWS]-(x) RETURN x.name")
        .expect("inbound to A");
    assert_eq!(
        collect_strings(&inbound.rows),
        vec!["C".to_string()],
        "#486: inbound to A must be exactly [C] — a direction-ignoring engine \
         would return [B] here too"
    );
}

// ── Test 5: self-loop — both directions coincide ─────────────────────────────
//
// Graph: Loner --KNOWS--> Loner  (self-loop)
//
// Both `->` and `<-` from Loner must return Loner, since the single edge's
// source and destination are the same node.

#[test]
fn issue_486_self_loop_both_directions_coincide() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Loner'})")
        .expect("Loner");
    db.execute("MATCH (a:Person {name:'Loner'}) CREATE (a)-[:KNOWS]->(a)")
        .expect("Loner->Loner self-loop");

    let outbound = db
        .execute("MATCH (a:Person {name:'Loner'})-[:KNOWS]->(x) RETURN x.name")
        .expect("outbound self-loop");
    assert_eq!(
        collect_strings(&outbound.rows),
        vec!["Loner".to_string()],
        "outbound self-loop must return Loner"
    );

    let inbound = db
        .execute("MATCH (a:Person {name:'Loner'})<-[:KNOWS]-(x) RETURN x.name")
        .expect("inbound self-loop");
    assert_eq!(
        collect_strings(&inbound.rows),
        vec!["Loner".to_string()],
        "#486: inbound self-loop must also return Loner — both directions \
         coincide on a self-loop"
    );
}

// ── Test 6: multiple relationship types — inbound must not cross types ──────
//
// Graph: Q --R--> P,  T --S--> P   (two different rel types pointing at P)
//
//   MATCH (p:Person {name:'P'})<-[:R]-(x) RETURN x.name  → ["Q"] only,
//   never "T" (T's edge to P is type S, not R).

#[test]
fn issue_486_multiple_rel_types_no_cross_contamination() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'P'})").expect("P");
    db.execute("CREATE (:Person {name: 'Q'})").expect("Q");
    db.execute("CREATE (:Person {name: 'T'})").expect("T");

    db.execute("MATCH (q:Person {name:'Q'}), (p:Person {name:'P'}) CREATE (q)-[:R]->(p)")
        .expect("Q-R->P");
    db.execute("MATCH (t:Person {name:'T'}), (p:Person {name:'P'}) CREATE (t)-[:S]->(p)")
        .expect("T-S->P");

    let result = db
        .execute("MATCH (p:Person {name:'P'})<-[:R]-(x) RETURN x.name")
        .expect("inbound :R to P");

    assert_eq!(
        collect_strings(&result.rows),
        vec!["Q".to_string()],
        "#486: inbound :R traversal must not pick up the :S edge from T"
    );
}
