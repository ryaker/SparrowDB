//! Regression tests for GitHub issue #436:
//! `MATCH (n:Label) DELETE n` leaves dangling edges; `DETACH DELETE` fails
//! with `relationship type '...' not found in catalog for labels (...)`.
//!
//! ## Root cause
//!
//! CSR forward/backward files (`crates/sparrowdb-storage/src/csr.rs`) are
//! indexed by **label-relative slot** — the lower 32 bits of a packed
//! `NodeId` — not by the packed `NodeId` itself.
//!
//! `WriteTx::delete_node` (crates/sparrowdb/src/write_tx.rs) called
//! `csr.neighbors(node_id.0)` / `csr.predecessors(node_id.0)` with the full
//! packed id (label bits included). For any label other than label 0 this
//! value is far larger than `CsrForward::n_nodes`/`CsrBackward::n_nodes`, so
//! `neighbors`/`predecessors` returned `&[]` unconditionally and the
//! `NodeHasEdges` safety check never fired for *checkpointed* edges. A node
//! with a checkpointed edge could therefore be `DELETE`d, leaving that edge's
//! endpoint pointing at a tombstoned node — the dangling edge from the issue.
//!
//! `WriteTx::detach_delete_node` correctly stripped the label bits
//! (`node_slot`) before indexing, but checked *every* registered
//! relationship table's CSR regardless of whether `node_id`'s own label
//! matched that table's declared src/dst label. When `node_id`'s slot number
//! coincidentally aligned with a valid slot in an unrelated table (a
//! different label pair entirely — e.g. an ontology-internal `__SO_*` rel
//! type, or here a second application rel type), the code treated a
//! completely unrelated adjacency-list entry as if it were `node_id`'s own
//! edge, including manufacturing a nonexistent self-loop. The bogus edge
//! tuple then failed `catalog.get_rel_table(src_label, dst_label, rel_type)`
//! and `DETACH DELETE` returned an `InvalidArgument` "not found in catalog"
//! error instead of ever deleting the node — exactly the error text from the
//! issue.
//!
//! The fix (crates/sparrowdb/src/write_tx.rs):
//!   - `delete_node`: index CSR files with `node_id.0 & 0xFFFF_FFFF` (slot,
//!     not full id), and only consult a table's forward CSR when node_id's
//!     label matches that table's src label / backward CSR when it matches
//!     the dst label.
//!   - `detach_delete_node`: apply the same src/dst label filter before
//!     consulting a table's CSR, so an unrelated table's coincidental slot
//!     can never be attributed to node_id.
//!
//! All expected values below are derived by hand from each fixture's create
//! calls, never captured from program output. Every `Result` is asserted on
//! explicitly — nothing is discarded with `.ok()`.

use sparrowdb::open;
use sparrowdb_common::Error;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Build the same "bump the label counter" prelude every fixture below needs:
/// without it, the first two labels created (Person, Company) would land on
/// label_id 0 and 1, and label_id 0 is exactly the case where packed
/// `NodeId == slot`, masking the original `delete_node` bug entirely. A
/// throwaway label ensures Person/Company get non-zero label ids, matching
/// the realistic case (any DB with more than one label already in use).
fn seed_dummy_label(db: &sparrowdb::GraphDb) {
    db.execute("CREATE (:Dummy {x: 1})").expect("dummy label");
}

// ── 1. Plain DELETE on a node with a checkpointed edge must error ──────────

/// Before checkpoint (delta-only edge): `DELETE` on a node with an edge must
/// return `NodeHasEdges`, not silently succeed. This path was not broken by
/// #436 (the delta-log check compares full NodeIds, which is unambiguous),
/// but is included as a baseline so the checkpointed case below has a known-
/// good control.
#[test]
fn delete_without_detach_errors_pre_checkpoint() {
    let (_dir, db) = make_db();
    seed_dummy_label(&db);

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("alice");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("acme");
    db.execute(
        "MATCH (a:Person {name:'Alice'}), (c:Company {name:'Acme'}) CREATE (a)-[:WORKS_FOR]->(c)",
    )
    .expect("edge");

    let result = db.execute("MATCH (n:Person {name: 'Alice'}) DELETE n");
    match result {
        Err(Error::NodeHasEdges { .. }) => {}
        other => panic!(
            "expected NodeHasEdges for DELETE on a node with a (delta-only) edge, got {other:?}"
        ),
    }

    // Alice must still exist — the delete must not have partially applied.
    let alice = db
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice");
    assert_eq!(
        alice.rows.len(),
        1,
        "Alice must still be present after the errored DELETE"
    );
}

/// The actual #436 bug: after `checkpoint()`, `DELETE` on a node with an edge
/// must *still* return `NodeHasEdges`. Pre-fix, `csr.neighbors(node_id.0)`
/// indexed with the full packed id (Person's label_id is 1 here, since
/// Dummy took label 0, so `node_id.0 == 1 << 32`, far beyond
/// `CsrForward::n_nodes`), so the check silently no-opped and the DELETE
/// succeeded — leaving Acme's incoming edge pointing at a tombstoned Alice.
#[test]
fn delete_without_detach_errors_post_checkpoint() {
    let (dir, db) = make_db();
    seed_dummy_label(&db);

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("alice");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("acme");
    db.execute(
        "MATCH (a:Person {name:'Alice'}), (c:Company {name:'Acme'}) CREATE (a)-[:WORKS_FOR]->(c)",
    )
    .expect("edge");

    db.checkpoint().expect("checkpoint");

    let result = db.execute("MATCH (n:Person {name: 'Alice'}) DELETE n");
    match result {
        Err(Error::NodeHasEdges { .. }) => {}
        other => panic!(
            "#436: expected NodeHasEdges for DELETE on a node with a checkpointed edge, got {other:?}"
        ),
    }

    // Alice must still exist and the edge must still resolve correctly —
    // this is the "traverse from the other end" check: if the delete had
    // silently gone through, this MATCH would still return Alice as a
    // dangling endpoint even though `n:Person` no longer would.
    let alice = db
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice");
    assert_eq!(
        alice.rows.len(),
        1,
        "Alice must still be present after the errored DELETE"
    );

    let traversal = db
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("traverse");
    assert_eq!(
        traversal.rows,
        vec![vec![
            Value::String("Alice".into()),
            Value::String("Acme".into())
        ]],
        "the real edge must be unchanged"
    );

    // Reopen and re-verify: the rejection (and thus the still-live node and
    // edge) must be durable, not an artifact of in-memory delta-log state.
    drop(db);
    let db2 = open(dir.path()).expect("reopen");
    let alice2 = db2
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice after reopen");
    assert_eq!(alice2.rows.len(), 1, "Alice must survive reopen");
    let traversal2 = db2
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("traverse after reopen");
    assert_eq!(
        traversal2.rows,
        vec![vec![
            Value::String("Alice".into()),
            Value::String("Acme".into())
        ]],
        "the real edge must survive reopen unchanged"
    );
}

// ── 2. DETACH DELETE must actually remove the node AND its edges ───────────

/// DETACH DELETE before checkpoint (delta-only edge): node and edge both
/// gone, verified from both traversal directions, and durable across reopen.
#[test]
fn detach_delete_removes_node_and_edge_pre_checkpoint() {
    let (dir, db) = make_db();
    seed_dummy_label(&db);

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("alice");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("acme");
    db.execute(
        "MATCH (a:Person {name:'Alice'}), (c:Company {name:'Acme'}) CREATE (a)-[:WORKS_FOR]->(c)",
    )
    .expect("edge");

    db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .expect("DETACH DELETE must succeed");

    let alice = db
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice");
    assert!(
        alice.rows.is_empty(),
        "Alice must be gone after DETACH DELETE"
    );

    let fwd = db
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("forward traversal");
    assert!(
        fwd.rows.is_empty(),
        "no edge must remain (forward direction)"
    );

    // Traverse from the other end: a stale edge would show up as Acme
    // returning a predecessor that no longer exists.
    let bwd = db
        .execute("MATCH (c:Company)<-[:WORKS_FOR]-(a:Person) RETURN c.name, a.name")
        .expect("backward traversal");
    assert!(
        bwd.rows.is_empty(),
        "no edge must remain (backward direction)"
    );

    drop(db);
    let db2 = open(dir.path()).expect("reopen");
    let alice2 = db2
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice after reopen");
    assert!(
        alice2.rows.is_empty(),
        "Alice must remain gone after reopen"
    );
    let fwd2 = db2
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("forward traversal after reopen");
    assert!(fwd2.rows.is_empty(), "no edge must remain after reopen");
}

/// DETACH DELETE after checkpoint: the CSR-backed path. This is the case
/// that previously either raised the wrong "not found in catalog" error
/// (multi-table pollution below) or, in the single-table case, worked by
/// accident because the collected edge tuple's labels happened to be
/// self-consistent — verifying it explicitly here as the durability case.
#[test]
fn detach_delete_removes_node_and_edge_post_checkpoint() {
    let (dir, db) = make_db();
    seed_dummy_label(&db);

    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("alice");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("acme");
    db.execute(
        "MATCH (a:Person {name:'Alice'}), (c:Company {name:'Acme'}) CREATE (a)-[:WORKS_FOR]->(c)",
    )
    .expect("edge");

    db.checkpoint().expect("checkpoint");

    db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .expect("DETACH DELETE must succeed post-checkpoint");

    let alice = db
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice");
    assert!(
        alice.rows.is_empty(),
        "Alice must be gone after DETACH DELETE"
    );

    let fwd = db
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("forward traversal");
    assert!(
        fwd.rows.is_empty(),
        "no checkpointed edge must remain (forward)"
    );
    let bwd = db
        .execute("MATCH (c:Company)<-[:WORKS_FOR]-(a:Person) RETURN c.name, a.name")
        .expect("backward traversal");
    assert!(
        bwd.rows.is_empty(),
        "no checkpointed edge must remain (backward)"
    );

    drop(db);
    let db2 = open(dir.path()).expect("reopen");
    let alice2 = db2
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice after reopen");
    assert!(
        alice2.rows.is_empty(),
        "Alice must remain gone after reopen"
    );
    let fwd2 = db2
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("forward traversal after reopen");
    assert!(
        fwd2.rows.is_empty(),
        "no checkpointed edge must remain after reopen"
    );

    // Company node itself (never targeted for deletion) must be untouched.
    let acme = db2
        .execute("MATCH (n:Company {name:'Acme'}) RETURN n.name")
        .expect("match acme after reopen");
    assert_eq!(
        acme.rows.len(),
        1,
        "Acme must survive — only Alice was deleted"
    );
}

// ── 3. Cross-table pollution: the exact "not found in catalog" repro ───────

/// Two independent relationship tables (WORKS_FOR: Person→Company, PART_OF:
/// Widget→Assembly) are checkpointed. Alice (Person, slot 0) is the target
/// of DETACH DELETE. Because Widget's first node (slot 0) also has an
/// outgoing PART_OF edge, and Alice's own slot is 0, the pre-fix code
/// misattributed PART_OF's CSR entries to Alice — producing edge tuples
/// whose (src_label, dst_label, rel_type) combination did not exist in the
/// catalog, which is exactly the `relationship type '...' not found in
/// catalog for labels (...)` error from the issue.
///
/// The fix must both (a) let DETACH DELETE succeed, and (b) leave the
/// unrelated Widget→Assembly edge completely untouched — a naive fix that
/// merely suppressed the error without fixing the label filter could still
/// silently delete the wrong edge.
#[test]
fn detach_delete_does_not_cross_pollute_unrelated_rel_table() {
    let (dir, db) = make_db();

    // Person = label 0, Company = label 1 (first two labels created).
    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("alice");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("acme");
    db.execute(
        "MATCH (a:Person {name:'Alice'}), (c:Company {name:'Acme'}) CREATE (a)-[:WORKS_FOR]->(c)",
    )
    .expect("works_for edge");

    // Widget = label 2, Assembly = label 3. W0 is Widget's slot 0, same as
    // Alice's slot 0 in Person — the coincidence the pre-fix code mishandled.
    db.execute("CREATE (:Widget {name: 'W0'})").expect("w0");
    db.execute("CREATE (:Assembly {name: 'AsmX'})")
        .expect("asmx");
    db.execute(
        "MATCH (w:Widget {name:'W0'}), (asm:Assembly {name:'AsmX'}) CREATE (w)-[:PART_OF]->(asm)",
    )
    .expect("part_of edge");

    db.checkpoint().expect("checkpoint");

    let result = db.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n");
    match result {
        Ok(_) => {}
        Err(e) => panic!(
            "#436: DETACH DELETE must succeed, not fail resolving an unrelated \
             rel table's catalog entry; got {e:?}"
        ),
    }

    // Alice and her WORKS_FOR edge are gone.
    let alice = db
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice");
    assert!(alice.rows.is_empty(), "Alice must be gone");
    let works_for = db
        .execute("MATCH (a:Person)-[:WORKS_FOR]->(c:Company) RETURN a.name, c.name")
        .expect("works_for traversal");
    assert!(works_for.rows.is_empty(), "WORKS_FOR edge must be gone");

    // The completely unrelated Widget -[:PART_OF]-> Assembly edge, and both
    // of its endpoints, must be untouched.
    let w0 = db
        .execute("MATCH (n:Widget {name:'W0'}) RETURN n.name")
        .expect("match w0");
    assert_eq!(
        w0.rows.len(),
        1,
        "W0 must be untouched by Alice's DETACH DELETE"
    );
    let asmx = db
        .execute("MATCH (n:Assembly {name:'AsmX'}) RETURN n.name")
        .expect("match asmx");
    assert_eq!(
        asmx.rows.len(),
        1,
        "AsmX must be untouched by Alice's DETACH DELETE"
    );
    let part_of = db
        .execute("MATCH (w:Widget)-[:PART_OF]->(a:Assembly) RETURN w.name, a.name")
        .expect("part_of traversal");
    assert_eq!(
        part_of.rows,
        vec![vec![
            Value::String("W0".to_string()),
            Value::String("AsmX".to_string())
        ]],
        "PART_OF edge must survive Alice's unrelated DETACH DELETE"
    );

    // Durable across reopen.
    drop(db);
    let db2 = open(dir.path()).expect("reopen");
    let part_of2 = db2
        .execute("MATCH (w:Widget)-[:PART_OF]->(a:Assembly) RETURN w.name, a.name")
        .expect("part_of traversal after reopen");
    assert_eq!(
        part_of2.rows,
        vec![vec![
            Value::String("W0".to_string()),
            Value::String("AsmX".to_string())
        ]],
        "PART_OF edge must survive reopen"
    );
    let alice2 = db2
        .execute("MATCH (n:Person {name:'Alice'}) RETURN n.name")
        .expect("match alice after reopen");
    assert!(
        alice2.rows.is_empty(),
        "Alice must remain gone after reopen"
    );
}
