//! Regression guards for issue #478 — `UNION`-wrapped `CREATE` reports
//! success and durably writes nothing.
//!
//! `Engine::is_mutation` enumerated `Merge`, `MatchMergeRel`, `MatchMutate`,
//! `MatchCreate`, `Create`, and recursive `CallSubquery` — but not
//! `Statement::Union`, which fell through to `_ => false`. A query shaped
//! `CREATE (...) RETURN 1 UNION CREATE (...) RETURN 2` therefore skipped the
//! mutation gate in `GraphDb::execute`, was dispatched through the read-only
//! engine, and `execute_union` recursed into the non-transactional
//! `mutation.rs::execute_create` — which writes against a `NodeStore` opened
//! for that one read-only call and never durably persists. The statement
//! returned `Ok` with the expected row count and zero nodes existed
//! afterwards.
//!
//! The fix rejects `UNION` containing a mutating branch at bind time
//! (`sparrowdb-cypher::binder::bind`), matching the Cypher spec (UNION is
//! only defined over read queries) rather than trying to route the mutation
//! through the write-transaction path. `Statement::is_mutation` was also
//! promoted to a single method on `Statement` (`sparrowdb-cypher::ast`) so
//! `Engine::is_mutation` and the binder's UNION check share one
//! classification instead of two enumerations that can drift apart — which
//! is exactly the shape of bug #478 was.
//!
//! The load-bearing assertion in every durability test here is **after
//! reopening the database from a dropped handle** — querying the same handle
//! that ran the mutation can pass even when nothing reached disk, which is
//! how this bug hid in the first place.
//!
//! Every expected value below is derived by hand from the fixture the test
//! itself builds, not recorded from the code's current output.

use sparrowdb::GraphDb;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

/// Reopen `dir` as a fresh `GraphDb` handle and count `:Label` nodes via a
/// plain read query. Used after `drop`-ing the handle that ran the mutation
/// under test, so the count reflects only what is actually durable on disk.
fn count_label_after_reopen(dir: &std::path::Path, label: &str) -> i64 {
    let db = GraphDb::open(dir).expect("reopen db");
    let result = db
        .execute(&format!("MATCH (n:{label}) RETURN count(n)"))
        .expect("count query must succeed");
    match result.rows[0][0] {
        sparrowdb_execution::Value::Int64(n) => n,
        ref other => panic!("expected Int64 count, got {other:?}"),
    }
}

// ── Test 1: UNION of two CREATEs must be rejected, not silently no-op ────────

#[test]
fn union_of_two_creates_is_rejected_not_silently_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let result = db.execute(
        "CREATE (:UnionA {x: 1}) RETURN 1 \
         UNION \
         CREATE (:UnionB {y: 2}) RETURN 2",
    );

    // The defect was: this returned Ok. The fix must make it an error rather
    // than let it through to durably write nothing.
    assert!(
        result.is_err(),
        "UNION of two CREATEs must be rejected at bind time, got Ok: {:?}",
        result
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("UNION") || msg.to_lowercase().contains("mutat"),
        "error must explain the UNION/mutation restriction, got: {msg}"
    );

    drop(db);

    // Zero nodes of either label may exist — not the "wrote nothing but
    // reported success" shape and not a partial write either.
    assert_eq!(
        count_label_after_reopen(dir.path(), "UnionA"),
        0,
        "rejected UNION CREATE must not have created any UnionA node"
    );
    assert_eq!(
        count_label_after_reopen(dir.path(), "UnionB"),
        0,
        "rejected UNION CREATE must not have created any UnionB node"
    );
}

// ── Test 2: UNION mixing a CREATE with a plain MATCH must also be rejected ──

#[test]
fn union_mixing_create_and_match_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Existing {name: 'Alice'})").unwrap();

    // Left side is a plain read; right side mutates. The mutating side must
    // still be caught regardless of position.
    let result = db.execute(
        "MATCH (n:Existing) RETURN n.name \
         UNION \
         CREATE (:MixedCreate {z: 3}) RETURN 'created'",
    );
    assert!(
        result.is_err(),
        "UNION mixing MATCH with CREATE must be rejected, got Ok: {:?}",
        result
    );

    // And the same with the mutation on the left.
    let result2 = db.execute(
        "CREATE (:MixedCreate2 {z: 4}) RETURN 'created' \
         UNION \
         MATCH (n:Existing) RETURN n.name",
    );
    assert!(
        result2.is_err(),
        "UNION mixing CREATE (left) with MATCH must be rejected, got Ok: {:?}",
        result2
    );

    drop(db);

    assert_eq!(
        count_label_after_reopen(dir.path(), "MixedCreate"),
        0,
        "rejected UNION must not have created a MixedCreate node"
    );
    assert_eq!(
        count_label_after_reopen(dir.path(), "MixedCreate2"),
        0,
        "rejected UNION must not have created a MixedCreate2 node"
    );
    // The pre-existing node from the plain CREATE above is untouched.
    assert_eq!(
        count_label_after_reopen(dir.path(), "Existing"),
        1,
        "the plain CREATE that ran before either UNION attempt must be unaffected"
    );
}

// ── Test 3: UNION ALL of two CREATEs is rejected the same way ───────────────

#[test]
fn union_all_of_two_creates_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let result = db.execute(
        "CREATE (:UnionAllA {x: 1}) RETURN 1 \
         UNION ALL \
         CREATE (:UnionAllB {y: 2}) RETURN 2",
    );
    assert!(
        result.is_err(),
        "UNION ALL of two CREATEs must be rejected, got Ok: {:?}",
        result
    );

    drop(db);

    assert_eq!(
        count_label_after_reopen(dir.path(), "UnionAllA"),
        0,
        "rejected UNION ALL CREATE must not have created any UnionAllA node"
    );
    assert_eq!(
        count_label_after_reopen(dir.path(), "UnionAllB"),
        0,
        "rejected UNION ALL CREATE must not have created any UnionAllB node"
    );
}

// ── Test 4: a purely read-only UNION must keep working exactly as before ────

#[test]
fn read_only_union_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Carol', age: 35})")
        .unwrap();

    let result = db
        .execute(
            "MATCH (n:Person) WHERE n.age > 28 RETURN n.age \
             UNION ALL \
             MATCH (n:Person) WHERE n.age < 28 RETURN n.age",
        )
        .expect("read-only UNION ALL must still succeed after the #478 fix");

    // Left: Alice(30), Carol(35) = 2 rows; Right: Bob(25) = 1 row → total 3.
    assert_eq!(
        result.rows.len(),
        3,
        "read-only UNION ALL must be unaffected by the mutation-rejection fix, got {:?}",
        result.rows
    );
}

// ── Test 5: MERGE (a different mutation shape) inside UNION is also rejected ─
//
// Unlike CREATE, a UNION-wrapped MERGE already failed before this fix — but
// with an unrelated, confusing error ("mutation statements must be executed
// via execute_mutation", from a leftover guard inside the read engine's
// dispatch, since Merge was never wired into the read engine's match at all).
// So this test is not a regression guard for the #478 *symptom* (MERGE never
// silently no-opped), but it does confirm the new bind-time UNION check
// covers MERGE too — with the correct, UNION-specific explanation — instead
// of leaving that pre-existing, differently-broken error path in place.
#[test]
fn union_with_merge_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let result = db.execute(
        "MERGE (:MergedNode {k: 1}) RETURN 1 \
         UNION \
         MATCH (n:MergedNode) RETURN n.k",
    );
    assert!(
        result.is_err(),
        "UNION containing MERGE must be rejected like CREATE, got Ok: {:?}",
        result
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("UNION"),
        "post-fix, MERGE-in-UNION must be rejected by the new UNION-specific \
         check (not the old unrelated 'must be executed via execute_mutation' \
         error), got: {msg}"
    );

    drop(db);

    assert_eq!(
        count_label_after_reopen(dir.path(), "MergedNode"),
        0,
        "rejected UNION MERGE must not have created any MergedNode node"
    );
}
