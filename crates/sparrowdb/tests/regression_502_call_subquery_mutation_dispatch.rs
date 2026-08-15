//! Regression guards for issue #502 — a bare top-level `CALL { <mutation> }`
//! statement panicked instead of erroring or executing.
//!
//! Found while independently verifying the #478 fix. `Statement::is_mutation`
//! classifies `Statement::CallSubquery { subquery, .. }` as a mutation
//! whenever its inner `subquery` is itself a mutation (recursively), so
//! `GraphDb::execute`'s `if Engine::is_mutation(&bound.inner) { … }` branch
//! was entered for a bare `CALL { CREATE (:M1 {z:1}) }`. But the match inside
//! that branch had explicit arms only for `Merge`, `MatchMergeRel`,
//! `MatchMutate`, `UnwindMatchMutate`, `MatchCreate`, and `Create` — no arm
//! for `CallSubquery` — and fell through to `_ => unreachable!()`, which is
//! reachable on ordinary user input and panics the calling thread.
//!
//! Same defect class as #478 (classification says "route to the write path"
//! but the write path was never actually wired up for that shape), just one
//! level removed: #478 was a *read*-path gap (reports success, writes
//! nothing), #502 is a *write*-dispatch gap (never reports anything, panics).
//!
//! Independently, `execute_with_timeout`'s copy of this same match was
//! missing the `MatchMergeRel` arm entirely — a second, unrelated instance of
//! the identical defect class, found while fixing this one. Both are fixed by
//! replacing the three independently-drifting copies of this match
//! (`execute`, `execute_chunked`, `execute_with_timeout`) — plus hardening
//! `execute_with_params`'s and `execute_batch_mutation`'s pre-existing (but
//! differently worded) guards — with a single shared `GraphDb::dispatch_mutation`
//! whose non-mutation fallback arm is exhaustively listed (no `_` wildcard),
//! so a variant `Statement::is_mutation` classifies as `true` without a
//! matching arm here is a compile error, not a runtime panic.
//!
//! A panic crosses the test-harness thread boundary as a `thread '...' panicked`
//! message plus the test being marked FAILED; `std::panic::catch_unwind` (with
//! the panic hook silenced so the pre-fix runs below don't spam stderr) lets
//! us assert "did not panic" without abort-on-panic settings.
//!
//! Every expected value below is derived by hand from the fixture the test
//! itself builds, not recorded from the code's current output.

use sparrowdb::GraphDb;
use std::panic::{self, AssertUnwindSafe};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

/// Reopen `dir` as a fresh `GraphDb` handle and count `:Label` nodes via a
/// plain read query. Used after `drop`-ing the handle under test, so the
/// count reflects only what is actually durable on disk.
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

/// Run `f` with the default panic hook silenced, so a pre-fix panic in these
/// tests doesn't print a `thread '...' panicked at ...` backtrace to stderr
/// on every run — we still observe and assert on whether it panicked via the
/// returned `Result`.
fn run_catching_panic<F, T>(f: F) -> std::thread::Result<T>
where
    F: FnOnce() -> T,
{
    let prev_hook = panic::take_hook();
    panic::set_hook(Box::new(|_info| {}));
    let result = panic::catch_unwind(AssertUnwindSafe(f));
    panic::set_hook(prev_hook);
    result
}

// ── Test 1: bare top-level CALL { CREATE ... } must not panic ────────────────

#[test]
fn bare_call_subquery_create_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let outcome = run_catching_panic(|| db.execute("CALL { CREATE (:M1 {z: 1}) }"));

    assert!(
        outcome.is_ok(),
        "bare CALL {{ CREATE ... }} must not panic the calling thread (issue #502)"
    );
    let result = outcome.unwrap();
    assert!(
        result.is_err(),
        "bare CALL {{ CREATE ... }} has no transactional executor yet — it must \
         error cleanly, not silently succeed while writing nothing (the #478 shape), \
         got: {:?}",
        result
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("CALL") && msg.to_lowercase().contains("mutat"),
        "error must explain that CALL {{ }} cannot wrap a mutation, got: {msg}"
    );

    drop(db);

    assert_eq!(
        count_label_after_reopen(dir.path(), "M1"),
        0,
        "rejected CALL {{ CREATE }} must not have durably created any M1 node"
    );
}

// ── Test 2: bare top-level CALL { MERGE ... } — a different mutation shape ──

#[test]
fn bare_call_subquery_merge_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let outcome = run_catching_panic(|| db.execute("CALL { MERGE (:M3 {z: 3}) }"));

    assert!(
        outcome.is_ok(),
        "bare CALL {{ MERGE ... }} must not panic the calling thread"
    );
    let result = outcome.unwrap();
    assert!(
        result.is_err(),
        "bare CALL {{ MERGE ... }} must error cleanly like CALL {{ CREATE ... }}, got: {:?}",
        result
    );

    drop(db);

    assert_eq!(
        count_label_after_reopen(dir.path(), "M3"),
        0,
        "rejected CALL {{ MERGE }} must not have durably created any M3 node"
    );
}

// ── Test 3: the same rejection, and the same message, via every entry point ──
//
// Before this fix, `execute`/`execute_chunked`/`execute_with_timeout` each
// carried an independently drifting copy of the mutation dispatch: the first
// two panicked on this exact input, `execute_with_timeout` also panicked (via
// its own copy's `_ => unreachable!()`), and `execute_with_params` alone had
// already been hardened — but with different wording ("parameterized
// MATCH...MERGE relationship and CALL subquery mutations are not yet
// supported"). All four now share one dispatch function and must answer
// identically.

#[test]
fn call_subquery_create_rejected_consistently_across_entry_points() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let cypher = "CALL { CREATE (:M4 {z: 4}) }";

    let via_execute = db.execute(cypher);
    let via_chunked = db.execute_chunked(cypher);
    let via_timeout = db.execute_with_timeout(cypher, std::time::Duration::from_secs(5));
    let via_params = db.execute_with_params(cypher, std::collections::HashMap::new());

    for (name, result) in [
        ("execute", &via_execute),
        ("execute_chunked", &via_chunked),
        ("execute_with_timeout", &via_timeout),
        ("execute_with_params", &via_params),
    ] {
        assert!(
            result.is_err(),
            "{name}: CALL {{ CREATE ... }} must be rejected, got Ok: {:?}",
            result
        );
    }

    // Same wording from every entry point — not four different explanations
    // for the same restriction.
    let msg_execute = format!("{:?}", via_execute.unwrap_err());
    let msg_chunked = format!("{:?}", via_chunked.unwrap_err());
    let msg_timeout = format!("{:?}", via_timeout.unwrap_err());
    let msg_params = format!("{:?}", via_params.unwrap_err());
    assert_eq!(
        msg_execute, msg_chunked,
        "execute and execute_chunked must give the identical error for the identical input"
    );
    assert_eq!(
        msg_execute, msg_timeout,
        "execute and execute_with_timeout must give the identical error for the identical input"
    );
    assert_eq!(
        msg_execute, msg_params,
        "execute and execute_with_params must give the identical error for the identical input"
    );

    drop(db);

    assert_eq!(
        count_label_after_reopen(dir.path(), "M4"),
        0,
        "none of the four rejected calls may have durably created an M4 node"
    );
}

// ── Test 4: MatchMergeRel through execute_with_timeout — found while fixing #502 ─
//
// `execute_with_timeout`'s pre-fix match had arms for Merge, MatchMutate,
// UnwindMatchMutate, MatchCreate, and Create, but not MatchMergeRel — a
// second, independent instance of the same "is_mutation says yes, dispatch
// has no arm" defect, unrelated to CALL { }. `MATCH ... MERGE (a)-[r:T]->(b)`
// run through execute_with_timeout hit that same `_ => unreachable!()`.

#[test]
fn match_merge_rel_through_execute_with_timeout_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:P5 {name: 'A'})").unwrap();
    db.execute("CREATE (:P5 {name: 'B'})").unwrap();

    // MATCH ... MERGE (a)-[r:TYPE]->(b) does not accept a trailing RETURN
    // clause (confirmed against spa_233_merge_relationship.rs's usage).
    let cypher = "MATCH (a:P5 {name: 'A'}), (b:P5 {name: 'B'}) MERGE (a)-[r:LINKED5]->(b)";
    let outcome =
        run_catching_panic(|| db.execute_with_timeout(cypher, std::time::Duration::from_secs(5)));

    assert!(
        outcome.is_ok(),
        "MATCH ... MERGE relationship through execute_with_timeout must not panic"
    );
    let result = outcome.unwrap();
    assert!(
        result.is_ok(),
        "MATCH ... MERGE relationship through execute_with_timeout must succeed \
         (it already worked through plain execute()), got: {:?}",
        result
    );

    drop(db);

    // Hand-derived: MERGE creates exactly one LINKED5 edge A->B.
    let db2 = GraphDb::open(dir.path()).expect("reopen db");
    let check = db2
        .execute("MATCH (:P5 {name: 'A'})-[:LINKED5]->(:P5 {name: 'B'}) RETURN count(*)")
        .expect("count query must succeed");
    assert_eq!(
        check.rows[0][0],
        sparrowdb_execution::Value::Int64(1),
        "exactly one LINKED5 edge must have been durably created"
    );
}
