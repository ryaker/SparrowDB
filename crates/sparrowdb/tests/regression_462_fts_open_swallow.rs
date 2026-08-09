//! Regression test for issue #462: a failed `FtsIndex::open` was swallowed on
//! the write path, so a node CREATE reported success while silently never
//! indexing the node's text — forever, since nothing retries and nothing
//! reports it.
//!
//! # The defect
//!
//! `FtsIndex::open` only returns `Err` when the on-disk index file for a
//! `(label, property)` pair **exists but cannot be read** — truncated,
//! corrupt, or otherwise unreadable. A pair with no index file yet is `Ok`
//! with a fresh empty index (see `FtsIndex::open` in
//! `sparrowdb-storage/src/fts_index.rs`), so `Err` here can only mean
//! "present but broken", never "not configured".
//!
//! Before this fix, both live write paths —
//! `GraphDb::execute_create_standalone` (`crates/sparrowdb/src/db.rs`) and
//! `Engine::execute_create` (`crates/sparrowdb-execution/src/engine/mutation.rs`)
//! — wrapped that `open` in `if let Ok(mut idx) = FtsIndex::open(...)`. On
//! `Err` the whole block was skipped: the node still got created and the
//! statement still returned `Ok`, but the node's indexed text property was
//! never inserted into the BM25 index. The same class of bug already cost
//! this project a silent HNSW data-loss incident (#441/#442/#456); this is
//! its FTS twin.
//!
//! # What this test does
//!
//! Forces `FtsIndex::open` to fail by a real mechanism: create a genuine
//! fulltext index (so the on-disk file exists), then truncate that file to
//! fewer bytes than its own fixed-width header requires. `FtsIndex::load`
//! reads a `u64` first (see `fts_index.rs::load`/`read_u64`); a file shorter
//! than 8 bytes cannot satisfy that read and `read_u64` returns
//! `Error::Corruption("FTS index: unexpected end of data")` — a genuine
//! decode failure, not a permissions trick (which behaves differently under
//! CI's root containers per this repo's testing notes).
//!
//! Every expected value below is derived by hand from the fixture built in
//! this test, not captured from a run of the code (see `regression_406.rs`
//! for what recording observed output instead of deriving it costs).

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

/// Truncate the on-disk FTS index file for `(label, property)` to 3 bytes —
/// too short to satisfy even the first `read_u64` in `FtsIndexData::load`,
/// which needs 8. This is real file corruption (not a permissions change),
/// matching the "prefer corrupting content over chmod" guidance for this repo.
fn corrupt_fts_index_file(db_root: &std::path::Path, label: &str, property: &str) {
    let path = db_root.join("fts").join(format!("{label}__{property}.bin"));
    assert!(
        path.exists(),
        "expected {} to already exist from CREATE FULLTEXT INDEX before corrupting it",
        path.display()
    );
    std::fs::write(&path, [0xFFu8, 0xFF, 0xFF]).expect("truncate fts index file to 3 bytes");
}

/// Force `FtsIndex::save` (not `open`) to fail, without touching permission
/// bits — chmod behaves differently under CI's root containers, and by the
/// time `save` runs, `open` must already have succeeded (otherwise this
/// would just be exercising the `open` failure again).
///
/// `FtsIndex::save` (`fts_index.rs`) writes new bytes to a *temp* file next
/// to the real one — `{label}__{property}.bin` becomes
/// `{label}__{property}.fts.tmp` via `Path::with_extension("fts.tmp")` — then
/// renames it over the original. Pre-creating that exact temp path *as a
/// directory* makes `std::fs::write(&tmp, &bytes)` fail with "is a
/// directory": a real, permission-independent conflict, and one that
/// deliberately leaves `fts/registry.json` and the original
/// `{label}__{property}.bin` untouched.
///
/// That matters for the open-must-succeed precondition: `open()` loads the
/// pre-existing, still-valid `{label}__{property}.bin` written by the
/// `CREATE FULLTEXT INDEX` the caller already ran, so it succeeds
/// normally — unlike `corrupt_fts_index_file`, this does not touch the file
/// `open()` reads at all, only the file `save()` is about to write.
fn make_fts_save_fail(db_root: &std::path::Path, label: &str, property: &str) {
    let bin_path = db_root.join("fts").join(format!("{label}__{property}.bin"));
    assert!(
        bin_path.is_file(),
        "expected {} to already exist as a valid file from CREATE FULLTEXT INDEX",
        bin_path.display()
    );
    let tmp_path = bin_path.with_extension("fts.tmp");
    std::fs::create_dir(&tmp_path).unwrap_or_else(|e| {
        panic!(
            "failed to pre-create {} as a directory: {e}",
            tmp_path.display()
        )
    });
}

/// The core regression: a CREATE against a corrupt, registered FTS index must
/// now fail loudly instead of silently succeeding with the text unindexed.
#[test]
fn create_fails_when_fts_index_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Register a fulltext index on (Doc, text). This writes a valid, empty
    // index file to `fts/Doc__text.bin` via `FtsIndex::create` -> `save()`.
    db.execute("CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)")
        .expect("CREATE FULLTEXT INDEX should succeed");

    corrupt_fts_index_file(dir.path(), "Doc", "text");

    // Before the fix: this returned Ok(..) and the node was created with its
    // text silently absent from the (now-broken) FTS index forever.
    let result = db.execute("CREATE (:Doc {text: 'hello searchable world'})");

    assert!(
        result.is_err(),
        "CREATE against a corrupt registered FTS index must fail, not silently \
         succeed with the text unindexed (issue #462); got {result:?}"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("FTS index") && msg.contains("Doc") && msg.contains("text"),
        "error should name the broken (label, property) pair so an operator can \
         act on it; got: {msg}"
    );

    // The CREATE is a single-statement write transaction that hits the FTS
    // block before `tx.commit()` — so the failure must abort the whole
    // statement, not leave a node persisted with a silently-skipped index
    // entry (which would just be a differently-shaped version of the same
    // bug). Nothing with label Doc should exist.
    let rows = db
        .execute("MATCH (n:Doc) RETURN n.text")
        .expect("read-only MATCH should still succeed");
    assert_eq!(
        rows.rows.len(),
        0,
        "the failed CREATE must not leave a half-written node behind"
    );
}

/// The second half of #462: `open()` succeeding and `insert()` succeeding
/// (both in-memory) are not enough — a failed `save()` used to be logged and
/// ignored, so the write still reported success while the on-disk index
/// never gained the entry. This is a distinct code path from `open()`
/// failing (asserted above): `save()` cannot be reached without `open()`
/// having already succeeded, so a fix that only covers `open()` leaves this
/// half of the bug in place.
#[test]
fn create_fails_when_fts_index_cannot_be_saved() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)")
        .expect("CREATE FULLTEXT INDEX should succeed");

    make_fts_save_fail(dir.path(), "Doc", "text");

    // Before this fix: this returned Ok(..) — open() took the "no file yet"
    // branch (Ok, empty index), insert() succeeded in memory, save() failed
    // and was only warn!-logged, so the CREATE still reported success with
    // the text never reaching disk.
    let result = db.execute("CREATE (:Doc {text: 'hello searchable world'})");

    assert!(
        result.is_err(),
        "CREATE must fail when the FTS index cannot be saved to disk, not \
         silently succeed with the text unindexed (issue #462); got {result:?}"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("FTS index") && msg.contains("saved"),
        "error should name the failure as a save failure (distinct from an \
         open failure) so an operator can tell which half of the write broke; \
         got: {msg}"
    );

    // Same atomicity expectation as the open-failure case: the FTS block
    // runs before `tx.commit()`, so nothing should be persisted.
    let rows = db
        .execute("MATCH (n:Doc) RETURN n.text")
        .expect("read-only MATCH should still succeed");
    assert_eq!(
        rows.rows.len(),
        0,
        "the failed CREATE must not leave a half-written node behind"
    );
}

/// A healthy (non-corrupt, non-registered-index) CREATE is completely
/// unaffected: this fix must not turn ordinary writes into failures.
#[test]
fn create_still_succeeds_without_fts_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)")
        .expect("CREATE FULLTEXT INDEX should succeed");
    db.execute("CREATE (:Doc {text: 'hello searchable world'})")
        .expect("CREATE against a healthy FTS index must still succeed");

    let rows = db
        .execute("MATCH (n:Doc) WHERE full_text_search('Doc', 'text', 'searchable') RETURN n.text")
        .expect("full_text_search should succeed");
    assert_eq!(
        rows.rows.len(),
        1,
        "the node must be indexed and findable when the FTS index is healthy"
    );
}

/// Read-side companion to the write fix: `full_text_search()` and
/// `bm25_score()` must not report a corrupt index the same way they report a
/// genuine non-match. Before this fix, `ReadSnapshot::fts_index` returned
/// `None` for both "no index configured" (correctly => false/0.0) and "index
/// present but broken" (incorrectly => the same false/0.0), so a corrupt
/// index was indistinguishable from an empty search result.
#[test]
fn full_text_search_and_bm25_score_return_null_on_corrupt_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Build one healthy, indexed node first so the label/property exist and
    // a query has something to (fail to) find.
    db.execute("CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)")
        .expect("CREATE FULLTEXT INDEX should succeed");
    db.execute("CREATE (:Doc {text: 'hello searchable world'})")
        .expect("CREATE against a healthy FTS index must succeed");

    corrupt_fts_index_file(dir.path(), "Doc", "text");

    // Function calls are only routed through the graph-aware evaluator that
    // dispatches to `eval_full_text_search`/`eval_bm25_score`
    // (`Engine::eval_expr_graph`, see `crates/sparrowdb-execution/src/engine/
    // expr.rs`) from a WHERE clause or a WITH projection — a bare `MATCH ...
    // RETURN full_text_search(...)` with no WHERE/WITH takes a different,
    // plain-expression path that does not know this function and silently
    // returns NULL for an unrelated reason (unknown function name). WITH is
    // used here so the assertion actually exercises the fixed code, matching
    // the call shape `fts_index.rs`'s existing tests already rely on.
    let rows = db
        .execute(
            "MATCH (n:Doc) WITH full_text_search('Doc', 'text', 'searchable') AS hit RETURN hit",
        )
        .expect("read-only MATCH must not fail even though the index is broken");
    assert_eq!(
        rows.rows.len(),
        1,
        "the node itself still exists and is scanned"
    );
    let hit = &rows.rows[0][0];
    assert!(
        matches!(hit, Value::Null),
        "full_text_search() against a corrupt index must return NULL, not `false` \
         (which is indistinguishable from a genuine non-match); got {hit:?}"
    );

    let rows = db
        .execute("MATCH (n:Doc) WITH bm25_score(n.text, 'searchable') AS score RETURN score")
        .expect("read-only MATCH must not fail even though the index is broken");
    let score = &rows.rows[0][0];
    assert!(
        matches!(score, Value::Null),
        "bm25_score() against a corrupt index must return NULL, not `0.0`; got {score:?}"
    );
}
