//! Regression guard for #451 — fail-closed `open()` is one-shot.
//!
//! #441/#442/#445/#446/#447 all closed instances of the same silent-drop
//! shape on the *open* path. The residual hole #451 names is different: it
//! is not about `open()` at all, it is about what happens **after** `open()`
//! has already succeeded a second time.
//!
//! 1. An index is damaged. The **first** `open()` after that correctly
//!    refuses (`Error::Corruption`) and quarantines the bad bytes to
//!    `<path>.corrupt.<millis>` (#442).
//! 2. The **second** `open()` sees no live `.bin` for that `(label, prop)`,
//!    which is indistinguishable from "never indexed" by every signal that
//!    existed before this fix, so it succeeds — silently, with no index
//!    registered.
//! 3. Every vector write to that `(label, prop)` from that point on used to
//!    fall through to whatever the ordinary (non-vector) write path does for
//!    a value with no live index — which, depending on caller, ranged from a
//!    generic non-scalar-value rejection that never mentions quarantine to,
//!    in `addToVectorIndex`'s pre-#410 shape, an outright silent drop.
//!
//! The fix: a write against a `(label, prop)` with an unrecovered #451
//! quarantine artifact and no live index now fails loudly, naming the label,
//! the property, the quarantine file, and the recovery step
//! (`createVectorIndex`) — instead of silently doing nothing or erroring with
//! a message that gives no indication anything was ever indexed at all.
//!
//! A `(label, prop)` that was genuinely never indexed is a different, legal
//! case and must not be affected by this change; this file checks both.
//!
//! Every expected value below is derived by hand from the fixture this file
//! builds, not recorded from a run of the code — see
//! `~/Dev/SparrowDB/CLAUDE.md`.

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ── Fixture helpers (mirrors regression_456_load_is_not_destructive.rs) ──────

const DIMS: usize = 3;

/// Path of the on-disk index file for `(label, prop)` inside a db root.
fn index_file(db_root: &Path, label: &str, prop: &str) -> PathBuf {
    db_root
        .join("vector_indexes")
        .join(format!("hnsw_{label}_{prop}.bin"))
}

/// Every file in `<db_root>/vector_indexes/` whose name contains `.corrupt.`,
/// i.e. every #442 quarantine artifact.
fn quarantine_artifacts(db_root: &Path) -> Vec<PathBuf> {
    let dir = db_root.join("vector_indexes");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".corrupt."))
        })
        .collect()
}

/// Truncate `file` to its first 4 bytes — guaranteed undecodable by either the
/// v2 header (which needs >= 36 bytes) or the legacy bincode path (whose
/// 8-byte length prefix alone does not fit in 4 bytes). Identical rationale to
/// `regression_456_load_is_not_destructive.rs`'s helper of the same name.
fn truncate_to_4_bytes(file: &Path) {
    let original = std::fs::metadata(file).expect("stat index file").len();
    assert!(
        original > 4,
        "fixture precondition: a serialised VectorIndex must exceed the \
         4-byte truncation point, got {original} bytes"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(file)
        .expect("open index file for truncation")
        .set_len(4)
        .expect("truncate index file to 4 bytes");
}

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn embedding_param() -> Value {
    Value::List(vec![
        Value::Float64(1.0),
        Value::Float64(2.0),
        Value::Float64(3.0),
    ])
}

/// Build a fresh db with a damaged-then-quarantined `(Doc, embedding)` index,
/// two nodes (`key: 'x'` and `key: 'y'`), and hand back the db handle from
/// the *second* `open()` — the "clean-looking but silently dropping" state
/// #451 is about. Also hands back the (single) quarantine artifact path.
fn make_quarantined_db() -> (tempfile::TempDir, GraphDb, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    {
        let db = GraphDb::open(&root).expect("open fresh db");
        db.create_vector_index("Doc", "embedding", DIMS, "cosine")
            .expect("create Doc.embedding index");
        db.execute("CREATE (:Doc {key: 'x'})").expect("create x");
        db.execute("CREATE (:Doc {key: 'y'})").expect("create y");
    }

    let file = index_file(&root, "Doc", "embedding");
    truncate_to_4_bytes(&file);

    // First open: refuses, quarantines.
    let first = GraphDb::open(&root);
    assert!(
        first.is_err(),
        "fixture precondition: open() must refuse while the damaged file is live"
    );
    let artifacts = quarantine_artifacts(&root);
    assert_eq!(
        artifacts.len(),
        1,
        "fixture precondition: exactly one artifact after the first open, got {artifacts:?}"
    );
    let artifact = artifacts[0].clone();

    // Second open: the #451 state — succeeds, no index registered.
    let db = GraphDb::open(&root).expect("fixture precondition: the second open must succeed");
    assert!(
        db.get_vector_index("Doc", "embedding").is_none(),
        "fixture precondition: no live index is registered after the second open"
    );

    (dir, db, artifact)
}

// ── 1. The write-refusal itself, across every $param write path ─────────────

/// `MATCH ... SET n.embedding = $emb` against the quarantined pair must fail
/// loudly, naming the label, the property, the artifact, and the recovery
/// step — not silently do nothing and not error with a message that gives no
/// indication the property was ever indexed.
#[test]
fn match_set_on_quarantined_pair_is_refused_with_actionable_error() {
    let (_dir, db, artifact) = make_quarantined_db();

    let err = db
        .execute_with_params(
            "MATCH (n:Doc {key: 'x'}) SET n.embedding = $emb",
            params(&[("emb", embedding_param())]),
        )
        .expect_err("a vector write against a quarantined, unrecovered pair must be refused");
    let msg = err.to_string();

    for token in ["Doc", "embedding", "createVectorIndex", "451"] {
        assert!(
            msg.contains(token),
            "error must be actionable — missing `{token}`; got: {msg}"
        );
    }
    let artifact_name = artifact
        .file_name()
        .and_then(|n| n.to_str())
        .expect("artifact must have a utf8 file name");
    assert!(
        msg.contains(artifact_name),
        "error must name the exact quarantine file ({artifact_name}); got: {msg}"
    );

    // The write must not have landed as an ordinary property either — this is
    // a refusal, not a silent downgrade to plain-property storage.
    let rows = db
        .execute("MATCH (n:Doc {key: 'x'}) RETURN n.embedding")
        .expect("read back")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        sparrowdb_execution::Value::Null,
        "the refused write must leave no trace on the property either"
    );
}

/// Same guard, reached through `MERGE ... ON CREATE SET`.
#[test]
fn merge_on_create_set_on_quarantined_pair_is_refused() {
    let (_dir, db, _artifact) = make_quarantined_db();

    let err = db
        .execute_with_params(
            "MERGE (n:Doc {key: 'new'}) ON CREATE SET n.embedding = $emb",
            params(&[("emb", embedding_param())]),
        )
        .expect_err("MERGE...ON CREATE SET against a quarantined pair must be refused");
    let msg = err.to_string();
    for token in ["Doc", "embedding", "createVectorIndex"] {
        assert!(msg.contains(token), "missing `{token}`; got: {msg}");
    }
}

/// Same guard, reached through `UNWIND ... MATCH ... SET` (the second entry
/// point #410's fix had to cover independently — see db.rs's
/// `execute_unwind_mutate_inner`).
#[test]
fn unwind_match_set_on_quarantined_pair_is_refused() {
    let (_dir, db, _artifact) = make_quarantined_db();

    let rows = vec![sparrowdb_execution::Value::Map(vec![(
        "key".to_string(),
        sparrowdb_execution::Value::String("x".to_string()),
    )])];
    let err = db
        .execute_with_params(
            "UNWIND $rows AS row MATCH (n:Doc {key: row.key}) SET n.embedding = $emb",
            params(&[("rows", Value::List(rows)), ("emb", embedding_param())]),
        )
        .expect_err("UNWIND...MATCH...SET against a quarantined pair must be refused");
    let msg = err.to_string();
    for token in ["Doc", "embedding", "createVectorIndex"] {
        assert!(msg.contains(token), "missing `{token}`; got: {msg}");
    }
}

// ── 2. A never-indexed pair is a different, legal case — unaffected ─────────

/// `(Doc, other)` was never indexed at all — no `.bin`, no quarantine
/// artifact. Writing a vector-shaped value to it hits the ordinary,
/// unrelated non-scalar-value rejection, unchanged by this fix, and that
/// error must NOT claim anything about quarantine, since nothing was ever
/// indexed there.
#[test]
fn never_indexed_pair_is_unaffected_and_does_not_mention_quarantine() {
    let (_dir, db, _artifact) = make_quarantined_db();

    let err = db
        .execute_with_params(
            "MATCH (n:Doc {key: 'x'}) SET n.other = $emb",
            params(&[("emb", embedding_param())]),
        )
        .expect_err("a non-scalar value still has nowhere to go without an index");
    let msg = err.to_string();
    assert!(
        !msg.to_lowercase().contains("quarantine") && !msg.contains("451"),
        "a pair that was never indexed must not be blamed on a quarantine \
         that never happened for it; got: {msg}"
    );
}

/// A second label sharing a property *name* with an indexed pair on a
/// *different* label must not be silently treated as indexed.
///
/// This is not the quarantine scenario at all — it is a bug the refactor
/// needed to introduce a `label` parameter incidentally fixed: routing used
/// to key off `prop` name alone (`vector_indexes.keys().any(|(_, p)| p ==
/// prop)`), ignoring which label the index actually belongs to. With
/// `(Doc, embedding)` live-indexed and `(Note, embedding)` never indexed at
/// all, writing a vector to `Note.embedding` used to be routed as "skip the
/// property write, an index will handle it" — and then the HNSW write-path
/// block looked up `(Note, embedding)`, found nothing, and did nothing.
/// Net effect: no property written, no index entry, no error — a true
/// silent drop, just not the one #451 is named for. Hand-derivation of the
/// pre-fix result: run this test against the parent commit (`git stash` the
/// four source-file changes, keep this test) and the assertions below fail
/// because no `Err` is returned at all.
#[test]
fn cross_label_prop_name_collision_does_not_silently_drop_the_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = GraphDb::open(root).expect("open fresh db");
    db.create_vector_index("Doc", "embedding", DIMS, "cosine")
        .expect("create Doc.embedding index");
    // Note never gets an index on "embedding" at all.
    db.execute("CREATE (:Note {key: 'n1'})")
        .expect("create Note node");

    let err = db
        .execute_with_params(
            "MATCH (n:Note {key: 'n1'}) SET n.embedding = $emb",
            params(&[("emb", embedding_param())]),
        )
        .expect_err(
            "a vector written to a label with no index of its own must not be silently \
             swallowed just because a DIFFERENT label happens to index a property with \
             the same name",
        );
    let msg = err.to_string();
    assert!(
        !msg.to_lowercase().contains("quarantine"),
        "Note.embedding was never quarantined; got: {msg}"
    );

    // Nothing landed anywhere: not as a property, not in Doc's index (wrong
    // pair entirely), and no Note index was implicitly created.
    let rows = db
        .execute("MATCH (n:Note {key: 'n1'}) RETURN n.embedding")
        .expect("read back")
        .rows;
    assert_eq!(
        rows[0][0],
        sparrowdb_execution::Value::Null,
        "the refused write must leave no property behind"
    );
    assert!(
        db.get_vector_index("Note", "embedding").is_none(),
        "no index must have been implicitly created for Note"
    );

    // The unrelated Doc.embedding write path must still work — this fix must
    // not have broken the case it was meant to leave untouched.
    db.execute("CREATE (:Doc {key: 'd1'})")
        .expect("create Doc node");
    db.execute_with_params(
        "MATCH (n:Doc {key: 'd1'}) SET n.embedding = $emb",
        params(&[("emb", embedding_param())]),
    )
    .expect("Doc.embedding is genuinely indexed and must still accept the write");
}

// ── 3. Recovery: create_vector_index clears the guard, health goes clean ────

#[test]
fn creating_a_fresh_index_clears_the_guard_and_health_goes_clean() {
    let (dir, db, _artifact) = make_quarantined_db();
    let root = dir.path();

    // Blocked before remediation (already covered above; re-confirm inline
    // so this test is self-contained about the state it starts from).
    assert!(
        db.execute_with_params(
            "MATCH (n:Doc {key: 'x'}) SET n.embedding = $emb",
            params(&[("emb", embedding_param())]),
        )
        .is_err(),
        "fixture precondition: write must be blocked before remediation"
    );

    // Static health: the pair must be `active` (unrecovered) before
    // remediation.
    let before = GraphDb::vector_index_health(root);
    assert_eq!(
        before
            .active
            .iter()
            .map(|f| (f.label.as_str(), f.prop.as_str()))
            .collect::<Vec<_>>(),
        vec![("Doc", "embedding")],
        "fixture precondition: the pair must be active before remediation, got {:?}",
        before.active
    );
    assert!(before.historical.is_empty());

    // Remediate.
    db.create_vector_index("Doc", "embedding", DIMS, "cosine")
        .expect("create a fresh index for the quarantined pair");

    // The write must now succeed.
    db.execute_with_params(
        "MATCH (n:Doc {key: 'x'}) SET n.embedding = $emb",
        params(&[("emb", embedding_param())]),
    )
    .expect("a fresh live index must accept the write");

    // And it must have actually landed in the new index, not been silently
    // dropped a second time under a different disguise.
    let idx = db
        .get_vector_index("Doc", "embedding")
        .expect("a live index must be registered now");
    let node_id = match &db
        .execute("MATCH (n:Doc {key: 'x'}) RETURN id(n) AS nid")
        .expect("id query")
        .rows[0][0]
    {
        sparrowdb_execution::Value::Int64(n) => *n as u64,
        other => panic!("expected Int64 node id, got {other:?}"),
    };
    assert!(
        idx.read().expect("read lock").has_vector(node_id),
        "the vector must actually be present in the newly created index"
    );

    // Static health must go clean: the artifact is now superseded
    // (`historical`), not `active` — this is the "must clear after
    // remediation" half of the diagnostic-design rule this repo holds
    // itself to.
    let after = GraphDb::vector_index_health(root);
    assert!(
        after.active.is_empty(),
        "the pair must no longer be active once a live index supersedes the \
         artifact, got {:?}",
        after.active
    );
    assert_eq!(
        after
            .historical
            .iter()
            .map(|f| (f.label.as_str(), f.prop.as_str()))
            .collect::<Vec<_>>(),
        vec![("Doc", "embedding")],
        "the artifact must still be visible as historical forensic evidence, \
         got {:?}",
        after.historical
    );

    // The in-memory write-guard itself must have cleared too, not merely be
    // masked by the now-live index winning the first check in
    // `resolve_vector_write_route`. Prove it by dropping the index again —
    // an explicit, deliberate operator action distinct from quarantine — and
    // confirming the write now hits the *ordinary* unindexed-write path
    // (case (a), unaffected by this fix) rather than a stale claim that this
    // pair is still quarantined.
    db.drop_vector_index("Doc", "embedding")
        .expect("drop the index again");
    let err_after_drop = db
        .execute_with_params(
            "MATCH (n:Doc {key: 'y'}) SET n.embedding = $emb",
            params(&[("emb", embedding_param())]),
        )
        .expect_err("still no scalar representation for a vector without any index");
    let msg = err_after_drop.to_string();
    assert!(
        !msg.to_lowercase().contains("quarantine") && !msg.contains("451"),
        "after create_vector_index + drop_vector_index, the pair must not be \
         reported as quarantined — that state was cleared by create_vector_index \
         and must not resurrect itself; got: {msg}"
    );
}
