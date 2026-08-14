//! `create_vector_index` documents: "If an index already exists for this
//! (label, prop) pair, this is a no-op."
//!
//! The `contains_key` guard cannot see an index a *different* handle created —
//! a handle opened before the index existed has an empty map. It therefore fell
//! through to `idx.save()` with a fresh empty index, which the #441/#442
//! generation guard correctly refused, surfacing a hard error in the one case
//! where the function's precondition is most certainly true.
//!
//! A generation conflict is proof the index exists, so the contract says no-op.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;

fn populated_index(dir: &std::path::Path) {
    let b = GraphDb::open(dir).unwrap();
    b.create_vector_index("M", "emb", 4, "cosine").unwrap();
    b.execute("CREATE (n:M {id: 'k1'})").unwrap();
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::String("k1".into()));
    p.insert("emb".to_string(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]));
    b.execute_with_params("MATCH (n:M {id: $id}) SET n.emb = $emb", p)
        .unwrap();
}

fn index_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir.join("vector_indexes"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

#[test]
fn create_vector_index_is_a_noop_for_an_index_another_handle_owns() {
    let d = tempfile::tempdir().unwrap();

    // `a` opens BEFORE any index exists, so its in-memory map stays empty.
    let a = GraphDb::open(d.path()).unwrap();
    populated_index(d.path());

    let before = index_bytes(d.path());
    assert!(before > 0, "fixture must have written an index file");

    // The documented no-op, across handles.
    a.create_vector_index("M", "emb", 4, "cosine")
        .expect("must be a no-op, not an error: the index demonstrably exists");

    assert_eq!(
        index_bytes(d.path()),
        before,
        "the populated index file must be byte-identical — a no-op writes nothing"
    );

    // The load-bearing assertion. A careless fix "restores" the no-op by
    // inserting a fresh empty index into this handle's map, which discards the
    // other writer's vectors in memory — the data loss the generation guard
    // just prevented on disk. The stale handle must gain nothing.
    assert!(
        a.get_vector_index("M", "emb").is_none(),
        "the stale handle must NOT insert an empty index into its own map; \
         reopen is what picks up the real one"
    );

    // Ground truth: the other writer's vector survives a fresh open.
    let c = GraphDb::open(d.path()).unwrap();
    let hits = c
        .get_vector_index("M", "emb")
        .expect("reopen must load the real index")
        .read()
        .unwrap()
        .search(&[1.0, 0.0, 0.0, 0.0], 5, 20)
        .len();
    assert_eq!(hits, 1, "the populated vector must survive");
}

/// The same-handle path must keep working: a genuine duplicate call is still a
/// no-op via `contains_key`, without reaching the save at all.
#[test]
fn create_vector_index_twice_on_one_handle_is_still_a_noop() {
    let d = tempfile::tempdir().unwrap();
    let db = GraphDb::open(d.path()).unwrap();
    db.create_vector_index("M", "emb", 4, "cosine").unwrap();
    let before = index_bytes(d.path());

    db.create_vector_index("M", "emb", 4, "cosine")
        .expect("duplicate create on one handle is a no-op");

    assert_eq!(
        index_bytes(d.path()),
        before,
        "no rewrite on the no-op path"
    );
    assert!(
        db.get_vector_index("M", "emb").is_some(),
        "the handle that created it must still hold it"
    );
}

/// A non-lost-update I/O failure must still surface. Making the target a
/// directory produces a real write error that is not a generation conflict.
#[test]
fn create_vector_index_still_reports_real_io_errors() {
    let d = tempfile::tempdir().unwrap();
    let db = GraphDb::open(d.path()).unwrap();
    let vdir = d.path().join("vector_indexes");
    std::fs::create_dir_all(&vdir).unwrap();
    // Occupy the exact file path with a directory.
    std::fs::create_dir_all(vdir.join("hnsw_M_emb.bin")).unwrap();

    let r = db.create_vector_index("M", "emb", 4, "cosine");
    assert!(
        r.is_err(),
        "a genuine I/O failure must not be swallowed as a no-op; got {r:?}"
    );
}
