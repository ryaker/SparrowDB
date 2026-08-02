//! Regression tests: a damaged vector index file must never be indistinguishable
//! from an absent one (see the issue filed alongside this test; relates to #441
//! and #442).
//!
//! The defect: `load_vector_indexes` did
//!
//! ```ignore
//! if let Ok(Some(idx)) = VectorIndex::load(&dir, label, prop) { map.insert(..) }
//! ```
//!
//! so every load error was discarded and the `(label, prop)` key simply never
//! appeared in the map.  Downstream, a missing key means "no index configured",
//! which makes the insert path drop every vector write for that pair and makes
//! every `vectorSearch` return nothing, while the engine reports "no vector
//! index; call createVectorIndex first" — a data-loss condition wearing the
//! costume of a configuration mistake.
//!
//! Policy asserted here: **fail-closed**.  `GraphDb::open` returns
//! `Error::Corruption` when an index file is present but unloadable, and
//! `GraphDb::vector_index_load_failures` names the offending files without
//! failing, so the condition is diagnosable on a database that will not open.
//!
//! Every expected value below is derived by hand from the on-disk format; none
//! was captured from program output.

use sparrowdb::{Error, GraphDb};
use std::path::{Path, PathBuf};

/// Build a database at `dir` holding one persisted 3-dimensional cosine index
/// on `(Memory, embedding)`, then close it.  Returns the path of the index file.
///
/// Hand-derived path: `VectorIndex::index_path` formats
/// `hnsw_{label}_{prop}.bin` under `<db_root>/vector_indexes/`, so with
/// label `Memory` and prop `embedding` the file is
/// `<db_root>/vector_indexes/hnsw_Memory_embedding.bin`.
fn make_db_with_persisted_index(dir: &Path) -> PathBuf {
    let db = GraphDb::open(dir).expect("open fresh db");
    db.create_vector_index("Memory", "embedding", 3, "cosine")
        .expect("create vector index");
    drop(db);

    let file = dir.join("vector_indexes").join("hnsw_Memory_embedding.bin");
    assert!(
        file.is_file(),
        "fixture precondition: create_vector_index must persist {}",
        file.display()
    );
    file
}

/// Damage `file` so that it cannot possibly deserialize.
///
/// Hand-derivation: the file is `bincode::serialize(&VectorIndex)` and the first
/// field of `VectorIndex` is `nodes: Vec<Node>`, which bincode encodes as an
/// 8-byte little-endian length prefix followed by the elements.  Truncating the
/// file to 4 bytes leaves the *length prefix itself* incomplete, so the very
/// first read hits end-of-input.  There is no byte string of length 4 that
/// bincode can decode into a `VectorIndex`, so `load` must return `Err` — this
/// does not depend on which 4 bytes survive, on the field values, or on any
/// header/CRC scheme layered on top later (#442 would reject it too).
fn truncate_to_4_bytes(file: &Path) {
    let original = std::fs::metadata(file).expect("stat index file").len();
    assert!(
        original > 4,
        "fixture precondition: a serialized VectorIndex must be longer than the \
         4-byte truncation point, got {original} bytes"
    );
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(file)
        .expect("open index file for truncation");
    f.set_len(4).expect("truncate index file to 4 bytes");
}

// ── 1. The failure is surfaced, not swallowed ─────────────────────────────────

#[test]
fn damaged_index_file_makes_open_fail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = make_db_with_persisted_index(dir.path());
    truncate_to_4_bytes(&file);

    // Expected by hand: the file exists and cannot deserialize, therefore the
    // loader must report corruption rather than pretend no index is configured.
    let err = match GraphDb::open(dir.path()) {
        Ok(_) => panic!(
            "open() must fail when {} exists but cannot be loaded; succeeding here is \
             exactly the swallow that turns one damaged file into silently dropped \
             vector writes",
            file.display()
        ),
        Err(e) => e,
    };

    match &err {
        Error::Corruption(msg) => {
            // The message must name the index, or an operator cannot act on it.
            assert!(
                msg.contains("Memory") && msg.contains("embedding"),
                "corruption message must name the (label, prop) pair, got: {msg}"
            );
        }
        other => panic!("expected Error::Corruption, got {other:?}"),
    }
}

// ── 2. Absent and unloadable are distinguishable ──────────────────────────────

#[test]
fn absent_index_and_unloadable_index_are_distinguishable() {
    // Case A — no index file at all.  Hand-derived expectation: a fresh database
    // has no `vector_indexes/` directory, so the map is empty, `open` succeeds
    // and `get_vector_index` yields `None` meaning "no index configured".
    let absent_dir = tempfile::tempdir().expect("tempdir");
    let absent_db = GraphDb::open(absent_dir.path()).expect("absent case must open cleanly");
    assert!(
        absent_db.get_vector_index("Memory", "embedding").is_none(),
        "absent case: no index was ever created, so there must be no handle"
    );
    assert!(
        GraphDb::vector_index_load_failures(absent_dir.path()).is_empty(),
        "absent case: nothing on disk can have failed to load"
    );
    drop(absent_db);

    // Case B — the index file exists and is damaged.
    let damaged_dir = tempfile::tempdir().expect("tempdir");
    let file = make_db_with_persisted_index(damaged_dir.path());
    truncate_to_4_bytes(&file);

    let damaged_result = GraphDb::open(damaged_dir.path());
    assert!(
        damaged_result.is_err(),
        "damaged case must not produce the same observable as the absent case"
    );

    // The bug being guarded is precisely that these two produced the *same*
    // observable: open() -> Ok, get_vector_index() -> None.
    let failures = GraphDb::vector_index_load_failures(damaged_dir.path());
    // Hand-derived: exactly one file was planted, for exactly one (label, prop).
    assert_eq!(
        failures.len(),
        1,
        "expected exactly 1 unloadable index, got {failures:?}"
    );
    assert_eq!(
        (failures[0].0.as_str(), failures[0].1.as_str()),
        ("Memory", "embedding"),
        "the reported failure must identify the (label, prop) pair"
    );
}

// ── 3. Happy path is unchanged ────────────────────────────────────────────────

#[test]
fn valid_index_still_loads_across_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path();

    {
        let db = GraphDb::open(path).expect("open fresh db");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create vector index");
        let arc = db.get_vector_index("Memory", "embedding").expect("handle");
        arc.write()
            .expect("write lock")
            .insert(7, &[1.0_f32, 0.0, 0.0]);
        arc.read()
            .expect("read lock")
            .save(&path.join("vector_indexes"), "Memory", "embedding")
            .expect("persist index");
    }

    let db = GraphDb::open(path).expect("re-open with a valid index must succeed");
    let arc = db
        .get_vector_index("Memory", "embedding")
        .expect("valid index must survive restart");
    let guard = arc.read().expect("read lock");

    // Hand-derived: one vector was inserted, node id 7 = [1,0,0].  A k=1 cosine
    // search for [1,0,0] can only return that node, and cos([1,0,0],[1,0,0]) =
    // (1*1+0*0+0*0) / (1*1) = 1.0 exactly — the components are exactly
    // representable in f32 and the norms are exactly 1, so no rounding occurs.
    assert_eq!(guard.len(), 1, "exactly one vector was inserted");
    let hits = guard.search(&[1.0_f32, 0.0, 0.0], 1, 16);
    assert_eq!(hits.len(), 1, "k=1 over a 1-element index returns 1 hit");
    assert_eq!(hits[0].0, 7, "the only node id in the index is 7");
    assert_eq!(
        hits[0].1, 1.0_f32,
        "cosine of a unit vector with itself is 1.0"
    );
}

#[test]
fn absent_index_is_not_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Hand-derived: nothing has created a vector index, so `vector_indexes/`
    // does not exist and there is nothing to load or to fail on.
    assert!(
        !dir.path().join("vector_indexes").exists(),
        "fixture precondition: a fresh db has no vector_indexes directory"
    );
    let db = GraphDb::open(dir.path()).expect("a db with no vector index must open");
    assert!(db.get_vector_index("Memory", "embedding").is_none());
    assert!(GraphDb::vector_index_load_failures(dir.path()).is_empty());
}

// ── 4. Interop with the #442 quarantine naming ────────────────────────────────

#[test]
fn quarantined_corrupt_file_is_not_treated_as_a_live_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let vidx = dir.path().join("vector_indexes");
    std::fs::create_dir_all(&vidx).expect("create vector_indexes dir");

    // PR #442 renames a rejected index to `<path>.corrupt.<millis>`.  Such a
    // file is deliberately no longer a live index: it must neither be loaded
    // nor block `open`.  Hand-derived: the file name's extension is `1712345678901`,
    // not `bin`, so it is not a candidate index file at all.
    std::fs::write(
        vidx.join("hnsw_Memory_embedding.bin.corrupt.1712345678901"),
        [0xFF_u8; 32],
    )
    .expect("plant quarantined file");

    let db = GraphDb::open(dir.path())
        .expect("a quarantined artifact must not prevent the database from opening");
    assert!(
        db.get_vector_index("Memory", "embedding").is_none(),
        "a quarantined file is not a live index, so no handle must be produced"
    );
    assert!(
        GraphDb::vector_index_load_failures(dir.path()).is_empty(),
        "a quarantined file is not a pending failure — it has already been dealt with"
    );
}
