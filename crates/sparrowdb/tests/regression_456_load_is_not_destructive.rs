//! Regression guards for #456: `VectorIndex::load` must not modify the store.
//!
//! #442 moved the quarantine rename *inside* `VectorIndex::load`, so every
//! caller inherited a destructive read.  Three non-test callers existed on
//! `main`; only one of them — the open path — was entitled to mutate:
//!
//! | caller | may mutate? |
//! |---|---|
//! | `helpers::load_vector_indexes` (open) | yes, intended |
//! | `helpers::vector_index_load_failures` (diagnostic) | **no** |
//! | `engine/expr.rs` `hybrid_search` cached loader (query) | **no** |
//!
//! The observable defect: a `hybrid_search` **read** against an index damaged
//! after `open` returned `Ok` with zero hits — no error, no log line — *and*
//! renamed the last live copy of the index aside as a side effect.  A read
//! query returned success, returned nothing, and destroyed data.
//!
//! The fix splits `load` (non-destructive, returns the error) from
//! `load_and_quarantine` (renames on failure, open path only), which repairs
//! the query path and the diagnostic in one change.
//!
//! Also guarded here, from the same review:
//!
//! * N damaged indexes must not require N failed restarts — one `open` reports
//!   all of them.
//! * A stale quarantine artifact must not keep a rebuilt `(label, prop)` red
//!   forever; it moves to `historical`, where an operator can still see it and
//!   a monitor can ignore it.
//! * A dangling symlink must not classify as "no index configured" — the
//!   present-but-treated-as-absent case #445 exists to kill.
//! * A `vector_indexes/` directory that cannot be listed must not report the
//!   same thing a healthy store reports.  A monitor must be able to tell green
//!   from blind.
//! * The poll-frequency tier must not read index contents: the deep tier fully
//!   deserialises every healthy index on every call, which is a permanent cost
//!   in the common case, not an edge case.
//!
//! Every expected value below is derived by hand from the fixture the test
//! itself builds.  Nothing here is a recording of what the code returns.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use sparrowdb_storage::vector_index::VectorIndex;
use std::path::{Path, PathBuf};

// ── Fixture helpers ───────────────────────────────────────────────────────────

/// Path of the on-disk index file for `(label, prop)` inside a db root.
///
/// Hand-derived: `VectorIndex::index_path` (private) formats
/// `hnsw_{label}_{prop}.bin` under `<db_root>/vector_indexes/`.
fn index_file(db_root: &Path, label: &str, prop: &str) -> PathBuf {
    db_root
        .join("vector_indexes")
        .join(format!("hnsw_{label}_{prop}.bin"))
}

/// Every file in `<db_root>/vector_indexes/` whose name contains `.corrupt.`,
/// i.e. every #442 quarantine artifact.  Empty when the directory is absent.
fn quarantine_artifacts(db_root: &Path) -> Vec<PathBuf> {
    let dir = db_root.join("vector_indexes");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut found: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".corrupt."))
        })
        .collect();
    found.sort();
    found
}

/// Truncate `file` to its first 4 bytes.
///
/// Hand-derivation of why this is guaranteed undecodable, independent of which
/// 4 bytes survive:
///
/// * The v2 path requires `len >= 36` (8-byte magic, `version:u32`,
///   `reserved:u32`, `payload_len:u64`, `generation:u64`, `crc32c:u32`).  4 < 36,
///   so the v2 decoder is never even reached.
/// * The legacy path hands the bytes to `bincode`, whose first act is to read
///   the 8-byte little-endian length prefix of `nodes: Vec<HnswNode>`.  Only 4
///   bytes exist, so it hits end-of-input immediately.
///
/// There is therefore no 4-byte string that decodes into a `VectorIndex`.
fn truncate_to_4_bytes(file: &Path) {
    let original = std::fs::metadata(file).expect("stat index file").len();
    assert!(
        original > 4,
        "fixture precondition: a serialised VectorIndex must exceed the 4-byte \
         truncation point, got {original} bytes"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(file)
        .expect("open index file for truncation")
        .set_len(4)
        .expect("truncate index file to 4 bytes");
}

// ── 1. HEADLINE: a read query neither destroys nor lies ───────────────────────

/// `hybrid_search` against an index damaged *after* `open` must not rename the
/// file, and must not answer "Ok, zero hits".
///
/// The damage is applied without closing the database, exactly as reported, so
/// open-time validation never sees it and the only code that touches the file
/// is the query path's own disk read.
#[test]
fn hybrid_search_on_an_index_damaged_after_open_neither_renames_nor_reports_zero_hits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = GraphDb::open(root).expect("open fresh db");

    // Fixture: one Doc node, one 3-dimensional cosine vector, no FTS index.
    db.create_vector_index("Doc", "embedding", 3, "cosine")
        .expect("create vector index");
    db.execute("CREATE (d:Doc {dockey: 'only', content: 'alpha'})")
        .expect("create node");
    let node_id = match &db
        .execute("MATCH (n:Doc) WHERE n.dockey = 'only' RETURN id(n) AS nid")
        .expect("id query")
        .rows[0][0]
    {
        Value::Int64(n) => *n as u64,
        other => panic!("expected Int64 node id, got {other:?}"),
    };

    let arc = db.get_vector_index("Doc", "embedding").expect("handle");
    arc.write()
        .expect("write lock")
        .insert(node_id, &[1.0_f32, 0.0, 0.0]);
    arc.read()
        .expect("read lock")
        .save(&root.join("vector_indexes"), "Doc", "embedding")
        .expect("persist index");

    const QUERY: &str = "RETURN hybrid_search('Doc', 'embedding', 'content', \
                         [1.0, 0.0, 0.0], 'alpha', 1) AS hits";

    // ── Baseline, hand-derived ──
    // Vector side: the index holds exactly one vector, so a k*2 = 2 search over
    // a 1-element index returns exactly that one node id.
    // Text side: no FTS index was ever created, so `FtsIndex::open` fails and
    // the text list is empty.
    // RRF over (one 1-element list, one empty list) yields one fused entry;
    // `truncate(k = 1)` leaves it at one.  So: exactly one hit, and it is
    // `node_id`.
    let baseline = db.execute(QUERY).expect("baseline hybrid_search executes");
    let baseline_ids = hit_ids(&baseline.rows[0][0]);
    assert_eq!(
        baseline_ids,
        vec![node_id],
        "fixture precondition: the healthy index must return exactly the one \
         node that is in it"
    );

    // ── Damage the file underneath the open database ──
    let file = index_file(root, "Doc", "embedding");
    let healthy_len = std::fs::metadata(&file).expect("stat").len();
    truncate_to_4_bytes(&file);
    let damaged_bytes = std::fs::read(&file).expect("read damaged file");
    assert_eq!(
        damaged_bytes.len(),
        4,
        "fixture precondition: the file must now be exactly 4 bytes (was {healthy_len})"
    );

    // ── The query under test ──
    //
    // Hand-derived expectation: the cached copy from the baseline query cannot
    // be reused, and this is exact rather than probabilistic.  The cache is
    // validated against `VectorIndex::fingerprint`.  For the healthy v2 file
    // that is `(generation, crc32c as u64)`, whose second element is at most
    // `u32::MAX` = 4_294_967_295.  For the 4-byte file the header no longer
    // fits, so `fingerprint` falls back to `(file_len, mtime_nanos)` =
    // `(4, nanoseconds since 1970)`, whose second element is on the order of
    // 1.7e18 — greater than `u32::MAX` for every instant after 1970-01-01
    // 00:00:04.295.  The two tuples can never compare equal, so the loader is
    // forced back to disk.
    let result = db.execute(QUERY).expect("the query itself still executes");

    // (a) The file must still be there, byte-for-byte as we left it.
    assert!(
        file.is_file(),
        "a READ QUERY renamed {} away — this is the data destruction in #456",
        file.display()
    );
    assert_eq!(
        std::fs::read(&file).expect("re-read the damaged file"),
        damaged_bytes,
        "the damaged bytes are the only surviving copy of the vectors; a read \
         query must leave them exactly as it found them"
    );
    assert!(
        quarantine_artifacts(root).is_empty(),
        "a read query must not create a quarantine artifact, found {:?}",
        quarantine_artifacts(root)
    );

    // (b) The caller must not be told "Ok, zero hits".
    let value = &result.rows[0][0];
    assert!(
        !matches!(value, Value::List(_)),
        "hybrid_search returned a result list ({value:?}) for an index it could \
         not read; a caller cannot distinguish that from 'nothing matched'"
    );
    assert!(
        matches!(value, Value::Null),
        "expected Value::Null — this function's existing failure signal — got {value:?}"
    );
}

/// Extract the `node_id` of every hit in a `hybrid_search` return value.
fn hit_ids(value: &Value) -> Vec<u64> {
    match value {
        Value::List(items) => items
            .iter()
            .filter_map(|item| match item {
                Value::Map(kvs) => {
                    kvs.iter()
                        .find(|(k, _)| k == "node_id")
                        .and_then(|(_, v)| match v {
                            Value::Int64(n) => Some(*n as u64),
                            _ => None,
                        })
                }
                _ => None,
            })
            .collect(),
        other => panic!("expected a hit list, got {other:?}"),
    }
}

// ── 2. One open reports every damaged index, not just the first ───────────────

/// Two damaged indexes must be named by a single failed `open`.
///
/// `load_vector_indexes` used to `return` on the first `Err`, so exactly one bad
/// file was probed — and quarantined — per `open()`.  With two planted that cost
/// three restarts (Err, Err, then Ok with *both* pairs silently reported as "no
/// index configured"), and each failed start named only one of them.
#[test]
fn one_open_reports_every_damaged_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    {
        let db = GraphDb::open(root).expect("open fresh db");
        db.create_vector_index("Doc", "embedding", 3, "cosine")
            .expect("create Doc index");
        db.create_vector_index("Note", "vec", 3, "cosine")
            .expect("create Note index");
    }

    // Both files are persisted by `create_vector_index`; damage both.
    let doc = index_file(root, "Doc", "embedding");
    let note = index_file(root, "Note", "vec");
    truncate_to_4_bytes(&doc);
    truncate_to_4_bytes(&note);

    let err = match GraphDb::open(root) {
        Ok(_) => panic!("open must fail while any damaged index file is live"),
        Err(e) => e.to_string(),
    };

    // Hand-derived: two files were planted, for two distinct (label, prop)
    // pairs, and both are undecodable, so one open must account for both.
    // Pre-fix this message names only `Doc`/`embedding` — the scan is sorted by
    // (label, prop) and "Doc" < "Note", so the early return fires on Doc and
    // Note is never probed.
    for token in ["Doc", "embedding", "Note", "vec"] {
        assert!(
            err.contains(token),
            "the failure must name every damaged index; `{token}` missing from: {err}"
        );
    }

    // Hand-derived: the open path quarantines what it rejects, and it rejected
    // both, so both `.bin` files are gone and exactly two artifacts exist.
    let artifacts = quarantine_artifacts(root);
    assert_eq!(
        artifacts.len(),
        2,
        "each damaged file must be quarantined by the same open that reported \
         it, got {artifacts:?}"
    );
    assert!(
        !doc.exists() && !note.exists(),
        "both live files moved aside"
    );

    // Hand-derived: with both sets of bytes out of service, the next open
    // succeeds — two restarts total, not three.
    let db = GraphDb::open(root).expect("the next open must succeed");
    assert!(db.get_vector_index("Doc", "embedding").is_none());
    assert!(db.get_vector_index("Note", "vec").is_none());
    drop(db);

    // And the damage stays visible: two artifacts, neither superseded by a live
    // index, so two failures.
    let health = GraphDb::vector_index_load_failures(root);
    assert_eq!(health.unscannable, None, "the directory is readable");
    assert!(
        health.historical.is_empty(),
        "neither pair has been rebuilt, so nothing is merely historical: {:?}",
        health.historical
    );
    assert_eq!(
        health.active.len(),
        2,
        "both quarantined pairs must remain visible, got {:?}",
        health.active
    );
    assert_eq!(
        health
            .active
            .iter()
            .map(|f| (f.label.as_str(), f.prop.as_str()))
            .collect::<Vec<_>>(),
        vec![("Doc", "embedding"), ("Note", "vec")],
        "results are sorted by (label, prop)"
    );
    assert!(!health.is_healthy(), "unrecovered damage is not healthy");
}

// ── 3. A rebuilt pair is not false-flagged by its own wreckage ────────────────

/// A leftover artifact must not keep a repaired store red forever, **and** must
/// still alarm on a pair that has no working index, **and** must remain visible
/// to an operator either way.
///
/// Quarantine artifacts are never removed automatically, so a report that keeps
/// naming a repaired pair can only ever go green -> red with no path back.  An
/// alert that never clears after the problem is fixed trains people to ignore
/// it, which is worse than no alert: the attention has been spent and nothing
/// was bought with it.
///
/// The artifact is moved to `historical` rather than suppressed, because #442
/// records no reason at quarantine time — these bytes are the only surviving
/// evidence that the incident happened at all.
#[test]
fn a_rebuilt_pair_moves_from_active_to_historical() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    {
        let db = GraphDb::open(root).expect("open fresh db");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create vector index");
        let arc = db.get_vector_index("Memory", "embedding").expect("handle");
        arc.write()
            .expect("write lock")
            .insert(7, &[1.0_f32, 0.0, 0.0]);
        arc.read()
            .expect("read lock")
            .save(&root.join("vector_indexes"), "Memory", "embedding")
            .expect("persist index");
    }

    // Wreckage from an earlier incident on the SAME pair, which the operator
    // has since rebuilt (the live file above).
    let superseded = root
        .join("vector_indexes")
        .join("hnsw_Memory_embedding.bin.corrupt.1712345678901");
    std::fs::write(&superseded, [0xFF_u8; 32]).expect("plant superseded artifact");

    // Wreckage on a pair with NO live index — real, unrecovered damage.
    let orphan = root
        .join("vector_indexes")
        .join("hnsw_Ghost_vec.bin.corrupt.1712345678902");
    std::fs::write(&orphan, [0xFF_u8; 32]).expect("plant orphan artifact");

    GraphDb::open(root)
        .expect("artifacts must never block open")
        .get_vector_index("Memory", "embedding")
        .expect("the rebuilt index must load");

    // Hand-derived: two artifacts on disk, one per pair.
    //   (Memory, embedding) — a live file exists and loads, so the pair is back
    //     in service and its artifact is debris -> exactly 1 `historical` entry,
    //     naming `superseded`.
    //   (Ghost, vec) — no live file at all, so its artifact is the only
    //     remaining evidence those vectors are gone -> exactly 1 `active` entry,
    //     naming `orphan`.
    // Both tiers must agree here: neither conclusion needs a file's contents.
    for (tier, health) in [
        ("deep", GraphDb::vector_index_load_failures(root)),
        ("cheap", GraphDb::vector_index_health(root)),
    ] {
        assert_eq!(
            health.unscannable, None,
            "{tier}: the directory is readable"
        );
        assert_eq!(
            health.active.len(),
            1,
            "{tier}: a rebuilt pair must not stay in `active`, and an \
             unrecovered one must be there; got {:?}",
            health.active
        );
        assert_eq!(
            (
                health.active[0].label.as_str(),
                health.active[0].prop.as_str()
            ),
            ("Ghost", "vec"),
            "{tier}: the wrong pair was reported active: {:?}",
            health.active
        );
        assert_eq!(
            health.active[0].path, orphan,
            "{tier}: the report must name the surviving bytes"
        );
        assert_eq!(
            health.historical.len(),
            1,
            "{tier}: the repaired pair's wreckage must stay visible as history, \
             got {:?}",
            health.historical
        );
        assert_eq!(
            (
                health.historical[0].label.as_str(),
                health.historical[0].prop.as_str()
            ),
            ("Memory", "embedding"),
            "{tier}: wrong pair in history"
        );
        assert_eq!(
            health.historical[0].path, superseded,
            "{tier}: history must name the artifact an operator would inspect"
        );
        assert!(
            !health.is_healthy(),
            "{tier}: (Ghost, vec) is still unrecovered, so the store is not healthy"
        );
    }

    // And once the orphan is dealt with, the store goes green again with the
    // history intact — the property an alarm needs and did not have.
    std::fs::remove_file(&orphan).expect("operator removes the orphan artifact");
    let after = GraphDb::vector_index_load_failures(root);
    assert!(
        after.is_healthy(),
        "with nothing unrecovered the store must read healthy, got {after:?}"
    );
    assert_eq!(
        after.historical.len(),
        1,
        "going green must not erase the forensic trail"
    );

    assert!(
        superseded.is_file(),
        "the diagnostic must not delete anything — the superseded bytes are \
         still the operator's to inspect"
    );
}

// ── 4. The diagnostic is read-only, and its path field is honest (#455) ───────

/// Running the diagnostic twice must give the same answer as running it once,
/// and the path it reports must resolve when the caller receives it.
///
/// Pre-fix the live arm reported the `.bin` path while `load` renamed that very
/// file during the same probe, so `exists(path)` was false on the first — and
/// most-acted-on — report, and the second call answered differently.
#[test]
fn the_diagnostic_is_read_only_and_reports_a_path_that_resolves() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    {
        let db = GraphDb::open(root).expect("open fresh db");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create vector index");
    }
    let file = index_file(root, "Memory", "embedding");
    truncate_to_4_bytes(&file);
    let damaged_bytes = std::fs::read(&file).expect("read damaged");

    // Hand-derived, first call: `vector_indexes/` holds exactly one recognised
    // entry, a live `.bin` for (Memory, embedding) that cannot decode.  So one
    // failure, from the live arm, whose path is that `.bin`.
    let first = GraphDb::vector_index_load_failures(root);
    assert_eq!(first.unscannable, None, "the directory is readable");
    assert_eq!(
        first.active.len(),
        1,
        "exactly one damaged file, got {:?}",
        first.active
    );
    assert_eq!(
        (
            first.active[0].label.as_str(),
            first.active[0].prop.as_str()
        ),
        ("Memory", "embedding")
    );
    assert_eq!(
        first.active[0].path, file,
        "the live arm must name the live file"
    );
    assert!(
        first.active[0].path.is_file(),
        "the reported path {} does not resolve — the only machine-readable \
         field in the report is a lie (#455)",
        first.active[0].path.display()
    );

    // Hand-derived: nothing above mutates, so the directory is byte-identical
    // and the second call must produce an identical report.
    assert!(
        quarantine_artifacts(root).is_empty(),
        "the diagnostic created a quarantine artifact: {:?}",
        quarantine_artifacts(root)
    );
    assert_eq!(
        std::fs::read(&file).expect("re-read"),
        damaged_bytes,
        "the diagnostic must not touch the bytes it is diagnosing"
    );

    let second = GraphDb::vector_index_load_failures(root);
    assert_eq!(
        second, first,
        "a health check that runs twice must not get a different answer than one \
         that runs once"
    );
}

// ── 5. `load` vs `load_and_quarantine`, at the storage boundary ───────────────

/// The split itself: same verdict, different side effect.
#[test]
fn load_leaves_the_file_alone_and_load_and_quarantine_moves_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let vdir = dir.path();
    let mut idx = VectorIndex::new(3, sparrowdb_storage::vector_index::Metric::Cosine);
    idx.insert(1, &[1.0_f32, 0.0, 0.0]);
    idx.save(vdir, "L", "p").expect("save");

    let path = vdir.join("hnsw_L_p.bin");
    truncate_to_4_bytes(&path);
    let damaged = std::fs::read(&path).expect("read damaged");

    // Hand-derived: 4 bytes cannot decode (see `truncate_to_4_bytes`), so both
    // entry points must return Err.  `load` leaves the file; only
    // `load_and_quarantine` renames it.
    let err = VectorIndex::load(vdir, "L", "p").expect_err("4 bytes cannot decode");
    assert!(
        err.to_string().contains("corrupt"),
        "the error must name the problem, got: {err}"
    );
    assert!(path.is_file(), "load() must not move the file");
    assert_eq!(
        std::fs::read(&path).expect("re-read"),
        damaged,
        "load() must not alter the bytes"
    );

    // Repeatable, because it is non-destructive.
    VectorIndex::load(vdir, "L", "p").expect_err("second load must fail the same way");
    assert!(path.is_file(), "load() is still non-destructive on repeat");

    VectorIndex::load_and_quarantine(vdir, "L", "p").expect_err("still undecodable");
    assert!(
        !path.exists(),
        "load_and_quarantine() must move the damaged file aside"
    );
    let artifacts: Vec<PathBuf> = std::fs::read_dir(vdir)
        .expect("read_dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".corrupt."))
        })
        .collect();
    assert_eq!(
        artifacts.len(),
        1,
        "exactly one artifact, got {artifacts:?}"
    );
    assert_eq!(
        std::fs::read(&artifacts[0]).expect("read artifact"),
        damaged,
        "the quarantined copy must be the damaged bytes, unmodified"
    );
}

// ── 6. A dangling symlink is present, not absent ──────────────────────────────

/// `scan_vector_index_dir` classifies by file name, so a symlink named
/// `hnsw_<label>_<prop>.bin` reads as a live index.  `load` used to begin with
/// `path.exists()`, which *follows* symlinks and is false for a broken one, so
/// the loader answered `Ok(None)` — "no index configured".  That is exactly the
/// present-but-treated-as-absent failure #445 exists to kill: every vector write
/// for the pair is dropped and every search returns nothing, with no error.
#[cfg(unix)]
#[test]
fn a_dangling_symlink_is_not_mistaken_for_an_absent_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let vdir = root.join("vector_indexes");
    std::fs::create_dir_all(&vdir).expect("create vector_indexes");

    let target = vdir.join("gone.bin");
    let link = vdir.join("hnsw_Memory_embedding.bin");
    std::os::unix::fs::symlink(&target, &link).expect("plant dangling symlink");

    // Hand-derived fixture check: the entry is there (lstat succeeds) but does
    // not resolve (stat fails).  This is the whole premise of the test.
    assert!(
        std::fs::symlink_metadata(&link).is_ok(),
        "fixture precondition: the directory entry must exist"
    );
    assert!(
        !link.exists(),
        "fixture precondition: the link must not resolve"
    );

    // Hand-derived: one recognised live entry that cannot be read, therefore
    // one active failure — never silence.  Both tiers must see it: detecting a
    // non-resolving entry costs one `stat`, not a payload read.
    for (tier, health) in [
        ("deep", GraphDb::vector_index_load_failures(root)),
        ("cheap", GraphDb::vector_index_health(root)),
    ] {
        assert_eq!(
            health.unscannable, None,
            "{tier}: the directory is readable"
        );
        assert_eq!(
            health.active.len(),
            1,
            "{tier}: a present-but-unreadable entry must be reported, got {:?}",
            health.active
        );
        assert_eq!(
            (
                health.active[0].label.as_str(),
                health.active[0].prop.as_str()
            ),
            ("Memory", "embedding"),
            "{tier}: the report must name the pair whose writes would otherwise vanish"
        );
        assert!(
            !health.is_healthy(),
            "{tier}: a configured pair that resolves to nothing is not healthy"
        );
    }

    // And `open` must refuse rather than start up silently missing the index.
    match GraphDb::open(root) {
        Ok(_) => panic!(
            "open() succeeded with a present-but-unreadable index at {}; the pair \
             is now silently 'no index configured' and every vector write for it \
             is dropped",
            link.display()
        ),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Memory") && msg.contains("embedding"),
                "the failure must name the pair, got: {msg}"
            );
        }
    }

    // Nothing was renamed: there are no bytes to preserve, so quarantining a
    // dangling link would only destroy the evidence.
    assert!(
        quarantine_artifacts(root).is_empty(),
        "a dangling symlink must not be quarantined, found {:?}",
        quarantine_artifacts(root)
    );
    assert!(
        std::fs::symlink_metadata(&link).is_ok(),
        "the link itself must be left in place for the operator"
    );
}

// ── 7. "I could not look" must not read as "nothing is wrong" ────────────────

/// A `vector_indexes/` directory that cannot be listed must not produce a report
/// byte-identical to a healthy store's.
///
/// `read_dir` errors were swallowed into an empty vector, so a permission block
/// — a misconfigured service account, a transient mount failure during a poll —
/// answered exactly what a genuinely healthy store answers.  A monitor could not
/// distinguish green from blind.
#[cfg(unix)]
#[test]
fn an_unlistable_directory_is_not_reported_as_healthy() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    {
        let db = GraphDb::open(root).expect("open fresh db");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create vector index");
    }
    let vdir = root.join("vector_indexes");

    // Control: hand-derived, this store is healthy — one live index that loads,
    // no artifacts, directory readable.
    let control = GraphDb::vector_index_load_failures(root);
    assert!(
        control.is_healthy() && control.unscannable.is_none(),
        "fixture precondition: the control must be healthy, got {control:?}"
    );

    std::fs::set_permissions(&vdir, std::fs::Permissions::from_mode(0o000))
        .expect("chmod 000 the vector index directory");

    // Root ignores the permission bits, so the block would not be real there and
    // the test would assert something that is not true of the run.  Skip rather
    // than fail: the property under test cannot be exercised.
    if std::fs::read_dir(&vdir).is_ok() {
        std::fs::set_permissions(&vdir, std::fs::Permissions::from_mode(0o755)).ok();
        eprintln!("skipping: chmod 000 did not block this uid (running as root?)");
        return;
    }

    // Hand-derived: the directory exists and cannot be listed, so nothing about
    // its contents was observed.  `unscannable` must say so, both lists must be
    // empty (nothing was seen — not nothing is wrong), and the report must not
    // read as healthy.
    for (tier, health) in [
        ("deep", GraphDb::vector_index_load_failures(root)),
        ("cheap", GraphDb::vector_index_health(root)),
    ] {
        assert!(
            health.unscannable.is_some(),
            "{tier}: an unlistable directory must say so, got {health:?}"
        );
        assert!(
            health.active.is_empty() && health.historical.is_empty(),
            "{tier}: nothing was observed, so nothing may be claimed: {health:?}"
        );
        assert!(
            !health.is_healthy(),
            "{tier}: not knowing is not the same as being fine"
        );
        assert_ne!(
            health, control,
            "{tier}: blind must not be observationally identical to green"
        );
    }

    // And `open` must refuse: a directory we cannot list may hold indexes, and
    // starting anyway means "no index configured" for every one of them, with
    // every vector write for them dropped.
    let opened = GraphDb::open(root);
    let refused = opened.is_err();
    // Restore permissions before the assert so the tempdir can always be
    // cleaned up, whichever way the assert goes.
    std::fs::set_permissions(&vdir, std::fs::Permissions::from_mode(0o755))
        .expect("restore permissions");
    assert!(
        refused,
        "open() must not treat an unlistable vector_indexes/ as empty"
    );
}

// ── 8. The cheap tier reads no index contents ────────────────────────────────

/// `vector_index_health` must not open index files.
///
/// The deep tier fully deserialises every healthy index on every call — for an
/// 8 MB index polled hourly, 8 MB of I/O and a complete HNSW graph rebuild an
/// hour spent confirming that nothing is wrong.  That is a permanent cost in the
/// common case, as opposed to the mutation, which is a correctness problem in an
/// edge case.  Both argue for the same split; only this one applies when nothing
/// is damaged at all.
///
/// Proving "it did not read the file" directly is not possible from a test, so
/// this makes the file *unreadable* while leaving it present: `chmod 000` still
/// permits `stat` (which the cheap tier does) and denies `open` (which only the
/// deep tier does).  The two tiers must therefore disagree, and that disagreement
/// is only possible if the cheap tier never opened it.
#[cfg(unix)]
#[test]
fn the_cheap_tier_reads_no_index_contents() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    {
        let db = GraphDb::open(root).expect("open fresh db");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create vector index");
    }
    let file = index_file(root, "Memory", "embedding");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000))
        .expect("chmod 000 the index file");

    if std::fs::read(&file).is_ok() {
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).ok();
        eprintln!("skipping: chmod 000 did not block this uid (running as root?)");
        return;
    }

    // Hand-derived, cheap tier: the directory lists one live entry, `stat`
    // resolves it (permission bits do not affect `stat`), and no contents are
    // read.  Nothing else is on disk.  So: no active failures, no history,
    // healthy.
    let cheap = GraphDb::vector_index_health(root);
    assert_eq!(cheap.unscannable, None, "the directory itself is readable");
    assert!(
        cheap.is_healthy(),
        "the cheap tier opened the index file — it must not; got {cheap:?}"
    );

    // Hand-derived, deep tier: it opens the file, `open` fails with EACCES, and
    // an unreadable live index is a failure, not an absence.  So: exactly one
    // active entry, naming this pair and this path.
    let deep = GraphDb::vector_index_load_failures(root);
    assert_eq!(deep.unscannable, None, "the directory itself is readable");
    assert_eq!(
        deep.active.len(),
        1,
        "the deep tier must report the file it cannot read, got {:?}",
        deep.active
    );
    assert_eq!(
        (deep.active[0].label.as_str(), deep.active[0].prop.as_str()),
        ("Memory", "embedding")
    );
    assert_eq!(deep.active[0].path, file);

    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644))
        .expect("restore permissions");
}

// ── 9. Composition with #453's save() sidecars ───────────────────────────────

/// #453 put two new files next to every index: `<index>.bin.lock`, which it
/// deliberately never unlinks, and `<index>.bin.tmp.<pid>.<nonce>` staging
/// files, which a crashed writer leaves behind for good.
///
/// Neither is an index. Both live in the directory this PR's scanner classifies
/// **by file name**, and a scanner that guessed wrong would either refuse to
/// open a healthy store or report a healthy store as damaged — on every store
/// that has ever saved a vector, which is all of them.
///
/// #442 and #446 each behaved correctly alone and composed into #450, so this
/// is asserted by execution rather than by reading the two patches side by side.
#[test]
fn save_sidecars_from_issue_453_are_not_mistaken_for_indexes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let vdir = root.join("vector_indexes");
    {
        let db = GraphDb::open(root).expect("open fresh db");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create vector index");
        let arc = db.get_vector_index("Memory", "embedding").expect("handle");
        arc.write()
            .expect("write lock")
            .insert(7, &[1.0_f32, 0.0, 0.0]);
        // A real save, so the lock file is created by #453's own code path
        // rather than by this test guessing at its name.
        arc.read()
            .expect("read lock")
            .save(&vdir, "Memory", "embedding")
            .expect("persist index");
    }

    // Hand-derived from #453: `lock_path` appends `.lock` to the index path, so
    // a store that has saved once holds `hnsw_Memory_embedding.bin.lock`. Assert
    // it is really there — if #453 ever stops creating it, this test must fail
    // loudly rather than quietly stop testing anything.
    let lock = vdir.join("hnsw_Memory_embedding.bin.lock");
    assert!(
        lock.is_file(),
        "fixture precondition: #453's save() must leave {} behind",
        lock.display()
    );

    // And a staging file from a writer that died mid-save. `temp_path` appends
    // `.tmp.<pid>.<nonce>`; the exact pid and nonce do not matter, only the
    // shape, so plant one directly.
    let staging = vdir.join("hnsw_Memory_embedding.bin.tmp.99999.0");
    std::fs::write(&staging, [0xFF_u8; 64]).expect("plant stale staging file");
    // Plus the pre-#452 fixed name, which #453's sweep still recognises.
    let legacy_staging = vdir.join("hnsw_Memory_embedding.bin.tmp");
    std::fs::write(&legacy_staging, [0xFF_u8; 64]).expect("plant legacy staging file");

    // Hand-derived expectation. The scanner recognises exactly two shapes:
    // a name ending `.bin`, and a name containing `.bin.corrupt.`.
    //   hnsw_Memory_embedding.bin.lock      -> ends `.lock`, not `.bin`; no `.corrupt.`
    //   hnsw_Memory_embedding.bin.tmp.99999.0 -> ends `.0`;    no `.corrupt.`
    //   hnsw_Memory_embedding.bin.tmp       -> ends `.tmp`;    no `.corrupt.`
    // So all three are ignored, and the only recognised entry is the healthy
    // live index. Expected at BOTH tiers: 0 active, 0 historical, healthy.
    for (tier, health) in [
        ("deep", GraphDb::vector_index_load_failures(root)),
        ("cheap", GraphDb::vector_index_health(root)),
    ] {
        assert_eq!(
            health.unscannable, None,
            "{tier}: the directory is perfectly readable"
        );
        assert!(
            health.active.is_empty(),
            "{tier}: #453's sidecars must not be reported as damage, got {:?}",
            health.active
        );
        assert!(
            health.historical.is_empty(),
            "{tier}: and they are not quarantine artifacts either, got {:?}",
            health.historical
        );
        assert!(
            health.is_healthy(),
            "{tier}: a store that merely saved a vector must read healthy"
        );
    }

    // The other direction: open() must not refuse because of them, and the real
    // index must still load with its one vector.
    let db = GraphDb::open(root).expect("sidecars must not make open() fail");
    let arc = db
        .get_vector_index("Memory", "embedding")
        .expect("the healthy index must still load");
    assert_eq!(
        arc.read().expect("read lock").len(),
        1,
        "one vector was saved, so one must come back"
    );
}

// ── 10. A damaged index is read once per incident, not once per row ──────────

/// `hybrid_search` is a scalar function: it is evaluated once per candidate row.
/// The success path has been cached against the on-disk fingerprint since #442,
/// but the *failure* path was not, so a damaged index was re-read and re-decoded
/// on every row and emitted an identical warning on every row.
///
/// For KMSmcp's 8 MB `Knowledge.embedding` index an N-row scan is N x 8 MB of
/// I/O plus N identical log lines — an I/O storm and a log flood arriving
/// exactly when the store is already degraded and someone is reading the logs to
/// find out why. A file that decodes cleanly and then fails
/// `validate_invariants` paid a full `bincode` decode every row.
///
/// The warning count is the observable: row 2 can only skip its warning by
/// taking the cached path, so `warns == 1` over a 5-row scan is direct evidence
/// that four rows never touched the file.
///
/// The second half is the dangerous half. A cached negative that outlived its
/// repair would recreate, one layer down, the "fixed but still reports broken"
/// failure this PR removes from the diagnostic — so the repair must become
/// visible mid-session, and it is asserted here rather than assumed.
mod warn_counter {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub static HYBRID_WARNS: AtomicUsize = AtomicUsize::new(0);
    static INIT: std::sync::Once = std::sync::Once::new();

    /// Collects an event's formatted fields so we can tell *our* warning apart
    /// from any other warning the engine might emit.
    struct Collect(String);

    impl tracing::field::Visit for Collect {
        fn record_debug(&mut self, _f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
            use std::fmt::Write;
            let _ = write!(self.0, "{v:?} ");
        }
    }

    struct Counter;

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Counter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            let mut fields = Collect(String::new());
            event.record(&mut fields);
            if fields.0.contains("hybrid_search") {
                HYBRID_WARNS.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Install the counting subscriber once for this test binary.
    ///
    /// A global subscriber rather than a thread-local one: the query engine is
    /// free to evaluate rows on a worker thread, and a thread-local default
    /// would silently count zero there — a test that cannot observe the thing it
    /// asserts is worse than no test. No other test in this binary produces a
    /// warning mentioning `hybrid_search`, so the shared counter is unambiguous.
    pub fn install() {
        INIT.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            let subscriber = tracing_subscriber::registry().with(Counter);
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
    }

    pub fn reset() {
        HYBRID_WARNS.store(0, Ordering::SeqCst);
    }

    pub fn count() -> usize {
        HYBRID_WARNS.load(Ordering::SeqCst)
    }
}

#[test]
fn a_damaged_index_is_read_and_warned_about_once_per_incident_not_once_per_row() {
    warn_counter::install();

    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = GraphDb::open(root).expect("open fresh db");

    // Fixture: 5 Doc nodes, each with a vector, one saved index, no FTS index.
    const ROWS: usize = 5;
    db.create_vector_index("Doc", "embedding", 3, "cosine")
        .expect("create vector index");
    let arc = db.get_vector_index("Doc", "embedding").expect("handle");
    for i in 0..ROWS {
        db.execute(&format!(
            "CREATE (d:Doc {{dockey: 'n{i}', content: 'alpha'}})"
        ))
        .expect("create node");
        let node_id = match &db
            .execute(&format!(
                "MATCH (n:Doc) WHERE n.dockey = 'n{i}' RETURN id(n) AS nid"
            ))
            .expect("id query")
            .rows[0][0]
        {
            Value::Int64(n) => *n as u64,
            other => panic!("expected Int64 node id, got {other:?}"),
        };
        arc.write()
            .expect("write lock")
            .insert(node_id, &[1.0_f32, 0.0, 0.0]);
    }
    arc.read()
        .expect("read lock")
        .save(&root.join("vector_indexes"), "Doc", "embedding")
        .expect("persist index");

    // One row per Doc node, and `hybrid_search` is evaluated once per row.
    //
    // The `WITH d` is load-bearing, not decoration. Without it the chunked
    // pipeline serves the scan and never dispatches `hybrid_search` at all —
    // it returns Null for a perfectly healthy index, which would make this test
    // pass for entirely the wrong reason. The baseline assertion below is what
    // catches that: a shape that cannot produce a list when the index is intact
    // cannot prove anything about what happens when it is damaged.
    const QUERY: &str = "MATCH (d:Doc) WITH d RETURN hybrid_search('Doc', 'embedding', 'content', \
                         [1.0, 0.0, 0.0], 'alpha', 3) AS hits";

    // ── Baseline: healthy. Hand-derived: ROWS rows, every value a list. ──
    let baseline = db.execute(QUERY).expect("baseline executes");
    assert_eq!(
        baseline.rows.len(),
        ROWS,
        "fixture precondition: one row per Doc node, so the scalar is evaluated \
         {ROWS} times"
    );
    for (i, row) in baseline.rows.iter().enumerate() {
        assert!(
            matches!(row[0], Value::List(_)),
            "fixture precondition: row {i} of a healthy index must be a list, got {:?}",
            row[0]
        );
    }

    let file = index_file(root, "Doc", "embedding");
    let healthy_bytes = std::fs::read(&file).expect("read healthy index");

    // ── Damage it under the open database. ──
    truncate_to_4_bytes(&file);
    warn_counter::reset();
    let damaged = db.execute(QUERY).expect("the query still executes");

    // Hand-derived. The scalar is evaluated once per row, so {ROWS} times.
    // Row 1 finds no cache entry matching the new fingerprint, reads the file,
    // fails, caches the verdict and warns. Rows 2..{ROWS} find that verdict
    // under an unchanged fingerprint and reuse it without touching the file and
    // without warning. Expected: {ROWS} Null values, and exactly ONE warning.
    assert_eq!(
        damaged.rows.len(),
        ROWS,
        "the scan still produces every row"
    );
    for (i, row) in damaged.rows.iter().enumerate() {
        assert!(
            matches!(row[0], Value::Null),
            "row {i}: a damaged index must yield Null, not {:?}",
            row[0]
        );
    }
    assert_eq!(
        warn_counter::count(),
        1,
        "a {ROWS}-row scan against one damaged index must warn once, not once \
         per row; N identical lines bury the signal at the moment someone is \
         reading the logs to find out what broke"
    );
    // Still non-destructive, and still exactly the bytes we wrote.
    assert!(file.is_file(), "the read must not have moved the file");
    assert!(
        quarantine_artifacts(root).is_empty(),
        "a read must not quarantine, found {:?}",
        quarantine_artifacts(root)
    );

    // ── Repair it mid-session, the way an operator restoring a backup would. ──
    //
    // Hand-derived: writing the original image back restores the original
    // fingerprint, `(generation, crc32c)`, which cannot equal the damaged file's
    // `(4, mtime_nanos)` fallback — mtime_nanos is ~1.7e18 and crc32c is at most
    // u32::MAX = 4_294_967_295, so the second elements can never match. The
    // cached negative therefore cannot be reused, every row must see the
    // repaired index, and no row may warn.
    std::fs::write(&file, &healthy_bytes).expect("restore the index");
    warn_counter::reset();
    let repaired = db.execute(QUERY).expect("post-repair query executes");
    assert_eq!(repaired.rows.len(), ROWS);
    for (i, row) in repaired.rows.iter().enumerate() {
        assert!(
            matches!(row[0], Value::List(_)),
            "row {i}: the repair must be visible immediately — a cached negative \
             that outlives its repair is the 'fixed but still reports broken' \
             failure this PR exists to remove, one layer down. Got {:?}",
            row[0]
        );
    }
    assert_eq!(
        warn_counter::count(),
        0,
        "a repaired index must not warn at all"
    );
}
