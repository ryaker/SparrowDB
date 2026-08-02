//! Durability and visibility guards for the HNSW vector index (issue #441).
//!
//! Background — the incident these tests exist for:
//! a consumer's `hnsw_Knowledge_embedding.bin` measured 2,497,757 bytes before
//! an embedding backfill, 8,084,693 bytes immediately after it, and 4,352,529
//! bytes after the hosting daemon restarted, with only 1299 vectors still
//! reachable.  No error was raised at any point.
//!
//! Every expected value below is derived by hand from the fixture that the test
//! itself constructs.  Nothing here is a recording of what the code happens to
//! return.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use sparrowdb_storage::vector_index::{InsertOutcome, Metric, VectorIndex};

/// Length of the v2 header: `magic[8] || version:u32 || reserved:u32 ||
/// payload_len:u64 || generation:u64 || crc32c:u32`.
const V2_HEADER_LEN: usize = 36;

/// Path of the on-disk index file for `(label, prop)` inside `dir`.
/// Mirrors `VectorIndex::index_path`, which is private.
fn index_file(dir: &std::path::Path, label: &str, prop: &str) -> std::path::PathBuf {
    dir.join(format!("hnsw_{label}_{prop}.bin"))
}

/// Deterministic 4-dimensional unit vector: all zeros except dimension
/// `hot % 4`, which is 1.0.  Cosine similarity between two such vectors is
/// exactly 1.0 when their hot dimensions match and exactly 0.0 otherwise.
fn unit4(hot: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 4];
    v[hot % 4] = 1.0;
    v
}

// ── 1. Torn / overwritten index file must never load as a smaller index ───────

/// A shorter index written over the front of a longer file — the exact damage
/// pattern a non-atomic, non-truncating overwrite or two racing writers
/// produce — must be rejected, not silently accepted with fewer vectors.
///
/// Hand-derivation of the fixture and the expectation:
///
/// * Index A holds node ids 0..20 → **20 vectors**, by construction.
/// * Index B holds node ids 0..5  → **5 vectors**, by construction.
/// * B's serialised image is strictly shorter than A's: both encode the same
///   struct shape, and B's `nodes` vector has 15 fewer 4-float entries plus
///   15 fewer adjacency lists.  (The test asserts this rather than assuming it.)
/// * The damaged image is `B_bytes ++ A_bytes[B_bytes.len()..]`, i.e. exactly
///   what the filesystem holds if B's bytes land on top of A's file without the
///   file first being truncated.  Its length equals A's length.
///
/// The only two defensible outcomes for `load()` are "the 20 vectors A wrote"
/// or "an error".  Returning 5 vectors is silent data loss, because the caller
/// that wrote 20 gets 5 back and never learns.
#[test]
fn shorter_index_written_over_longer_file_is_rejected_not_silently_truncated() {
    let dir_a = tempfile::tempdir().expect("tempdir a");
    let dir_b = tempfile::tempdir().expect("tempdir b");

    let mut a = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..20 {
        a.insert(i, &unit4(i as usize));
    }
    a.save(dir_a.path(), "L", "p").expect("save a");
    assert_eq!(
        a.len(),
        20,
        "fixture A must hold 20 vectors by construction"
    );

    let mut b = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..5 {
        b.insert(i, &unit4(i as usize));
    }
    b.save(dir_b.path(), "L", "p").expect("save b");
    assert_eq!(b.len(), 5, "fixture B must hold 5 vectors by construction");

    let bytes_a = std::fs::read(index_file(dir_a.path(), "L", "p")).expect("read a");
    let bytes_b = std::fs::read(index_file(dir_b.path(), "L", "p")).expect("read b");
    assert!(
        bytes_b.len() < bytes_a.len(),
        "a 5-vector image ({} bytes) must be smaller than a 20-vector image ({} bytes)",
        bytes_b.len(),
        bytes_a.len()
    );

    // Splice: B's bytes over the front of A's file, A's tail left behind.
    let mut damaged = bytes_b.clone();
    damaged.extend_from_slice(&bytes_a[bytes_b.len()..]);
    assert_eq!(
        damaged.len(),
        bytes_a.len(),
        "the damaged image must be exactly as long as the file it overwrote"
    );
    std::fs::write(index_file(dir_a.path(), "L", "p"), &damaged).expect("write damaged");

    match VectorIndex::load(dir_a.path(), "L", "p") {
        Err(_) => { /* detected — the only safe outcome for a damaged file */ }
        Ok(Some(idx)) => panic!(
            "load() accepted a partially-overwritten index and returned {} vectors; \
             20 were written and 5 came back — this is the silent shrink from issue #441",
            idx.len()
        ),
        Ok(None) => panic!("load() reported 'no index' for a file that exists"),
    }
}

/// The damaged bytes must survive a rejected load: they are the only remaining
/// copy of the vectors, and a caller that treats a load error as "no index"
/// would otherwise let the next `save()` overwrite them with an empty index.
///
/// Expected: exactly one `*.corrupt.*` sibling exists afterwards, and it is
/// byte-identical to the damaged image we wrote.
///
/// Since #456 quarantine is opt-in: it belongs to `load_and_quarantine`, the
/// open path's loader, because that is the caller whose later `save()` would
/// overwrite the damaged bytes.  Plain `load` is non-destructive — see
/// `regression_456_load_is_not_destructive.rs`.
#[test]
fn rejected_index_file_is_quarantined_rather_than_left_to_be_overwritten() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut idx = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..8 {
        idx.insert(i, &unit4(i as usize));
    }
    idx.save(dir.path(), "L", "p").expect("save");

    let path = index_file(dir.path(), "L", "p");
    let good = std::fs::read(&path).expect("read");
    // Flip one bit in the payload, past the header.
    let mut damaged = good.clone();
    let victim = (V2_HEADER_LEN + 4).min(damaged.len() - 1);
    damaged[victim] ^= 0xFF;
    std::fs::write(&path, &damaged).expect("write damaged");

    let err = VectorIndex::load_and_quarantine(dir.path(), "L", "p")
        .expect_err("a single flipped payload byte must be detected");
    assert!(
        err.to_string().contains("corrupt"),
        "error must name the problem, got: {err}"
    );

    assert!(
        !path.exists(),
        "the damaged file must be moved aside, not left in place where the next save() clobbers it"
    );
    let quarantined: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".corrupt."))
        })
        .collect();
    assert_eq!(
        quarantined.len(),
        1,
        "expected exactly one quarantined copy, found {quarantined:?}"
    );
    assert_eq!(
        std::fs::read(&quarantined[0]).expect("read quarantined"),
        damaged,
        "the quarantined copy must be the damaged bytes, unmodified"
    );
}

/// A file truncated mid-write must not load as a shorter index.
///
/// Derivation: the fixture writes 20 vectors; the file is then cut to 60% of
/// its length.  The only safe answers are "20" or "error"; any smaller count
/// means a partial write was accepted as authoritative.
#[test]
fn truncated_index_file_never_loads_as_fewer_vectors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut idx = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..20 {
        idx.insert(i, &unit4(i as usize));
    }
    idx.save(dir.path(), "L", "p").expect("save");

    let path = index_file(dir.path(), "L", "p");
    let full = std::fs::read(&path).expect("read");
    let cut = full.len() * 60 / 100;
    std::fs::write(&path, &full[..cut]).expect("truncate");

    match VectorIndex::load(dir.path(), "L", "p") {
        Err(_) => {}
        Ok(Some(i)) => panic!(
            "a file truncated to 60% loaded as {} vectors; 20 were written",
            i.len()
        ),
        Ok(None) => panic!("load() reported 'no index' for a file that exists"),
    }
}

// ── 2. Crash during save must not destroy the previous index ─────────────────

/// A `save()` that fails partway must leave the previously saved index intact
/// and fully readable.  This is the property `fs::write` cannot provide: it
/// opens the destination with `O_TRUNC`, so the good copy is gone before the
/// replacement exists.
///
/// The failure is injected deterministically — no timing, no signals, no
/// flakiness — by removing write permission from the directory that holds the
/// index.  Creating a *new* file in a directory requires write permission on
/// the directory, so `File::create(<staging path>)` fails with `EACCES`, while
/// re-opening files that already exist (the index itself, the save lock) still
/// succeeds.  The save therefore aborts at exactly the point a crash would:
/// after serialisation, before the destination is touched.
///
/// This used to be injected by creating a directory at the fixed staging path
/// `<file>.bin.tmp`.  Since #452 the staging name carries a pid and a nonce, so
/// no single path can be blocked ahead of time; permissions are the remaining
/// deterministic lever.
///
/// Hand-derived expectations:
/// * before: 10 vectors on disk (ids 0..10, one `insert` each);
/// * the aborted save carries 30 vectors (ids 0..30);
/// * after: `save()` returns `Err`, and `load()` returns **10**, not 30 and not
///   an error — the old index is untouched.
#[cfg(unix)]
#[test]
fn failed_save_leaves_the_previous_index_intact() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");

    let mut small = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..10 {
        small.insert(i, &unit4(i as usize));
    }
    small.save(dir.path(), "L", "p").expect("first save");

    let path = index_file(dir.path(), "L", "p");
    let good_bytes = std::fs::read(&path).expect("read good");

    // Block creation of any new file in the directory.
    let original = std::fs::metadata(dir.path()).expect("stat").permissions();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))
        .expect("make the index directory read-only");

    // Directory permissions do not apply to a superuser, so confirm the
    // injection actually took before asserting on it.
    let probe = dir.path().join(".write_probe");
    if std::fs::File::create(&probe).is_ok() {
        let _ = std::fs::remove_file(&probe);
        std::fs::set_permissions(dir.path(), original).expect("restore");
        eprintln!(
            "skipping failed_save_leaves_the_previous_index_intact: this process can write to a \
             read-only directory (running as root?), so the failure cannot be injected"
        );
        return;
    }

    let mut big = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..30 {
        big.insert(i, &unit4(i as usize));
    }
    let result = big.save(dir.path(), "L", "p");

    assert!(
        result.is_err(),
        "a save that cannot stage its bytes must fail loudly, not overwrite the good index"
    );
    assert_eq!(
        std::fs::read(&path).expect("read after failed save"),
        good_bytes,
        "the destination file must be byte-identical to the last successful save"
    );

    // Unblock and confirm the survivor is a real, loadable, 10-vector index.
    std::fs::set_permissions(dir.path(), original).expect("restore permissions");
    let reloaded = VectorIndex::load(dir.path(), "L", "p")
        .expect("the surviving index must still load")
        .expect("the surviving index file must exist");
    assert_eq!(
        reloaded.len(),
        10,
        "10 vectors were durably saved before the failed save; all 10 must survive"
    );
}

/// A leftover staging file from a crashed save must never be mistaken for the
/// index, and `save()` must reclaim it rather than leave it to accumulate.
///
/// Since #452 staging names are `<index>.tmp.<pid>.<nonce>`, so a crashed
/// process leaves behind a name no later run will ever choose again: reclaiming
/// cannot be "overwrite the one fixed name", it has to sweep the whole family.
/// The fixture therefore plants three shapes at once:
///
/// * `<index>.tmp` — the fixed name a pre-#452 build wrote, still on disk in
///   any deployment that crashed before upgrading;
/// * `<index>.tmp.999999.0` — a per-pid staging file from a process that is
///   long gone (pid 999999 is not this test);
/// * `<index>.tmp.999999.1` — a second one from the same dead process.
///
/// All three must be gone after one save, and the unrelated sibling
/// `<index>.tmpfoo` — which is not a staging file, it just starts with the same
/// characters — must survive, or the sweep is deleting things it does not own.
#[test]
fn leftover_staging_file_is_ignored_and_reclaimed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut idx = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..6 {
        idx.insert(i, &unit4(i as usize));
    }
    idx.save(dir.path(), "L", "p").expect("save");

    let path = index_file(dir.path(), "L", "p");
    let sibling = |suffix: &str| {
        let mut s = path.clone().into_os_string();
        s.push(suffix);
        std::path::PathBuf::from(s)
    };
    let leftovers = [
        sibling(".tmp"),
        sibling(".tmp.999999.0"),
        sibling(".tmp.999999.1"),
    ];
    for p in &leftovers {
        std::fs::write(p, b"garbage from a crashed save").expect("write leftover");
    }
    let bystander = sibling(".tmpfoo");
    std::fs::write(&bystander, b"not a staging file").expect("write bystander");

    let loaded = VectorIndex::load(dir.path(), "L", "p")
        .expect("leftover staging file must not affect load")
        .expect("index must exist");
    assert_eq!(loaded.len(), 6, "6 vectors were saved; 6 must load");

    idx.save(dir.path(), "L", "p")
        .expect("save must reclaim a stale staging file");
    for p in &leftovers {
        assert!(
            !p.exists(),
            "{} outlived a save; per-pid staging files from crashed processes are never \
             reused, so nothing else will ever clean them up",
            p.display()
        );
    }
    assert!(
        bystander.exists(),
        "the sweep removed {}, which is not a staging file",
        bystander.display()
    );

    // And the save itself still produced a good index.
    assert_eq!(
        VectorIndex::load(dir.path(), "L", "p")
            .expect("load after reclaim")
            .expect("index must exist")
            .len(),
        6,
        "6 vectors were saved; 6 must survive the reclaim"
    );
}

// ── 3. Re-inserting an existing node id must be observable ───────────────────

/// Re-inserting the same `node_id` with a different vector must (a) tell the
/// caller what happened and (b) actually change what the index returns.
///
/// Hand-derivation, using the orthogonal unit vectors from `unit4`:
/// * node 100 starts at `[1,0,0,0]`, node 200 at `[0,1,0,0]`;
/// * node 100 is then re-inserted as `[0,0,1,0]`;
/// * query `[0,0,1,0]` scores, by the cosine formula on unit vectors:
///   cos(q, `[0,0,1,0]`) = 1·1 / (1·1) = **1.0** — node 100 after the update;
///   cos(q, `[0,1,0,0]`) = 0 / (1·1) = **0.0** — node 200;
///   cos(q, `[1,0,0,0]`) = 0 / (1·1) = **0.0** — node 100 before the update.
///   So the top result must be node 100 with score 1.0.  Against the old
///   behaviour (re-insert ignored) the best achievable score is 0.0.
/// * the index must still hold exactly **2** vectors: an update reuses the
///   node's slot and must not append a duplicate.
#[test]
fn reinserting_a_node_id_updates_the_vector_and_reports_it() {
    let mut idx = VectorIndex::new(4, Metric::Cosine);

    assert_eq!(
        idx.insert(100, &unit4(0)),
        InsertOutcome::Inserted,
        "first write of node 100 is an insert"
    );
    assert_eq!(
        idx.insert(200, &unit4(1)),
        InsertOutcome::Inserted,
        "first write of node 200 is an insert"
    );
    assert_eq!(
        idx.insert(100, &unit4(2)),
        InsertOutcome::Updated,
        "re-writing node 100 must be reported as an update, not silently dropped"
    );

    assert_eq!(
        idx.len(),
        2,
        "an update must reuse node 100's slot, not append a second copy"
    );

    let results = idx.search(&unit4(2), 2, 50);
    assert!(!results.is_empty(), "search must return the updated node");
    assert_eq!(
        results[0].0, 100,
        "node 100 now holds the query vector and must rank first"
    );
    assert!(
        (results[0].1 - 1.0).abs() < 1e-5,
        "cosine similarity against an identical unit vector is exactly 1.0, got {}",
        results[0].1
    );

    // The old vector must no longer be findable at node 100: querying [1,0,0,0]
    // is now orthogonal to everything in the index, so every score is 0.0.
    for (id, score) in idx.search(&unit4(0), 2, 50) {
        assert!(
            score.abs() < 1e-5,
            "node {id} scored {score} for the replaced vector; the update did not take effect"
        );
    }
}

/// An updated vector must survive a save/load round-trip — the update has to
/// reach disk, not just the in-memory copy.
///
/// Derivation: one node, written as `[1,0,0,0]` then as `[0,0,1,0]`; after
/// reload, querying `[0,0,1,0]` must score exactly 1.0 and the index must hold
/// exactly 1 vector.
#[test]
fn updated_vector_survives_save_and_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut idx = VectorIndex::new(4, Metric::Cosine);
    idx.insert(7, &unit4(0));
    idx.insert(7, &unit4(2));
    idx.save(dir.path(), "L", "p").expect("save");

    let loaded = VectorIndex::load(dir.path(), "L", "p")
        .expect("load")
        .expect("file exists");
    assert_eq!(
        loaded.len(),
        1,
        "one node id was written; one vector expected"
    );
    let results = loaded.search(&unit4(2), 1, 50);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 7);
    assert!(
        (results[0].1 - 1.0).abs() < 1e-5,
        "the reloaded vector must be the updated one (score 1.0), got {}",
        results[0].1
    );
}

// ── 4. End-to-end: vectors survive checkpoint + reopen ───────────────────────

/// Vectors written through Cypher must still be searchable after `checkpoint()`
/// and a fresh `GraphDb::open` — the reopen path is where the incident's
/// vectors disappeared.
///
/// Hand-derivation: three `Doc` nodes are written with the orthogonal unit
/// vectors `[1,0,0,0]`, `[0,1,0,0]`, `[0,0,1,0]`.  Querying `[0,0,1,0]` after
/// reopen must return `d3` first with cosine similarity 1.0, while `d1` and
/// `d2` are orthogonal to the query and score 0.0.  The index must hold
/// exactly 3 vectors.
#[test]
fn vectors_are_reachable_after_checkpoint_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let db = GraphDb::open(dir.path()).expect("open");
        db.execute(
            "CREATE VECTOR INDEX FOR (n:Doc) ON (n.embedding) \
             OPTIONS { dimensions: 4, similarity: 'cosine' }",
        )
        .expect("create vector index");

        for (id, hot) in [("d1", 0usize), ("d2", 1), ("d3", 2)] {
            db.execute(&format!("CREATE (n:Doc {{id: '{id}'}})"))
                .expect("create doc");
            let mut params = std::collections::HashMap::new();
            params.insert("id".to_string(), Value::String(id.to_string()));
            params.insert("emb".to_string(), Value::Vector(unit4(hot)));
            db.execute_with_params("MATCH (n:Doc {id: $id}) SET n.embedding = $emb", params)
                .expect("set embedding");
        }

        db.checkpoint().expect("checkpoint");
    }

    let db = GraphDb::open(dir.path()).expect("reopen");
    let arc = db
        .get_vector_index("Doc", "embedding")
        .expect("the vector index must still be registered after reopen");
    let idx = arc.read().expect("read lock");

    assert_eq!(
        idx.len(),
        3,
        "three documents were embedded; three vectors must survive checkpoint + reopen"
    );

    let results = idx.search(&unit4(2), 3, 50);
    assert_eq!(results.len(), 3, "all three vectors must be reachable");
    assert!(
        (results[0].1 - 1.0).abs() < 1e-5,
        "the exact-match document must score 1.0 after reopen, got {}",
        results[0].1
    );
    for (_, score) in results.iter().skip(1) {
        assert!(
            score.abs() < 1e-5,
            "the two orthogonal documents must score 0.0, got {score}"
        );
    }
}

/// Two `GraphDb` handles open on the same directory hold two independent
/// copies of the vector index: it is read from disk once, in `GraphDb::open`,
/// and nothing ever reloads it.  Whichever handle saves last used to replace
/// the other's vectors outright — no race window needed, just a long-lived
/// process that opened before someone else's backfill.
///
/// The stale handle's write must now fail instead, and the earlier handle's
/// vectors must survive.
///
/// Hand-derivation:
/// * both handles open when the index is empty;
/// * handle A embeds `a1` → the file holds **1** vector;
/// * handle B, which still believes the file is empty, embeds `b1`;
///   its save is refused, so the file must still hold exactly **1** vector,
///   and searching for A's vector must return score 1.0.
#[test]
fn a_stale_handle_cannot_silently_revert_another_writers_vectors() {
    let dir = tempfile::tempdir().expect("tempdir");

    let db_a = GraphDb::open(dir.path()).expect("open a");
    db_a.create_vector_index("Doc", "embedding", 4, "cosine")
        .expect("create index");
    db_a.execute("CREATE (n:Doc {id: 'a1'})")
        .expect("create a1");
    db_a.execute("CREATE (n:Doc {id: 'b1'})")
        .expect("create b1");

    // B opens *before* A writes any vector — the daemon-started-first scenario.
    let db_b = GraphDb::open(dir.path()).expect("open b");

    let mut params_a = std::collections::HashMap::new();
    params_a.insert("id".to_string(), Value::String("a1".to_string()));
    params_a.insert("emb".to_string(), Value::Vector(unit4(0)));
    db_a.execute_with_params("MATCH (n:Doc {id: $id}) SET n.embedding = $emb", params_a)
        .expect("A's write must succeed");

    let mut params_b = std::collections::HashMap::new();
    params_b.insert("id".to_string(), Value::String("b1".to_string()));
    params_b.insert("emb".to_string(), Value::Vector(unit4(1)));
    let err = db_b
        .execute_with_params("MATCH (n:Doc {id: $id}) SET n.embedding = $emb", params_b)
        .expect_err("the stale handle's write must be refused, not silently applied");
    assert!(
        err.to_string().contains("generation conflict"),
        "the failure must name the cause, got: {err}"
    );

    // A's vector must still be on disk and reachable from a fresh open.
    drop(db_a);
    drop(db_b);
    let db = GraphDb::open(dir.path()).expect("reopen");
    let arc = db
        .get_vector_index("Doc", "embedding")
        .expect("index must exist");
    let idx = arc.read().expect("read lock");
    assert_eq!(
        idx.len(),
        1,
        "exactly one vector was durably written; the refused write must not have added or removed any"
    );
    let results = idx.search(&unit4(0), 1, 50);
    assert_eq!(results.len(), 1);
    assert!(
        (results[0].1 - 1.0).abs() < 1e-5,
        "the surviving vector must be A's, scoring 1.0 against its own query, got {}",
        results[0].1
    );
}

/// A v1 index file — bare `bincode`, no header — written by an older build must
/// still load, and must be rewritten in the checksummed v2 format on the next
/// save.  Existing deployments cannot be asked to re-embed.
///
/// Derivation: 12 vectors in, 12 vectors out; after `save()` the file must
/// begin with the v2 magic `SPRWHNSW`.
#[test]
fn legacy_headerless_index_still_loads_and_is_upgraded_on_save() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut idx = VectorIndex::new(4, Metric::Cosine);
    for i in 0u64..12 {
        idx.insert(i, &unit4(i as usize));
    }

    // Build the v1 image without depending on bincode here: a v2 file is
    // `36-byte header || bincode payload`, so stripping the header yields
    // exactly the bytes the pre-#441 `save()` wrote.
    idx.save(dir.path(), "L", "p").expect("save v2");
    let path = index_file(dir.path(), "L", "p");
    let v2 = std::fs::read(&path).expect("read v2");
    assert_eq!(&v2[..8], b"SPRWHNSW", "fixture must start from a v2 file");
    std::fs::write(&path, &v2[V2_HEADER_LEN..]).expect("write legacy");

    let loaded = VectorIndex::load(dir.path(), "L", "p")
        .expect("a v1 file must still load")
        .expect("file exists");
    assert_eq!(loaded.len(), 12, "12 vectors were written; 12 must load");

    loaded.save(dir.path(), "L", "p").expect("resave");
    let upgraded = std::fs::read(&path).expect("read upgraded");
    assert_eq!(
        &upgraded[..8],
        b"SPRWHNSW",
        "the next save must upgrade the file to the checksummed v2 format"
    );
    assert_eq!(
        VectorIndex::load(dir.path(), "L", "p")
            .expect("load upgraded")
            .expect("exists")
            .len(),
        12,
        "the upgraded file must still hold all 12 vectors"
    );
}
