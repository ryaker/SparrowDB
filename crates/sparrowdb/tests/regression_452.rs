//! Regression guards for issue #452 — concurrent `VectorIndex::save`.
//!
//! #442 made `save()` crash-safe by staging into a temp file and renaming it,
//! and added a generation counter so a stale handle cannot silently replace a
//! newer index.  Both mechanisms were verified only *sequentially*: the guard
//! test `concurrent_handles_cannot_silently_overwrite_each_other` lets `a.save()`
//! return before `b.save()` begins, so no two writers are ever inside `save()`
//! at the same time and neither defect below can occur in it.
//!
//! Two defects live in that overlap.
//!
//! 1. The staging path was a deterministic `<index>.tmp`.  Two writers both
//!    `File::create` it; the first to `rename` vacates it; the loser's `rename`
//!    fails with `ENOENT` and its vectors are gone.  `is_lost_update()` matches
//!    on the generation-conflict message, so `ENOENT` reads as a disk fault and
//!    a caller following the documented recovery contract does not retry.
//!
//! 2. `save()` read the on-disk generation, then serialised, then wrote and
//!    renamed, with nothing held across that window.  Two writers that both
//!    read generation `N` both pass the check, both return `Ok`, and the second
//!    silently discards the first's vectors — issue #441's failure mode again,
//!    narrowed to milliseconds rather than removed.
//!
//! Every expected value below is derived by hand from the fixture the test
//! itself builds.  Nothing here records what the code happens to return.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};

use sparrowdb_storage::vector_index::{Metric, VectorIndex};

/// Vector width used throughout.  Eight dimensions is wide enough that the
/// serialised payload is a few kilobytes — the write and `fsync` a racing pair
/// must overlap in — and narrow enough to stay fast.
const DIMS: usize = 8;

/// Deterministic unit vector: all zeros except dimension `hot % DIMS`, which is
/// 1.0.  Only used to give each node id *some* distinct content; the assertions
/// below are about which ids are on disk, not about distances.
fn unit(hot: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIMS];
    v[hot % DIMS] = 1.0;
    v
}

/// Path of the on-disk index file, mirroring the private `index_path`.
fn index_file(dir: &Path, label: &str, prop: &str) -> PathBuf {
    dir.join(format!("hnsw_{label}_{prop}.bin"))
}

/// Every staging-file sibling of the index currently on disk.
fn staging_debris(dir: &Path, label: &str, prop: &str) -> Vec<PathBuf> {
    let prefix = format!("hnsw_{label}_{prop}.bin.tmp");
    std::fs::read_dir(dir)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(&prefix))
        })
        .map(|e| e.path())
        .collect()
}

// ── Defect 1 + 2: two writers genuinely inside save() at the same time ───────

/// Number of barrier-synchronised rounds.
///
/// The reporter reproduced the `ENOENT` collision in 27 of 27 barrier-
/// synchronised runs, so a single round already fails against the pre-fix code
/// with high probability.  Forty rounds is chosen so the test would still fail
/// pre-fix even if the per-round collision probability on some other machine
/// were as low as 10%: 1 − 0.9^40 ≈ 0.985.  It costs well under a second.
const ROUNDS: usize = 40;

/// Two handles loaded from the same generation, both saving at once.
///
/// Hand-derivation, per round:
///
/// * the seed index is built from node ids `0..64`, one `insert` each, all
///   distinct → the file holds **64** vectors at generation 1;
/// * handle A and handle B both `load()` that file, so both hold 64 vectors and
///   both believe they are at generation 1;
/// * A inserts ids 100, 101, 102 → A holds 64 + 3 = **67**;
/// * B inserts ids 200, 201, 202 → B holds 64 + 3 = **67**;
/// * both call `save()` from separate threads released by one `Barrier(2)`.
///
/// Exactly one of the two can be allowed to land, because each carries a whole
/// index image and neither contains the other's ids.  Therefore:
///
/// * exactly **one** `save()` returns `Ok`;
/// * the other returns `Err`, and that error must satisfy `is_lost_update()` —
///   "you lost, reload and retry" — not an `ENOENT` a caller must read as a
///   broken disk;
/// * reloading the file from disk must yield exactly **67** vectors: the 64
///   seeds plus the winner's 3.  Not 70 (the two writes cannot both be there,
///   they are whole-file replacements), not 64 (the winner's write must have
///   landed), and not an error;
/// * the winner's three ids are present and the loser's three are absent —
///   `len()` alone cannot tell 64 + A from 64 + B apart.
#[test]
fn overlapping_saves_leave_one_winner_and_a_retryable_loser() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        let mut seed = VectorIndex::new(DIMS, Metric::Cosine);
        for i in 0u64..64 {
            seed.insert(i, &unit(i as usize));
        }
        assert_eq!(
            seed.len(),
            64,
            "round {round}: 64 distinct ids were inserted"
        );
        seed.save(&path, "L", "p").expect("seed save");

        let mut a = VectorIndex::load(&path, "L", "p")
            .expect("load a")
            .expect("seed file exists");
        let mut b = VectorIndex::load(&path, "L", "p")
            .expect("load b")
            .expect("seed file exists");
        for i in 0u64..3 {
            a.insert(100 + i, &unit(i as usize));
            b.insert(200 + i, &unit(i as usize));
        }
        assert_eq!(a.len(), 67, "round {round}: 64 seeds + A's 3 new ids");
        assert_eq!(b.len(), 67, "round {round}: 64 seeds + B's 3 new ids");

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [a, b]
            .into_iter()
            .map(|idx| {
                let barrier = Arc::clone(&barrier);
                let path = path.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    idx.save(&path, "L", "p")
                })
            })
            .collect();
        let results: Vec<std::io::Result<()>> = handles
            .into_iter()
            .map(|h| h.join().expect("save thread must not panic"))
            .collect();

        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            winners, 1,
            "round {round}: exactly one of two overlapping saves may land, got {winners} \
             (results: {results:?})"
        );

        let loser = results
            .iter()
            .find_map(|r| r.as_ref().err())
            .expect("one save must have been refused");
        assert!(
            VectorIndex::is_lost_update(loser),
            "round {round}: the fenced-out writer must get a lost-update refusal it can act on \
             by reloading and retrying, got: {loser} (kind {:?})",
            loser.kind()
        );

        let on_disk = VectorIndex::load(&path, "L", "p")
            .expect("the winner's index must still load")
            .expect("index file must exist");
        assert_eq!(
            on_disk.len(),
            67,
            "round {round}: 64 seeded ids plus the winning writer's 3 = 67"
        );

        // A won iff the first result is Ok — the threads were spawned in [a, b]
        // order and `results` preserves it.
        let (present, absent): ([u64; 3], [u64; 3]) = if results[0].is_ok() {
            ([100, 101, 102], [200, 201, 202])
        } else {
            ([200, 201, 202], [100, 101, 102])
        };
        for id in present {
            assert!(
                on_disk.has_vector(id),
                "round {round}: id {id} belongs to the writer whose save returned Ok and must be \
                 on disk"
            );
        }
        for id in absent {
            assert!(
                !on_disk.has_vector(id),
                "round {round}: id {id} belongs to the writer whose save was refused; a refused \
                 save must not have written anything"
            );
        }

        assert!(
            staging_debris(&path, "L", "p").is_empty(),
            "round {round}: staging files outlived the saves: {:?}",
            staging_debris(&path, "L", "p")
        );
    }
}

// ── The same race across two real processes ──────────────────────────────────

/// Set on a child process to turn `child_writer` into a worker.  Its value is
/// the scratch directory; `CHILD_BASE_ENV` carries the first node id the child
/// should write.
const CHILD_DIR_ENV: &str = "SPARROWDB_452_CHILD_DIR";
const CHILD_BASE_ENV: &str = "SPARROWDB_452_CHILD_BASE";

/// Exit codes the child uses to report what `save()` did.  Anything else is a
/// panic or an unexpected `io::Error`, which the parent reports verbatim.
const EXIT_SAVED: i32 = 10;
const EXIT_LOST_UPDATE: i32 = 11;
const EXIT_OTHER_ERROR: i32 = 12;

/// Deadline for the cross-process rendezvous.  Generous — it only has to cover
/// process startup on a loaded CI box, and a miss fails the test rather than
/// hanging it.
const RENDEZVOUS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Spin until `flag` appears, or fail.
fn await_flag(flag: &Path, who: &str) {
    let deadline = std::time::Instant::now() + RENDEZVOUS_TIMEOUT;
    while !flag.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "{who}: {} never appeared within {RENDEZVOUS_TIMEOUT:?}",
            flag.display()
        );
        std::thread::yield_now();
    }
}

/// Worker half of `two_processes_saving_at_once_leave_one_winner`.
///
/// Inert unless `CHILD_DIR_ENV` is set, and `#[ignore]`d so an ordinary
/// `cargo test` run never invokes it; the parent re-executes this same test
/// binary with `--ignored --exact` to reach it.
#[test]
#[ignore = "worker process for two_processes_saving_at_once_leave_one_winner"]
fn child_writer() {
    // Return rather than panic: `cargo test -- --ignored` is a standard way to
    // sweep ignored tests, and in that run the parent never sets CHILD_DIR_ENV.
    // Panicking there fails the job even though nothing is wrong.  The guard's
    // purpose — do nothing useful when invoked directly — is preserved, and
    // `two_processes_saving_at_once_leave_one_winner` still fails loudly if the
    // child never writes its ready flag.
    let Ok(scratch) = std::env::var(CHILD_DIR_ENV) else {
        eprintln!(
            "child_writer is the worker half of \
             two_processes_saving_at_once_leave_one_winner and does nothing when run directly"
        );
        return;
    };
    let base: u64 = std::env::var(CHILD_BASE_ENV)
        .expect("base id")
        .parse()
        .expect("base id is a number");
    let scratch = PathBuf::from(scratch);
    let idx_dir = scratch.join("idx");
    let rdv = scratch.join("rdv");

    let mut idx = VectorIndex::load(&idx_dir, "L", "p")
        .expect("child load")
        .expect("seed index must exist");
    for i in 0..3 {
        idx.insert(base + i, &unit((base + i) as usize));
    }

    // Announce readiness, then spin on the start flag so both children leave
    // the gate within microseconds of each other.
    std::fs::write(rdv.join(format!("ready_{base}")), b"1").expect("write ready flag");
    await_flag(&rdv.join("go"), "child");

    let code = match idx.save(&idx_dir, "L", "p") {
        Ok(()) => EXIT_SAVED,
        Err(e) if VectorIndex::is_lost_update(&e) => EXIT_LOST_UPDATE,
        Err(e) => {
            eprintln!(
                "child {base}: unexpected save error: {e} (kind {:?})",
                e.kind()
            );
            EXIT_OTHER_ERROR
        }
    };
    std::process::exit(code);
}

/// The production shape of this bug is two *processes* — a long-lived daemon
/// and a backfill script — not two threads.  Threads and processes exercise the
/// staging-path collision identically, but only separate processes show that
/// the exclusion mechanism is genuinely inter-process rather than a
/// process-local mutex that happens to pass a threaded test.
///
/// Hand-derivation, identical to the threaded case:
///
/// * the seed index holds node ids `0..64` → **64** vectors at generation 1;
/// * child A loads it and adds ids 100, 101, 102; child B adds 200, 201, 202;
///   each child therefore holds 64 + 3 = **67**;
/// * both block on a file flag the parent creates only after both report ready,
///   then call `save()`;
/// * exactly one child must exit `EXIT_SAVED` and the other `EXIT_LOST_UPDATE`
///   — never `EXIT_OTHER_ERROR`, which is what an `ENOENT` staging collision
///   produces;
/// * the file must then hold exactly **67** vectors, with the winner's three
///   ids present and the loser's three absent.
#[test]
fn two_processes_saving_at_once_leave_one_winner() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let idx_dir = scratch.path().join("idx");
    let rdv = scratch.path().join("rdv");
    std::fs::create_dir_all(&idx_dir).expect("mkdir idx");
    std::fs::create_dir_all(&rdv).expect("mkdir rdv");

    let mut seed = VectorIndex::new(DIMS, Metric::Cosine);
    for i in 0u64..64 {
        seed.insert(i, &unit(i as usize));
    }
    assert_eq!(seed.len(), 64, "64 distinct ids were inserted");
    seed.save(&idx_dir, "L", "p").expect("seed save");

    let exe = std::env::current_exe().expect("current_exe");
    let bases = [100u64, 200];
    let children: Vec<std::process::Child> = bases
        .iter()
        .map(|base| {
            std::process::Command::new(&exe)
                .args(["child_writer", "--exact", "--ignored", "--nocapture"])
                .env(CHILD_DIR_ENV, scratch.path())
                .env(CHILD_BASE_ENV, base.to_string())
                .spawn()
                .expect("spawn child writer")
        })
        .collect();

    for base in bases {
        await_flag(&rdv.join(format!("ready_{base}")), "parent");
    }
    std::fs::write(rdv.join("go"), b"1").expect("write go flag");

    let codes: Vec<i32> = children
        .into_iter()
        .map(|mut c| {
            c.wait()
                .expect("child must exit")
                .code()
                .expect("child must not be killed by a signal")
        })
        .collect();

    assert_eq!(
        codes.iter().filter(|&&c| c == EXIT_OTHER_ERROR).count(),
        0,
        "a save was refused with an error the lost-update contract cannot act on \
         (see the child's stderr above); exit codes: {codes:?}"
    );
    assert_eq!(
        codes.iter().filter(|&&c| c == EXIT_SAVED).count(),
        1,
        "exactly one of two overlapping processes may land its save; exit codes: {codes:?} \
         ({EXIT_SAVED} = saved, {EXIT_LOST_UPDATE} = refused, {EXIT_OTHER_ERROR} = other error)"
    );
    assert_eq!(
        codes.iter().filter(|&&c| c == EXIT_LOST_UPDATE).count(),
        1,
        "the process that lost must be told it lost; exit codes: {codes:?}"
    );

    let on_disk = VectorIndex::load(&idx_dir, "L", "p")
        .expect("the winner's index must still load")
        .expect("index file must exist");
    assert_eq!(
        on_disk.len(),
        67,
        "64 seeded ids plus the winning process's 3 = 67"
    );
    let (present, absent): ([u64; 3], [u64; 3]) = if codes[0] == EXIT_SAVED {
        ([100, 101, 102], [200, 201, 202])
    } else {
        ([200, 201, 202], [100, 101, 102])
    };
    for id in present {
        assert!(
            on_disk.has_vector(id),
            "the winner's id {id} must be on disk"
        );
    }
    for id in absent {
        assert!(
            !on_disk.has_vector(id),
            "id {id} belongs to the refused process and must not be on disk"
        );
    }
    assert!(
        staging_debris(&idx_dir, "L", "p").is_empty(),
        "staging files outlived the processes: {:?}",
        staging_debris(&idx_dir, "L", "p")
    );
}

// ── Defect 2: the documented recovery contract must actually converge ────────

/// Number of concurrent writers in the retry test.
const WRITERS: u64 = 3;
/// Vectors each writer adds, one `save()` per vector.
const PER_WRITER: u64 = 10;
/// Cap on retries for a single vector, so a livelock fails the test instead of
/// hanging CI.  Far above anything three writers should need: each accepted
/// save advances the generation exactly once, so a writer can only be forced to
/// retry once per *other* writer's success, i.e. at most
/// `(WRITERS - 1) * PER_WRITER = 20` times in the worst interleaving.
const MAX_ATTEMPTS: usize = 200;

/// Three writers each add ten vectors, one `save()` per vector, following the
/// contract `is_lost_update()` documents: on refusal, reload from disk and
/// retry.  Nothing may be lost.
///
/// This is the shape of the backfill in the incident behind #441 — every
/// `insert` is followed by a `save`, so a thousand-vector backfill opens the
/// read-modify-write window a thousand times.
///
/// Hand-derivation:
///
/// * the seed index holds one vector, node id 90000 → **1** vector on disk;
/// * writer `t` (t = 0, 1, 2) writes ids `1000·t + 0 … 1000·t + 9`, i.e.
///   0…9, 1000…1009 and 2000…2009 — thirty ids, all distinct from one another
///   and from 90000, by construction;
/// * every `save()` either lands or is refused; a refusal reloads the current
///   file, re-applies that writer's one `insert`, and saves again, so no
///   accepted image can be missing an earlier accepted id;
/// * therefore the file must end with exactly 1 + 3 × 10 = **31** vectors, and
///   every one of the thirty-one ids must be individually present.
///
/// Against the pre-fix code both failure modes show up here: a colliding
/// staging path surfaces as an `ENOENT` the retry loop refuses to swallow, and
/// a lost update surfaces as a final count below 31.
#[test]
fn concurrent_writers_that_retry_on_refusal_lose_no_vectors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    let mut seed = VectorIndex::new(DIMS, Metric::Cosine);
    seed.insert(90000, &unit(0));
    seed.save(&path, "L", "p").expect("seed save");

    let barrier = Arc::new(Barrier::new(WRITERS as usize));
    let handles: Vec<_> = (0..WRITERS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for k in 0..PER_WRITER {
                    let id = 1000 * t + k;
                    let mut attempts = 0usize;
                    loop {
                        attempts += 1;
                        assert!(
                            attempts <= MAX_ATTEMPTS,
                            "writer {t} could not place id {id} in {MAX_ATTEMPTS} attempts"
                        );
                        let mut idx = VectorIndex::load(&path, "L", "p")
                            .expect("reload must succeed")
                            .expect("the index file must exist");
                        idx.insert(id, &unit(id as usize));
                        match idx.save(&path, "L", "p") {
                            Ok(()) => break,
                            Err(e) if VectorIndex::is_lost_update(&e) => continue,
                            Err(e) => panic!(
                                "writer {t}, id {id}: save failed with something the documented \
                                 recovery contract cannot act on — a caller sees this as a broken \
                                 disk and stops retrying, losing the vector: {e} (kind {:?})",
                                e.kind()
                            ),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("writer thread must not panic");
    }

    let on_disk = VectorIndex::load(&path, "L", "p")
        .expect("the final index must load")
        .expect("index file must exist");
    assert_eq!(
        on_disk.len(),
        31,
        "1 seeded vector + 3 writers × 10 vectors = 31; a smaller number means an accepted save \
         discarded another writer's work"
    );

    let expected: BTreeSet<u64> = std::iter::once(90000)
        .chain((0..WRITERS).flat_map(|t| (0..PER_WRITER).map(move |k| 1000 * t + k)))
        .collect();
    assert_eq!(
        expected.len(),
        31,
        "the fixture must define 31 distinct ids"
    );
    let missing: Vec<u64> = expected
        .iter()
        .copied()
        .filter(|&id| !on_disk.has_vector(id))
        .collect();
    assert!(
        missing.is_empty(),
        "these ids were written and acknowledged but are not on disk: {missing:?}"
    );

    assert!(
        staging_debris(&path, "L", "p").is_empty(),
        "staging files outlived the writers: {:?}",
        staging_debris(&path, "L", "p")
    );
    assert!(
        index_file(&path, "L", "p").exists(),
        "the index file itself must exist at the end"
    );
}
