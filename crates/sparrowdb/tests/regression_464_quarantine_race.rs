//! Regression guard for issue #464 — `quarantine()` can silently destroy a
//! concurrent, successful repair.
//!
//! `VectorIndex::load_and_quarantine` (the open path's loader) judges a file
//! with one unlocked read, and — before #464's fix — moved it aside with a
//! bare `std::fs::rename`, taking neither a lock nor a second look. Between
//! the judgement and the rename, another process can complete a repairing
//! `save()`. The rename then fires on whatever is at `path` *now*, not on
//! the bytes that were judged — so it moves the newly repaired, healthy
//! index into quarantine instead of the corrupt bytes it was supposed to
//! preserve. Net effect: the repair is silently undone, the pair reads as
//! absent, and the file sitting in `*.corrupt.*` — the only place an
//! operator would look for an explanation — is a perfectly healthy index.
//!
//! The fix (`VectorIndex::quarantine_after_recheck`) takes the same
//! `SaveLock` `save()` uses and re-runs the judgement itself once the lock is
//! held, since `save()` cannot be mid-write past that point. If the judgement
//! comes back `Loaded` — the repair landed in the window — that index is
//! returned exactly as if the repair had landed a moment earlier; nothing is
//! renamed.
//!
//! # Why this test needs a synchronisation hook
//!
//! The window between the unlocked read and the (pre-fix) rename is a
//! handful of CPU instructions with no syscall in between — sub-microsecond.
//! A concurrent `save()`, which has to complete a serialise, a write, two
//! `fsync`s and a rename, cannot land inside that window by scheduling luck;
//! relying on luck would make this test flaky at best and, at worst, quietly
//! never hit the window and pass against the unfixed code for the wrong
//! reason — the exact failure mode `regression_406.rs` shipped with. Instead,
//! this drives `VectorIndex::test_pause_before_quarantine_lock` (present
//! only under `debug_assertions`, inert unless
//! `SPARROWDB_TEST_QUARANTINE_PAUSE_DIR` is also set — see its doc comment in
//! `vector_index.rs`) to hold the opener process open at exactly the point
//! issue #464 named: immediately before it would acquire `SaveLock`, i.e.
//! between the unlocked judgement and the quarantine.
//!
//! Because the hook is compiled out under `#[cfg(not(debug_assertions))]`,
//! **this test does not exercise anything under `cargo test --release`**: the
//! opener never pauses, so it either quarantines before the repairer can act
//! or the parent times out waiting for a pause that will never come. That is
//! expected, not a bug in the test — see the hook's doc comment in
//! `vector_index.rs` for why leaving it compiled into release builds is the
//! actual risk being traded away.
//!
//! # The interleaving this forces, across two real processes
//!
//! | step | opener (child A) | repairer (child B) |
//! |---|---|---|
//! | 1 | | loads the index while it is still healthy |
//! | 2 | | (parent corrupts a payload byte, header intact) |
//! | 3 | | inserts one more id in memory (the "fix") |
//! | 4 | calls `load_and_quarantine`; unlocked read sees the corrupt bytes | waits |
//! | 5 | pauses immediately before `SaveLock::acquire` | waits |
//! | 6 | (paused) | `save()`: acquires the lock, writes, renames, releases |
//! | 7 | resumes: acquires the now-free lock, re-reads, sees the repaired file | (exited) |
//!
//! Every expected value below is derived by hand from the fixture this test
//! builds, not recorded from a run of the code.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use sparrowdb::GraphDb;
use sparrowdb_storage::vector_index::{Metric, VectorIndex};

// ── Fixture helpers ────────────────────────────────────────────────────────────

/// Vector width. Small and arbitrary; the assertions are about which ids
/// survive, not about distances.
const DIMS: usize = 4;

/// Number of ids the seed index starts with, before either child runs.
const SEED_COUNT: u64 = 5;

/// The id the repairer inserts. Distinct from every seed id (`0..SEED_COUNT`)
/// by construction, so its presence on disk afterwards can only be explained
/// by the repairer's `save()` having survived.
const REPAIR_ID: u64 = 999;

/// Byte length of the v2 header (`HNSW_HEADER_LEN`, private to
/// `vector_index.rs`): magic(8) + version(4) + reserved(4) + payload_len(8) +
/// generation(8) + crc32c(4) = 36. Duplicated here rather than exported,
/// matching the existing convention in `vector_index_durability.rs`.
const V2_HEADER_LEN: usize = 36;

fn unit(hot: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIMS];
    v[hot % DIMS] = 1.0;
    v
}

/// Path of the on-disk index file for `(label, prop)` under `<idx_dir>`.
fn index_file(idx_dir: &Path, label: &str, prop: &str) -> PathBuf {
    idx_dir.join(format!("hnsw_{label}_{prop}.bin"))
}

/// `<scratch>/db_root/vector_indexes` — the directory `VectorIndex::save` /
/// `load` / `load_and_quarantine` take as `dir`, and the child of `db_root`
/// that `GraphDb::vector_index_load_failures` scans.
fn idx_dir_of(scratch: &Path) -> PathBuf {
    scratch.join("db_root").join("vector_indexes")
}

/// Deadline for every cross-process rendezvous below. Generous — it only has
/// to cover process startup on a loaded CI box; a miss fails the test rather
/// than hanging it.
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(60);

/// Spin until `flag` appears, or fail. Mirrors `regression_452.rs`'s
/// `await_flag`.
fn await_flag(flag: &Path, who: &str) {
    let deadline = Instant::now() + RENDEZVOUS_TIMEOUT;
    while !flag.exists() {
        assert!(
            Instant::now() < deadline,
            "{who}: {} never appeared within {RENDEZVOUS_TIMEOUT:?}",
            flag.display()
        );
        std::thread::yield_now();
    }
}

// ── Repairer (child B) ────────────────────────────────────────────────────────

/// Set on the repairer child; its value is the scratch directory shared with
/// the parent and the opener.
const REPAIR_SCRATCH_ENV: &str = "SPARROWDB_464_REPAIR_SCRATCH";

const REPAIR_EXIT_SAVED: i32 = 30;
const REPAIR_EXIT_LOST_UPDATE: i32 = 31;
const REPAIR_EXIT_OTHER_ERROR: i32 = 32;

/// Worker half of `quarantine_recheck_survives_a_concurrent_repair`, playing
/// process B: loads the index while it is healthy, waits for the parent to
/// corrupt it, applies one in-memory edit (the "repair"), then waits to be
/// told to save.
///
/// Inert unless `REPAIR_SCRATCH_ENV` is set, and `#[ignore]`d so an ordinary
/// `cargo test` run never invokes it directly.
#[test]
#[ignore = "worker process for quarantine_recheck_survives_a_concurrent_repair"]
fn child_repairer() {
    // Return rather than panic: `cargo test -- --ignored` is a standard way
    // to sweep every ignored test, and in that run the parent never sets
    // REPAIR_SCRATCH_ENV. Panicking there would fail the job even though
    // nothing is wrong (see #453 review history for this exact rule).
    let Ok(scratch) = std::env::var(REPAIR_SCRATCH_ENV) else {
        eprintln!(
            "child_repairer is the repairer half of \
             quarantine_recheck_survives_a_concurrent_repair and does nothing when run directly"
        );
        return;
    };
    let scratch = PathBuf::from(scratch);
    let idx_dir = idx_dir_of(&scratch);
    let rdv = scratch.join("rdv");

    // Loaded while the file is still healthy: this handle's disk_generation
    // matches what is on disk, so its later save() is not a lost update.
    let mut idx = VectorIndex::load(&idx_dir, "L", "p")
        .expect("repairer load")
        .expect("seed index must exist and be healthy at this point");

    std::fs::write(rdv.join("b_loaded"), b"1").expect("write b_loaded flag");
    await_flag(&rdv.join("corrupt_done"), "repairer");

    // The "repair": one more id that was not in the seed.
    idx.insert(REPAIR_ID, &unit(REPAIR_ID as usize));

    await_flag(&rdv.join("go_b"), "repairer");

    let code = match idx.save(&idx_dir, "L", "p") {
        Ok(()) => REPAIR_EXIT_SAVED,
        Err(e) if VectorIndex::is_lost_update(&e) => REPAIR_EXIT_LOST_UPDATE,
        Err(e) => {
            eprintln!("repairer: unexpected save error: {e} (kind {:?})", e.kind());
            REPAIR_EXIT_OTHER_ERROR
        }
    };
    std::process::exit(code);
}

// ── Opener (child A) ──────────────────────────────────────────────────────────

/// Set on the opener child; its value is the scratch directory shared with
/// the parent and the repairer.
const OPENER_SCRATCH_ENV: &str = "SPARROWDB_464_OPENER_SCRATCH";

/// Must match the env var name `VectorIndex::test_pause_before_quarantine_lock`
/// reads in `crates/sparrowdb-storage/src/vector_index.rs`.
const PAUSE_DIR_ENV: &str = "SPARROWDB_TEST_QUARANTINE_PAUSE_DIR";

/// Worker half of `quarantine_recheck_survives_a_concurrent_repair`, playing
/// process A: calls `load_and_quarantine` on the (at that moment) corrupt
/// file. Its unlocked read judges it `Undecodable`, then
/// `quarantine_after_recheck` pauses — via `PAUSE_DIR_ENV` — immediately
/// before acquiring `SaveLock`, giving the parent a deterministic window in
/// which to let the repairer finish.
///
/// Inert unless `OPENER_SCRATCH_ENV` is set, and `#[ignore]`d so an ordinary
/// `cargo test` run never invokes it directly.
#[test]
#[ignore = "worker process for quarantine_recheck_survives_a_concurrent_repair"]
fn child_opener() {
    let Ok(scratch) = std::env::var(OPENER_SCRATCH_ENV) else {
        eprintln!(
            "child_opener is the opener half of \
             quarantine_recheck_survives_a_concurrent_repair and does nothing when run directly"
        );
        return;
    };
    let scratch = PathBuf::from(scratch);
    let idx_dir = idx_dir_of(&scratch);
    let rdv = scratch.join("rdv");

    let line = match VectorIndex::load_and_quarantine(&idx_dir, "L", "p") {
        Ok(Some(idx)) => format!("LOADED {} {}", idx.len(), idx.has_vector(REPAIR_ID)),
        Ok(None) => "ABSENT".to_owned(),
        Err(e) => format!("ERROR {e}"),
    };
    std::fs::write(rdv.join("a_result"), line).expect("write a_result");
    std::process::exit(0);
}

// ── The test ───────────────────────────────────────────────────────────────────

/// See the module doc comment for the full interleaving table and why a
/// synchronisation hook is necessary rather than optional here.
///
/// Hand-derivation:
///
/// * the seed index holds ids `0..SEED_COUNT` → **`SEED_COUNT`** vectors at
///   generation 1;
/// * the repairer loads that (healthy) file, so its handle also believes it
///   holds `SEED_COUNT` vectors at generation 1;
/// * the parent then flips one payload byte (past the header, so the header's
///   generation field — which `save()`'s lost-update check reads — still
///   reads 1) → the file is now undecodable, but `save()` from a handle that
///   loaded generation 1 is still not refused;
/// * the repairer inserts `REPAIR_ID`, giving it `SEED_COUNT + 1` vectors in
///   memory, then saves — the on-disk file becomes healthy again, at
///   generation 2, holding `SEED_COUNT + 1` vectors;
/// * the opener's `load_and_quarantine`, released only after the repairer's
///   save is confirmed to have landed, must therefore report exactly
///   `SEED_COUNT + 1` vectors including `REPAIR_ID` — not an error, not
///   `ABSENT`, and not `SEED_COUNT` (which would mean it saw a rolled-back or
///   re-corrupted file).
///
/// Against the unfixed `quarantine()` (5f36b02), forcing this same
/// interleaving destroys the repair: see the PR description for the observed
/// pre-fix result.
#[test]
fn quarantine_recheck_survives_a_concurrent_repair() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let scratch_path = scratch.path().to_path_buf();
    let idx_dir = idx_dir_of(&scratch_path);
    let rdv = scratch_path.join("rdv");
    let pause_dir = scratch_path.join("pause");
    std::fs::create_dir_all(&idx_dir).expect("mkdir idx_dir");
    std::fs::create_dir_all(&rdv).expect("mkdir rdv");
    std::fs::create_dir_all(&pause_dir).expect("mkdir pause_dir");

    let mut seed = VectorIndex::new(DIMS, Metric::Cosine);
    for i in 0..SEED_COUNT {
        seed.insert(i, &unit(i as usize));
    }
    assert_eq!(
        seed.len(),
        SEED_COUNT as usize,
        "fixture precondition: {SEED_COUNT} distinct seed ids were inserted"
    );
    seed.save(&idx_dir, "L", "p").expect("seed save");
    let healthy_bytes = std::fs::read(index_file(&idx_dir, "L", "p")).expect("read healthy bytes");
    assert!(
        healthy_bytes.len() > V2_HEADER_LEN + 4,
        "fixture precondition: the seed payload must be long enough to flip a byte past the \
         header"
    );

    let exe = std::env::current_exe().expect("current_exe");

    // 1) Start the repairer. It loads the (still healthy) file and then
    //    blocks on `corrupt_done`.
    let mut repairer = std::process::Command::new(&exe)
        .args(["child_repairer", "--exact", "--ignored", "--nocapture"])
        .env(REPAIR_SCRATCH_ENV, &scratch_path)
        .spawn()
        .expect("spawn repairer");
    await_flag(&rdv.join("b_loaded"), "parent (waiting for repairer load)");

    // 2) Corrupt the file now that the repairer's in-memory copy is safe.
    //    Flip a payload byte past the header, exactly as
    //    `rejected_index_file_is_quarantined_rather_than_left_to_be_overwritten`
    //    in vector_index_durability.rs does, so the header's generation field
    //    survives and the repairer's later save() is not refused as a lost
    //    update.
    let path = index_file(&idx_dir, "L", "p");
    let mut damaged = healthy_bytes.clone();
    let victim = (V2_HEADER_LEN + 4).min(damaged.len() - 1);
    damaged[victim] ^= 0xFF;
    std::fs::write(&path, &damaged).expect("write damaged bytes");
    std::fs::write(rdv.join("corrupt_done"), b"1").expect("signal corrupt_done");
    // The repairer now applies its in-memory edit and blocks on `go_b`.

    // 3) Start the opener. Its unlocked read observes the corrupt file
    //    written in step 2, judges it Undecodable, and pauses immediately
    //    before acquiring SaveLock.
    let mut opener = std::process::Command::new(&exe)
        .args(["child_opener", "--exact", "--ignored", "--nocapture"])
        .env(OPENER_SCRATCH_ENV, &scratch_path)
        .env(PAUSE_DIR_ENV, &pause_dir)
        .spawn()
        .expect("spawn opener");
    await_flag(
        &pause_dir.join("paused"),
        "parent (waiting for opener to pause)",
    );

    // 4) The opener is now confirmed blocked before it holds (or has even
    //    attempted to acquire) any lock. Release the repairer to complete a
    //    full save() — acquire SaveLock, write, fsync, rename, release —
    //    entirely while the opener cannot observe or race any part of it.
    std::fs::write(rdv.join("go_b"), b"1").expect("signal go_b");
    let repair_status = repairer.wait().expect("repairer must exit");
    let repair_code = repair_status
        .code()
        .expect("repairer must not be killed by a signal");
    assert_eq!(
        repair_code, REPAIR_EXIT_SAVED,
        "the repairer's save() must land cleanly while the opener is paused and holds no lock \
         (exit codes: {REPAIR_EXIT_SAVED} = saved, {REPAIR_EXIT_LOST_UPDATE} = lost update, \
         {REPAIR_EXIT_OTHER_ERROR} = other error); got {repair_code}"
    );

    // 5) The repair is now durably on disk, generation 2. Release the opener
    //    to resume inside `quarantine_after_recheck` and attempt the
    //    quarantine it paused before.
    std::fs::write(pause_dir.join("resume"), b"1").expect("signal resume");
    let opener_status = opener.wait().expect("opener must exit");
    assert!(
        opener_status.success(),
        "opener process must exit 0, got {opener_status:?}"
    );

    let result = std::fs::read_to_string(rdv.join("a_result")).expect("read opener result");
    let expected_len = SEED_COUNT + 1;
    assert_eq!(
        result.trim(),
        format!("LOADED {expected_len} true"),
        "the opener must see the repaired index (LOADED <len> <has REPAIR_ID>), not an error \
         and not a smaller or absent one; got: {result:?}"
    );

    // The good, repaired index must still be exactly where it was.
    assert!(
        path.is_file(),
        "the repaired index must still be at {}",
        path.display()
    );
    let on_disk = VectorIndex::load(&idx_dir, "L", "p")
        .expect("the repaired index must still load")
        .expect("index file must exist");
    assert_eq!(on_disk.len(), expected_len as usize);
    for i in 0..SEED_COUNT {
        assert!(on_disk.has_vector(i), "seeded id {i} must survive");
    }
    assert!(
        on_disk.has_vector(REPAIR_ID),
        "the repairer's id must survive"
    );

    // No `.corrupt.` artifact anywhere: the good file was never renamed.
    let quarantine_artifacts: Vec<PathBuf> = std::fs::read_dir(&idx_dir)
        .expect("read_dir idx_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".corrupt."))
        })
        .collect();
    assert!(
        quarantine_artifacts.is_empty(),
        "the good, repaired index must never be renamed aside; found {quarantine_artifacts:?}"
    );

    // The diagnostic must report healthy: no unscannable directory, no
    // active (unrecovered) damage.
    let health = GraphDb::vector_index_load_failures(&scratch_path.join("db_root"));
    assert!(
        health.is_healthy(),
        "vector_index_load_failures must report healthy once the repair has survived; got \
         {health:?}"
    );
}
