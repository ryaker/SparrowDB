//! Regression tests for issue #524 — two processes writing concurrently to
//! one database root permanently corrupt `catalog.tlv`, and the database
//! then cannot be opened at all:
//!
//! ```text
//! Error: corruption: duplicate label_id 0 in catalog file
//! ```
//!
//! ## Root cause
//!
//! `GraphDb::open` derives `next_label_id` (and other catalog counters) from
//! its own in-memory state, with no coordination across handles. Two
//! processes each opening the same root, each `CREATE`ing a node of a label
//! the catalog does not yet have, both allocate `label_id` 0 and both write
//! it to `catalog.tlv` — confirmed against pre-fix `origin/main` (4d15340)
//! using the CLI's `query` subcommand as two racing processes, mirroring the
//! Node.js repro in the issue:
//!
//! ```text
//! run 1: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 2: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 3: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 4: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 5: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 6: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 7: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! run 8: BRICKED -> Error: corruption: duplicate label_id 0 in catalog file
//! ```
//!
//! (8/8 here rather than the issue's 4/5 — likely because the CLI pays
//! process-startup cost before `open()`, which widens the race window
//! relative to the in-process Node repro.)
//!
//! ## The fix
//!
//! `GraphDb::open`/`open_encrypted` now take an exclusive `flock` on
//! `<db_root>/db.lock` (`crate::process_lock::ProcessLock`) before touching
//! anything else. A second *process* racing to open the same root gets an
//! immediate `Error::DatabaseLocked` instead of a chance to corrupt the
//! catalog; released automatically — by the kernel, on fd close — when the
//! holder's handle drops, including on crash or `SIGKILL`.
//!
//! ## What these tests prove, across real OS processes
//!
//! * [`concurrent_open_one_wins_one_gets_a_clean_error`]: two processes
//!   racing `GraphDb::open` + `CREATE` against a fresh root never both get
//!   in — exactly one succeeds, the other gets `Error::DatabaseLocked`, and
//!   a subsequent open sees exactly the winner's data with no corruption.
//! * [`lock_is_released_when_holder_is_sigkilled`]: a second open is refused
//!   while a holder is alive, and succeeds immediately after the holder is
//!   `SIGKILL`ed — proving release does not depend on the holder's `Drop`
//!   impl running.
//! * [`sequential_open_write_close_reopen_still_works`]: the ordinary
//!   single-process open/write/close/reopen pattern — what real callers
//!   actually do — is unaffected.

use sparrowdb::GraphDb;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Deadline for every cross-process rendezvous below. Generous — it only has
/// to cover process startup on a loaded CI box; a miss fails the test rather
/// than hanging it. Mirrors `regression_464_quarantine_race.rs`.
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(60);

/// Spin until `flag` appears, or fail. Mirrors `regression_464_quarantine_race.rs`'s
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

// ── Test 1: two processes racing GraphDb::open ──────────────────────────────

/// Set on each racing child; its value is `<scratch>/db` (shared) joined
/// with the child's own result-file name to write to.
const RACE_DB_ENV: &str = "SPARROWDB_524_RACE_DB";
const RACE_LABEL_ENV: &str = "SPARROWDB_524_RACE_LABEL";
const RACE_RESULT_ENV: &str = "SPARROWDB_524_RACE_RESULT_FILE";

/// One racing child: open the shared root and `CREATE` one node carrying its
/// own label, then record the outcome and exit. Never panics on a failed
/// open — `Error::DatabaseLocked` from the loser is the expected, correct
/// outcome for one of the two children, not a test failure by itself.
///
/// Inert unless `RACE_DB_ENV` is set, and `#[ignore]`d so an ordinary
/// `cargo test` run never invokes it directly (see #464's identical
/// convention).
#[test]
#[ignore = "worker process for concurrent_open_one_wins_one_gets_a_clean_error"]
fn child_524_race_writer() {
    let Ok(db_path) = std::env::var(RACE_DB_ENV) else {
        eprintln!(
            "child_524_race_writer is a worker for \
             concurrent_open_one_wins_one_gets_a_clean_error and does nothing when run directly"
        );
        return;
    };
    let label = std::env::var(RACE_LABEL_ENV).expect("RACE_LABEL_ENV must be set for the child");
    let result_file =
        PathBuf::from(std::env::var(RACE_RESULT_ENV).expect("RACE_RESULT_ENV must be set"));

    let outcome = match GraphDb::open(Path::new(&db_path)) {
        Ok(db) => {
            let cypher = format!("CREATE (n:{label} {{id: '{label}'}})");
            match db.execute(&cypher) {
                Ok(_) => "OPENED_AND_WROTE".to_string(),
                Err(e) => format!("OPENED_BUT_WRITE_FAILED: {e}"),
            }
        }
        Err(e) => format!("OPEN_FAILED: {e}"),
    };
    std::fs::write(&result_file, outcome).expect("write result file");
}

/// Hand-derivation: whichever child's `open()` wins the `flock`, its
/// `CREATE` is the only mutation ever applied to this root, so after both
/// children exit the root must contain **exactly one** node, whose `id`
/// equals the winner's label — not both (that would be the pre-fix
/// duplicate-`label_id` corruption avoided rather than reproduced) and not
/// neither (that would mean the winner's write was lost).
#[test]
fn concurrent_open_one_wins_one_gets_a_clean_error() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let db_path = scratch.path().join("db");
    let result_alpha = scratch.path().join("result_alpha.txt");
    let result_beta = scratch.path().join("result_beta.txt");

    let exe = std::env::current_exe().expect("current_exe");

    // Spawn both children back-to-back with no rendezvous between them —
    // unlike #464's forced microsecond window, this fix's correctness does
    // not depend on hitting a narrow interleaving: *no* interleaving of two
    // concurrent opens may corrupt the catalog or let both in. Spawning
    // back-to-back is what produced the 8/8 collision rate quoted in the
    // module doc against the unfixed code, so it reliably exercises the
    // race without extra synchronisation machinery.
    let mut alpha = std::process::Command::new(&exe)
        .args([
            "child_524_race_writer",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(RACE_DB_ENV, &db_path)
        .env(RACE_LABEL_ENV, "Alpha")
        .env(RACE_RESULT_ENV, &result_alpha)
        .spawn()
        .expect("spawn child alpha");
    let mut beta = std::process::Command::new(&exe)
        .args([
            "child_524_race_writer",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(RACE_DB_ENV, &db_path)
        .env(RACE_LABEL_ENV, "Beta")
        .env(RACE_RESULT_ENV, &result_beta)
        .spawn()
        .expect("spawn child beta");

    let status_alpha = alpha.wait().expect("alpha must exit");
    let status_beta = beta.wait().expect("beta must exit");
    assert!(
        status_alpha.success(),
        "alpha child process itself must not crash"
    );
    assert!(
        status_beta.success(),
        "beta child process itself must not crash"
    );

    let outcome_alpha = std::fs::read_to_string(&result_alpha).expect("read alpha result");
    let outcome_beta = std::fs::read_to_string(&result_beta).expect("read beta result");

    let alpha_won = outcome_alpha == "OPENED_AND_WROTE";
    let beta_won = outcome_beta == "OPENED_AND_WROTE";
    assert!(
        alpha_won ^ beta_won,
        "exactly one racer must win the lock and write; got alpha={outcome_alpha:?} beta={outcome_beta:?}"
    );

    let loser_outcome = if alpha_won {
        &outcome_beta
    } else {
        &outcome_alpha
    };
    assert!(
        loser_outcome.starts_with("OPEN_FAILED:"),
        "the loser must fail at open(), not at the write, got: {loser_outcome}"
    );
    assert!(
        loser_outcome.contains("database locked"),
        "the loser's error must name the real cause (Error::DatabaseLocked's Display), got: {loser_outcome}"
    );

    // No corruption: a fresh open succeeds and sees exactly the winner's row.
    let db = GraphDb::open(&db_path).expect("reopen after the race must succeed cleanly");
    let result = db
        .execute("MATCH (n) RETURN n.id")
        .expect("query after the race must succeed — no catalog corruption");
    let winner_label = if alpha_won { "Alpha" } else { "Beta" };
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one node must exist after the race, got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0],
        vec![sparrowdb_execution::types::Value::String(
            winner_label.to_string()
        )],
        "the surviving node must be the winner's, not the loser's or a corrupted mix"
    );
}

// ── Test 2: lock is released on SIGKILL, not just on graceful Drop ─────────

const HOLD_DB_ENV: &str = "SPARROWDB_524_HOLD_DB";
const HOLD_READY_FLAG_ENV: &str = "SPARROWDB_524_HOLD_READY_FLAG";

/// Opens the shared root, signals readiness, then sleeps far longer than
/// this test ever waits — it is expected to be `SIGKILL`ed, never to exit on
/// its own. Inert unless `HOLD_DB_ENV` is set (see #464's identical
/// convention for why: an `--ignored` sweep must not fail on this).
#[test]
#[ignore = "worker process for lock_is_released_when_holder_is_sigkilled"]
fn child_524_lock_holder() {
    let Ok(db_path) = std::env::var(HOLD_DB_ENV) else {
        eprintln!(
            "child_524_lock_holder is a worker for \
             lock_is_released_when_holder_is_sigkilled and does nothing when run directly"
        );
        return;
    };
    let ready_flag = PathBuf::from(std::env::var(HOLD_READY_FLAG_ENV).expect("READY_FLAG_ENV"));

    let _db = GraphDb::open(Path::new(&db_path)).expect("holder open");
    std::fs::write(&ready_flag, b"1").expect("signal ready");
    // Never returns on its own; the parent SIGKILLs this process.
    std::thread::sleep(Duration::from_secs(300));
}

/// Kills a spawned child on drop, including on an unwinding panic.
///
/// Without this, a child that outlives its intended lifetime — e.g. the
/// SIGKILL test's holder, still sleeping if an assertion panics before this
/// test reaches its own `kill()` call — keeps a duplicate of this test
/// process's captured stdout/stderr pipe open. `cargo test`'s harness then
/// blocks forever reading for EOF on that pipe waiting to report the
/// (already-decided) failure: a hang masquerading as a slow test, not the
/// clean failure the assertion actually produced. Piping the child's own
/// stdio to `/dev/null` (see `spawn_holder` below) already prevents that
/// specific pipe-inheritance hang; this guard is the second, independent
/// reason not to leak the process itself — a stray sleeper left running
/// after `cargo test` exits, one per flaky rerun.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Proves release does not depend on `ProcessLock`'s `Drop` impl running —
/// `SIGKILL` skips destructors entirely, so if this passes, release comes
/// from the kernel closing the file descriptor, exactly as the module docs
/// on `ProcessLock` claim.
#[test]
fn lock_is_released_when_holder_is_sigkilled() {
    let scratch = tempfile::tempdir().expect("tempdir");
    let db_path = scratch.path().join("db");
    let ready_flag = scratch.path().join("holder_ready");

    let exe = std::env::current_exe().expect("current_exe");
    let mut holder = KillOnDrop(
        std::process::Command::new(&exe)
            .args([
                "child_524_lock_holder",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env(HOLD_DB_ENV, &db_path)
            .env(HOLD_READY_FLAG_ENV, &ready_flag)
            // Not inherited: see `KillOnDrop`'s doc comment for the hang
            // this avoids if an assertion below panics before this test
            // reaches its own explicit kill.
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn holder"),
    );

    await_flag(&ready_flag, "parent (waiting for holder to open)");

    // While the holder is alive, a second open from this (different)
    // process must be refused cleanly.
    let err = GraphDb::open(&db_path)
        .err()
        .expect("open must fail while another process holds the lock");
    assert!(
        err.to_string().contains("database locked"),
        "must be the actionable DatabaseLocked error, got: {err}"
    );

    // SIGKILL the holder — no graceful shutdown, no Drop impls run in it.
    holder.0.kill().expect("SIGKILL holder");
    let status = holder.0.wait().expect("reap killed holder");
    assert!(
        !status.success(),
        "a SIGKILLed process must not report success"
    );

    // The kernel released the flock the instant the fd closed. This must
    // succeed now, not require a wait — the point of `flock` over a PID
    // file is precisely that there is nothing to time out or reclaim.
    let db =
        GraphDb::open(&db_path).expect("open must succeed immediately after the holder is killed");
    db.execute("CREATE (n:PostKill {id: 'ok'})")
        .expect("db must be fully usable, not just openable, after reclaiming the lock");
}

// ── Test 3: the ordinary single-process case is unaffected ─────────────────

/// The pattern real callers actually use: open, write, close, reopen. No
/// subprocess needed — this exercises `ProcessLock`'s release-on-`Drop`
/// path, the counterpart to the SIGKILL test's release-without-`Drop` path.
#[test]
fn sequential_open_write_close_reopen_still_works() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("db");

    {
        let db = GraphDb::open(&db_path).expect("first open");
        db.execute("CREATE (n:Seq {id: 'x'})").expect("create");
        // db dropped here — releases the lock.
    }

    let db2 = GraphDb::open(&db_path).expect("reopen after close must succeed");
    let result = db2
        .execute("MATCH (n:Seq) RETURN n.id")
        .expect("query after reopen");
    assert_eq!(
        result.rows,
        vec![vec![sparrowdb_execution::types::Value::String(
            "x".to_string()
        )]],
        "sequential open/write/close/reopen must see the earlier write, unaffected by the lock"
    );
}
