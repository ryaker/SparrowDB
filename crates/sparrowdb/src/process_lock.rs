// ── Cross-process database lock (#524) ───────────────────────────────────────
//
// `GraphDb::open` used to place no lock at all on the database root. Two
// processes opening the same root each derive counters like `next_label_id`
// from their own in-memory state (see the comment on `WriteGuard` usage in
// `db.rs`), so concurrent catalog writes from two independent handles can
// assign the same id twice. The result isn't a wrong answer — it is
// `catalog.tlv` corrupted beyond what `open()` will even parse again
// ("corruption: duplicate label_id 0 in catalog file"), for every future
// process, including the ones that caused it. `ProcessLock` closes that at
// the door: a second `open()` against a root some *other process* already
// holds fails immediately and cleanly instead of racing.

use sparrowdb_common::{Error, Result};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// Name of the lock file created at the database root.
const LOCK_FILE_NAME: &str = "db.lock";

/// The held `flock`, released when the last [`ProcessLock`] pointing at it
/// (and thus the last `Arc` below) drops.
struct LockedFile(File);

impl Drop for LockedFile {
    fn drop(&mut self) {
        // Closing the descriptor releases the lock on its own — including on
        // panic unwind and process kill, which is the whole point (see the
        // module doc). Unlocking first just keeps the release explicit and
        // independent of exactly when the `File` is actually dropped.
        let _ = self.0.unlock();
    }
}

/// Live in-process locks, keyed by canonicalized database root.
///
/// [`std::fs::File::try_lock`] is `flock(2)` on Unix, scoped to the *open
/// file description* — not the process. Two threads (or two sequential
/// `GraphDb::open` calls) in one process that each open the lock file fresh
/// would therefore exclude each other exactly as two separate OS processes
/// do. That is correct for the bug this module fixes, but it is *not* what
/// several existing tests intentionally rely on: opening two live `GraphDb`
/// handles on one root from the same process to test how a stale in-process
/// handle behaves (e.g.
/// `vector_index_durability::a_stale_handle_cannot_silently_revert_...`).
/// #524 is about two *processes* racing an uncoordinated in-memory counter
/// — it does not intend to forbid two handles sharing one address space.
///
/// So the registry hands out the *same* `Arc<LockedFile>` to every
/// `ProcessLock::acquire` in this process for a given root, and only the
/// first caller for that root actually contends for the OS-level lock. A
/// second process, with its own empty registry, still calls `try_lock` on
/// the same file fresh and is refused by the kernel exactly as before.
fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<LockedFile>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<LockedFile>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A lock over one database root: exclusive across processes, shared within
/// this one. Held for the lifetime of the
/// [`DbInner`](crate::types::DbInner) that acquired it.
///
/// # Why `flock` and not a PID file
///
/// A PID file has to answer "what if the holder died?", and every answer is
/// a heuristic: a liveness probe races with PID reuse, an age threshold
/// either wedges the database after a crash or breaks the lock out from
/// under a slow-but-live writer — and a broken lock is exactly the
/// corruption this exists to prevent. An `flock`-style advisory lock is
/// released by the kernel when the file descriptor closes, including when
/// the process is `SIGKILL`ed, so a crashed holder leaves nothing to
/// reclaim and nothing stale to reason about. This mirrors `SaveLock` in
/// `sparrowdb_storage::vector_index`, which made the identical call for
/// per-index locking (#452).
///
/// # Why exclusive, not scoped to catalog mutation
///
/// An advisory lock scoped only to catalog-mutating calls would preserve
/// concurrent *readers*. But every language binding exposes only `open()`
/// with no read-only mode, so "concurrent readers" is not a case this gives
/// up — it is aspirational today regardless. Exclusive-at-`open()` now, and
/// a real read-only mode later, is the honest ordering. Decision recorded
/// on issue #524.
///
/// # Why non-blocking
///
/// `open()` uses `try_lock` rather than blocking `lock`: a second process
/// racing to open the same root should get an immediate, actionable
/// [`Error::DatabaseLocked`] it can report to an operator, not a hang with
/// no indication of what it is waiting for.
// Never read in normal operation — `LockedFile`'s presence (and its `Drop`
// impl) is the entire point. Read directly (via `.0`) only by the
// `Arc::ptr_eq` unit test above, which is why `dead_code` fires without this.
#[allow(dead_code)]
pub(crate) struct ProcessLock(Arc<LockedFile>);

impl ProcessLock {
    /// Path of the lock file for a database rooted at `db_path`.
    fn lock_path(db_path: &Path) -> PathBuf {
        db_path.join(LOCK_FILE_NAME)
    }

    /// Acquire the lock for the database rooted at `db_path`.
    ///
    /// `db_path` must already exist (`GraphDb::open` creates it before
    /// calling this). Returns [`Error::DatabaseLocked`] if another
    /// process's handle already holds it; succeeds and shares the existing
    /// lock if this process already holds it.
    pub(crate) fn acquire(db_path: &Path) -> Result<Self> {
        // Canonicalize so two spellings of one root (relative vs. absolute,
        // a symlinked temp dir) register as the same key within this
        // process. Falls back to the given path on failure rather than
        // block opening on a bookkeeping concern — `db_path` exists at this
        // point, so canonicalize only fails for genuinely exotic filesystem
        // issues, and the worst outcome of the fallback is this process not
        // sharing its own lock with itself under the alternate spelling,
        // which just means it contends with itself as if it were a second
        // process (safe, merely a spurious `DatabaseLocked`).
        let key = db_path
            .canonicalize()
            .unwrap_or_else(|_| db_path.to_path_buf());

        let mut reg = registry().lock().expect("process lock registry poisoned");
        // Sweep dead entries so this map doesn't grow without bound across a
        // long-running process that opens many different roots over its
        // lifetime (every test in this workspace's suite, for instance).
        reg.retain(|_, weak| weak.strong_count() > 0);

        if let Some(shared) = reg.get(&key).and_then(Weak::upgrade) {
            return Ok(ProcessLock(shared));
        }

        let path = Self::lock_path(db_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(Error::Io)?;
        match file.try_lock() {
            Ok(()) => {
                let locked = Arc::new(LockedFile(file));
                reg.insert(key, Arc::downgrade(&locked));
                Ok(ProcessLock(locked))
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                Err(Error::DatabaseLocked(db_path.display().to_string()))
            }
            Err(std::fs::TryLockError::Error(e)) => Err(Error::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_handle_in_this_process_shares_the_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = ProcessLock::acquire(dir.path()).expect("first acquire");
        let b = ProcessLock::acquire(dir.path()).expect("second acquire, same process");
        assert!(
            Arc::ptr_eq(&a.0, &b.0),
            "same-process handles must share one Arc<LockedFile>, not contend"
        );
    }

    #[test]
    fn lock_is_released_when_every_handle_in_this_process_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = ProcessLock::acquire(dir.path()).expect("first acquire");
        drop(a);
        // Re-acquiring after the only holder dropped must succeed, not see
        // a stale "still locked" state.
        let _b = ProcessLock::acquire(dir.path()).expect("re-acquire after drop");
    }

    #[test]
    fn registry_does_not_grow_across_unrelated_roots() {
        for _ in 0..8 {
            let dir = tempfile::tempdir().expect("tempdir");
            let lock = ProcessLock::acquire(dir.path()).expect("acquire");
            drop(lock);
        }
        let reg = registry().lock().expect("registry lock");
        let live: usize = reg.values().filter(|w| w.strong_count() > 0).count();
        assert_eq!(live, 0, "no handle should still be held");
        // The sweep in `acquire` only runs on the next call, so a bounded
        // number of dead entries may linger — but not one per iteration.
        assert!(
            reg.len() <= 1,
            "dead entries must not accumulate unboundedly, got {}",
            reg.len()
        );
    }
}
