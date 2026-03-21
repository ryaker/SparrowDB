use sparrowdb_common::Result;
use std::path::{Path, PathBuf};

/// The top-level SparrowDB handle.
///
/// Obtain an instance via [`SparrowDB::open`] or [`open`].
pub struct SparrowDB {
    _path: PathBuf,
}

impl SparrowDB {
    /// Open (or create) a SparrowDB database at `path`.
    ///
    /// Phase 0: creates the directory if it does not exist and returns `Ok`.
    /// Full storage bring-up is implemented in Phase 1.
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        Ok(SparrowDB {
            _path: path.to_path_buf(),
        })
    }
}

/// Convenience wrapper — equivalent to [`SparrowDB::open`].
pub fn open(path: &Path) -> Result<SparrowDB> {
    SparrowDB::open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Phase 0 gate: create a DB directory, open it, and let it drop cleanly.
    #[test]
    fn open_and_close_empty_db() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.sparrow");

        // DB directory must not exist yet.
        assert!(!db_path.exists());

        // Open creates the directory and returns Ok.
        let db = open(&db_path).expect("open must succeed");

        // Directory now exists.
        assert!(db_path.is_dir());

        // Drop closes the handle cleanly (no panic).
        drop(db);
    }

    #[test]
    fn open_existing_dir_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("existing.sparrow");
        std::fs::create_dir_all(&db_path).unwrap();

        // Opening an already-existing directory must succeed.
        let db = SparrowDB::open(&db_path).expect("open existing must succeed");
        drop(db);
    }

    #[test]
    fn sparrowdb_open_fn_exists() {
        // Verify the public `open` function has the expected signature.
        let _: fn(&Path) -> Result<SparrowDB> = open;
    }
}
