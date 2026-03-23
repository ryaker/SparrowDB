//! SPA-99 — `wrong_key_returns_error` test + encrypted-page golden fixture
//!           (Phase 6 completion).
//!
//! These tests exercise encryption at the `PageStore` and `EncryptionContext`
//! layers, which is where at-rest encryption lives in SparrowDB.
//!
//! # Tests
//!
//! 1. `wrong_key_returns_error` — write pages with key A, read with key B → `EncryptionAuthFailed`
//! 2. `correct_key_round_trips` — write pages with key A, read with key A → data intact
//! 3. `no_key_on_encrypted_db_returns_error` — write encrypted, read in passthrough mode → error
//! 4. `encrypted_golden_fixture_opens` — open a committed binary fixture with the known key
//!
//! # Fixture
//!
//! `tests/fixtures/encryption/v1.0/encrypted_graph.tar` is a tar archive
//! containing:
//!   - `encrypted_graph.bin` — an encrypted PageStore file written with:
//!     - Key: `[0x42u8; 32]`
//!     - page_size: 4096
//!     - Pages: 0 → `[0xAAu8; 4096]` (Node A), 1 → `[0xBBu8; 4096]` (Node B)
//!
//! Run: `cargo test -p sparrowdb spa_99`

use sparrowdb_common::{Error, PageId};
use sparrowdb_storage::PageStore;

// ── Constants ─────────────────────────────────────────────────────────────────

const KEY_A: [u8; 32] = [0x42u8; 32];
const KEY_B: [u8; 32] = [0x99u8; 32];
const PAGE_SIZE: usize = 4096;

/// Path to the golden fixture tarball (relative to workspace root).
fn fixture_tar() -> std::path::PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent() // crates/
        .unwrap()
        .parent() // repo root
        .unwrap()
        .join("tests")
        .join("fixtures")
        .join("encryption")
        .join("v1.0")
        .join("encrypted_graph.tar")
}

/// Extract the fixture tarball into a temp directory and return the directory
/// handle (keeps the temp dir alive) plus the path to `encrypted_graph.bin`.
fn extract_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let tar_path = fixture_tar();
    assert!(
        tar_path.exists(),
        "encrypted_graph.tar not found at {}\n\
         Regenerate with: cargo run --bin gen-encrypted-fixture -- \
         --out tests/fixtures/encryption/v1.0/encrypted_graph.tar",
        tar_path.display()
    );

    let tmp = tempfile::tempdir().expect("tempdir for fixture extraction");

    let status = std::process::Command::new("tar")
        .arg("xf")
        .arg(tar_path.as_os_str())
        .arg("-C")
        .arg(tmp.path().as_os_str())
        .status()
        .expect("tar command failed to start");

    assert!(
        status.success(),
        "tar extraction failed for encrypted fixture (exit: {status})"
    );

    let store_path = tmp.path().join("encrypted_graph.bin");
    assert!(
        store_path.exists(),
        "expected encrypted_graph.bin inside fixture tar, but not found at {}",
        store_path.display()
    );

    (tmp, store_path)
}

// ── Test 1: wrong_key_returns_error ──────────────────────────────────────────

/// Write two pages with KEY_A, close, reopen with KEY_B.
/// Every read must return `Error::EncryptionAuthFailed` — never silently
/// corrupt or panic.
#[test]
fn spa_99_wrong_key_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");

    // Write with correct key.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY_A)
            .expect("open_encrypted with KEY_A must succeed");
        store
            .write_page(PageId(0), &vec![0xAAu8; PAGE_SIZE])
            .expect("write page 0 must succeed");
        store
            .write_page(PageId(1), &vec![0xBBu8; PAGE_SIZE])
            .expect("write page 1 must succeed");
    }

    // Reopen with wrong key — reads must fail.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY_B)
            .expect("open_encrypted with KEY_B must not fail on open (error deferred to read)");

        let mut buf = vec![0u8; PAGE_SIZE];

        let result0 = store.read_page(PageId(0), &mut buf);
        assert!(
            matches!(result0, Err(Error::EncryptionAuthFailed)),
            "wrong key on page 0 must return EncryptionAuthFailed, got: {result0:?}"
        );

        let result1 = store.read_page(PageId(1), &mut buf);
        assert!(
            matches!(result1, Err(Error::EncryptionAuthFailed)),
            "wrong key on page 1 must return EncryptionAuthFailed, got: {result1:?}"
        );
    }
}

// ── Test 2: correct_key_round_trips ──────────────────────────────────────────

/// Write two pages with KEY_A, close, reopen with KEY_A.
/// Both pages must decrypt back to the original plaintext.
#[test]
fn spa_99_correct_key_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");

    let page0_data = vec![0xAAu8; PAGE_SIZE];
    let page1_data = vec![0xBBu8; PAGE_SIZE];

    // Write with KEY_A.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY_A)
            .expect("open_encrypted must succeed for write");
        store
            .write_page(PageId(0), &page0_data)
            .expect("write page 0");
        store
            .write_page(PageId(1), &page1_data)
            .expect("write page 1");
    }

    // Reopen with KEY_A — reads must succeed and return the original data.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY_A)
            .expect("open_encrypted must succeed for read");

        let mut buf0 = vec![0u8; PAGE_SIZE];
        store
            .read_page(PageId(0), &mut buf0)
            .expect("read page 0 with correct key must succeed");
        assert_eq!(buf0, page0_data, "page 0 must round-trip to original data");

        let mut buf1 = vec![0u8; PAGE_SIZE];
        store
            .read_page(PageId(1), &mut buf1)
            .expect("read page 1 with correct key must succeed");
        assert_eq!(buf1, page1_data, "page 1 must round-trip to original data");
    }
}

// ── Test 3: spa_99_no_key_on_encrypted_db_returns_error ──────────────────────

/// Write pages in encrypted mode, then open in passthrough (no-key) mode.
/// Because the encrypted stride is page_size+40 but the passthrough store
/// expects page_size bytes at each page offset, reading page 0 returns raw
/// ciphertext (not the plaintext), and reading page 1 returns data from the
/// wrong file offset (or a short read error).
///
/// We verify that the returned data is NOT the original plaintext, meaning
/// an unkeyed open cannot silently expose the plaintext.  This is the threat
/// model: data at rest is opaque without the key.
#[test]
fn spa_99_no_key_on_encrypted_db_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");

    let expected_page0 = vec![0xAAu8; PAGE_SIZE];

    // Write with encryption.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY_A)
            .expect("encrypted open must succeed");
        store
            .write_page(PageId(0), &expected_page0)
            .expect("write page 0");
    }

    // Reopen WITHOUT encryption (passthrough PageStore::open).
    // The file's page-0 on-disk slot is page_size+40 bytes; opening with
    // page_size=4096 means the passthrough store reads 4096 bytes starting at
    // offset 0, which includes the nonce + part of the ciphertext — definitely
    // not the original plaintext.
    {
        let store = PageStore::open(&path).expect("passthrough open must not error at open time");

        let mut buf = vec![0u8; PAGE_SIZE];
        // The read either succeeds (returning ciphertext/garbage) or fails with
        // an I/O error if the file is shorter than expected.  Either way the
        // returned data must not equal the plaintext.
        match store.read_page(PageId(0), &mut buf) {
            Ok(()) => {
                assert_ne!(
                    buf, expected_page0,
                    "passthrough read of encrypted file must not return original plaintext \
                     (encryption is not transparent)"
                );
            }
            Err(_) => {
                // Any error (short read, I/O error) is also acceptable — the
                // key point is that the plaintext is not exposed.
            }
        }
    }
}

// ── Test 4: spa_99_encrypted_golden_fixture_opens ────────────────────────────

/// Open the committed golden fixture `encrypted_graph.tar` and verify that:
///   1. The store opens with KEY_A without error.
///   2. Page 0 decrypts to `[0xAAu8; 4096]`.
///   3. Page 1 decrypts to `[0xBBu8; 4096]`.
///   4. Opening with KEY_B returns `EncryptionAuthFailed`.
///
/// This test is the format-stability canary for the encrypted PageStore: if
/// the on-disk layout or AEAD construction ever changes in a backward-
/// incompatible way this test will catch it on CI before any user data is
/// affected.
#[test]
fn spa_99_encrypted_golden_fixture_opens() {
    let (_tmp, store_path) = extract_fixture();

    // ── Correct key: both pages must decrypt correctly ────────────────────────
    {
        let store = PageStore::open_encrypted(&store_path, PAGE_SIZE, KEY_A)
            .expect("fixture must open with KEY_A");

        let mut buf0 = vec![0u8; PAGE_SIZE];
        store
            .read_page(PageId(0), &mut buf0)
            .expect("fixture page 0 must decrypt with KEY_A");
        assert!(
            buf0.iter().all(|&b| b == 0xAA),
            "fixture page 0 must be all 0xAA bytes after decryption"
        );

        let mut buf1 = vec![0u8; PAGE_SIZE];
        store
            .read_page(PageId(1), &mut buf1)
            .expect("fixture page 1 must decrypt with KEY_A");
        assert!(
            buf1.iter().all(|&b| b == 0xBB),
            "fixture page 1 must be all 0xBB bytes after decryption"
        );
    }

    // ── Wrong key: both reads must return EncryptionAuthFailed ────────────────
    {
        let store = PageStore::open_encrypted(&store_path, PAGE_SIZE, KEY_B)
            .expect("fixture open with wrong key must not fail at open time");

        let mut buf = vec![0u8; PAGE_SIZE];

        let r0 = store.read_page(PageId(0), &mut buf);
        assert!(
            matches!(r0, Err(Error::EncryptionAuthFailed)),
            "fixture page 0 with KEY_B must return EncryptionAuthFailed, got: {r0:?}"
        );

        let r1 = store.read_page(PageId(1), &mut buf);
        assert!(
            matches!(r1, Err(Error::EncryptionAuthFailed)),
            "fixture page 1 with KEY_B must return EncryptionAuthFailed, got: {r1:?}"
        );
    }
}
