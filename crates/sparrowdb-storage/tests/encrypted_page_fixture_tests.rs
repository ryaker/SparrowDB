//! P6-5: Golden fixture tests for `encrypted-page-v1.bin`.
//!
//! Fixture parameters:
//!   Key:      `[0x42u8; 32]`
//!   page_id:  `7`
//!   Nonce:    `[0xA1u8; 24]`  (fixed for determinism)
//!   Plaintext: 4096 bytes of `0xAB`
//!   On-disk:  4136 bytes  (24-byte nonce + 4096-byte ciphertext + 16-byte auth tag)

use sparrowdb_common::Error;
use sparrowdb_storage::encryption::EncryptionContext;

const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/encrypted-page-v1.bin"
);
const KEY: [u8; 32] = [0x42u8; 32];
const WRONG_KEY: [u8; 32] = [0xFF_u8; 32];
const PAGE_ID: u64 = 7;
const PAGE_SIZE: usize = 4096;
const ENCRYPTED_STRIDE: usize = PAGE_SIZE + 40; // 24 nonce + PAGE_SIZE + 16 tag

fn load_fixture() -> Vec<u8> {
    std::fs::read(FIXTURE_PATH).expect("encrypted-page-v1.bin must exist in tests/fixtures/")
}

// ── Test 1: fixture has the correct length ────────────────────────────────────

#[test]
fn fixture_has_correct_length() {
    let data = load_fixture();
    assert_eq!(
        data.len(),
        ENCRYPTED_STRIDE,
        "encrypted-page-v1.bin must be {} bytes (page_size={} + 40 overhead)",
        ENCRYPTED_STRIDE,
        PAGE_SIZE
    );
}

// ── Test 2: round-trip — decrypt fixture, compare plaintext ──────────────────

#[test]
fn fixture_round_trip_decrypt() {
    let data = load_fixture();
    let ctx = EncryptionContext::with_key(KEY);

    let plaintext = ctx
        .decrypt_page(PAGE_ID, &data)
        .expect("decrypt of golden fixture must succeed with the correct key");

    assert_eq!(
        plaintext.len(),
        PAGE_SIZE,
        "decrypted plaintext must be {} bytes",
        PAGE_SIZE
    );
    assert!(
        plaintext.iter().all(|&b| b == 0xAB),
        "decrypted plaintext must be all 0xAB bytes"
    );
}

// ── Test 3: re-encrypt plaintext, re-decrypt, compare ────────────────────────
// Verifies that our encrypt/decrypt cycle produces the same plaintext
// regardless of the nonce (since nonces are random in production).

#[test]
fn encrypt_decrypt_produces_original_plaintext() {
    let ctx = EncryptionContext::with_key(KEY);
    let plaintext = vec![0xABu8; PAGE_SIZE];

    let ciphertext = ctx
        .encrypt_page(PAGE_ID, &plaintext)
        .expect("encrypt must succeed");
    assert_eq!(ciphertext.len(), ENCRYPTED_STRIDE, "encrypted stride check");

    let recovered = ctx
        .decrypt_page(PAGE_ID, &ciphertext)
        .expect("decrypt must succeed");
    assert_eq!(recovered, plaintext, "round-trip must recover original plaintext");
}

// ── Test 4: corruption — flip one ciphertext byte ────────────────────────────
// Flipping any byte in the ciphertext region (bytes 24..end) must cause
// the AEAD tag to reject the data and return EncryptionAuthFailed.

#[test]
fn single_ciphertext_byte_flip_returns_auth_failed() {
    let mut data = load_fixture();
    let ctx = EncryptionContext::with_key(KEY);

    // Flip byte 100 (well into the ciphertext, past the nonce).
    data[100] ^= 0xFF;

    let result = ctx.decrypt_page(PAGE_ID, &data);
    assert!(
        matches!(result, Err(Error::EncryptionAuthFailed)),
        "flipped ciphertext byte must return EncryptionAuthFailed, got: {:?}",
        result
    );
}

// ── Test 5: corruption — flip one auth-tag byte ──────────────────────────────

#[test]
fn auth_tag_byte_flip_returns_auth_failed() {
    let mut data = load_fixture();
    let ctx = EncryptionContext::with_key(KEY);

    // Flip the last byte (part of the 16-byte auth tag).
    let last = data.len() - 1;
    data[last] ^= 0x01;

    let result = ctx.decrypt_page(PAGE_ID, &data);
    assert!(
        matches!(result, Err(Error::EncryptionAuthFailed)),
        "flipped auth tag byte must return EncryptionAuthFailed, got: {:?}",
        result
    );
}

// ── Test 6: corruption — flip nonce byte ─────────────────────────────────────
// Flipping a nonce byte effectively decrypts with the wrong nonce, which
// produces garbage plaintext and a tag mismatch.

#[test]
fn nonce_byte_flip_returns_auth_failed() {
    let mut data = load_fixture();
    let ctx = EncryptionContext::with_key(KEY);

    // Flip the first nonce byte.
    data[0] ^= 0xFF;

    let result = ctx.decrypt_page(PAGE_ID, &data);
    assert!(
        matches!(result, Err(Error::EncryptionAuthFailed)),
        "flipped nonce byte must return EncryptionAuthFailed, got: {:?}",
        result
    );
}

// ── Test 7: wrong key returns EncryptionAuthFailed ───────────────────────────

#[test]
fn wrong_key_returns_encryption_auth_failed() {
    let data = load_fixture();
    let ctx = EncryptionContext::with_key(WRONG_KEY);

    let result = ctx.decrypt_page(PAGE_ID, &data);
    assert!(
        matches!(result, Err(Error::EncryptionAuthFailed)),
        "wrong key must return Error::EncryptionAuthFailed, got: {:?}",
        result
    );
}

// ── Test 8: wrong page_id (AAD mismatch) returns EncryptionAuthFailed ────────
// The fixture was encrypted with page_id = 7. Decrypting with page_id = 0
// produces an AAD mismatch and must be rejected.

#[test]
fn wrong_page_id_aad_mismatch_returns_auth_failed() {
    let data = load_fixture();
    let ctx = EncryptionContext::with_key(KEY);

    // Wrong page_id — AAD will not match.
    let result = ctx.decrypt_page(0, &data);
    assert!(
        matches!(result, Err(Error::EncryptionAuthFailed)),
        "wrong page_id (AAD mismatch) must return EncryptionAuthFailed, got: {:?}",
        result
    );
}

// ── Test 9: PageStore encrypted write → read round-trip ──────────────────────
// Verifies that the PageStore wiring (P6-2) works end-to-end with 4096-byte pages.

#[test]
fn page_store_encrypted_4096_roundtrip() {
    use sparrowdb_common::PageId;
    use sparrowdb_storage::PageStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");

    // Write page 7 with [0xAB; 4096].
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY).unwrap();
        let plaintext = vec![0xABu8; PAGE_SIZE];
        store.write_page(PageId(PAGE_ID), &plaintext).unwrap();
    }

    // Read it back and verify.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE];
        store.read_page(PageId(PAGE_ID), &mut buf).unwrap();
        assert!(
            buf.iter().all(|&b| b == 0xAB),
            "round-trip via PageStore must recover 0xAB plaintext"
        );
    }
}

// ── Test 10: PageStore wrong key → EncryptionAuthFailed ──────────────────────

#[test]
fn page_store_wrong_key_returns_auth_failed() {
    use sparrowdb_common::PageId;
    use sparrowdb_storage::PageStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");

    // Write with correct key.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, KEY).unwrap();
        store
            .write_page(PageId(PAGE_ID), &vec![0xABu8; PAGE_SIZE])
            .unwrap();
    }

    // Read with wrong key.
    {
        let store = PageStore::open_encrypted(&path, PAGE_SIZE, WRONG_KEY).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE];
        let result = store.read_page(PageId(PAGE_ID), &mut buf);
        assert!(
            matches!(result, Err(Error::EncryptionAuthFailed)),
            "wrong key on PageStore::read_page must return EncryptionAuthFailed, got: {:?}",
            result
        );
    }
}
