/// Phase 6 — at-rest encryption acceptance tests (TDD RED → GREEN).
///
/// All tests exercise `sparrowdb_storage::encryption::EncryptionContext`.
use sparrowdb_storage::encryption::EncryptionContext;

// ── helpers ─────────────────────────────────────────────────────────────────

const KEY_42: [u8; 32] = [0x42u8; 32];
const KEY_OTHER: [u8; 32] = [0x99u8; 32];
const PAGE_SIZE: usize = 512;
const ENCRYPTED_STRIDE: usize = PAGE_SIZE + 40; // 24-byte nonce + page_size + 16-byte tag

fn make_plaintext(byte: u8) -> Vec<u8> {
    vec![byte; PAGE_SIZE]
}

// ── test 1: encrypt → decrypt roundtrip ─────────────────────────────────────

#[test]
fn encrypt_decrypt_roundtrip() {
    let ctx = EncryptionContext::with_key(KEY_42);
    let plaintext = make_plaintext(0xAB);

    let ciphertext = ctx
        .encrypt_page(0, &plaintext)
        .expect("encrypt_page should succeed");
    assert_eq!(
        ciphertext.len(),
        ENCRYPTED_STRIDE,
        "encrypted output must be page_size + 40 bytes"
    );

    let recovered = ctx
        .decrypt_page(0, &ciphertext)
        .expect("decrypt_page should succeed");
    assert_eq!(
        recovered, plaintext,
        "decrypted plaintext must match original"
    );
}

// ── test 2: wrong key → EncryptionAuthFailed ─────────────────────────────────

#[test]
fn wrong_key_returns_decryption_failed() {
    let ctx_encrypt = EncryptionContext::with_key(KEY_42);
    let ctx_wrong = EncryptionContext::with_key(KEY_OTHER);

    let plaintext = make_plaintext(0x55);
    let ciphertext = ctx_encrypt
        .encrypt_page(1, &plaintext)
        .expect("encrypt_page should succeed");

    let result = ctx_wrong.decrypt_page(1, &ciphertext);
    assert!(
        matches!(result, Err(sparrowdb_common::Error::EncryptionAuthFailed)),
        "wrong key must return Error::EncryptionAuthFailed, got: {result:?}"
    );
}

// ── test 3: passthrough mode (no key) ───────────────────────────────────────

#[test]
fn passthrough_mode_no_change() {
    let ctx = EncryptionContext::none();
    let plaintext = make_plaintext(0xCD);

    let encrypted = ctx
        .encrypt_page(0, &plaintext)
        .expect("passthrough encrypt should succeed");
    assert_eq!(
        encrypted, plaintext,
        "passthrough encrypt must return plaintext unchanged"
    );

    let decrypted = ctx
        .decrypt_page(0, &plaintext)
        .expect("passthrough decrypt should succeed");
    assert_eq!(
        decrypted, plaintext,
        "passthrough decrypt must return data unchanged"
    );
}

// ── test 4 & 5: golden fixture ───────────────────────────────────────────────
// The fixture `tests/fixtures/encrypted_page.bin` was generated with:
//   key = [0x42; 32], page_id = 0, plaintext = [0xAB; 512]
// It is 552 bytes on disk.
//
// Because nonces are now random, `golden_fixture_encrypt` no longer checks
// byte-for-byte equality of the produced ciphertext against the fixture
// (each encryption produces a different nonce and therefore different bytes).
// Instead we verify the fixture is a valid encryption of the expected plaintext
// (i.e., that the fixture round-trips correctly under the known key).

#[test]
fn golden_fixture_has_correct_length() {
    let fixture_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/encrypted_page.bin"
    );
    let fixture = std::fs::read(fixture_path).expect("golden fixture must exist");
    assert_eq!(
        fixture.len(),
        ENCRYPTED_STRIDE,
        "golden fixture must be {} bytes (page_size + 40)",
        ENCRYPTED_STRIDE
    );
}

#[test]
fn golden_fixture_decrypt() {
    let ctx = EncryptionContext::with_key(KEY_42);
    let fixture_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/encrypted_page.bin"
    );
    let fixture = std::fs::read(fixture_path).expect("golden fixture must exist");

    let plaintext = ctx
        .decrypt_page(0, &fixture)
        .expect("decrypt of golden fixture should succeed");
    assert_eq!(
        plaintext,
        make_plaintext(0xAB),
        "decrypted golden fixture must be 512 bytes of 0xAB"
    );
}

// ── test: page-swap attack is detected ──────────────────────────────────────
// Page identity is cryptographically bound via AEAD AAD (page_id.to_le_bytes()).
// Swapping page 1's ciphertext into slot 0 causes an AAD mismatch, which
// makes the AEAD tag verification fail and returns EncryptionAuthFailed.

#[test]
fn page_swap_attack_detected() {
    let key = [0x42u8; 32];
    let ctx = EncryptionContext::with_key(key);
    let pt = vec![0xABu8; 512];
    let ct0 = ctx.encrypt_page(0, &pt).unwrap();
    let ct1 = ctx.encrypt_page(1, &pt).unwrap();
    // Swapping page 1's ciphertext into slot 0 must be rejected (AAD mismatch)
    assert!(
        ctx.decrypt_page(0, &ct1).is_err(),
        "page swap must be detected via AAD mismatch"
    );
    // Correct page still decrypts fine
    assert!(ctx.decrypt_page(0, &ct0).is_ok());
}

// ── test 6: different page_ids produce different ciphertext ──────────────────

#[test]
fn different_page_ids_produce_different_ciphertext() {
    let ctx = EncryptionContext::with_key(KEY_42);
    let plaintext = make_plaintext(0x77);

    let ct0 = ctx.encrypt_page(0, &plaintext).expect("encrypt page 0");
    let ct1 = ctx.encrypt_page(1, &plaintext).expect("encrypt page 1");

    assert_ne!(
        ct0, ct1,
        "same plaintext with different page_ids must produce different ciphertext"
    );

    // Both must still decrypt correctly.
    assert_eq!(ctx.decrypt_page(0, &ct0).unwrap(), plaintext);
    assert_eq!(ctx.decrypt_page(1, &ct1).unwrap(), plaintext);
}

// ── test 7: UC-5 acceptance check ───────────────────────────────────────────

#[test]
fn uc5_acceptance_check() {
    let key: [u8; 32] = {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(7).wrapping_add(13);
        }
        k
    };
    let wrong_key: [u8; 32] = {
        let mut k = key;
        k[0] ^= 0xFF;
        k
    };

    let ctx = EncryptionContext::with_key(key);
    let ctx_wrong = EncryptionContext::with_key(wrong_key);

    // Encrypt 10 pages with varied content.
    let mut plaintexts: Vec<Vec<u8>> = Vec::with_capacity(10);
    let mut ciphertexts: Vec<Vec<u8>> = Vec::with_capacity(10);

    for page_id in 0u64..10 {
        let mut plain = vec![0u8; PAGE_SIZE];
        for (j, b) in plain.iter_mut().enumerate() {
            *b = ((page_id as usize + j) % 256) as u8;
        }
        let ct = ctx
            .encrypt_page(page_id, &plain)
            .expect("encrypt must succeed");
        assert_eq!(
            ct.len(),
            ENCRYPTED_STRIDE,
            "encrypted stride must be page_size+40"
        );
        plaintexts.push(plain);
        ciphertexts.push(ct);
    }

    // Decrypt all 10 — must match original.
    for (page_id, (pt, ct)) in plaintexts.iter().zip(ciphertexts.iter()).enumerate() {
        let recovered = ctx
            .decrypt_page(page_id as u64, ct)
            .expect("decrypt with correct key must succeed");
        assert_eq!(&recovered, pt, "page {page_id} must round-trip");
    }

    // Wrong key must fail on every page.
    for (page_id, ct) in ciphertexts.iter().enumerate() {
        let result = ctx_wrong.decrypt_page(page_id as u64, ct);
        assert!(
            matches!(result, Err(sparrowdb_common::Error::EncryptionAuthFailed)),
            "page {page_id}: wrong key must return EncryptionAuthFailed"
        );
    }

    // Verify no 0xAB pattern leaks into the first encrypted page
    // (we encrypt a page of all 0xAB to check ciphertext doesn't echo plaintext).
    let sentinel = make_plaintext(0xAB);
    let sentinel_ct = ctx.encrypt_page(42, &sentinel).expect("encrypt sentinel");
    // The nonce (first 24 bytes) may contain zeros but the ciphertext portion
    // must not be identical to the plaintext — a run of 512 identical bytes is astronomically unlikely.
    let ct_body = &sentinel_ct[24..]; // skip nonce
    let all_ab = ct_body.iter().all(|&b| b == 0xAB);
    assert!(
        !all_ab,
        "ciphertext body must not equal plaintext (0xAB pattern leaked)"
    );
}
