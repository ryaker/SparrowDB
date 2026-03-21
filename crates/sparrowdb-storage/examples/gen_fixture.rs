//! Generate the golden fixture `tests/fixtures/encrypted_page.bin`.
//!
//! Key  = [0x42; 32]
//! page_id = 0
//! Plaintext = [0xAB; 512]
//! Output = 552 bytes (512 + 40)

use sparrowdb_storage::encryption::EncryptionContext;
use std::path::PathBuf;

fn main() {
    let ctx = EncryptionContext::with_key([0x42u8; 32]);
    let plaintext = vec![0xABu8; 512];
    let ciphertext = ctx
        .encrypt_page(0, &plaintext)
        .expect("encrypt must succeed");

    assert_eq!(ciphertext.len(), 552, "fixture must be 552 bytes");

    // Write to tests/fixtures/ relative to workspace root.
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // sparrowdb-storage
    path.pop(); // crates
    path.push("tests");
    path.push("fixtures");
    std::fs::create_dir_all(&path).expect("create fixture directory");
    path.push("encrypted_page.bin");

    std::fs::write(&path, &ciphertext).expect("write fixture");
    println!(
        "Wrote golden fixture ({} bytes) to {}",
        ciphertext.len(),
        path.display()
    );
}
