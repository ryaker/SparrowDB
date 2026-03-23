//! SPA-99 — Generate the encrypted PageStore golden fixture.
//!
//! Produces `tests/fixtures/encryption/v1.0/encrypted_graph.tar`, a tar
//! archive containing a two-page encrypted PageStore file:
//!
//!   Key:       `[0x42u8; 32]`
//!   page_size: 4096
//!   Pages:     0 → `[0xAAu8; 4096]`  (simulates Node A)
//!              1 → `[0xBBu8; 4096]`  (simulates Node B)
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin gen-encrypted-fixture -- \
//!     --out tests/fixtures/encryption/v1.0/encrypted_graph.tar
//! ```

use clap::Parser;
use sparrowdb_common::PageId;
use sparrowdb_storage::PageStore;
use std::path::PathBuf;

const KEY: [u8; 32] = [0x42u8; 32];
const PAGE_SIZE: usize = 4096;

#[derive(Parser, Debug)]
#[command(name = "gen-encrypted-fixture")]
#[command(about = "Generate an encrypted PageStore golden fixture for SPA-99")]
struct Args {
    /// Output path for the resulting tar archive.
    #[arg(
        long,
        default_value = "tests/fixtures/encryption/v1.0/encrypted_graph.tar"
    )]
    out: PathBuf,
}

fn main() {
    let args = Args::parse();

    // Create a temp directory to build the store file, then archive it.
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let tmp_dir = std::env::temp_dir().join(format!("sparrowdb_enc_fixture_{ts}"));
    std::fs::create_dir_all(&tmp_dir).expect("create tmp dir");

    let store_path = tmp_dir.join("encrypted_graph.bin");

    println!("Generating encrypted PageStore fixture …");
    println!("  Key:       [0x42; 32]");
    println!("  page_size: {PAGE_SIZE}");
    println!("  page 0:    [0xAA; {PAGE_SIZE}]");
    println!("  page 1:    [0xBB; {PAGE_SIZE}]");

    // ── Write two pages ──────────────────────────────────────────────────────
    {
        let store = PageStore::open_encrypted(&store_path, PAGE_SIZE, KEY)
            .expect("open_encrypted must succeed");
        store
            .write_page(PageId(0), &vec![0xAAu8; PAGE_SIZE])
            .expect("write page 0");
        store
            .write_page(PageId(1), &vec![0xBBu8; PAGE_SIZE])
            .expect("write page 1");
    }

    // ── Sanity-check: read back with correct key ─────────────────────────────
    {
        let store = PageStore::open_encrypted(&store_path, PAGE_SIZE, KEY)
            .expect("reopen for sanity check");

        let mut buf0 = vec![0u8; PAGE_SIZE];
        store
            .read_page(PageId(0), &mut buf0)
            .expect("read page 0 sanity check");
        assert!(
            buf0.iter().all(|&b| b == 0xAA),
            "sanity: page 0 must be all 0xAA"
        );

        let mut buf1 = vec![0u8; PAGE_SIZE];
        store
            .read_page(PageId(1), &mut buf1)
            .expect("read page 1 sanity check");
        assert!(
            buf1.iter().all(|&b| b == 0xBB),
            "sanity: page 1 must be all 0xBB"
        );
    }

    let on_disk_size = store_path.metadata().expect("stat store file").len();
    println!(
        "  on-disk size: {on_disk_size} bytes  (expected {})",
        2 * (PAGE_SIZE + 40)
    );
    assert_eq!(
        on_disk_size as usize,
        2 * (PAGE_SIZE + 40),
        "encrypted file must be 2 × (page_size + 40) bytes"
    );

    // ── Archive ──────────────────────────────────────────────────────────────
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent).expect("create output dir");
    }

    let status = std::process::Command::new("tar")
        .arg("cf")
        .arg(args.out.as_os_str())
        .arg("-C")
        .arg(tmp_dir.as_os_str())
        .arg("encrypted_graph.bin")
        .status()
        .expect("tar command failed to start");

    assert!(
        status.success(),
        "tar exited with non-zero status: {status}"
    );

    // ── Cleanup ──────────────────────────────────────────────────────────────
    let _ = std::fs::remove_dir_all(&tmp_dir);

    println!("  wrote {}", args.out.display());
    println!("Done. Commit {} to the repository.", args.out.display());
}
