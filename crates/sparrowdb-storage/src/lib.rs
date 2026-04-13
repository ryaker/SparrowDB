pub mod metapage;

/// Simple inverted full-text index for CALL db.index.fulltext.queryNodes.
pub mod fulltext_index;

/// BM25 inverted full-text index (issue #395).
pub mod fts_index;

/// At-rest page encryption using XChaCha20-Poly1305.
pub mod encryption;

/// CHECKPOINT and OPTIMIZE maintenance operations.
pub mod maintenance;

/// WAL subsystem — codec, writer, and replay.
pub mod wal;

/// CSR (Compressed Sparse Row) forward and backward edge files.
pub mod csr;

/// Node property column storage.
pub mod node_store;

/// In-memory B-tree property equality index (SPA-249).
pub mod property_index;

/// In-memory text search index for CONTAINS and STARTS WITH (SPA-251).
pub mod text_index;

/// Edge delta log and CSR rebuild on checkpoint.
pub mod edge_store;

/// HNSW vector similarity index (issue #394).
pub mod vector_index;

pub use vector_index::{Metric, VectorIndex};

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use sparrowdb_common::{Error, PageId, Result};

use crate::encryption::EncryptionContext;

/// Compute CRC32C (Castagnoli) of the entire buffer.
pub fn crc32_of(buf: &[u8]) -> u32 {
    crc32c::crc32c(buf)
}

/// Compute CRC32C (Castagnoli) of `buf` treating `zeroed_offset..zeroed_offset+zeroed_len`
/// as all zeros.
/// Used so CRC can be stored in the buffer itself (field is zeroed during calculation).
///
/// Returns `Err(Error::InvalidArgument)` if the zeroed range is out of bounds or overflows.
pub fn crc32_zeroed_at(
    buf: &[u8],
    zeroed_offset: usize,
    zeroed_len: usize,
) -> sparrowdb_common::Result<u32> {
    // Validate inputs before any arithmetic that could panic.
    let end = zeroed_offset.checked_add(zeroed_len).ok_or_else(|| {
        sparrowdb_common::Error::InvalidArgument("zeroed range overflows usize".into())
    })?;
    if end > buf.len() {
        return Err(sparrowdb_common::Error::InvalidArgument(format!(
            "zeroed range {}..{} out of bounds for buffer of length {}",
            zeroed_offset,
            end,
            buf.len()
        )));
    }
    // Feed the three segments separately; avoids any heap allocation.
    let crc = crc32c::crc32c(&buf[..zeroed_offset]);
    // Feed zeros in fixed-size stack chunks to support arbitrarily large zeroed_len.
    const CHUNK: usize = 64;
    let zeros = [0u8; CHUNK];
    let mut crc = crc;
    let mut remaining = zeroed_len;
    while remaining > 0 {
        let n = remaining.min(CHUNK);
        crc = crc32c::crc32c_append(crc, &zeros[..n]);
        remaining -= n;
    }
    Ok(crc32c::crc32c_append(crc, &buf[end..]))
}

/// Page store: maps logical page IDs to on-disk locations, with optional
/// at-rest encryption via [`EncryptionContext`].
///
/// ## On-disk layout
///
/// When encryption is **disabled** (passthrough mode), each page occupies
/// exactly `page_size` bytes at offset `page_id * page_size`.
///
/// When encryption is **enabled**, each page occupies `page_size + 40` bytes
/// (the "encrypted stride"):
///
/// ```text
/// ┌────────────────────────┬───────────────────────────────────┐
/// │  nonce (24 bytes)      │  ciphertext + auth tag            │
/// │                        │  (page_size + 16 bytes)           │
/// └────────────────────────┴───────────────────────────────────┘
/// total: page_size + 40 bytes per page slot
/// ```
///
/// The encryption stride ensures that a file opened without a key cannot be
/// accidentally decoded as raw pages (sizes won't align).
pub struct PageStore {
    /// The backing data file.
    ///
    /// `pread`/`pwrite` (`FileExt::read_exact_at` / `write_all_at`) are
    /// used for all I/O so the file descriptor is never mutated (no seek
    /// cursor movement).  This makes concurrent reads safe without a mutex.
    file: std::fs::File,
    /// Logical page size in bytes (plaintext).
    page_size: usize,
    /// The encryption context — passthrough if no key was provided.
    enc: EncryptionContext,
    /// Path for diagnostics.
    _path: PathBuf,
}

impl PageStore {
    /// Open (or create) a page store at `path` with the given `page_size` and
    /// no encryption.
    ///
    /// Use [`PageStore::open_encrypted`] to enable at-rest encryption.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_inner(path, 4096, EncryptionContext::none())
    }

    /// Open (or create) a page store with the given page size and no encryption.
    pub fn open_with_page_size(path: &Path, page_size: usize) -> Result<Self> {
        Self::open_inner(path, page_size, EncryptionContext::none())
    }

    /// Open (or create) an **encrypted** page store.
    ///
    /// `key` — 32-byte master encryption key (XChaCha20-Poly1305).
    ///
    /// If the file already exists and was written with a different key, the
    /// first `read_page` call will return
    /// [`Error::EncryptionAuthFailed`].
    pub fn open_encrypted(path: &Path, page_size: usize, key: [u8; 32]) -> Result<Self> {
        Self::open_inner(path, page_size, EncryptionContext::with_key(key))
    }

    fn open_inner(path: &Path, page_size: usize, enc: EncryptionContext) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(PageStore {
            file,
            page_size,
            enc,
            _path: path.to_path_buf(),
        })
    }

    /// Byte offset of page `id` in the backing file.
    fn page_offset(&self, id: PageId) -> u64 {
        let stride = if self.enc.is_encrypted() {
            self.page_size + 40
        } else {
            self.page_size
        };
        id.0 * stride as u64
    }

    /// Read page `id` into `buf`.
    ///
    /// `buf` must be exactly `page_size` bytes.
    ///
    /// # Errors
    /// - [`Error::InvalidArgument`] — `buf.len() != page_size`.
    /// - [`Error::EncryptionAuthFailed`] — AEAD tag rejected (wrong key or
    ///   corrupted page).
    /// - [`Error::Io`] — underlying I/O failure.
    pub fn read_page(&self, id: PageId, buf: &mut [u8]) -> Result<()> {
        if buf.len() != self.page_size {
            return Err(Error::InvalidArgument(format!(
                "read_page: buf len {} != page_size {}",
                buf.len(),
                self.page_size
            )));
        }

        let offset = self.page_offset(id);
        let on_disk_size = if self.enc.is_encrypted() {
            self.page_size + 40
        } else {
            self.page_size
        };

        let mut on_disk_buf = vec![0u8; on_disk_size];
        self.file.read_exact_at(&mut on_disk_buf, offset)?;

        if self.enc.is_encrypted() {
            let plaintext = self.enc.decrypt_page(id.0, &on_disk_buf)?;
            buf.copy_from_slice(&plaintext);
        } else {
            buf.copy_from_slice(&on_disk_buf);
        }

        Ok(())
    }

    /// Write `buf` as page `id`.
    ///
    /// `buf` must be exactly `page_size` bytes.
    ///
    /// # Errors
    /// - [`Error::InvalidArgument`] — `buf.len() != page_size`.
    /// - [`Error::Io`] — underlying I/O failure.
    pub fn write_page(&self, id: PageId, buf: &[u8]) -> Result<()> {
        if buf.len() != self.page_size {
            return Err(Error::InvalidArgument(format!(
                "write_page: buf len {} != page_size {}",
                buf.len(),
                self.page_size
            )));
        }

        let on_disk_data = if self.enc.is_encrypted() {
            self.enc.encrypt_page(id.0, buf)?
        } else {
            buf.to_vec()
        };

        let offset = self.page_offset(id);
        self.file.write_all_at(&on_disk_data, offset)?;
        Ok(())
    }

    /// Flush and fsync the backing file to durable storage.
    pub fn fsync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_store_type_exists() {
        // Verify the open function compiles with the right signature.
        let _: fn(&Path) -> Result<PageStore> = PageStore::open;
    }

    #[test]
    fn page_store_plaintext_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pages.bin");
        let store = PageStore::open_with_page_size(&path, 512).unwrap();

        let write_buf = vec![0x42u8; 512];
        store.write_page(PageId(0), &write_buf).unwrap();

        let mut read_buf = vec![0u8; 512];
        store.read_page(PageId(0), &mut read_buf).unwrap();
        assert_eq!(read_buf, write_buf);
    }

    #[test]
    fn page_store_encrypted_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc_pages.bin");
        let key = [0x11u8; 32];
        let store = PageStore::open_encrypted(&path, 512, key).unwrap();

        let write_buf = vec![0xAAu8; 512];
        store.write_page(PageId(3), &write_buf).unwrap();

        let mut read_buf = vec![0u8; 512];
        store.read_page(PageId(3), &mut read_buf).unwrap();
        assert_eq!(read_buf, write_buf);
    }

    #[test]
    fn page_store_wrong_key_returns_auth_failed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc_pages.bin");

        // Write with key A.
        {
            let store = PageStore::open_encrypted(&path, 512, [0xAAu8; 32]).unwrap();
            store.write_page(PageId(0), &vec![0x55u8; 512]).unwrap();
        }

        // Read with key B.
        let store = PageStore::open_encrypted(&path, 512, [0xBBu8; 32]).unwrap();
        let mut buf = vec![0u8; 512];
        let result = store.read_page(PageId(0), &mut buf);
        assert!(
            matches!(result, Err(Error::EncryptionAuthFailed)),
            "expected EncryptionAuthFailed, got {:?}",
            result
        );
    }

    #[test]
    fn page_store_multiple_pages_encrypted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.bin");
        let key = [0x33u8; 32];
        let store = PageStore::open_encrypted(&path, 256, key).unwrap();

        for i in 0u64..4 {
            let buf = vec![i as u8; 256];
            store.write_page(PageId(i), &buf).unwrap();
        }

        for i in 0u64..4 {
            let mut buf = vec![0u8; 256];
            store.read_page(PageId(i), &mut buf).unwrap();
            assert!(buf.iter().all(|&b| b == i as u8));
        }
    }

    #[test]
    fn crc32_zeroed_at_matches_manual_zeroing() {
        let mut buf = [0xABu8; 16];
        // Place a known value at [4..8]
        buf[4..8].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        // Manual: zero [4..8], compute crc32c
        let mut manual = buf;
        manual[4..8].copy_from_slice(&[0u8; 4]);
        let expected = crc32c::crc32c(&manual);
        let actual = crc32_zeroed_at(&buf, 4, 4).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn crc32_zeroed_at_rejects_out_of_bounds() {
        let buf = [0u8; 16];
        assert!(crc32_zeroed_at(&buf, 14, 4).is_err()); // 14+4=18 > 16
        assert!(crc32_zeroed_at(&buf, usize::MAX, 1).is_err()); // overflow
    }
}
