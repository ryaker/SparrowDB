pub mod metapage;

use sparrowdb_common::{PageId, Result};

/// Compute CRC32 of the entire buffer.
pub fn crc32_of(buf: &[u8]) -> u32 {
    crc32fast::hash(buf)
}

/// Compute CRC32 of `buf` treating `zeroed_offset..zeroed_offset+zeroed_len` as all zeros.
/// Used so CRC can be stored in the buffer itself (field is zeroed during calculation).
pub fn crc32_zeroed_at(buf: &[u8], zeroed_offset: usize, zeroed_len: usize) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[..zeroed_offset]);
    hasher.update(&vec![0u8; zeroed_len]);
    hasher.update(&buf[zeroed_offset + zeroed_len..]);
    hasher.finalize()
}

/// Stub page store — full buffer pool implementation in Phase 2+.
pub struct PageStore;

impl PageStore {
    /// Open (or create) a page store rooted at `path`.
    pub fn open(_path: &std::path::Path) -> Result<Self> {
        unimplemented!("PageStore::open — implemented in Phase 2")
    }

    /// Read a page by ID into the provided buffer.
    pub fn read_page(&self, _id: PageId, _buf: &mut [u8]) -> Result<()> {
        unimplemented!("PageStore::read_page — implemented in Phase 2")
    }

    /// Write a page by ID from the provided buffer.
    pub fn write_page(&self, _id: PageId, _buf: &[u8]) -> Result<()> {
        unimplemented!("PageStore::write_page — implemented in Phase 2")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_store_type_exists() {
        let _: fn(&std::path::Path) -> Result<PageStore> = PageStore::open;
    }

    #[test]
    fn crc32_zeroed_at_matches_manual_zeroing() {
        let mut buf = [0xABu8; 16];
        // Place a known value at [4..8]
        buf[4..8].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        // Manual: zero [4..8], compute crc
        let mut manual = buf;
        manual[4..8].copy_from_slice(&[0u8; 4]);
        let expected = crc32fast::hash(&manual);
        let actual = crc32_zeroed_at(&buf, 4, 4);
        assert_eq!(actual, expected);
    }
}
