pub mod metapage;

use sparrowdb_common::{PageId, Result};

/// Compute CRC32C (Castagnoli) of the entire buffer.
pub fn crc32_of(buf: &[u8]) -> u32 {
    crc32c::crc32c(buf)
}

/// Compute CRC32C (Castagnoli) of `buf` treating `zeroed_offset..zeroed_offset+zeroed_len`
/// as all zeros.
/// Used so CRC can be stored in the buffer itself (field is zeroed during calculation).
pub fn crc32_zeroed_at(buf: &[u8], zeroed_offset: usize, zeroed_len: usize) -> u32 {
    // Feed the three segments separately; avoids any heap allocation.
    let crc = crc32c::crc32c(&buf[..zeroed_offset]);
    // zeroed_len is always small (4 bytes in all current callers); use a stack buffer.
    const MAX_ZEROED: usize = 64;
    assert!(
        zeroed_len <= MAX_ZEROED,
        "zeroed_len exceeds stack buffer size"
    );
    let zeros = [0u8; MAX_ZEROED];
    let crc = crc32c::crc32c_append(crc, &zeros[..zeroed_len]);
    crc32c::crc32c_append(crc, &buf[zeroed_offset + zeroed_len..])
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
        // Manual: zero [4..8], compute crc32c
        let mut manual = buf;
        manual[4..8].copy_from_slice(&[0u8; 4]);
        let expected = crc32c::crc32c(&manual);
        let actual = crc32_zeroed_at(&buf, 4, 4);
        assert_eq!(actual, expected);
    }
}
