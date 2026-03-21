pub mod metapage;

/// At-rest page encryption using XChaCha20-Poly1305.
pub mod encryption;

/// WAL subsystem — codec, writer, and replay.
pub mod wal;

/// CSR (Compressed Sparse Row) forward and backward edge files.
pub mod csr;

/// Node property column storage.
pub mod node_store;

/// Edge delta log and CSR rebuild on checkpoint.
pub mod edge_store;

use sparrowdb_common::{PageId, Result};

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
