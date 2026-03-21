use sparrowdb_common::{PageId, Result};

/// Stub page store — full implementation in Phase 1+.
pub struct PageStore;

impl PageStore {
    /// Open (or create) a page store rooted at `path`.
    pub fn open(_path: &std::path::Path) -> Result<Self> {
        unimplemented!("PageStore::open — implemented in Phase 1")
    }

    /// Read a page by ID into the provided buffer.
    pub fn read_page(&self, _id: PageId, _buf: &mut [u8]) -> Result<()> {
        unimplemented!("PageStore::read_page — implemented in Phase 1")
    }

    /// Write a page by ID from the provided buffer.
    pub fn write_page(&self, _id: PageId, _buf: &[u8]) -> Result<()> {
        unimplemented!("PageStore::write_page — implemented in Phase 1")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_store_type_exists() {
        // Verify that PageStore is importable and the crate compiles.
        let _: fn(&std::path::Path) -> Result<PageStore> = PageStore::open;
    }
}
