use sparrowdb_common::{Error, Result};

/// Stub catalog — full TLV implementation in Phase 2.
pub struct Catalog;

impl Catalog {
    /// Load (or initialise) the catalog from the database directory.
    pub fn open(_path: &std::path::Path) -> Result<Self> {
        Err(Error::Unimplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_type_exists() {
        let _: fn(&std::path::Path) -> Result<Catalog> = Catalog::open;
    }

    #[test]
    fn catalog_open_returns_unimplemented() {
        let result = Catalog::open(std::path::Path::new("dummy"));
        assert!(matches!(result, Err(Error::Unimplemented)));
    }
}
