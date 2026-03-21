use sparrowdb_common::Result;

/// Stub catalog — full TLV implementation in Phase 2.
pub struct Catalog;

impl Catalog {
    /// Load (or initialise) the catalog from the database directory.
    pub fn open(_path: &std::path::Path) -> Result<Self> {
        unimplemented!("Catalog::open — implemented in Phase 2")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_type_exists() {
        let _: fn(&std::path::Path) -> Result<Catalog> = Catalog::open;
    }
}
