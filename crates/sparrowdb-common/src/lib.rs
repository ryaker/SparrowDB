/// Logical sequence number identifying a WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

/// Physical page identifier within a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u64);

/// Transaction identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

/// Node identifier: upper 16 bits = label_id, lower 48 bits = slot_id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeId(pub u64);

/// Edge identifier: monotonic u64 sourced from the active metapage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct EdgeId(pub u64);

/// All errors that SparrowDB can return.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    InvalidMagic,
    ChecksumMismatch,
    VersionMismatch,
    NotFound,
    AlreadyExists,
    InvalidArgument(String),
    Corruption(String),
    OutOfMemory,
    Unimplemented,
    /// AEAD authentication tag verification failed — wrong key or corrupted ciphertext.
    DecryptionFailed,
    /// A write transaction is already active; only one writer is allowed at a time.
    WriterBusy,
    /// AEAD authentication tag rejected on page/WAL decrypt — signals that the
    /// database was opened with the wrong encryption key (distinct from a
    /// generic checksum error so callers can present a clear "wrong key" message).
    EncryptionAuthFailed,
    /// Two concurrent write transactions both modified the same node.
    ///
    /// The transaction that committed second is aborted to maintain consistency.
    WriteWriteConflict {
        node_id: u64,
    },
    /// The node has attached edges and cannot be deleted without removing them first.
    NodeHasEdges {
        node_id: u64,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::InvalidMagic => write!(f, "invalid magic bytes"),
            Error::ChecksumMismatch => write!(f, "checksum mismatch"),
            Error::VersionMismatch => write!(f, "version mismatch"),
            Error::NotFound => write!(f, "not found"),
            Error::AlreadyExists => write!(f, "already exists"),
            Error::InvalidArgument(s) => write!(f, "invalid argument: {s}"),
            Error::Corruption(s) => write!(f, "corruption: {s}"),
            Error::OutOfMemory => write!(f, "out of memory"),
            Error::Unimplemented => write!(f, "not yet implemented"),
            Error::DecryptionFailed => write!(f, "decryption failed: wrong key or corrupted data"),
            Error::WriterBusy => write!(f, "writer busy: a write transaction is already active"),
            Error::EncryptionAuthFailed => write!(
                f,
                "encryption authentication failed: wrong key or corrupted ciphertext"
            ),
            Error::WriteWriteConflict { node_id } => write!(
                f,
                "write-write conflict on node {node_id}: another transaction modified this node"
            ),
            Error::NodeHasEdges { node_id } => write!(
                f,
                "node {node_id} has attached edges and cannot be deleted without removing them first"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, Error>;

// ── Canonical column-ID derivation ───────────────────────────────────────────

/// Derive a stable `u32` column ID from a property key name.
///
/// Uses FNV-1a 32-bit hash for deterministic, catalog-free mapping.
/// This is the **single authoritative implementation** — both the storage
/// layer and the execution engine must call this function so that the
/// `col_id` written to disk and the `col_id` used at query time always agree.
pub fn col_id_of(name: &str) -> u32 {
    const FNV_PRIME: u32 = 16_777_619;
    const OFFSET_BASIS: u32 = 2_166_136_261;
    let mut hash = OFFSET_BASIS;
    for byte in name.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_roundtrip() {
        let id = PageId(42);
        assert_eq!(id.0, 42);
    }

    #[test]
    fn lsn_ordering() {
        assert!(Lsn(1) < Lsn(2));
    }

    #[test]
    fn txn_id_copy() {
        let t = TxnId(99);
        let t2 = t;
        assert_eq!(t, t2);
    }

    #[test]
    fn node_id_packing_roundtrip() {
        let label_id: u64 = 3;
        let slot_id: u64 = 0x0000_BEEF_CAFE;
        let packed = (label_id << 48) | (slot_id & 0x0000_FFFF_FFFF_FFFF);
        let node = NodeId(packed);
        let recovered_label = node.0 >> 48;
        let recovered_slot = node.0 & 0x0000_FFFF_FFFF_FFFF;
        assert_eq!(recovered_label, label_id);
        assert_eq!(recovered_slot, slot_id);
    }

    #[test]
    fn error_display() {
        let e = Error::InvalidMagic;
        assert!(!e.to_string().is_empty());
    }
}
