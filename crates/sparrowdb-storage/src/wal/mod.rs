/// WAL (Write-Ahead Log) subsystem.
///
/// ## Segment file layout
///
/// Each segment file begins with a 1-byte version header ([`codec::WAL_FORMAT_VERSION`]).
/// Records follow immediately after.
///
/// Binary format per record:
///   [4-byte length (u32 LE, covers everything after the length field)]
///   [1-byte record type]
///   [8-byte LSN (u64 LE)]
///   [8-byte txn_id (u64 LE)]
///   [payload (type-specific)]
///   [4-byte CRC32C (Castagnoli) of (type + lsn + txn_id + payload)]
///
/// Segments: `wal/segment-{:020}.wal`, 64 MiB each.
pub mod codec;
pub mod migrate;
pub mod replay;
pub mod writer;

pub use codec::{WalPayload, WalRecord, WalRecordKind};
pub use migrate::{migrate_wal, MigrationResult};
pub use replay::{WalReplayer, WalSchema};
pub use writer::WalWriter;
