//! TLV (Tag-Length-Value) codec for catalog payload pages.
//!
//! Wire format per entry (§10.2.2 of v3 spec):
//! ```text
//! [tag: u16 LE][length: u32 LE][payload: length bytes]
//! ```
//!
//! Tag values:
//! | Tag    | Meaning                              |
//! |--------|--------------------------------------|
//! | 0x0001 | Label definition                     |
//! | 0x0002 | Relationship table definition        |
//! | 0x0003 | Column definition                    |
//! | 0x0004 | Secondary-label reverse-index entry  |
//! | 0x0005 | Format metadata                      |
//! | 0x0006 | Node label set (multi-label, SPA-200)|

use sparrowdb_common::{Error, Result};

/// TLV tag constants.
pub const TAG_LABEL: u16 = 0x0001;
pub const TAG_REL_TABLE: u16 = 0x0002;
pub const TAG_COLUMN: u16 = 0x0003;
pub const TAG_REVERSE_INDEX: u16 = 0x0004;
pub const TAG_FORMAT_META: u16 = 0x0005;
/// Tag for a node's secondary label assignment (SPA-200 multi-label support).
pub const TAG_NODE_LABEL_SET: u16 = 0x0006;

/// A single decoded TLV entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlvEntry {
    /// Tag 0x0001 — label definition.
    Label(LabelEntry),
    /// Tag 0x0002 — relationship table definition.
    RelTable(RelTableEntry),
    /// Tag 0x0003 — column definition.
    Column(ColumnEntry),
    /// Tag 0x0004 — secondary-label reverse-index entry.
    ReverseIndex(ReverseIndexEntry),
    /// Tag 0x0005 — format metadata.
    FormatMeta(FormatMetaEntry),
    /// Tag 0x0006 — node label set (multi-label assignment, SPA-200).
    NodeLabelSet(NodeLabelSetEntry),
}

/// Label definition payload.
///
/// ```text
/// label_id: u16
/// name_len: u16
/// name: [u8; name_len]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelEntry {
    pub label_id: u16,
    pub name: String,
}

/// Relationship table definition payload.
///
/// ```text
/// rel_table_id: u64
/// src_label_id: u16
/// dst_label_id: u16
/// rel_type_len: u16
/// rel_type: [u8; rel_type_len]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelTableEntry {
    pub rel_table_id: u64,
    pub src_label_id: u16,
    pub dst_label_id: u16,
    pub rel_type: String,
}

/// Column definition payload.
///
/// ```text
/// owner_kind: u8   (0=node label, 1=relationship table)
/// owner_id: u64
/// field_id: u32
/// name_len: u16
/// name: [u8; name_len]
/// type_tag: u8
/// nullable: u8
/// has_default: u8
/// default_len: u32
/// default_bytes: [u8; default_len]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnEntry {
    pub owner_kind: u8,
    pub owner_id: u64,
    pub field_id: u32,
    pub name: String,
    pub type_tag: u8,
    pub nullable: u8,
    pub has_default: u8,
    pub default_bytes: Vec<u8>,
}

/// Secondary-label reverse-index entry payload.
///
/// ```text
/// secondary_label_id: u16
/// owner_count: u16
/// owner_label_ids: [u16; owner_count]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReverseIndexEntry {
    pub secondary_label_id: u16,
    pub owner_label_ids: Vec<u16>,
}

/// Format metadata entry payload.
///
/// ```text
/// version: u16
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatMetaEntry {
    pub version: u16,
}

/// Node label set entry payload (SPA-200 multi-label support).
///
/// Records that a node (identified by `primary_label_id` + `slot`) also
/// carries one or more secondary labels.  This entry is appended to the
/// catalog whenever a multi-label node is created.  Existing single-label
/// databases will never have entries of this type; the catalog is fully
/// backward-compatible.
///
/// ```text
/// primary_label_id: u16
/// slot:             u32
/// count:            u16
/// secondary_label_ids: [u16; count]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLabelSetEntry {
    /// Primary label id (determines NodeId encoding and storage directory).
    pub primary_label_id: u16,
    /// Slot within the primary label's column store.
    pub slot: u32,
    /// Secondary label ids (all labels beyond the primary).
    pub secondary_label_ids: Vec<u16>,
}

impl TlvEntry {
    /// Encode this entry into its wire bytes (tag + length + payload).
    pub fn encode(&self) -> Vec<u8> {
        let (tag, payload) = match self {
            TlvEntry::Label(e) => (TAG_LABEL, encode_label(e)),
            TlvEntry::RelTable(e) => (TAG_REL_TABLE, encode_rel_table(e)),
            TlvEntry::Column(e) => (TAG_COLUMN, encode_column(e)),
            TlvEntry::ReverseIndex(e) => (TAG_REVERSE_INDEX, encode_reverse_index(e)),
            TlvEntry::FormatMeta(e) => (TAG_FORMAT_META, encode_format_meta(e)),
            TlvEntry::NodeLabelSet(e) => (TAG_NODE_LABEL_SET, encode_node_label_set(e)),
        };
        let mut out = Vec::with_capacity(6 + payload.len());
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Decode a single TLV entry from `data`, returning the entry and the number of bytes consumed.
    pub fn decode_one(data: &[u8]) -> Result<(TlvEntry, usize)> {
        if data.len() < 6 {
            return Err(Error::Corruption(
                "TLV entry too short for header".to_string(),
            ));
        }
        let tag = u16::from_le_bytes(data[0..2].try_into().unwrap());
        let length = u32::from_le_bytes(data[2..6].try_into().unwrap()) as usize;
        if data.len() < 6 + length {
            return Err(Error::Corruption(format!(
                "TLV entry truncated: need {} payload bytes, have {}",
                length,
                data.len() - 6
            )));
        }
        let payload = &data[6..6 + length];
        let entry = match tag {
            TAG_LABEL => TlvEntry::Label(decode_label(payload)?),
            TAG_REL_TABLE => TlvEntry::RelTable(decode_rel_table(payload)?),
            TAG_COLUMN => TlvEntry::Column(decode_column(payload)?),
            TAG_REVERSE_INDEX => TlvEntry::ReverseIndex(decode_reverse_index(payload)?),
            TAG_FORMAT_META => TlvEntry::FormatMeta(decode_format_meta(payload)?),
            TAG_NODE_LABEL_SET => TlvEntry::NodeLabelSet(decode_node_label_set(payload)?),
            _ => {
                // Forward-compatible: skip unrecognised tags rather than failing.
                // This allows newer catalog versions to be read by older readers
                // without corruption errors.
                return Err(Error::InvalidArgument(format!(
                    "unknown TLV tag: 0x{tag:04X}"
                )));
            }
        };
        Ok((entry, 6 + length))
    }

    /// Decode all TLV entries from `data`.
    pub fn decode_all(mut data: &[u8]) -> Result<Vec<TlvEntry>> {
        let mut entries = Vec::new();
        while !data.is_empty() {
            // Skip zero bytes (padding)
            if data[0] == 0 {
                data = &data[1..];
                continue;
            }
            let (entry, consumed) = TlvEntry::decode_one(data)?;
            entries.push(entry);
            data = &data[consumed..];
        }
        Ok(entries)
    }
}

// --- Payload encoders ---

fn encode_label(e: &LabelEntry) -> Vec<u8> {
    let name_bytes = e.name.as_bytes();
    let mut out = Vec::with_capacity(4 + name_bytes.len());
    out.extend_from_slice(&e.label_id.to_le_bytes());
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(name_bytes);
    out
}

fn encode_rel_table(e: &RelTableEntry) -> Vec<u8> {
    let rel_bytes = e.rel_type.as_bytes();
    let mut out = Vec::with_capacity(14 + rel_bytes.len());
    out.extend_from_slice(&e.rel_table_id.to_le_bytes());
    out.extend_from_slice(&e.src_label_id.to_le_bytes());
    out.extend_from_slice(&e.dst_label_id.to_le_bytes());
    out.extend_from_slice(&(rel_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(rel_bytes);
    out
}

fn encode_column(e: &ColumnEntry) -> Vec<u8> {
    let name_bytes = e.name.as_bytes();
    let mut out = Vec::with_capacity(20 + name_bytes.len() + e.default_bytes.len());
    out.push(e.owner_kind);
    out.extend_from_slice(&e.owner_id.to_le_bytes());
    out.extend_from_slice(&e.field_id.to_le_bytes());
    out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(name_bytes);
    out.push(e.type_tag);
    out.push(e.nullable);
    out.push(e.has_default);
    out.extend_from_slice(&(e.default_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&e.default_bytes);
    out
}

fn encode_reverse_index(e: &ReverseIndexEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + e.owner_label_ids.len() * 2);
    out.extend_from_slice(&e.secondary_label_id.to_le_bytes());
    out.extend_from_slice(&(e.owner_label_ids.len() as u16).to_le_bytes());
    for id in &e.owner_label_ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

fn encode_format_meta(e: &FormatMetaEntry) -> Vec<u8> {
    e.version.to_le_bytes().to_vec()
}

fn encode_node_label_set(e: &NodeLabelSetEntry) -> Vec<u8> {
    // primary_label_id (2) + slot (4) + count (2) + secondary_label_ids (count*2)
    let mut out = Vec::with_capacity(8 + e.secondary_label_ids.len() * 2);
    out.extend_from_slice(&e.primary_label_id.to_le_bytes());
    out.extend_from_slice(&e.slot.to_le_bytes());
    out.extend_from_slice(&(e.secondary_label_ids.len() as u16).to_le_bytes());
    for id in &e.secondary_label_ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

// --- Payload decoders ---

fn decode_label(p: &[u8]) -> Result<LabelEntry> {
    if p.len() < 4 {
        return Err(Error::Corruption("label payload too short".to_string()));
    }
    let label_id = u16::from_le_bytes(p[0..2].try_into().unwrap());
    let name_len = u16::from_le_bytes(p[2..4].try_into().unwrap()) as usize;
    if p.len() < 4 + name_len {
        return Err(Error::Corruption("label name truncated".to_string()));
    }
    let name = String::from_utf8(p[4..4 + name_len].to_vec())
        .map_err(|_| Error::Corruption("label name is not valid UTF-8".to_string()))?;
    Ok(LabelEntry { label_id, name })
}

fn decode_rel_table(p: &[u8]) -> Result<RelTableEntry> {
    if p.len() < 14 {
        return Err(Error::Corruption("rel_table payload too short".to_string()));
    }
    let rel_table_id = u64::from_le_bytes(p[0..8].try_into().unwrap());
    let src_label_id = u16::from_le_bytes(p[8..10].try_into().unwrap());
    let dst_label_id = u16::from_le_bytes(p[10..12].try_into().unwrap());
    let rel_type_len = u16::from_le_bytes(p[12..14].try_into().unwrap()) as usize;
    if p.len() < 14 + rel_type_len {
        return Err(Error::Corruption("rel_type string truncated".to_string()));
    }
    let rel_type = String::from_utf8(p[14..14 + rel_type_len].to_vec())
        .map_err(|_| Error::Corruption("rel_type is not valid UTF-8".to_string()))?;
    Ok(RelTableEntry {
        rel_table_id,
        src_label_id,
        dst_label_id,
        rel_type,
    })
}

fn decode_column(p: &[u8]) -> Result<ColumnEntry> {
    if p.len() < 17 {
        return Err(Error::Corruption("column payload too short".to_string()));
    }
    let owner_kind = p[0];
    let owner_id = u64::from_le_bytes(p[1..9].try_into().unwrap());
    let field_id = u32::from_le_bytes(p[9..13].try_into().unwrap());
    let name_len = u16::from_le_bytes(p[13..15].try_into().unwrap()) as usize;
    if p.len() < 15 + name_len + 3 {
        return Err(Error::Corruption("column name truncated".to_string()));
    }
    let name = String::from_utf8(p[15..15 + name_len].to_vec())
        .map_err(|_| Error::Corruption("column name is not valid UTF-8".to_string()))?;
    let base = 15 + name_len;
    if p.len() < base + 7 {
        return Err(Error::Corruption(
            "column payload truncated after name".to_string(),
        ));
    }
    let type_tag = p[base];
    let nullable = p[base + 1];
    let has_default = p[base + 2];
    let default_len = u32::from_le_bytes(p[base + 3..base + 7].try_into().unwrap()) as usize;
    if p.len() < base + 7 + default_len {
        return Err(Error::Corruption(
            "column default bytes truncated".to_string(),
        ));
    }
    let default_bytes = p[base + 7..base + 7 + default_len].to_vec();
    Ok(ColumnEntry {
        owner_kind,
        owner_id,
        field_id,
        name,
        type_tag,
        nullable,
        has_default,
        default_bytes,
    })
}

fn decode_reverse_index(p: &[u8]) -> Result<ReverseIndexEntry> {
    if p.len() < 4 {
        return Err(Error::Corruption(
            "reverse_index payload too short".to_string(),
        ));
    }
    let secondary_label_id = u16::from_le_bytes(p[0..2].try_into().unwrap());
    let owner_count = u16::from_le_bytes(p[2..4].try_into().unwrap()) as usize;
    if p.len() < 4 + owner_count * 2 {
        return Err(Error::Corruption(
            "reverse_index owner_label_ids truncated".to_string(),
        ));
    }
    let mut owner_label_ids = Vec::with_capacity(owner_count);
    for i in 0..owner_count {
        let off = 4 + i * 2;
        owner_label_ids.push(u16::from_le_bytes(p[off..off + 2].try_into().unwrap()));
    }
    Ok(ReverseIndexEntry {
        secondary_label_id,
        owner_label_ids,
    })
}

fn decode_format_meta(p: &[u8]) -> Result<FormatMetaEntry> {
    if p.len() < 2 {
        return Err(Error::Corruption(
            "format_meta payload too short".to_string(),
        ));
    }
    let version = u16::from_le_bytes(p[0..2].try_into().unwrap());
    Ok(FormatMetaEntry { version })
}

fn decode_node_label_set(p: &[u8]) -> Result<NodeLabelSetEntry> {
    // primary_label_id (2) + slot (4) + count (2) = 8 bytes minimum
    if p.len() < 8 {
        return Err(Error::Corruption(
            "node_label_set payload too short".to_string(),
        ));
    }
    let primary_label_id = u16::from_le_bytes(p[0..2].try_into().unwrap());
    let slot = u32::from_le_bytes(p[2..6].try_into().unwrap());
    let count = u16::from_le_bytes(p[6..8].try_into().unwrap()) as usize;
    if p.len() < 8 + count * 2 {
        return Err(Error::Corruption(
            "node_label_set secondary_label_ids truncated".to_string(),
        ));
    }
    let mut secondary_label_ids = Vec::with_capacity(count);
    for i in 0..count {
        let off = 8 + i * 2;
        secondary_label_ids.push(u16::from_le_bytes(p[off..off + 2].try_into().unwrap()));
    }
    Ok(NodeLabelSetEntry {
        primary_label_id,
        slot,
        secondary_label_ids,
    })
}

/// Encode a sequence of TLV entries into a contiguous byte buffer.
pub fn encode_entries(entries: &[TlvEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in entries {
        out.extend_from_slice(&entry.encode());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tlv_round_trip_label() {
        let entry = TlvEntry::Label(LabelEntry {
            label_id: 1,
            name: "Person".to_string(),
        });
        let encoded = entry.encode();
        let (decoded, consumed) = TlvEntry::decode_one(&encoded).expect("decode must succeed");
        assert_eq!(decoded, entry);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_tlv_round_trip_rel_table() {
        let entry = TlvEntry::RelTable(RelTableEntry {
            rel_table_id: 1,
            src_label_id: 0,
            dst_label_id: 1,
            rel_type: "KNOWS".to_string(),
        });
        let encoded = entry.encode();
        let (decoded, consumed) = TlvEntry::decode_one(&encoded).expect("decode must succeed");
        assert_eq!(decoded, entry);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_tlv_round_trip_column() {
        let entry = TlvEntry::Column(ColumnEntry {
            owner_kind: 0,
            owner_id: 42,
            field_id: 0,
            name: "name".to_string(),
            type_tag: 3, // String
            nullable: 0,
            has_default: 0,
            default_bytes: vec![],
        });
        let encoded = entry.encode();
        let (decoded, _) = TlvEntry::decode_one(&encoded).expect("decode must succeed");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_tlv_round_trip_reverse_index() {
        let entry = TlvEntry::ReverseIndex(ReverseIndexEntry {
            secondary_label_id: 5,
            owner_label_ids: vec![1, 2, 3],
        });
        let encoded = entry.encode();
        let (decoded, _) = TlvEntry::decode_one(&encoded).expect("decode must succeed");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_tlv_round_trip_format_meta() {
        let entry = TlvEntry::FormatMeta(FormatMetaEntry { version: 1 });
        let encoded = entry.encode();
        let (decoded, _) = TlvEntry::decode_one(&encoded).expect("decode must succeed");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_tlv_decode_all_multiple_entries() {
        let entries = vec![
            TlvEntry::Label(LabelEntry {
                label_id: 0,
                name: "Person".to_string(),
            }),
            TlvEntry::Label(LabelEntry {
                label_id: 1,
                name: "Movie".to_string(),
            }),
            TlvEntry::RelTable(RelTableEntry {
                rel_table_id: 0,
                src_label_id: 0,
                dst_label_id: 1,
                rel_type: "ACTED_IN".to_string(),
            }),
        ];
        let encoded = encode_entries(&entries);
        let decoded = TlvEntry::decode_all(&encoded).expect("decode_all must succeed");
        assert_eq!(decoded, entries);
    }

    #[test]
    fn test_tlv_tag_bytes_are_correct() {
        // Verify the first 2 bytes of each encoded entry match the expected tag
        let label = TlvEntry::Label(LabelEntry {
            label_id: 0,
            name: "X".to_string(),
        })
        .encode();
        assert_eq!(&label[0..2], &TAG_LABEL.to_le_bytes());

        let rel = TlvEntry::RelTable(RelTableEntry {
            rel_table_id: 0,
            src_label_id: 0,
            dst_label_id: 0,
            rel_type: "R".to_string(),
        })
        .encode();
        assert_eq!(&rel[0..2], &TAG_REL_TABLE.to_le_bytes());

        let fmt = TlvEntry::FormatMeta(FormatMetaEntry { version: 1 }).encode();
        assert_eq!(&fmt[0..2], &TAG_FORMAT_META.to_le_bytes());
    }

    #[test]
    fn test_catalog_tlv_golden_fixture() {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests/fixtures/catalog_tlv.bin");
        let data = std::fs::read(&fixture_path).expect("catalog_tlv.bin fixture must exist");

        let decoded = TlvEntry::decode_all(&data).expect("fixture must decode cleanly");

        // Fixture contains: 3 label entries + 2 rel table entries
        let label_count = decoded
            .iter()
            .filter(|e| matches!(e, TlvEntry::Label(_)))
            .count();
        let rel_count = decoded
            .iter()
            .filter(|e| matches!(e, TlvEntry::RelTable(_)))
            .count();
        assert_eq!(label_count, 3, "fixture must have 3 label entries");
        assert_eq!(rel_count, 2, "fixture must have 2 rel table entries");

        // Round-trip: re-encode all entries, compare byte-exact
        let re_encoded = encode_entries(&decoded);
        assert_eq!(
            re_encoded, data,
            "re-encoded TLV must be byte-exact match to fixture"
        );
    }
}
