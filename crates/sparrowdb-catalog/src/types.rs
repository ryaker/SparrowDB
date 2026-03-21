/// Property type tags (§10.2.2 column definition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeTag {
    Bool = 0,
    Int64 = 1,
    Float64 = 2,
    String = 3,
    Bytes = 4,
}

impl TypeTag {
    /// Encode as a single byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Decode from a byte.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(TypeTag::Bool),
            1 => Some(TypeTag::Int64),
            2 => Some(TypeTag::Float64),
            3 => Some(TypeTag::String),
            4 => Some(TypeTag::Bytes),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_tag_roundtrip() {
        for (expected, byte) in [
            (TypeTag::Bool, 0u8),
            (TypeTag::Int64, 1),
            (TypeTag::Float64, 2),
            (TypeTag::String, 3),
            (TypeTag::Bytes, 4),
        ] {
            assert_eq!(expected.as_u8(), byte);
            assert_eq!(TypeTag::from_u8(byte), Some(expected));
        }
        assert_eq!(TypeTag::from_u8(5), None);
        assert_eq!(TypeTag::from_u8(255), None);
    }
}
