//! PackStream v1 encoding / decoding.
//!
//! PackStream is a binary serialization format used by the Bolt protocol.
//! Reference: <https://neo4j.com/docs/bolt/current/packstream/>

use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

// ── Value type ──────────────────────────────────────────────────────

/// A Bolt-compatible value.
#[derive(Debug, Clone, PartialEq)]
pub enum BoltValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    List(Vec<BoltValue>),
    Map(HashMap<String, BoltValue>),
}

// ── PackStream markers ──────────────────────────────────────────────

const TINY_STRING_NIBBLE: u8 = 0x80;
const TINY_LIST_NIBBLE: u8 = 0x90;
const TINY_MAP_NIBBLE: u8 = 0xA0;

const NULL: u8 = 0xC0;
const FLOAT_64: u8 = 0xC1;
const FALSE: u8 = 0xC2;
const TRUE: u8 = 0xC3;
const INT_8: u8 = 0xC8;
const INT_16: u8 = 0xC9;
const INT_32: u8 = 0xCA;
const INT_64: u8 = 0xCB;
const STRING_8: u8 = 0xD0;
const STRING_16: u8 = 0xD1;
const STRING_32: u8 = 0xD2;
const LIST_8: u8 = 0xD4;
const LIST_16: u8 = 0xD5;
const LIST_32: u8 = 0xD6;
const MAP_8: u8 = 0xD8;
const MAP_16: u8 = 0xD9;
const MAP_32: u8 = 0xDA;
const TINY_STRUCT_NIBBLE: u8 = 0xB0;

// Bolt message signature bytes
pub const SIG_SUCCESS: u8 = 0x70;
pub const SIG_FAILURE: u8 = 0x7F;
pub const SIG_RECORD: u8 = 0x71;
pub const SIG_HELLO: u8 = 0x01;
pub const SIG_LOGON: u8 = 0x6A;
pub const SIG_RUN: u8 = 0x10;
pub const SIG_PULL: u8 = 0x3F;
pub const SIG_DISCARD: u8 = 0x2F;
pub const SIG_RESET: u8 = 0x0F;
pub const SIG_GOODBYE: u8 = 0x02;
pub const SIG_BEGIN: u8 = 0x11;
pub const SIG_COMMIT: u8 = 0x12;
pub const SIG_ROLLBACK: u8 = 0x13;

// ── Encoding ────────────────────────────────────────────────────────

pub fn encode_value(buf: &mut BytesMut, val: &BoltValue) {
    match val {
        BoltValue::Null => buf.put_u8(NULL),
        BoltValue::Boolean(b) => buf.put_u8(if *b { TRUE } else { FALSE }),
        BoltValue::Integer(n) => encode_integer(buf, *n),
        BoltValue::Float(f) => {
            buf.put_u8(FLOAT_64);
            buf.put_f64(*f);
        }
        BoltValue::String(s) => encode_string(buf, s),
        BoltValue::List(items) => {
            encode_list_header(buf, items.len());
            for item in items {
                encode_value(buf, item);
            }
        }
        BoltValue::Map(map) => {
            encode_map_header(buf, map.len());
            for (k, v) in map {
                encode_string(buf, k);
                encode_value(buf, v);
            }
        }
    }
}

fn encode_integer(buf: &mut BytesMut, n: i64) {
    if (-16..=127).contains(&n) {
        buf.put_i8(n as i8);
    } else if i8::MIN as i64 <= n && n <= i8::MAX as i64 {
        buf.put_u8(INT_8);
        buf.put_i8(n as i8);
    } else if i16::MIN as i64 <= n && n <= i16::MAX as i64 {
        buf.put_u8(INT_16);
        buf.put_i16(n as i16);
    } else if i32::MIN as i64 <= n && n <= i32::MAX as i64 {
        buf.put_u8(INT_32);
        buf.put_i32(n as i32);
    } else {
        buf.put_u8(INT_64);
        buf.put_i64(n);
    }
}

fn encode_string(buf: &mut BytesMut, s: &str) {
    let len = s.len();
    if len < 16 {
        buf.put_u8(TINY_STRING_NIBBLE | len as u8);
    } else if len <= 255 {
        buf.put_u8(STRING_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(STRING_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(STRING_32);
        buf.put_u32(len as u32);
    }
    buf.extend_from_slice(s.as_bytes());
}

fn encode_list_header(buf: &mut BytesMut, len: usize) {
    if len < 16 {
        buf.put_u8(TINY_LIST_NIBBLE | len as u8);
    } else if len <= 255 {
        buf.put_u8(LIST_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(LIST_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(LIST_32);
        buf.put_u32(len as u32);
    }
}

fn encode_map_header(buf: &mut BytesMut, len: usize) {
    if len < 16 {
        buf.put_u8(TINY_MAP_NIBBLE | len as u8);
    } else if len <= 255 {
        buf.put_u8(MAP_8);
        buf.put_u8(len as u8);
    } else if len <= 65535 {
        buf.put_u8(MAP_16);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(MAP_32);
        buf.put_u32(len as u32);
    }
}

/// Encode a Bolt struct header (marker + signature byte).
pub fn encode_struct_header(buf: &mut BytesMut, num_fields: u8, signature: u8) {
    buf.put_u8(TINY_STRUCT_NIBBLE | num_fields);
    buf.put_u8(signature);
}

// ── Decoding ────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum DecodeError {
    UnexpectedEnd,
    InvalidMarker(u8),
    InvalidUtf8,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnexpectedEnd => write!(f, "unexpected end of data"),
            DecodeError::InvalidMarker(m) => write!(f, "invalid marker: 0x{m:02X}"),
            DecodeError::InvalidUtf8 => write!(f, "invalid UTF-8 in string"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn decode_value(buf: &mut &[u8]) -> Result<BoltValue, DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::UnexpectedEnd);
    }
    let marker = buf[0];
    match marker {
        NULL => {
            buf.advance(1);
            Ok(BoltValue::Null)
        }
        TRUE => {
            buf.advance(1);
            Ok(BoltValue::Boolean(true))
        }
        FALSE => {
            buf.advance(1);
            Ok(BoltValue::Boolean(false))
        }
        FLOAT_64 => {
            buf.advance(1);
            if buf.remaining() < 8 {
                return Err(DecodeError::UnexpectedEnd);
            }
            Ok(BoltValue::Float(buf.get_f64()))
        }
        INT_8 => {
            buf.advance(1);
            if buf.remaining() < 1 {
                return Err(DecodeError::UnexpectedEnd);
            }
            Ok(BoltValue::Integer(buf.get_i8() as i64))
        }
        INT_16 => {
            buf.advance(1);
            if buf.remaining() < 2 {
                return Err(DecodeError::UnexpectedEnd);
            }
            Ok(BoltValue::Integer(buf.get_i16() as i64))
        }
        INT_32 => {
            buf.advance(1);
            if buf.remaining() < 4 {
                return Err(DecodeError::UnexpectedEnd);
            }
            Ok(BoltValue::Integer(buf.get_i32() as i64))
        }
        INT_64 => {
            buf.advance(1);
            if buf.remaining() < 8 {
                return Err(DecodeError::UnexpectedEnd);
            }
            Ok(BoltValue::Integer(buf.get_i64()))
        }
        // Tiny int: -16..=127 stored directly
        m if (m as i8) >= -16 && !matches!(m, 0x80..=0xEF) => {
            buf.advance(1);
            Ok(BoltValue::Integer(m as i8 as i64))
        }
        // Tiny string: 0x80..=0x8F
        m if (0x80..=0x8F).contains(&m) => {
            buf.advance(1);
            let len = (m & 0x0F) as usize;
            decode_string_body(buf, len)
        }
        STRING_8 => {
            buf.advance(1);
            if buf.remaining() < 1 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u8() as usize;
            decode_string_body(buf, len)
        }
        STRING_16 => {
            buf.advance(1);
            if buf.remaining() < 2 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u16() as usize;
            decode_string_body(buf, len)
        }
        STRING_32 => {
            buf.advance(1);
            if buf.remaining() < 4 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u32() as usize;
            decode_string_body(buf, len)
        }
        // Tiny list: 0x90..=0x9F
        m if (0x90..=0x9F).contains(&m) => {
            buf.advance(1);
            let len = (m & 0x0F) as usize;
            decode_list_body(buf, len)
        }
        LIST_8 => {
            buf.advance(1);
            if buf.remaining() < 1 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u8() as usize;
            decode_list_body(buf, len)
        }
        LIST_16 => {
            buf.advance(1);
            if buf.remaining() < 2 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u16() as usize;
            decode_list_body(buf, len)
        }
        LIST_32 => {
            buf.advance(1);
            if buf.remaining() < 4 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u32() as usize;
            decode_list_body(buf, len)
        }
        // Tiny map: 0xA0..=0xAF
        m if (0xA0..=0xAF).contains(&m) => {
            buf.advance(1);
            let len = (m & 0x0F) as usize;
            decode_map_body(buf, len)
        }
        MAP_8 => {
            buf.advance(1);
            if buf.remaining() < 1 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u8() as usize;
            decode_map_body(buf, len)
        }
        MAP_16 => {
            buf.advance(1);
            if buf.remaining() < 2 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u16() as usize;
            decode_map_body(buf, len)
        }
        MAP_32 => {
            buf.advance(1);
            if buf.remaining() < 4 {
                return Err(DecodeError::UnexpectedEnd);
            }
            let len = buf.get_u32() as usize;
            decode_map_body(buf, len)
        }
        // Tiny struct: 0xB0..=0xBF — decode as map (skip signature)
        m if (0xB0..=0xBF).contains(&m) => {
            buf.advance(1);
            let n_fields = (m & 0x0F) as usize;
            // skip signature byte
            if buf.is_empty() {
                return Err(DecodeError::UnexpectedEnd);
            }
            buf.advance(1);
            // decode fields as a list (for struct values)
            let mut items = Vec::with_capacity(n_fields);
            for _ in 0..n_fields {
                items.push(decode_value(buf)?);
            }
            Ok(BoltValue::List(items))
        }
        other => Err(DecodeError::InvalidMarker(other)),
    }
}

fn decode_string_body(buf: &mut &[u8], len: usize) -> Result<BoltValue, DecodeError> {
    if buf.remaining() < len {
        return Err(DecodeError::UnexpectedEnd);
    }
    let bytes = &buf[..len];
    let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
    let result = s.to_string();
    buf.advance(len);
    Ok(BoltValue::String(result))
}

/// Maximum number of elements pre-allocated from an untrusted network length.
/// Larger collections grow dynamically via `push`.
const MAX_PREALLOC: usize = 64;

fn decode_list_body(buf: &mut &[u8], len: usize) -> Result<BoltValue, DecodeError> {
    let mut items = Vec::with_capacity(len.min(MAX_PREALLOC));
    for _ in 0..len {
        items.push(decode_value(buf)?);
    }
    Ok(BoltValue::List(items))
}

fn decode_map_body(buf: &mut &[u8], len: usize) -> Result<BoltValue, DecodeError> {
    let mut map = HashMap::with_capacity(len.min(MAX_PREALLOC));
    for _ in 0..len {
        let key = match decode_value(buf)? {
            BoltValue::String(s) => s,
            _ => return Err(DecodeError::InvalidMarker(0)),
        };
        let val = decode_value(buf)?;
        map.insert(key, val);
    }
    Ok(BoltValue::Map(map))
}

/// Decode a struct header: returns (num_fields, signature).
pub fn decode_struct_header(buf: &mut &[u8]) -> Result<(u8, u8), DecodeError> {
    if buf.remaining() < 2 {
        return Err(DecodeError::UnexpectedEnd);
    }
    let marker = buf.get_u8();
    if marker & 0xF0 != TINY_STRUCT_NIBBLE {
        return Err(DecodeError::InvalidMarker(marker));
    }
    let n_fields = marker & 0x0F;
    let signature = buf.get_u8();
    Ok((n_fields, signature))
}
