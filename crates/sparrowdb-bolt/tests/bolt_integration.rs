//! Integration test: start a Bolt server, connect with a raw TCP client,
//! execute Cypher queries, and verify results.

use bytes::{Buf, BufMut, BytesMut};
use sparrowdb::GraphDb;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ── Minimal PackStream helpers (client-side) ────────────────────────

fn encode_struct_header(buf: &mut BytesMut, n_fields: u8, sig: u8) {
    buf.put_u8(0xB0 | n_fields);
    buf.put_u8(sig);
}

fn encode_string(buf: &mut BytesMut, s: &str) {
    let len = s.len();
    if len < 16 {
        buf.put_u8(0x80 | len as u8);
    } else if len <= 255 {
        buf.put_u8(0xD0);
        buf.put_u8(len as u8);
    } else {
        buf.put_u8(0xD1);
        buf.put_u16(len as u16);
    }
    buf.extend_from_slice(s.as_bytes());
}

fn encode_map_header(buf: &mut BytesMut, len: usize) {
    if len < 16 {
        buf.put_u8(0xA0 | len as u8);
    } else {
        buf.put_u8(0xD8);
        buf.put_u8(len as u8);
    }
}

fn encode_tiny_map(buf: &mut BytesMut, entries: &[(&str, &str)]) {
    encode_map_header(buf, entries.len());
    for (k, v) in entries {
        encode_string(buf, k);
        encode_string(buf, v);
    }
}

fn encode_empty_map(buf: &mut BytesMut) {
    buf.put_u8(0xA0); // tiny map, 0 entries
}

/// Send a chunked message.
async fn send_msg(stream: &mut TcpStream, data: &[u8]) {
    let mut out = BytesMut::new();
    out.put_u16(data.len() as u16);
    out.extend_from_slice(data);
    out.put_u16(0); // end marker
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
}

/// Read one chunked message.
async fn recv_msg(stream: &mut TcpStream) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await.unwrap();
        let chunk_len = u16::from_be_bytes(len_buf) as usize;
        if chunk_len == 0 {
            break;
        }
        let mut chunk = vec![0u8; chunk_len];
        stream.read_exact(&mut chunk).await.unwrap();
        result.extend_from_slice(&chunk);
    }
    result
}

/// Decode a struct header from raw bytes.
fn decode_struct_header(data: &[u8]) -> (u8, u8, usize) {
    let marker = data[0];
    let n_fields = marker & 0x0F;
    let sig = data[1];
    (n_fields, sig, 2)
}

/// Simple value decoder for test assertions.
#[derive(Debug, Clone, PartialEq)]
enum TestValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<TestValue>),
    Map(HashMap<String, TestValue>),
}

fn decode_value(buf: &mut &[u8]) -> TestValue {
    if buf.is_empty() {
        panic!("unexpected end of data");
    }
    let marker = buf[0];
    match marker {
        0xC0 => {
            buf.advance(1);
            TestValue::Null
        }
        0xC2 => {
            buf.advance(1);
            TestValue::Bool(false)
        }
        0xC3 => {
            buf.advance(1);
            TestValue::Bool(true)
        }
        0xC1 => {
            buf.advance(1);
            TestValue::Float(buf.get_f64())
        }
        0xC8 => {
            buf.advance(1);
            TestValue::Int(buf.get_i8() as i64)
        }
        0xC9 => {
            buf.advance(1);
            TestValue::Int(buf.get_i16() as i64)
        }
        0xCA => {
            buf.advance(1);
            TestValue::Int(buf.get_i32() as i64)
        }
        0xCB => {
            buf.advance(1);
            TestValue::Int(buf.get_i64())
        }
        m if (0x80..=0x8F).contains(&m) => {
            buf.advance(1);
            let len = (m & 0x0F) as usize;
            let s = std::str::from_utf8(&buf[..len]).unwrap().to_string();
            buf.advance(len);
            TestValue::Str(s)
        }
        0xD0 => {
            buf.advance(1);
            let len = buf.get_u8() as usize;
            let s = std::str::from_utf8(&buf[..len]).unwrap().to_string();
            buf.advance(len);
            TestValue::Str(s)
        }
        0xD1 => {
            buf.advance(1);
            let len = buf.get_u16() as usize;
            let s = std::str::from_utf8(&buf[..len]).unwrap().to_string();
            buf.advance(len);
            TestValue::Str(s)
        }
        m if (0x90..=0x9F).contains(&m) => {
            buf.advance(1);
            let len = (m & 0x0F) as usize;
            let mut items = Vec::new();
            for _ in 0..len {
                items.push(decode_value(buf));
            }
            TestValue::List(items)
        }
        m if (0xA0..=0xAF).contains(&m) => {
            buf.advance(1);
            let len = (m & 0x0F) as usize;
            let mut map = HashMap::new();
            for _ in 0..len {
                let key = match decode_value(buf) {
                    TestValue::Str(s) => s,
                    other => panic!("expected string key, got {other:?}"),
                };
                let val = decode_value(buf);
                map.insert(key, val);
            }
            TestValue::Map(map)
        }
        0xD8 => {
            buf.advance(1);
            let len = buf.get_u8() as usize;
            let mut map = HashMap::new();
            for _ in 0..len {
                let key = match decode_value(buf) {
                    TestValue::Str(s) => s,
                    other => panic!("expected string key, got {other:?}"),
                };
                let val = decode_value(buf);
                map.insert(key, val);
            }
            TestValue::Map(map)
        }
        // Struct — decode as list of fields (skip sig)
        m if (0xB0..=0xBF).contains(&m) => {
            buf.advance(1);
            let n = (m & 0x0F) as usize;
            let _sig = buf.get_u8();
            let mut items = Vec::new();
            for _ in 0..n {
                items.push(decode_value(buf));
            }
            TestValue::List(items)
        }
        // Tiny int
        m => {
            buf.advance(1);
            TestValue::Int(m as i8 as i64)
        }
    }
}

/// Receive a message and return its signature + decoded fields.
async fn recv_and_decode(stream: &mut TcpStream) -> (u8, Vec<TestValue>) {
    let raw = recv_msg(stream).await;
    let (n_fields, sig, offset) = decode_struct_header(&raw);
    let mut slice: &[u8] = &raw[offset..];
    let mut fields = Vec::new();
    for _ in 0..n_fields {
        fields.push(decode_value(&mut slice));
    }
    (sig, fields)
}

const SIG_SUCCESS: u8 = 0x70;
const SIG_RECORD: u8 = 0x71;
const SIG_FAILURE: u8 = 0x7F;

/// Start the server on a random port, return the port.
async fn start_server(db: GraphDb) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let db = db.clone();
                    tokio::spawn(async move {
                        // Import the handler module via the binary crate.
                        // Since we can't import from a bin crate, we'll inline
                        // the connection handler via the library path.
                        sparrowdb_bolt::handle_connection(stream, db).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    port
}

/// Perform the Bolt handshake.
async fn do_handshake(stream: &mut TcpStream) {
    // Send preamble: magic + 4 version proposals
    let mut preamble = Vec::new();
    preamble.extend_from_slice(&[0x60, 0x60, 0xB0, 0x17]); // magic
                                                           // Version proposal 1: Bolt 4.4 (range=4, minor=0, major=4, padding=0)
    preamble.extend_from_slice(&[4, 0, 4, 0]);
    // Remaining 3 proposals: zeroes
    preamble.extend_from_slice(&[0, 0, 0, 0]);
    preamble.extend_from_slice(&[0, 0, 0, 0]);
    preamble.extend_from_slice(&[0, 0, 0, 0]);
    stream.write_all(&preamble).await.unwrap();

    // Read 4-byte version response
    let mut version = [0u8; 4];
    stream.read_exact(&mut version).await.unwrap();
    assert_eq!(version[2], 4, "should negotiate Bolt 4.x");
}

/// Send HELLO message.
async fn do_hello(stream: &mut TcpStream) {
    let mut buf = BytesMut::new();
    encode_struct_header(&mut buf, 1, 0x01); // HELLO
    encode_tiny_map(&mut buf, &[("user_agent", "sparrowdb-test/1.0")]);
    send_msg(stream, &buf).await;

    let (sig, _) = recv_and_decode(stream).await;
    assert_eq!(sig, SIG_SUCCESS, "HELLO should return SUCCESS");
}

/// Send RUN + PULL and return all records.
async fn run_query(stream: &mut TcpStream, query: &str) -> (u8, Vec<Vec<TestValue>>) {
    // RUN
    let mut buf = BytesMut::new();
    encode_struct_header(&mut buf, 2, 0x10); // RUN
    encode_string(&mut buf, query);
    encode_empty_map(&mut buf); // params
    send_msg(stream, &buf).await;

    let (sig, _fields) = recv_and_decode(stream).await;
    if sig == SIG_FAILURE {
        return (sig, vec![]);
    }
    assert_eq!(sig, SIG_SUCCESS, "RUN should return SUCCESS");

    // PULL
    let mut buf = BytesMut::new();
    encode_struct_header(&mut buf, 1, 0x3F); // PULL
    encode_empty_map(&mut buf); // metadata
    send_msg(stream, &buf).await;

    // Read records
    let mut records = Vec::new();
    loop {
        let (sig, fields) = recv_and_decode(stream).await;
        if sig == SIG_RECORD {
            // RECORD has one field: a list of values
            if let Some(TestValue::List(row)) = fields.into_iter().next() {
                records.push(row);
            }
        } else {
            // SUCCESS or FAILURE
            return (sig, records);
        }
    }
}

#[tokio::test]
async fn test_bolt_create_and_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let port = start_server(db).await;

    // Give server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    do_handshake(&mut stream).await;
    do_hello(&mut stream).await;

    // CREATE a node
    let (sig, records) =
        run_query(&mut stream, "CREATE (n:Test {name: 'hello'}) RETURN n.name").await;
    assert_eq!(sig, SIG_SUCCESS, "CREATE query should succeed");
    assert_eq!(records.len(), 1, "should return 1 row");
    assert_eq!(
        records[0],
        vec![TestValue::Str("hello".into())],
        "should return the name"
    );

    // MATCH the node
    let (sig, records) = run_query(&mut stream, "MATCH (n:Test) RETURN n.name").await;
    assert_eq!(sig, SIG_SUCCESS, "MATCH query should succeed");
    assert_eq!(records.len(), 1, "should find 1 node");
    assert_eq!(
        records[0],
        vec![TestValue::Str("hello".into())],
        "should return the name"
    );

    // Send GOODBYE
    let mut buf = BytesMut::new();
    encode_struct_header(&mut buf, 0, 0x02); // GOODBYE
    send_msg(&mut stream, &buf).await;
}

#[tokio::test]
async fn test_bolt_error_handling() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let port = start_server(db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    do_handshake(&mut stream).await;
    do_hello(&mut stream).await;

    // Run an invalid query
    let (sig, _) = run_query(&mut stream, "THIS IS NOT VALID CYPHER").await;
    assert_eq!(sig, SIG_FAILURE, "invalid query should return FAILURE");

    // After FAILURE, need RESET to continue
    let mut buf = BytesMut::new();
    encode_struct_header(&mut buf, 0, 0x0F); // RESET
    send_msg(&mut stream, &buf).await;

    let (sig, _) = recv_and_decode(&mut stream).await;
    assert_eq!(sig, SIG_SUCCESS, "RESET should return SUCCESS");

    // Now we should be able to run queries again
    let (sig, records) = run_query(&mut stream, "RETURN 42 AS num").await;
    assert_eq!(sig, SIG_SUCCESS);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], vec![TestValue::Int(42)]);
}
