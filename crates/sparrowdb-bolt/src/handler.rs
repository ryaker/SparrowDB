//! Per-connection Bolt message handler.
//!
//! Implements the Bolt v4.x state machine: CONNECTED → READY → STREAMING.

use crate::packstream::{self, BoltValue};
use bytes::{BufMut, BytesMut};
use sparrowdb::GraphDb;
use sparrowdb_execution::{QueryResult, Value};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Duration;

/// Maximum total size of a single chunked message (4 MiB).
const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Maximum size of a single outgoing chunk (Bolt protocol limit).
const MAX_CHUNK_SIZE: usize = 65_535;

/// Timeout for the initial Bolt handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bolt magic preamble (4 bytes).
const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// State machine for a single connection.
enum State {
    Connected,
    Ready,
    Streaming { result: QueryResult, cursor: usize },
    Failed,
}

pub async fn handle_connection(mut stream: TcpStream, db: GraphDb) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    tracing::info!("new connection from {peer}");

    if let Err(e) = handle_inner(&mut stream, &db).await {
        tracing::debug!("connection {peer} ended: {e}");
    }
}

async fn handle_inner(
    stream: &mut TcpStream,
    db: &GraphDb,
) -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Handshake ────────────────────────────────────────────────

    let mut preamble = [0u8; 20];
    tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut preamble))
        .await
        .map_err(|_| "handshake timeout")??;

    if preamble[..4] != BOLT_MAGIC {
        return Err("invalid bolt magic".into());
    }

    // Parse 4 version proposals (each 4 bytes).
    // Bolt 4.x version encoding: [range, minor, major, 0]
    // e.g. [4, 0, 4, 0] means versions 4.0 through 4.4
    // We support Bolt 4.x — pick the best match.
    let mut selected: Option<[u8; 4]> = None;
    for i in 0..4 {
        let off = 4 + i * 4;
        let proposal = &preamble[off..off + 4];
        let major = proposal[2] as u16; // 3rd byte = major in Bolt 4+ encoding
        let minor = proposal[1];
        let range = proposal[0];

        if major == 4 {
            // Accept any Bolt 4.x
            selected = Some([0, minor, major as u8, 0]);
            break;
        }
        // Also check the older encoding where major is in byte[3]
        if proposal[3] == 4 || (proposal[3] == 0 && proposal[2] == 4) {
            selected = Some([0, 0, 4, 0]);
            break;
        }
        let _ = range; // version range field — reserved for future use
    }

    let version_bytes = match selected {
        Some(v) => v,
        None => {
            // No supported version — send zeroes and close.
            stream.write_all(&[0, 0, 0, 0]).await?;
            return Ok(());
        }
    };

    stream.write_all(&version_bytes).await?;
    tracing::info!("negotiated Bolt {}.{}", version_bytes[2], version_bytes[1]);

    // ── 2. Message loop ─────────────────────────────────────────────

    let mut state = State::Connected;

    loop {
        // Read one chunked message.
        let msg_bytes = match read_chunked_message(stream).await {
            Ok(b) if b.is_empty() => return Ok(()), // clean close
            Ok(b) => b,
            Err(e) => return Err(e),
        };

        // Decode struct header (signature tells us the message type).
        let mut slice: &[u8] = &msg_bytes;
        let (n_fields, sig) = packstream::decode_struct_header(&mut slice)?;

        // Decode fields.
        let mut fields = Vec::with_capacity(n_fields as usize);
        for _ in 0..n_fields {
            fields.push(packstream::decode_value(&mut slice)?);
        }

        match sig {
            packstream::SIG_HELLO => {
                // Accept any credentials.
                let meta = hello_success_meta();
                send_success(stream, meta).await?;
                state = State::Ready;
            }
            packstream::SIG_LOGON => {
                // Bolt 5.1+ LOGON — accept any credentials.
                send_success(stream, HashMap::new()).await?;
                // stay in Ready
            }
            packstream::SIG_GOODBYE => {
                tracing::debug!("GOODBYE received");
                return Ok(());
            }
            packstream::SIG_RESET => {
                state = State::Ready;
                send_success(stream, HashMap::new()).await?;
            }
            packstream::SIG_RUN => {
                if matches!(state, State::Failed) {
                    send_ignored(stream).await?;
                    continue;
                }

                let query = match fields.first() {
                    Some(BoltValue::String(s)) => s.clone(),
                    _ => {
                        send_failure(stream, "Request.Invalid", "missing query string").await?;
                        state = State::Failed;
                        continue;
                    }
                };

                tracing::debug!("RUN: {query}");

                let db2 = db.clone();
                let exec_result = tokio::task::spawn_blocking(move || db2.execute(&query))
                    .await
                    .map_err(|e| format!("task join error: {e}"))?;

                match exec_result {
                    Ok(result) => {
                        let mut meta = HashMap::new();
                        let field_names: Vec<BoltValue> = result
                            .columns
                            .iter()
                            .map(|c| BoltValue::String(c.clone()))
                            .collect();
                        meta.insert("fields".into(), BoltValue::List(field_names));

                        send_success(stream, meta).await?;
                        state = State::Streaming { result, cursor: 0 };
                    }
                    Err(e) => {
                        tracing::warn!("query error: {e}");
                        send_failure(stream, "SparrowDB.ExecutionError", &e.to_string()).await?;
                        state = State::Failed;
                    }
                }
            }
            packstream::SIG_PULL => {
                if matches!(state, State::Failed) {
                    send_ignored(stream).await?;
                    continue;
                }

                // Extract `n` (batch size) from PULL metadata map, default -1 = all.
                let n_requested: i64 = fields
                    .first()
                    .and_then(|v| {
                        if let BoltValue::Map(m) = v {
                            m.get("n").and_then(|n| {
                                if let BoltValue::Integer(i) = n {
                                    Some(*i)
                                } else {
                                    None
                                }
                            })
                        } else {
                            None
                        }
                    })
                    .unwrap_or(-1);

                match &state {
                    State::Streaming { result, cursor } => {
                        let cursor = *cursor;
                        let total = result.rows.len();
                        let limit = if n_requested < 0 {
                            total - cursor
                        } else {
                            (n_requested as usize).min(total - cursor)
                        };

                        // Buffer all records into a single write before flushing.
                        let end = cursor + limit;
                        let mut combined = BytesMut::new();
                        for row in &result.rows[cursor..end] {
                            let bolt_row: Vec<BoltValue> =
                                row.iter().map(sparrow_to_bolt).collect();
                            append_record(&mut combined, bolt_row);
                        }

                        // SUCCESS metadata
                        let has_more = end < total;
                        let mut meta = HashMap::new();
                        meta.insert("has_more".into(), BoltValue::Boolean(has_more));
                        append_success(&mut combined, meta);

                        write_chunked_batch(stream, &combined).await?;

                        if has_more {
                            state = State::Streaming {
                                result: result.clone(),
                                cursor: end,
                            };
                        } else {
                            state = State::Ready;
                        }
                    }
                    _ => {
                        send_success(stream, HashMap::new()).await?;
                    }
                }
            }
            packstream::SIG_DISCARD => {
                // Discard results — just transition back to READY.
                state = State::Ready;
                let mut meta = HashMap::new();
                meta.insert("has_more".into(), BoltValue::Boolean(false));
                send_success(stream, meta).await?;
            }
            packstream::SIG_BEGIN => {
                // SparrowDB does not support multi-statement transactions.
                // Returning SUCCESS here would give clients a false atomicity
                // guarantee, so we reject BEGIN explicitly.
                send_failure(
                    stream,
                    "SparrowDB.Unsupported",
                    "explicit transactions are not supported; use auto-commit mode",
                )
                .await?;
                state = State::Failed;
            }
            packstream::SIG_COMMIT => {
                send_failure(
                    stream,
                    "SparrowDB.Unsupported",
                    "explicit transactions are not supported; use auto-commit mode",
                )
                .await?;
                state = State::Failed;
            }
            packstream::SIG_ROLLBACK => {
                send_failure(
                    stream,
                    "SparrowDB.Unsupported",
                    "explicit transactions are not supported; use auto-commit mode",
                )
                .await?;
                state = State::Failed;
            }
            other => {
                tracing::warn!("unknown message signature: 0x{other:02X}");
                send_failure(stream, "Request.Invalid", "unknown message type").await?;
                state = State::Failed;
            }
        }
    }
}

// ── Chunked transport ───────────────────────────────────────────────

/// Read a full chunked message from the stream.
///
/// Bolt chunking: `[u16 len][data]...  [0x0000]`
/// Chunks are concatenated until a zero-length chunk is seen.
/// Returns an error if the accumulated size exceeds `MAX_MESSAGE_SIZE`.
async fn read_chunked_message(
    stream: &mut TcpStream,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut result = Vec::new();
    loop {
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let chunk_len = u16::from_be_bytes(len_buf) as usize;
        if chunk_len == 0 {
            break;
        }
        if result.len() + chunk_len > MAX_MESSAGE_SIZE {
            return Err(format!("message too large: would exceed {MAX_MESSAGE_SIZE} bytes").into());
        }
        let mut chunk = vec![0u8; chunk_len];
        stream.read_exact(&mut chunk).await?;
        result.extend_from_slice(&chunk);
    }
    Ok(result)
}

/// Write a message using chunked transport, splitting into ≤65535-byte chunks.
///
/// Per the Bolt spec, each chunk is prefixed by its u16 length, and the
/// message is terminated by a zero-length chunk (`0x00 0x00`).
async fn write_chunked_message(
    stream: &mut TcpStream,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in data.chunks(MAX_CHUNK_SIZE) {
        let len = chunk.len() as u16;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(chunk).await?;
    }
    stream.write_all(&[0, 0]).await?; // end marker
    stream.flush().await?;
    Ok(())
}

/// Write a pre-built buffer of concatenated chunked messages in a single flush.
///
/// Used by PULL to batch-write all records before the final SUCCESS, avoiding
/// a per-record `flush()` call.
async fn write_chunked_batch(
    stream: &mut TcpStream,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

// ── Message senders ─────────────────────────────────────────────────

async fn send_success(
    stream: &mut TcpStream,
    metadata: HashMap<String, BoltValue>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::new();
    packstream::encode_struct_header(&mut buf, 1, packstream::SIG_SUCCESS);
    let val = BoltValue::Map(metadata);
    packstream::encode_value(&mut buf, &val);
    write_chunked_message(stream, &buf).await
}

async fn send_failure(
    stream: &mut TcpStream,
    code: &str,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut meta = HashMap::new();
    meta.insert("code".into(), BoltValue::String(code.into()));
    meta.insert("message".into(), BoltValue::String(message.into()));

    let mut buf = BytesMut::new();
    packstream::encode_struct_header(&mut buf, 1, packstream::SIG_FAILURE);
    packstream::encode_value(&mut buf, &BoltValue::Map(meta));
    write_chunked_message(stream, &buf).await
}

/// Append a serialized RECORD message (with chunked framing) to `out`, without flushing.
fn append_record(out: &mut BytesMut, fields: Vec<BoltValue>) {
    let mut msg = BytesMut::new();
    packstream::encode_struct_header(&mut msg, 1, packstream::SIG_RECORD);
    packstream::encode_value(&mut msg, &BoltValue::List(fields));
    append_chunked(out, &msg);
}

/// Append a serialized SUCCESS message (with chunked framing) to `out`, without flushing.
fn append_success(out: &mut BytesMut, metadata: HashMap<String, BoltValue>) {
    let mut msg = BytesMut::new();
    packstream::encode_struct_header(&mut msg, 1, packstream::SIG_SUCCESS);
    packstream::encode_value(&mut msg, &BoltValue::Map(metadata));
    append_chunked(out, &msg);
}

/// Serialize `data` into Bolt chunked framing and append to `out`.
fn append_chunked(out: &mut BytesMut, data: &[u8]) {
    for chunk in data.chunks(MAX_CHUNK_SIZE) {
        out.put_u16(chunk.len() as u16);
        out.extend_from_slice(chunk);
    }
    out.put_u16(0); // end marker
}

async fn send_ignored(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::new();
    // IGNORED = 0x7E, struct with 0 fields
    packstream::encode_struct_header(&mut buf, 0, 0x7E);
    write_chunked_message(stream, &buf).await
}

// ── Value mapping ───────────────────────────────────────────────────

fn sparrow_to_bolt(val: &Value) -> BoltValue {
    match val {
        Value::Null => BoltValue::Null,
        Value::Int64(n) => BoltValue::Integer(*n),
        Value::Float64(f) => BoltValue::Float(*f),
        Value::Bool(b) => BoltValue::Boolean(*b),
        Value::String(s) => BoltValue::String(s.clone()),
        Value::NodeRef(id) => BoltValue::Integer(id.0 as i64),
        Value::EdgeRef(id) => BoltValue::Integer(id.0 as i64),
        Value::List(items) => BoltValue::List(items.iter().map(sparrow_to_bolt).collect()),
        Value::Map(pairs) => {
            let mut map = HashMap::new();
            for (k, v) in pairs {
                map.insert(k.clone(), sparrow_to_bolt(v));
            }
            BoltValue::Map(map)
        }
        // Serialize vector as a Bolt list of floats so clients can receive it.
        Value::Vector(floats) => {
            BoltValue::List(floats.iter().map(|f| BoltValue::Float(*f as f64)).collect())
        }
    }
}

fn hello_success_meta() -> HashMap<String, BoltValue> {
    let mut meta = HashMap::new();
    // Spoof Neo4j identity: gdotv and other Bolt clients validate the server
    // string against "Neo4j/" prefix and reject unknown vendors.
    meta.insert("server".into(), BoltValue::String("Neo4j/5.20.0".into()));
    meta.insert(
        "connection_id".into(),
        BoltValue::String("sparrowdb-bolt-0".into()),
    );
    meta
}
