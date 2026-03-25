//! Regression tests for GitHub issue #208 / SPA-208: read_from_string_heap
//! must not load the entire strings.bin into RAM.
//!
//! The original implementation used `fs::read(path)` which loads the full heap
//! file into memory. The fix replaces this with `File::seek + read_exact` so
//! only the `len` bytes at `offset` are loaded, regardless of heap size.
//!
//! These tests verify the fix is functionally correct end-to-end by exercising
//! the Cypher interface (which routes through `encode_value` / `decode_raw_value`
//! and ultimately `read_from_string_heap` for any overflow string).

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Basic round-trip: write a short overflow string (> 7 bytes) and read it back.
#[test]
fn string_roundtrip_basic() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Thing {name: 'HelloWorld'})").unwrap();
    let result = db
        .execute("MATCH (n:Thing) RETURN n.name")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("HelloWorld".to_string()),
        "10-byte overflow string must round-trip intact (SPA-208)"
    );
}

/// A string stored at heap offset 0 must be read correctly (no off-by-one on zero).
#[test]
fn string_zero_value_not_confused() {
    let (_dir, db) = make_db();
    // This is the first string written, so it will land at offset 0 in strings.bin.
    db.execute("CREATE (n:First {val: 'OffsetZeroTest'})").unwrap();
    let result = db
        .execute("MATCH (n:First) RETURN n.val")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("OffsetZeroTest".to_string()),
        "String at heap offset 0 must round-trip correctly (SPA-208)"
    );
}

/// Write 100 distinct overflow strings; read them back in insertion order.
/// This exercises multiple heap entries without full-file loads per read.
#[test]
fn multiple_strings_independent() {
    let (_dir, db) = make_db();

    // Write 100 distinct strings that all exceed the 7-byte inline limit.
    for i in 0u32..100 {
        db.execute(&format!(
            "CREATE (n:Item {{id: {i}, label: 'overflow-string-{i:04}'}})"
        ))
        .unwrap();
    }

    let result = db
        .execute("MATCH (n:Item) RETURN n.id, n.label ORDER BY n.id")
        .expect("MATCH RETURN");
    assert_eq!(
        result.rows.len(),
        100,
        "All 100 nodes must be returned (SPA-208)"
    );
    for (i, row) in result.rows.iter().enumerate() {
        let expected_label = format!("overflow-string-{i:04}");
        assert_eq!(
            row[1],
            Value::String(expected_label.clone()),
            "Row {i}: expected '{expected_label}', got {:?} (SPA-208)",
            row[1]
        );
    }
}

/// A string written after many others lives at a high heap offset; it must
/// still be read back correctly (seek to arbitrary offsets, not just the start).
#[test]
fn long_string_seek_not_full_load() {
    let (_dir, db) = make_db();

    // Push a bunch of strings into the heap first to advance the write pointer.
    for i in 0u32..50 {
        db.execute(&format!(
            "CREATE (n:Filler {{val: 'filler-data-padding-{i:04}'}})"
        ))
        .unwrap();
    }

    // Write the target string that lands deep in the heap.
    db.execute("CREATE (n:Target {val: 'DeepHeapOffsetValue'})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Target) RETURN n.val")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("DeepHeapOffsetValue".to_string()),
        "String at high heap offset must be read correctly via seek (SPA-208)"
    );
}

/// An empty string (0 bytes) stored as an overflow value must round-trip as an
/// empty string without corrupting adjacent heap entries.
#[test]
fn empty_string_roundtrip() {
    let (_dir, db) = make_db();

    // Empty strings are inline (0 bytes < 7-byte limit), so pair with a real
    // overflow string before and after to exercise heap adjacency.
    db.execute("CREATE (n:Before {val: 'before-boundary'})").unwrap();
    db.execute("CREATE (n:Empty  {val: ''})").unwrap();
    db.execute("CREATE (n:After  {val: 'after-boundary-ok'})").unwrap();

    let before = db
        .execute("MATCH (n:Before) RETURN n.val")
        .expect("before");
    assert_eq!(before.rows[0][0], Value::String("before-boundary".into()));

    let empty = db
        .execute("MATCH (n:Empty) RETURN n.val")
        .expect("empty");
    assert_eq!(
        empty.rows[0][0],
        Value::String(String::new()),
        "Empty string must round-trip as empty (SPA-208)"
    );

    let after = db
        .execute("MATCH (n:After) RETURN n.val")
        .expect("after");
    assert_eq!(after.rows[0][0], Value::String("after-boundary-ok".into()));
}
