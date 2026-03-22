//! Regression tests for SPA-212: string property values truncated at 7 bytes.
//!
//! The v1 inline encoding packs up to 7 bytes of string data into a `u64`
//! (1 byte is used for the type tag).  Strings longer than 7 bytes were
//! silently truncated at write time; `"TypeScript"` became `"TypeScr"`.
//!
//! The fix introduces overflow string storage: strings > 7 UTF-8 bytes are
//! written to a string heap (`strings.bin`) and the u64 slot holds a tagged
//! pointer + length instead of inline bytes.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Boundary tests: 6, 7, 8, and 16-byte strings ─────────────────────────────

/// 6-byte string — fits comfortably inline, must round-trip.
#[test]
fn six_byte_string_round_trips() {
    let (_dir, db) = make_db();
    // "abcdef" = 6 bytes
    db.execute("CREATE (n:Node {val: 'abcdef'})").unwrap();
    let result = db
        .execute("MATCH (n:Node) RETURN n.val")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("abcdef".to_string()),
        "6-byte string must round-trip intact (SPA-212)"
    );
}

/// 7-byte string — exactly fills the inline payload, must round-trip without truncation.
#[test]
fn seven_byte_string_round_trips() {
    let (_dir, db) = make_db();
    // "abcdefg" = 7 bytes
    db.execute("CREATE (n:Node {val: 'abcdefg'})").unwrap();
    let result = db
        .execute("MATCH (n:Node) RETURN n.val")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("abcdefg".to_string()),
        "7-byte string must round-trip intact (SPA-212)"
    );
}

/// 8-byte string — one byte beyond the inline limit; requires overflow storage.
#[test]
fn eight_byte_string_round_trips() {
    let (_dir, db) = make_db();
    // "abcdefgh" = 8 bytes
    db.execute("CREATE (n:Node {val: 'abcdefgh'})").unwrap();
    let result = db
        .execute("MATCH (n:Node) RETURN n.val")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("abcdefgh".to_string()),
        "8-byte string must round-trip intact (SPA-212) — was truncated to 7 bytes before fix"
    );
}

/// 16-byte string — well beyond inline limit; requires overflow storage.
#[test]
fn sixteen_byte_string_round_trips() {
    let (_dir, db) = make_db();
    // "TypeScriptLang!!" = 16 bytes
    db.execute("CREATE (n:Node {val: 'TypeScriptLang!!'})")
        .unwrap();
    let result = db
        .execute("MATCH (n:Node) RETURN n.val")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("TypeScriptLang!!".to_string()),
        "16-byte string must round-trip intact (SPA-212) — was truncated to 7 bytes before fix"
    );
}

/// Real-world example from the ticket: "TypeScript" (10 bytes) must not become "TypeScr".
#[test]
fn typescript_string_round_trips() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Language {name: 'TypeScript'})")
        .unwrap();
    let result = db
        .execute("MATCH (n:Language) RETURN n.name")
        .expect("MATCH RETURN");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("TypeScript".to_string()),
        "'TypeScript' must not be truncated to 'TypeScr' (SPA-212)"
    );
}

/// WHERE filter on a long string must match correctly after overflow fix.
#[test]
fn long_string_where_filter_works() {
    let (_dir, db) = make_db();
    db.execute("CREATE (n:Language {name: 'TypeScript'})")
        .unwrap();
    db.execute("CREATE (n:Language {name: 'JavaScript'})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Language) WHERE n.name = 'TypeScript' RETURN n.name")
        .expect("WHERE filter");
    assert_eq!(
        result.rows.len(),
        1,
        "WHERE n.name = 'TypeScript' must return exactly 1 row (SPA-212)"
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("TypeScript".to_string()),
        "Filtered result must be full 'TypeScript' string (SPA-212)"
    );
}

/// Long string must survive a DB reopen (overflow heap persisted correctly).
#[test]
fn long_string_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: write.
    {
        let db = open(dir.path()).expect("open session 1");
        db.execute("CREATE (n:Node {val: 'TypeScript'})").unwrap();
    }

    // Session 2: read from disk.
    {
        let db = open(dir.path()).expect("open session 2");
        let result = db
            .execute("MATCH (n:Node) RETURN n.val")
            .expect("MATCH after reopen");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            Value::String("TypeScript".to_string()),
            "Long string must survive DB reopen (SPA-212)"
        );
    }
}
