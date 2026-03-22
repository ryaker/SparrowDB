//! SPA-208: Reject node labels and relationship types that start with `__SO_`.
//!
//! The `__SO_` prefix is reserved for internal SparrowDB system objects.
//! Any `CREATE` statement that specifies a label or rel-type beginning with
//! `__SO_` must return an `InvalidArgument` error containing the word
//! "reserved".  Normal labels (`Person`, `__SomeOther`) must continue to work.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Error-path tests ──────────────────────────────────────────────────────────

/// `CREATE (n:__SO_Node)` — reserved node label — must be rejected.
#[test]
fn create_node_with_reserved_label_is_rejected() {
    let (_dir, db) = make_db();

    let err = db
        .execute("CREATE (n:__SO_Node)")
        .expect_err("CREATE with reserved label must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("reserved"),
        "error message must contain 'reserved'; got: {msg}"
    );
    assert!(
        msg.contains("__SO_Node"),
        "error message must include the offending label; got: {msg}"
    );
}

/// `CREATE (n:Person)-[:__SO_REL]->(m:Person)` — reserved rel-type — must be rejected.
#[test]
fn create_edge_with_reserved_rel_type_is_rejected() {
    let (_dir, db) = make_db();

    // Pre-create the nodes so the edge CREATE can reference them.
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    // Standalone CREATE (a:Person)-[:__SO_REL]->(b:Person).
    let err = db
        .execute("CREATE (a:Person)-[:__SO_REL]->(b:Person)")
        .expect_err("CREATE with reserved rel-type must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("reserved"),
        "error message must contain 'reserved'; got: {msg}"
    );
    assert!(
        msg.contains("__SO_REL"),
        "error message must include the offending type; got: {msg}"
    );
}

/// `CREATE (n:__SO_)` — exact prefix (empty suffix) — must still be rejected.
#[test]
fn create_node_with_exact_prefix_is_rejected() {
    let (_dir, db) = make_db();

    let err = db
        .execute("CREATE (n:__SO_)")
        .expect_err("CREATE with exact __SO_ label must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("reserved"),
        "error message must contain 'reserved'; got: {msg}"
    );
}

// ── Success-path tests ────────────────────────────────────────────────────────

/// `CREATE (n:NormalLabel)` — normal label — must succeed.
#[test]
fn create_node_with_normal_label_succeeds() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:NormalLabel)")
        .expect("CREATE with normal label must succeed");
}

/// `CREATE (n:__SomeOtherPrefix)` — only `__SO_` is reserved; other
/// double-underscore prefixes must be allowed.
#[test]
fn create_node_with_other_double_underscore_prefix_succeeds() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:__SomeOtherPrefix)")
        .expect("CREATE with __SomeOtherPrefix must succeed — only __SO_ is reserved");
}

/// MATCH…CREATE with a reserved rel-type must also be rejected.
#[test]
fn match_create_edge_with_reserved_rel_type_is_rejected() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let err = db
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:__SO_INTERNAL]->(b)",
        )
        .expect_err("MATCH…CREATE with reserved rel-type must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("reserved"),
        "error message must contain 'reserved'; got: {msg}"
    );
    assert!(
        msg.contains("__SO_INTERNAL"),
        "error message must include the offending type; got: {msg}"
    );
}
