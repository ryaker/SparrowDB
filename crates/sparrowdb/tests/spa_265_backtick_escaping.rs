//! SPA-265: Backtick escaping for Cypher-reserved identifiers.
//!
//! Node labels and relationship types that collide with Cypher keywords
//! (e.g. `Order`, `CONTAINS`) must be usable via backtick escaping
//! (`` `Order` ``, `` `CONTAINS` ``).  Additionally, bare keyword names
//! after `:` in label/rel-type position should be accepted without
//! backticks so that `[:CONTAINS]` works the same as `` [:`CONTAINS`] ``.

use sparrowdb::GraphDb;
use tempfile::tempdir;

// ── Backtick-escaped node labels ────────────────────────────────────────────

/// CREATE and MATCH with backtick-escaped label `Order`.
#[test]
fn backtick_escaped_label_order() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (n:`Order` {name: 'test-order'})")
        .expect("CREATE with backtick-escaped Order label");

    let r = db
        .execute("MATCH (n:`Order`) RETURN n.name AS name")
        .expect("MATCH with backtick-escaped Order label");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "test-order");
}

/// Bare keyword `Order` as a label (no backticks) should also work.
#[test]
fn bare_keyword_label_order() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (n:Order {name: 'bare-order'})")
        .expect("CREATE with bare Order label");

    let r = db
        .execute("MATCH (n:Order) RETURN n.name AS name")
        .expect("MATCH with bare Order label");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "bare-order");
}

/// Backtick-escaped label `Set` — another common keyword collision.
#[test]
fn backtick_escaped_label_set() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (n:`Set` {v: 42})")
        .expect("CREATE with backtick-escaped Set label");

    let r = db
        .execute("MATCH (n:`Set`) RETURN n.v AS v")
        .expect("MATCH with backtick-escaped Set label");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "42");
}

// ── Backtick-escaped relationship types ─────────────────────────────────────

/// CREATE and MATCH with backtick-escaped rel type `CONTAINS`.
#[test]
fn backtick_escaped_rel_type_contains() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:Basket {name: 'cart'})")
        .expect("CREATE Basket");
    db.execute("CREATE (b:Product {name: 'widget'})")
        .expect("CREATE Product");
    db.execute(
        "MATCH (a:Basket {name: 'cart'}), (b:Product {name: 'widget'}) \
         CREATE (a)-[:`CONTAINS`]->(b)",
    )
    .expect("CREATE with backtick-escaped CONTAINS rel type");

    let r = db
        .execute("MATCH ()-[r:`CONTAINS`]->() RETURN COUNT(r) AS n")
        .expect("MATCH with backtick-escaped CONTAINS rel type");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "1");
}

/// Bare keyword `CONTAINS` as a rel type (no backticks) should also work.
#[test]
fn bare_keyword_rel_type_contains() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:Folder {name: 'root'})")
        .expect("CREATE Folder");
    db.execute("CREATE (b:File {name: 'readme'})")
        .expect("CREATE File");
    db.execute(
        "MATCH (a:Folder {name: 'root'}), (b:File {name: 'readme'}) \
         CREATE (a)-[:CONTAINS]->(b)",
    )
    .expect("CREATE with bare CONTAINS rel type");

    let r = db
        .execute("MATCH ()-[r:CONTAINS]->() RETURN COUNT(r) AS n")
        .expect("MATCH with bare CONTAINS rel type");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "1");
}

/// Backtick-escaped rel type `IN` — keyword that would otherwise conflict.
#[test]
fn backtick_escaped_rel_type_in() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:Item {name: 'x'})").expect("CREATE a");
    db.execute("CREATE (b:Category {name: 'y'})")
        .expect("CREATE b");
    db.execute(
        "MATCH (a:Item {name: 'x'}), (b:Category {name: 'y'}) \
         CREATE (a)-[:`IN`]->(b)",
    )
    .expect("CREATE with backtick-escaped IN rel type");

    let r = db
        .execute("MATCH ()-[r:`IN`]->() RETURN COUNT(r) AS n")
        .expect("MATCH with backtick-escaped IN rel type");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "1");
}

// ── Standalone CREATE path with backtick-escaped identifiers ────────────────

/// Standalone CREATE (a:`Label`)-[:`TYPE`]->(b:Normal) should parse and execute.
#[test]
fn standalone_create_with_backtick_identifiers() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    db.execute("CREATE (a:`Delete` {v: 1})-[:`LIMIT`]->(b:Normal {v: 2})")
        .expect("Standalone CREATE with backtick-escaped Delete and LIMIT");

    let r = db
        .execute("MATCH (a:`Delete`) RETURN COUNT(a) AS n")
        .expect("COUNT nodes with backtick-escaped Delete label");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "1");

    let r = db
        .execute("MATCH ()-[r:`LIMIT`]->() RETURN COUNT(r) AS n")
        .expect("COUNT edges with backtick-escaped LIMIT rel type");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "1");
}

// ── Lexer edge cases ────────────────────────────────────────────────────────

/// Unterminated backtick should produce a parse error.
#[test]
fn unterminated_backtick_is_error() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let err = db
        .execute("CREATE (n:`UnclosedLabel)")
        .expect_err("unterminated backtick must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("unterminated"),
        "error must mention 'unterminated'; got: {msg}"
    );
}

/// Empty backtick identifier should produce a parse error.
#[test]
fn empty_backtick_is_error() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    let err = db
        .execute("CREATE (n:`` {v: 1})")
        .expect_err("empty backtick identifier must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("empty"),
        "error must mention 'empty'; got: {msg}"
    );
}

/// Backtick-escaped and bare keyword with matching case are interchangeable.
#[test]
fn backtick_and_bare_keyword_same_case_are_interchangeable() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();

    // Bare keyword ORDER creates label "ORDER"; backtick `ORDER` also creates "ORDER".
    db.execute("CREATE (n:Order {name: 'a'})").unwrap();
    db.execute("CREATE (n:`ORDER` {name: 'b'})").unwrap();

    // Both should match when queried with backtick `ORDER`.
    let r = db
        .execute("MATCH (n:`ORDER`) RETURN COUNT(n) AS cnt")
        .unwrap();
    assert_eq!(r.rows[0][0].to_string(), "2");

    // And both should match when queried with bare keyword Order.
    let r = db
        .execute("MATCH (n:Order) RETURN COUNT(n) AS cnt")
        .unwrap();
    assert_eq!(r.rows[0][0].to_string(), "2");
}
