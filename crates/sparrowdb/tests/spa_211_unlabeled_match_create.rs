//! SPA-211: MATCH with no label and a property filter inside MATCH…CREATE context
//! was silently producing 0 edges because `get_label("")` returned `None` and the
//! variable got an empty candidate list.
//!
//! The fix makes `scan_match_create()` fall back to scanning ALL registered labels
//! when no label is specified on a MATCH node pattern, mirroring the behaviour of
//! the regular labelled MATCH path.

use sparrowdb::open;
use sparrowdb_common;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: cross-label MATCH…CREATE ─────────────────────────────────────────

/// `MATCH (a {name: "Alice"}), (b {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)`
/// where Alice is a Person and Bob is an Animal (different labels).
/// The unlabeled MATCH must scan all labels and find both nodes, then create
/// the edge.
#[test]
fn unlabeled_match_create_cross_label_edge() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Animal {name: 'Bob'})")
        .expect("CREATE Bob");

    // Unlabeled MATCH — the bug caused 0 edges to be created.
    db.execute("MATCH (a {name: 'Alice'}), (b {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("MATCH…CREATE with unlabeled nodes must succeed");

    // Verify edge is visible: Alice is Person, Bob is Animal.
    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Animal) RETURN a.name, b.name")
        .expect("traversal must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 KNOWS edge between Person and Animal; got rows: {:?}",
        result.rows
    );
}

// ── Test 2: unlabeled MATCH RETURN ───────────────────────────────────────────

/// `MATCH (a {name: "Alice"}) RETURN a.name` where Alice has label Person but
/// the query specifies no label.  Must find and return Alice.
#[test]
fn unlabeled_match_return_finds_node() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let result = db
        .execute("MATCH (a {name: 'Alice'}) RETURN a.name")
        .expect("unlabeled MATCH RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row for Alice; got rows: {:?}",
        result.rows
    );
}

// ── Test 3: no-match → 0 rows, no error ──────────────────────────────────────

/// When the property filter on an unlabeled MATCH node matches nothing, the
/// MATCH…CREATE must return Ok and create 0 edges.
#[test]
fn unlabeled_match_no_match_no_edge_no_error() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");

    let result =
        db.execute("MATCH (a {name: 'Ghost'}), (b {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b)");

    assert!(
        result.is_ok(),
        "MATCH…CREATE with no-match unlabeled node must not error; got: {:?}",
        result
    );

    // No edges — verify by attempting a KNOWS traversal.
    // An "unknown relationship type" error is also acceptable (rel type was never
    // registered) and proves no edge was created.
    let edge_result = db.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name");
    match edge_result {
        Ok(qr) => {
            assert_eq!(
                qr.rows.len(),
                0,
                "no edges should have been created; got rows: {:?}",
                qr.rows
            );
        }
        Err(sparrowdb_common::Error::InvalidArgument(msg))
            if msg.contains("unknown relationship type") =>
        {
            // Acceptable: rel type was never registered because no edge was created.
        }
        Err(e) => panic!("unexpected error checking for edges: {:?}", e),
    }
}

// ── Test 4: same-label unlabeled MATCH…CREATE ────────────────────────────────

/// Both nodes have the same label (Person) but the MATCH uses no labels.
/// The edge must still be created.
#[test]
fn unlabeled_match_create_same_label_edge() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    db.execute("MATCH (a {name: 'Alice'}), (b {name: 'Bob'}) CREATE (a)-[:FRIENDS]->(b)")
        .expect("MATCH…CREATE same-label unlabeled must succeed");

    let result = db
        .execute("MATCH (a:Person)-[:FRIENDS]->(b:Person) RETURN a.name, b.name")
        .expect("traversal must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 FRIENDS edge; got rows: {:?}",
        result.rows
    );
}
