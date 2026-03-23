//! SPA-251: In-memory text search index for CONTAINS and STARTS WITH predicates.
//!
//! These tests verify that:
//! - `WHERE n.prop CONTAINS 'str'` returns correct results via the text index fast path.
//! - `WHERE n.prop STARTS WITH 'str'` returns correct results via binary-search prefix range.
//! - Empty results are returned correctly when no node matches.
//! - Nodes without the searched property are silently excluded.
//! - The text index handles long strings (overflow heap strings > 7 bytes).

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

fn setup_product_db(dir: &std::path::Path) -> GraphDb {
    let db = open_db(dir);
    db.execute("CREATE (n:Product {name: 'Widget Pro', category: 'electronics'})")
        .unwrap();
    db.execute("CREATE (n:Product {name: 'Widget Lite', category: 'electronics'})")
        .unwrap();
    db.execute("CREATE (n:Product {name: 'Gadget Max', category: 'electronics'})")
        .unwrap();
    db.execute("CREATE (n:Product {name: 'Banana Phone', category: 'phones'})")
        .unwrap();
    db
}

// ── CONTAINS tests ─────────────────────────────────────────────────────────────

/// CONTAINS returns exactly the nodes whose property contains the substring.
#[test]
fn contains_returns_matching_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup_product_db(dir.path());

    let r = db
        .execute("MATCH (n:Product) WHERE n.name CONTAINS 'Widget' RETURN n.name")
        .expect("CONTAINS query must succeed");

    assert_eq!(
        r.rows.len(),
        2,
        "expected 2 rows matching 'Widget', got {:?}",
        r.rows
    );

    let names: Vec<&Value> = r.rows.iter().map(|row| &row[0]).collect();
    assert!(
        names.contains(&&Value::String("Widget Pro".into())),
        "Widget Pro should be in results"
    );
    assert!(
        names.contains(&&Value::String("Widget Lite".into())),
        "Widget Lite should be in results"
    );
}

/// CONTAINS with no matching nodes returns an empty result set.
#[test]
fn contains_no_match_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup_product_db(dir.path());

    let r = db
        .execute("MATCH (n:Product) WHERE n.name CONTAINS 'xyz' RETURN n.name")
        .expect("CONTAINS no-match query must not error");

    assert_eq!(
        r.rows.len(),
        0,
        "expected 0 rows for 'xyz', got {:?}",
        r.rows
    );
}

/// CONTAINS where the pattern matches an entire value still returns that node.
#[test]
fn contains_exact_full_string_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (n:Item {name: 'hello'})").unwrap();
    db.execute("CREATE (n:Item {name: 'world'})").unwrap();

    let r = db
        .execute("MATCH (n:Item) WHERE n.name CONTAINS 'hello' RETURN n.name")
        .expect("CONTAINS exact match must succeed");

    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::String("hello".into()));
}

/// Nodes missing the searched property are silently excluded (not an error).
#[test]
fn contains_missing_property_excluded() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Node A has a name; node B has no name property.
    db.execute("CREATE (n:Thing {name: 'hello there'})")
        .unwrap();
    db.execute("CREATE (n:Thing {tag: 'other'})").unwrap();

    let r = db
        .execute("MATCH (n:Thing) WHERE n.name CONTAINS 'hello' RETURN n.name")
        .expect("missing property must not error");

    assert_eq!(
        r.rows.len(),
        1,
        "only node with matching name should be returned"
    );
    assert_eq!(r.rows[0][0], Value::String("hello there".into()));
}

// ── STARTS WITH tests ─────────────────────────────────────────────────────────

/// STARTS WITH returns exactly the nodes whose property starts with the prefix.
#[test]
fn starts_with_returns_matching_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup_product_db(dir.path());

    let r = db
        .execute("MATCH (n:Product) WHERE n.name STARTS WITH 'Widget' RETURN n.name")
        .expect("STARTS WITH query must succeed");

    assert_eq!(
        r.rows.len(),
        2,
        "expected 2 rows starting with 'Widget', got {:?}",
        r.rows
    );

    let names: Vec<&Value> = r.rows.iter().map(|row| &row[0]).collect();
    assert!(
        names.contains(&&Value::String("Widget Pro".into())),
        "Widget Pro should be in results"
    );
    assert!(
        names.contains(&&Value::String("Widget Lite".into())),
        "Widget Lite should be in results"
    );
}

/// STARTS WITH with no matching nodes returns an empty result set.
#[test]
fn starts_with_no_match_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = setup_product_db(dir.path());

    let r = db
        .execute("MATCH (n:Product) WHERE n.name STARTS WITH 'Zephyr' RETURN n.name")
        .expect("STARTS WITH no-match must not error");

    assert_eq!(
        r.rows.len(),
        0,
        "expected 0 rows for 'Zephyr', got {:?}",
        r.rows
    );
}

/// STARTS WITH with a single-character prefix matches multiple nodes.
#[test]
fn starts_with_single_char_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (n:Animal {name: 'Aardvark'})").unwrap();
    db.execute("CREATE (n:Animal {name: 'Antelope'})").unwrap();
    db.execute("CREATE (n:Animal {name: 'Bear'})").unwrap();

    let r = db
        .execute("MATCH (n:Animal) WHERE n.name STARTS WITH 'A' RETURN n.name")
        .expect("single char STARTS WITH must succeed");

    assert_eq!(
        r.rows.len(),
        2,
        "expected 2 animals starting with 'A', got {:?}",
        r.rows
    );
}

/// STARTS WITH with empty prefix matches all nodes that have the property.
#[test]
fn starts_with_empty_prefix_matches_all() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (n:Tag {name: 'alpha'})").unwrap();
    db.execute("CREATE (n:Tag {name: 'beta'})").unwrap();
    db.execute("CREATE (n:Tag {name: 'gamma'})").unwrap();

    let r = db
        .execute("MATCH (n:Tag) WHERE n.name STARTS WITH '' RETURN n.name")
        .expect("empty prefix STARTS WITH must succeed");

    assert_eq!(
        r.rows.len(),
        3,
        "empty prefix should match all tagged nodes, got {:?}",
        r.rows
    );
}

// ── Long strings (overflow heap) ──────────────────────────────────────────────

/// CONTAINS works correctly for strings longer than 7 bytes (overflow heap storage).
#[test]
fn contains_long_strings_overflow_heap() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // These strings are all > 7 bytes, stored in the overflow heap.
    db.execute("CREATE (n:Article {title: 'Introduction to Rust programming'})")
        .unwrap();
    db.execute("CREATE (n:Article {title: 'Advanced Rust techniques'})")
        .unwrap();
    db.execute("CREATE (n:Article {title: 'Python for beginners'})")
        .unwrap();

    let r = db
        .execute("MATCH (n:Article) WHERE n.title CONTAINS 'Rust' RETURN n.title")
        .expect("CONTAINS on long strings must succeed");

    assert_eq!(
        r.rows.len(),
        2,
        "expected 2 articles containing 'Rust', got {:?}",
        r.rows
    );
}

/// STARTS WITH works correctly for strings longer than 7 bytes (overflow heap storage).
#[test]
fn starts_with_long_strings_overflow_heap() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (n:Doc {title: 'Introduction to Graph Databases'})")
        .unwrap();
    db.execute("CREATE (n:Doc {title: 'Introduction to SQL'})")
        .unwrap();
    db.execute("CREATE (n:Doc {title: 'Advanced Graph Theory'})")
        .unwrap();

    let r = db
        .execute("MATCH (n:Doc) WHERE n.title STARTS WITH 'Introduction' RETURN n.title")
        .expect("STARTS WITH on long strings must succeed");

    assert_eq!(
        r.rows.len(),
        2,
        "expected 2 docs starting with 'Introduction', got {:?}",
        r.rows
    );
}

// ── Correctness: results match full sequential scan ───────────────────────────

/// Verify text index results match what a full scan would return.
/// Creates many nodes and compares filtered vs unfiltered results.
#[test]
fn text_index_results_match_sequential_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let names = [
        "Alice Anderson",
        "Bob Brown",
        "Alice Williams",
        "Charlie Davis",
        "Alicia Keys",
        "David Bowie",
    ];
    for name in &names {
        db.execute(&format!("CREATE (n:Person {{name: '{name}'}})"))
            .unwrap();
    }

    let prefix = "Alice";
    let r = db
        .execute(&format!(
            "MATCH (n:Person) WHERE n.name STARTS WITH '{prefix}' RETURN n.name"
        ))
        .expect("STARTS WITH must succeed");

    // Manual filter to get expected result.
    let expected: Vec<&str> = names
        .iter()
        .filter(|n| n.starts_with(prefix))
        .copied()
        .collect();

    assert_eq!(
        r.rows.len(),
        expected.len(),
        "text index result count should match manual filter count"
    );
}
