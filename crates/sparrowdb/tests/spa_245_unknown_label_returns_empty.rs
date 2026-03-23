//! SPA-245: MATCH on a nonexistent label must return 0 rows, not an error.
//!
//! Standard Cypher semantics: if a label has never been created, MATCH simply
//! finds nothing.  Previously the binder raised `InvalidArgument("unknown
//! label: …")` which blocked SparrowOntology's `create_entity` flow when
//! checking for `__SO_Property` on a fresh database.

use sparrowdb::GraphDb;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── Test 1: basic unknown label returns 0 rows ────────────────────────────────

#[test]
fn match_nonexistent_label_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let result = db
        .execute("MATCH (n:DoesNotExist) RETURN n")
        .expect("MATCH on nonexistent label must not error (SPA-245)");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows for nonexistent label, got {:?}",
        result.rows
    );
}

// ── Test 2: __SO_Property label on fresh DB ───────────────────────────────────

#[test]
fn match_so_property_label_returns_empty_on_fresh_db() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // __SO_Property is the label that was blocking SparrowOntology's
    // create_entity on a fresh database (SPA-245).
    let result = db
        .execute("MATCH (n:__SO_Property) RETURN n")
        .expect("MATCH (n:__SO_Property) on fresh db must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows for __SO_Property on fresh db, got {:?}",
        result.rows
    );
}

// ── Test 3: create one label, MATCH a different label returns 0 rows ──────────

#[test]
fn create_then_match_different_label_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Seed with a Person node.
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();

    // Querying a different label should yield 0 rows, not an error.
    let result = db
        .execute("MATCH (n:Animal) RETURN n")
        .expect("MATCH on absent label must not error even after other labels exist (SPA-245)");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows for Animal label, got {:?}",
        result.rows
    );
}

// ── Test 4: MATCH on unknown rel-type returns 0 rows ─────────────────────────

#[test]
fn match_nonexistent_rel_type_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (a:Person)-[:NONEXISTENT]->(b:Person) RETURN a")
        .expect("MATCH on nonexistent rel-type must not error (SPA-245)");

    assert_eq!(
        result.rows.len(),
        0,
        "expected 0 rows for nonexistent rel-type, got {:?}",
        result.rows
    );
}

// ── Test 5: existing label still works after the fix ─────────────────────────

#[test]
fn match_existing_label_still_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH on existing label must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows for Person label, got {:?}",
        result.rows
    );
}
