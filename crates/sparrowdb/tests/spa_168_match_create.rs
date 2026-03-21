//! SPA-168: MATCH…CREATE edge must not be a silent no-op.
//!
//! Before this fix, `Statement::MatchCreate` was not included in
//! `Engine::is_mutation()`, so `GraphDb::execute()` routed it to the read
//! path where `execute_bound()` returned `Ok(QueryResult::empty(...))` without
//! writing anything.
//!
//! The fix:
//! 1. `Engine::is_mutation()` now returns `true` for `Statement::MatchCreate`.
//! 2. `GraphDb::execute()` routes `MatchCreate` to `execute_match_create()`.
//! 3. `execute_match_create()` scans MATCH patterns, then calls
//!    `WriteTx::create_edge` for each matching (src, dst) pair — writing the
//!    edge to the delta log + WAL and registering the rel type in the catalog.

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── SPA-168 test 1: edge persists and is visible in subsequent MATCH ──────────

/// After `MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'})
///       CREATE (a)-[:KNOWS]->(b)`,
/// a subsequent `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name`
/// must return the (Alice, Bob) pair.
#[test]
fn match_create_edge_persists() {
    let (_dir, db) = make_db();

    // Create two Person nodes.
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    // MATCH…CREATE edge — this was previously a no-op.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE must succeed");

    // Verify the edge is visible via a MATCH traversal.
    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH traversal must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 KNOWS edge; got rows: {:?}",
        result.rows
    );
}

// ── SPA-168 test 2: rel type is registered in catalog after match_create ──────

/// After `MATCH…CREATE (a)-[:KNOWS]->(b)`, the relationship type "KNOWS" must
/// appear in the catalog so that subsequent bound queries referencing [:KNOWS]
/// do not fail with "unknown relationship type".
#[test]
fn match_create_rel_type_registered() {
    let (dir, db) = make_db();

    // Seed two nodes.
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    // MATCH…CREATE edge.
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("MATCH…CREATE must succeed");

    // Re-open the catalog from disk and verify KNOWS is present.
    let catalog = Catalog::open(dir.path()).expect("catalog re-open");
    let tables = catalog.list_rel_tables().expect("list_rel_tables");
    let knows_registered = tables.iter().any(|(_, _, rt)| rt == "KNOWS");

    assert!(
        knows_registered,
        "rel type 'KNOWS' must be persisted in catalog after MATCH…CREATE; tables: {:?}",
        tables
    );
}

// ── SPA-168 test 3: no-match → no edge, no crash ─────────────────────────────

/// When the MATCH clause finds no nodes (label present but no node matches the
/// filter), CREATE must not create any edges and must return Ok (not crash).
#[test]
fn match_create_no_match_no_edge() {
    let (dir, db) = make_db();

    // Seed one Person node that does NOT match the filter we will use.
    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");

    // MATCH for a person named 'Ghost' — will find nothing.
    let result = db.execute(
        "MATCH (a:Person {name: 'Ghost'}), (b:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b)",
    );

    // Must not return an error.
    assert!(
        result.is_ok(),
        "MATCH…CREATE with no matching nodes must not error; got: {:?}",
        result
    );

    // No KNOWS edges must have been created.
    let catalog = Catalog::open(dir.path()).expect("catalog re-open");
    let tables = catalog.list_rel_tables().expect("list_rel_tables");
    let knows_registered = tables.iter().any(|(_, _, rt)| rt == "KNOWS");

    assert!(
        !knows_registered,
        "no rel type must be registered when MATCH found no nodes; tables: {:?}",
        tables
    );
}
