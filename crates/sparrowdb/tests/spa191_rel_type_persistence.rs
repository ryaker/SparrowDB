//! SPA-191: relationship type names must survive across sessions.
//!
//! Steps:
//! 1. Open DB, create two Person nodes and a KNOWS edge, checkpoint, drop DB.
//! 2. Reopen DB (new GraphDb::open) and query MATCH (a:Person)-[:KNOWS]->(b:Person).
//!    Must succeed without "unknown relationship type: KNOWS".

use sparrowdb::GraphDb;

#[test]
fn spa191_rel_type_persists_across_sessions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    // Session 1: create graph data and checkpoint.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (a:Person {name:'Alice'})")
            .expect("create Alice");
        db.execute("CREATE (b:Person {name:'Bob'})")
            .expect("create Bob");
        db.execute(
            "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("create KNOWS edge");
        db.checkpoint().expect("checkpoint");
        // `db` is dropped here — simulates process exit.
    }

    // Session 2: reopen from disk and query the KNOWS edge.
    {
        let db2 = GraphDb::open(&db_path).expect("open session 2");
        let result = db2
            .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
            .expect("KNOWS query must succeed after reopen — SPA-191");
        assert_eq!(result.rows.len(), 1, "must find exactly one KNOWS edge");
    }
}

/// Verify rel type persists even WITHOUT checkpoint (WAL-only state).
///
/// The catalog.tlv is written synchronously at create_edge time, so the
/// rel type should be in the catalog even if checkpoint never ran.
#[test]
fn spa191_rel_type_persists_without_checkpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    // Session 1: create graph data but do NOT checkpoint.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (a:Person {name:'Alice'})")
            .expect("create Alice");
        db.execute("CREATE (b:Person {name:'Bob'})")
            .expect("create Bob");
        db.execute(
            "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("create KNOWS edge");
        // No checkpoint — rel type should still be in catalog.tlv from create_edge.
    }

    // Session 2: reopen and verify rel type is resolvable (binder must not reject it).
    {
        let db2 = GraphDb::open(&db_path).expect("open session 2");
        // The edge data won't be in the CSR yet (no checkpoint), but the
        // binder must not throw "unknown relationship type: KNOWS".
        let result = db2.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name");
        match result {
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("unknown relationship type"),
                    "binder must not reject KNOWS — SPA-191: {msg}"
                );
            }
            Ok(_) => {} // query succeeded — even better
        }
    }
}

/// Verify the catalog correctly persists rel table entries via list_rel_tables
/// after reopen (direct catalog inspection).
#[test]
fn spa191_catalog_rel_table_survives_reopen() {
    use sparrowdb_catalog::catalog::Catalog;

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    // Session 1: create a KNOWS rel.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (a:Person {name:'Alice'})")
            .expect("create Alice");
        db.execute("CREATE (b:Person {name:'Bob'})")
            .expect("create Bob");
        db.execute(
            "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("create KNOWS edge");
    }

    // After session 1 closes, directly inspect catalog.tlv.
    {
        let catalog = Catalog::open(&db_path).expect("open catalog after session 1");
        let rel_tables = catalog.list_rel_tables().expect("list_rel_tables");
        assert!(
            rel_tables.iter().any(|(_, _, rt)| rt == "KNOWS"),
            "catalog must contain KNOWS rel type after session 1: {:?}",
            rel_tables
        );
    }
}

/// Multiple rel types must all survive across sessions.
#[test]
fn spa191_multiple_rel_types_all_persist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.sparrow");

    // Session 1: create KNOWS and FOLLOWS edges.
    {
        let db = GraphDb::open(&db_path).expect("open session 1");
        db.execute("CREATE (a:Person {name:'Alice'})")
            .expect("create Alice");
        db.execute("CREATE (b:Person {name:'Bob'})")
            .expect("create Bob");
        db.execute(
            "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("create KNOWS edge");
        db.execute(
            "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:FOLLOWS]->(b)",
        )
        .expect("create FOLLOWS edge");
        db.checkpoint().expect("checkpoint");
    }

    // Session 2: both rel types must be queryable.
    {
        let db2 = GraphDb::open(&db_path).expect("open session 2");

        let r1 = db2
            .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name")
            .expect("KNOWS must resolve after reopen");
        assert_eq!(r1.rows.len(), 1, "KNOWS edge must be found");

        let r2 = db2
            .execute("MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.name")
            .expect("FOLLOWS must resolve after reopen");
        assert_eq!(r2.rows.len(), 1, "FOLLOWS edge must be found");
    }
}
