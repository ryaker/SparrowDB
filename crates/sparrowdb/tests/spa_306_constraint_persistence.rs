//! Issue #306: unique constraints must survive database close/reopen.
//!
//! Verifies that `CREATE CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE`
//! persists to `constraints.bin` and is reloaded on `GraphDb::open`, so
//! duplicate inserts are still rejected after a restart.

use sparrowdb::GraphDb;

#[test]
fn unique_constraint_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path();

    // Session 1: register constraint and insert a node.
    {
        let db = GraphDb::open(db_path).expect("open db");
        db.execute("CREATE CONSTRAINT ON (n:Account) ASSERT n.email IS UNIQUE")
            .expect("register constraint");
        db.execute("CREATE (:Account {email: 'alice@example.com'})")
            .expect("first insert must succeed");
    }
    // db is dropped — simulates process exit.

    // Session 2: reopen and verify the constraint is still enforced.
    {
        let db = GraphDb::open(db_path).expect("reopen db");

        // The duplicate must be rejected.
        let err = db
            .execute("CREATE (:Account {email: 'alice@example.com'})")
            .expect_err("duplicate insert after reopen must fail");

        let msg = format!("{err:?}").to_lowercase();
        assert!(
            msg.contains("unique") || msg.contains("constraint") || msg.contains("violation"),
            "error should mention constraint violation, got: {msg}"
        );

        // A different value must still be allowed.
        db.execute("CREATE (:Account {email: 'bob@example.com'})")
            .expect("distinct value must succeed after reopen");
    }
}

#[test]
fn constraint_file_missing_is_fresh_db() {
    // Opening a brand-new directory (no constraints.bin) must not error.
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).expect("fresh open must succeed");

    // Without any constraint, duplicate values are allowed.
    db.execute("CREATE (:X {k: 1})").unwrap();
    db.execute("CREATE (:X {k: 1})").unwrap();
    let result = db
        .execute("MATCH (n:X) RETURN n.k")
        .expect("query must succeed");
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn multiple_constraints_persist() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path();

    // Session 1: register two constraints on different labels.
    {
        let db = GraphDb::open(db_path).expect("open db");
        db.execute("CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE")
            .unwrap();
        db.execute("CREATE CONSTRAINT ON (n:Product) ASSERT n.sku IS UNIQUE")
            .unwrap();
        db.execute("CREATE (:User {email: 'u1@test.com'})").unwrap();
        db.execute("CREATE (:Product {sku: 'SKU-001'})").unwrap();
    }

    // Session 2: both constraints must still be enforced.
    {
        let db = GraphDb::open(db_path).expect("reopen db");

        db.execute("CREATE (:User {email: 'u1@test.com'})")
            .expect_err("User email constraint must survive reopen");

        db.execute("CREATE (:Product {sku: 'SKU-001'})")
            .expect_err("Product sku constraint must survive reopen");

        // Distinct values still work.
        db.execute("CREATE (:User {email: 'u2@test.com'})").unwrap();
        db.execute("CREATE (:Product {sku: 'SKU-002'})").unwrap();
    }
}
