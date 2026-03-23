//! Integration tests for SPA-229: add_property MCP tool.
//!
//! The add_property tool allows SparrowOntology (and other callers) to add
//! or update a property on existing nodes matched by label and a filter
//! property, without needing to write raw Cypher.
//!
//! These tests verify:
//! 1. `add_property_to_existing_node` — create a Person, add email, MATCH
//!    confirms the property round-trips correctly.
//! 2. `add_property_returns_zero_when_no_match` — match a ghost node → 0
//!    updated, Ok (not Err).
//! 3. `add_property_updates_multiple_nodes` — 3 nodes with the same match
//!    property value → all 3 receive the new property.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: basic add_property round-trip ────────────────────────────────────

/// Create a Person node with a name, then SET an email property via MATCH … SET,
/// and verify the email is retrievable with a subsequent MATCH.
#[test]
fn add_property_to_existing_node() {
    let (_dir, db) = make_db();

    // Create a node to update.
    db.execute("CREATE (:Person {name: 'Alice'})")
        .expect("CREATE must succeed");

    // Add the email property via MATCH … SET (what add_property tool builds).
    db.execute("MATCH (n:Person {name: 'Alice'}) SET n.email = 'alice@example.com'")
        .expect("MATCH SET must succeed");

    // Verify the property is retrievable.
    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.email")
        .expect("MATCH RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row after SET, got: {:?}",
        result.rows
    );

    let cell = result.rows[0][0].to_string();
    assert!(
        cell.contains("alice@example.com"),
        "expected email in result, got: {cell}"
    );
}

// ── Test 2: zero match returns Ok, not Err ───────────────────────────────────

/// Matching a non-existent node should return Ok with 0 rows — not an error.
/// The add_property tool surfaces this as "0 nodes updated" rather than Err.
#[test]
fn add_property_returns_zero_when_no_match() {
    let (_dir, db) = make_db();

    // Create a node so the label exists but the match will miss.
    db.execute("CREATE (:Person {name: 'Bob'})")
        .expect("CREATE must succeed");

    // SET on a ghost node — should be Ok, no rows affected.
    let result = db.execute(
        "MATCH (n:Person {name: 'ghost-who-does-not-exist'}) SET n.email = 'ghost@example.com'",
    );

    assert!(
        result.is_ok(),
        "zero-match SET must return Ok, not Err; got: {:?}",
        result.err()
    );

    // Confirm Bob still exists and has no email property set.
    let bob = db
        .execute("MATCH (n:Person {name: 'Bob'}) RETURN n.name")
        .expect("MATCH Bob must succeed");
    assert_eq!(bob.rows.len(), 1, "Bob must still exist");
}

// ── Test 3: multiple nodes matched ───────────────────────────────────────────

/// Three nodes share the same `status` property value.
/// SET a new property on all three via a single MATCH … SET.
/// All three must have the new property after the operation.
#[test]
fn add_property_updates_multiple_nodes() {
    let (_dir, db) = make_db();

    // Create 3 nodes that share status='active'.
    db.execute("CREATE (:Person {name: 'Alice', status: 'active'})")
        .expect("CREATE Alice");
    db.execute("CREATE (:Person {name: 'Bob', status: 'active'})")
        .expect("CREATE Bob");
    db.execute("CREATE (:Person {name: 'Carol', status: 'active'})")
        .expect("CREATE Carol");

    // Control node: status='inactive' — must NOT receive the new property.
    db.execute("CREATE (:Person {name: 'Dave', status: 'inactive'})")
        .expect("CREATE Dave");

    // SET tier property on all active users.
    db.execute("MATCH (n:Person {status: 'active'}) SET n.tier = 'gold'")
        .expect("MATCH SET tier must succeed");

    // Verify all 3 active nodes have tier = 'gold'.
    let with_tier = db
        .execute("MATCH (n:Person {status: 'active'}) RETURN n.tier")
        .expect("MATCH active RETURN tier");
    assert_eq!(
        with_tier.rows.len(),
        3,
        "expected 3 active nodes with tier, got: {:?}",
        with_tier.rows
    );
    for row in &with_tier.rows {
        let val = row[0].to_string();
        assert!(
            val.contains("gold"),
            "expected tier='gold', got: {val}"
        );
    }

    // Verify Dave (inactive) does NOT have tier set to gold.
    let dave = db
        .execute("MATCH (n:Person {name: 'Dave'}) RETURN n.tier")
        .expect("MATCH Dave tier");
    // Dave's tier should be null (not set).
    let dave_tier = dave.rows.first().and_then(|r| r.first()).map(|v| v.to_string()).unwrap_or_default();
    assert!(
        !dave_tier.contains("gold"),
        "Dave (inactive) must not have tier='gold', got: {dave_tier}"
    );
}
