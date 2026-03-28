//! Integration tests for multi-label nodes (SPA-289).
//!
//! Verifies that nodes with multiple labels (e.g. `CREATE (n:A:B)`) are stored,
//! indexed, and queried correctly.  Key scenarios:
//!
//! 1. `CREATE (n:A:B)` stores both labels (primary A, secondary B).
//! 2. `MATCH (n:A)` returns the multi-label node (primary label scan).
//! 3. `MATCH (n:B)` finds nodes where B is a secondary label.
//! 4. `MATCH (n:A:B)` returns only nodes that carry both labels (intersection).
//! 5. `labels(n)` returns the full ordered set [primary, secondary...].
//! 6. Single-label existing behaviour is unchanged.
//! 7. `MERGE (n:A {prop: val})` followed by `MATCH (n:A:B)` — a node created
//!    with `CREATE (n:A:B)` is correctly found by the intersection scan.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: CREATE (n:A:B) stores both labels ─────────────────────────────────

/// After `CREATE (n:Animal:Pet {name: 'Rex'})`, a subsequent
/// `MATCH (n:Animal) RETURN n.name` must return "Rex".
#[test]
fn create_multi_label_stores_primary_label() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Animal:Pet {name: 'Rex'})")
        .expect("CREATE multi-label node");

    let result = db
        .execute("MATCH (n:Animal) RETURN n.name")
        .expect("MATCH on primary label");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row from primary-label MATCH; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Rex".to_string()),
        "n.name must be 'Rex'; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 2: MATCH (n:B) finds nodes where B is a secondary label ──────────────

/// `CREATE (n:Animal:Pet {name: 'Rex'})` creates a node with primary label
/// Animal and secondary label Pet.  `MATCH (n:Pet) RETURN n.name` must find
/// this node via the secondary-label index.
#[test]
fn match_on_secondary_label_finds_node() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Animal:Pet {name: 'Rex'})")
        .expect("CREATE multi-label node");

    let result = db
        .execute("MATCH (n:Pet) RETURN n.name")
        .expect("MATCH on secondary label");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row from secondary-label MATCH; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Rex".to_string()),
        "n.name must be 'Rex'; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 3: MATCH (n:A:B) returns only intersection nodes ────────────────────

/// Given two nodes — one with both labels A and B, one with only A — the
/// pattern `MATCH (n:Animal:Pet)` must return only the node with both labels.
#[test]
fn match_multi_label_pattern_returns_intersection() {
    let (_dir, db) = make_db();

    // Node with both labels.
    db.execute("CREATE (n:Animal:Pet {name: 'Rex'})")
        .expect("CREATE Animal+Pet");

    // Node with only the primary label.
    db.execute("CREATE (n:Animal {name: 'Simba'})")
        .expect("CREATE Animal only");

    let result = db
        .execute("MATCH (n:Animal:Pet) RETURN n.name")
        .expect("MATCH (n:Animal:Pet)");

    assert_eq!(
        result.rows.len(),
        1,
        "MATCH (n:Animal:Pet) must return only the node with both labels; got: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Rex".to_string()),
        "only 'Rex' has both labels; got: {:?}",
        result.rows[0][0]
    );
}

// ── Test 4: labels(n) returns the full set [primary, secondary...] ────────────

/// `CREATE (n:Animal:Pet {name: 'Rex'})` followed by
/// `MATCH (n:Animal) RETURN labels(n)` must return a list containing both
/// "Animal" and "Pet".
#[test]
fn labels_fn_returns_full_label_set() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Animal:Pet {name: 'Rex'})")
        .expect("CREATE multi-label node");

    let result = db
        .execute("MATCH (n:Animal) RETURN labels(n)")
        .expect("MATCH with labels(n)");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );

    let label_list = match &result.rows[0][0] {
        Value::List(items) => items.clone(),
        other => panic!("expected Value::List from labels(n), got: {:?}", other),
    };

    assert!(
        label_list.contains(&Value::String("Animal".to_string())),
        "labels(n) must include 'Animal'; got: {:?}",
        label_list
    );
    assert!(
        label_list.contains(&Value::String("Pet".to_string())),
        "labels(n) must include 'Pet'; got: {:?}",
        label_list
    );
}

// ── Test 5: labels(n) from secondary-label MATCH also returns full set ─────────

/// `MATCH (n:Pet) RETURN labels(n)` must also return both "Animal" and "Pet",
/// not just "Pet".
#[test]
fn labels_fn_from_secondary_match_returns_full_set() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Animal:Pet {name: 'Rex'})")
        .expect("CREATE multi-label node");

    let result = db
        .execute("MATCH (n:Pet) RETURN labels(n)")
        .expect("MATCH on secondary label with labels(n)");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );

    let label_list = match &result.rows[0][0] {
        Value::List(items) => items.clone(),
        other => panic!("expected Value::List from labels(n), got: {:?}", other),
    };

    assert!(
        label_list.contains(&Value::String("Animal".to_string())),
        "labels(n) must include primary label 'Animal' even when matched on secondary; got: {:?}",
        label_list
    );
    assert!(
        label_list.contains(&Value::String("Pet".to_string())),
        "labels(n) must include 'Pet'; got: {:?}",
        label_list
    );
}

// ── Test 6: single-label behaviour unchanged ──────────────────────────────────

/// Creating a single-label node and querying it must work exactly as before
/// multi-label support was added.
#[test]
fn single_label_behaviour_unchanged() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE single-label node");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE second single-label node");

    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH single-label");

    assert_eq!(
        result.rows.len(),
        2,
        "single-label MATCH must return both nodes; got: {:?}",
        result.rows
    );

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match &row[0] {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        names.contains(&"Alice".to_string()),
        "Alice must be present"
    );
    assert!(names.contains(&"Bob".to_string()), "Bob must be present");
}

// ── Test 7: labels(n) on single-label node returns a one-element list ─────────

/// `MATCH (n:Person) RETURN labels(n)` must return `["Person"]` for a node
/// created with a single label — the multi-label code path must not affect this.
#[test]
fn labels_fn_single_label_returns_one_element_list() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE single-label node");

    let result = db
        .execute("MATCH (n:Person) RETURN labels(n)")
        .expect("MATCH with labels(n) on single-label node");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row; got: {:?}",
        result.rows
    );

    let label_list = match &result.rows[0][0] {
        Value::List(items) => items.clone(),
        other => panic!("expected Value::List from labels(n), got: {:?}", other),
    };

    assert_eq!(
        label_list.len(),
        1,
        "single-label node must return exactly 1 label; got: {:?}",
        label_list
    );
    assert_eq!(
        label_list[0],
        Value::String("Person".to_string()),
        "label must be 'Person'; got: {:?}",
        label_list[0]
    );
}

// ── Test 8: MERGE creates node, then MATCH (n:A:B) finds it ──────────────────

/// A node created via MERGE on primary label A, then a separate CREATE adds
/// another node with both A and B.  `MATCH (n:A:B)` must return only the
/// node that carries both labels.
///
/// Note: `MERGE (n:A:B)` is not yet supported in the parser (MergeStatement
/// has a single `label` field); this test instead exercises the combination
/// of MERGE (single label) + CREATE (multi-label) to confirm the intersection
/// scan correctly discriminates between them.
#[test]
fn merge_single_label_then_match_multi_label_intersection() {
    let (_dir, db) = make_db();

    // Create a node via MERGE with only the primary label.
    db.execute("MERGE (n:Vehicle {name: 'Car'})")
        .expect("MERGE single-label Vehicle");

    // Create a node with both labels via CREATE.
    db.execute("CREATE (n:Vehicle:Electric {name: 'Tesla'})")
        .expect("CREATE Vehicle:Electric");

    // MATCH on primary only — both nodes.
    let all_result = db
        .execute("MATCH (n:Vehicle) RETURN n.name")
        .expect("MATCH primary label");
    assert_eq!(
        all_result.rows.len(),
        2,
        "MATCH (n:Vehicle) must return both nodes; got: {:?}",
        all_result.rows
    );

    // MATCH on intersection — only the multi-label node.
    let intersection = db
        .execute("MATCH (n:Vehicle:Electric) RETURN n.name")
        .expect("MATCH (n:Vehicle:Electric)");
    assert_eq!(
        intersection.rows.len(),
        1,
        "MATCH (n:Vehicle:Electric) must return only 'Tesla'; got: {:?}",
        intersection.rows
    );
    assert_eq!(
        intersection.rows[0][0],
        Value::String("Tesla".to_string()),
        "MATCH (n:Vehicle:Electric) must return 'Tesla'; got: {:?}",
        intersection.rows[0][0]
    );
}

// ── Test 9: Three-label node, all combinations found ─────────────────────────

/// `CREATE (n:A:B:C {name: 'tri'})` — the node must be found by MATCH (n:A),
/// MATCH (n:B), MATCH (n:C), MATCH (n:A:B), MATCH (n:A:C), and MATCH (n:A:B:C).
#[test]
fn three_label_node_all_patterns_match() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Alpha:Beta:Gamma {name: 'tri'})")
        .expect("CREATE three-label node");

    for pattern in &[
        "MATCH (n:Alpha) RETURN n.name",
        "MATCH (n:Beta) RETURN n.name",
        "MATCH (n:Gamma) RETURN n.name",
        "MATCH (n:Alpha:Beta) RETURN n.name",
        "MATCH (n:Alpha:Gamma) RETURN n.name",
        "MATCH (n:Alpha:Beta:Gamma) RETURN n.name",
    ] {
        let result = db.execute(pattern).expect(pattern);
        assert_eq!(
            result.rows.len(),
            1,
            "pattern '{}' must find the three-label node; got: {:?}",
            pattern,
            result.rows
        );
        assert_eq!(
            result.rows[0][0],
            Value::String("tri".to_string()),
            "pattern '{}' must return 'tri'; got: {:?}",
            pattern,
            result.rows[0][0]
        );
    }
}

// ── Test 10: Multi-label MATCH discriminates correctly in mixed graph ─────────

/// Given a graph with Animal-only, Animal+Pet, and Pet-only (impossible since
/// Pet is always secondary here) nodes, the intersection MATCH (n:Animal:Pet)
/// must return exactly the node carrying both labels.
///
/// Note: `WHERE 'Pet' IN labels(n)` is not yet supported by the parser
/// (function calls on the right-hand side of IN are not parsed).
/// This test validates equivalent semantics via the pattern-based path.
#[test]
fn multi_label_match_discriminates_in_mixed_graph() {
    let (_dir, db) = make_db();

    // Three animals: one with Pet, one without, one with a different secondary.
    db.execute("CREATE (n:Animal:Pet {name: 'Rex'})")
        .expect("CREATE Animal:Pet");
    db.execute("CREATE (n:Animal {name: 'Simba'})")
        .expect("CREATE Animal");
    db.execute("CREATE (n:Animal:Wild {name: 'Leo'})")
        .expect("CREATE Animal:Wild");

    // MATCH (n:Animal) — all three.
    let all = db
        .execute("MATCH (n:Animal) RETURN n.name")
        .expect("MATCH Animal");
    assert_eq!(
        all.rows.len(),
        3,
        "MATCH (n:Animal) must return all three nodes; got: {:?}",
        all.rows
    );

    // MATCH (n:Animal:Pet) — only Rex.
    let pets = db
        .execute("MATCH (n:Animal:Pet) RETURN n.name")
        .expect("MATCH Animal:Pet");
    assert_eq!(
        pets.rows.len(),
        1,
        "MATCH (n:Animal:Pet) must return only Rex; got: {:?}",
        pets.rows
    );
    assert_eq!(
        pets.rows[0][0],
        Value::String("Rex".to_string()),
        "must return 'Rex'; got: {:?}",
        pets.rows[0][0]
    );

    // MATCH (n:Animal:Wild) — only Leo.
    let wild = db
        .execute("MATCH (n:Animal:Wild) RETURN n.name")
        .expect("MATCH Animal:Wild");
    assert_eq!(
        wild.rows.len(),
        1,
        "MATCH (n:Animal:Wild) must return only Leo; got: {:?}",
        wild.rows
    );
    assert_eq!(
        wild.rows[0][0],
        Value::String("Leo".to_string()),
        "must return 'Leo'; got: {:?}",
        wild.rows[0][0]
    );

    // MATCH (n:Pet) via secondary label — only Rex.
    let sec_pet = db
        .execute("MATCH (n:Pet) RETURN n.name")
        .expect("MATCH secondary Pet");
    assert_eq!(
        sec_pet.rows.len(),
        1,
        "MATCH (n:Pet) via secondary must return only Rex; got: {:?}",
        sec_pet.rows
    );
    assert_eq!(
        sec_pet.rows[0][0],
        Value::String("Rex".to_string()),
        "secondary MATCH must return 'Rex'; got: {:?}",
        sec_pet.rows[0][0]
    );
}
