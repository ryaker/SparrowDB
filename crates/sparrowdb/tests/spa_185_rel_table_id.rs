//! E2E tests for SPA-185: per-type RelTableId assignment.
//!
//! Regression guard: creating edges of two different relationship types
//! (KNOWS and LIKES) must not collide.  A type-filtered MATCH must return
//! only edges of the requested type.

use sparrowdb::open;

fn name_strings(result: &sparrowdb_execution::QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| {
            row.first().and_then(|v| {
                if let sparrowdb_execution::Value::String(s) = v {
                    Some(s.clone())
                } else {
                    None
                }
            })
        })
        .collect()
}

/// Create a minimal social graph with two relationship types, then verify that
/// type-filtered queries return only the expected edges and that there is no
/// cross-contamination between KNOWS and LIKES.
#[test]
fn spa185_distinct_rel_types_no_cross_contamination() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    // ── Create nodes ────────────────────────────────────────────────────────
    db.execute("CREATE (:Person {name: 'Alice'})").expect("Alice");
    db.execute("CREATE (:Person {name: 'Bob'})").expect("Bob");
    db.execute("CREATE (:Person {name: 'Carol'})").expect("Carol");

    // ── Create edges of different types ────────────────────────────────────
    // Alice -[:KNOWS]-> Bob
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("KNOWS edge");

    // Alice -[:LIKES]-> Carol
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:LIKES]->(c)",
    )
    .expect("LIKES edge");

    // ── Query KNOWS — must return only Bob ──────────────────────────────────
    let knows_result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
        .expect("KNOWS query");

    let knows_names = name_strings(&knows_result);

    assert!(
        knows_names.contains(&"Bob".to_string()),
        "KNOWS query must return Bob; got: {:?}",
        knows_names
    );
    assert!(
        !knows_names.contains(&"Carol".to_string()),
        "KNOWS query must NOT return Carol (cross-contamination); got: {:?}",
        knows_names
    );
    assert_eq!(
        knows_names.len(),
        1,
        "KNOWS query must return exactly 1 row; got: {:?}",
        knows_names
    );

    // ── Query LIKES — must return only Carol ────────────────────────────────
    let likes_result = db
        .execute("MATCH (a:Person)-[:LIKES]->(b:Person) RETURN b.name")
        .expect("LIKES query");

    let likes_names = name_strings(&likes_result);

    assert!(
        likes_names.contains(&"Carol".to_string()),
        "LIKES query must return Carol; got: {:?}",
        likes_names
    );
    assert!(
        !likes_names.contains(&"Bob".to_string()),
        "LIKES query must NOT return Bob (cross-contamination); got: {:?}",
        likes_names
    );
    assert_eq!(
        likes_names.len(),
        1,
        "LIKES query must return exactly 1 row; got: {:?}",
        likes_names
    );
}

/// After a CHECKPOINT the CSR files are rebuilt per rel type.  Verify that
/// type-filtered queries still return correct results post-checkpoint.
#[test]
fn spa185_distinct_rel_types_after_checkpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    db.execute("CREATE (:Person {name: 'Alice'})").expect("Alice");
    db.execute("CREATE (:Person {name: 'Bob'})").expect("Bob");
    db.execute("CREATE (:Person {name: 'Carol'})").expect("Carol");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("KNOWS edge");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:LIKES]->(c)",
    )
    .expect("LIKES edge");

    // Checkpoint so edges are persisted in the CSR.
    db.checkpoint().expect("checkpoint");

    // Post-checkpoint KNOWS query must still be type-filtered.
    let knows_result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
        .expect("KNOWS query after checkpoint");
    let knows_names = name_strings(&knows_result);

    assert!(
        knows_names.contains(&"Bob".to_string()),
        "post-checkpoint KNOWS query must return Bob; got: {:?}",
        knows_names
    );
    assert!(
        !knows_names.contains(&"Carol".to_string()),
        "post-checkpoint KNOWS query must NOT return Carol; got: {:?}",
        knows_names
    );
    assert_eq!(knows_names.len(), 1, "post-checkpoint KNOWS must return 1 row");

    // Post-checkpoint LIKES query must return only Carol.
    let likes_result = db
        .execute("MATCH (a:Person)-[:LIKES]->(b:Person) RETURN b.name")
        .expect("LIKES query after checkpoint");
    let likes_names = name_strings(&likes_result);

    assert!(
        likes_names.contains(&"Carol".to_string()),
        "post-checkpoint LIKES query must return Carol; got: {:?}",
        likes_names
    );
    assert!(
        !likes_names.contains(&"Bob".to_string()),
        "post-checkpoint LIKES query must NOT return Bob; got: {:?}",
        likes_names
    );
    assert_eq!(likes_names.len(), 1, "post-checkpoint LIKES must return 1 row");
}

/// Three relationship types: verify each filters independently.
#[test]
fn spa185_three_rel_types_independent_filters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    db.execute("CREATE (:Person {name: 'Alice'})").expect("Alice");
    db.execute("CREATE (:Person {name: 'Bob'})").expect("Bob");
    db.execute("CREATE (:Person {name: 'Carol'})").expect("Carol");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("KNOWS");

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:LIKES]->(c)",
    )
    .expect("LIKES");

    // KNOWS → must return exactly Bob (1 row).
    let r = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
        .expect("KNOWS");
    let knows_names = name_strings(&r);
    assert_eq!(
        knows_names,
        vec!["Bob".to_string()],
        "KNOWS must return only Bob; got: {:?}",
        knows_names
    );

    // LIKES → must return exactly Carol (1 row).
    let r = db
        .execute("MATCH (a:Person)-[:LIKES]->(b:Person) RETURN b.name")
        .expect("LIKES");
    let likes_names = name_strings(&r);
    assert_eq!(
        likes_names,
        vec!["Carol".to_string()],
        "LIKES must return only Carol; got: {:?}",
        likes_names
    );
}
