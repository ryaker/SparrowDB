//! Phase 9 path-feature acceptance tests (SPA-139).
//!
//! Covers all path-related Cypher capabilities that landed in Phase 9:
//! - Variable-length paths `[:R*min..max]` with correct node sets
//! - Variable-length paths with property filters on endpoints
//! - `shortestPath((a)-[:R*]->(b))` returning minimum-hop length
//! - `shortestPath` returning null when no path exists
//! - `EXISTS { (n)-[:REL]->(:Label) }` filtering correctly
//! - `EXISTS` with nested property conditions on target node
//! - Multi-pattern MATCH `(a:X), (b:Y)` producing a cross-product
//! - Multi-pattern MATCH used to create a relationship between matched nodes
//! - Chained 3-hop path `(a)-[:R]->(b)-[:R]->(c)-[:R]->(d)`
//!
//! Tests that exercise known gaps are marked `#[ignore]` with a ticket
//! reference so they surface in `cargo test -- --ignored` when the gap closes.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Collect all string values from one column of a query result.
fn col_strings(result: &sparrowdb_execution::types::QueryResult, col: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| match &row[col] {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ── 1. Variable-length paths: range `[:R*1..3]` returns correct node sets ─────

/// Chain A→B→C→D→E.
/// `[:LINK*1..3]` from A must reach B (depth 1), C (depth 2), D (depth 3)
/// and must NOT reach E (depth 4).
#[test]
fn varpath_range_1_to_3_correct_set() {
    let (_dir, db) = make_db();

    for name in &["Alice", "Bob", "Carol", "Dave", "Eve"] {
        db.execute(&format!("CREATE (n:Node {{name: '{name}'}})"))
            .unwrap();
    }
    for (src, dst) in &[
        ("Alice", "Bob"),
        ("Bob", "Carol"),
        ("Carol", "Dave"),
        ("Dave", "Eve"),
    ] {
        db.execute(&format!(
            "MATCH (a:Node {{name: '{src}'}}), (b:Node {{name: '{dst}'}}) CREATE (a)-[:LINK]->(b)"
        ))
        .unwrap();
    }

    let result = db
        .execute("MATCH (a:Node {name: 'Alice'})-[:LINK*1..3]->(b:Node) RETURN b.name")
        .expect("[:LINK*1..3] query must succeed");

    let names = col_strings(&result, 0);

    assert!(
        names.contains(&"Bob".to_string()),
        "[:LINK*1..3] must include Bob (depth 1); got: {names:?}"
    );
    assert!(
        names.contains(&"Carol".to_string()),
        "[:LINK*1..3] must include Carol (depth 2); got: {names:?}"
    );
    assert!(
        names.contains(&"Dave".to_string()),
        "[:LINK*1..3] must include Dave (depth 3); got: {names:?}"
    );
    assert!(
        !names.contains(&"Eve".to_string()),
        "[:LINK*1..3] must NOT include Eve (depth 4); got: {names:?}"
    );
}

// ── 2. Variable-length paths with property filter on the endpoint ─────────────

/// Chain A→B→C. `[:LINK*1..2]` from A with a property filter `{active: true}`
/// on the endpoint must return only the nodes that match both the depth range
/// and the property condition.
///
/// Graph:
///   Alice -[:LINK]-> Bob   {active: true}
///   Bob   -[:LINK]-> Carol {active: false}
///
/// Query: `MATCH (a:Node {name:'Alice'})-[:LINK*1..2]->(b:Node {active: true})`
/// Expected: only Bob.
///
/// GAP: SPA-232 — varpath BFS engine does not apply inline endpoint property
/// filters; the dst filter is silently ignored and returns an empty set.
#[test]
#[ignore = "SPA-232: varpath dst property filter not yet applied by BFS engine"]
fn varpath_with_endpoint_property_filter() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Node {name: 'Alice', active: true})")
        .unwrap();
    db.execute("CREATE (n:Node {name: 'Bob', active: true})")
        .unwrap();
    db.execute("CREATE (n:Node {name: 'Carol', active: false})")
        .unwrap();

    db.execute("MATCH (a:Node {name: 'Alice'}), (b:Node {name: 'Bob'}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (a:Node {name: 'Bob'}), (b:Node {name: 'Carol'}) CREATE (a)-[:LINK]->(b)")
        .unwrap();

    let result = db
        .execute(
            "MATCH (a:Node {name: 'Alice'})-[:LINK*1..2]->(b:Node {active: true}) RETURN b.name",
        )
        .expect("varpath with endpoint property filter must succeed");

    let names = col_strings(&result, 0);

    assert!(
        names.contains(&"Bob".to_string()),
        "Bob (active=true, depth 1) must be included; got: {names:?}"
    );
    assert!(
        !names.contains(&"Carol".to_string()),
        "Carol (active=false) must be excluded; got: {names:?}"
    );
}

// ── 3. shortestPath returns the minimum-hop length ───────────────────────────

/// Two paths exist between A and C:
///   Direct:  A -[:LINK]-> C          (1 hop)
///   Indirect: A -[:LINK]-> B -[:LINK]-> C  (2 hops)
///
/// `shortestPath((a)-[:LINK*]->(c))` must return 1, not 2.
#[test]
fn shortest_path_prefers_minimum_hops() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Node {name: 'A'})").unwrap();
    db.execute("CREATE (n:Node {name: 'B'})").unwrap();
    db.execute("CREATE (n:Node {name: 'C'})").unwrap();

    // Direct A→C
    db.execute("MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) CREATE (a)-[:LINK]->(c)")
        .unwrap();
    // Indirect A→B→C
    db.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    db.execute("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)")
        .unwrap();

    let result = db
        .execute(
            "MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
             RETURN shortestPath((a)-[:LINK*]->(c))",
        )
        .expect("shortestPath must succeed");

    assert_eq!(result.rows.len(), 1, "expected one result row");
    assert_eq!(
        result.rows[0][0],
        Value::Int64(1),
        "shortestPath must pick the 1-hop direct edge, not the 2-hop route; got: {:?}",
        result.rows[0][0]
    );
}

// ── 4. shortestPath returns null when no path exists ─────────────────────────

/// Two isolated nodes with no connecting edges.
/// `shortestPath` must return `NULL`.
#[test]
fn shortest_path_null_when_disconnected() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Island {name: 'X'})").unwrap();
    db.execute("CREATE (n:Island {name: 'Y'})").unwrap();
    // No edges.

    let result = db
        .execute(
            "MATCH (x:Island {name: 'X'}), (y:Island {name: 'Y'}) \
             RETURN shortestPath((x)-[:BRIDGE*]->(y))",
        )
        .expect("shortestPath with no path must not error");

    assert_eq!(result.rows.len(), 1, "expected one result row");
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "shortestPath must return NULL when no path exists; got: {:?}",
        result.rows[0][0]
    );
}

// ── 5. EXISTS filters to only nodes with a matching outgoing edge ─────────────

/// Three Person nodes: Alice, Bob, Carol.
/// Alice -[:KNOWS]-> Bob (Bob is an Employee).
/// Carol has no outgoing KNOWS edges.
///
/// `WHERE EXISTS { (n)-[:KNOWS]->(:Employee) }` must return only Alice.
#[test]
fn exists_filters_by_label_on_target() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Employee {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Carol'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Employee {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->(:Employee) } RETURN n.name")
        .expect("EXISTS with label filter must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "only Alice has a KNOWS->Employee edge; got rows: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "result must be Alice; got: {:?}",
        result.rows[0][0]
    );
}

// ── 6. EXISTS with nested property conditions on target node ──────────────────

/// Alice -[:MANAGES]-> Project {status: 'active'}
/// Bob   -[:MANAGES]-> Project {status: 'closed'}
///
/// `WHERE EXISTS { (n)-[:MANAGES]->(:Project {status: 'active'}) }`
/// must return only Alice.
#[test]
fn exists_with_target_property_condition() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Manager {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Manager {name: 'Bob'})").unwrap();
    db.execute("CREATE (p:Project {title: 'Alpha', status: 'active'})")
        .unwrap();
    db.execute("CREATE (p:Project {title: 'Beta', status: 'closed'})")
        .unwrap();

    db.execute(
        "MATCH (m:Manager {name: 'Alice'}), (p:Project {title: 'Alpha'}) \
         CREATE (m)-[:MANAGES]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (m:Manager {name: 'Bob'}), (p:Project {title: 'Beta'}) \
         CREATE (m)-[:MANAGES]->(p)",
    )
    .unwrap();

    let result = db
        .execute(
            "MATCH (m:Manager) \
             WHERE EXISTS { (m)-[:MANAGES]->(:Project {status: 'active'}) } \
             RETURN m.name",
        )
        .expect("EXISTS with property condition on target must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "only Alice manages an active project; got rows: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Alice".to_string()),
        "result must be Alice; got: {:?}",
        result.rows[0][0]
    );
}

// ── 7. Multi-pattern MATCH produces a cross-product ──────────────────────────

/// Two Category nodes (Tech, Science) and two Topic nodes (AI, Biology).
/// `MATCH (c:Category), (t:Topic) RETURN c.name, t.name`
/// must return 4 rows (the full cross-product).
#[test]
fn multi_pattern_match_cross_product() {
    let (_dir, db) = make_db();

    db.execute("CREATE (c:Category {name: 'Tech'})").unwrap();
    db.execute("CREATE (c:Category {name: 'Science'})").unwrap();
    db.execute("CREATE (t:Topic {name: 'AI'})").unwrap();
    db.execute("CREATE (t:Topic {name: 'Biology'})").unwrap();

    let result = db
        .execute("MATCH (c:Category), (t:Topic) RETURN c.name, t.name")
        .expect("multi-pattern cross-product must succeed");

    assert_eq!(
        result.rows.len(),
        4,
        "2 categories × 2 topics = 4 rows; got: {:?}",
        result.rows
    );

    // Every (category, topic) pair must appear.
    let pairs: Vec<(String, String)> = result
        .rows
        .iter()
        .filter_map(|row| match (&row[0], &row[1]) {
            (Value::String(c), Value::String(t)) => Some((c.clone(), t.clone())),
            _ => None,
        })
        .collect();

    for cat in &["Tech", "Science"] {
        for topic in &["AI", "Biology"] {
            assert!(
                pairs.contains(&(cat.to_string(), topic.to_string())),
                "pair ({cat}, {topic}) must be in cross-product; got: {pairs:?}"
            );
        }
    }
}

// ── 8. Multi-pattern MATCH with relationship creation ────────────────────────

/// MATCH two specific nodes, then CREATE a relationship between them.
/// Verify the edge is visible in a subsequent MATCH.
///
/// This reuses the MATCH…CREATE pattern tested in SPA-168 but adds a second
/// node pattern to confirm multi-pattern matching feeds the CREATE correctly.
#[test]
fn multi_pattern_match_then_create_rel() {
    let (_dir, db) = make_db();

    db.execute("CREATE (s:Supplier {name: 'Acme'})").unwrap();
    db.execute("CREATE (p:Product {name: 'Widget'})").unwrap();

    db.execute(
        "MATCH (s:Supplier {name: 'Acme'}), (p:Product {name: 'Widget'}) \
         CREATE (s)-[:SUPPLIES]->(p)",
    )
    .unwrap();

    let result = db
        .execute("MATCH (s:Supplier)-[:SUPPLIES]->(p:Product) RETURN s.name, p.name")
        .expect("traversal after multi-pattern MATCH…CREATE must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 SUPPLIES edge; got rows: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("Acme".to_string()),
        "supplier must be Acme; got: {:?}",
        result.rows[0][0]
    );
    assert_eq!(
        result.rows[0][1],
        Value::String("Widget".to_string()),
        "product must be Widget; got: {:?}",
        result.rows[0][1]
    );
}

// ── 9. Chained 3-hop path (a)-[:R]->(b)-[:R]->(c)-[:R]->(d) ─────────────────

/// Build a 4-node chain A→B→C→D and query all four nodes in a single
/// 3-hop inline pattern.  All four names must appear in the result.
///
/// GAP: SPA-252 — 3-hop inline chain MATCH returns wrong variable bindings;
/// all 4 columns resolve to the same node (one row per node) rather than
/// producing one row with 4 distinct hop bindings.
#[test]
#[ignore = "SPA-252: 3-hop inline chain variable bindings are incorrect"]
fn three_hop_inline_chain() {
    let (_dir, db) = make_db();

    for name in &["Alpha", "Beta", "Gamma", "Delta"] {
        db.execute(&format!("CREATE (n:Step {{name: '{name}'}})"))
            .unwrap();
    }
    for (src, dst) in &[("Alpha", "Beta"), ("Beta", "Gamma"), ("Gamma", "Delta")] {
        db.execute(&format!(
            "MATCH (a:Step {{name: '{src}'}}), (b:Step {{name: '{dst}'}}) CREATE (a)-[:NEXT]->(b)"
        ))
        .unwrap();
    }

    let result = db
        .execute(
            "MATCH (a:Step)-[:NEXT]->(b:Step)-[:NEXT]->(c:Step)-[:NEXT]->(d:Step) \
             RETURN a.name, b.name, c.name, d.name",
        )
        .expect("3-hop inline chain query must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "exactly one 3-hop chain exists; got: {:?}",
        result.rows
    );

    let row = &result.rows[0];
    assert_eq!(
        row[0],
        Value::String("Alpha".to_string()),
        "hop 0 must be Alpha; got: {:?}",
        row[0]
    );
    assert_eq!(
        row[1],
        Value::String("Beta".to_string()),
        "hop 1 must be Beta; got: {:?}",
        row[1]
    );
    assert_eq!(
        row[2],
        Value::String("Gamma".to_string()),
        "hop 2 must be Gamma; got: {:?}",
        row[2]
    );
    assert_eq!(
        row[3],
        Value::String("Delta".to_string()),
        "hop 3 must be Delta; got: {:?}",
        row[3]
    );
}

// ── 10. Variable-length path respects min-hop lower bound ────────────────────

/// Chain A→B→C.
/// `[:LINK*2..3]` from A must include C (depth 2) but NOT B (depth 1).
#[test]
fn varpath_lower_bound_excludes_shallow_nodes() {
    let (_dir, db) = make_db();

    for name in &["Start", "Mid", "End"] {
        db.execute(&format!("CREATE (n:Hop {{name: '{name}'}})"))
            .unwrap();
    }
    for (src, dst) in &[("Start", "Mid"), ("Mid", "End")] {
        db.execute(&format!(
            "MATCH (a:Hop {{name: '{src}'}}), (b:Hop {{name: '{dst}'}}) CREATE (a)-[:JUMP]->(b)"
        ))
        .unwrap();
    }

    let result = db
        .execute("MATCH (a:Hop {name: 'Start'})-[:JUMP*2..3]->(b:Hop) RETURN b.name")
        .expect("[:JUMP*2..3] must succeed");

    let names = col_strings(&result, 0);

    assert!(
        names.contains(&"End".to_string()),
        "[:JUMP*2..3] must reach End (depth 2); got: {names:?}"
    );
    assert!(
        !names.contains(&"Mid".to_string()),
        "[:JUMP*2..3] must NOT include Mid (depth 1 < lower bound 2); got: {names:?}"
    );
}
