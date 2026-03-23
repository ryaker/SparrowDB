//! Integration tests for SPA-134: Multi-clause Cypher pipeline patterns.
//!
//! Covers chained WITH, MATCH, WHERE, UNWIND, and RETURN clauses that form
//! multi-stage processing pipelines.  Each test exercises a distinct structural
//! pattern that is common in real-world Cypher workloads.
//!
//! Tests that exercise patterns not yet implemented in the parser or execution
//! engine are marked `#[ignore]` with a comment referencing the tracking ticket.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

// ── Test fixture helpers ───────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Seed a small social graph: three Person nodes with ages and KNOWS edges.
///
/// Graph:
///   Alice (age=30) -[:KNOWS]-> Bob (age=25)
///   Alice (age=30) -[:KNOWS]-> Carol (age=35)
///   Bob   (age=25) -[:KNOWS]-> Carol (age=35)
fn seed_social_graph(db: &sparrowdb::GraphDb) {
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol', age: 35})")
        .expect("CREATE Carol");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("Alice KNOWS Bob");
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .expect("Alice KNOWS Carol");
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .expect("Bob KNOWS Carol");
}

// ── Test 1: MATCH … WITH … MATCH … RETURN ─────────────────────────────────────
//
// Two-stage match with filtering in between.
// Pattern: first MATCH finds a set of nodes, WITH renames/filters the result,
// then a second MATCH re-traverses the graph constrained by the intermediate values.
//
// Status: NOT YET IMPLEMENTED.
// The parser's `parse_with_pipeline` expects RETURN immediately after WITH (with
// optional WHERE). A second MATCH after WITH is not parsed — the statement variant
// `MatchWithMatchStatement` does not exist in the AST.
// Tracked by: SPA-134 (this ticket) — implement multi-stage MATCH … WITH … MATCH.

#[test]
fn spa134_match_with_match_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Find all Person nodes, pass their names through WITH, then re-match by name.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n.name AS pname \
             MATCH (m:Person {name: pname}) \
             RETURN m.name",
        )
        .expect("MATCH … WITH … MATCH … RETURN must succeed");

    // Every person name passed through WITH should find itself via the second MATCH.
    assert_eq!(
        result.rows.len(),
        3,
        "second MATCH should find all 3 persons by name"
    );
}

// ── Test 2: MATCH … WITH … WHERE … RETURN ─────────────────────────────────────
//
// WITH + WHERE filter pipeline.
// Pattern: MATCH produces rows, WITH renames properties, WHERE filters the
// intermediate rows, RETURN projects final results.
//
// Status: SUPPORTED (SPA-130 / MatchWithStatement with where_clause).

#[test]
fn spa134_match_with_where_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Pass all persons through WITH renaming age, then filter for age > 25.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n.name AS name, n.age AS age \
             WHERE age > 25 \
             RETURN name",
        )
        .expect("MATCH … WITH … WHERE … RETURN must succeed");

    // Alice (30) and Carol (35) survive the age > 25 predicate; Bob (25) does not.
    assert_eq!(
        result.rows.len(),
        2,
        "WHERE age > 25 should keep Alice and Carol, got {:?}",
        result.rows
    );

    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String for name, got {:?}", other),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["Alice".to_string(), "Carol".to_string()],
        "only Alice and Carol should survive the WHERE filter"
    );
}

// ── Test 3: MATCH … WITH count(*) AS c WHERE c > N RETURN ─────────────────────
//
// Aggregation in WITH followed by a filter on the aggregate value.
// Pattern: COUNT(*) aggregates all matched rows into a scalar, WITH passes it
// as alias `c`, WHERE filters based on `c`, RETURN projects it.
//
// Status: NOT YET IMPLEMENTED.
// The execute_match_with function evaluates WITH items per-row using eval_expr,
// which returns Value::Null for CountStar rather than computing a true aggregate.
// Aggregation in the WITH stage requires dedicated aggregation handling before
// the per-row WITH projection loop.
// Tracked by: SPA-134 (this ticket) — aggregate functions in WITH clause.

#[test]
fn spa134_match_with_count_where_filter() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Count all Person nodes; the count (3) is greater than 2, so the row survives.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH count(*) AS c \
             WHERE c > 2 \
             RETURN c",
        )
        .expect("MATCH … WITH count(*) AS c WHERE c > 2 RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "count is 3 which satisfies c > 2 — exactly one row expected"
    );
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "returned count must equal 3"
    );
}

#[test]
fn spa134_match_with_count_where_filter_blocks_row() {
    // This verifies that when the (conceptual) aggregate does NOT satisfy the
    // WHERE predicate, zero rows are returned.
    //
    // Note: this test currently passes for the *wrong* reason — eval_expr
    // returns Null for CountStar, and Null > 10 evaluates to false, so the
    // WHERE blocks all rows.  Once aggregation in WITH is implemented, this
    // test should continue to pass with the correct semantics.
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Count all Person nodes (3); 3 is NOT > 10, so WHERE blocks the row.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH count(*) AS c \
             WHERE c > 10 \
             RETURN c",
        )
        .expect("MATCH … WITH count(*) AS c WHERE c > 10 RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        0,
        "count=3 does not satisfy c > 10; WHERE should produce 0 rows"
    );
}

// ── Test 4: MATCH … WITH … ORDER BY … LIMIT … MATCH … RETURN ──────────────────
//
// Pagination between clauses (ORDER BY + LIMIT inside WITH, then second MATCH).
// Pattern: first MATCH + WITH paginate a result set, then a second MATCH
// re-traverses using the limited set.
//
// Status: NOT YET IMPLEMENTED.
// Two separate reasons:
// (a) ORDER BY is not applied in execute_match_with — the m.order_by field is
//     parsed but silently ignored by the execution function.
// (b) A second MATCH after WITH is not parseable (same as Test 1).
// Tracked by: SPA-134 (this ticket) — ORDER BY in WITH pipeline + multi-stage MATCH.

#[test]
fn spa134_match_with_order_limit_match_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Take the top-1 Person by age (descending = Carol, age 35), then re-fetch her.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n.name AS name \
             ORDER BY n.age DESC LIMIT 1 \
             MATCH (m:Person {name: name}) \
             RETURN m.name",
        )
        .expect("MATCH … WITH … ORDER BY … LIMIT … MATCH … RETURN must succeed");

    assert_eq!(result.rows.len(), 1, "only the top-1 person is re-fetched");
    assert_eq!(
        result.rows[0][0],
        Value::String("Carol".to_string()),
        "Carol has the highest age and should be the only row"
    );
}

// ── Test 5: UNWIND [1,2,3] AS x WITH x*2 AS y RETURN y ───────────────────────
//
// UNWIND into a WITH transform that applies arithmetic to the unwound values.
// Pattern: UNWIND expands a list, WITH applies an expression to each element,
// RETURN projects the transformed values.
//
// Status: NOT YET IMPLEMENTED.
// The `parse_unwind` function expects RETURN immediately after the alias binding
// (`UNWIND list AS x RETURN x`). A WITH clause following UNWIND is not parsed —
// the UnwindStatement AST node only carries a return_clause, not a with_clause.
// Tracked by: SPA-134 (this ticket) — add WITH support after UNWIND.

#[test]
fn spa134_unwind_with_transform_return() {
    let (_dir, db) = make_db();

    let result = db
        .execute("UNWIND [1, 2, 3] AS x WITH x * 2 AS y RETURN y")
        .expect("UNWIND … WITH … RETURN must succeed");

    assert_eq!(result.columns, vec!["y"]);
    assert_eq!(result.rows.len(), 3, "one row per UNWIND element");
    assert_eq!(result.rows[0][0], Value::Int64(2), "1*2=2");
    assert_eq!(result.rows[1][0], Value::Int64(4), "2*2=4");
    assert_eq!(result.rows[2][0], Value::Int64(6), "3*2=6");
}

// ── Test 6: MATCH … WITH collect(n.prop) AS list UNWIND list AS item RETURN ───
//
// Collect all values into a list via WITH, then UNWIND that list back into rows.
// Pattern: commonly used to fan-out aggregated lists for per-element processing.
//
// Status: NOT YET IMPLEMENTED.
// The WITH parser only accepts RETURN as the continuation clause, not UNWIND.
// Additionally, `collect()` in a WITH item would need aggregate-aware execution
// plumbing (same underlying issue as COUNT(*) in WITH, Test 3).
// Tracked by: SPA-134 (this ticket) — UNWIND as continuation after WITH.

#[test]
fn spa134_match_with_collect_unwind_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH collect(n.name) AS names \
             UNWIND names AS item \
             RETURN item",
        )
        .expect("MATCH … WITH collect … UNWIND … RETURN must succeed");

    // Should fan-out the 3 collected names back into 3 rows.
    assert_eq!(result.columns, vec!["item"]);
    assert_eq!(
        result.rows.len(),
        3,
        "collect then unwind should yield 3 rows"
    );

    let mut items: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String item, got {:?}", other),
        })
        .collect();
    items.sort_unstable();
    assert_eq!(
        items,
        vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()],
        "all 3 person names must appear after collect + unwind"
    );
}

// ── Test 7: MATCH … WITH … MATCH … WITH … RETURN (three-stage pipeline) ───────
//
// Three-stage pipeline chaining two WITH clauses separated by a MATCH.
// Pattern: MATCH → WITH (filter/rename) → MATCH (re-traverse) → WITH (transform) → RETURN.
//
// Status: NOT YET IMPLEMENTED.
// No multi-stage AST node exists; the parser terminates MatchWithStatement at
// the first RETURN it encounters.
// Tracked by: SPA-134 (this ticket) — full multi-stage pipeline execution.

#[test]
fn spa134_three_stage_match_with_pipeline() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Stage 1: find persons older than 25 (Alice=30, Carol=35).
    // Stage 2: re-match those persons via KNOWS relationships.
    // Stage 3: return the names of their known contacts.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n.name AS name, n.age AS age \
             WHERE age > 25 \
             MATCH (m:Person {name: name})-[:KNOWS]->(k:Person) \
             WITH k.name AS contact \
             RETURN contact",
        )
        .expect("three-stage pipeline must succeed");

    // Alice (30) knows Bob and Carol.
    // Carol (35) has no outgoing KNOWS edges in the seed data.
    // → contacts = [Bob, Carol] from Alice only.
    assert_eq!(result.columns, vec!["contact"]);
    assert_eq!(
        result.rows.len(),
        2,
        "Alice's two KNOWS contacts expected; Carol has no outgoing edges"
    );
}

// ── Test 8: MATCH … WHERE EXISTS { (n)-[:REL]->() } WITH … RETURN ─────────────
//
// EXISTS subquery in the leading MATCH WHERE, followed by a WITH projection.
// Pattern: filter nodes by existence of relationships before passing through WITH.
//
// Status: NOT YET IMPLEMENTED for the MatchWith pipeline.
// Although EXISTS subqueries are supported in plain MATCH … WHERE … RETURN
// (SPA-137), the collect_match_rows_for_with helper builds row_vals using
// build_row_vals which only inserts "{var}.col_{col_id}" entries — it does NOT
// inject a NodeRef entry.  eval_exists_subquery requires resolve_node_id_from_var
// to find a NodeRef, so it always returns false for rows produced by the WITH
// pipeline, causing zero rows to survive.
// Tracked by: SPA-134 (this ticket) — inject NodeRef into WITH-pipeline row_vals.

#[test]
fn spa134_exists_subquery_with_clause_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Only Alice and Bob have outgoing KNOWS relationships; Carol does not.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WHERE EXISTS { (n)-[:KNOWS]->(:Person) } \
             WITH n.name AS name, n.age AS age \
             RETURN name",
        )
        .expect("MATCH … WHERE EXISTS … WITH … RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "only Alice and Bob have outgoing KNOWS edges; Carol is excluded"
    );

    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String for name, got {:?}", other),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["Alice".to_string(), "Bob".to_string()],
        "Alice and Bob should be the only persons with outgoing KNOWS edges"
    );
}

/// Variant: EXISTS in the WITH WHERE (after WITH projection) also not supported.
///
/// Even if WITH n AS person WHERE EXISTS { ... } is syntactically valid,
/// the NodeRef is not available in the projected with_vals map, so the
/// EXISTS predicate cannot resolve the source node and returns false.
#[test]
fn spa134_exists_subquery_where_in_with_clause() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // WITH n AS person passes the node through, then WHERE filters by EXISTS.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n AS person \
             WHERE EXISTS { (person)-[:KNOWS]->(:Person) } \
             RETURN person.name",
        )
        .expect("MATCH … WITH n AS person WHERE EXISTS … RETURN must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "Alice and Bob have outgoing KNOWS edges; Carol should be excluded"
    );

    let mut names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String for name, got {:?}", other),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(names, vec!["Alice".to_string(), "Bob".to_string()],);
}

// ── Additional covered patterns ────────────────────────────────────────────────

/// MATCH … WITH … ORDER BY … LIMIT … RETURN (single-stage WITH + final ordering).
///
/// ORDER BY and LIMIT on MatchWithStatement are parsed but not applied by the
/// execution engine (execute_match_with does not use m.order_by or apply sorting).
/// Marked ignored because the ORDER BY result is non-deterministic.
#[test]
fn spa134_match_with_order_by_limit_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    // Get all persons, project name + age via WITH, sort by age DESC, take top 2.
    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n.name AS name, n.age AS age \
             RETURN name \
             ORDER BY age DESC LIMIT 2",
        )
        .expect("MATCH … WITH … RETURN ORDER BY … LIMIT must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "LIMIT 2 should return exactly 2 rows; got {:?}",
        result.rows
    );

    // Top-2 by age DESC: Carol (35), Alice (30).
    assert_eq!(
        result.rows[0][0],
        Value::String("Carol".to_string()),
        "first row should be Carol (age=35)"
    );
    assert_eq!(
        result.rows[1][0],
        Value::String("Alice".to_string()),
        "second row should be Alice (age=30)"
    );
}

/// MATCH … WITH … WHERE … RETURN with empty result set.
///
/// A WHERE predicate that matches no rows should return an empty result without error.
#[test]
fn spa134_match_with_where_empty_result() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH n.name AS name, n.age AS age \
             WHERE age > 999 \
             RETURN name",
        )
        .expect("MATCH … WITH WHERE age > 999 must not error");

    assert_eq!(
        result.rows.len(),
        0,
        "no person has age > 999; result should be empty"
    );
}

/// MATCH … WITH count(*) AS c RETURN c — aggregation through WITH without WHERE.
///
/// COUNT(*) in WITH is not aggregated by the engine; eval_expr returns Null for
/// CountStar. This test documents the current (incorrect) behaviour: 3 rows of
/// Null are returned rather than 1 row of Int64(3).
#[test]
fn spa134_match_with_count_no_filter_return() {
    let (_dir, db) = make_db();
    seed_social_graph(&db);

    let result = db
        .execute(
            "MATCH (n:Person) \
             WITH count(*) AS c \
             RETURN c",
        )
        .expect("MATCH … WITH count(*) AS c RETURN c must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "aggregate always returns exactly 1 row"
    );
    assert_eq!(result.rows[0][0], Value::Int64(3), "3 persons in the graph");
}
