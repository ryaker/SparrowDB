//! Regression tests for #444 and #430 — projection silently fabricating Null.
//!
//! Both issues share one root cause: the fast (non-eval) projection paths
//! resolved a RETURN column's *stored property* by re-parsing the (possibly
//! aliased) **output** column name instead of dispatching on the underlying
//! AST expression (`ReturnItem.expr`). Two independent symptoms fell out of
//! that:
//!
//! - **#444**: `RETURN n.name AS personName` hashed `"personName"` as if it
//!   were the property name, so the wrong (nonexistent) column was fetched
//!   from storage and the value came back `Null` — silently wrong, not an
//!   error.
//! - **#430**: `execute_pipeline_match_hop` (the executor for a MATCH stage
//!   following `WITH`) only ever read `pat.rels[0]` / `pat.nodes[0..2]` and
//!   dropped every hop after the first. A 2+-hop pattern after `WITH` bound
//!   only the first destination node; every later variable in the RETURN
//!   clause silently projected `Null` — or, worse, produced a row at all for
//!   a path that does not exist in the graph.
//!
//! All expected values below are derived by hand from the fixtures, never
//! captured from program output (see repo `feedback_derive_expected_from_source`).

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

// ═══════════════════════════════════════════════════════════════════════════
// #444 — aliased property projection
// ═══════════════════════════════════════════════════════════════════════════

/// One `Person` node: name="Alice", age=30. A second node, `Eve`, has a
/// `name` but no `age` at all — the control for "genuinely null" below.
fn setup_alias_graph() -> (GraphDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Eve'})")
        .expect("CREATE Eve (no age)");
    (db, dir)
}

/// alias == prop name: was already correct pre-fix; locks in no regression.
#[test]
fn alias_equal_to_property_name_is_correct() {
    let (db, _dir) = setup_alias_graph();
    let r = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
        .unwrap();
    assert_eq!(r.columns, vec!["name"]);
    assert_eq!(r.rows, vec![vec![Value::String("Alice".into())]]);
}

/// alias != prop name: the exact #444 reproduction.
#[test]
fn alias_different_from_property_name_resolves_correctly() {
    let (db, _dir) = setup_alias_graph();
    let r = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS personName")
        .unwrap();
    assert_eq!(r.columns, vec!["personName"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::String("Alice".into())]],
        "personName must resolve n.name, not Null (#444)"
    );
}

/// Multiple aliased columns in one RETURN, both different from their props.
#[test]
fn multiple_aliased_columns_all_resolve() {
    let (db, _dir) = setup_alias_graph();
    let r = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS a, n.age AS b")
        .unwrap();
    assert_eq!(r.columns, vec!["a", "b"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::String("Alice".into()), Value::Int64(30)]],
        "both aliased columns must resolve to their source properties"
    );
}

/// Alias on an expression (not a bare property) — this already routes
/// through the eval path (`expr_needs_eval_path`), so it must already work;
/// this test locks that in rather than assuming it from the code read.
#[test]
fn alias_on_function_expression_resolves_correctly() {
    let (db, _dir) = setup_alias_graph();
    let r = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN toUpper(n.name) AS upperName")
        .unwrap();
    assert_eq!(r.columns, vec!["upperName"]);
    assert_eq!(r.rows, vec![vec![Value::String("ALICE".into())]]);
}

/// Control: a genuinely absent property must still project Null — the fix
/// must not turn a real null into an error or a fabricated value.
#[test]
fn genuinely_missing_property_still_projects_null() {
    let (db, _dir) = setup_alias_graph();
    let r = db
        .execute("MATCH (n:Person {name: 'Eve'}) RETURN n.age AS personAge")
        .unwrap();
    assert_eq!(r.columns, vec!["personAge"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::Null]],
        "Eve has no age property; this Null is real, not a resolution failure"
    );
}

/// The degree-cache ORDER BY fast-path (`try_degree_sort_fastpath`,
/// SPA-272) has its own inline projection that had the identical bug: it
/// parsed the (possibly aliased) output column string instead of the AST
/// expression. `cypher_order_by_degree_alias_returns_top_k` in
/// spa_272_q7_cypher_wiring.rs only covers aliasing the *function name*
/// (`degree` vs `out_degree`), not an `AS` alias on the RETURN item.
#[test]
fn degree_fastpath_with_aliased_property_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Widget {id: 1})").unwrap();
    db.execute("CREATE (n:Widget {id: 2})").unwrap();
    db.execute("CREATE (n:Widget {id: 3})").unwrap();
    // id=1 has out-degree 2 (the top-1 by degree).
    db.execute("MATCH (a:Widget {id:1}),(b:Widget {id:2}) CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:Widget {id:1}),(b:Widget {id:3}) CREATE (a)-[:E]->(b)")
        .unwrap();

    let r = db
        .execute(
            "MATCH (n:Widget) RETURN n.id AS widgetId \
             ORDER BY out_degree(n) DESC LIMIT 1",
        )
        .unwrap();
    assert_eq!(r.columns, vec!["widgetId"]);
    assert_eq!(
        r.rows,
        vec![vec![Value::Int64(1)]],
        "widgetId must resolve n.id (the top-degree node is id=1), not Null"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// #430 — MATCH after WITH drops hops past the first
// ═══════════════════════════════════════════════════════════════════════════

/// Chain A -[:KNOWS]-> B -[:KNOWS]-> C -[:KNOWS]-> D.
/// A separate, isolated node E has no outgoing KNOWS edges at all.
fn setup_chain_graph() -> (GraphDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    for name in ["A", "B", "C", "D", "E"] {
        db.execute(&format!("CREATE (n:Person {{name: '{name}'}})"))
            .unwrap_or_else(|e| panic!("CREATE {name}: {e}"));
    }
    db.execute("MATCH (a:Person {name:'A'}),(b:Person {name:'B'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("A KNOWS B");
    db.execute("MATCH (b:Person {name:'B'}),(c:Person {name:'C'}) CREATE (b)-[:KNOWS]->(c)")
        .expect("B KNOWS C");
    db.execute("MATCH (c:Person {name:'C'}),(d:Person {name:'D'}) CREATE (c)-[:KNOWS]->(d)")
        .expect("C KNOWS D");
    (db, dir)
}

/// Single hop after WITH already worked pre-fix; locks in no regression.
#[test]
fn one_hop_after_with_still_works() {
    let (db, _dir) = setup_chain_graph();
    let r = db
        .execute(
            "MATCH (a:Person {name:'A'}) WITH a \
             MATCH (a)-[:KNOWS]->(b:Person) RETURN b.name",
        )
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::String("B".into())]]);
}

/// The exact #430 reproduction: two hops after WITH must bind the third node.
#[test]
fn two_hop_after_with_binds_third_node() {
    let (db, _dir) = setup_chain_graph();
    let r = db
        .execute(
            "MATCH (a:Person {name:'A'}) WITH a \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN b.name, c.name",
        )
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::String("B".into()), Value::String("C".into())]],
        "c must bind to C, not project Null (#430)"
    );
}

/// Three hops after WITH must bind the fourth node.
#[test]
fn three_hop_after_with_binds_fourth_node() {
    let (db, _dir) = setup_chain_graph();
    let r = db
        .execute(
            "MATCH (a:Person {name:'A'}) WITH a \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) \
             RETURN d.name",
        )
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::String("D".into())]]);
}

/// WHERE on the final node of a 2-hop post-WITH pattern must still filter.
#[test]
fn two_hop_after_with_respects_where_on_final_node() {
    let (db, _dir) = setup_chain_graph();
    let r = db
        .execute(
            "MATCH (a:Person {name:'A'}) WITH a \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE c.name = 'Z' RETURN b.name, c.name",
        )
        .unwrap();
    assert_eq!(
        r.rows.len(),
        0,
        "no node named Z exists two hops out; WHERE must filter it to zero rows"
    );
}

/// A post-WITH path that genuinely does not exist must yield zero rows, not
/// a fabricated `[[Null]]` row. D has no outgoing KNOWS edge, so the 4-hop
/// chain A->B->C->D->? does not exist.
#[test]
fn post_with_nonexistent_path_yields_empty_not_null_row() {
    let (db, _dir) = setup_chain_graph();
    let r = db
        .execute(
            "MATCH (a:Person {name:'A'}) WITH a \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person)-[:KNOWS]->(e:Person) \
             RETURN e.name",
        )
        .unwrap();
    assert_eq!(
        r.rows.len(),
        0,
        "the 4-hop pattern does not exist in the graph; expected [] but got {:?} \
         (a fabricated [[Null]] row would be worse than dropping the hop)",
        r.rows
    );
}

/// A node with zero outgoing edges at hop 1 must not produce any rows for a
/// downstream multi-hop pattern.
#[test]
fn multi_hop_after_with_from_isolated_node_yields_empty() {
    let (db, _dir) = setup_chain_graph();
    let r = db
        .execute(
            "MATCH (a:Person {name:'E'}) WITH a \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN c.name",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 0, "E has no outgoing KNOWS edges at all");
}
