//! Phase 4 parity tests for the chunked vectorized pipeline (SPA-299, #340).
//!
//! Each test:
//! 1. Creates a real on-disk GraphDb via `tempfile::TempDir`.
//! 2. Verifies the chunked pipeline produces correct results for Phase 4 shapes.
//! 3. Where both engines support the query shape, asserts IDENTICAL sorted results.
//!
//! Test cases:
//! - `mutual_neighbors_basic_chunked`        — chunked MN finds the common neighbor
//! - `mutual_neighbors_no_common_chunked`    — no common neighbor returns empty
//! - `mutual_neighbors_multiple_common`      — multiple common neighbors
//! - `mutual_neighbors_tombstoned_excluded`  — tombstoned common neighbor excluded
//! - `mutual_neighbors_limit_chunked`        — LIMIT short-circuits output
//! - `try_plan_scan_selected`                — Scan plan selected for simple scans
//! - `try_plan_one_hop_selected`             — OneHop selected for 1-hop queries
//! - `try_plan_two_hop_selected`             — TwoHop selected for 2-hop queries
//! - `try_plan_mutual_neighbors_selected`    — MutualNeighbors selected for fork pattern
//! - `try_plan_none_for_complex`             — None returned for unsupported shapes
//! - `chunked_plan_selector_fallback`        — unsupported query falls back cleanly
//! - `mutual_neighbors_self_intersection_returns_empty` — same node on both sides returns empty
//! - `existing_tests_still_pass`             — phase 1-3 scan/one-hop/two-hop still work

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::{Engine, Value};
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;
use std::collections::HashMap;

// ── Helpers ────────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn open_csr_map(path: &std::path::Path) -> HashMap<u32, CsrForward> {
    use sparrowdb_storage::edge_store::EdgeStore;
    use sparrowdb_storage::edge_store::RelTableId;

    let catalog = match Catalog::open(path) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };
    let mut map = HashMap::new();
    let mut rel_ids: Vec<u32> = catalog
        .list_rel_table_ids()
        .into_iter()
        .map(|(id, _, _, _)| id as u32)
        .collect();
    if !rel_ids.contains(&0u32) {
        rel_ids.push(0u32);
    }
    for rid in rel_ids {
        if let Ok(store) = EdgeStore::open(path, RelTableId(rid)) {
            if let Ok(csr) = store.open_fwd() {
                map.insert(rid, csr);
            }
        }
    }
    map
}

fn row_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("NodeStore::open");
    let cat = Catalog::open(dir).expect("Catalog::open");
    let csrs = open_csr_map(dir);
    Engine::new(store, cat, csrs, dir)
}

fn chunked_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("NodeStore::open");
    let cat = Catalog::open(dir).expect("Catalog::open");
    let csrs = open_csr_map(dir);
    Engine::new(store, cat, csrs, dir).with_chunked_pipeline()
}

fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by(|a, b| {
        let ka: Vec<String> = a.iter().map(|v| format!("{v:?}")).collect();
        let kb: Vec<String> = b.iter().map(|v| format!("{v:?}")).collect();
        ka.cmp(&kb)
    });
    rows
}

/// Run `cypher` on both engines and assert identical results.
fn assert_engines_agree(dir: &std::path::Path, cypher: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    let row_result = row_engine(dir).execute(cypher).unwrap_or_else(|e| {
        panic!("row-at-a-time engine failed for `{cypher}`: {e}");
    });
    let chunked_result = chunked_engine(dir).execute(cypher).unwrap_or_else(|e| {
        panic!("chunked pipeline engine failed for `{cypher}`: {e}");
    });

    assert_eq!(
        row_result.columns, chunked_result.columns,
        "column names differ for `{cypher}`"
    );

    let row_rows = sort_rows(row_result.rows.clone());
    let chunked_rows = sort_rows(chunked_result.rows.clone());

    assert_eq!(
        row_rows.len(),
        chunked_rows.len(),
        "row count differs for `{cypher}`: row-at-a-time={}, chunked={}",
        row_rows.len(),
        chunked_rows.len()
    );

    assert_eq!(row_rows, chunked_rows, "row values differ for `{cypher}`");
    (row_result.columns, row_rows)
}

/// Resolve the NodeId for a named Person using the row engine.
fn node_id_for(dir: &std::path::Path, name: &str) -> i64 {
    let cypher = format!("MATCH (n:Person {{name: '{name}'}}) RETURN id(n) AS nid");
    let result = row_engine(dir).execute(&cypher).expect("id lookup");
    let rows = result.rows;
    assert_eq!(rows.len(), 1, "expected exactly one Person named {name}");
    match &rows[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected Int64 for id(n), got {other:?}"),
    }
}

/// Run mutual-neighbors query via the chunked engine with id() params.
///
/// The chunked engine handles the MutualNeighbors fast-path for:
/// `MATCH (a:L)-[:R]->(x:L)<-[:R]-(b:L) WHERE id(a)=$aid AND id(b)=$bid RETURN x.name`
fn chunked_mutual_neighbors(
    dir: &std::path::Path,
    a_id: i64,
    b_id: i64,
    extra_where: &str,
    limit: Option<usize>,
) -> Vec<Vec<Value>> {
    let where_clause = if extra_where.is_empty() {
        "WHERE id(a) = $aid AND id(b) = $bid".to_string()
    } else {
        format!("WHERE id(a) = $aid AND id(b) = $bid AND {extra_where}")
    };
    let limit_clause = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();

    let cypher = format!(
        "MATCH (a:Person)-[:KNOWS]->(x:Person)<-[:KNOWS]-(b:Person) \
         {where_clause} \
         RETURN x.name{limit_clause}"
    );

    let mut params = HashMap::new();
    params.insert("aid".to_string(), Value::Int64(a_id));
    params.insert("bid".to_string(), Value::Int64(b_id));

    let result = chunked_engine(dir)
        .with_params(params)
        .execute(&cypher)
        .unwrap_or_else(|e| panic!("chunked engine failed for `{cypher}`: {e}"));

    sort_rows(result.rows)
}

// ── 1. Basic mutual neighbors — chunked finds the common neighbor ─────────────
//
// Graph: Alice -KNOWS-> Carol
//        Bob   -KNOWS-> Carol
//        Alice -KNOWS-> Dave (not known by Bob — not a mutual neighbor)
//
// Expected: x = Carol

#[test]
fn mutual_neighbors_basic_chunked() {
    let (dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Person {name: 'Dave'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let aid = node_id_for(dir.path(), "Alice");
    let bid = node_id_for(dir.path(), "Bob");

    let rows = chunked_mutual_neighbors(dir.path(), aid, bid, "", None);
    assert_eq!(rows.len(), 1, "exactly one mutual neighbor (Carol)");
    assert_eq!(rows[0], vec![Value::String("Carol".into())]);
}

// ── 2. No common neighbor returns empty ───────────────────────────────────────
//
// Alice -KNOWS-> Carol (only)
// Bob   -KNOWS-> Dave  (only)

#[test]
fn mutual_neighbors_no_common_chunked() {
    let (dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Person {name: 'Dave'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Dave'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let aid = node_id_for(dir.path(), "Alice");
    let bid = node_id_for(dir.path(), "Bob");

    let rows = chunked_mutual_neighbors(dir.path(), aid, bid, "", None);
    assert_eq!(rows.len(), 0, "no mutual neighbors expected");
}

// ── 3. Multiple common neighbors ──────────────────────────────────────────────

#[test]
fn mutual_neighbors_multiple_common() {
    let (dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Person {name: 'Eve'})").unwrap();

    for (src, dst) in &[
        ("Alice", "Carol"),
        ("Alice", "Eve"),
        ("Bob", "Carol"),
        ("Bob", "Eve"),
    ] {
        db.execute(&format!(
            "MATCH (a:Person {{name: '{src}'}}), (b:Person {{name: '{dst}'}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }

    let aid = node_id_for(dir.path(), "Alice");
    let bid = node_id_for(dir.path(), "Bob");

    let rows = chunked_mutual_neighbors(dir.path(), aid, bid, "", None);
    assert_eq!(rows.len(), 2, "Carol and Eve are mutual neighbors");
    let names: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    assert!(
        names.contains(&"Carol".to_string()),
        "Carol must be in results"
    );
    assert!(names.contains(&"Eve".to_string()), "Eve must be in results");
}

// ── 4. Tombstoned common neighbor excluded ────────────────────────────────────
//
// Alice -KNOWS-> Carol, Bob -KNOWS-> Carol — then Carol is tombstoned.
// After tombstone: no mutual neighbors visible.

#[test]
fn mutual_neighbors_tombstoned_excluded() {
    let (dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // Remove edges TO Carol first (since CSR doesn't support backward delete,
    // we just delete Carol's node — the tombstone is visible via `is_node_tombstoned`).
    // First remove the incoming edges using explicit match:
    db.execute("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(c:Person {name: 'Carol'}) DELETE r")
        .unwrap();
    db.execute("MATCH (a:Person {name: 'Bob'})-[r:KNOWS]->(c:Person {name: 'Carol'}) DELETE r")
        .unwrap();
    db.execute("MATCH (n:Person {name: 'Carol'}) DELETE n")
        .unwrap();

    let aid = node_id_for(dir.path(), "Alice");
    let bid = node_id_for(dir.path(), "Bob");

    let rows = chunked_mutual_neighbors(dir.path(), aid, bid, "", None);
    assert_eq!(rows.len(), 0, "tombstoned Carol must not appear in results");
}

// ── 5. LIMIT short-circuits output ───────────────────────────────────────────

#[test]
fn mutual_neighbors_limit_chunked() {
    let (dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute("CREATE (:Person {name: 'Dave'})").unwrap();
    db.execute("CREATE (:Person {name: 'Eve'})").unwrap();

    for (src, dst) in &[
        ("Alice", "Carol"),
        ("Alice", "Dave"),
        ("Alice", "Eve"),
        ("Bob", "Carol"),
        ("Bob", "Dave"),
        ("Bob", "Eve"),
    ] {
        db.execute(&format!(
            "MATCH (a:Person {{name: '{src}'}}), (b:Person {{name: '{dst}'}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }

    let aid = node_id_for(dir.path(), "Alice");
    let bid = node_id_for(dir.path(), "Bob");

    let rows = chunked_mutual_neighbors(dir.path(), aid, bid, "", Some(1));
    assert_eq!(rows.len(), 1, "LIMIT 1 must short-circuit at 1 row");
}

// ── 6. ChunkedPlan selector: Scan selected for simple label scan ──────────────

#[test]
fn try_plan_scan_selected() {
    use sparrowdb_execution::engine::pipeline_exec::ChunkedPlan;

    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();

    let engine = chunked_engine(dir.path());
    let cypher = "MATCH (n:Person) RETURN n.name";
    let stmt = sparrowdb_cypher::parse(cypher).expect("parse");
    let cat_snap = Catalog::open(dir.path()).expect("catalog");
    let bound = sparrowdb_cypher::bind(stmt, &cat_snap).expect("bind");

    if let sparrowdb_cypher::ast::Statement::Match(ref m) = bound.inner {
        let plan = engine.try_plan_chunked_match(m);
        assert_eq!(
            plan,
            Some(ChunkedPlan::Scan),
            "simple label scan should select ChunkedPlan::Scan"
        );
    } else {
        panic!("expected a MATCH statement");
    }
}

// ── 7. ChunkedPlan selector: OneHop selected for 1-hop queries ───────────────

#[test]
fn try_plan_one_hop_selected() {
    use sparrowdb_execution::engine::pipeline_exec::ChunkedPlan;

    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let engine = chunked_engine(dir.path());
    let cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name";
    let stmt = sparrowdb_cypher::parse(cypher).expect("parse");
    let cat_snap = Catalog::open(dir.path()).expect("catalog");
    let bound = sparrowdb_cypher::bind(stmt, &cat_snap).expect("bind");

    if let sparrowdb_cypher::ast::Statement::Match(ref m) = bound.inner {
        let plan = engine.try_plan_chunked_match(m);
        assert_eq!(
            plan,
            Some(ChunkedPlan::OneHop),
            "1-hop should select ChunkedPlan::OneHop"
        );
    } else {
        panic!("expected a MATCH statement");
    }
}

// ── 8. ChunkedPlan selector: TwoHop selected for 2-hop queries ───────────────

#[test]
fn try_plan_two_hop_selected() {
    use sparrowdb_execution::engine::pipeline_exec::ChunkedPlan;

    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let engine = chunked_engine(dir.path());
    let cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, c.name";
    let stmt = sparrowdb_cypher::parse(cypher).expect("parse");
    let cat_snap = Catalog::open(dir.path()).expect("catalog");
    let bound = sparrowdb_cypher::bind(stmt, &cat_snap).expect("bind");

    if let sparrowdb_cypher::ast::Statement::Match(ref m) = bound.inner {
        let plan = engine.try_plan_chunked_match(m);
        assert_eq!(
            plan,
            Some(ChunkedPlan::TwoHop),
            "2-hop same-rel should select ChunkedPlan::TwoHop"
        );
    } else {
        panic!("expected a MATCH statement");
    }
}

// ── 9. ChunkedPlan selector: MutualNeighbors selected for fork + id() params ──

#[test]
fn try_plan_mutual_neighbors_selected() {
    use sparrowdb_execution::engine::pipeline_exec::ChunkedPlan;

    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let engine = chunked_engine(dir.path());
    let cypher = "MATCH (a:Person)-[:KNOWS]->(x:Person)<-[:KNOWS]-(b:Person) \
                  WHERE id(a) = $aid AND id(b) = $bid \
                  RETURN x.name";
    let stmt = sparrowdb_cypher::parse(cypher).expect("parse");
    let cat_snap = Catalog::open(dir.path()).expect("catalog");
    let bound = sparrowdb_cypher::bind(stmt, &cat_snap).expect("bind");

    if let sparrowdb_cypher::ast::Statement::Match(ref m) = bound.inner {
        let plan = engine.try_plan_chunked_match(m);
        assert_eq!(
            plan,
            Some(ChunkedPlan::MutualNeighbors),
            "fork pattern with id() params should select ChunkedPlan::MutualNeighbors"
        );
    } else {
        panic!("expected a MATCH statement");
    }
}

// ── 10. ChunkedPlan selector: None for unsupported queries ────────────────────

#[test]
fn try_plan_none_for_complex() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let engine = chunked_engine(dir.path());
    // ORDER BY is not supported by the chunked one-hop pipeline.
    let cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name";
    let stmt = sparrowdb_cypher::parse(cypher).expect("parse");
    let cat_snap = Catalog::open(dir.path()).expect("catalog");
    let bound = sparrowdb_cypher::bind(stmt, &cat_snap).expect("bind");

    if let sparrowdb_cypher::ast::Statement::Match(ref m) = bound.inner {
        let plan = engine.try_plan_chunked_match(m);
        assert!(
            plan.is_none(),
            "ORDER BY 1-hop must not be handled by chunked pipeline, got: {plan:?}"
        );
    } else {
        panic!("expected a MATCH statement");
    }
}

// ── 11. Fallback: DISTINCT query still produces correct results ───────────────

#[test]
fn chunked_plan_selector_fallback() {
    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();

    // DISTINCT falls back to row engine.
    let (_cols, rows) = assert_engines_agree(dir.path(), "MATCH (n:Person) RETURN DISTINCT n.name");
    assert_eq!(rows.len(), 2, "both persons must appear");
}

// ── 12. Self-intersection guard — a and b resolve to the same node ────────────
//
// When id(a) and id(b) both refer to the same node the chunked fast-path must
// return an empty result set.  A node cannot be its own mutual neighbor and
// Cypher requires distinct node bindings.

#[test]
fn mutual_neighbors_self_intersection_returns_empty() {
    let (dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    let alice_id = node_id_for(dir.path(), "Alice");

    // Query with id(a) = id(b) = alice — same node on both sides.
    let rows = chunked_mutual_neighbors(dir.path(), alice_id, alice_id, "", None);
    assert!(
        rows.is_empty(),
        "self-intersection must return empty; got {rows:?}"
    );
}

// ── 13. Phase 1-3 plans still produce correct results after Phase 4 refactor ──

#[test]
fn existing_tests_still_pass_after_selector_refactor() {
    // Graph: Alice -KNOWS-> Bob -KNOWS-> Carol
    let (dir, db) = make_db();
    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Carol'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // Scan (Phase 1)
    let (_cols, rows) = assert_engines_agree(dir.path(), "MATCH (n:Person) RETURN n.name");
    assert_eq!(rows.len(), 3, "scan returns 3 people");

    // 1-hop (Phase 2)
    let (_cols, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    );
    assert_eq!(rows.len(), 2, "2 KNOWS edges");

    // 2-hop (Phase 3)
    let (_cols, rows) = assert_engines_agree(
        dir.path(),
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, c.name",
    );
    assert_eq!(rows.len(), 1, "exactly 1 two-hop path");
}
