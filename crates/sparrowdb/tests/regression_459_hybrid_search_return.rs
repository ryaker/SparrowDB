//! Regression guard for #459: `MATCH ... RETURN hybrid_search(...)` (and the
//! `WHERE`/`UNWIND` shapes) returned `Value::Null` against a genuinely
//! **healthy** index.
//!
//! # Root cause (established by reading, not by running the code first)
//!
//! Two independent bugs, not the one the issue title guessed at:
//!
//! 1. `Engine::execute_scan`'s eval path funnels non-aggregate RETURN items
//!    through `aggregate_rows_graph` (`engine/expr.rs`), which only recurses
//!    into `eval_expr_graph` when `expr_needs_graph()` (`engine/mod.rs`)
//!    returns `true` for a RETURN item. That function checked for
//!    `ShortestPath`/`ExistsSubquery`/`CaseWhen` but never `Expr::FnCall`, so
//!    a bare `hybrid_search(...)` fell through to the non-aggregate
//!    "plain projection" branch of the free `aggregate_rows()` helper, which
//!    evaluates every item with the **non-engine-aware** free function
//!    `eval_expr()`. That function's `FnCall` arm dispatches to
//!    `functions::dispatch_function`, which has never heard of
//!    `hybrid_search`/`full_text_search`/`bm25_score` (those three are
//!    special-cased only inside `Engine::eval_expr_graph`), so it silently
//!    returns `Value::Null` for an unrelated reason: unknown function name.
//!    This explains the `MATCH ... RETURN` and `MATCH ... WHERE ... RETURN`
//!    rows in the issue table.
//!
//!    This is *not* the chunked pipeline: `can_use_chunked_pipeline` already
//!    routes any non-aggregate `FnCall` in RETURN to the row engine via
//!    `expr_needs_eval_path` (`engine/mod.rs`), which predates this fix and
//!    already covers `hybrid_search`. The chunked scan path is never reached
//!    for this query shape.
//!
//! 2. `Engine::execute_unwind` (`engine/mutation.rs`) is a *separate* code
//!    path (bare `UNWIND ... RETURN` parses to `Statement::Unwind`, not
//!    `Statement::Pipeline`). It projected RETURN items with a hand-rolled
//!    check: if the item was exactly `Expr::Var(alias)` it returned the
//!    unwound value, and for literally every other expression shape —
//!    including `hybrid_search(...)`, arithmetic, or any other function
//!    call — it hardcoded `Value::Null`. This explains the `UNWIND ...
//!    RETURN` row independently of bug 1.
//!
//! # What must NOT change
//!
//! The `expr_needs_graph` fix only adds the three functions
//! `Engine::eval_expr_graph` already special-cases
//! (`hybrid_search`/`full_text_search`/`bm25_score`); it does not weaken the
//! `#456`/`#458`/`#462` absent-vs-damaged guarantee. A genuinely corrupt
//! index must still surface as `Value::Null` through this same
//! `MATCH ... RETURN` shape — asserted below by reusing the truncation
//! technique from `regression_456_load_is_not_destructive.rs`.
//!
//! Every expected value below is derived by hand from the fixture, per this
//! repo's testing rule (see `regression_406.rs`/[[feedback_derive_expected_from_source]]).

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

fn exec(db: &GraphDb, cypher: &str) {
    db.execute(cypher)
        .unwrap_or_else(|e| panic!("exec failed for `{cypher}`: {e}"));
}

fn get_node_id(db: &GraphDb, label: &str, key_prop: &str, key_val: &str) -> u64 {
    let q = format!("MATCH (n:{label}) WHERE n.{key_prop} = '{key_val}' RETURN id(n) AS nid");
    let res = db.execute(&q).expect("get_node_id query");
    match &res.rows[0][0] {
        Value::Int64(n) => *n as u64,
        other => panic!("expected Int64 node_id, got {other:?}"),
    }
}

/// Flatten a `Value::List<Value::Map{node_id, ...}>` hybrid_search result
/// into the ordered list of node ids it contains. Panics on any other shape
/// so a Null (or otherwise malformed) result cannot silently read as "empty".
fn hit_ids(v: &Value) -> Vec<u64> {
    match v {
        Value::List(items) => items
            .iter()
            .map(|item| match item {
                Value::Map(kvs) => match kvs.iter().find(|(k, _)| k == "node_id") {
                    Some((_, Value::Int64(n))) => *n as u64,
                    other => panic!("expected node_id: Int64 entry, got {other:?}"),
                },
                other => panic!("expected Map hit, got {other:?}"),
            })
            .collect(),
        other => panic!("expected List, got {other:?}"),
    }
}

/// Build the fixture two-node `Doc` index this whole file shares:
/// - n0: content "rust graph database", embedding [1,0,0,0] — the perfect
///   match on both the vector query [1,0,0,0] and the text query "rust
///   graph" (contains both terms; n1 contains neither).
/// - n1: content "unrelated content", embedding [0,1,0,0] — orthogonal
///   vector, zero text-term overlap.
///
/// Hand-derived expectation for `hybrid_search('Doc','embedding','content',
/// [1,0,0,0],'rust graph',1)`: vector search ranks n0 first (cosine
/// similarity 1.0 vs 0.0), BM25 ranks n0 first (2 matching terms vs 0), so
/// n0 is first under RRF fusion by both signals regardless of tie-break
/// details for n1 — and `k=1` truncates to exactly that one entry.
fn build_two_node_fixture(dir: &std::path::Path) -> (GraphDb, u64, u64) {
    let db = open_db(dir);
    db.create_vector_index("Doc", "embedding", 4, "cosine")
        .expect("create vector index");
    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.content)");

    exec(
        &db,
        "CREATE (d:Doc {dockey: 'n0', content: 'rust graph database'})",
    );
    exec(
        &db,
        "CREATE (d:Doc {dockey: 'n1', content: 'unrelated content'})",
    );
    let n0 = get_node_id(&db, "Doc", "dockey", "n0");
    let n1 = get_node_id(&db, "Doc", "dockey", "n1");

    let arc = db.get_vector_index("Doc", "embedding").expect("vec index");
    arc.write()
        .expect("w")
        .insert(n0, &[1.0_f32, 0.0, 0.0, 0.0]);
    arc.write()
        .expect("w")
        .insert(n1, &[0.0_f32, 1.0, 0.0, 0.0]);
    let vec_dir = dir.join("vector_indexes");
    arc.read()
        .expect("r")
        .save(&vec_dir, "Doc", "embedding")
        .expect("save vector index");

    (db, n0, n1)
}

const QUERY_EXPR: &str = "hybrid_search('Doc', 'embedding', 'content', \
    [1.0, 0.0, 0.0, 0.0], 'rust graph', 1)";

// ── 1. MATCH ... RETURN hybrid_search(...) ─────────────────────────────────

#[test]
fn match_return_hybrid_search_healthy_index_returns_hits() {
    let dir = tempfile::tempdir().unwrap();
    let (db, n0, _n1) = build_two_node_fixture(dir.path());

    let result = db
        .execute(&format!("MATCH (d:Doc) RETURN {QUERY_EXPR} AS hits"))
        .expect("MATCH ... RETURN hybrid_search must execute");

    assert_eq!(result.rows.len(), 2, "one row per Doc node (n0 and n1)");
    for (i, row) in result.rows.iter().enumerate() {
        let ids = hit_ids(&row[0]);
        assert_eq!(
            ids,
            vec![n0],
            "row {i}: hybrid_search does not depend on the matched node, so \
             every row must carry the same single top hit (n0); got {ids:?} \
             (raw value {:?})",
            row[0]
        );
    }
}

// ── 2. MATCH ... WHERE ... RETURN hybrid_search(...) ───────────────────────

#[test]
fn match_where_return_hybrid_search_healthy_index_returns_hits() {
    let dir = tempfile::tempdir().unwrap();
    let (db, n0, _n1) = build_two_node_fixture(dir.path());

    let result = db
        .execute(&format!(
            "MATCH (d:Doc) WHERE d.dockey = 'n0' RETURN {QUERY_EXPR} AS hits"
        ))
        .expect("MATCH ... WHERE ... RETURN hybrid_search must execute");

    assert_eq!(result.rows.len(), 1, "WHERE narrows the scan to one row");
    let ids = hit_ids(&result.rows[0][0]);
    assert_eq!(ids, vec![n0], "got {:?}", result.rows[0][0]);
}

// ── 3. UNWIND ... RETURN hybrid_search(...) ────────────────────────────────

#[test]
fn unwind_return_hybrid_search_healthy_index_returns_hits() {
    let dir = tempfile::tempdir().unwrap();
    let (db, n0, _n1) = build_two_node_fixture(dir.path());

    let result = db
        .execute(&format!(
            "UNWIND [1, 2, 3] AS i RETURN {QUERY_EXPR} AS hits"
        ))
        .expect("UNWIND ... RETURN hybrid_search must execute");

    assert_eq!(result.rows.len(), 3, "one row per unwound element");
    for (i, row) in result.rows.iter().enumerate() {
        let ids = hit_ids(&row[0]);
        assert_eq!(
            ids,
            vec![n0],
            "row {i}: hybrid_search does not reference the UNWIND alias, so \
             every row must carry the same single top hit (n0); got {ids:?} \
             (raw value {:?})",
            row[0]
        );
    }
}

// ── 4. UNWIND ... RETURN i (baseline: the alias itself must still work) ───

/// The pre-fix `execute_unwind` distinguished the bare alias variable by a
/// hand-rolled `Expr::Var` check. Routing everything through
/// `eval_expr_graph` must not regress that ordinary case.
#[test]
fn unwind_return_bare_alias_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    let result = db
        .execute("UNWIND [10, 20, 30] AS i RETURN i AS val")
        .expect("UNWIND ... RETURN i must execute");

    let vals: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int64(n) => *n,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

// ── 5. Healthy-but-empty index: genuine non-match must be an empty List,
//        never Null (Null is reserved for damage — #445/#456/#458). ────────

#[test]
fn match_return_hybrid_search_no_match_returns_empty_list_not_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // A second, valid, healthy index registered on a label with zero nodes
    // and zero inserted vectors — persisted to disk so the query path loads
    // a real (empty) index rather than treating it as "not configured".
    db.create_vector_index("EmptyDoc", "emb", 4, "cosine")
        .expect("create empty vector index");
    exec(&db, "CREATE FULLTEXT INDEX FOR (n:EmptyDoc) ON (n.text)");
    let arc = db
        .get_vector_index("EmptyDoc", "emb")
        .expect("empty vec index handle");
    let vec_dir = dir.path().join("vector_indexes");
    arc.read()
        .expect("r")
        .save(&vec_dir, "EmptyDoc", "emb")
        .expect("save empty vector index");

    // Need at least one row to project from; use an unrelated populated label.
    exec(&db, "CREATE (d:Doc {dockey: 'only'})");

    let result = db
        .execute(
            "MATCH (d:Doc) RETURN hybrid_search('EmptyDoc', 'emb', 'text', \
             [1.0, 0.0, 0.0, 0.0], 'anything', 5) AS hits",
        )
        .expect("hybrid_search against a healthy empty index must not error");

    assert_eq!(result.rows.len(), 1);
    let ids = hit_ids(&result.rows[0][0]);
    assert!(
        ids.is_empty(),
        "an index with zero vectors and zero indexed text is healthy and has \
         no matches — the result must be an empty List, not Null (which is \
         reserved for damage); got {:?}",
        result.rows[0][0]
    );
}

// ── 6. Corrupt index: the #456/#458 guard must survive through this shape ──

/// Truncate the on-disk HNSW file to 4 bytes — the same undecodable-by-hand
/// derivation as `regression_456_load_is_not_destructive.rs::truncate_to_4_bytes`.
fn truncate_to_4_bytes(file: &std::path::Path) {
    let original = std::fs::metadata(file).expect("stat index file").len();
    assert!(
        original > 4,
        "fixture precondition: a serialised VectorIndex must exceed the 4-byte \
         truncation point, got {original} bytes"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(file)
        .expect("open index file for truncation")
        .set_len(4)
        .expect("truncate index file to 4 bytes");
}

#[test]
fn match_return_hybrid_search_corrupt_index_still_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _n0, _n1) = build_two_node_fixture(dir.path());

    // Sanity: the healthy baseline must actually produce hits through this
    // exact query shape before we corrupt anything underneath it.
    let baseline = db
        .execute(&format!("MATCH (d:Doc) RETURN {QUERY_EXPR} AS hits"))
        .expect("baseline must execute");
    assert!(
        !hit_ids(&baseline.rows[0][0]).is_empty(),
        "fixture precondition: the healthy index must return hits"
    );

    let file = dir
        .path()
        .join("vector_indexes")
        .join("hnsw_Doc_embedding.bin");
    truncate_to_4_bytes(&file);

    let result = db
        .execute(&format!("MATCH (d:Doc) RETURN {QUERY_EXPR} AS hits"))
        .expect("the query itself must not error even though the index is damaged");

    assert_eq!(result.rows.len(), 2);
    for (i, row) in result.rows.iter().enumerate() {
        assert!(
            matches!(row[0], Value::Null),
            "row {i}: a damaged index must still surface as Null through \
             MATCH ... RETURN — the #459 routing fix must not weaken the \
             #456/#458 absent-vs-damaged guard; got {:?}",
            row[0]
        );
    }
}
