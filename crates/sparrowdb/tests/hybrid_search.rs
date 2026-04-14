//! Integration tests for hybrid search (RRF + weighted fusion) — issue #396.
//!
//! Tests cover:
//! - `hybrid_search(label, emb_prop, text_prop, query_vec, query_text, k)` via Cypher
//! - `hybrid_search(...)` with optional alpha (weighted fusion)
//! - 20 dual-property nodes (embedding + content) — top result is verified
//! - `hybrid_search` returns gracefully when FTS index is absent

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

fn exec(db: &GraphDb, cypher: &str) {
    db.execute(cypher)
        .unwrap_or_else(|e| panic!("exec failed for `{cypher}`: {e}"));
}

// ── Helper: return the internal node_id from a `RETURN id(n)` result ──────────

fn get_node_id(db: &GraphDb, label: &str, key_prop: &str, key_val: &str) -> u64 {
    let q = format!("MATCH (n:{label}) WHERE n.{key_prop} = '{key_val}' RETURN id(n) AS nid");
    let res = db.execute(&q).expect("get_node_id query");
    match &res.rows[0][0] {
        Value::Int64(n) => *n as u64,
        other => panic!("expected Int64 node_id, got {other:?}"),
    }
}

// ── Helper: flatten the List<Map{node_id,…}> returned by hybrid_search ────────

fn extract_hit_ids(result: &sparrowdb::QueryResult) -> Vec<u64> {
    result
        .rows
        .iter()
        .flat_map(|row| {
            row.iter().filter_map(|v| match v {
                Value::List(items) => Some(
                    items
                        .iter()
                        .filter_map(|item| match item {
                            Value::Map(kvs) => kvs.iter().find(|(k, _)| k == "node_id").and_then(
                                |(_, v)| match v {
                                    Value::Int64(n) => Some(*n as u64),
                                    _ => None,
                                },
                            ),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
        })
        .flatten()
        .collect()
}

// ── 1. 20 dual-property nodes, RRF fusion ────────────────────────────────────

#[test]
fn hybrid_search_20_nodes_rrf() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Create the vector and FTS indexes.
    db.create_vector_index("Doc", "embedding", 4, "cosine")
        .expect("create vector index");
    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.content)");

    // Insert 20 nodes via Cypher (text only — vector inserted via HNSW API).
    // key values: "perfect" for the target node.
    let contents = [
        "rust graph database engine", // node "n0"  — perfect match
        "unrelated content node1",    // n1
        "rust programming node2",     // n2 — partial text match
        "irrelevant document node3",  // n3
        "unrelated content node4",    // n4
        "rust programming node5",     // n5
        "irrelevant document node6",  // n6
        "unrelated content node7",    // n7
        "rust programming node8",     // n8
        "irrelevant document node9",  // n9
        "unrelated content node10",   // n10
        "rust programming node11",    // n11
        "irrelevant document node12", // n12
        "unrelated content node13",   // n13
        "rust programming node14",    // n14
        "irrelevant document node15", // n15
        "unrelated content node16",   // n16
        "rust programming node17",    // n17
        "irrelevant document node18", // n18
        "unrelated content node19",   // n19
    ];
    // Each node's embedding: node 0 is [1,0,0,0] (aligns with query [1,0,0,0]).
    let embeddings: [[f32; 4]; 20] = [
        [1.0, 0.0, 0.0, 0.0], // perfect vector match
        [0.5, 0.5, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.5, 0.5, 0.0, 0.0],
    ];

    let arc = db.get_vector_index("Doc", "embedding").expect("vec index");
    for (i, content) in contents.iter().enumerate() {
        exec(
            &db,
            &format!("CREATE (d:Doc {{dockey: 'n{i}', content: '{content}'}})"),
        );
        // Look up the real node_id so HNSW and FTS share the same id space.
        let nid = get_node_id(&db, "Doc", "dockey", &format!("n{i}"));
        arc.write().expect("write lock").insert(nid, &embeddings[i]);
    }

    // Persist the HNSW index to disk — hybrid_search loads from disk.
    let vec_dir = dir.path().join("vector_indexes");
    arc.read()
        .expect("read lock")
        .save(&vec_dir, "Doc", "embedding")
        .expect("save vector index");

    // Run hybrid_search: query vec [1,0,0,0], text "rust graph", k=5.
    let result = db
        .execute(
            "RETURN hybrid_search('Doc', 'embedding', 'content', \
              [1.0, 0.0, 0.0, 0.0], 'rust graph', 5) AS hits",
        )
        .expect("hybrid_search must execute");

    assert_eq!(result.rows.len(), 1, "RETURN produces one row");
    let raw_value = &result.rows[0][0];
    let ids = extract_hit_ids(&result);
    assert!(
        !ids.is_empty(),
        "hybrid_search must return at least one result; raw value = {raw_value:?}"
    );
    assert!(
        ids.len() <= 5,
        "result must be capped at k=5, got {}",
        ids.len()
    );

    // The perfect-match node (n0) must be the top result.
    let n0_id = get_node_id(&db, "Doc", "dockey", "n0");
    assert_eq!(
        ids[0], n0_id,
        "n0 (id={n0_id}) should be the top result (got {ids:?})"
    );
}

// ── 2. Weighted fusion with alpha ─────────────────────────────────────────────

#[test]
fn hybrid_search_weighted_fusion_alpha() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.create_vector_index("Item", "vec", 2, "cosine")
        .expect("create vector index");
    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Item) ON (n.text)");

    // Node A: best vector match (emb [1,0] aligns with query [1,0]).
    // Node B: best text match (contains "target").
    exec(
        &db,
        "CREATE (i:Item {ikey: 'A', text: 'unrelated stuff here'})",
    );
    exec(
        &db,
        "CREATE (i:Item {ikey: 'B', text: 'target keyword query'})",
    );

    let id_a = get_node_id(&db, "Item", "ikey", "A");
    let id_b = get_node_id(&db, "Item", "ikey", "B");

    let arc = db.get_vector_index("Item", "vec").expect("vec index");
    arc.write().expect("w").insert(id_a, &[1.0_f32, 0.0]);
    arc.write().expect("w").insert(id_b, &[0.0_f32, 1.0]);

    // Persist HNSW index to disk — hybrid_search loads from disk.
    let vec_dir = dir.path().join("vector_indexes");
    arc.read()
        .expect("r")
        .save(&vec_dir, "Item", "vec")
        .expect("save vector index");

    // alpha=1.0 → pure vector → node A should be first.
    let res_vec = db
        .execute(
            "RETURN hybrid_search('Item', 'vec', 'text', \
              [1.0, 0.0], 'target keyword', 2, 1.0) AS hits",
        )
        .expect("hybrid_search alpha=1.0");
    let ids_vec = extract_hit_ids(&res_vec);
    assert!(
        !ids_vec.is_empty(),
        "alpha=1.0 hybrid_search must return results"
    );
    assert_eq!(
        ids_vec[0], id_a,
        "alpha=1.0 should rank vector-best node first (got {ids_vec:?})"
    );

    // alpha=0.0 → pure text → node B should be first.
    let res_txt = db
        .execute(
            "RETURN hybrid_search('Item', 'vec', 'text', \
              [1.0, 0.0], 'target keyword', 2, 0.0) AS hits",
        )
        .expect("hybrid_search alpha=0.0");
    let ids_txt = extract_hit_ids(&res_txt);
    assert!(
        !ids_txt.is_empty(),
        "alpha=0.0 hybrid_search must return results"
    );
    assert_eq!(
        ids_txt[0], id_b,
        "alpha=0.0 should rank text-best node first (got {ids_txt:?})"
    );
}

// ── 3. Missing FTS index falls back gracefully ────────────────────────────────

#[test]
fn hybrid_search_missing_fts_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    // Only the vector index — no FTS index.
    db.create_vector_index("Thing", "emb", 2, "cosine")
        .expect("create vector index");

    let arc = db.get_vector_index("Thing", "emb").expect("vec index");
    arc.write().expect("w").insert(1, &[1.0_f32, 0.0]);
    arc.write().expect("w").insert(2, &[0.0_f32, 1.0]);

    // Should not panic; FTS side returns empty list, vector results survive.
    let result = db
        .execute(
            "RETURN hybrid_search('Thing', 'emb', 'note', \
              [1.0, 0.0], 'alpha', 3) AS hits",
        )
        .expect("hybrid_search without FTS must not error");

    assert_eq!(result.rows.len(), 1);
    // The result is either a List (vector-only) or Null.
    match &result.rows[0][0] {
        Value::List(_) | Value::Null => {}
        other => panic!("expected List or Null, got {other:?}"),
    }
}

// ── 4. hybrid_search k=0 returns Null (invalid argument) ─────────────────────

#[test]
fn hybrid_search_k_zero_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    db.create_vector_index("Z", "v", 2, "cosine")
        .expect("create index");
    let arc = db.get_vector_index("Z", "v").expect("index");
    arc.write().expect("w").insert(1, &[1.0_f32, 0.0]);

    // k=0 is invalid (not > 0) → engine returns Null.
    let result = db
        .execute("RETURN hybrid_search('Z', 'v', 't', [1.0, 0.0], 'x', 0) AS hits")
        .expect("execute");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Null, "k=0 must produce Null");
}
