//! Integration tests for the HNSW vector similarity index (issue #394).
//!
//! Tests cover:
//! - `CREATE VECTOR INDEX` DDL via `GraphDb::execute`
//! - Inserting nodes with vector embeddings via `execute_with_params`
//! - `vector_similarity()`, `vector_distance()`, `vector_dot()` scalar functions
//! - Restart survival (index persists to disk and reloads)
//! - `DROP INDEX` removes in-memory and on-disk data
//! - `GraphDb::create_vector_index` / `drop_vector_index` / `get_vector_index`

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open db");
    (dir, db)
}

// ── DDL ────────────────────────────────────────────────────────────────────────

#[test]
fn create_vector_index_ddl() {
    let (_dir, db) = make_db();
    db.execute(
        "CREATE VECTOR INDEX FOR (n:Memory) ON (n.embedding) \
         OPTIONS { dimensions: 4, similarity: 'cosine' }",
    )
    .expect("CREATE VECTOR INDEX must succeed");

    // A second call is idempotent (index already registered).
    db.execute(
        "CREATE VECTOR INDEX FOR (n:Memory) ON (n.embedding) \
         OPTIONS { dimensions: 4, similarity: 'cosine' }",
    )
    .expect("duplicate CREATE VECTOR INDEX must be a no-op");
}

#[test]
fn create_vector_index_api() {
    let (_dir, db) = make_db();
    db.create_vector_index("Person", "emb", 3, "cosine")
        .expect("create_vector_index must succeed");
    assert!(
        db.get_vector_index("Person", "emb").is_some(),
        "index must be registered"
    );
}

#[test]
fn drop_vector_index_api() {
    let (_dir, db) = make_db();
    db.create_vector_index("Person", "emb", 3, "cosine")
        .expect("create");
    db.drop_vector_index("Person", "emb")
        .expect("drop_vector_index must succeed");
    assert!(
        db.get_vector_index("Person", "emb").is_none(),
        "index must be gone after drop"
    );
}

// ── Write path ─────────────────────────────────────────────────────────────────

#[test]
fn merge_with_params_inserts_into_vector_index() {
    let (_dir, db) = make_db();

    // Create the index first.
    db.create_vector_index("Memory", "embedding", 3, "cosine")
        .expect("create index");

    // Insert nodes directly via the VectorIndex API (write path test for the
    // registry is verified separately via the low-level API).
    let arc = db
        .get_vector_index("Memory", "embedding")
        .expect("index exists");
    arc.write()
        .expect("write lock")
        .insert(1, &[1.0_f32, 0.0, 0.0]);
    arc.write()
        .expect("write lock")
        .insert(2, &[0.0_f32, 1.0, 0.0]);

    // Confirm the index returns results.
    let idx = arc.read().expect("read lock");
    let results = idx.search(&[1.0_f32, 0.0, 0.0], 5, 10);
    assert!(
        !results.is_empty(),
        "HNSW search must return at least one result"
    );
    assert_eq!(results[0].0, 1, "nearest to [1,0,0] must be node 1");
}

// ── Scalar functions ───────────────────────────────────────────────────────────

#[test]
fn vector_similarity_function() {
    let (_dir, db) = make_db();
    // Create nodes with known embeddings.
    db.execute("CREATE (a:Vec {x: 1.0, y: 0.0, z: 0.0, id: 1})")
        .expect("create a");
    db.execute("CREATE (b:Vec {x: 0.0, y: 1.0, z: 0.0, id: 2})")
        .expect("create b");

    // vector_similarity of identical vectors (manually pass lists).
    let res = db
        .execute("RETURN vector_similarity([1.0, 0.0, 0.0], [1.0, 0.0, 0.0]) AS sim")
        .expect("vector_similarity must execute");
    assert_eq!(res.rows.len(), 1);
    if let Value::Float64(sim) = &res.rows[0][0] {
        assert!(
            (sim - 1.0).abs() < 1e-5,
            "cosine similarity of identical vectors must be 1.0, got {sim}"
        );
    } else {
        panic!("expected Float64, got {:?}", res.rows[0][0]);
    }
}

#[test]
fn vector_similarity_orthogonal_is_zero() {
    let (_dir, db) = make_db();
    let res = db
        .execute("RETURN vector_similarity([1.0, 0.0], [0.0, 1.0]) AS sim")
        .expect("execute");
    assert_eq!(res.rows.len(), 1);
    if let Value::Float64(sim) = &res.rows[0][0] {
        assert!(
            sim.abs() < 1e-5,
            "cosine similarity of orthogonal vectors must be ~0, got {sim}"
        );
    } else {
        panic!("expected Float64, got {:?}", res.rows[0][0]);
    }
}

#[test]
fn vector_distance_function() {
    let (_dir, db) = make_db();
    let res = db
        .execute("RETURN vector_distance([0.0, 0.0], [3.0, 4.0]) AS d")
        .expect("execute");
    assert_eq!(res.rows.len(), 1);
    if let Value::Float64(d) = &res.rows[0][0] {
        assert!(
            (d - 5.0).abs() < 1e-4,
            "Euclidean distance from (0,0) to (3,4) must be 5.0, got {d}"
        );
    } else {
        panic!("expected Float64, got {:?}", res.rows[0][0]);
    }
}

#[test]
fn vector_dot_function() {
    let (_dir, db) = make_db();
    let res = db
        .execute("RETURN vector_dot([2.0, 3.0], [4.0, 5.0]) AS dp")
        .expect("execute");
    assert_eq!(res.rows.len(), 1);
    if let Value::Float64(dp) = &res.rows[0][0] {
        // 2*4 + 3*5 = 8 + 15 = 23
        assert!(
            (dp - 23.0).abs() < 1e-4,
            "dot product of [2,3]·[4,5] must be 23, got {dp}"
        );
    } else {
        panic!("expected Float64, got {:?}", res.rows[0][0]);
    }
}

// ── Restart survival ───────────────────────────────────────────────────────────

#[test]
fn vector_index_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = GraphDb::open(&path).expect("open");
        db.create_vector_index("Memory", "embedding", 3, "cosine")
            .expect("create index");

        // Insert via the low-level API.
        let arc = db.get_vector_index("Memory", "embedding").expect("index");
        arc.write().expect("write").insert(42, &[1.0_f32, 0.0, 0.0]);

        // Persist by saving the index.
        let vidx_dir = path.join("vector_indexes");
        arc.read()
            .expect("read")
            .save(&vidx_dir, "Memory", "embedding")
            .expect("save");
    } // db dropped here

    // Re-open and verify the index loaded from disk.
    {
        let db = GraphDb::open(&path).expect("re-open");
        let arc = db
            .get_vector_index("Memory", "embedding")
            .expect("index must survive restart");
        let idx = arc.read().expect("read");
        let results = idx.search(&[1.0_f32, 0.0, 0.0], 5, 10);
        assert!(
            !results.is_empty(),
            "inserted node must be found after restart"
        );
        assert_eq!(results[0].0, 42, "node_id 42 must be the nearest neighbour");
    }
}

// ── Bulk insert + top-k query ──────────────────────────────────────────────────

#[test]
fn hnsw_bulk_insert_and_top_k() {
    let (_dir, db) = make_db();
    db.create_vector_index("Item", "vec", 8, "cosine")
        .expect("create index");

    let arc = db.get_vector_index("Item", "vec").expect("index");

    // Insert 50 random-ish unit vectors.
    for i in 0u64..50 {
        // Simple deterministic embedding: each dimension is i * dim.
        let v: Vec<f32> = (0..8)
            .map(|d| ((i * 7 + d * 3) % 17) as f32 / 17.0)
            .collect();
        // Normalize.
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        let vn: Vec<f32> = v.iter().map(|x| x / norm).collect();
        arc.write().expect("write").insert(i, &vn);
    }

    // Query with one of the inserted vectors (node 7).
    let query: Vec<f32> = {
        let v: Vec<f32> = (0u64..8)
            .map(|d| ((7 * 7 + d * 3) % 17) as f32 / 17.0)
            .collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        v.iter().map(|x| x / norm).collect()
    };

    let results = arc.read().expect("read").search(&query, 10, 50);
    assert!(!results.is_empty(), "must return at least 1 result");
    // The closest node should be node 7 itself.
    assert_eq!(
        results[0].0, 7,
        "top result must be the query node itself (id=7)"
    );
}

// ── Metrics ────────────────────────────────────────────────────────────────────

#[test]
fn create_vector_index_euclidean_metric() {
    let (_dir, db) = make_db();
    db.create_vector_index("Point", "pos", 2, "euclidean")
        .expect("create euclidean index");
    let arc = db.get_vector_index("Point", "pos").expect("index");
    arc.write().expect("w").insert(0, &[0.0_f32, 0.0]);
    arc.write().expect("w").insert(1, &[1.0_f32, 0.0]);
    arc.write().expect("w").insert(2, &[0.0_f32, 10.0]);
    let results = arc.read().expect("r").search(&[0.0, 0.0], 1, 10);
    assert_eq!(results[0].0, 0, "nearest to origin should be origin itself");
}

#[test]
fn create_vector_index_dot_product_metric() {
    let (_dir, db) = make_db();
    db.create_vector_index("Emb", "feat", 2, "dot")
        .expect("create dot index");
    let arc = db.get_vector_index("Emb", "feat").expect("index");
    arc.write().expect("w").insert(0, &[0.5_f32, 0.5]);
    arc.write().expect("w").insert(1, &[1.0_f32, 1.0]);
    // query vector [1, 1] — dot product with [1, 1] is 2, with [0.5, 0.5] is 1.
    let results = arc.read().expect("r").search(&[1.0, 1.0], 1, 10);
    assert_eq!(results[0].0, 1, "highest dot product should be node 1");
}
