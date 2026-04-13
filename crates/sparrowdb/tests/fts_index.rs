//! Integration tests for BM25 full-text index (issue #395).
//!
//! Tests cover:
//!  1. `CREATE FULLTEXT INDEX` DDL
//!  2. Auto-indexing on `CREATE` for nodes whose label/property is registered
//!  3. `full_text_search(label, property, query)` WHERE predicate
//!  4. `bm25_score(n.prop, query)` AS score expression + ORDER BY
//!  5. Multi-word queries (union of term scores)
//!  6. Restart survival (index is persisted to disk)
//!  7. Score ordering (nodes with more matching terms rank higher)

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

fn open_db(dir: &std::path::Path) -> GraphDb {
    GraphDb::open(dir).expect("open db")
}

// ── Helper: run a Cypher statement, panic on error ────────────────────────────

fn exec(db: &GraphDb, cypher: &str) {
    db.execute(cypher)
        .unwrap_or_else(|e| panic!("exec failed for `{cypher}`: {e}"));
}

// ── 1. CREATE FULLTEXT INDEX DDL ──────────────────────────────────────────────

#[test]
fn test_create_fulltext_index_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());
    exec(
        &db,
        "CREATE FULLTEXT INDEX memory_content FOR (n:Memory) ON (n.content)",
    );
    // A second call should be idempotent (no error).
    exec(
        &db,
        "CREATE FULLTEXT INDEX memory_content FOR (n:Memory) ON (n.content)",
    );
}

// ── 2. Auto-indexing on CREATE ────────────────────────────────────────────────

#[test]
fn test_auto_index_on_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Memory) ON (n.content)");
    exec(
        &db,
        "CREATE (m:Memory {content: 'transformer attention mechanism'})",
    );

    // Should be findable via full_text_search.
    let result = db
        .execute(
            "MATCH (n:Memory) WHERE full_text_search('Memory', 'content', 'transformer') RETURN n.content",
        )
        .expect("query failed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row, got {}: {:?}",
        result.rows.len(),
        result.rows
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("transformer attention mechanism".into())
    );
}

// ── 3. full_text_search predicate ─────────────────────────────────────────────

#[test]
fn test_full_text_search_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Article) ON (n.body)");
    exec(
        &db,
        "CREATE (a:Article {body: 'rust programming language safety'})",
    );
    exec(
        &db,
        "CREATE (a:Article {body: 'python machine learning frameworks'})",
    );
    exec(&db, "CREATE (a:Article {body: 'rust async tokio runtime'})");
    exec(
        &db,
        "CREATE (a:Article {body: 'java spring boot microservices'})",
    );

    // Query for 'rust' — should match 2 articles.
    let result = db
        .execute(
            "MATCH (a:Article) WHERE full_text_search('Article', 'body', 'rust') RETURN a.body",
        )
        .expect("query failed");

    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 rows for 'rust', got {}: {:?}",
        result.rows.len(),
        result.rows
    );

    // Query for 'python' — should match 1.
    let result2 = db
        .execute(
            "MATCH (a:Article) WHERE full_text_search('Article', 'body', 'python') RETURN a.body",
        )
        .expect("query failed");

    assert_eq!(result2.rows.len(), 1);

    // Query for 'nonexistent' — should return 0.
    let result3 = db
        .execute(
            "MATCH (a:Article) WHERE full_text_search('Article', 'body', 'nonexistent') RETURN a.body",
        )
        .expect("query failed");

    assert_eq!(result3.rows.len(), 0);
}

// ── 4. bm25_score expression + ORDER BY ───────────────────────────────────────

#[test]
fn test_bm25_score_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Memory) ON (n.content)");

    // Insert nodes with different relevance to 'attention transformer'.
    exec(
        &db,
        "CREATE (m:Memory {content: 'attention is all you need transformer model'})",
    );
    exec(
        &db,
        "CREATE (m:Memory {content: 'convolutional neural network image classification'})",
    );
    exec(
        &db,
        "CREATE (m:Memory {content: 'transformer encoder decoder attention mechanism attention'})",
    );
    exec(
        &db,
        "CREATE (m:Memory {content: 'recurrent neural network language model'})",
    );

    let result = db
        .execute(
            "MATCH (n:Memory) \
             WHERE full_text_search('Memory', 'content', 'transformer attention') \
             RETURN n.content, bm25_score(n.content, 'transformer attention') AS score \
             ORDER BY score DESC LIMIT 20",
        )
        .expect("query failed");

    // We should get the two documents that mention 'transformer' or 'attention'.
    assert!(
        result.rows.len() >= 2,
        "expected at least 2 rows, got {}",
        result.rows.len()
    );

    // Scores should be in descending order.
    let scores: Vec<f64> = result
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::Float64(f) => *f,
            _ => 0.0,
        })
        .collect();

    for window in scores.windows(2) {
        assert!(
            window[0] >= window[1],
            "rows should be ordered by descending score: {} < {}",
            window[0],
            window[1]
        );
    }
}

// ── 5. Multi-word query (union of term scores) ────────────────────────────────

#[test]
fn test_multiword_query_union_scoring() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Doc) ON (n.text)");
    exec(&db, "CREATE (d:Doc {text: 'alpha beta gamma'})");
    exec(&db, "CREATE (d:Doc {text: 'delta epsilon'})");
    exec(&db, "CREATE (d:Doc {text: 'alpha delta zeta'})");

    // Query for 'alpha beta' — doc 1 matches both, doc 3 matches 'alpha' only.
    let result = db
        .execute("MATCH (d:Doc) WHERE full_text_search('Doc', 'text', 'alpha beta') RETURN d.text")
        .expect("query failed");

    // Documents 1 and 3 contain 'alpha'; document 1 also contains 'beta'.
    assert!(
        result.rows.len() >= 2,
        "expected at least 2 results for 'alpha beta', got {}",
        result.rows.len()
    );
}

// ── 6. Restart survival (index persisted to disk) ────────────────────────────

#[test]
fn test_fts_index_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = open_db(dir.path());
        exec(&db, "CREATE FULLTEXT INDEX FOR (n:Note) ON (n.text)");
        exec(
            &db,
            "CREATE (n:Note {text: 'persistent full text indexing rocks'})",
        );
        exec(
            &db,
            "CREATE (n:Note {text: 'unrelated content about databases'})",
        );
        // db drops here, flushing to disk
    }

    // Reopen without re-creating the index.
    let db2 = open_db(dir.path());

    let result = db2
        .execute(
            "MATCH (n:Note) WHERE full_text_search('Note', 'text', 'persistent') RETURN n.text",
        )
        .expect("query after restart failed");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row after restart, got {}",
        result.rows.len()
    );
    assert_eq!(
        result.rows[0][0],
        Value::String("persistent full text indexing rocks".into())
    );
}

// ── 7. BM25 ranking with 50 nodes ────────────────────────────────────────────

#[test]
fn test_bm25_ranking_50_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Knowledge) ON (n.fact)");

    // Create 50 nodes. Every 5th mentions 'quantum', every 10th also 'entanglement'.
    for i in 0..50u32 {
        let text = if i % 10 == 0 {
            format!("quantum entanglement physics phenomenon node{i}")
        } else if i % 5 == 0 {
            format!("quantum computing superposition node{i}")
        } else {
            format!("classical physics determinism node{i}")
        };
        db.execute(&format!("CREATE (k:Knowledge {{fact: '{text}'}})"))
            .expect("create node failed");
    }

    // Query for 'quantum entanglement'.
    let result = db
        .execute(
            "MATCH (k:Knowledge) \
             WHERE full_text_search('Knowledge', 'fact', 'quantum entanglement') \
             RETURN k.fact, bm25_score(k.fact, 'quantum entanglement') AS score \
             ORDER BY score DESC LIMIT 10",
        )
        .expect("query failed");

    assert!(
        !result.rows.is_empty(),
        "expected at least one result for 'quantum entanglement'"
    );

    // Verify descending score order.
    let scores: Vec<f64> = result
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::Float64(f) => *f,
            _ => 0.0,
        })
        .collect();

    for window in scores.windows(2) {
        assert!(
            window[0] >= window[1],
            "scores not in descending order: {} < {}",
            window[0],
            window[1]
        );
    }

    // Top result should contain both 'quantum' and 'entanglement'.
    let top_fact = match &result.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected string for fact, got: {other:?}"),
    };
    assert!(
        top_fact.contains("quantum") && top_fact.contains("entanglement"),
        "top result should mention both terms: {top_fact}"
    );
}

// ── 8. Direct index file access ───────────────────────────────────────────────
#[test]
fn test_fts_index_file_exists_and_is_searchable() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    exec(&db, "CREATE FULLTEXT INDEX FOR (n:Memory) ON (n.content)");
    exec(
        &db,
        "CREATE (m:Memory {content: 'transformer attention mechanism'})",
    );

    // Check index file exists
    let idx_path = dir.path().join("fts").join("Memory__content.bin");
    assert!(
        idx_path.exists(),
        "index file should exist at {:?}",
        idx_path
    );

    // Load the index and verify it has the document
    let idx = sparrowdb_storage::fts_index::FtsIndex::open(dir.path(), "Memory", "content")
        .expect("open index");
    let results = idx.search("transformer", usize::MAX);
    assert!(
        !results.is_empty(),
        "FTS index should contain 'transformer'; search returned empty"
    );
}
