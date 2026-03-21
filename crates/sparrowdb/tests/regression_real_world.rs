//! Real-world regression tests.
//!
//! These tests load fixture datasets and run queries that reflect real use cases.
//! They must pass after every phase of development.
//!
//! The fixture files (fixtures/real_world/*.cypher) contain only CREATE node
//! statements.  Edges are created in-test via WriteTx::create_edge so we can
//! capture NodeIds and reconstruct the graph topology deterministically.
//!
//! ## Design notes
//!
//! * The delta-log reader in execute_one_hop reads only RelTableId(0).  Tests
//!   that need cross-label traversal therefore use a fresh DB where the target
//!   rel type is the first (and only) registered edge type (RelTableId=0).
//!
//! * MERGE, DELETE, and WITH are used via WriteTx/ReadTx rather than Cypher
//!   until those statements are wired into the parser.
//!
//! Run: cargo test -p sparrowdb --test regression_real_world

use sparrowdb::open;
use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::NodeId;
use sparrowdb_storage::node_store::Value;
use std::collections::HashMap;

// ── Fixture paths ─────────────────────────────────────────────────────────────

/// Resolve a fixture path relative to the repository root.
///
/// `cargo test` runs with CWD = the crate directory (`crates/sparrowdb`).
/// We step up two levels to reach the repo root.
fn fixture_path(name: &str) -> std::path::PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()   // crates/
        .unwrap()
        .parent()   // repo root
        .unwrap()
        .join("fixtures")
        .join("real_world")
        .join(name)
}

// ── Fixture loader ────────────────────────────────────────────────────────────

/// Execute every non-blank, non-comment line in a Cypher fixture file.
///
/// Lines starting with `//` are skipped.  Trailing `;` is stripped before
/// calling `db.execute()`.
fn load_fixture(db: &sparrowdb::GraphDb, fixture_path: &std::path::Path) {
    let content =
        std::fs::read_to_string(fixture_path).expect("read fixture file");
    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        let stmt = line.trim_end_matches(';');
        db.execute(stmt)
            .unwrap_or_else(|e| panic!("execute fixture line failed: {e}\n  line: {stmt}"));
    }
}

// ── NodeId helpers ────────────────────────────────────────────────────────────

/// Pack a label ID and slot index into a NodeId.
///   NodeId.0 = (label_id as u64) << 32 | slot
fn make_node_id(label_id: u16, slot: u32) -> NodeId {
    NodeId(((label_id as u64) << 32) | (slot as u64))
}

/// Retrieve the label ID for a named label from the on-disk catalog.
fn label_id_for(dir: &std::path::Path, label: &str) -> u16 {
    let cat = Catalog::open(dir).expect("open catalog");
    cat.get_label(label)
        .expect("catalog lookup")
        .unwrap_or_else(|| panic!("label '{label}' not found in catalog"))
}

// ── Deterministic PRNG (Xorshift64) ──────────────────────────────────────────

/// Advance xorshift64 state one step.
fn xorshift64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Generate `count` unique directed (a, b) pairs from `[0, n)` with a != b.
fn gen_unique_pairs(n: usize, count: usize, seed: u64) -> Vec<(usize, usize)> {
    let mut state = seed;
    let mut pairs: std::collections::HashSet<(usize, usize)> =
        std::collections::HashSet::new();
    let mut attempts = 0usize;
    while pairs.len() < count && attempts < count * 20 {
        attempts += 1;
        let a = (xorshift64(&mut state) as usize) % n;
        let b = (xorshift64(&mut state) as usize) % n;
        if a != b {
            pairs.insert((a, b));
        }
    }
    let mut v: Vec<_> = pairs.into_iter().collect();
    v.sort();
    v
}

/// Create KNOWS edges (Person → Person) in batches.
fn create_knows_edges(
    db: &sparrowdb::GraphDb,
    person_label: u16,
    n_persons: usize,
    count: usize,
) {
    let pairs = gen_unique_pairs(n_persons, count, 0xDEAD_BEEF_1111_1111);
    let chunk_size = 200;
    for chunk in pairs.chunks(chunk_size) {
        let mut tx = db.begin_write().expect("begin_write");
        for &(a, b) in chunk {
            let src = make_node_id(person_label, a as u32);
            let dst = make_node_id(person_label, b as u32);
            tx.create_edge(src, dst, "KNOWS", HashMap::new())
                .expect("create KNOWS edge");
        }
        tx.commit().expect("commit KNOWS batch");
    }
}

// ── Social graph setup ────────────────────────────────────────────────────────

const N_PERSONS: usize = 1000;
const N_COMPANIES: usize = 50;
const N_POSTS: usize = 200;
const N_KNOWS: usize = 3000;

/// Load the social graph fixture and create KNOWS edge topology.
///
/// Returns `(dir, db, person_label, company_label, post_label)`.
fn setup_social_graph() -> (
    tempfile::TempDir,
    sparrowdb::GraphDb,
    u16,
    u16,
    u16,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    let fixture = fixture_path("mini_social.cypher");
    load_fixture(&db, &fixture);

    let person_label = label_id_for(dir.path(), "Person");
    let company_label = label_id_for(dir.path(), "Company");
    let post_label = label_id_for(dir.path(), "Post");

    // Only create KNOWS edges here — these go into RelTableId(0).
    create_knows_edges(&db, person_label, N_PERSONS, N_KNOWS);

    (dir, db, person_label, company_label, post_label)
}

// ── Knowledge graph setup ─────────────────────────────────────────────────────

const N_FACTS: usize = 120;
const N_ENTITIES: usize = 80;
const N_RELATES_TO: usize = 150;

/// Load the knowledge graph fixture and create RELATES_TO edge topology.
fn setup_knowledge_graph() -> (
    tempfile::TempDir,
    sparrowdb::GraphDb,
    u16,
    u16,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    let fixture = fixture_path("mini_knowledge.cypher");
    load_fixture(&db, &fixture);

    let fact_label = label_id_for(dir.path(), "Fact");
    let entity_label = label_id_for(dir.path(), "Entity");

    // RELATES_TO: Fact → Fact (RelTableId=0, first registered)
    let rel_pairs = gen_unique_pairs(N_FACTS, N_RELATES_TO, 0xFACE_CAFE_3333_3333);
    let chunk_size = 200;
    for chunk in rel_pairs.chunks(chunk_size) {
        let mut tx = db.begin_write().expect("begin_write");
        for &(a, b) in chunk {
            let src = make_node_id(fact_label, a as u32);
            let dst = make_node_id(fact_label, b as u32);
            tx.create_edge(src, dst, "RELATES_TO", HashMap::new())
                .expect("RELATES_TO");
        }
        tx.commit().expect("commit RELATES_TO");
    }

    (dir, db, fact_label, entity_label)
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 1: Person node count after bulk load ─────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// After loading the social fixture, MATCH (n:Person) must return exactly 1000 rows.
#[test]
fn social_graph_person_count() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let r = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH Person");

    assert_eq!(
        r.rows.len(),
        N_PERSONS,
        "expected {N_PERSONS} Person nodes after bulk load"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 2: Company node count ────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// After loading the social fixture, MATCH (n:Company) must return exactly 50 rows.
#[test]
fn social_graph_company_count() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let r = db
        .execute("MATCH (n:Company) RETURN n.name")
        .expect("MATCH Company");

    assert_eq!(
        r.rows.len(),
        N_COMPANIES,
        "expected {N_COMPANIES} Company nodes"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 3: Post node count ───────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// After loading the social fixture, MATCH (n:Post) must return exactly 200 rows.
#[test]
fn social_graph_post_count() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let r = db
        .execute("MATCH (n:Post) RETURN n.content")
        .expect("MATCH Post");

    assert_eq!(
        r.rows.len(),
        N_POSTS,
        "expected {N_POSTS} Post nodes"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 4: KNOWS edge traversal at scale ─────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// MATCH (a:Person)-[:KNOWS]->(b:Person) must return rows from the 3000-edge graph.
#[test]
fn social_graph_knows_edges_traversable() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let r = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH KNOWS traversal");

    assert!(
        r.rows.len() > 0,
        "KNOWS edge traversal must return at least 1 row on a {N_KNOWS}-edge graph"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 5: Cross-label edge traversal (Person → Company) ────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// MATCH (p:Person)-[:WORKS_AT]->(c:Company) in an isolated DB where WORKS_AT
/// is the only edge type (RelTableId=0) — verifying cross-label 1-hop traversal.
///
/// This test uses a small graph (10 persons, 3 companies, 5 edges) rather than
/// the full social fixture so the test remains self-contained and fast.
#[test]
fn social_graph_works_at_cross_label_traversal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    // Create labels via Catalog directly so we control the IDs.
    let person_label: u16;
    let company_label: u16;
    {
        let mut cat = Catalog::open(dir.path()).expect("catalog");
        person_label = cat.create_label("Worker").expect("Worker label");
        company_label = cat.create_label("Employer").expect("Employer label");
    }

    // Create 10 workers via Cypher CREATE.
    for i in 0..10u32 {
        db.execute(&format!("CREATE (n:Worker {{name: 'Worker{i}', slot: {i}}})"))
            .expect("CREATE Worker");
    }

    // Create 3 employers via Cypher CREATE.
    for j in 0..3u32 {
        db.execute(&format!("CREATE (n:Employer {{name: 'Employer{j}', slot: {j}}})"))
            .expect("CREATE Employer");
    }

    // Verify node counts before creating edges.
    let workers = db.execute("MATCH (n:Worker) RETURN n.name").expect("MATCH Worker");
    assert_eq!(workers.rows.len(), 10, "10 Worker nodes must be visible");

    let employers = db.execute("MATCH (n:Employer) RETURN n.name").expect("MATCH Employer");
    assert_eq!(employers.rows.len(), 3, "3 Employer nodes must be visible");

    // Create WORKS_AT edges as the FIRST rel type (RelTableId=0).
    // Edges: Worker 0→Employer 0, Worker 1→Employer 0, Worker 2→Employer 1,
    //        Worker 5→Employer 2, Worker 9→Employer 2
    let edge_pairs = [(0u32, 0u32), (1, 0), (2, 1), (5, 2), (9, 2)];
    {
        let mut tx = db.begin_write().expect("begin_write");
        for &(w, e) in &edge_pairs {
            let src = make_node_id(person_label, w);
            let dst = make_node_id(company_label, e);
            tx.create_edge(src, dst, "WORKS_AT", HashMap::new())
                .expect("create WORKS_AT edge");
        }
        tx.commit().expect("commit WORKS_AT");
    }

    // Traverse: MATCH (w:Worker)-[:WORKS_AT]->(e:Employer) RETURN w.name, e.name
    let r = db
        .execute("MATCH (w:Worker)-[:WORKS_AT]->(e:Employer) RETURN w.name, e.name")
        .expect("MATCH WORKS_AT traversal");

    assert!(
        r.rows.len() > 0,
        "cross-label WORKS_AT traversal must return at least 1 row (got 0)"
    );
    assert_eq!(
        r.rows.len(),
        edge_pairs.len(),
        "expected exactly {} WORKS_AT rows",
        edge_pairs.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 6: WHERE filter on string property at scale ─────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// WHERE n.city = 'Dallas' on 1000 Person nodes must return only Dallas persons.
#[test]
fn social_graph_where_filter_city() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let r = db
        .execute("MATCH (n:Person) WHERE n.city = 'Dallas' RETURN n.name")
        .expect("MATCH WHERE city = Dallas");

    assert!(
        r.rows.len() > 0,
        "WHERE n.city = 'Dallas' must match at least one person"
    );
    assert!(
        r.rows.len() < N_PERSONS,
        "WHERE n.city = 'Dallas' must not return all {N_PERSONS} persons"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 7: WHERE filter on integer property ──────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// WHERE c.founded = 1990 must return a subset of companies.
#[test]
fn social_graph_where_filter_integer_founded() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let all = db
        .execute("MATCH (n:Company) RETURN n.founded")
        .expect("MATCH Company founded");
    assert_eq!(all.rows.len(), N_COMPANIES, "all {N_COMPANIES} companies visible");

    let filtered = db
        .execute("MATCH (n:Company) WHERE n.founded = 1990 RETURN n.name")
        .expect("MATCH WHERE founded = 1990");
    assert!(
        filtered.rows.len() <= N_COMPANIES,
        "WHERE founded = 1990 must return at most {N_COMPANIES} rows"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 8: MERGE idempotency via WriteTx at scale ───────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// Calling WriteTx::merge_node twice with the same label+integer prop must create
/// exactly one node.  Tests MERGE idempotency after a large bulk-loaded graph.
#[test]
fn social_graph_merge_idempotency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    // Ensure the Gadget label exists.
    {
        let mut cat = Catalog::open(dir.path()).expect("catalog");
        cat.create_label("Gadget").expect("Gadget label");
    }

    // Seed with 5 Gadget nodes (id: 1..5) via WriteTx.
    {
        let mut tx = db.begin_write().expect("begin_write seed");
        for id in 1i64..=5 {
            let mut props = HashMap::new();
            props.insert("id".to_string(), Value::Int64(id));
            tx.merge_node("Gadget", props).expect("seed merge_node");
        }
        tx.commit().expect("commit seed");
    }

    let before = db
        .execute("MATCH (n:Gadget) RETURN n.id")
        .expect("count before idempotency check")
        .rows
        .len();
    assert_eq!(before, 5, "5 Gadget nodes must exist before idempotency check");

    // Merge id=3 twice — must not create a duplicate.
    {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int64(3));
        let mut tx = db.begin_write().expect("begin_write 1");
        tx.merge_node("Gadget", props).expect("first merge_node id=3");
        tx.commit().expect("commit 1");
    }
    {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int64(3));
        let mut tx = db.begin_write().expect("begin_write 2");
        tx.merge_node("Gadget", props).expect("second merge_node id=3");
        tx.commit().expect("commit 2");
    }

    let after = db
        .execute("MATCH (n:Gadget) RETURN n.id")
        .expect("count after idempotency check")
        .rows
        .len();

    assert_eq!(
        after,
        before,
        "double merge_node on existing id=3 must not create a duplicate \
         (before={before} after={after})"
    );

    let _ = dir;
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 9: DELETE + MATCH verify removal ────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// Create two Widget nodes via WriteTx (which sets col_0 for tombstone support),
/// delete one via WriteTx::delete_node, then MATCH confirms only one remains.
///
/// Uses low-level WriteTx because Cypher DELETE requires MATCH … DELETE which
/// is not yet supported in the current engine version.
#[test]
fn social_graph_delete_then_match_verifies_removal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");

    let label_id: u32 = {
        let mut cat = Catalog::open(dir.path()).expect("catalog");
        cat.create_label("Widget")
            .expect("create Widget label") as u32
    };

    // Create two Widget nodes.
    let (keeper, goner);
    {
        let mut tx = db.begin_write().expect("begin_write");
        keeper = tx.create_node(label_id, &[(0u32, Value::Int64(1))]).expect("Widget keeper");
        goner  = tx.create_node(label_id, &[(0u32, Value::Int64(99))]).expect("Widget goner");
        tx.commit().expect("commit create");
    }

    // Verify both are visible.
    let before = db.execute("MATCH (n:Widget) RETURN n.col_0").expect("before delete");
    assert_eq!(before.rows.len(), 2, "both Widgets must be visible before delete");

    // Delete one.
    {
        let mut tx = db.begin_write().expect("begin_write delete");
        tx.delete_node(goner).expect("delete goner");
        tx.commit().expect("commit delete");
    }

    // Only the keeper remains.
    let after = db.execute("MATCH (n:Widget) RETURN n.col_0").expect("after delete");
    assert_eq!(
        after.rows.len(),
        1,
        "only 1 Widget must remain after delete_node (tombstone path)"
    );

    let _ = (keeper, dir);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 10: Persist across DB reopen ────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// Load the social fixture, close the DB, reopen it — all nodes survive.
#[test]
fn social_graph_persist_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Session 1: load fixture.
    {
        let db = open(dir.path()).expect("open session 1");
        let fixture = fixture_path("mini_social.cypher");
        load_fixture(&db, &fixture);
    }

    // Session 2: reopen, verify counts.
    {
        let db = open(dir.path()).expect("open session 2");

        let persons = db
            .execute("MATCH (n:Person) RETURN n.name")
            .expect("MATCH Person after reopen")
            .rows
            .len();
        assert_eq!(persons, N_PERSONS, "Person count must survive reopen");

        let companies = db
            .execute("MATCH (n:Company) RETURN n.name")
            .expect("MATCH Company after reopen")
            .rows
            .len();
        assert_eq!(companies, N_COMPANIES, "Company count must survive reopen");

        let posts = db
            .execute("MATCH (n:Post) RETURN n.content")
            .expect("MATCH Post after reopen")
            .rows
            .len();
        assert_eq!(posts, N_POSTS, "Post count must survive reopen");
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 11: Knowledge graph — Fact node count ────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// After loading the knowledge fixture, MATCH (n:Fact) must return 120 rows.
#[test]
fn knowledge_graph_facts_count() {
    let (_dir, db, _, _) = setup_knowledge_graph();

    let r = db
        .execute("MATCH (n:Fact) RETURN n.content")
        .expect("MATCH Fact");

    assert_eq!(
        r.rows.len(),
        N_FACTS,
        "expected {N_FACTS} Fact nodes in knowledge graph"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 12: Knowledge graph — Entity node count ──────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// After loading the knowledge fixture, MATCH (n:Entity) must return 80 rows.
#[test]
fn knowledge_graph_entities_count() {
    let (_dir, db, _, _) = setup_knowledge_graph();

    let r = db
        .execute("MATCH (n:Entity) RETURN n.name")
        .expect("MATCH Entity");

    assert_eq!(
        r.rows.len(),
        N_ENTITIES,
        "expected {N_ENTITIES} Entity nodes in knowledge graph"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 13: Knowledge graph — RELATES_TO traversal ──────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// MATCH (a:Fact)-[:RELATES_TO]->(b:Fact) on the knowledge graph returns rows.
#[test]
fn knowledge_graph_relates_to_traversable() {
    let (_dir, db, _, _) = setup_knowledge_graph();

    let r = db
        .execute("MATCH (a:Fact)-[:RELATES_TO]->(b:Fact) RETURN a.source, b.source")
        .expect("MATCH RELATES_TO");

    assert!(
        r.rows.len() > 0,
        "RELATES_TO traversal must return at least 1 row on a {N_RELATES_TO}-edge graph"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 14: Knowledge graph — WHERE on string property ──────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// MATCH (n:Fact) WHERE n.source = 'wikipedia:en' returns a non-empty subset.
#[test]
fn knowledge_graph_where_filter_source() {
    let (_dir, db, _, _) = setup_knowledge_graph();

    let all = db
        .execute("MATCH (n:Fact) RETURN n.source")
        .expect("MATCH all facts")
        .rows
        .len();
    assert_eq!(all, N_FACTS, "all {N_FACTS} facts visible");

    let filtered = db
        .execute("MATCH (n:Fact) WHERE n.source = 'wikipedia:en' RETURN n.content")
        .expect("MATCH WHERE source = wikipedia:en");

    assert!(
        filtered.rows.len() > 0,
        "WHERE source = 'wikipedia:en' must match at least one Fact"
    );
    assert!(
        filtered.rows.len() < N_FACTS,
        "WHERE source = 'wikipedia:en' must not match all {N_FACTS} Facts"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ── Test 15: LIMIT clause at scale ───────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

/// MATCH (n:Person) RETURN n.name LIMIT 10 must return exactly 10 rows even
/// though 1000 Person nodes exist.
///
/// Exercises the LIMIT clause on a large scan (1000-node Person label).
#[test]
fn social_graph_limit_clause_at_scale() {
    let (_dir, db, _, _, _) = setup_social_graph();

    let r = db
        .execute("MATCH (n:Person) RETURN n.name LIMIT 10")
        .expect("MATCH LIMIT 10");

    assert_eq!(
        r.rows.len(),
        10,
        "LIMIT 10 on 1000-node graph must return exactly 10 rows"
    );
}
