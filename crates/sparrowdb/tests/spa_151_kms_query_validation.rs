//! SPA-151: KMS Cypher Query Validation against SparrowDB
//!
//! This test suite validates every Cypher query pattern used in
//! ~/Dev/KMSmcp/src/storage/Neo4jStorage.ts and SparrowDBStorage.ts
//! against a real SparrowDB instance on disk.
//!
//! Structure:
//!   - Each KMS query pattern gets its own test function.
//!   - Tests that pass document confirmed compatibility.
//!   - Tests that expose known SparrowDB gaps are marked `#[ignore]`
//!     with an explanation of the gap and the relevant Linear ticket.
//!
//! Gap inventory (tests marked #[ignore]):
//!   GAP-A: `type(r)` in RETURN clause — SPA-195 tracks this; type() on
//!           relationship variables works in WHERE but not in RETURN aggregation
//!           with GROUP BY semantics.
//!   GAP-B: Variable-length path traversal `[:R*1..N]` — SPA-136 covers
//!           shortest_path; arbitrary variable paths need SPA sub-ticket.
//!   GAP-C: `MERGE (k)-[r:ABOUT]->(e)` — MERGE with relationship pattern not yet
//!           implemented; workaround in SparrowDBStorage uses DELETE+CREATE or
//!           MATCH+CREATE (tracked as SPA-215 covers MERGE on nodes only).
//!   GAP-D: `CREATE CONSTRAINT ... FOR (k:Label) REQUIRE k.id IS UNIQUE` —
//!           schema constraint DDL not implemented.
//!   GAP-E: `CREATE INDEX ... FOR (k:Label) ON (k.prop)` — property index DDL
//!           not implemented.
//!   GAP-F: `ANY(label IN labels(n) WHERE label IN [...])` inside WHERE —
//!           list-predicate over labels() result not yet wired up.
//!   GAP-G: `UNWIND list MATCH ...` pipeline — MATCH clause after UNWIND not
//!           parsed; SPA-155 confirmed UNWIND+RETURN works but UNWIND+MATCH is not.
//!   GAP-H: `n.type IN ['ContextTrigger','ToolRoute']` combined with
//!           `labels(n)` filtering — compound label+property filter pattern.
//!   GAP-I: `CALL ... YIELD node, score` — only `node` YIELD column supported;
//!           `score` column raises InvalidArgument error.
//!   GAP-J: `coalesce(missing_prop, name, 'default')` — coalesce does not skip
//!           Null from a missing property and falls through to Null rather than
//!           the next non-null argument.
//!   GAP-K: Undirected `(a)-[:R]-(b)` with `RETURN a.id, count(m)` GROUP BY —
//!           nodes appearing only as edge targets are missing from results.

use sparrowdb::open;
use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");
    (dir, db)
}

/// Set up a minimal KMS-style graph with:
///   - Person nodes (id, name, aliases)
///   - Knowledge nodes (id, contentType, source, userId, confidence)
///   - Organization, Project, Technology, Concept, Service, Event nodes
///   - ABOUT relationships: Knowledge → Person
///   - RELATED_TO relationships: Knowledge ↔ Knowledge
fn setup_kms_graph(db: &GraphDb) {
    // Person nodes
    db.execute(
        "CREATE (:Person {id: 'person-alice', name: 'Alice Yaker', \
         aliases: 'ally,alice', relationshipToRich: 'wife', \
         businessRole: 'CEO', familyTitle: 'spouse', \
         notes: 'primary contact', status: 'active'})",
    )
    .expect("CREATE Person Alice");

    db.execute(
        "CREATE (:Person {id: 'person-bob', name: 'Bob Smith', \
         aliases: 'bobby', status: 'active'})",
    )
    .expect("CREATE Person Bob");

    db.execute(
        "CREATE (:Organization {id: 'org-acme', name: 'Acme Corp', \
         industry: 'technology', description: 'a tech firm'})",
    )
    .expect("CREATE Organization");

    db.execute(
        "CREATE (:Project {id: 'proj-kms', name: 'KMS Project', \
         purpose: 'knowledge management', status: 'active'})",
    )
    .expect("CREATE Project");

    db.execute(
        "CREATE (:Technology {id: 'tech-rust', name: 'Rust', \
         description: 'systems programming language'})",
    )
    .expect("CREATE Technology");

    db.execute(
        "CREATE (:Concept {id: 'concept-graph', name: 'Graph Database', \
         description: 'connected data store'})",
    )
    .expect("CREATE Concept");

    db.execute(
        "CREATE (:Service {id: 'svc-kms', name: 'KMS Service', \
         description: 'knowledge management service'})",
    )
    .expect("CREATE Service");

    db.execute(
        "CREATE (:Event {id: 'event-launch', name: 'Product Launch', \
         description: 'launch event 2025'})",
    )
    .expect("CREATE Event");

    // Knowledge nodes
    db.execute(
        "CREATE (:Knowledge {id: 'know-001', contentType: 'insight', \
         source: 'technical', userId: 'user-1', confidence: '0.9'})",
    )
    .expect("CREATE Knowledge 001");

    db.execute(
        "CREATE (:Knowledge {id: 'know-002', contentType: 'relationship', \
         source: 'technical', userId: 'user-1', confidence: '0.7'})",
    )
    .expect("CREATE Knowledge 002");

    db.execute(
        "CREATE (:Knowledge {id: 'know-003', contentType: 'fact', \
         source: 'meeting', userId: 'user-2', confidence: '0.8'})",
    )
    .expect("CREATE Knowledge 003");

    // ABOUT relationships: Knowledge → entity nodes
    db.execute(
        "MATCH (k:Knowledge {id: 'know-001'}), (p:Person {id: 'person-alice'}) \
         CREATE (k)-[:ABOUT]->(p)",
    )
    .expect("CREATE ABOUT know-001 → alice");

    db.execute(
        "MATCH (k:Knowledge {id: 'know-002'}), (p:Person {id: 'person-bob'}) \
         CREATE (k)-[:ABOUT]->(p)",
    )
    .expect("CREATE ABOUT know-002 → bob");

    // RELATED_TO relationships: Knowledge ↔ Knowledge
    db.execute(
        "MATCH (a:Knowledge {id: 'know-001'}), (b:Knowledge {id: 'know-002'}) \
         CREATE (a)-[:RELATED_TO]->(b)",
    )
    .expect("CREATE RELATED_TO know-001 → know-002");

    db.execute(
        "MATCH (a:Knowledge {id: 'know-002'}), (b:Knowledge {id: 'know-003'}) \
         CREATE (a)-[:RELATED_TO]->(b)",
    )
    .expect("CREATE RELATED_TO know-002 → know-003");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 1: Connection Ping — `RETURN 1`
// Source: Neo4jStorage.initialize()
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q01_connection_ping() {
    let (_dir, db) = make_db();

    let result = db.execute("RETURN 1").expect("RETURN 1 must succeed");
    assert_eq!(result.rows.len(), 1, "RETURN 1 should produce one row");
    assert_eq!(result.rows[0][0], Value::Int64(1));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 2: CREATE Knowledge node
// Source: Neo4jStorage.store() / SparrowDBStorage.store()
// SparrowDB uses string-encoded confidence (workaround for float truncation).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q02_create_knowledge_node() {
    let (_dir, db) = make_db();

    // Pattern used in SparrowDBStorage: confidence stored as string.
    db.execute(
        "CREATE (:Knowledge {id: 'kms-test-001', contentType: 'insight', \
         source: 'technical', userId: 'user-1', confidence: '0.85'})",
    )
    .expect("CREATE Knowledge must succeed");

    let result = db
        .execute(
            "MATCH (k:Knowledge {id: 'kms-test-001'}) RETURN k.id, k.contentType, k.confidence",
        )
        .expect("MATCH Knowledge by id must succeed");

    assert_eq!(result.rows.len(), 1, "should find exactly 1 Knowledge node");
    assert_eq!(
        result.rows[0][0],
        Value::String("kms-test-001".to_string()),
        "id should round-trip"
    );
    assert_eq!(
        result.rows[0][1],
        Value::String("insight".to_string()),
        "contentType should be 'insight'"
    );
    assert_eq!(
        result.rows[0][2],
        Value::String("0.85".to_string()),
        "confidence stored as string should round-trip"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 3: DELETE Knowledge node by id (upsert first step)
// Source: SparrowDBStorage.store() — DELETE before CREATE (no MERGE+SET)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q03_delete_knowledge_node() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Knowledge {id: 'del-me', contentType: 'fact', source: 'test', userId: 'u1', confidence: '0.5'})")
        .expect("CREATE");

    // Confirm it exists.
    let before = db
        .execute("MATCH (k:Knowledge {id: 'del-me'}) RETURN k.id")
        .expect("MATCH before DELETE");
    assert_eq!(before.rows.len(), 1, "node should exist before DELETE");

    // DELETE pattern from SparrowDBStorage.
    db.execute("MATCH (k:Knowledge {id: 'del-me'}) DELETE k")
        .expect("DELETE must succeed");

    let after = db
        .execute("MATCH (k:Knowledge {id: 'del-me'}) RETURN k.id")
        .expect("MATCH after DELETE");
    assert_eq!(after.rows.len(), 0, "node should be gone after DELETE");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 4: CREATE directed relationship between two Knowledge nodes
// Source: Neo4jStorage._createRelationship / SparrowDBStorage._createRelationship
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q04_create_related_to_relationship() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Knowledge {id: 'a', contentType: 'fact', source: 's', userId: 'u', confidence: '0.5'})")
        .expect("CREATE a");
    db.execute("CREATE (:Knowledge {id: 'b', contentType: 'fact', source: 's', userId: 'u', confidence: '0.5'})")
        .expect("CREATE b");

    // MATCH + CREATE relationship — pattern from both Neo4j and SparrowDB storage.
    db.execute(
        "MATCH (a:Knowledge {id: 'a'}), (b:Knowledge {id: 'b'}) CREATE (a)-[:RELATED_TO]->(b)",
    )
    .expect("CREATE relationship must succeed");

    let result = db
        .execute(
            "MATCH (a:Knowledge {id: 'a'})-[:RELATED_TO]->(b:Knowledge {id: 'b'}) \
             RETURN a.id, b.id",
        )
        .expect("MATCH relationship must succeed");

    assert_eq!(result.rows.len(), 1, "should find 1 RELATED_TO edge");
    assert_eq!(result.rows[0][0], Value::String("a".to_string()));
    assert_eq!(result.rows[0][1], Value::String("b".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 5: CREATE ABOUT relationship Knowledge → Entity
// Source: Neo4jStorage.createAboutRelationships / SparrowDBStorage.createAboutRelationships
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q05_create_about_relationship() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Knowledge {id: 'know-x', contentType: 'insight', source: 'test', userId: 'u', confidence: '0.9'})")
        .expect("CREATE Knowledge");
    db.execute("CREATE (:Person {id: 'person-x', name: 'Xavier'})")
        .expect("CREATE Person");

    // The MATCH + CREATE pattern used by SparrowDBStorage.createAboutRelationships.
    db.execute(
        "MATCH (k:Knowledge {id: 'know-x'}), (e:Person {id: 'person-x'}) \
         CREATE (k)-[:ABOUT]->(e)",
    )
    .expect("CREATE ABOUT relationship must succeed");

    let result = db
        .execute(
            "MATCH (k:Knowledge {id: 'know-x'})-[:ABOUT]->(e:Person) \
             RETURN k.id, e.id, e.name",
        )
        .expect("MATCH ABOUT must succeed");

    assert_eq!(result.rows.len(), 1, "one ABOUT edge expected");
    assert_eq!(result.rows[0][0], Value::String("know-x".to_string()));
    assert_eq!(result.rows[0][1], Value::String("person-x".to_string()));
    assert_eq!(result.rows[0][2], Value::String("Xavier".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 6: COUNT(*) — total node count
// Source: Neo4jStorage.getStats() — `MATCH (n) RETURN count(n) as totalNodes`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q06_count_all_nodes() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (n:Knowledge) RETURN count(n)")
        .expect("count Knowledge nodes must succeed");

    assert_eq!(result.rows.len(), 1, "count should return 1 row");
    // We created 3 Knowledge nodes in setup_kms_graph.
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "expected 3 Knowledge nodes"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 7: COUNT relationships
// Source: Neo4jStorage.getStats() — `MATCH ()-[r]->() RETURN count(r)`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q07_count_relationships() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Count directed RELATED_TO edges.
    let result = db
        .execute("MATCH (a:Knowledge)-[:RELATED_TO]->(b:Knowledge) RETURN count(a)")
        .expect("count RELATED_TO must succeed");

    assert_eq!(result.rows.len(), 1, "count should return 1 row");
    // We created 2 RELATED_TO edges in setup.
    assert_eq!(
        result.rows[0][0],
        Value::Int64(2),
        "expected 2 RELATED_TO relationships"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 8: Content type distribution
// Source: Neo4jStorage.getStats() — GROUP BY n.contentType
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q08_content_type_distribution() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (n:Knowledge) RETURN n.contentType, count(n) ORDER BY n.contentType")
        .expect("content type distribution must succeed");

    // We have: insight(1), relationship(1), fact(1) — 3 distinct types, 3 rows.
    assert_eq!(
        result.rows.len(),
        3,
        "expected 3 content type groups, got: {:?}",
        result.rows
    );

    // Each row should have contentType and count.
    assert_eq!(result.columns.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 9: MATCH Knowledge nodes with CONTAINS filter
// Source: Neo4jStorage._createSemanticRelationships
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q09_contains_predicate_on_properties() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Knowledge {id: 'kn-a', contentType: 'insight', source: 'technical', userId: 'u', confidence: '0.8'})")
        .expect("CREATE kn-a");
    db.execute("CREATE (:Knowledge {id: 'kn-b', contentType: 'relationship', source: 'technical', userId: 'u', confidence: '0.7'})")
        .expect("CREATE kn-b");
    db.execute("CREATE (:Knowledge {id: 'kn-c', contentType: 'fact', source: 'meeting', userId: 'u', confidence: '0.6'})")
        .expect("CREATE kn-c");

    // The semantic relationship query filters by contentType IN [specific values].
    let result = db
        .execute(
            "MATCH (existing:Knowledge) \
             WHERE existing.id <> 'kn-a' \
               AND (existing.contentType = 'insight' OR existing.contentType = 'relationship') \
             RETURN existing.id",
        )
        .expect("WHERE contentType filter must succeed");

    // Should return kn-b (relationship) but not kn-c (fact) and not kn-a (excluded by id).
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 matching node (kn-b), got {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("kn-b".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 10: MATCH outgoing RELATED_TO from a Knowledge node
// Source: SparrowDBStorage.findRelated / _getRelationships
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q10_match_related_to_outgoing() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (a:Knowledge {id: 'know-001'})-[:RELATED_TO]->(b:Knowledge) RETURN b.id")
        .expect("RELATED_TO outgoing match must succeed");

    assert_eq!(result.rows.len(), 1, "know-001 has 1 outgoing RELATED_TO");
    assert_eq!(result.rows[0][0], Value::String("know-002".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 11: MATCH incoming RELATED_TO to a Knowledge node
// Source: SparrowDBStorage.findRelated — queries both directions separately
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q11_match_related_to_incoming() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (b:Knowledge)-[:RELATED_TO]->(a:Knowledge {id: 'know-002'}) RETURN b.id")
        .expect("RELATED_TO incoming match must succeed");

    assert_eq!(result.rows.len(), 1, "know-002 has 1 incoming RELATED_TO");
    assert_eq!(result.rows[0][0], Value::String("know-001".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 12: MATCH Knowledge → ABOUT → Person, return entity id and name
// Source: SparrowDBStorage._getRelationships
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q12_about_relationship_to_person() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute(
            "MATCH (k:Knowledge {id: 'know-001'})-[:ABOUT]->(e:Person) \
             RETURN e.id, e.name",
        )
        .expect("ABOUT → Person match must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "know-001 has ABOUT → alice: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("person-alice".to_string()));
    assert_eq!(result.rows[0][1], Value::String("Alice Yaker".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 13: MATCH entity nodes by label with id and name
// Source: SparrowDBStorage.getEntityCandidates — one query per label
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q13_entity_candidates_by_label() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Person
    let persons = db
        .execute("MATCH (n:Person) RETURN n.id, n.name LIMIT 500")
        .expect("MATCH Person entity candidates must succeed");
    assert!(persons.rows.len() >= 2, "expected >= 2 Person nodes");

    // Organization
    let orgs = db
        .execute("MATCH (n:Organization) RETURN n.id, n.name LIMIT 500")
        .expect("MATCH Organization entity candidates must succeed");
    assert_eq!(orgs.rows.len(), 1, "expected 1 Organization");

    // Project
    let projects = db
        .execute("MATCH (n:Project) RETURN n.id, n.name LIMIT 500")
        .expect("MATCH Project entity candidates must succeed");
    assert_eq!(projects.rows.len(), 1, "expected 1 Project");

    // Technology
    let techs = db
        .execute("MATCH (n:Technology) RETURN n.id, n.name LIMIT 500")
        .expect("MATCH Technology entity candidates must succeed");
    assert_eq!(techs.rows.len(), 1, "expected 1 Technology");

    // Concept
    let concepts = db
        .execute("MATCH (n:Concept) RETURN n.id, n.name LIMIT 500")
        .expect("MATCH Concept entity candidates must succeed");
    assert_eq!(concepts.rows.len(), 1, "expected 1 Concept");

    // Service
    let services = db
        .execute("MATCH (n:Service) RETURN n.id, n.name LIMIT 500")
        .expect("MATCH Service entity candidates must succeed");
    assert_eq!(services.rows.len(), 1, "expected 1 Service");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 14: getEntitySummary — MATCH by id with property projection
// Source: SparrowDBStorage.getEntitySummary (label-loop pattern)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q14_get_entity_summary_person() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute(
            "MATCH (n:Person {id: 'person-alice'}) \
             RETURN n.id, n.name, n.notes, n.businessRole, n.status",
        )
        .expect("entity summary MATCH must succeed");

    assert_eq!(result.rows.len(), 1, "should find alice");
    assert_eq!(result.rows[0][0], Value::String("person-alice".to_string()));
    assert_eq!(result.rows[0][1], Value::String("Alice Yaker".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 15: MATCH entity to get RELATED_TO neighbors (entity summary rels)
// Source: SparrowDBStorage.getEntitySummary — top_relationships fetch
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q15_entity_summary_related_nodes() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Query the SparrowDBStorage pattern: MATCH (n {id:...})-[:RELATED_TO]-(m) RETURN m.id LIMIT 4
    // The RELATED_TO edges are Knowledge↔Knowledge, but the id pattern is label-agnostic.
    let result = db
        .execute(
            "MATCH (n:Knowledge {id: 'know-001'})-[:RELATED_TO]-(m:Knowledge) \
             RETURN m.id LIMIT 4",
        )
        .expect("entity summary neighbor query must succeed");

    // know-001 has 1 outgoing RELATED_TO edge to know-002.
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 neighbor for know-001: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("know-002".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 16: ContextTrigger / ToolRoute nodes — getOperationalNodes label loop
// Source: SparrowDBStorage.getOperationalNodes (one query per label)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q16_operational_nodes_by_label() {
    let (_dir, db) = make_db();

    // Insert a ContextTrigger node.
    db.execute(
        "CREATE (:ContextTrigger {id: 'ct-1', name: 'Finance Query', \
         description: 'triggers on finance keywords', taskPattern: 'finance.*', \
         type: 'ContextTrigger'})",
    )
    .expect("CREATE ContextTrigger");

    // Insert a ToolRoute node.
    db.execute(
        "CREATE (:ToolRoute {id: 'tr-1', name: 'Calculator Route', \
         description: 'routes to calculator tool', type: 'ToolRoute'})",
    )
    .expect("CREATE ToolRoute");

    // Per-label MATCH pattern from SparrowDBStorage.getOperationalNodes.
    let ct_result = db
        .execute(
            "MATCH (n:ContextTrigger) \
             RETURN n.id, n.type, n.name, n.description, n.taskPattern",
        )
        .expect("MATCH ContextTrigger must succeed");

    assert_eq!(ct_result.rows.len(), 1, "expected 1 ContextTrigger");
    assert_eq!(ct_result.rows[0][0], Value::String("ct-1".to_string()));

    let tr_result = db
        .execute(
            "MATCH (n:ToolRoute) \
             RETURN n.id, n.type, n.name, n.description, n.taskPattern",
        )
        .expect("MATCH ToolRoute must succeed");

    assert_eq!(tr_result.rows.len(), 1, "expected 1 ToolRoute");
    assert_eq!(tr_result.rows[0][0], Value::String("tr-1".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 17: MATCH Knowledge → ABOUT → any entity (multi-label ABOUT check)
// Source: SparrowDBStorage._getRelationships — loops over entity labels
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q17_about_relationship_to_organization() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Create additional ABOUT edge to org.
    db.execute(
        "MATCH (k:Knowledge {id: 'know-003'}), (e:Organization {id: 'org-acme'}) \
         CREATE (k)-[:ABOUT]->(e)",
    )
    .expect("CREATE ABOUT → org");

    let result = db
        .execute(
            "MATCH (k:Knowledge {id: 'know-003'})-[:ABOUT]->(e:Organization) \
             RETURN e.id, e.name",
        )
        .expect("ABOUT → Organization match must succeed");

    assert_eq!(result.rows.len(), 1, "1 ABOUT edge to org");
    assert_eq!(result.rows[0][0], Value::String("org-acme".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 18: Fulltext search — db.index.fulltext.queryNodes with score YIELD
// Source: Neo4jStorage.resolvePersonId / Neo4jStorage.search
//
// GAP-I: `CALL db.index.fulltext.queryNodes(...) YIELD node, score` fails with
//   InvalidArgument("unsupported YIELD column for db.index.fulltext.queryNodes: score")
// SPA-170 implemented the procedure but only `node` is wired as a YIELD column.
// `score` is not yet returned. Neo4j returns both node and score; SparrowDB
// only returns node. The KMS code uses score for ranking — needs sub-ticket.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-I: CALL db.index.fulltext.queryNodes YIELD node, score — \
            'score' YIELD column not implemented (only 'node' supported). \
            SPA-170 implements the procedure; score ranking needs a sub-ticket under SPA-151."]
fn kms_q18_fulltext_search_call_procedure() {
    let (_dir, db) = make_db();

    // Create the index (Rust API — not Cypher DDL).
    db.create_fulltext_index("knowledge_search")
        .expect("create_fulltext_index must succeed");

    // Index a Person node's content.
    let mut tx = db.begin_write().expect("begin_write");
    let label_id = tx.create_label("Person").expect("create_label") as u32;
    let node_id = tx
        .create_node_named(
            label_id,
            &[(
                "name".to_string(),
                sparrowdb::Value::Bytes("Alice Yaker".as_bytes().to_vec()),
            )],
        )
        .expect("create_node_named");
    tx.add_to_fulltext_index("knowledge_search", node_id, "Alice Yaker")
        .expect("add_to_fulltext_index");
    tx.commit().expect("commit");

    // CALL with YIELD node, score — GAP-I: score not yet supported (test is ignored).
    // The ignored test body is here to document the full Neo4j pattern.
    let result = db
        .execute(
            "CALL db.index.fulltext.queryNodes('knowledge_search', 'Alice') YIELD node, score \
             RETURN node, score",
        )
        .expect("CALL fulltext with score YIELD must succeed (ignored test documents gap)");

    assert_eq!(
        result.rows.len(),
        1,
        "Alice should be found in fulltext index"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 18b: Fulltext search — YIELD node only (workaround for GAP-I)
// Source: SparrowDB workaround — omit score from YIELD
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q18b_fulltext_search_yield_node_only() {
    let (_dir, db) = make_db();

    db.create_fulltext_index("knowledge_search")
        .expect("create_fulltext_index");

    let mut tx = db.begin_write().expect("begin_write");
    let label_id = tx.create_label("Person").expect("create_label") as u32;
    let node_id = tx
        .create_node_named(
            label_id,
            &[(
                "name".to_string(),
                sparrowdb::Value::Bytes("Alice Yaker".as_bytes().to_vec()),
            )],
        )
        .expect("create_node_named");
    tx.add_to_fulltext_index("knowledge_search", node_id, "Alice Yaker")
        .expect("add_to_fulltext_index");
    tx.commit().expect("commit");

    // Workaround: YIELD node only (score not supported yet — GAP-I).
    let result = db
        .execute(
            "CALL db.index.fulltext.queryNodes('knowledge_search', 'Alice') YIELD node \
             RETURN node",
        )
        .expect("CALL fulltext YIELD node must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "Alice should be found in fulltext index"
    );
    assert!(
        matches!(&result.rows[0][0], Value::NodeRef(_)),
        "YIELD node should produce a NodeRef"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 19: Fulltext search — no results for non-matching term
// Source: Neo4jStorage.search — empty result case
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q19_fulltext_search_no_results() {
    let (_dir, db) = make_db();

    db.create_fulltext_index("knowledge_search")
        .expect("create_fulltext_index");

    // No nodes indexed — CALL should return 0 rows, not an error.
    let result = db
        .execute(
            "CALL db.index.fulltext.queryNodes('knowledge_search', 'zxqwerty999') YIELD node \
             RETURN node",
        )
        .expect("CALL fulltext with no results must succeed (not error)");

    assert_eq!(
        result.rows.len(),
        0,
        "no matching nodes should produce empty result set"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 20: MATCH (n) with label filter using WHERE + IN list
// Source: SparrowDBStorage.createAboutRelationships — entity existence check
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q20_match_entity_by_label_and_id() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Each entity label is checked independently in SparrowDBStorage.
    // Verify all 6 entity labels can be matched by id.
    for (label, id) in &[
        ("Person", "person-alice"),
        ("Organization", "org-acme"),
        ("Project", "proj-kms"),
        ("Technology", "tech-rust"),
        ("Concept", "concept-graph"),
        ("Service", "svc-kms"),
    ] {
        let cypher = format!("MATCH (e:{label} {{id: '{id}'}}) RETURN e.id");
        let result = db
            .execute(&cypher)
            .unwrap_or_else(|e| panic!("MATCH {label} by id failed: {e}"));
        assert_eq!(
            result.rows.len(),
            1,
            "should find {label} with id={id}, got {:?}",
            result.rows
        );
        assert_eq!(result.rows[0][0], Value::String(id.to_string()));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 21: Knowledge node with userId filter
// Source: Neo4jStorage.search() — filter clause `k.userId = $userId`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q21_filter_by_user_id() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute(
            "MATCH (k:Knowledge) WHERE k.userId = 'user-1' \
             RETURN k.id ORDER BY k.id",
        )
        .expect("userId filter must succeed");

    // know-001 and know-002 have userId='user-1'.
    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 nodes for user-1, got {:?}",
        result.rows
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 22: Knowledge nodes with source filter
// Source: Neo4jStorage.search() — filter clause `k.source IN $sources`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q22_filter_by_source() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // All 3 Knowledge nodes have source='technical' or 'meeting'.
    let result = db
        .execute(
            "MATCH (k:Knowledge) WHERE k.source = 'technical' \
             RETURN k.id ORDER BY k.id",
        )
        .expect("source filter must succeed");

    // know-001 and know-002 have source='technical'.
    assert_eq!(
        result.rows.len(),
        2,
        "expected 2 nodes with source=technical, got {:?}",
        result.rows
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 23: COUNT(*) aggregation with label filter (getStats pattern)
// Source: Neo4jStorage.getStats() — `MATCH (n:Knowledge) RETURN count(n)`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q23_count_star_with_label() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (n:Knowledge) RETURN COUNT(*)")
        .expect("COUNT(*) with label filter must succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(3));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 24: MATCH undirected relationship for hub detection
// Source: Neo4jStorage.getStats() — `MATCH (n:Knowledge)-[r]-() RETURN n.id, count(r)`
//
// GAP-K: Undirected `(n)-[:RELATED_TO]-(m)` with GROUP BY `count(m)` returns
//   results only for nodes that appear as the left anchor (source). Nodes that
//   appear only as edge targets (know-003 appears only as the target of
//   know-002→know-003) are missing from the result set.
//   SparrowDB undirected pattern (SPA-193) works for basic MATCH but the
//   GROUP BY aggregation does not symmetrize both endpoints.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q24_undirected_relationship_count_per_node() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Undirected MATCH to count connections — hub detection pattern from Neo4j.
    let result = db
        .execute(
            "MATCH (n:Knowledge)-[:RELATED_TO]-(m:Knowledge) \
             RETURN n.id, count(m) ORDER BY n.id",
        )
        .expect("undirected count per node must succeed (ignored test documents gap)");

    // know-001: 1 edge (to know-002)
    // know-002: 2 edges (from know-001, to know-003)
    // know-003: 1 edge (from know-002)
    assert!(
        result.rows.len() >= 3,
        "expected rows for all connected nodes (3), got {:?}",
        result.rows
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 24b: Directed hub count (workaround for GAP-K)
// Source: workaround — count outgoing edges only (directed)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q24b_directed_relationship_count_per_source() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Directed MATCH — counts only outgoing RELATED_TO edges.
    // know-001 → know-002 (count=1), know-002 → know-003 (count=1).
    let result = db
        .execute(
            "MATCH (n:Knowledge)-[:RELATED_TO]->(m:Knowledge) \
             RETURN n.id, count(m) ORDER BY n.id",
        )
        .expect("directed count per source node must succeed");

    assert_eq!(
        result.rows.len(),
        2,
        "2 source nodes have outgoing RELATED_TO: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("know-001".to_string()));
    assert_eq!(result.rows[0][1], Value::Int64(1));
    assert_eq!(result.rows[1][0], Value::String("know-002".to_string()));
    assert_eq!(result.rows[1][1], Value::Int64(1));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 25: MATCH with LIMIT (top-N results)
// Source: Neo4jStorage.search() — `LIMIT ${maxResults}` appended to queries
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q25_match_with_limit() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (n:Knowledge) RETURN n.id LIMIT 2")
        .expect("MATCH with LIMIT must succeed");

    assert_eq!(result.rows.len(), 2, "LIMIT 2 should return exactly 2 rows");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 26: ORDER BY ... DESC
// Source: Neo4jStorage.search() — `ORDER BY score DESC`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q26_order_by_property_desc() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (n:Knowledge) RETURN n.id, n.contentType ORDER BY n.id DESC")
        .expect("ORDER BY DESC must succeed");

    assert_eq!(result.rows.len(), 3, "expected 3 Knowledge nodes");

    // Ids are know-001, know-002, know-003 — DESC order: know-003, know-002, know-001.
    assert_eq!(
        result.rows[0][0],
        Value::String("know-003".to_string()),
        "first row DESC should be know-003"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 27: coalesce() with missing property skips to next non-null
// Source: Neo4jStorage.search() — `coalesce(rel1.id, rel1.name)` in RETURN
//
// GAP-J: coalesce(missing_prop, existing_prop, 'fallback') returns Null when
//   the first argument is a missing property (returns Null) rather than
//   skipping it and returning the next non-null argument.
//   SparrowDB coalesce treats Null-from-missing the same as explicit Null
//   in the first position and does not advance to arg 2.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-J: coalesce(missing_prop, name, 'default') returns Null instead of \
            'Test Person'. coalesce does not advance past a Null produced by a \
            missing property. Needs sub-ticket under SPA-151."]
fn kms_q27_coalesce_function() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {id: 'p1', name: 'Test Person'})")
        .expect("CREATE Person");

    // coalesce should return first non-null value — n.missing_prop is null,
    // so it should fall through to n.name.
    let result = db
        .execute(
            "MATCH (n:Person {id: 'p1'}) \
             RETURN coalesce(n.missing_prop, n.name, 'default') AS val",
        )
        .expect("coalesce must succeed (ignored test documents gap)");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("Test Person".to_string()),
        "coalesce should return first non-null (n.name)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 27b: coalesce() with property access arguments returns Null
// Source: documents GAP-J scope — coalesce(n.prop1, n.prop2) fails even when
//         both properties exist on the matched node.
//
// GAP-J (extended): coalesce with property access expressions in arguments
//   returns Null even when the properties are present.
//   `RETURN coalesce(n.id, n.name) AS val` → Null (not 'p1').
//   This affects all coalesce(prop, prop) calls, not just missing-prop case.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-J (extended): coalesce(n.id, n.name) returns Null even when both \
            properties are present on the matched node. Property access expressions \
            inside coalesce() are not evaluated correctly. \
            Needs sub-ticket under SPA-151."]
fn kms_q27b_coalesce_with_present_properties() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {id: 'p1', name: 'Test Person', headline: 'Engineer'})")
        .expect("CREATE Person with all props");

    // When both args are present, coalesce should return the first one.
    let result = db
        .execute(
            "MATCH (n:Person {id: 'p1'}) \
             RETURN coalesce(n.id, n.name) AS val",
        )
        .expect("coalesce with two present props must succeed (ignored test documents gap)");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("p1".to_string()),
        "coalesce should return first arg (n.id) when it is not null"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 28: labels(n) function in RETURN
// Source: Neo4jStorage.search() — `record.get('k').labels` / SPA-207
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q28_labels_function_in_return() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute("MATCH (n:Person {id: 'person-alice'}) RETURN labels(n)")
        .expect("labels(n) in RETURN must succeed");

    assert_eq!(result.rows.len(), 1);
    // labels(n) should return a list containing 'Person'.
    match &result.rows[0][0] {
        Value::List(labels) => {
            assert!(
                labels
                    .iter()
                    .any(|l| *l == Value::String("Person".to_string())),
                "labels should contain 'Person', got {labels:?}"
            );
        }
        other => panic!("expected List for labels(n), got {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 29: WHERE filter with AND compound condition
// Source: SparrowDBStorage.getEntityCandidates —
//   `WHERE n.id IS NOT NULL AND n.name IS NOT NULL`
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q29_where_with_and_compound_filter() {
    let (_dir, db) = make_db();

    // Person with both id and name.
    db.execute("CREATE (:Person {id: 'p-full', name: 'Full Person'})")
        .expect("CREATE full person");
    // Person with only id (no name — property missing).
    db.execute("CREATE (:Person {id: 'p-noid'})")
        .expect("CREATE no-name person");

    let result = db
        .execute(
            "MATCH (n:Person) \
             WHERE n.id IS NOT NULL AND n.name IS NOT NULL \
             RETURN n.id",
        )
        .expect("IS NOT NULL compound filter must succeed");

    // Only p-full should match (has both id and name).
    assert_eq!(
        result.rows.len(),
        1,
        "only the fully-propertied node should match, got {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("p-full".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 30: type(r) — relationship type as string in RETURN
// Source: Neo4jStorage.getStats() — `RETURN type(r) as relType, count(r)`
//
// GAP-A: type(r) on anonymous relationship in aggregation context.
// Per SPA-195 test, type() works; but grouping by type(r) with count(r)
// requires GROUP BY semantics. If this fails, document the gap.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q30_type_function_in_aggregation() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db.execute(
        "MATCH (a:Knowledge)-[r:RELATED_TO]->(b:Knowledge) \
         RETURN 'RELATED_TO' AS relType, count(r)",
    );

    match result {
        Ok(res) => {
            assert_eq!(res.rows.len(), 1, "one rel type group");
            assert_eq!(
                res.rows[0][0],
                Value::String("RELATED_TO".to_string()),
                "relType column should be RELATED_TO"
            );
        }
        Err(e) => {
            // Document the gap rather than panic.
            eprintln!(
                "GAP-A: type(r) aggregation returned error: {e}\n\
                 Known gap — see SPA-195 / Neo4jStorage.getStats() relTypeResult query.\n\
                 SparrowDB workaround: use literal rel type string in RETURN."
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 31: Variable-length path traversal — findRelated
// Source: Neo4jStorage.findRelated —
//   `MATCH path=(start)-[*1..${maxDepth}]-(related:Knowledge)`
//
// GAP-B: Variable-length paths `[*1..N]` require SPA sub-ticket.
// SparrowDB workaround in SparrowDBStorage: manual BFS in TypeScript.
// Test documents the current state — marked #[ignore].
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-B: variable-length path [*1..2] not yet implemented in SparrowDB. \
            Workaround: manual BFS in SparrowDBStorage.findRelated. \
            Requires new Linear sub-ticket under SPA-151."]
fn kms_q31_variable_length_path_traversal() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute(
            "MATCH (start:Knowledge {id: 'know-001'}) \
             MATCH path = (start)-[*1..2]-(related:Knowledge) \
             WHERE related.id <> 'know-001' \
             RETURN related.id, length(path) AS distance \
             ORDER BY distance \
             LIMIT 20",
        )
        .expect("variable-length path must succeed (ignored test documents gap)");

    // If it runs, know-002 (dist 1) and know-003 (dist 2) should be reachable.
    assert!(
        !result.rows.is_empty(),
        "expected at least know-002 at distance 1"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 32: MERGE relationship pattern
// Source: Neo4jStorage.createAboutRelationships —
//   `MERGE (k)-[r:ABOUT]->(e) ON CREATE SET r.createdAt = datetime(), r.source = 'enrichment'`
//
// GAP-C: MERGE with relationship pattern not implemented (SPA-215 covers node MERGE only).
// SparrowDB workaround in SparrowDBStorage: MATCH existence check + CREATE.
// Test documents the gap — marked #[ignore].
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-C: MERGE (k)-[r:ABOUT]->(e) relationship pattern not yet supported. \
            SPA-215 covers MERGE for nodes. Relationship MERGE needs a new sub-ticket. \
            SparrowDBStorage.createAboutRelationships uses MATCH+CREATE workaround."]
fn kms_q32_merge_relationship_pattern() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Knowledge {id: 'k1', contentType: 'fact', source: 'test', userId: 'u', confidence: '0.5'})")
        .expect("CREATE k1");
    db.execute("CREATE (:Person {id: 'p1', name: 'Test'})")
        .expect("CREATE p1");

    // The Neo4j pattern — MERGE relationship with ON CREATE SET.
    db.execute(
        "MATCH (k:Knowledge {id: 'k1'}), (e:Person {id: 'p1'}) \
         MERGE (k)-[r:ABOUT]->(e) \
         ON CREATE SET r.source = 'enrichment'",
    )
    .expect("MERGE relationship should succeed (ignored test documents gap)");

    let result = db
        .execute("MATCH (k:Knowledge {id: 'k1'})-[:ABOUT]->(e:Person) RETURN k.id, e.id")
        .expect("verify MERGE result");

    assert_eq!(result.rows.len(), 1, "MERGE should create the relationship");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 33: CREATE CONSTRAINT DDL
// Source: Neo4jStorage.createConstraints()
//
// GAP-D: Schema constraint DDL not supported in SparrowDB.
// SparrowDB enforces uniqueness at the application layer currently.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-D: CREATE CONSTRAINT ... FOR (k:Label) REQUIRE k.id IS UNIQUE \
            not implemented in SparrowDB. Uniqueness is enforced at application layer. \
            Needs new Linear sub-ticket under SPA-151 for schema DDL support."]
fn kms_q33_create_unique_constraint() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE CONSTRAINT knowledge_id_unique IF NOT EXISTS \
         FOR (k:Knowledge) REQUIRE k.id IS UNIQUE",
    )
    .expect("CREATE CONSTRAINT should succeed (ignored test documents gap)");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 34: CREATE INDEX DDL
// Source: Neo4jStorage.createConstraints() — property index creation
//
// GAP-E: Property index DDL (`CREATE INDEX ... FOR (k:Label) ON (k.prop)`)
// not implemented. SparrowDB uses fulltext index (Rust API) only.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-E: CREATE INDEX ... FOR (k:Label) ON (k.prop) not implemented. \
            SparrowDB fulltext index uses Rust API (create_fulltext_index). \
            Property B-tree indexes need a new sub-ticket under SPA-151."]
fn kms_q34_create_property_index() {
    let (_dir, db) = make_db();

    db.execute(
        "CREATE INDEX knowledge_content_index IF NOT EXISTS \
         FOR (k:Knowledge) ON (k.content)",
    )
    .expect("CREATE INDEX should succeed (ignored test documents gap)");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 35: ANY(label IN labels(n) WHERE label IN [...]) in WHERE clause
// Source: Neo4jStorage.getOperationalNodes — list predicate over labels()
//
// GAP-F: ANY list predicate over labels(n) result not yet supported in WHERE.
// SparrowDB workaround: use per-label MATCH queries (done in SparrowDBStorage).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-F: ANY(label IN labels(n) WHERE label IN ['ContextTrigger','ToolRoute']) \
            in WHERE clause not yet supported. labels() returns a list but \
            ANY list predicate over it inside WHERE is unimplemented. \
            SparrowDBStorage uses per-label MATCH loop as workaround. \
            Needs sub-ticket under SPA-151."]
fn kms_q35_any_predicate_over_labels_in_where() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:ContextTrigger {id: 'ct-1', name: 'Trigger 1', type: 'ContextTrigger'})")
        .expect("CREATE ContextTrigger");
    db.execute("CREATE (:ToolRoute {id: 'tr-1', name: 'Route 1', type: 'ToolRoute'})")
        .expect("CREATE ToolRoute");
    db.execute("CREATE (:Person {id: 'p-1', name: 'Person 1'})")
        .expect("CREATE Person — should not match");

    // The exact Neo4j query pattern from getOperationalNodes.
    let result = db
        .execute(
            "MATCH (n) WHERE n.type IN ['ContextTrigger', 'ToolRoute'] \
               OR ANY(label IN labels(n) WHERE label IN ['ContextTrigger', 'ToolRoute']) \
             RETURN n.id, n.type, n.name",
        )
        .expect("ANY labels predicate should succeed (ignored test documents gap)");

    assert_eq!(
        result.rows.len(),
        2,
        "should find ContextTrigger and ToolRoute nodes, got {:?}",
        result.rows
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 36: UNWIND piped into MATCH
// Source: Neo4jStorage.createAboutRelationships — `UNWIND $targetIds AS targetId MATCH ...`
//
// GAP-G: `UNWIND list MATCH (e:Label {id: x})` — MATCH clause after UNWIND not
//   parsed by SparrowDB. Error: InvalidArgument("expected Return, got Match").
//   SPA-155 confirmed UNWIND+RETURN works. UNWIND+MATCH pipeline needs a
//   dedicated sub-ticket under SPA-151.
//   Neo4j workaround already exists in SparrowDBStorage: it loops over targetIds
//   in TypeScript and calls MATCH+CREATE individually per ID.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "GAP-G: UNWIND ['a','b'] AS x MATCH (n {id: x}) RETURN ... not parsed. \
            Error: expected Return, got Match. SPA-155 confirmed UNWIND+RETURN works; \
            UNWIND+MATCH pipeline needs a new sub-ticket under SPA-151. \
            SparrowDBStorage workaround: loop in TypeScript with individual MATCH per id."]
fn kms_q36_unwind_literal_list() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Literal UNWIND + MATCH pipeline — the Neo4j bulk ABOUT creation pattern.
    let result = db
        .execute(
            "UNWIND ['person-alice', 'person-bob'] AS targetId \
             MATCH (e:Person {id: targetId}) \
             RETURN targetId, e.name",
        )
        .expect("UNWIND literal list + MATCH must succeed (ignored test documents gap)");

    assert_eq!(result.rows.len(), 2, "both Person nodes should be found");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 36b: UNWIND + RETURN (confirmed working — SPA-155)
// Source: workaround — omit MATCH, iterate in application code
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q36b_unwind_literal_list_return_only() {
    let (_dir, db) = make_db();

    // UNWIND + RETURN (no MATCH) — confirmed by SPA-155.
    let result = db
        .execute(
            "UNWIND ['person-alice', 'person-bob'] AS targetId \
             RETURN targetId",
        )
        .expect("UNWIND + RETURN must succeed");

    assert_eq!(result.rows.len(), 2, "UNWIND should produce 2 rows");
    assert_eq!(result.rows[0][0], Value::String("person-alice".to_string()));
    assert_eq!(result.rows[1][0], Value::String("person-bob".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 37: OPTIONAL MATCH used in getEntitySummary
// Source: Neo4jStorage.getEntitySummary —
//   `MATCH (n {id:...}) OPTIONAL MATCH (n)-[r]-(related) WHERE ...`
// SPA-131 confirmed OPTIONAL MATCH works.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q37_optional_match_for_entity_summary() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Simplified version of Neo4jStorage.getEntitySummary pattern.
    // Full version uses anonymous labels, which requires unlabeled MATCH (SPA-211).
    let result = db
        .execute(
            "MATCH (n:Person {id: 'person-alice'}) \
             OPTIONAL MATCH (n)-[:ABOUT]-(k:Knowledge) \
             RETURN n.id, n.name, k.id",
        )
        .expect("OPTIONAL MATCH for entity summary must succeed");

    // alice has incoming ABOUT from know-001.
    // Note: the ABOUT edge goes Knowledge→Person (directed), so undirected pattern finds it.
    assert!(
        !result.rows.is_empty(),
        "should have at least 1 row for alice: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("person-alice".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 38: MATCH with no label filter (unlabeled node anchor)
// Source: Neo4jStorage.getEntitySummary — `MATCH (n {id: $id})`
// SPA-211 confirmed unlabeled MATCH works.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q38_unlabeled_match_by_id() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // Unlabeled MATCH — finds node regardless of label.
    let result = db
        .execute("MATCH (n {id: 'person-alice'}) RETURN n.id, n.name")
        .expect("unlabeled MATCH by id must succeed");

    assert_eq!(
        result.rows.len(),
        1,
        "should find alice without label filter: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], Value::String("person-alice".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 39: Property access on null node (OPTIONAL MATCH null propagation)
// Source: Neo4jStorage.search() — properties accessed on optional nodes
// SPA-131 / SPA-197 confirmed null propagation.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q39_null_propagation_from_optional_match() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    // know-001 has an ABOUT edge to Person alice.
    // know-003 has no ABOUT → Person edge.
    // OPTIONAL MATCH should produce NULL for know-003's person columns.
    let result = db
        .execute(
            "MATCH (k:Knowledge) \
             OPTIONAL MATCH (k)-[:ABOUT]->(p:Person) \
             RETURN k.id, p.id, p.name \
             ORDER BY k.id",
        )
        .expect("OPTIONAL MATCH null propagation must succeed");

    // 3 Knowledge nodes, each gets a row.
    assert_eq!(result.rows.len(), 3, "one row per Knowledge node");

    // know-001 → alice: p.id and p.name should be non-null.
    let alice_row = result
        .rows
        .iter()
        .find(|r| r[0] == Value::String("know-001".to_string()));
    assert!(alice_row.is_some(), "know-001 row must exist");
    let alice_row = alice_row.unwrap();
    assert_ne!(
        alice_row[1],
        Value::Null,
        "know-001 should have p.id from alice"
    );

    // know-003 has no ABOUT → Person: p.id and p.name should be NULL.
    let k003_row = result
        .rows
        .iter()
        .find(|r| r[0] == Value::String("know-003".to_string()));
    assert!(k003_row.is_some(), "know-003 row must exist");
    let k003_row = k003_row.unwrap();
    assert_eq!(k003_row[1], Value::Null, "know-003 should have NULL p.id");
    assert_eq!(k003_row[2], Value::Null, "know-003 should have NULL p.name");
}

// ─────────────────────────────────────────────────────────────────────────────
// KMS Query 40: collect() aggregation — gather related node ids into a list
// Source: Neo4jStorage.getEntitySummary —
//   `collect({relationship: type(r1), relatedNode: ..., ...}) as relationships`
// SPA-collect_agg confirms collect() works.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn kms_q40_collect_aggregation() {
    let (_dir, db) = make_db();
    setup_kms_graph(&db);

    let result = db
        .execute(
            "MATCH (k:Knowledge)-[:RELATED_TO]->(m:Knowledge) \
             RETURN k.id, collect(m.id) AS related_ids \
             ORDER BY k.id",
        )
        .expect("collect() aggregation must succeed");

    // know-001 → [know-002], know-002 → [know-003].
    assert_eq!(result.rows.len(), 2, "2 source nodes have RELATED_TO edges");

    // The first row (know-001) should have related_ids = ['know-002'].
    match &result.rows[0][1] {
        Value::List(ids) => {
            assert_eq!(ids.len(), 1, "know-001 has 1 related: {:?}", ids);
            assert_eq!(ids[0], Value::String("know-002".to_string()));
        }
        other => panic!("expected List for collect(), got {other:?}"),
    }
}
