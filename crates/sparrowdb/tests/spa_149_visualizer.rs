//! E2E tests for SPA-149: DOT / Graphviz export from a SparrowDB graph.
//!
//! `GraphDb::export_dot()` walks all nodes and relationships via Cypher and
//! emits a `digraph` in the DOT language suitable for piping through
//! `dot -Tsvg` or saving to a `.dot` file.

use sparrowdb::open;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

// ── Test 1: empty graph produces a valid (empty) digraph ─────────────────────

/// An empty database must still emit a syntactically correct DOT header and
/// footer without panicking or returning an error.
#[test]
fn export_dot_empty_graph_is_valid() {
    let (_dir, db) = make_db();

    let dot = db
        .export_dot()
        .expect("export_dot must not fail on empty db");

    assert!(
        dot.contains("digraph"),
        "DOT output must contain 'digraph'; got:\n{dot}"
    );
    assert!(
        dot.trim_end().ends_with('}'),
        "DOT output must end with '}}'; got:\n{dot}"
    );
}

// ── Test 2: nodes appear with their label and name ────────────────────────────

/// After inserting two Person nodes the DOT output must reference both names.
#[test]
fn export_dot_nodes_contain_label_and_name() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");

    let dot = db.export_dot().expect("export_dot must succeed");

    assert!(
        dot.contains("Alice"),
        "DOT must mention 'Alice'; got:\n{dot}"
    );
    assert!(dot.contains("Bob"), "DOT must mention 'Bob'; got:\n{dot}");
    assert!(
        dot.contains("Person"),
        "DOT must mention label 'Person'; got:\n{dot}"
    );
}

// ── Test 3: relationships appear with their type ──────────────────────────────

/// After creating a KNOWS edge the DOT output must include an arrow and the
/// "KNOWS" edge label.
#[test]
fn export_dot_produces_valid_output() {
    let (_dir, db) = make_db();

    db.execute("CREATE (a:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (b:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person{name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("CREATE KNOWS edge");

    let dot = db.export_dot().expect("export_dot must succeed");

    // Must be a digraph.
    assert!(dot.contains("digraph"), "must contain 'digraph'");
    // Both node names must appear.
    assert!(dot.contains("Alice"), "must contain 'Alice'");
    assert!(dot.contains("Bob"), "must contain 'Bob'");
    // Edge type must appear as an edge label.
    assert!(dot.contains("KNOWS"), "must contain 'KNOWS'");
    // Must have at least one directed arrow.
    assert!(dot.contains("->"), "must contain '->' for directed edges");
}

// ── Test 4: multiple relationship types all appear ────────────────────────────

/// A graph with KNOWS and LIKES edges must include both type strings.
#[test]
fn export_dot_multiple_rel_types() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Person {name: 'Bob'})")
        .expect("CREATE Bob");
    db.execute("CREATE (n:Person {name: 'Carol'})")
        .expect("CREATE Carol");
    db.execute("MATCH (a:Person {name:'Alice'}),(b:Person{name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")
        .expect("CREATE KNOWS");
    db.execute("MATCH (a:Person {name:'Alice'}),(c:Person{name:'Carol'}) CREATE (a)-[:LIKES]->(c)")
        .expect("CREATE LIKES");

    let dot = db.export_dot().expect("export_dot must succeed");

    assert!(dot.contains("KNOWS"), "DOT must contain 'KNOWS'");
    assert!(dot.contains("LIKES"), "DOT must contain 'LIKES'");
}

// ── Test 5: multiple label types all appear ───────────────────────────────────

/// Nodes with different labels (Person, Company) should both be present.
#[test]
fn export_dot_mixed_labels() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Person {name: 'Alice'})")
        .expect("CREATE Alice");
    db.execute("CREATE (n:Company {name: 'Acme'})")
        .expect("CREATE Acme");
    db.execute(
        "MATCH (p:Person {name:'Alice'}),(c:Company{name:'Acme'}) CREATE (p)-[:WORKS_AT]->(c)",
    )
    .expect("CREATE WORKS_AT");

    let dot = db.export_dot().expect("export_dot must succeed");

    assert!(dot.contains("Person"), "DOT must contain label 'Person'");
    assert!(dot.contains("Company"), "DOT must contain label 'Company'");
    assert!(dot.contains("WORKS_AT"), "DOT must contain 'WORKS_AT'");
}

// ── Test 6: DOT node identifiers are stable identifiers (prefixed with 'n') ──

/// Every node in the DOT output should use the `nXXX` identifier pattern.
#[test]
fn export_dot_node_ids_are_prefixed() {
    let (_dir, db) = make_db();

    db.execute("CREATE (n:Widget {name: 'Sprocket'})")
        .expect("CREATE Sprocket");

    let dot = db.export_dot().expect("export_dot must succeed");

    // The node declaration should look like: n<integer> [label="..."]
    assert!(
        dot.contains("n0")
            || dot
                .lines()
                .any(|l: &str| l.trim_start().starts_with('n') && l.contains("[label=")),
        "DOT nodes must use 'nXXX' identifiers; got:\n{dot}"
    );
}
