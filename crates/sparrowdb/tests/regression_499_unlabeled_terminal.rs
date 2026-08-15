//! Regression tests for #499: two-hop MATCH with an unlabeled terminal node.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");

    db.execute("CREATE (:A {name: 'source'})").unwrap();
    db.execute("CREATE (:B {name: 'middle'})").unwrap();
    db.execute("CREATE (:C {name: 'terminal'})").unwrap();
    db.execute(
        "MATCH (a:A {name: 'source'}), (b:B {name: 'middle'}) \
         CREATE (a)-[:R]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:A {name: 'source'}), (b:B {name: 'middle'}) \
         CREATE (a)-[:ALT]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:B {name: 'middle'}), (c:C {name: 'terminal'}) \
         CREATE (b)-[:S]->(c)",
    )
    .unwrap();

    (dir, db)
}

#[test]
fn two_hop_unlabeled_terminal_projects_property() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MATCH (a:A)-[:R]->(b:B)-[:S]->(c) RETURN c.name")
        .expect("two-hop MATCH with unlabeled terminal");

    assert_eq!(result.rows, vec![vec![Value::String("terminal".into())]]);
}

#[test]
fn two_hop_unlabeled_terminal_aggregates_node_binding() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MATCH (a:A)-[:R]->(b:B)-[:S]->(c) RETURN count(c)")
        .expect("aggregate over unlabeled terminal");

    assert_eq!(result.rows, vec![vec![Value::Int64(1)]]);
}

#[test]
fn two_hop_unlabeled_terminal_supports_relationship_predicate() {
    let (_dir, db) = make_db();

    let result = db
        .execute(
            "MATCH (a:A)-[r:R]->(b:B)-[:S]->(c) \
             WHERE type(r) = 'R' RETURN c.name",
        )
        .expect("relationship predicate with unlabeled terminal");

    assert_eq!(result.rows, vec![vec![Value::String("terminal".into())]]);
}

#[test]
fn two_hop_unlabeled_terminal_exposes_resolved_label_metadata() {
    let (_dir, db) = make_db();

    let result = db
        .execute("MATCH (a:A)-[:R]->(b:B)-[:S]->(c) RETURN labels(c)")
        .expect("label metadata for unlabeled terminal");

    assert_eq!(
        result.rows,
        vec![vec![Value::List(vec![Value::String("C".into())])]]
    );
}

#[test]
fn two_hop_unlabeled_terminal_resolves_untyped_relationship_metadata() {
    let (_dir, db) = make_db();

    let result = db
        .execute(
            "MATCH (a:A)-[r]->(b:B)-[:S]->(c) \
             RETURN type(r)",
        )
        .expect("catalog relationship types for untyped pattern");

    let mut rel_types: Vec<String> = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::String(value) => value.clone(),
            other => panic!("expected relationship type string, got {other:?}"),
        })
        .collect();
    rel_types.sort();
    assert_eq!(rel_types, vec!["ALT".to_string(), "R".to_string()]);
}

#[test]
fn two_hop_unlabeled_terminal_counts_distinct_relationship_bindings() {
    let (_dir, db) = make_db();
    db.execute("CHECKPOINT").expect("checkpoint");

    let result = db
        .execute("MATCH (a:A)-[r]->(b:B)-[:S]->(c) RETURN count(r)")
        .expect("aggregate over untyped relationship binding");

    assert_eq!(result.rows, vec![vec![Value::Int64(2)]]);
}
