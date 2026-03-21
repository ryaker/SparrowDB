//! Binder tests — RED phase: must fail before Phase 4a implementation.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_cypher::binder::bind;
use sparrowdb_cypher::parser::parse;

fn make_catalog() -> (tempfile::TempDir, Catalog) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cat = Catalog::open(dir.path()).expect("catalog open");
    cat.create_label("Person").expect("Person");
    cat.create_label("Knowledge").expect("Knowledge");
    cat.create_label("Technology").expect("Technology");
    let person_id = cat.get_label("Person").unwrap().unwrap();
    cat.create_rel_table(person_id, person_id, "KNOWS")
        .expect("KNOWS");
    let knowledge_id = cat.get_label("Knowledge").unwrap().unwrap();
    let tech_id = cat.get_label("Technology").unwrap().unwrap();
    cat.create_rel_table(knowledge_id, tech_id, "ABOUT")
        .expect("ABOUT");
    (dir, cat)
}

#[test]
fn bind_match_known_label_succeeds() {
    let (_dir, cat) = make_catalog();
    let stmt = parse("MATCH (n:Person) RETURN n.name").expect("parse");
    bind(stmt, &cat).expect("bind must succeed for known label");
}

#[test]
fn bind_match_unknown_label_fails() {
    let (_dir, cat) = make_catalog();
    let stmt = parse("MATCH (n:Ghost) RETURN n.name").expect("parse");
    let result = bind(stmt, &cat);
    assert!(result.is_err(), "Ghost label must fail binder");
}

#[test]
fn bind_create_known_label_succeeds() {
    let (_dir, cat) = make_catalog();
    let stmt = parse("CREATE (n:Person {name: \"Alice\"})").expect("parse");
    bind(stmt, &cat).expect("bind must succeed for known label");
}

#[test]
fn bind_match_1hop_known_rel_succeeds() {
    let (_dir, cat) = make_catalog();
    let stmt = parse("MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(f:Person) RETURN f.name")
        .expect("parse");
    bind(stmt, &cat).expect("bind 1-hop KNOWS must succeed");
}

#[test]
fn bind_match_unknown_rel_type_fails() {
    let (_dir, cat) = make_catalog();
    let stmt = parse("MATCH (a:Person)-[:HATES]->(b:Person) RETURN b.name").expect("parse");
    let result = bind(stmt, &cat);
    assert!(result.is_err(), "unknown rel type HATES must fail binder");
}

#[test]
fn bind_match_2hop_succeeds() {
    let (_dir, cat) = make_catalog();
    let stmt = parse(
        "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->()-[:KNOWS]->(fof:Person) \
         RETURN DISTINCT fof.name",
    )
    .expect("parse");
    bind(stmt, &cat).expect("bind 2-hop must succeed");
}
