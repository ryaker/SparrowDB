//! UC-1 Social Graph integration tests.
//!
//! Acceptance check #3: single_reader_match_person_names
//! Acceptance check #4: two_hop_match_via_binary_asp_join

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};

/// Build a small social graph in a temp directory and return the engine.
fn setup_social_graph(dir: &std::path::Path) -> Engine {
    let mut cat = Catalog::open(dir).expect("catalog");
    let person_id = cat.create_label("Person").expect("Person") as u32;
    // KNOWS rel table: Person->Person
    cat.create_rel_table(person_id as u16, person_id as u16, "KNOWS")
        .expect("KNOWS rel table");

    let mut store = NodeStore::open(dir).expect("node store");
    // col 0 = name (stored as i64 id: 1=Alice, 2=Bob, 3=Carol, 4=Dave, 5=Eve)
    // col 1 = age
    let alice = store
        .create_node(
            person_id,
            &[(0, StoreValue::Int64(1)), (1, StoreValue::Int64(30))],
        )
        .expect("Alice");
    let bob = store
        .create_node(
            person_id,
            &[(0, StoreValue::Int64(2)), (1, StoreValue::Int64(25))],
        )
        .expect("Bob");
    let carol = store
        .create_node(
            person_id,
            &[(0, StoreValue::Int64(3)), (1, StoreValue::Int64(35))],
        )
        .expect("Carol");
    let dave = store
        .create_node(
            person_id,
            &[(0, StoreValue::Int64(4)), (1, StoreValue::Int64(28))],
        )
        .expect("Dave");
    let _eve = store
        .create_node(
            person_id,
            &[(0, StoreValue::Int64(5)), (1, StoreValue::Int64(22))],
        )
        .expect("Eve");

    // Edges: Alice->Bob, Alice->Carol, Bob->Dave, Carol->Dave, Carol->Eve
    let alice_slot = alice.0 & 0xFFFF_FFFF;
    let bob_slot = bob.0 & 0xFFFF_FFFF;
    let carol_slot = carol.0 & 0xFFFF_FFFF;
    let dave_slot = dave.0 & 0xFFFF_FFFF;

    let edges = vec![
        (alice_slot, bob_slot),
        (alice_slot, carol_slot),
        (bob_slot, dave_slot),
        (carol_slot, dave_slot),
        (carol_slot, 4u64), // Carol -> Eve (slot 4)
    ];

    let fwd = CsrForward::build(5, &edges);
    let fwd_path = dir.join("edges").join("0").join("base.fwd.csr");
    std::fs::create_dir_all(fwd_path.parent().unwrap()).unwrap();
    fwd.write(&fwd_path).expect("write CSR");

    Engine::new(store, cat, fwd, dir)
}

/// Acceptance check #3: 1-hop MATCH friend lookup.
#[test]
fn single_reader_match_person_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_social_graph(dir.path());

    // This is the acceptance check query: MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name
    // Since names are stored as i64, we check that we get the right count.
    let result = engine
        .execute("MATCH (a:Person {col_0: 1})-[:KNOWS]->(f:Person) RETURN f.col_0")
        .expect("execute 1-hop");

    // Alice (col_0=1) knows Bob(2) and Carol(3)
    assert_eq!(result.rows.len(), 2, "Alice should have 2 friends");
    let mut names: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            sparrowdb_execution::types::Value::Int64(v) => *v,
            _ => panic!("expected Int64"),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec![2, 3], "friends should be Bob(2) and Carol(3)");
}

/// Acceptance check #4: 2-hop friend-of-friend via binary ASP-Join.
#[test]
fn two_hop_match_via_binary_asp_join() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = setup_social_graph(dir.path());

    // 2-hop: friends of Alice's friends excluding direct friends of Alice
    let result = engine
        .execute(
            "MATCH (a:Person {col_0: 1})-[:KNOWS]->()-[:KNOWS]->(fof:Person) \
             RETURN DISTINCT fof.col_0",
        )
        .expect("execute 2-hop");

    // Alice -> {Bob, Carol}
    // Bob -> {Dave}
    // Carol -> {Dave, Eve}
    // fof = {Dave(4), Eve(5)} (distinct)
    // (Note: we don't filter direct friends in this simplified query)
    let mut fof: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            sparrowdb_execution::types::Value::Int64(v) => *v,
            _ => panic!("expected Int64"),
        })
        .collect();
    fof.sort();
    assert_eq!(fof, vec![4, 5], "fof should be Dave(4) and Eve(5)");
}
