//! Regression test for GitHub issue #294:
//! 2-hop reverse arrow pattern (a)-[:REL]->(b)<-[:REL]-(c) returns 0 rows
//! when the src/mid/fof labels differ (e.g. Person → Movie ← Person).
//!
//! The bug: `hop2_rel_ids` filters rel tables as (src=mid, dst=fof), but for
//! an incoming second hop the catalog stores edges as (src=fof, dst=mid).
//! When mid and fof have the same label the filter accidentally works; with
//! different labels it yields an empty set → zero results.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn collect_strings(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .filter_map(|row| match row.first() {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ── Test: co-actor pattern with heterogeneous labels ─────────────────────────
//
// Graph:  (Tom:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(coactor:Person)
//
//   Tom --ACTED_IN--> Matrix <--ACTED_IN-- Keanu
//   Tom --ACTED_IN--> Matrix <--ACTED_IN-- Carrie
//
// Expected: coactor names include "Keanu" and "Carrie"

#[test]
fn reverse_arrow_cross_label_coactor() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Person {name: 'Tom'})").expect("Tom");
    db.execute("CREATE (:Person {name: 'Keanu'})")
        .expect("Keanu");
    db.execute("CREATE (:Person {name: 'Carrie'})")
        .expect("Carrie");
    db.execute("CREATE (:Movie {title: 'Matrix'})")
        .expect("Matrix");

    db.execute(
        "MATCH (p:Person {name:'Tom'}), (m:Movie {title:'Matrix'}) CREATE (p)-[:ACTED_IN]->(m)",
    )
    .expect("Tom->Matrix");
    db.execute(
        "MATCH (p:Person {name:'Keanu'}), (m:Movie {title:'Matrix'}) CREATE (p)-[:ACTED_IN]->(m)",
    )
    .expect("Keanu->Matrix");
    db.execute(
        "MATCH (p:Person {name:'Carrie'}), (m:Movie {title:'Matrix'}) CREATE (p)-[:ACTED_IN]->(m)",
    )
    .expect("Carrie->Matrix");

    let result = db
        .execute(
            "MATCH (tom:Person {name:'Tom'})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(coactor:Person) \
             RETURN coactor.name",
        )
        .expect("co-actor query");

    let mut names = collect_strings(&result.rows);
    names.sort();

    assert!(
        !names.is_empty(),
        "#294: reverse arrow cross-label pattern returned 0 rows"
    );
    // Tom should NOT appear as his own co-actor (self-loop), but Keanu and Carrie should.
    // NOTE: if the engine doesn't filter self-loops, Tom may appear — that's a separate issue.
    assert!(
        names.contains(&"Keanu".to_string()),
        "#294: expected Keanu in co-actors, got {:?}",
        names
    );
    assert!(
        names.contains(&"Carrie".to_string()),
        "#294: expected Carrie in co-actors, got {:?}",
        names
    );
}

// ── Test: single co-actor, minimal graph ─────────────────────────────────────
//
// Graph:  Alice --WORKS_AT--> Acme <--WORKS_AT-- Bob
//
// Expected: MATCH (a:Employee {name:'Alice'})-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Employee)
//           RETURN b.name  →  ["Bob"]

#[test]
fn reverse_arrow_cross_label_minimal() {
    let (_dir, db) = make_db();

    db.execute("CREATE (:Employee {name: 'Alice'})")
        .expect("Alice");
    db.execute("CREATE (:Employee {name: 'Bob'})").expect("Bob");
    db.execute("CREATE (:Company {name: 'Acme'})")
        .expect("Acme");

    db.execute(
        "MATCH (e:Employee {name:'Alice'}), (c:Company {name:'Acme'}) CREATE (e)-[:WORKS_AT]->(c)",
    )
    .expect("Alice->Acme");
    db.execute(
        "MATCH (e:Employee {name:'Bob'}), (c:Company {name:'Acme'}) CREATE (e)-[:WORKS_AT]->(c)",
    )
    .expect("Bob->Acme");

    let result = db
        .execute(
            "MATCH (a:Employee {name:'Alice'})-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Employee) \
             RETURN b.name",
        )
        .expect("colleague query");

    let names = collect_strings(&result.rows);
    assert!(
        names.contains(&"Bob".to_string()),
        "#294: expected Bob in colleagues, got {:?}",
        names
    );
}
