//! Integration tests for LDBC SNB IC queries — SPA-146.

use sparrowdb::GraphDb;
use sparrowdb_bench::ic_queries::{self, PersonName};
use sparrowdb_bench::load;
use std::path::PathBuf;

fn fixture_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("fixtures/ldbc/mini");
    p
}

fn db_with_mini_fixture() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = GraphDb::open(dir.path()).expect("open GraphDb");
    load(&db, &fixture_dir()).expect("load mini fixture");
    db.checkpoint().expect("checkpoint after load");
    (dir, db)
}

#[test]
fn ic1_returns_friends_of_alice_within_3_hops() {
    let (_dir, db) = db_with_mini_fixture();
    let results = ic_queries::ic1_friends_named(&db, "Alice").expect("IC1 should not error");

    assert!(
        !results.is_empty(),
        "IC1: should find at least one friend of Alice; got none"
    );

    let alice_in_results = results.iter().any(|(f, _)| f == "Alice");
    assert!(
        !alice_in_results,
        "IC1: Alice's own name should be excluded from results"
    );

    let last_names: Vec<&str> = results.iter().map(|r: &PersonName| r.1.as_str()).collect();
    let mut sorted = last_names.clone();
    sorted.sort();
    assert_eq!(
        last_names, sorted,
        "IC1: results should be sorted by lastName"
    );

    assert!(
        results.len() <= 20,
        "IC1: LIMIT 20 must be respected; got {}",
        results.len()
    );
}

#[test]
fn ic1_unknown_person_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let results = ic_queries::ic1_friends_named(&db, "Zephyr")
        .expect("IC1 for unknown name should not error");
    assert!(
        results.is_empty(),
        "IC1: no results expected for name not in fixture; got {}",
        results.len()
    );
}

#[test]
fn ic2_returns_direct_friends_of_alice() {
    let (_dir, db) = db_with_mini_fixture();
    let results = ic_queries::ic2_recent_friends(&db, 1).expect("IC2 should not error");

    assert!(
        !results.is_empty(),
        "IC2: Alice (id=1) should have at least one friend; got none"
    );

    assert_eq!(
        results.len(),
        3,
        "IC2: Alice should have 3 direct friends, got {}",
        results.len()
    );

    let first_names: Vec<&str> = results.iter().map(|r: &PersonName| r.0.as_str()).collect();
    assert!(
        first_names.contains(&"Bob"),
        "IC2: Bob should be in Alice's friends"
    );
    assert!(
        first_names.contains(&"Carol"),
        "IC2: Carol should be in Alice's friends"
    );
    assert!(
        first_names.contains(&"Frank"),
        "IC2: Frank should be in Alice's friends"
    );

    assert!(results.len() <= 20, "IC2: LIMIT 20 must be respected");
}

#[test]
fn ic2_unknown_person_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let results =
        ic_queries::ic2_recent_friends(&db, 9999).expect("IC2 for unknown id should not error");
    assert!(
        results.is_empty(),
        "IC2: unknown person id should return empty; got {}",
        results.len()
    );
}

#[test]
fn ic3_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic3_friends_in_countries(&db, 1, "Germany", "France", 14)
        .expect("IC3 stub should not error");
    assert!(r.is_empty(), "IC3 stub must return empty Vec");
}

#[test]
fn ic4_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic4_top_tags(&db, 1, "2012-01-01", 30).expect("IC4 stub should not error");
    assert!(r.is_empty(), "IC4 stub must return empty Vec");
}

#[test]
fn ic5_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic5_forums_with_friends(&db, 1, "2012-01-01")
        .expect("IC5 stub should not error");
    assert!(r.is_empty(), "IC5 stub must return empty Vec");
}

#[test]
fn ic6_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic6_tag_co_occurrence(&db, 1, "Music").expect("IC6 stub should not error");
    assert!(r.is_empty(), "IC6 stub must return empty Vec");
}

#[test]
fn ic7_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic7_latest_likes(&db, 1).expect("IC7 stub should not error");
    assert!(r.is_empty(), "IC7 stub must return empty Vec");
}

#[test]
fn ic8_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic8_replies(&db, 1).expect("IC8 stub should not error");
    assert!(r.is_empty(), "IC8 stub must return empty Vec");
}

#[test]
fn ic9_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic9_recent_posts_by_friends(&db, 1, "2012-01-01")
        .expect("IC9 stub should not error");
    assert!(r.is_empty(), "IC9 stub must return empty Vec");
}

#[test]
fn ic10_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic10_friend_recommendations(&db, 1, 3).expect("IC10 stub should not error");
    assert!(r.is_empty(), "IC10 stub must return empty Vec");
}

#[test]
fn ic11_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r =
        ic_queries::ic11_job_referral(&db, 1, "Germany", 2005).expect("IC11 stub should not error");
    assert!(r.is_empty(), "IC11 stub must return empty Vec");
}

#[test]
fn ic12_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic12_expert_search(&db, 1, "Writer").expect("IC12 stub should not error");
    assert!(r.is_empty(), "IC12 stub must return empty Vec");
}

#[test]
fn ic13_stub_returns_negative_one() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic13_shortest_path(&db, 1, 10).expect("IC13 stub should not error");
    assert_eq!(r, -1, "IC13 stub should return -1 (not-found sentinel)");
}

#[test]
fn ic14_stub_returns_empty() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic14_weighted_path(&db, 1, 10).expect("IC14 stub should not error");
    assert!(r.is_empty(), "IC14 stub must return empty Vec");
}
