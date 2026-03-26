//! Integration tests for LDBC SNB IC3-IC14 queries (SPA-111 phase 2).
//!
//! Uses the mini LDBC fixture data loaded via the sparrowdb-bench loader.
//! Each test asserts non-empty results on the synthetic dataset.

use sparrowdb::GraphDb;
use sparrowdb_bench::ic_queries;
use std::path::PathBuf;

fn load_mini_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sparrowdb::open(dir.path()).expect("open db");

    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("sparrowdb-bench")
        .join("fixtures")
        .join("ldbc")
        .join("mini");

    let stats = sparrowdb_bench::load(&db, &fixture_dir).expect("load mini LDBC data");
    assert!(stats.nodes_loaded > 0, "should load at least one node");
    assert!(stats.edges_loaded > 0, "should load at least one edge");

    db.checkpoint().expect("checkpoint after load");
    (dir, db)
}

// ── IC3 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic3_friends_in_country() {
    let (_dir, db) = load_mini_db();

    // Alice (ldbc_id=1) knows Bob (UK) and Carol (US).
    // Person 1 is located in US, person 2 in UK.
    let results =
        ic_queries::ic3_friends_in_countries(&db, 1, "United Kingdom", "Germany", 30).unwrap();

    // Bob (ldbc_id=2) is in UK and is a direct friend of Alice.
    assert!(
        !results.is_empty(),
        "IC3 should find friends in United Kingdom; got empty"
    );
}

// ── IC4 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic4_top_tags_of_friend_posts() {
    let (_dir, db) = load_mini_db();

    // Alice (1) knows Bob (2) and Carol (3).
    // Bob created post 2 tagged with Rust.
    // Carol created post 3 tagged with SocialNetworks.
    let results = ic_queries::ic4_top_tags(&db, 1, "2010-01-01", 365).unwrap();

    assert!(
        !results.is_empty(),
        "IC4 should find tags on friend posts; got empty"
    );
    // Verify tag names are strings
    for (name, count) in &results {
        assert!(!name.is_empty(), "tag name should not be empty");
        assert!(*count > 0, "count should be positive");
    }
}

// ── IC5 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic5_forums_with_friends() {
    let (_dir, db) = load_mini_db();

    // Alice (1) knows Bob (2) and Carol (3).
    // Forum 1 "Graph Databases" has members: 1 (Alice), 2 (Bob), 3 (Carol).
    // Forum 2 has member 4 (Dave), 5 (Eve) — not Alice's direct friends.
    let results = ic_queries::ic5_forums_with_friends(&db, 1, "2010-01-01").unwrap();

    assert!(
        !results.is_empty(),
        "IC5 should find forums with friends; got empty"
    );
}

// ── IC6 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic6_tag_co_occurrence() {
    let (_dir, db) = load_mini_db();

    // Alice (1) knows Bob (2) and Carol (3).
    // Bob created post 2 tagged with Rust.
    // Carol created post 3 tagged with SocialNetworks.
    // Exclude "Rust" — should find other tags on friend posts.
    let results = ic_queries::ic6_tag_co_occurrence(&db, 1, "Rust").unwrap();

    assert!(
        !results.is_empty(),
        "IC6 should find co-occurring tags; got empty"
    );
    for (name, _count) in &results {
        assert_ne!(name, "Rust", "should not include the excluded tag");
    }
}

// ── IC7 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic7_latest_likes() {
    let (_dir, db) = load_mini_db();

    // Alice (1) created posts 1 and 4.
    // Post 1 was liked by Bob (2) and Carol (3).
    let results = ic_queries::ic7_latest_likes(&db, 1).unwrap();

    assert!(
        !results.is_empty(),
        "IC7 should find likers of person 1's posts; got empty"
    );
}

// ── IC8 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic8_replies() {
    let (_dir, db) = load_mini_db();

    // Alice (1) created posts 1 and 4.
    // Comment 1 replies to post 1, created by Bob (2).
    // Comment 4 replies to post 4, created by Dave (4).
    let results = ic_queries::ic8_replies(&db, 1).unwrap();

    assert!(
        !results.is_empty(),
        "IC8 should find commenters on person 1's posts; got empty"
    );
}

// ── IC9 ─────────────────────────────────────────────────────────────────────

#[test]
fn ic9_recent_posts_by_friends() {
    let (_dir, db) = load_mini_db();

    // Alice (1) knows Bob (2) and Carol (3).
    // Bob created post 2 "Rust is amazing".
    // Carol created post 3 "Social networks are complex".
    let results = ic_queries::ic9_recent_posts_by_friends(&db, 1, "2012-01-01").unwrap();

    assert!(
        !results.is_empty(),
        "IC9 should find posts by friends; got empty"
    );
}

// ── IC10 ────────────────────────────────────────────────────────────────────

#[test]
fn ic10_friend_recommendations() {
    let (_dir, db) = load_mini_db();

    // Alice (1) -> Bob (2) -> Dave (4).
    // Alice (1) -> Carol (3) -> Dave (4).
    // Dave is 2 hops from Alice and should appear as a recommendation.
    let results = ic_queries::ic10_friend_recommendations(&db, 1, 6).unwrap();

    assert!(
        !results.is_empty(),
        "IC10 should find friend-of-friend recommendations; got empty"
    );
}

// ── IC11 ────────────────────────────────────────────────────────────────────

#[test]
fn ic11_job_referral() {
    let (_dir, db) = load_mini_db();

    // Alice (1) knows Bob (2). Bob works at Acme Corp (org 1) in United States.
    // Alice (1) -> Bob (2) -> Dave (4). Dave works at TechStart (org 3) in Germany.
    let results = ic_queries::ic11_job_referral(&db, 1, "United States", 2005).unwrap();

    assert!(
        !results.is_empty(),
        "IC11 should find friends working in US; got empty"
    );
}

// ── IC12 ────────────────────────────────────────────────────────────────────

#[test]
fn ic12_expert_search() {
    let (_dir, db) = load_mini_db();

    // Alice (1) knows Bob (2). Bob created post 2 tagged with Rust (tag 3).
    // Tag 3 (Rust) has type TagClass 3 (Technology).
    let results = ic_queries::ic12_expert_search(&db, 1, "Technology").unwrap();

    assert!(
        !results.is_empty(),
        "IC12 should find experts in Technology tag class; got empty"
    );
}

// ── IC13 ────────────────────────────────────────────────────────────────────

#[test]
fn ic13_shortest_path_direct() {
    let (_dir, db) = load_mini_db();

    // Alice (1) -> Bob (2): direct edge, distance = 1.
    let dist = ic_queries::ic13_shortest_path(&db, 1, 2).unwrap();
    assert_eq!(dist, 1, "direct friends should have shortest path 1");
}

#[test]
fn ic13_shortest_path_two_hops() {
    let (_dir, db) = load_mini_db();

    // Alice (1) -> Bob (2) -> Dave (4): distance = 2.
    let dist = ic_queries::ic13_shortest_path(&db, 1, 4).unwrap();
    assert_eq!(dist, 2, "two-hop path should have shortest path 2");
}

// ── IC14 ────────────────────────────────────────────────────────────────────

#[test]
fn ic14_weighted_path() {
    let (_dir, db) = load_mini_db();

    // Alice (1) -> Bob (2): direct edge.
    let path = ic_queries::ic14_weighted_path(&db, 1, 2).unwrap();
    assert!(!path.is_empty(), "IC14 should return non-empty path");
    assert_eq!(path[0], 1, "direct path should have length 1");
}
