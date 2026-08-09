//! Integration tests for LDBC SNB IC3-IC14 queries (SPA-111 phase 2).
//!
//! Uses the mini LDBC fixture data loaded via the sparrowdb-bench loader.
//!
//! Most tests assert non-empty results on the synthetic dataset, but non-empty
//! is not the goal — correctness is. IC6 asserts *empty*, because empty is the
//! right answer there and asserting otherwise is what let #422 hide.

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

// Fixture facts used by IC3/IC11 below, read by hand out of
// `crates/sparrowdb-bench/fixtures/ldbc/mini` — never captured from output.
//
// person_knows_person_0_0.csv (directed; the loader creates one edge per row):
//   1→2, 1→3, 1→6, 2→4, 2→5, 3→4, 3→5, 4→6, 5→7, 6→7, 7→8, 8→9, 9→10
// So from Alice (1): depth 1 = {2, 3, 6}; depth 2 = {4, 5, 7}.
// `knows*1..2` therefore binds {2, 3, 4, 5, 6, 7}.
//
// person_isLocatedIn_place_0_0.csv, with place 1 = United States,
// 2 = United Kingdom, 3 = Germany:
//   1→1, 2→2, 3→1, 4→3, 5→2, 6→3, 7→1, 8→2, 9→3, 10→1
//
// person_0_0.csv names: 1 Alice Smith, 2 Bob Jones, 3 Carol White,
//   4 Dave Brown, 5 Eve Davis, 6 Frank Miller, 7 Grace Wilson.

#[test]
fn ic3_friends_in_country() {
    let (_dir, db) = load_mini_db();

    // Alice's 1..2-hop friends are {2, 3, 4, 5, 6, 7}.
    // In the United Kingdom (place 2): persons 2 (Bob Jones) and 5 (Eve Davis).
    // The query is `RETURN DISTINCT friend.firstName, friend.lastName
    // ORDER BY friend.lastName`, so Davis precedes Jones.
    let results =
        ic_queries::ic3_friends_in_countries(&db, 1, "United Kingdom", "Germany", 30).unwrap();

    assert_eq!(
        results,
        vec![
            ("Eve".to_string(), "Davis".to_string()),
            ("Bob".to_string(), "Jones".to_string()),
        ],
        "IC3(1, United Kingdom): person 2 (Bob Jones, depth 1) and person 5 \
         (Eve Davis, depth 2) are the UK residents within 2 hops of Alice; got {results:?}"
    );
}

/// #421 regression: the depth-2 match must survive the trailing `isLocatedIn`
/// hop.  Before the fix this returned only `[Frank Miller]` — Dave Brown sits
/// at depth 2 (1→2→4 and 1→3→4) and was silently dropped.
#[test]
fn ic3_friends_in_germany_includes_depth_two_match() {
    let (_dir, db) = load_mini_db();

    // Germany is place 3; its residents are persons 4, 6 and 9.
    // Within Alice's 1..2-hop set {2, 3, 4, 5, 6, 7}: person 4 (Dave Brown,
    // depth 2) and person 6 (Frank Miller, depth 1). Person 9 is 4 hops away
    // (1→6→7→8→9).
    // ORDER BY lastName → Brown, Miller.
    let results = ic_queries::ic3_friends_in_countries(&db, 1, "Germany", "France", 14).unwrap();

    assert_eq!(
        results,
        vec![
            ("Dave".to_string(), "Brown".to_string()),
            ("Frank".to_string(), "Miller".to_string()),
        ],
        "IC3(1, Germany): Dave Brown is reached at depth 2 and must not be \
         truncated away by the trailing isLocatedIn hop (#421); got {results:?}"
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

    // IC6 asks which OTHER tags appear on the posts that carry the given tag and
    // were created by the person's friends. It is not "every friend tag except
    // this one" — that was #422, and this test used to assert it: it expected a
    // non-empty result for "Rust" under the comment "should find other tags on
    // friend posts", which is the bug written down as the expectation.
    //
    // Alice (1) knows Bob (2), Carol (3) and Frank (6).
    // Bob created post 2, tagged {Rust}. Carol created post 3, tagged
    // {SocialNetworks}. Frank created nothing.
    //
    // So post 2 is the only friend post carrying Rust, and it carries no other
    // tag — nothing co-occurs. SocialNetworks is on post 3, which is not tagged
    // Rust, so it must not appear.
    let results = ic_queries::ic6_tag_co_occurrence(&db, 1, "Rust").unwrap();
    assert!(
        results.is_empty(),
        "IC6: post 2 is the only friend post tagged Rust and carries no other tag; got {results:?}"
    );

    // A tag no post carries must yield nothing. Before #422 this returned every
    // friend tag, because the tag was only ever excluded from the output and
    // never used to select posts.
    let unknown = ic_queries::ic6_tag_co_occurrence(&db, 1, "ZZZ_nonexistent").unwrap();
    assert!(
        unknown.is_empty(),
        "IC6: a tag absent from the graph must return empty; got {unknown:?}"
    );

    // Liveness guard. Every IC6 expectation this fixture can express is empty:
    // the only multi-tag post is 1 ({Databases, GraphTheory}), it belongs to
    // Alice, and nobody `knows` Alice (person 1 is never a knows target), so
    // post 1 is never anyone's friend post. That makes both assertions above
    // satisfiable by an ic6 that always returns empty. IC4 walks the same
    // friend→post→tag pipeline without the tag restriction, so its non-emptiness
    // proves the empties are the restriction at work, not a dead query.
    // Tracked in #428 — a friend-owned multi-tag post would let IC6 be asserted
    // positively and retire this guard.
    let ic4 = ic_queries::ic4_top_tags(&db, 1, "2010-01-01", 365).unwrap();
    assert!(
        !ic4.is_empty(),
        "IC6 liveness: IC4 shares IC6's friend→post→tag pipeline and must be non-empty, \
         otherwise IC6's empty results prove nothing; got {ic4:?}"
    );
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

// IC11 walks `(p)-[:knows*1..2]->(friend)-[:workAt]->(org)-[:isLocatedIn]->(place)`
// — a variable-length hop followed by *two* fixed hops.
//
// person_workAt_organisation_0_0.csv: 1→org1, 2→org1, 3→org2, 4→org3, 5→org1
// organisation_isLocatedIn_place_0_0.csv: org1→place1, org2→place2, org3→place3
// organisation_0_0.csv: 1 Acme Corp, 2 University of Oxford, 3 TechStart Inc
//
// Alice's 1..2-hop friend set is {2, 3, 4, 5, 6, 7} (see the IC3 notes above).

#[test]
fn ic11_job_referral() {
    let (_dir, db) = load_mini_db();

    // United States is place 1, reached only via org 1 (Acme Corp).
    // Acme employs persons 1, 2 and 5; of those, 2 (Bob Jones, depth 1) and
    // 5 (Eve Davis, depth 2) are in Alice's friend set — Alice herself is not.
    // ORDER BY lastName → Davis, Jones.
    let results = ic_queries::ic11_job_referral(&db, 1, "United States", 2005).unwrap();

    assert_eq!(
        results,
        vec![
            ("Eve".to_string(), "Davis".to_string()),
            ("Bob".to_string(), "Jones".to_string()),
        ],
        "IC11(1, United States): Acme Corp is the only US organisation and \
         employs friends 2 and 5; got {results:?}"
    );
}

/// #421 regression: IC11's varlen hop is followed by two fixed hops, so the
/// depth-2 friend must survive both joins.
#[test]
fn ic11_job_referral_germany_is_depth_two_only() {
    let (_dir, db) = load_mini_db();

    // Germany is place 3, reached only via org 3 (TechStart Inc), which employs
    // person 4 (Dave Brown) alone.  Dave sits at depth 2 from Alice, so before
    // #421 was fixed this returned nothing at all.
    let results = ic_queries::ic11_job_referral(&db, 1, "Germany", 2005).unwrap();

    assert_eq!(
        results,
        vec![("Dave".to_string(), "Brown".to_string())],
        "IC11(1, Germany): TechStart Inc is the only German organisation and \
         its sole employee, Dave Brown, is a depth-2 friend; got {results:?}"
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
