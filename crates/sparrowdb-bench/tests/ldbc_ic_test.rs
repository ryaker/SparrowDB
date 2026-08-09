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
    let is_sorted = last_names.windows(2).all(|w| w[0] <= w[1]);
    assert!(is_sorted, "IC1: results should be sorted by lastName");

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

// ── IC3–IC14 ────────────────────────────────────────────────────────────────
//
// These were written against stubs (5865f0e) and asserted `is_empty()`. The
// queries were implemented in 5b049ff (#279) without updating this file, and
// CI reported the resulting failures as green (#412), so they went unnoticed.
//
// Expectations below are derived by hand from the mini fixture, NOT captured
// from program output — see #421, where reading the CSVs is what exposed a
// silent variable-length truncation bug that captured output would have
// frozen in as "expected".
//
// Fixture facts used throughout (Alice = ldbc_id 1):
//   knows:        1→{2,3,6}; 2→{4,5}; 3→{4,5}; 6→{7}; 7→8; 8→9; 9→10
//   isLocatedIn:  1,3,7,10→US(1)  2,5,8→UK(2)  4,6,9→Germany(3)
//   posts:        1,4→Alice  2→Bob  3→Carol  5→Dave
//   post tags:    1→{Databases,GraphTheory} 2→{Rust} 3→{SocialNetworks}
//                 4→{Databases} 5→{GraphTheory}
//   likes:        2→p1, 3→p1, 1→p2, 4→p3, 3→p4
//   comments:     c1→p1 by Bob, c4→p4 by Dave
//   forums:       1 "Graph Databases" {1,2,3}; 2 "Rust Programming" {4,5};
//                 3 "Social Networks" {1,6}

// Un-ignored once #421 shipped (6254f91): a variable-length path followed by
// another hop now returns depth-2 matches instead of silently truncating to
// depth 1. Dave Brown is the depth-2 match this used to lose.
#[test]
fn ic3_friends_in_countries_returns_both_germans() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic3_friends_in_countries(&db, 1, "Germany", "France", 14)
        .expect("IC3 should not error");
    let names: Vec<String> = r.iter().map(|p| p.1.clone()).collect();
    assert_eq!(
        names,
        vec!["Brown".to_string(), "Miller".to_string()],
        "IC3: Alice's friends within 2 hops located in Germany are Dave Brown (depth 2) \
         and Frank Miller (depth 1); got {r:?}"
    );
}

#[test]
fn ic4_top_tags_of_friends_posts() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic4_top_tags(&db, 1, "2012-01-01", 30).expect("IC4 should not error");
    // Friends {2,3,6}: Bob→post2→Rust, Carol→post3→SocialNetworks, Frank→no posts.
    assert_eq!(
        r,
        vec![("Rust".to_string(), 1), ("SocialNetworks".to_string(), 1)],
        "IC4: expected one Rust and one SocialNetworks tag from friends' posts; got {r:?}"
    );
}

#[test]
fn ic5_forums_ranked_by_friend_membership() {
    let (_dir, db) = db_with_mini_fixture();
    let r =
        ic_queries::ic5_forums_with_friends(&db, 1, "2012-01-01").expect("IC5 should not error");
    // Forum 1 holds friends 2 and 3 → 2; forum 3 holds friend 6 → 1;
    // forum 2 holds only 4 and 5, neither a friend of Alice.
    assert_eq!(
        r,
        vec![
            ("Graph Databases".to_string(), 2),
            ("Social Networks".to_string(), 1)
        ],
        "IC5: expected Graph Databases=2, Social Networks=1; got {r:?}"
    );
}

#[test]
fn ic6_co_occurring_tags() {
    let (_dir, db) = db_with_mini_fixture();

    // Alice's friend posts are 2 (Bob, {Rust}) and 3 (Carol, {SocialNetworks});
    // Frank has none. Every expectation below follows from those two posts.

    // Only post 2 carries Rust, and it carries no other tag, so nothing
    // co-occurs with Rust among friends' posts.
    let r = ic_queries::ic6_tag_co_occurrence(&db, 1, "Rust").expect("IC6 should not error");
    assert!(
        r.is_empty(),
        "IC6: post 2 is the only friend post tagged Rust and has no other tag; got {r:?}"
    );

    // Symmetric case: only post 3 carries SocialNetworks, and it too is
    // single-tagged.
    let sn =
        ic_queries::ic6_tag_co_occurrence(&db, 1, "SocialNetworks").expect("IC6 should not error");
    assert!(
        sn.is_empty(),
        "IC6: post 3 is the only friend post tagged SocialNetworks and has no other tag; got {sn:?}"
    );

    // The #422 headline case. Databases sits on posts 1 and 4, both created by
    // Alice herself — no *friend* post carries it, so no friend post survives
    // the restriction and nothing can co-occur. Before the fix this returned
    // [(Rust,1), (SocialNetworks,1)]: every friend tag except the input one.
    let db_tag =
        ic_queries::ic6_tag_co_occurrence(&db, 1, "Databases").expect("IC6 should not error");
    assert!(
        db_tag.is_empty(),
        "IC6: Databases is only on Alice's own posts 1 and 4, never a friend's; got {db_tag:?}"
    );

    // A tag no post carries must yield nothing.
    let none = ic_queries::ic6_tag_co_occurrence(&db, 1, "ZZZ_nonexistent")
        .expect("IC6 unknown tag should not error");
    assert!(
        none.is_empty(),
        "IC6: unknown tag must return empty; got {none:?}"
    );

    // Liveness guard. Every IC6 expectation the mini fixture can express is
    // empty: the only multi-tag post is 1 ({Databases, GraphTheory}), it belongs
    // to Alice, and nobody `knows` Alice (person 1 is never a knows target), so
    // post 1 is never anyone's friend post. That makes the assertions above
    // satisfiable by an ic6 that always returns empty. IC4 walks the same
    // friend→post→tag pipeline without the tag restriction, so its non-emptiness
    // proves the empties are the restriction at work, not a dead query.
    // Tracked in #428 — a friend-owned multi-tag post would let IC6 be asserted
    // positively and retire this guard.
    let ic4 = ic_queries::ic4_top_tags(&db, 1, "2012-01-01", 30).expect("IC4 should not error");
    assert!(
        !ic4.is_empty(),
        "IC6 liveness: IC4 shares IC6's friend→post→tag pipeline and must be non-empty, \
         otherwise IC6's empty results prove nothing; got {ic4:?}"
    );
}

#[test]
fn ic7_likers_of_alices_posts() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic7_latest_likes(&db, 1).expect("IC7 should not error");
    // Alice created posts 1 and 4. Post 1 liked by 2 (Bob) and 3 (Carol);
    // post 4 liked by 3 (Carol).
    let names: Vec<String> = r.iter().map(|p| p.1.clone()).collect();
    assert_eq!(
        names,
        vec!["Jones".to_string(), "White".to_string()],
        "IC7: Bob Jones and Carol White liked Alice's posts; got {r:?}"
    );
}

#[test]
fn ic8_repliers_to_alices_posts() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic8_replies(&db, 1).expect("IC8 should not error");
    // Alice's posts are 1 and 4; comment 1 (Bob) replies to post 1,
    // comment 4 (Dave) replies to post 4.
    let names: Vec<String> = r.iter().map(|p| p.1.clone()).collect();
    assert_eq!(
        names,
        vec!["Jones".to_string(), "Brown".to_string()],
        "IC8: Bob Jones and Dave Brown replied to Alice's posts; got {r:?}"
    );
}

#[test]
fn ic9_recent_posts_by_friends() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic9_recent_posts_by_friends(&db, 1, "2012-01-01")
        .expect("IC9 should not error");
    // Friends {2,3,6}: Bob wrote post 2, Carol post 3, Frank none.
    let contents: Vec<String> = r.iter().map(|(_, c)| c.clone()).collect();
    assert_eq!(
        contents,
        vec![
            "Rust is amazing".to_string(),
            "Social networks are complex".to_string()
        ],
        "IC9: expected Bob's and Carol's posts; got {r:?}"
    );
}

#[test]
fn ic10_recommends_friends_of_friends() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic10_friend_recommendations(&db, 1, 3).expect("IC10 should not error");
    // Alice knows {2,3,6}. Friends-of-friends: 2→{4,5}, 3→{4,5}, 6→{7}.
    // Excluding existing friends and Alice herself leaves {4,5,7}.
    let names: Vec<String> = r.iter().map(|p| p.1.clone()).collect();
    assert_eq!(
        names,
        vec![
            "Brown".to_string(),
            "Davis".to_string(),
            "Wilson".to_string()
        ],
        "IC10: expected Dave Brown, Eve Davis, Grace Wilson; got {r:?}"
    );
}

#[test]
fn ic12_expert_search_by_tag_class() {
    let (_dir, db) = db_with_mini_fixture();
    // Friends {2,3,6}: Bob wrote post 2 → tag Rust → class Technology;
    // Carol wrote post 3 → tag SocialNetworks → class Science; Frank none.
    let tech = ic_queries::ic12_expert_search(&db, 1, "Technology").expect("IC12 should not error");
    assert_eq!(
        tech,
        vec![(("Bob".to_string(), "Jones".to_string()), 1)],
        "IC12: only Bob posted under tag class Technology; got {tech:?}"
    );

    let science = ic_queries::ic12_expert_search(&db, 1, "Science").expect("IC12 should not error");
    assert_eq!(
        science,
        vec![(("Carol".to_string(), "White".to_string()), 1)],
        "IC12: only Carol posted under tag class Science; got {science:?}"
    );

    // No tag maps to OffTopic in tag_hasType_tagclass_0_0.csv.
    let off = ic_queries::ic12_expert_search(&db, 1, "OffTopic").expect("IC12 should not error");
    assert!(off.is_empty(), "IC12: no expert for OffTopic; got {off:?}");
}

// Un-ignored once #421 shipped (6254f91) — see ic3 above.
#[test]
fn ic11_job_referral() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic11_job_referral(&db, 1, "Germany", 2005).expect("IC11 should not error");
    assert!(!r.is_empty(), "IC11 should find referrals; got {r:?}");
}

#[test]
fn ic13_shortest_path_alice_to_jack() {
    let (_dir, db) = db_with_mini_fixture();
    let hops = ic_queries::ic13_shortest_path(&db, 1, 10).expect("IC13 should not error");
    // 1→6→7→8→9→10 is 5 hops, shorter than 1→3→5→7→8→9→10 (6) and
    // 1→2→4→6→7→8→9→10 (7).
    assert_eq!(hops, 5, "IC13: shortest path 1→10 is 5 hops; got {hops}");
}

#[test]
fn ic13_unreachable_returns_negative_one() {
    let (_dir, db) = db_with_mini_fixture();
    // knows edges are directed and no path leads back to Alice. Person 10
    // appears only as a target (9→10) and has no outgoing knows edge at all.
    let hops = ic_queries::ic13_shortest_path(&db, 10, 1).expect("IC13 should not error");
    assert_eq!(
        hops, -1,
        "IC13: 10→1 is unreachable, expected -1; got {hops}"
    );

    // Person 2 does have outgoing knows edges (2→4, 2→5), but none of them lead
    // back to 1 — 1 is never a knows target. This separates "source is a dead
    // end" from "genuinely unreachable"; the pre-fix engine reported 1 hop here
    // too, via 2 -[:likes]-> Post slot 0 colliding with Person slot 0.
    let back = ic_queries::ic13_shortest_path(&db, 2, 1).expect("IC13 should not error");
    assert_eq!(
        back, -1,
        "IC13: 2→1 is unreachable (1 is never a knows target); got {back}"
    );

    // A person absent from the fixture is unreachable in both directions.
    let missing_dst = ic_queries::ic13_shortest_path(&db, 1, 9999).expect("IC13 should not error");
    assert_eq!(
        missing_dst, -1,
        "IC13: person 9999 does not exist; got {missing_dst}"
    );
    let missing_src = ic_queries::ic13_shortest_path(&db, 9999, 1).expect("IC13 should not error");
    assert_eq!(
        missing_src, -1,
        "IC13: person 9999 does not exist; got {missing_src}"
    );
}

#[test]
fn ic13_directed_and_self_paths() {
    let (_dir, db) = db_with_mini_fixture();
    // 1→2 is a stored knows edge, so exactly 1 hop forward...
    let fwd = ic_queries::ic13_shortest_path(&db, 1, 2).expect("IC13 should not error");
    assert_eq!(fwd, 1, "IC13: 1→2 is a direct knows edge; got {fwd}");
    // ...and 0 hops to oneself.
    let same = ic_queries::ic13_shortest_path(&db, 1, 1).expect("IC13 should not error");
    assert_eq!(
        same, 0,
        "IC13: a person is 0 hops from themselves; got {same}"
    );
    // A person who does not exist is not 0 hops from themselves.
    let ghost = ic_queries::ic13_shortest_path(&db, 9999, 9999).expect("IC13 should not error");
    assert_eq!(
        ghost, -1,
        "IC13: person 9999 does not exist, so there is no path; got {ghost}"
    );
    // 1→6→7→8→9 is 4 hops (1→3→5→7→8→9 is 5, 1→2→4→6→7→8→9 is 6).
    let iris = ic_queries::ic13_shortest_path(&db, 1, 9).expect("IC13 should not error");
    assert_eq!(iris, 4, "IC13: shortest path 1→9 is 4 hops; got {iris}");
}

#[test]
fn ic14_finds_a_path_alice_to_jack() {
    let (_dir, db) = db_with_mini_fixture();
    let r = ic_queries::ic14_weighted_path(&db, 1, 10).expect("IC14 should not error");
    // A path exists (see ic13); LDBC IC14 weighting semantics are not pinned
    // down by this fixture, so assert only that a path is reported.
    assert!(
        !r.is_empty(),
        "IC14: a path from 1 to 10 exists and should be reported; got {r:?}"
    );
}
