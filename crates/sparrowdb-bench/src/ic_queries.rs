//! LDBC SNB Interactive Complex (IC) query implementations — SPA-146.
//!
//! Queries are expressed as Cypher strings executed against a [`GraphDb`].
//! IC1 and IC2 are fully implemented; IC3–IC14 are stubbed and return empty
//! results until scheduled for implementation (see GitHub issue).

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;

// ── Result types ──────────────────────────────────────────────────────────────

/// A (firstName, lastName) pair returned by IC1 and IC2.
pub type PersonName = (String, String);

// ── Helper: extract String from a query Value ─────────────────────────────────

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ── IC1 — Friends within 3 hops named X ──────────────────────────────────────

/// **IC1** — Find all persons reachable within 1–3 KNOWS hops from any person
/// named `first_name`, where the friend's first name differs from the start
/// person's first name.
///
/// Returns up to 20 `(firstName, lastName)` pairs, ordered by `lastName`.
///
/// # Engine note
/// SparrowDB's variable-length path engine currently supports outgoing (`->`)
/// direction only.  The LDBC spec uses undirected traversal; full undirected
/// support is tracked separately.
pub fn ic1_friends_named(db: &GraphDb, first_name: &str) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (p:Person {firstName: $firstName})-[:knows*1..3]->(friend:Person) \
        WHERE friend.firstName <> p.firstName \
        RETURN DISTINCT friend.firstName, friend.lastName \
        ORDER BY friend.lastName LIMIT 20";

    let mut params: HashMap<String, Value> = HashMap::new();
    params.insert(
        "firstName".to_string(),
        Value::String(first_name.to_string()),
    );

    let result = db.execute_with_params(cypher, params)?;

    let rows = result
        .rows
        .iter()
        .filter_map(|row| {
            if row.len() < 2 {
                return None;
            }
            let first = value_to_string(&row[0]);
            let last = value_to_string(&row[1]);
            if first.is_empty() && last.is_empty() {
                None
            } else {
                Some((first, last))
            }
        })
        .collect();

    Ok(rows)
}

// ── IC2 — Recent messages by friends (simplified: returns friend names) ────────

/// **IC2** — Find direct friends of a person identified by `person_id`
/// (the LDBC integer id stored as `ldbc_id`).
///
/// Returns up to 20 `(firstName, lastName)` pairs.
pub fn ic2_recent_friends(db: &GraphDb, person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (p:Person {ldbc_id: $personId})-[:knows]-(friend:Person) \
        RETURN friend.firstName, friend.lastName \
        LIMIT 20";

    let mut params: HashMap<String, Value> = HashMap::new();
    params.insert("personId".to_string(), Value::Int64(person_id));

    let result = db.execute_with_params(cypher, params)?;

    let rows = result
        .rows
        .iter()
        .filter_map(|row| {
            if row.len() < 2 {
                return None;
            }
            let first = value_to_string(&row[0]);
            let last = value_to_string(&row[1]);
            if first.is_empty() && last.is_empty() {
                None
            } else {
                Some((first, last))
            }
        })
        .collect();

    Ok(rows)
}

// ── IC3–IC14 stubs ────────────────────────────────────────────────────────────

/// **IC3** — Friends and friends-of-friends that have been to countries X and Y.
/// (Stub — not yet implemented.)
pub fn ic3_friends_in_countries(
    _db: &GraphDb,
    _person_id: i64,
    _country_x: &str,
    _country_y: &str,
    _duration_days: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC3): implement friends-in-countries query
    Ok(Vec::new())
}

/// **IC4** — Top tags of recent posts by friends.
/// (Stub — not yet implemented.)
pub fn ic4_top_tags(
    _db: &GraphDb,
    _person_id: i64,
    _start_date: &str,
    _duration_days: i64,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    // TODO(SPA-146/IC4): implement top-tags query
    Ok(Vec::new())
}

/// **IC5** — Forums with recent activity from friends.
/// (Stub — not yet implemented.)
pub fn ic5_forums_with_friends(
    _db: &GraphDb,
    _person_id: i64,
    _min_date: &str,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    // TODO(SPA-146/IC5): implement forums-with-friends query
    Ok(Vec::new())
}

/// **IC6** — Tags for posts in a given forum that friends have liked.
/// (Stub — not yet implemented.)
pub fn ic6_tag_co_occurrence(
    _db: &GraphDb,
    _person_id: i64,
    _tag_name: &str,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    // TODO(SPA-146/IC6): implement tag-co-occurrence query
    Ok(Vec::new())
}

/// **IC7** — Latest likes on the person's messages.
/// (Stub — not yet implemented.)
pub fn ic7_latest_likes(_db: &GraphDb, _person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC7): implement latest-likes query
    Ok(Vec::new())
}

/// **IC8** — Replies to the person's messages.
/// (Stub — not yet implemented.)
pub fn ic8_replies(_db: &GraphDb, _person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC8): implement replies query
    Ok(Vec::new())
}

/// **IC9** — Recent posts by friends.
/// (Stub — not yet implemented.)
pub fn ic9_recent_posts_by_friends(
    _db: &GraphDb,
    _person_id: i64,
    _max_date: &str,
) -> sparrowdb::Result<Vec<(PersonName, String)>> {
    // TODO(SPA-146/IC9): implement recent-posts-by-friends query
    Ok(Vec::new())
}

/// **IC10** — Friends with similar interests (same birthday month/day window).
/// (Stub — not yet implemented.)
pub fn ic10_friend_recommendations(
    _db: &GraphDb,
    _person_id: i64,
    _month: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC10): implement friend-recommendations query
    Ok(Vec::new())
}

/// **IC11** — Friends that worked at a given company before a given year.
/// (Stub — not yet implemented.)
pub fn ic11_job_referral(
    _db: &GraphDb,
    _person_id: i64,
    _country_name: &str,
    _work_from_year: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC11): implement job-referral query
    Ok(Vec::new())
}

/// **IC12** — Friends who replied to posts with a given tag class.
/// (Stub — not yet implemented.)
pub fn ic12_expert_search(
    _db: &GraphDb,
    _person_id: i64,
    _tag_class_name: &str,
) -> sparrowdb::Result<Vec<(PersonName, i64)>> {
    // TODO(SPA-146/IC12): implement expert-search query
    Ok(Vec::new())
}

/// **IC13** — Shortest path between two persons via KNOWS.
/// (Stub — not yet implemented.)
pub fn ic13_shortest_path(
    _db: &GraphDb,
    _person1_id: i64,
    _person2_id: i64,
) -> sparrowdb::Result<i64> {
    // TODO(SPA-146/IC13): implement shortest-path query (requires BFS/path support)
    Ok(-1)
}

/// **IC14** — Weighted shortest path between two persons via KNOWS.
/// (Stub — not yet implemented.)
pub fn ic14_weighted_path(
    _db: &GraphDb,
    _person1_id: i64,
    _person2_id: i64,
) -> sparrowdb::Result<Vec<i64>> {
    // TODO(SPA-146/IC14): implement weighted-shortest-path query
    Ok(Vec::new())
}
