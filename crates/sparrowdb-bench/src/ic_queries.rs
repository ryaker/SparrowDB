//! LDBC SNB Interactive Complex (IC) query implementations — SPA-146.
//!
//! Queries are expressed as Cypher strings executed against a [`GraphDb`].
//! IC1 and IC2 are fully implemented; IC3–IC14 are stubbed and return empty
//! results until scheduled for implementation (see GitHub issue).

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;

/// A (firstName, lastName) pair returned by IC1 and IC2.
pub type PersonName = (String, String);

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn rows_to_person_names(result: sparrowdb::QueryResult) -> Vec<PersonName> {
    result
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
        .collect()
}

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

    let params = HashMap::from([("firstName".into(), Value::String(first_name.into()))]);

    let result = db.execute_with_params(cypher, params)?;

    Ok(rows_to_person_names(result))
}

/// **IC2** — Find direct friends of a person identified by `person_id`
/// (the LDBC integer id stored as `ldbc_id`).
///
/// Returns up to 20 `(firstName, lastName)` pairs.
pub fn ic2_recent_friends(db: &GraphDb, person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (p:Person {ldbc_id: $personId})-[:knows]-(friend:Person) \
        RETURN friend.firstName, friend.lastName \
        LIMIT 20";

    let params = HashMap::from([("personId".into(), Value::Int64(person_id))]);

    let result = db.execute_with_params(cypher, params)?;

    Ok(rows_to_person_names(result))
}

/// **IC3** — Friends in countries X and Y. (Stub)
pub fn ic3_friends_in_countries(
    _db: &GraphDb,
    _person_id: i64,
    _country_x: &str,
    _country_y: &str,
    _duration_days: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC3)
    Ok(Vec::new())
}

/// **IC4** — Top tags of recent posts by friends. (Stub)
pub fn ic4_top_tags(
    _db: &GraphDb,
    _person_id: i64,
    _start_date: &str,
    _duration_days: i64,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    // TODO(SPA-146/IC4)
    Ok(Vec::new())
}

/// **IC5** — Forums with recent activity from friends. (Stub)
pub fn ic5_forums_with_friends(
    _db: &GraphDb,
    _person_id: i64,
    _min_date: &str,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    // TODO(SPA-146/IC5)
    Ok(Vec::new())
}

/// **IC6** — Tag co-occurrence among friends. (Stub)
pub fn ic6_tag_co_occurrence(
    _db: &GraphDb,
    _person_id: i64,
    _tag_name: &str,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    // TODO(SPA-146/IC6)
    Ok(Vec::new())
}

/// **IC7** — Latest likes on the person's messages. (Stub)
pub fn ic7_latest_likes(_db: &GraphDb, _person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC7)
    Ok(Vec::new())
}

/// **IC8** — Replies to the person's messages. (Stub)
pub fn ic8_replies(_db: &GraphDb, _person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC8)
    Ok(Vec::new())
}

/// **IC9** — Recent posts by friends. (Stub)
pub fn ic9_recent_posts_by_friends(
    _db: &GraphDb,
    _person_id: i64,
    _max_date: &str,
) -> sparrowdb::Result<Vec<(PersonName, String)>> {
    // TODO(SPA-146/IC9)
    Ok(Vec::new())
}

/// **IC10** — Friend recommendations by birthday. (Stub)
pub fn ic10_friend_recommendations(
    _db: &GraphDb,
    _person_id: i64,
    _month: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC10)
    Ok(Vec::new())
}

/// **IC11** — Friends by job history. (Stub)
pub fn ic11_job_referral(
    _db: &GraphDb,
    _person_id: i64,
    _country_name: &str,
    _work_from_year: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    // TODO(SPA-146/IC11)
    Ok(Vec::new())
}

/// **IC12** — Expert search by tag class. (Stub)
pub fn ic12_expert_search(
    _db: &GraphDb,
    _person_id: i64,
    _tag_class_name: &str,
) -> sparrowdb::Result<Vec<(PersonName, i64)>> {
    // TODO(SPA-146/IC12)
    Ok(Vec::new())
}

/// **IC13** — Shortest path via KNOWS. (Stub)
pub fn ic13_shortest_path(
    _db: &GraphDb,
    _person1_id: i64,
    _person2_id: i64,
) -> sparrowdb::Result<i64> {
    // TODO(SPA-146/IC13)
    Ok(-1)
}

/// **IC14** — Weighted shortest path via KNOWS. (Stub)
pub fn ic14_weighted_path(
    _db: &GraphDb,
    _person1_id: i64,
    _person2_id: i64,
) -> sparrowdb::Result<Vec<i64>> {
    // TODO(SPA-146/IC14)
    Ok(Vec::new())
}
