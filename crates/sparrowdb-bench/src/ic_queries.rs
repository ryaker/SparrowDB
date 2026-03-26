//! LDBC SNB Interactive Complex (IC) query implementations — SPA-146 / SPA-111.
//!
//! Queries are expressed as Cypher strings executed against a [`GraphDb`].
//! IC1 and IC2 were implemented in SPA-146. IC3–IC14 are implemented as part of
//! SPA-111 phase 2.
//!
//! ## Engine limitations
//!
//! SparrowDB traverses edges only in their stored (outgoing) direction. Reverse
//! traversal (`<-[:REL]-`) and undirected chaining are unreliable for multi-hop
//! patterns. Queries that need to "start from a Person and find their Posts"
//! (where hasCreator goes Post->Person) use a two-step approach: first collect
//! friend IDs, then query with forward-only patterns.

use sparrowdb::GraphDb;
use sparrowdb_execution::Value;
use std::collections::HashMap;

// ── Result types ──────────────────────────────────────────────────────────────

/// A (firstName, lastName) pair returned by multiple IC queries.
pub type PersonName = (String, String);

// ── Helpers ───────────────────────────────────────────────────────────────────

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn value_to_i64(v: &Value) -> i64 {
    match v {
        Value::Int64(n) => *n,
        _ => 0,
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

/// Get direct friend ldbc_ids for a given person.
fn get_friend_ids(db: &GraphDb, person_id: i64) -> sparrowdb::Result<Vec<i64>> {
    let cypher = "\
        MATCH (p:Person {ldbc_id: $personId})-[:knows]->(friend:Person) \
        RETURN friend.ldbc_id";
    let params = HashMap::from([("personId".into(), Value::Int64(person_id))]);
    let result = db.execute_with_params(cypher, params)?;
    Ok(result
        .rows
        .iter()
        .map(|row| value_to_i64(&row[0]))
        .collect())
}

// ── IC1 — Friends within 3 hops named X ──────────────────────────────────────

/// **IC1** — Find persons reachable within 1-3 KNOWS hops from a person
/// named `first_name`, where the friend's first name differs.
///
/// Returns up to 20 `(firstName, lastName)` pairs, ordered by `lastName`.
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

// ── IC2 — Recent messages by friends ────────────────────────────────────────

/// **IC2** — Direct friends of a person by `ldbc_id`.
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

// ── IC3 — Friends in countries ──────────────────────────────────────────────

/// **IC3** — Friends/friends-of-friends located in a given country.
///
/// Simplified: no date-range filtering on messages. Uses forward-only chain:
/// Person -[:knows*1..2]-> Person -[:isLocatedIn]-> Place.
pub fn ic3_friends_in_countries(
    db: &GraphDb,
    person_id: i64,
    country_name: &str,
    _country_y: &str,
    _duration_days: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (p:Person {ldbc_id: $personId})-[:knows*1..2]->(friend:Person) \
              -[:isLocatedIn]->(place:Place) \
        WHERE place.name = $countryName \
        RETURN DISTINCT friend.firstName, friend.lastName \
        ORDER BY friend.lastName LIMIT 20";

    let params = HashMap::from([
        ("personId".into(), Value::Int64(person_id)),
        ("countryName".into(), Value::String(country_name.into())),
    ]);
    let result = db.execute_with_params(cypher, params)?;
    Ok(rows_to_person_names(result))
}

// ── IC4 — Top tags of recent posts by friends ───────────────────────────────

/// **IC4** — Tags on posts created by direct friends.
///
/// Two-step: first get friend IDs, then query posts by those creators.
pub fn ic4_top_tags(
    db: &GraphDb,
    person_id: i64,
    _start_date: &str,
    _duration_days: i64,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    let friend_ids = get_friend_ids(db, person_id)?;
    if friend_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids_list: Vec<String> = friend_ids.iter().map(|id| id.to_string()).collect();

    // Step 1: find post IDs created by friends
    let post_query = format!(
        "MATCH (post:Post)-[:hasCreator]->(creator:Person) \
         WHERE creator.ldbc_id IN [{}] \
         RETURN post.ldbc_id",
        ids_list.join(", ")
    );
    let post_result = db.execute(&post_query)?;
    let post_ids: Vec<i64> = post_result
        .rows
        .iter()
        .map(|row| value_to_i64(&row[0]))
        .collect();

    if post_ids.is_empty() {
        return Ok(Vec::new());
    }

    let post_ids_list: Vec<String> = post_ids.iter().map(|id| id.to_string()).collect();
    let tag_query = format!(
        "MATCH (post:Post)-[:hasTag]->(tag:Tag) \
         WHERE post.ldbc_id IN [{}] \
         RETURN tag.name, COUNT(*) AS cnt \
         ORDER BY cnt DESC LIMIT 10",
        post_ids_list.join(", ")
    );
    let result = db.execute(&tag_query)?;
    Ok(result
        .rows
        .iter()
        .map(|row| (value_to_string(&row[0]), value_to_i64(&row[1])))
        .collect())
}

// ── IC5 — Forums with friend activity ───────────────────────────────────────

/// **IC5** — Forums that the person's friends are members of.
///
/// Two-step: get friend IDs, then find forums containing those members.
pub fn ic5_forums_with_friends(
    db: &GraphDb,
    person_id: i64,
    _min_date: &str,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    let friend_ids = get_friend_ids(db, person_id)?;
    if friend_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids_list: Vec<String> = friend_ids.iter().map(|id| id.to_string()).collect();
    let cypher = format!(
        "MATCH (forum:Forum)-[:hasMember]->(member:Person) \
         WHERE member.ldbc_id IN [{}] \
         RETURN forum.title, COUNT(*) AS cnt \
         ORDER BY cnt DESC LIMIT 20",
        ids_list.join(", ")
    );
    let result = db.execute(&cypher)?;
    Ok(result
        .rows
        .iter()
        .map(|row| (value_to_string(&row[0]), value_to_i64(&row[1])))
        .collect())
}

// ── IC6 — Tag co-occurrence ─────────────────────────────────────────────────

/// **IC6** — Tags on posts by friends, excluding a given tag.
///
/// Two-step: reuses IC4 post discovery, filters tags.
pub fn ic6_tag_co_occurrence(
    db: &GraphDb,
    person_id: i64,
    tag_name: &str,
) -> sparrowdb::Result<Vec<(String, i64)>> {
    let friend_ids = get_friend_ids(db, person_id)?;
    if friend_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids_list: Vec<String> = friend_ids.iter().map(|id| id.to_string()).collect();
    let post_query = format!(
        "MATCH (post:Post)-[:hasCreator]->(creator:Person) \
         WHERE creator.ldbc_id IN [{}] \
         RETURN post.ldbc_id",
        ids_list.join(", ")
    );
    let post_result = db.execute(&post_query)?;
    let post_ids: Vec<i64> = post_result
        .rows
        .iter()
        .map(|row| value_to_i64(&row[0]))
        .collect();

    if post_ids.is_empty() {
        return Ok(Vec::new());
    }

    let post_ids_list: Vec<String> = post_ids.iter().map(|id| id.to_string()).collect();
    let params = HashMap::from([("tagName".into(), Value::String(tag_name.into()))]);
    let tag_query = format!(
        "MATCH (post:Post)-[:hasTag]->(tag:Tag) \
         WHERE post.ldbc_id IN [{}] AND tag.name <> $tagName \
         RETURN tag.name, COUNT(*) AS cnt \
         ORDER BY cnt DESC LIMIT 10",
        post_ids_list.join(", ")
    );
    let result = db.execute_with_params(&tag_query, params)?;
    Ok(result
        .rows
        .iter()
        .map(|row| (value_to_string(&row[0]), value_to_i64(&row[1])))
        .collect())
}

// ── IC7 — Latest likes on person's posts ────────────────────────────────────

/// **IC7** — People who liked the person's posts.
///
/// Forward chain: Person -[:likes]-> Post -[:hasCreator]-> Person.
pub fn ic7_latest_likes(db: &GraphDb, person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (liker:Person)-[:likes]->(post:Post)-[:hasCreator]->(creator:Person {ldbc_id: $personId}) \
        RETURN DISTINCT liker.firstName, liker.lastName \
        LIMIT 20";

    let params = HashMap::from([("personId".into(), Value::Int64(person_id))]);
    let result = db.execute_with_params(cypher, params)?;
    Ok(rows_to_person_names(result))
}

// ── IC8 — Replies to person's posts ─────────────────────────────────────────

/// **IC8** — People who replied (via Comment) to the person's posts.
///
/// Forward chain: Comment -[:replyOf]-> Post -[:hasCreator]-> Person.
/// Then get the commenter via a second step.
pub fn ic8_replies(db: &GraphDb, person_id: i64) -> sparrowdb::Result<Vec<PersonName>> {
    // Step 1: find comment IDs that reply to this person's posts
    let cypher1 = "\
        MATCH (comment:Comment)-[:replyOf]->(post:Post)-[:hasCreator]->(creator:Person {ldbc_id: $personId}) \
        RETURN comment.ldbc_id";
    let params = HashMap::from([("personId".into(), Value::Int64(person_id))]);
    let comment_result = db.execute_with_params(cypher1, params)?;
    let comment_ids: Vec<i64> = comment_result
        .rows
        .iter()
        .map(|row| value_to_i64(&row[0]))
        .collect();

    if comment_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Step 2: find who created those comments
    let ids_list: Vec<String> = comment_ids.iter().map(|id| id.to_string()).collect();
    let cypher2 = format!(
        "MATCH (comment:Comment)-[:hasCreator]->(commenter:Person) \
         WHERE comment.ldbc_id IN [{}] \
         RETURN DISTINCT commenter.firstName, commenter.lastName",
        ids_list.join(", ")
    );
    let result = db.execute(&cypher2)?;
    Ok(rows_to_person_names(result))
}

// ── IC9 — Recent posts by friends ───────────────────────────────────────────

/// **IC9** — Posts created by direct friends.
///
/// Two-step: get friend IDs, then find posts by those friends.
pub fn ic9_recent_posts_by_friends(
    db: &GraphDb,
    person_id: i64,
    _max_date: &str,
) -> sparrowdb::Result<Vec<(PersonName, String)>> {
    let friend_ids = get_friend_ids(db, person_id)?;
    if friend_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids_list: Vec<String> = friend_ids.iter().map(|id| id.to_string()).collect();
    let cypher = format!(
        "MATCH (post:Post)-[:hasCreator]->(friend:Person) \
         WHERE friend.ldbc_id IN [{}] \
         RETURN friend.firstName, friend.lastName, post.content \
         LIMIT 20",
        ids_list.join(", ")
    );
    let result = db.execute(&cypher)?;
    Ok(result
        .rows
        .iter()
        .filter_map(|row| {
            if row.len() < 3 {
                return None;
            }
            let first = value_to_string(&row[0]);
            let last = value_to_string(&row[1]);
            let content = value_to_string(&row[2]);
            Some(((first, last), content))
        })
        .collect())
}

// ── IC10 — Friend recommendations ──────────────────────────────────────────

/// **IC10** — Friends-of-friends as recommendations.
///
/// Simplified: no birthday month matching. Forward chain only.
pub fn ic10_friend_recommendations(
    db: &GraphDb,
    person_id: i64,
    _month: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (p:Person {ldbc_id: $personId})-[:knows*2..2]->(fof:Person) \
        WHERE fof.ldbc_id <> $personId \
        RETURN DISTINCT fof.firstName, fof.lastName \
        ORDER BY fof.lastName LIMIT 10";

    let params = HashMap::from([("personId".into(), Value::Int64(person_id))]);
    let result = db.execute_with_params(cypher, params)?;
    Ok(rows_to_person_names(result))
}

// ── IC11 — Job referral ─────────────────────────────────────────────────────

/// **IC11** — Friends (1-2 hops) who work at an organisation in a given country.
///
/// Forward chain: Person -[:knows*1..2]-> Person -[:workAt]-> Organisation
///                -[:isLocatedIn]-> Place.
pub fn ic11_job_referral(
    db: &GraphDb,
    person_id: i64,
    country_name: &str,
    _work_from_year: i64,
) -> sparrowdb::Result<Vec<PersonName>> {
    let cypher = "\
        MATCH (p:Person {ldbc_id: $personId})-[:knows*1..2]->(friend:Person) \
              -[:workAt]->(org:Organisation)-[:isLocatedIn]->(place:Place) \
        WHERE place.name = $countryName \
        RETURN DISTINCT friend.firstName, friend.lastName \
        ORDER BY friend.lastName LIMIT 10";

    let params = HashMap::from([
        ("personId".into(), Value::Int64(person_id)),
        ("countryName".into(), Value::String(country_name.into())),
    ]);
    let result = db.execute_with_params(cypher, params)?;
    Ok(rows_to_person_names(result))
}

// ── IC12 — Expert search by tag class ───────────────────────────────────────

/// **IC12** — Friends who created posts tagged with a tag of a given TagClass.
///
/// Three-step: get friend IDs, find their post IDs, then find tags with the
/// matching TagClass.
pub fn ic12_expert_search(
    db: &GraphDb,
    person_id: i64,
    tag_class_name: &str,
) -> sparrowdb::Result<Vec<(PersonName, i64)>> {
    let friend_ids = get_friend_ids(db, person_id)?;
    if friend_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids_list: Vec<String> = friend_ids.iter().map(|id| id.to_string()).collect();

    // Get posts by friends and their tags with matching TagClass
    let post_query = format!(
        "MATCH (post:Post)-[:hasCreator]->(creator:Person) \
         WHERE creator.ldbc_id IN [{}] \
         RETURN post.ldbc_id, creator.firstName, creator.lastName",
        ids_list.join(", ")
    );
    let post_result = db.execute(&post_query)?;

    if post_result.rows.is_empty() {
        return Ok(Vec::new());
    }

    // Build a map of post_id -> (firstName, lastName)
    let mut post_creators: HashMap<i64, PersonName> = HashMap::new();
    let mut post_ids: Vec<i64> = Vec::new();
    for row in &post_result.rows {
        let pid = value_to_i64(&row[0]);
        let first = value_to_string(&row[1]);
        let last = value_to_string(&row[2]);
        post_creators.insert(pid, (first, last));
        post_ids.push(pid);
    }

    let post_ids_list: Vec<String> = post_ids.iter().map(|id| id.to_string()).collect();
    let params = HashMap::from([("tagClassName".into(), Value::String(tag_class_name.into()))]);
    let tag_query = format!(
        "MATCH (post:Post)-[:hasTag]->(tag:Tag)-[:hasType]->(tc:TagClass) \
         WHERE post.ldbc_id IN [{}] AND tc.name = $tagClassName \
         RETURN post.ldbc_id, COUNT(*) AS cnt",
        post_ids_list.join(", ")
    );
    let tag_result = db.execute_with_params(&tag_query, params)?;

    // Aggregate counts per person
    let mut person_counts: HashMap<PersonName, i64> = HashMap::new();
    for row in &tag_result.rows {
        let pid = value_to_i64(&row[0]);
        let cnt = value_to_i64(&row[1]);
        if let Some(name) = post_creators.get(&pid) {
            *person_counts.entry(name.clone()).or_insert(0) += cnt;
        }
    }

    let mut results: Vec<(PersonName, i64)> = person_counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1));
    results.truncate(20);
    Ok(results)
}

// ── IC13 — Shortest path ────────────────────────────────────────────────────

/// **IC13** — Shortest path length between two persons via KNOWS.
///
/// Returns the hop count, or -1 if no path exists.
pub fn ic13_shortest_path(
    db: &GraphDb,
    person1_id: i64,
    person2_id: i64,
) -> sparrowdb::Result<i64> {
    let cypher = "\
        MATCH (a:Person {ldbc_id: $person1Id}), (b:Person {ldbc_id: $person2Id}) \
        RETURN shortestPath((a)-[:knows*]->(b))";

    let params = HashMap::from([
        ("person1Id".into(), Value::Int64(person1_id)),
        ("person2Id".into(), Value::Int64(person2_id)),
    ]);
    let result = db.execute_with_params(cypher, params)?;
    if result.rows.is_empty() || result.rows[0].is_empty() {
        return Ok(-1);
    }
    Ok(value_to_i64(&result.rows[0][0]))
}

// ── IC14 — Weighted shortest path ───────────────────────────────────────────

/// **IC14** — Weighted shortest path between two persons.
///
/// Simplified: SparrowDB does not support weighted path scoring. Returns
/// the unweighted shortest path length as a single-element vector.
pub fn ic14_weighted_path(
    db: &GraphDb,
    person1_id: i64,
    person2_id: i64,
) -> sparrowdb::Result<Vec<i64>> {
    let dist = ic13_shortest_path(db, person1_id, person2_id)?;
    if dist < 0 {
        Ok(Vec::new())
    } else {
        Ok(vec![dist])
    }
}
