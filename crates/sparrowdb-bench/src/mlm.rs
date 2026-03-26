//! MLM (multi-level marketing) referral-tree dataset generator — SPA-206.
//!
//! Generates a synthetic MLM referral network and loads it into a SparrowDB
//! instance via Cypher `CREATE` statements.
//!
//! ## Graph shape
//!
//! - **`:Member`** nodes with properties: `uid`, `join_date`, `rank`,
//!   `monthly_volume`, `active`
//! - **`:RECRUITED`** edges forming a tree (parent `->` child direction, so
//!   downline traversal uses the engine's outgoing variable-length paths)
//!
//! ## Parameters
//!
//! | Field | Default | Description |
//! |-------|---------|-------------|
//! | `members` | 1 000 | Total member count |
//! | `max_depth` | 8 | Maximum tree depth |
//! | `avg_fanout` | 3 | Average children per node (power-law) |
//! | `seed` | 42 | RNG seed for reproducibility |

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sparrowdb::GraphDb;

// ── Configuration ────────────────────────────────────────────────────────────

/// Parameters that control the synthetic MLM tree shape.
#[derive(Debug, Clone)]
pub struct MlmConfig {
    /// Total number of members to generate.
    pub members: usize,
    /// Maximum tree depth (root = depth 0).
    pub max_depth: u32,
    /// Average recruiter fan-out (actual per-node count is power-law-ish).
    pub avg_fanout: u32,
    /// RNG seed for deterministic generation.
    pub seed: u64,
}

impl Default for MlmConfig {
    fn default() -> Self {
        Self {
            members: 1_000,
            max_depth: 8,
            avg_fanout: 3,
            seed: 42,
        }
    }
}

// ── Generator ────────────────────────────────────────────────────────────────

/// Statistics returned after generation.
#[derive(Debug, Clone, Default)]
pub struct MlmStats {
    pub nodes_created: u64,
    pub edges_created: u64,
    pub actual_depth: u32,
}

/// Internal node used during tree construction.
struct MemberNode {
    uid: usize,
    depth: u32,
}

/// Build the MLM referral tree and ingest it into `db`.
///
/// Returns statistics about the generated graph.
pub fn generate(db: &GraphDb, cfg: &MlmConfig) -> sparrowdb::Result<MlmStats> {
    let mut rng = SmallRng::seed_from_u64(cfg.seed);
    let mut stats = MlmStats::default();

    // BFS queue: nodes whose children we still need to create.
    let mut queue: Vec<MemberNode> = Vec::new();
    let mut next_uid: usize = 1;

    // ── Create root member ───────────────────────────────────────────────
    let root_volume = rng.gen_range(500..5000);
    let root_cypher = format!(
        "CREATE (m:Member {{uid: {}, join_date: '2020-01-01', rank: 'Diamond', \
         monthly_volume: {}, active: 1}})",
        next_uid, root_volume,
    );
    db.execute(&root_cypher)?;
    stats.nodes_created += 1;

    queue.push(MemberNode {
        uid: next_uid,
        depth: 0,
    });
    next_uid += 1;

    // ── BFS expansion ────────────────────────────────────────────────────
    while !queue.is_empty() && next_uid <= cfg.members {
        let parent = queue.remove(0);

        // Skip if we've hit max depth.
        if parent.depth >= cfg.max_depth {
            continue;
        }

        // Draw fan-out from a geometric-ish distribution centered on avg_fanout.
        let fanout = sample_fanout(&mut rng, cfg.avg_fanout);

        for _ in 0..fanout {
            if next_uid > cfg.members {
                break;
            }

            let child_uid = next_uid;
            next_uid += 1;
            let child_depth = parent.depth + 1;

            let volume = rng.gen_range(50..3000);
            let active: u8 = if rng.gen_bool(0.85) { 1 } else { 0 };
            let rank = rank_for_depth(child_depth);
            let join_year = 2020 + (child_depth as u16).min(4);
            let join_month = rng.gen_range(1u8..=12);

            // Create the child node.
            let create_node = format!(
                "CREATE (m:Member {{uid: {}, join_date: '{}-{:02}-01', rank: '{}', \
                 monthly_volume: {}, active: {}}})",
                child_uid, join_year, join_month, rank, volume, active,
            );
            db.execute(&create_node)?;
            stats.nodes_created += 1;

            // Create RECRUITED edge: parent -[:RECRUITED]-> child.
            // Outgoing direction from parent so that downline traversal uses
            // the engine's supported outgoing variable-length paths.
            let create_edge = format!(
                "MATCH (parent:Member {{uid: {}}}), (child:Member {{uid: {}}}) \
                 CREATE (parent)-[:RECRUITED]->(child)",
                parent.uid, child_uid,
            );
            db.execute(&create_edge)?;
            stats.edges_created += 1;

            stats.actual_depth = stats.actual_depth.max(child_depth);

            queue.push(MemberNode {
                uid: child_uid,
                depth: child_depth,
            });
        }
    }

    Ok(stats)
}

// ── MLM query functions ──────────────────────────────────────────────────────

/// **Q-MLM1**: Count all downline members and sum their monthly volume
/// (variable-depth traversal up to 8 hops).
///
/// Returns `(count, total_volume)`. The aggregation is performed in Rust
/// because the engine does not yet support `COUNT`/`SUM` over variable-length
/// path results.
pub fn q_mlm1_downline_volume(db: &GraphDb, uid: usize) -> sparrowdb::Result<(i64, i64)> {
    let cypher = format!(
        "MATCH (root:Member {{uid: {}}})-[:RECRUITED*1..8]->(m) \
         RETURN m.monthly_volume",
        uid,
    );
    let result = db.execute(&cypher)?;
    let count = result.rows.len() as i64;
    let volume: i64 = result
        .rows
        .iter()
        .map(|r| r.first().map(value_to_i64).unwrap_or(0))
        .sum();
    Ok((count, volume))
}

/// **Q-MLM2**: Level-3 downline only — members recruited exactly 3 hops away.
pub fn q_mlm2_level3_downline(db: &GraphDb, uid: usize) -> sparrowdb::Result<Vec<(i64, i64)>> {
    let cypher = format!(
        "MATCH (root:Member {{uid: {}}})-[:RECRUITED*3..3]->(m) \
         RETURN m.uid, m.monthly_volume",
        uid,
    );
    let result = db.execute(&cypher)?;
    Ok(result
        .rows
        .iter()
        .map(|r| {
            let uid_val = r.first().map(value_to_i64).unwrap_or(0);
            let vol = r.get(1).map(value_to_i64).unwrap_or(0);
            (uid_val, vol)
        })
        .collect())
}

/// **Q-MLM3**: Upline path to the top — traverse RECRUITED edges backwards
/// (incoming) from the member toward the root.
///
/// Note: this uses single-hop iteration up to depth 8 since the engine's
/// variable-length path support is outgoing-only. Each call walks the
/// RECRUITED edges in reverse by matching `(ancestor)-[:RECRUITED]->(m)`.
pub fn q_mlm3_upline_path(db: &GraphDb, uid: usize) -> sparrowdb::Result<Vec<i64>> {
    // Walk up one hop at a time since incoming variable-length paths are not
    // yet supported by the engine.
    let mut ancestors = Vec::new();
    let mut current_uid = uid as i64;

    for _ in 0..8 {
        let cypher = format!(
            "MATCH (ancestor:Member)-[:RECRUITED]->(m:Member {{uid: {}}}) \
             RETURN ancestor.uid",
            current_uid,
        );
        let result = db.execute(&cypher)?;
        if let Some(row) = result.rows.first() {
            let ancestor_uid = row.first().map(value_to_i64).unwrap_or(0);
            if ancestor_uid == 0 {
                break;
            }
            ancestors.push(ancestor_uid);
            current_uid = ancestor_uid;
        } else {
            break; // Reached root (no parent).
        }
    }
    Ok(ancestors)
}

/// **Q-MLM4**: Subtree volume rollup — aggregate monthly_volume of active
/// members within 4 hops.
///
/// Aggregation performed in Rust (engine limitation on varpath + aggregation).
pub fn q_mlm4_subtree_volume(db: &GraphDb, uid: usize) -> sparrowdb::Result<i64> {
    let cypher = format!(
        "MATCH (root:Member {{uid: {}}})-[:RECRUITED*1..4]->(m:Member) \
         WHERE m.active = 1 \
         RETURN m.monthly_volume",
        uid,
    );
    let result = db.execute(&cypher)?;
    Ok(result
        .rows
        .iter()
        .map(|r| r.first().map(value_to_i64).unwrap_or(0))
        .sum())
}

/// **Q-MLM5**: Top 10 recruiters by direct downline count.
pub fn q_mlm5_top_recruiters(db: &GraphDb) -> sparrowdb::Result<Vec<(i64, i64)>> {
    let cypher = "MATCH (r:Member)-[:RECRUITED]->(m:Member) \
                  RETURN r.uid, COUNT(m) AS direct_recruits \
                  ORDER BY direct_recruits DESC LIMIT 10";
    let result = db.execute(cypher)?;
    Ok(result
        .rows
        .iter()
        .map(|r| {
            let uid_val = r.first().map(value_to_i64).unwrap_or(0);
            let cnt = r.get(1).map(value_to_i64).unwrap_or(0);
            (uid_val, cnt)
        })
        .collect())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Extract an i64 from a [`Value`], coercing where possible.
fn value_to_i64(v: &sparrowdb_execution::Value) -> i64 {
    use sparrowdb_execution::Value;
    match v {
        Value::Int64(i) => *i,
        Value::Float64(f) => *f as i64,
        Value::String(s) => s.parse::<i64>().unwrap_or(0),
        Value::Null => 0,
        _ => 0,
    }
}

/// Sample a fan-out count from a geometric-ish distribution.
///
/// Returns 0..max_fanout where expected value ~ `avg`.
fn sample_fanout(rng: &mut SmallRng, avg: u32) -> u32 {
    // Geometric distribution: P(k) = (1-p)^k * p, mean = (1-p)/p.
    // Solve for p given mean = avg: p = 1/(avg+1).
    let p = 1.0 / (avg as f64 + 1.0);
    let mut count = 0u32;
    let max = avg * 4; // cap at 4x average
    while count < max {
        if rng.gen_bool(p) {
            break;
        }
        count += 1;
    }
    count
}

/// Map tree depth to an MLM rank name.
fn rank_for_depth(depth: u32) -> &'static str {
    match depth {
        0 => "Diamond",
        1 => "Platinum",
        2 => "Gold",
        3 => "Silver",
        4 => "Bronze",
        _ => "Associate",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_fanout_produces_reasonable_values() {
        let mut rng = SmallRng::seed_from_u64(1);
        let samples: Vec<u32> = (0..100).map(|_| sample_fanout(&mut rng, 3)).collect();
        let mean = samples.iter().sum::<u32>() as f64 / samples.len() as f64;
        // Mean should be roughly around 3 (within a wide margin for small sample).
        assert!(mean > 1.0 && mean < 8.0, "mean={mean}");
    }
}
