//! Parallel BFS traversal primitives using Rayon.
//!
//! Two primitives:
//! - `parallel_reachability_bfs`: existential queries (RETURN DISTINCT, shortestPath)
//! - `parallel_path_enumeration_dfs`: openCypher enumerative *M..N (all simple paths)

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rayon::prelude::*;

/// Result of a reachability BFS: set of reachable node IDs.
pub struct ReachabilityResult {
    pub visited: HashSet<u64>,
}

/// Parallel BFS for existential/reachability queries.
///
/// Uses a global visited set (mutex-protected) shared across Rayon tasks.
/// Correct for: RETURN DISTINCT queries, shortestPath(), existential checks.
/// NOT correct for: enumerative path listing (use `parallel_path_enumeration_dfs` instead).
///
/// # Arguments
/// - `start_nodes`: seed nodes for the BFS frontier
/// - `min_hops`: minimum hop depth (inclusive) — nodes reachable before this depth are
///   still tracked in `visited` but the semantics of which hops "count" is caller's concern
/// - `max_hops`: maximum hop depth (inclusive); BFS stops after this many expansions
/// - `get_neighbors`: closure returning outgoing neighbor IDs for a given node ID
pub fn parallel_reachability_bfs<F>(
    start_nodes: Vec<u64>,
    _min_hops: usize,
    max_hops: usize,
    get_neighbors: F,
) -> ReachabilityResult
where
    F: Fn(u64) -> Vec<u64> + Send + Sync,
{
    let visited = Arc::new(Mutex::new(
        start_nodes.iter().copied().collect::<HashSet<_>>(),
    ));

    let mut frontier = start_nodes;
    let mut hop = 0usize;

    while !frontier.is_empty() && hop < max_hops {
        // Parallel expand frontier — each node expands independently
        let next_nodes: Vec<u64> = frontier
            .par_iter()
            .flat_map(|&node| get_neighbors(node))
            .collect();

        // Deduplicate against visited (serial — single mutex lock per frontier wave)
        let mut v = visited.lock().unwrap();
        frontier = next_nodes.into_iter().filter(|n| v.insert(*n)).collect();
        hop += 1;
    }

    let v = visited
        .lock()
        .expect("visited mutex should not be poisoned")
        .clone();
    ReachabilityResult { visited: v }
}

/// Per-path DFS state for enumeration.
#[derive(Clone)]
struct PathState {
    path: Vec<u64>,
    path_set: HashSet<u64>, // for O(1) cycle check
}

/// Shared context threaded through recursive DFS calls; avoids exceeding the
/// clippy `too_many_arguments` limit on `dfs_enumerate`.
struct DfsContext<'a, F> {
    min_hops: usize,
    max_hops: usize,
    limit: usize,
    get_neighbors: &'a F,
    results: &'a Arc<Mutex<Vec<Vec<u64>>>>,
    done: &'a Arc<AtomicBool>,
}

/// Parallel path enumeration DFS.
///
/// Each Rayon task has its own path state (no shared visited set), preserving
/// openCypher simple-path semantics: a node may appear on multiple distinct paths
/// but not more than once within a single path.
///
/// Correct for: `MATCH (a)-[*M..N]->(b)` enumerative semantics where the diamond
/// graph A→B→D, A→C→D should yield D twice (two distinct simple paths).
///
/// WARNING: can produce exponential results on dense graphs. Caller should pass a
/// reasonable `limit` and enforce `LIMIT` in the Cypher query.
///
/// # Arguments
/// - `start_nodes`: seed nodes for path exploration
/// - `min_hops`: minimum path length (paths shorter than this are not emitted)
/// - `max_hops`: maximum path length (DFS does not recurse deeper)
/// - `limit`: early-termination cap on total results collected; `0` returns immediately
/// - `get_neighbors`: closure returning outgoing neighbor IDs for a given node ID
pub fn parallel_path_enumeration_dfs<F>(
    start_nodes: Vec<u64>,
    min_hops: usize,
    max_hops: usize,
    limit: usize,
    get_neighbors: F,
) -> Vec<Vec<u64>>
where
    F: Fn(u64) -> Vec<u64> + Send + Sync,
{
    if limit == 0 {
        return Vec::new();
    }

    let results = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
    let done = Arc::new(AtomicBool::new(false));

    start_nodes.par_iter().for_each(|&start| {
        if done.load(Ordering::Relaxed) {
            return;
        }
        let mut initial_path_set = HashSet::new();
        initial_path_set.insert(start);
        let initial = PathState {
            path: vec![start],
            path_set: initial_path_set,
        };
        let ctx = DfsContext {
            min_hops,
            max_hops,
            limit,
            get_neighbors: &get_neighbors,
            results: &results,
            done: &done,
        };
        dfs_enumerate(initial, 0, &ctx);
    });

    Arc::try_unwrap(results)
        .expect("results Arc should be uniquely owned after parallel traversal")
        .into_inner()
        .expect("results Mutex should not be poisoned")
}

fn dfs_enumerate<F>(state: PathState, depth: usize, ctx: &DfsContext<'_, F>)
where
    F: Fn(u64) -> Vec<u64> + Send + Sync,
{
    if ctx.done.load(Ordering::Relaxed) {
        return;
    }

    if depth >= ctx.min_hops {
        let mut r = ctx
            .results
            .lock()
            .expect("results Mutex should not be poisoned");
        // Re-check limit under the lock: concurrent tasks may have filled the
        // buffer between the `done` pre-check above and acquiring the lock here.
        if r.len() >= ctx.limit {
            ctx.done.store(true, Ordering::Relaxed);
            return;
        }
        r.push(state.path.clone());
        if r.len() >= ctx.limit {
            ctx.done.store(true, Ordering::Relaxed);
            return;
        }
    }

    if depth >= ctx.max_hops {
        return;
    }

    let current = *state.path.last().unwrap();
    for neighbor in (ctx.get_neighbors)(current) {
        if !state.path_set.contains(&neighbor) {
            let mut next_state = state.clone();
            next_state.path.push(neighbor);
            next_state.path_set.insert(neighbor);
            dfs_enumerate(next_state, depth + 1, ctx);
        }
    }
}
