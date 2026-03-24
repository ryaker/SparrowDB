use sparrowdb_execution::parallel_bfs::{parallel_path_enumeration_dfs, parallel_reachability_bfs};

fn diamond_neighbors(node: u64) -> Vec<u64> {
    match node {
        0 => vec![1, 2], // A -> B, A -> C
        1 => vec![3],    // B -> D
        2 => vec![3],    // C -> D
        _ => vec![],
    }
}

#[test]
fn test_reachability_diamond() {
    // A->B->D, A->C->D, query *2..2 — D is reachable (appears once in reachability)
    let result = parallel_reachability_bfs(vec![0], 2, 2, diamond_neighbors);
    assert!(result.visited.contains(&3), "D should be reachable");
    assert!(result.visited.contains(&1), "B should be reachable");
    assert!(result.visited.contains(&2), "C should be reachable");
}

#[test]
fn test_enumeration_diamond() {
    // A->B->D, A->C->D, query *2..2 — D appears TWICE (two distinct simple paths)
    let paths = parallel_path_enumeration_dfs(vec![0], 2, 2, 1000, diamond_neighbors);
    let paths_ending_at_3: Vec<_> = paths.iter().filter(|p| *p.last().unwrap() == 3).collect();
    assert_eq!(
        paths_ending_at_3.len(),
        2,
        "Diamond: D must appear twice (two simple paths)"
    );
}

fn cycle_neighbors(node: u64) -> Vec<u64> {
    match node {
        0 => vec![1],
        1 => vec![2],
        2 => vec![0], // cycle back to A
        _ => vec![],
    }
}

#[test]
fn test_enumeration_no_revisit() {
    // A->B->C->A (cycle). Query *1..4. A->B->C is a valid path, A->B->C->A is NOT (revisits A)
    let paths = parallel_path_enumeration_dfs(vec![0], 1, 4, 1000, cycle_neighbors);
    // Should NOT find any path that revisits start node
    for path in &paths {
        assert_eq!(
            path.iter().filter(|&&n| n == 0).count(),
            1,
            "Start node should not be revisited: {:?}",
            path
        );
    }
}

#[test]
fn test_serial_parallel_equivalence() {
    // Run same query single-threaded vs multi-threaded, results should match
    use rayon::ThreadPoolBuilder;
    let pool1 = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let pool2 = ThreadPoolBuilder::new().num_threads(4).build().unwrap();

    let r1 = pool1.install(|| parallel_reachability_bfs(vec![0], 1, 3, diamond_neighbors));
    let r2 = pool2.install(|| parallel_reachability_bfs(vec![0], 1, 3, diamond_neighbors));
    assert_eq!(
        r1.visited, r2.visited,
        "Parallel and single-threaded must produce same visited set"
    );
}
