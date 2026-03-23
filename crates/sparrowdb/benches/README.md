# SparrowDB Criterion Benchmark Suite (SPA-147)

## Overview

Six benchmark groups covering the core workloads of SparrowDB:

| Benchmark | Group | What it measures |
|-----------|-------|-----------------|
| `create/create_node` | create | Latency of a single `CREATE (n:Person {...})` |
| `read/match_scan_1k` | read | Full label scan over 1000 Person nodes |
| `traversal/one_hop_1k_edges` | traversal | 1-hop `(a)-[:KNOWS]->(b)` over 1000 edges |
| `traversal/two_hop_1k_edges` | traversal | 2-hop `(a)-[:KNOWS]->()-[:KNOWS]->(c)` over 1000 edges |
| `maintenance/checkpoint_after_1k_writes` | maintenance | CHECKPOINT latency after 1000 writes |
| `aggregation/count_star_1k` | aggregation | `COUNT(*)` over 1000 nodes |
| `aggregation/avg_age_1k` | aggregation | `AVG(n.age)` over 1000 nodes |
| `aggregation/max_age_1k` | aggregation | `MAX(n.age)` over 1000 nodes |

## Running

### Compile check only (fast, used in CI)

```sh
cargo bench --bench graph_benchmarks -- --test
```

This compiles the benchmark binary and runs each benchmark exactly once
without statistical sampling.  It is fast (seconds) and catches compilation
errors or panics without burning CI minutes on real measurements.

### Full measurement run (local only)

```sh
cargo bench --bench graph_benchmarks
```

HTML reports are written to `target/criterion/`.  Open
`target/criterion/report/index.html` in a browser to compare runs.

### Run a single group

```sh
cargo bench --bench graph_benchmarks -- traversal
```

## Regression Policy

A **regression** is defined as a **>10% slowdown** on any of the key
benchmarks compared to the baseline stored in `target/criterion/`.

Criterion retains per-benchmark baselines across runs.  After each run it
prints a summary line such as:

```
one_hop_1k_edges   time:   [1.2345 ms 1.2500 ms 1.2678 ms]
                   change: [+11.3% +12.1% +13.0%] (p = 0.00 < 0.05)
                   Performance has regressed.
```

### CI integration

Add the following step to `.github/workflows/` to catch regressions on every PR:

```yaml
- name: Benchmark compile-check
  run: cargo bench --bench graph_benchmarks -- --test
```

For full regression detection (requires a stable baseline), use
[`cargo-criterion`](https://github.com/bheisler/cargo-criterion) or compare
Criterion JSON output between the base branch and the PR branch.

## Adding new benchmarks

1. Add a `fn bench_<name>(c: &mut Criterion)` function in `graph_benchmarks.rs`.
2. Register it in the `criterion_group!(benches, ...)` macro at the bottom.
3. Follow the same pattern: build the fixture **outside** `b.iter(...)`, only
   put the measured operation inside.
4. Update this README table.
