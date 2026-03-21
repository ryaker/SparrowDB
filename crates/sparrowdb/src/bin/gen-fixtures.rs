//! Deterministic test fixture generator for SparrowDB.
//!
//! Generates synthetic graph datasets used by integration tests and benchmarks.
//! All output is deterministic given the same `--seed`.
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin gen-fixtures -- --seed 42 --out tests/fixtures/
//! ```
//!
//! # Output files
//!
//! | File | Nodes | Edges | Use Case |
//! |------|-------|-------|----------|
//! | `social_small.json` | 50 | 100 | Fast CI test runs |
//! | `deps_small.json` | 20 | 50 | Fast CI test runs |
//! | `social_10k.json` | 10,000 | 50,000 | UC-1 benchmark |
//! | `social_100k.json` | 100,000 | 500,000 | UC-1 scale test |
//! | `deps_500.json` | 500 | 2,000 | UC-2 benchmark |
//! | `concepts_1k.json` | 1,000 | 3,000 | UC-3 KMS benchmark |

use std::path::PathBuf;

use clap::Parser;
use rand::prelude::*;
use serde::Serialize;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "gen-fixtures")]
#[command(about = "Generate deterministic test fixtures for SparrowDB")]
struct Args {
    /// RNG seed for deterministic output
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Output directory (created if it does not exist)
    #[arg(long, default_value = "tests/fixtures")]
    out: PathBuf,
}

// ── Data models ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct PersonNode {
    id: u64,
    label: &'static str,
    name: String,
}

#[derive(Debug, Serialize)]
struct PackageNode {
    id: u64,
    label: &'static str,
    name: String,
    version: String,
}

#[derive(Debug, Serialize)]
struct KnowledgeNode {
    id: u64,
    label: &'static str,
    content: String,
    confidence: f64,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct Edge {
    src: u64,
    dst: u64,
    rel: &'static str,
}

#[derive(Debug, Serialize)]
struct SocialGraph {
    nodes: Vec<PersonNode>,
    edges: Vec<Edge>,
}

#[derive(Debug, Serialize)]
struct DepsGraph {
    nodes: Vec<PackageNode>,
    edges: Vec<Edge>,
}

#[derive(Debug, Serialize)]
struct ConceptsGraph {
    nodes: Vec<KnowledgeNode>,
    edges: Vec<Edge>,
}

// ── Generator functions ──────────────────────────────────────────────────────

/// Generate a social graph with power-law degree distribution.
///
/// Uses a Zipf-like approximation: node 0 has the highest degree,
/// degrees fall off as `max_degree / (rank + 1)`. The total edge count
/// is adjusted to match `n_edges` exactly.
fn gen_social_graph(n_nodes: u64, n_edges: u64, rng: &mut impl Rng) -> SocialGraph {
    let nodes: Vec<PersonNode> = (0..n_nodes)
        .map(|id| PersonNode {
            id,
            label: "Person",
            name: format!("Person_{id}"),
        })
        .collect();

    let edges = gen_power_law_edges(n_nodes, n_edges, "KNOWS", rng);
    SocialGraph { nodes, edges }
}

/// Generate edges with a power-law degree distribution.
///
/// Each source node i gets a degree proportional to `n_nodes / (i + 1)`,
/// giving a Zipf-like distribution. Destinations are chosen uniformly
/// at random (excluding self-loops). The total edge count is trimmed or
/// padded to exactly `target_edges`.
fn gen_power_law_edges(
    n_nodes: u64,
    target_edges: u64,
    rel: &'static str,
    rng: &mut impl Rng,
) -> Vec<Edge> {
    // Compute a raw degree per node using a Zipf-like formula.
    // Scale so the sum ≈ target_edges.
    let harmonic_sum: f64 = (1..=n_nodes).map(|i| 1.0 / i as f64).sum();
    let scale = target_edges as f64 / harmonic_sum;

    let mut edges: Vec<Edge> = Vec::with_capacity(target_edges as usize + n_nodes as usize);

    for src in 0..n_nodes {
        let raw_degree = (scale / (src + 1) as f64).round() as u64;
        let degree = raw_degree.max(1).min(n_nodes - 1);
        // Pick `degree` distinct random destinations (no self-loops).
        let mut chosen = std::collections::BTreeSet::new();
        let mut attempts = 0u64;
        while chosen.len() < degree as usize && attempts < degree * 3 + 10 {
            let dst = rng.gen_range(0..n_nodes);
            if dst != src {
                chosen.insert(dst);
            }
            attempts += 1;
        }
        for dst in chosen {
            edges.push(Edge { src, dst, rel });
        }
    }

    // Trim or pad to exactly target_edges.
    if edges.len() > target_edges as usize {
        edges.truncate(target_edges as usize);
    } else {
        // Pad with random edges from random sources.
        while edges.len() < target_edges as usize {
            let src = rng.gen_range(0..n_nodes);
            let dst = rng.gen_range(0..n_nodes);
            if src != dst {
                edges.push(Edge { src, dst, rel });
            }
        }
    }

    edges
}

/// Generate a dependency graph with DAG structure (no cycles), ~6 levels deep.
///
/// Nodes are arranged in layers. Edges only go from lower-numbered layers
/// to higher-numbered layers, ensuring a DAG.
fn gen_deps_graph(n_nodes: u64, target_edges: u64, rng: &mut impl Rng) -> DepsGraph {
    let nodes: Vec<PackageNode> = (0..n_nodes)
        .map(|id| PackageNode {
            id,
            label: "Package",
            name: format!("pkg_{id}"),
            version: format!("1.{}.0", id % 10),
        })
        .collect();

    // Layer assignment: 6 levels, nodes distributed evenly.
    let n_levels = 6u64;
    let layer_size = n_nodes.div_ceil(n_levels);
    let layer_of = |node_id: u64| node_id / layer_size;

    let mut edges: Vec<Edge> = Vec::with_capacity(target_edges as usize);

    // First pass: generate edges from each node to 2-4 nodes in the next layer.
    for src in 0..n_nodes {
        let src_layer = layer_of(src);
        if src_layer >= n_levels - 1 {
            continue; // Last layer has no outgoing edges.
        }
        let next_layer_start = (src_layer + 1) * layer_size;
        let next_layer_end = ((src_layer + 2) * layer_size).min(n_nodes);
        if next_layer_start >= next_layer_end {
            continue;
        }
        let n_deps = rng
            .gen_range(2u64..5)
            .min(next_layer_end - next_layer_start);
        let mut chosen = std::collections::BTreeSet::new();
        let mut attempts = 0u64;
        while chosen.len() < n_deps as usize && attempts < n_deps * 3 + 5 {
            let dst = rng.gen_range(next_layer_start..next_layer_end);
            chosen.insert(dst);
            attempts += 1;
        }
        for dst in chosen {
            edges.push(Edge {
                src,
                dst,
                rel: "DEPENDS_ON",
            });
        }
    }

    // Second pass: trim or pad to target_edges, preserving DAG invariant.
    if edges.len() > target_edges as usize {
        edges.truncate(target_edges as usize);
    } else {
        // Add more DAG edges (src < dst constraint ensures acyclicity by node ID).
        while edges.len() < target_edges as usize {
            let src = rng.gen_range(0..n_nodes.saturating_sub(1));
            // dst must be in a strictly higher layer.
            let src_layer = layer_of(src);
            if src_layer >= n_levels - 1 {
                continue;
            }
            let min_dst = (src_layer + 1) * layer_size;
            let max_dst = n_nodes;
            if min_dst >= max_dst {
                continue;
            }
            let dst = rng.gen_range(min_dst..max_dst);
            edges.push(Edge {
                src,
                dst,
                rel: "DEPENDS_ON",
            });
        }
    }

    DepsGraph { nodes, edges }
}

/// Knowledge graph topics for UC-3 (KMS dataset).
const TOPICS: &[&str] = &[
    "AI",
    "machine learning",
    "neural networks",
    "databases",
    "distributed systems",
    "graph theory",
    "type theory",
    "cryptography",
    "operating systems",
    "compilers",
    "software architecture",
    "data structures",
    "algorithms",
    "security",
    "networking",
];

const SOURCES: &[&str] = &["manual", "import", "inference", "web"];
const EDGE_RELS: &[&str] = &["ABOUT", "MENTIONS", "RELATED_TO"];

/// Generate a concepts/knowledge graph for UC-3.
fn gen_concepts_graph(n_nodes: u64, target_edges: u64, rng: &mut impl Rng) -> ConceptsGraph {
    let nodes: Vec<KnowledgeNode> = (0..n_nodes)
        .map(|id| {
            let topic = TOPICS[id as usize % TOPICS.len()];
            let source = SOURCES[id as usize % SOURCES.len()];
            // Confidence in [0.5, 1.0] — deterministic from node id.
            let confidence = 0.5 + (id as f64 % 50.0) / 100.0;
            KnowledgeNode {
                id,
                label: "Knowledge",
                content: format!("fact about {topic} #{id}"),
                confidence,
                source,
            }
        })
        .collect();

    let mut edges: Vec<Edge> = Vec::with_capacity(target_edges as usize);

    while edges.len() < target_edges as usize {
        let src = rng.gen_range(0..n_nodes);
        let dst = rng.gen_range(0..n_nodes);
        if src != dst {
            let rel = EDGE_RELS[rng.gen_range(0..EDGE_RELS.len())];
            edges.push(Edge { src, dst, rel });
        }
    }

    ConceptsGraph { nodes, edges }
}

// ── Write helpers ────────────────────────────────────────────────────────────

fn write_json<T: Serialize>(out_dir: &std::path::Path, filename: &str, value: &T) {
    let path = out_dir.join(filename);
    let json = serde_json::to_string_pretty(value).expect("JSON serialization failed");
    std::fs::write(&path, json).unwrap_or_else(|e| panic!("Failed to write {filename}: {e}"));
    println!("  wrote {}", path.display());
}

// ── main ─────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    std::fs::create_dir_all(&args.out)
        .unwrap_or_else(|e| panic!("Failed to create output dir {}: {e}", args.out.display()));

    println!(
        "Generating fixtures with seed={}, out={}",
        args.seed,
        args.out.display()
    );

    // Each fixture gets its own seeded RNG derived from args.seed + a fixture-specific offset.
    // This keeps fixture outputs independent: changing social_10k does not affect deps_500.

    // social_small — 50 nodes, 100 edges
    {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(args.seed.wrapping_add(0));
        let graph = gen_social_graph(50, 100, &mut rng);
        write_json(&args.out, "social_small.json", &graph);
    }

    // deps_small — 20 nodes, 50 edges
    {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(args.seed.wrapping_add(1));
        let graph = gen_deps_graph(20, 50, &mut rng);
        write_json(&args.out, "deps_small.json", &graph);
    }

    // social_10k — 10,000 nodes, 50,000 edges
    {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(args.seed.wrapping_add(2));
        let graph = gen_social_graph(10_000, 50_000, &mut rng);
        write_json(&args.out, "social_10k.json", &graph);
    }

    // social_100k — 100,000 nodes, 500,000 edges
    {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(args.seed.wrapping_add(3));
        let graph = gen_social_graph(100_000, 500_000, &mut rng);
        write_json(&args.out, "social_100k.json", &graph);
    }

    // deps_500 — 500 nodes, 2,000 edges
    {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(args.seed.wrapping_add(4));
        let graph = gen_deps_graph(500, 2_000, &mut rng);
        write_json(&args.out, "deps_500.json", &graph);
    }

    // concepts_1k — 1,000 nodes, 3,000 edges
    {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(args.seed.wrapping_add(5));
        let graph = gen_concepts_graph(1_000, 3_000, &mut rng);
        write_json(&args.out, "concepts_1k.json", &graph);
    }

    println!("Done.");
}
