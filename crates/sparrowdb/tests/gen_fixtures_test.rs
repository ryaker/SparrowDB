use std::path::PathBuf;
use std::process::Command;

/// Path to the gen-fixtures binary built by cargo.
fn gen_fixtures_bin() -> PathBuf {
    // When run via `cargo test`, CARGO_BIN_EXE_gen-fixtures is set by cargo.
    // We use the build dir directly.
    let mut p = std::env::current_exe().unwrap();
    // Go up from deps/ to the target profile dir.
    p.pop(); // remove test binary name
    if p.ends_with("deps") {
        p.pop();
    }
    p.push("gen-fixtures");
    p
}

fn run_gen_fixtures(seed: u64, out: &std::path::Path) -> std::process::Output {
    Command::new(gen_fixtures_bin())
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--out")
        .arg(out)
        .output()
        .expect("failed to run gen-fixtures binary")
}

/// Verify deterministic output: same seed = same output.
#[test]
fn fixture_generation_is_deterministic() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let out1 = run_gen_fixtures(42, dir1.path());
    assert!(
        out1.status.success(),
        "gen-fixtures failed (run 1): {}",
        String::from_utf8_lossy(&out1.stderr)
    );

    let out2 = run_gen_fixtures(42, dir2.path());
    assert!(
        out2.status.success(),
        "gen-fixtures failed (run 2): {}",
        String::from_utf8_lossy(&out2.stderr)
    );

    // Compare file contents for all generated files.
    let files = [
        "social_small.json",
        "deps_small.json",
        "social_10k.json",
        "social_100k.json",
        "deps_500.json",
        "concepts_1k.json",
    ];

    for filename in &files {
        let path1 = dir1.path().join(filename);
        let path2 = dir2.path().join(filename);
        assert!(
            path1.exists(),
            "File {filename} not found in first run output"
        );
        assert!(
            path2.exists(),
            "File {filename} not found in second run output"
        );
        let content1 = std::fs::read(&path1).unwrap();
        let content2 = std::fs::read(&path2).unwrap();
        assert_eq!(
            content1, content2,
            "File {filename} differs between runs with same seed — not deterministic!"
        );
    }
}

/// Verify fixture structure is valid.
#[test]
fn social_small_fixture_has_correct_counts() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_gen_fixtures(42, dir.path());
    assert!(
        out.status.success(),
        "gen-fixtures failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let path = dir.path().join("social_small.json");
    let content = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();

    let nodes = v["nodes"].as_array().unwrap();
    let edges = v["edges"].as_array().unwrap();

    assert_eq!(nodes.len(), 50, "social_small must have 50 nodes");
    assert_eq!(edges.len(), 100, "social_small must have 100 edges");

    // Verify node structure.
    let node0 = &nodes[0];
    assert!(node0["id"].is_number());
    assert_eq!(node0["label"].as_str().unwrap(), "Person");
    assert!(node0["name"].as_str().unwrap().starts_with("Person_"));

    // Verify edge structure.
    let edge0 = &edges[0];
    assert!(edge0["src"].is_number());
    assert!(edge0["dst"].is_number());
    assert_eq!(edge0["rel"].as_str().unwrap(), "KNOWS");
}

/// Verify deps_small fixture structure.
#[test]
fn deps_small_fixture_has_correct_counts() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_gen_fixtures(42, dir.path());
    assert!(
        out.status.success(),
        "gen-fixtures failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let path = dir.path().join("deps_small.json");
    let content = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();

    let nodes = v["nodes"].as_array().unwrap();
    let edges = v["edges"].as_array().unwrap();

    assert_eq!(nodes.len(), 20, "deps_small must have 20 nodes");
    assert_eq!(edges.len(), 50, "deps_small must have 50 edges");

    // Verify node structure.
    let node0 = &nodes[0];
    assert!(node0["id"].is_number());
    assert_eq!(node0["label"].as_str().unwrap(), "Package");
    assert!(node0["name"].as_str().is_some());
    assert!(node0["version"].as_str().is_some());

    // Verify edge structure.
    let edge0 = &edges[0];
    assert!(edge0["src"].is_number());
    assert!(edge0["dst"].is_number());
    assert_eq!(edge0["rel"].as_str().unwrap(), "DEPENDS_ON");
}

/// Verify all large fixtures are generated with correct counts.
#[test]
fn large_fixtures_have_correct_counts() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_gen_fixtures(42, dir.path());
    assert!(
        out.status.success(),
        "gen-fixtures failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // social_10k: 10000 nodes, 50000 edges
    {
        let content = std::fs::read_to_string(dir.path().join("social_10k.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 10_000);
        assert_eq!(v["edges"].as_array().unwrap().len(), 50_000);
    }

    // social_100k: 100000 nodes, 500000 edges
    {
        let content = std::fs::read_to_string(dir.path().join("social_100k.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 100_000);
        assert_eq!(v["edges"].as_array().unwrap().len(), 500_000);
    }

    // deps_500: 500 nodes, 2000 edges
    {
        let content = std::fs::read_to_string(dir.path().join("deps_500.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 500);
        assert_eq!(v["edges"].as_array().unwrap().len(), 2_000);
    }

    // concepts_1k: 1000 nodes, 3000 edges
    {
        let content = std::fs::read_to_string(dir.path().join("concepts_1k.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 1_000);
        assert_eq!(v["edges"].as_array().unwrap().len(), 3_000);
    }
}

/// Verify different seeds produce different output.
#[test]
fn different_seeds_produce_different_output() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let out1 = run_gen_fixtures(1, dir1.path());
    assert!(out1.status.success());

    let out2 = run_gen_fixtures(99, dir2.path());
    assert!(out2.status.success());

    let c1 = std::fs::read(dir1.path().join("social_small.json")).unwrap();
    let c2 = std::fs::read(dir2.path().join("social_small.json")).unwrap();
    assert_ne!(c1, c2, "different seeds should produce different output");
}
