//! SPA-119 — Format compatibility fixture: acceptance test #13 (new check).
//!
//! Opens the committed v1.0 binary fixture (a tar archive of a real SparrowDB
//! directory) and verifies that the current code can read it without error,
//! returning the expected graph data.
//!
//! This test is the canary for on-disk format regressions: if a future change
//! breaks the binary layout, this test will fail on CI before any user data
//! is affected.
//!
//! # Fixture contents (see MANIFEST.toml)
//!
//! Nodes:
//!   - 4 × Person  (Alice, Bob, Carol, Dave)
//!   - 2 × Company (Acme, Initech)
//!
//! Edges:
//!   - KNOWS:    Alice→Bob, Bob→Carol, Carol→Dave  (3 edges)
//!   - WORKS_AT: Alice→Acme, Bob→Acme, Carol→Initech  (3 edges)
//!
//! Run: cargo test -p sparrowdb spa_119

use sparrowdb::GraphDb;
use sparrowdb_execution::types::Value;

// ── Fixture helpers ───────────────────────────────────────────────────────────

/// Absolute path to the v1.0 compatibility fixture tarball.
fn fixture_tar() -> std::path::PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent() // crates/
        .unwrap()
        .parent() // repo root
        .unwrap()
        .join("tests")
        .join("fixtures")
        .join("compatibility")
        .join("v1.0")
        .join("social_compat.tar")
}

/// Extract the fixture tarball into a temp directory and return the directory
/// plus the path to the extracted `v1.0/` database root.
fn extract_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let tar_path = fixture_tar();
    assert!(
        tar_path.exists(),
        "v1.0 compatibility fixture not found at {}\n\
         Run: cargo run --bin gen-compat-fixture",
        tar_path.display()
    );

    let tmp = tempfile::tempdir().expect("tempdir for fixture extraction");

    let status = std::process::Command::new("tar")
        .arg("xf")
        .arg(tar_path.as_os_str())
        .arg("-C")
        .arg(tmp.path().as_os_str())
        .status()
        .expect("tar command failed to start");

    assert!(
        status.success(),
        "tar extraction failed for v1.0 fixture (exit: {status})"
    );

    let db_root = tmp.path().join("v1.0");
    assert!(
        db_root.exists(),
        "expected v1.0/ directory inside fixture tar, but not found"
    );

    (tmp, db_root)
}

// ── Acceptance check #13 (compat): open v1.0 fixture ─────────────────────────

/// Check #13-compat: open the committed v1.0 binary fixture without error.
///
/// The fixture was frozen at the v1.0 format baseline (SPA-118).
/// This test proves that the current SparrowDB code can open and query
/// a database written by the v1.0 format.
#[test]
fn check_13_compat_v1_fixture_opens_without_error() {
    let (_tmp, db_root) = extract_fixture();

    // Opening must not panic or return an error.
    let db = GraphDb::open(&db_root)
        .expect("v1.0 fixture must open without error (SPA-119: format compat)");

    // ── Sub-check A: Person nodes are readable ────────────────────────────────
    let result = db
        .execute("MATCH (p:Person) RETURN p.name")
        .expect("MATCH Person must succeed on v1.0 fixture");

    assert!(
        result.rows.len() >= 4,
        "check 13-compat A: expected at least 4 Person nodes, got {}",
        result.rows.len()
    );

    // Collect all returned names.
    let names: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            v => panic!("check 13-compat A: expected String for p.name, got {v:?}"),
        })
        .collect();

    for expected in &["Alice", "Bob", "Carol", "Dave"] {
        assert!(
            names.iter().any(|n| n == expected),
            "check 13-compat A: Person '{expected}' not found in result. Got: {names:?}"
        );
    }

    // ── Sub-check B: KNOWS edges are traversable ──────────────────────────────
    let r2 = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH KNOWS must succeed on v1.0 fixture");

    assert!(
        r2.rows.len() >= 3,
        "check 13-compat B: expected at least 3 KNOWS edges, got {}",
        r2.rows.len()
    );

    // Verify at least one known edge pair.
    let pairs: Vec<(String, String)> = r2
        .rows
        .iter()
        .map(|row| {
            let a = match &row[0] {
                Value::String(s) => s.clone(),
                v => panic!("check 13-compat B: expected String for a.name, got {v:?}"),
            };
            let b = match &row[1] {
                Value::String(s) => s.clone(),
                v => panic!("check 13-compat B: expected String for b.name, got {v:?}"),
            };
            (a, b)
        })
        .collect();

    assert!(
        pairs.iter().any(|(a, b)| a == "Alice" && b == "Bob"),
        "check 13-compat B: expected Alice→Bob KNOWS edge. Got pairs: {pairs:?}"
    );
    assert!(
        pairs.iter().any(|(a, b)| a == "Bob" && b == "Carol"),
        "check 13-compat B: expected Bob→Carol KNOWS edge. Got pairs: {pairs:?}"
    );
    assert!(
        pairs.iter().any(|(a, b)| a == "Carol" && b == "Dave"),
        "check 13-compat B: expected Carol→Dave KNOWS edge. Got pairs: {pairs:?}"
    );

    // ── Sub-check C: Company nodes are readable ───────────────────────────────
    let r3 = db
        .execute("MATCH (c:Company) RETURN c.name")
        .expect("MATCH Company must succeed on v1.0 fixture");

    assert!(
        r3.rows.len() >= 2,
        "check 13-compat C: expected at least 2 Company nodes, got {}",
        r3.rows.len()
    );

    let companies: Vec<String> = r3
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            v => panic!("check 13-compat C: expected String for c.name, got {v:?}"),
        })
        .collect();

    for expected in &["Acme", "Initech"] {
        assert!(
            companies.iter().any(|n| n == expected),
            "check 13-compat C: Company '{expected}' not found. Got: {companies:?}"
        );
    }
}

/// Check #13-compat additional: integer properties survive the format round-trip.
///
/// Verifies that integer properties (age, founded) stored in the v1.0 fixture
/// are decoded correctly by the current format reader.
#[test]
fn check_13_compat_integer_properties_survive_reopen() {
    let (_tmp, db_root) = extract_fixture();
    let db = GraphDb::open(&db_root).expect("open v1.0 fixture");

    // Alice has age=30 in the fixture.
    let result = db
        .execute("MATCH (p:Person {name: 'Alice'}) RETURN p.age")
        .expect("MATCH Alice age must succeed on v1.0 fixture");

    assert_eq!(
        result.rows.len(),
        1,
        "check 13-compat int: expected exactly 1 row for Alice"
    );

    let age = match &result.rows[0][0] {
        Value::Int64(n) => *n,
        Value::Float64(f) => *f as i64,
        v => panic!("check 13-compat int: expected numeric age, got {v:?}"),
    };
    assert_eq!(age, 30, "check 13-compat int: Alice.age must be 30");
}
