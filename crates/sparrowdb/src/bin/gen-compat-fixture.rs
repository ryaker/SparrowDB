//! SPA-118 — v1.0 compatibility fixture generator.
//!
//! Creates a deterministic SparrowDB database directory containing a small
//! representative social graph, then archives it as a `.tar` file that can be
//! committed to the repository and used by compatibility tests.
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin gen-compat-fixture -- \
//!     --out tests/fixtures/compatibility/v1.0/social_compat.tar
//! ```

use std::path::PathBuf;

use clap::Parser;
use sparrowdb::GraphDb;

/// A minimal temp-dir RAII wrapper that does not need the `tempfile` crate.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(prefix: &str) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let p = std::env::temp_dir().join(format!("{prefix}_{ts}"));
        std::fs::create_dir_all(&p).expect("create tmp dir");
        TmpDir(p)
    }

    fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "gen-compat-fixture")]
#[command(about = "Generate a v1.0 on-disk compatibility fixture for SparrowDB")]
struct Args {
    /// Output path for the resulting tar archive.
    #[arg(
        long,
        default_value = "tests/fixtures/compatibility/v1.0/social_compat.tar"
    )]
    out: PathBuf,
}

// ── main ─────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    // Use a temporary directory to build the database, then tar it.
    let tmp = TmpDir::new("sparrowdb_compat");
    let db_path = tmp.path().join("v1.0");

    println!("Generating v1.0 compatibility fixture …");

    // ── Populate the graph ──────────────────────────────────────────────────
    // Deterministic: no RNG — all data is hardcoded.
    // Chosen to exercise: node labels, string properties, integer properties,
    // edge types, and multi-hop traversal.
    {
        let db = GraphDb::open(&db_path).expect("open db");

        // Create Person nodes
        db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
            .expect("Alice");
        db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
            .expect("Bob");
        db.execute("CREATE (n:Person {name: 'Carol', age: 35})")
            .expect("Carol");
        db.execute("CREATE (n:Person {name: 'Dave', age: 28})")
            .expect("Dave");

        // Create Company nodes
        db.execute("CREATE (n:Company {name: 'Acme', founded: 1999})")
            .expect("Acme");
        db.execute("CREATE (n:Company {name: 'Initech', founded: 2001})")
            .expect("Initech");

        // Create KNOWS edges (1-hop)
        db.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("Alice KNOWS Bob");
        db.execute(
            "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("Bob KNOWS Carol");
        db.execute(
            "MATCH (a:Person {name: 'Carol'}), (b:Person {name: 'Dave'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .expect("Carol KNOWS Dave");

        // Create WORKS_AT edges
        db.execute(
            "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)",
        ).expect("Alice WORKS_AT Acme");
        db.execute(
            "MATCH (p:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)",
        ).expect("Bob WORKS_AT Acme");
        db.execute(
            "MATCH (p:Person {name: 'Carol'}), (c:Company {name: 'Initech'}) CREATE (p)-[:WORKS_AT]->(c)",
        ).expect("Carol WORKS_AT Initech");

        // Checkpoint to flush WAL → stable binary layout
        db.execute("CHECKPOINT").expect("CHECKPOINT");
    }

    // ── Quick sanity check ──────────────────────────────────────────────────
    {
        let db = GraphDb::open(&db_path).expect("reopen for sanity check");

        let r = db
            .execute("MATCH (p:Person) RETURN p.name")
            .expect("sanity MATCH");
        println!("  Person count = {}", r.rows.len());
        assert_eq!(r.rows.len(), 4, "expected 4 Person nodes");

        let r2 = db
            .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
            .expect("sanity KNOWS");
        println!("  KNOWS edges = {}", r2.rows.len());
        assert_eq!(r2.rows.len(), 3, "expected 3 KNOWS edges");

        let r3 = db
            .execute(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
                 RETURN a.name, c.name",
            )
            .expect("sanity 2-hop");
        println!("  2-hop paths = {}", r3.rows.len());
        for row in &r3.rows {
            println!("    {:?} -> {:?}", row[0], row[1]);
        }
        // There are exactly 2 unique (a,c) combinations but the engine may
        // return one row per traversal. Record the actual count for the fixture.
        assert!(r3.rows.len() >= 2, "expected at least 2 two-hop paths");
    }

    // ── Archive ─────────────────────────────────────────────────────────────
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent).expect("create output dir");
    }

    // Use system tar to create the archive.
    let status = std::process::Command::new("tar")
        .arg("cf")
        .arg(args.out.as_os_str())
        .arg("-C")
        .arg(tmp.path().as_os_str())
        .arg("v1.0")
        .status()
        .expect("tar command failed to start");

    assert!(
        status.success(),
        "tar exited with non-zero status: {status}"
    );

    println!("  wrote {}", args.out.display());
    println!("Done. Commit {} to the repository.", args.out.display());
}
