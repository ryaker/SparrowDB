//! `ldbc-load` — CLI binary for the LDBC SNB data loader.
//!
//! ## Usage
//!
//! ```text
//! cargo run -p sparrowdb-bench --bin ldbc-load -- \
//!     --data-dir /path/to/ldbc/sf-0.1 \
//!     --db-path  /tmp/ldbc.sparrow
//! ```

use clap::Parser;
use sparrowdb::GraphDb;
use sparrowdb_bench::load;
use std::path::PathBuf;

/// Load LDBC SNB CSV data into a SparrowDB database.
#[derive(Parser, Debug)]
#[command(
    name = "ldbc-load",
    about = "LDBC SNB CSV → SparrowDB loader (SPA-145)"
)]
struct Args {
    /// Path to the LDBC SNB data directory (contains dynamic/ and/or static/).
    #[arg(long, default_value = "crates/sparrowdb-bench/fixtures/ldbc/mini")]
    data_dir: PathBuf,

    /// Path to the SparrowDB database to create or open.
    #[arg(long, default_value = "/tmp/ldbc.sparrow")]
    db_path: PathBuf,

    /// Run CHECKPOINT after loading to fold delta log into CSR base files.
    #[arg(long, default_value_t = true)]
    checkpoint: bool,
}

fn main() -> sparrowdb::Result<()> {
    let args = Args::parse();

    eprintln!("[ldbc-load] Opening database at {}", args.db_path.display());
    let db = GraphDb::open(&args.db_path)?;

    let stats = load(&db, &args.data_dir)?;

    if args.checkpoint && (stats.nodes_loaded > 0 || stats.edges_loaded > 0) {
        eprintln!("[ldbc-load] Running CHECKPOINT...");
        db.checkpoint()?;
        eprintln!("[ldbc-load] CHECKPOINT complete.");
    }

    eprintln!(
        "[ldbc-load] Done. {} nodes, {} edges, {} errors.",
        stats.nodes_loaded, stats.edges_loaded, stats.errors
    );
    Ok(())
}
