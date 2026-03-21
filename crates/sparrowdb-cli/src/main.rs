use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "sparrowdb",
    about = "SparrowDB command-line interface",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a Cypher query and print results as JSON
    Query {
        /// Path to the database directory
        #[arg(long)]
        db: PathBuf,
        /// Cypher query string
        cypher: String,
    },
    /// Flush the WAL and compact the database
    Checkpoint {
        /// Path to the database directory
        #[arg(long)]
        db: PathBuf,
    },
    /// Print database metadata (node count, edge count, current txn_id)
    Info {
        /// Path to the database directory
        #[arg(long)]
        db: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Query { db, cypher } => cmd_query(&db, &cypher),
        Commands::Checkpoint { db } => cmd_checkpoint(&db),
        Commands::Info { db } => cmd_info(&db),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn cmd_query(db_path: &std::path::Path, cypher: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = sparrowdb::GraphDb::open(db_path)?;

    match db.execute(cypher) {
        Ok(result) => {
            // QueryResult does not yet implement Serialize; use Debug for now.
            let json = serde_json::json!({ "rows": format!("{result:?}") });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        Err(e) if e.to_string().contains("not yet implemented") => {
            println!("NotImplemented: Cypher query execution not yet available");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn cmd_checkpoint(db_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = sparrowdb::GraphDb::open(db_path)?;
    db.checkpoint()?;
    println!("Checkpoint complete");
    Ok(())
}

fn cmd_info(db_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = sparrowdb::GraphDb::open(db_path)?;

    // Snapshot a read transaction to obtain the current txn_id.
    let rx = db.begin_read()?;
    let txn_id = rx.snapshot_txn_id;

    // Node/edge counts require catalog + node-store integration that exposes a
    // higher-level API — not yet surfaced on the GraphDb facade.  Report null
    // with a note so callers know these will be populated in a future phase.
    let info = serde_json::json!({
        "db_path": db_path.display().to_string(),
        "txn_id": txn_id,
        "node_count": null,
        "edge_count": null,
        "note": "node_count/edge_count will be populated once the catalog API is promoted"
    });
    println!("{}", serde_json::to_string_pretty(&info)?);
    Ok(())
}
