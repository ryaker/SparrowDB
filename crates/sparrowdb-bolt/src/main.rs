//! SparrowDB Bolt Protocol Server
//!
//! Accepts connections from Bolt-compatible clients (gdotv, Neo4j Browser, etc.)
//! and routes Cypher queries to a SparrowDB database.

use clap::Parser;
use sparrowdb::GraphDb;
use std::path::PathBuf;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[command(name = "sparrowdb-bolt", about = "Bolt protocol server for SparrowDB")]
struct Args {
    /// Path to the SparrowDB database directory.
    #[arg(long, env = "SPARROWDB_PATH")]
    db_path: PathBuf,

    /// Host to bind to.
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on.
    #[arg(long, default_value_t = 7687)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sparrowdb_bolt=info".into()),
        )
        .init();

    let args = Args::parse();
    let db = GraphDb::open(&args.db_path)?;
    tracing::info!("opened database at {:?}", args.db_path);

    let bind_addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&bind_addr).await?;
    tracing::info!("bolt server listening on {bind_addr}");

    loop {
        let (stream, _addr) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            sparrowdb_bolt::handle_connection(stream, db).await;
        });
    }
}
