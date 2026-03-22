use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sparrowdb_execution::{query_result_to_json, value_to_json};
use std::io::{self, BufRead, Write};
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
    /// Open a database and serve Cypher queries via NDJSON on stdin/stdout.
    ///
    /// Request format (one JSON object per line on stdin):
    ///   {"id": "q1", "cypher": "MATCH (n) RETURN n LIMIT 10"}
    ///
    /// Response format (one JSON object per line on stdout):
    ///   {"id": "q1", "columns": ["n"], "rows": [{"n": ...}], "error": null}
    ///   {"id": "q1", "columns": null, "rows": null, "error": "some error"}
    ///
    /// The process exits when stdin is closed.
    Serve {
        /// Path to the database directory
        #[arg(long)]
        db: PathBuf,
        /// Hex-encoded 32-byte encryption key (not yet implemented — see SPA-97).
        /// Passing this flag will return an error until at-rest encryption lands.
        #[arg(long)]
        key: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Query { db, cypher } => cmd_query(&db, &cypher),
        Commands::Checkpoint { db } => cmd_checkpoint(&db),
        Commands::Info { db } => cmd_info(&db),
        Commands::Serve { db, key } => cmd_serve(&db, key.as_deref()),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

// ── Commands ──────────────────────────────────────────────────────────────────

fn cmd_query(db_path: &std::path::Path, cypher: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db = sparrowdb::GraphDb::open(db_path)?;
    match db.execute(cypher) {
        Ok(result) => {
            let json = query_result_to_json(&result);
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
    let rx = db.begin_read()?;
    let txn_id = rx.snapshot_txn_id;
    let (node_count, edge_count) = db.db_counts()?;
    let info = serde_json::json!({
        "db_path": db_path.display().to_string(),
        "txn_id": txn_id,
        "node_count": node_count,
        "edge_count": edge_count,
    });
    println!("{}", serde_json::to_string_pretty(&info)?);
    Ok(())
}

// ── Serve mode ────────────────────────────────────────────────────────────────

/// One NDJSON request line from the caller.
#[derive(Deserialize)]
struct ServeRequest {
    /// Caller-supplied correlation id — echoed back in the response unchanged.
    id: serde_json::Value,
    /// Cypher query to execute.
    cypher: String,
}

/// One NDJSON response line sent back to the caller.
#[derive(Serialize)]
struct ServeResponse {
    /// Echoed correlation id from the request (null for parse errors).
    id: serde_json::Value,
    /// Column names in projection order; null on error.
    columns: Option<Vec<String>>,
    /// Rows as column-name → value objects; null on error.
    rows: Option<Vec<serde_json::Value>>,
    /// Human-readable error message; null on success.
    error: Option<String>,
}

fn cmd_serve(
    db_path: &std::path::Path,
    key: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if key.is_some() {
        // Encryption is not yet wired through to the storage layer (SPA-97 /
        // SPA-98).  Refuse to continue rather than silently opening the
        // database unencrypted and giving the caller a false sense of security.
        return Err(
            "sparrowdb serve --key: at-rest encryption is not yet available. \
             Remove --key to open the database without encryption, or wait for \
             SPA-97 to land."
                .into(),
        );
    }
    let db = sparrowdb::GraphDb::open(db_path)?;

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<ServeRequest>(&line) {
            Ok(req) => match db.execute(&req.cypher) {
                Ok(result) => {
                    let rows: Vec<serde_json::Value> = result
                        .rows
                        .iter()
                        .map(|row| {
                            let obj: serde_json::Map<String, serde_json::Value> = result
                                .columns
                                .iter()
                                .zip(row.iter())
                                .map(|(col, val)| (col.clone(), value_to_json(val)))
                                .collect();
                            serde_json::Value::Object(obj)
                        })
                        .collect();
                    ServeResponse {
                        id: req.id,
                        columns: Some(result.columns),
                        rows: Some(rows),
                        error: None,
                    }
                }
                Err(e) => ServeResponse {
                    id: req.id,
                    columns: None,
                    rows: None,
                    error: Some(e.to_string()),
                },
            },
            Err(e) => ServeResponse {
                id: serde_json::Value::Null,
                columns: None,
                rows: None,
                error: Some(format!("parse error: {e}")),
            },
        };

        match serde_json::to_string(&response) {
            Ok(resp_str) => {
                if writeln!(out, "{resp_str}").is_err() {
                    break;
                }
                if out.flush().is_err() {
                    break;
                }
            }
            Err(e) => {
                // JSON serialization failure is a bug — log to stderr so the
                // caller isn't left waiting for a response that never arrives.
                eprintln!("sparrowdb-cli: failed to serialize response: {e}");
            }
        }
    }
    Ok(())
}
