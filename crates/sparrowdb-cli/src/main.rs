use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sparrowdb_execution::{query_result_to_json, value_to_json};
use std::collections::HashMap;
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
    /// Import a Neo4j APOC CSV export (nodes + relationships) into SparrowDB.
    ///
    /// Accepts the two-file APOC exportGraph format:
    ///   nodes CSV:    _id,_labels,prop1,prop2,...
    ///   rels  CSV:    _start,_end,_type,prop1,...
    ///
    /// Example:
    ///   sparrowdb import --neo4j-csv nodes.csv,rels.csv --db /path/to/db
    Import {
        /// Comma-separated pair: nodes_csv,relationships_csv
        #[arg(long = "neo4j-csv")]
        neo4j_csv: String,
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
        Commands::Import { neo4j_csv, db } => cmd_import(&db, &neo4j_csv),
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

// ── Import (Neo4j APOC CSV) ────────────────────────────────────────────────────

/// Parse a single CSV line, respecting double-quoted fields.
///
/// Handles `""` as an escaped `"` inside a quoted field, bare (unquoted)
/// fields, and empty fields.  Returns one `String` per column.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    // Doubled quote inside a quoted field → literal `"`.
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            }
            '"' => {
                in_quotes = true;
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            other => {
                current.push(other);
            }
        }
    }
    fields.push(current);
    fields
}

/// Extract the primary label from an APOC `_labels` cell like `:Person:Knowledge`.
///
/// Returns the first non-empty segment after stripping leading `:`s.
/// Falls back to `"Node"` if no label is present.
fn primary_label(labels_cell: &str) -> &str {
    let stripped = labels_cell.trim_start_matches(':');
    stripped
        .split(':')
        .find(|s| !s.is_empty())
        .unwrap_or("Node")
}

fn cmd_import(
    db_path: &std::path::Path,
    neo4j_csv: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse the comma-separated file pair: "nodes.csv,rels.csv"
    let parts: Vec<&str> = neo4j_csv.splitn(2, ',').collect();
    if parts.len() != 2 {
        return Err(
            "--neo4j-csv must be two comma-separated file paths: nodes.csv,relationships.csv"
                .into(),
        );
    }
    let nodes_path = std::path::Path::new(parts[0].trim());
    let rels_path = std::path::Path::new(parts[1].trim());

    let db = sparrowdb::GraphDb::open(db_path)?;

    // ── Phase 1: Import nodes ─────────────────────────────────────────────────

    // neo4j_id (string) → SparrowDB NodeId packed u64 (for wiring edges).
    let mut id_map: HashMap<String, sparrowdb::NodeId> = HashMap::new();
    let mut node_count: u64 = 0;
    let mut labels_seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    {
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(nodes_path)
            .map_err(|e| format!("cannot open nodes CSV '{}': {e}", nodes_path.display()))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // Header row: _id,_labels,prop1,prop2,...
        let header_line = lines.next().ok_or("nodes CSV is empty")??;
        let headers = parse_csv_line(&header_line);
        if headers.len() < 2 {
            return Err("nodes CSV header must have at least _id and _labels columns".into());
        }

        let id_col = headers
            .iter()
            .position(|h| h == "_id")
            .ok_or("nodes CSV missing '_id' column")?;
        let labels_col = headers
            .iter()
            .position(|h| h == "_labels")
            .ok_or("nodes CSV missing '_labels' column")?;

        // Remaining columns are node properties.
        let prop_cols: Vec<(usize, String)> = headers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != id_col && *i != labels_col)
            .map(|(i, h)| (i, h.clone()))
            .collect();

        for (line_no, line_result) in lines.enumerate() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            let fields = parse_csv_line(&line);

            let neo_id = fields
                .get(id_col)
                .ok_or_else(|| format!("nodes CSV row {}: missing _id field", line_no + 2))?
                .clone();
            let label_cell = fields.get(labels_col).map(|s| s.as_str()).unwrap_or("");
            let label = primary_label(label_cell).to_string();
            labels_seen.insert(label.clone());

            // Create the node via WriteTx — avoids Cypher parse overhead and
            // gives us the NodeId directly for edge wiring.
            let named_props: Vec<(String, sparrowdb::Value)> = prop_cols
                .iter()
                .filter_map(|(col_idx, prop_name)| {
                    let raw = fields.get(*col_idx)?.as_str();
                    if raw.is_empty() {
                        return None; // skip null/missing
                    }
                    let val = if let Ok(i) = raw.parse::<i64>() {
                        sparrowdb::Value::Int64(i)
                    } else {
                        sparrowdb::Value::Bytes(raw.as_bytes().to_vec())
                    };
                    Some((prop_name.clone(), val))
                })
                .collect();

            let mut tx = db.begin_write()?;
            let label_id = tx.get_or_create_label_id(&label)?;
            let node_id = tx
                .create_node_named(label_id, &named_props)
                .map_err(|e| format!("nodes CSV row {}: create_node failed: {e}", line_no + 2))?;
            tx.commit()?;

            id_map.insert(neo_id, node_id);
            node_count += 1;

            if node_count.is_multiple_of(1000) {
                eprintln!("  nodes: {node_count} processed");
            }
        }
    }

    eprintln!("nodes imported: {node_count}");

    // ── Phase 2: Import relationships ─────────────────────────────────────────

    let mut edge_count: u64 = 0;
    let mut rel_types_seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    {
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(rels_path)
            .map_err(|e| format!("cannot open rels CSV '{}': {e}", rels_path.display()))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // Header row: _start,_end,_type,prop1,...
        let header_line = lines.next().ok_or("rels CSV is empty")??;
        let headers = parse_csv_line(&header_line);

        let start_col = headers
            .iter()
            .position(|h| h == "_start")
            .ok_or("rels CSV missing '_start' column")?;
        let end_col = headers
            .iter()
            .position(|h| h == "_end")
            .ok_or("rels CSV missing '_end' column")?;
        let type_col = headers
            .iter()
            .position(|h| h == "_type")
            .ok_or("rels CSV missing '_type' column")?;

        for (line_no, line_result) in lines.enumerate() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }
            let fields = parse_csv_line(&line);

            let start_id_str = fields
                .get(start_col)
                .map(|s| s.as_str())
                .ok_or_else(|| format!("rels CSV row {}: missing _start", line_no + 2))?;
            let end_id_str = fields
                .get(end_col)
                .map(|s| s.as_str())
                .ok_or_else(|| format!("rels CSV row {}: missing _end", line_no + 2))?;
            let rel_type = fields
                .get(type_col)
                .ok_or_else(|| format!("rels CSV row {}: missing _type", line_no + 2))?
                .clone();

            let src = id_map.get(start_id_str).copied().ok_or_else(|| {
                format!(
                    "rels CSV row {}: unknown _start id '{start_id_str}'",
                    line_no + 2
                )
            })?;
            let dst = id_map.get(end_id_str).copied().ok_or_else(|| {
                format!(
                    "rels CSV row {}: unknown _end id '{end_id_str}'",
                    line_no + 2
                )
            })?;

            rel_types_seen.insert(rel_type.clone());

            // Create the edge via the WriteTx API directly (avoids Cypher
            // parse overhead for the hot path).
            let mut tx = db.begin_write()?;
            tx.create_edge(src, dst, &rel_type, HashMap::new())?;
            tx.commit()?;

            edge_count += 1;

            if edge_count.is_multiple_of(1000) {
                eprintln!("  edges: {edge_count} processed");
            }
        }
    }

    eprintln!("edges imported: {edge_count}");

    // ── Summary ───────────────────────────────────────────────────────────────

    let mut labels_sorted: Vec<_> = labels_seen.into_iter().collect();
    labels_sorted.sort();
    let mut rel_types_sorted: Vec<_> = rel_types_seen.into_iter().collect();
    rel_types_sorted.sort();

    let summary = serde_json::json!({
        "nodes": node_count,
        "edges": edge_count,
        "labels": labels_sorted,
        "rel_types": rel_types_sorted,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
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
