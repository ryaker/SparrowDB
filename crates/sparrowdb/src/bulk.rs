//! Bulk CSV/edge-list loader for SparrowDB (SPA-296).
//!
//! [`BulkLoader`] batches node and edge creation into large [`WriteTx`]
//! transactions to amortise the per-transaction commit overhead.
//!
//! # Example
//!
//! ```no_run
//! use std::path::Path;
//! use sparrowdb::{GraphDb, bulk::{BulkLoader, BulkOptions}};
//!
//! let db = GraphDb::open(Path::new("/tmp/my.sparrow")).unwrap();
//! let loader = BulkLoader::new(&db, BulkOptions::default());
//! let n = loader.load_nodes(Path::new("nodes.csv"), "Person").unwrap();
//! let e = loader.load_edges(Path::new("edges.csv"), "KNOWS", "Person", "Person").unwrap();
//! eprintln!("loaded {n} nodes, {e} edges");
//! ```

use std::path::Path;

use sparrowdb_common::{Error, Result};

// ── Options ───────────────────────────────────────────────────────────────────

/// Configuration for [`BulkLoader`].
#[derive(Debug, Clone)]
pub struct BulkOptions {
    /// Number of CSV rows processed per [`WriteTx`] commit.
    ///
    /// Larger values reduce commit overhead but increase peak memory usage.
    /// Default: `10_000`.
    pub batch_size: usize,

    /// CSV field delimiter byte.  Default: `b','`.
    pub delimiter: u8,

    /// Whether the CSV file has a header row.  Default: `true`.
    pub has_header: bool,
}

impl Default for BulkOptions {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            delimiter: b',',
            has_header: true,
        }
    }
}

// ── BulkLoader ────────────────────────────────────────────────────────────────

/// Bulk loader that wraps a [`GraphDb`] and batches CSV rows into large
/// [`WriteTx`] transactions.
///
/// The speedup over naïve single-row imports comes from committing thousands of
/// mutations per transaction rather than one, thereby amortising WAL fsync and
/// lock-acquisition overhead.
pub struct BulkLoader<'a> {
    db: &'a crate::GraphDb,
    options: BulkOptions,
}

impl<'a> BulkLoader<'a> {
    /// Create a new `BulkLoader` wrapping `db` with the given `options`.
    pub fn new(db: &'a crate::GraphDb, options: BulkOptions) -> Self {
        Self { db, options }
    }

    /// Load nodes from a CSV file.
    ///
    /// The CSV must contain an `id` column (any type) plus zero or more
    /// additional property columns.  All columns are stored as properties on
    /// the created node with the given `label`.  The `id` column is stored as
    /// a regular `id` property so that [`load_edges`] can look up nodes with
    /// `MATCH (n:Label {id: …})`.
    ///
    /// # Returns
    ///
    /// The number of nodes created.
    pub fn load_nodes(&self, path: &Path, label: &str) -> Result<u64> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.options.delimiter)
            .has_headers(self.options.has_header)
            .from_path(path)
            .map_err(|e| Error::InvalidArgument(format!("csv open error: {e}")))?;

        // Collect header names once.
        let headers: Vec<String> = if self.options.has_header {
            rdr.headers()
                .map_err(|e| Error::InvalidArgument(format!("csv header error: {e}")))?
                .iter()
                .map(|s| s.to_owned())
                .collect()
        } else {
            Vec::new()
        };

        let mut total: u64 = 0;
        let mut batch_rows: Vec<Vec<String>> = Vec::with_capacity(self.options.batch_size);

        let mut record = csv::StringRecord::new();
        loop {
            match rdr.read_record(&mut record) {
                Ok(false) => break,
                Ok(true) => {}
                Err(e) => return Err(Error::InvalidArgument(format!("csv read error: {e}"))),
            }

            let fields: Vec<String> = record.iter().map(|s| s.to_owned()).collect();
            batch_rows.push(fields);
            total += 1;

            if batch_rows.len() >= self.options.batch_size {
                flush_node_batch(self.db, label, &headers, &batch_rows)?;
                eprintln!("  {total} rows loaded...");
                batch_rows.clear();
            }
        }

        // Flush the last (possibly partial) batch.
        flush_node_batch(self.db, label, &headers, &batch_rows)?;
        eprintln!("  {total} rows loaded...");

        Ok(total)
    }

    /// Load edges from a CSV file.
    ///
    /// The CSV must have exactly two columns: `src_id` and `dst_id`.  Both
    /// values are matched against the `id` property of nodes with the
    /// corresponding labels using a Cypher MATCH+CREATE statement.
    ///
    /// Nodes **must** have been loaded via [`load_nodes`] (or otherwise exist)
    /// before calling this method, because unresolvable src/dst ids are silently
    /// skipped (the MATCH returns no rows).
    ///
    /// # Returns
    ///
    /// The number of edges created.
    pub fn load_edges(
        &self,
        path: &Path,
        rel_type: &str,
        src_label: &str,
        dst_label: &str,
    ) -> Result<u64> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.options.delimiter)
            .has_headers(self.options.has_header)
            .from_path(path)
            .map_err(|e| Error::InvalidArgument(format!("csv open error: {e}")))?;

        // Consume/skip header row if present (csv reader already consumed it).
        if self.options.has_header {
            let _ = rdr
                .headers()
                .map_err(|e| Error::InvalidArgument(format!("csv header error: {e}")))?;
        }

        let mut total: u64 = 0;
        let mut batch: Vec<(String, String)> = Vec::with_capacity(self.options.batch_size);

        let mut record = csv::StringRecord::new();
        loop {
            match rdr.read_record(&mut record) {
                Ok(false) => break,
                Ok(true) => {}
                Err(e) => return Err(Error::InvalidArgument(format!("csv read error: {e}"))),
            }

            let src = record.get(0).unwrap_or("").to_owned();
            let dst = record.get(1).unwrap_or("").to_owned();
            if src.is_empty() || dst.is_empty() {
                continue;
            }

            batch.push((src, dst));

            if batch.len() >= self.options.batch_size {
                let n = flush_edge_batch(self.db, rel_type, src_label, dst_label, &batch)?;
                total += n;
                eprintln!("  {total} rows loaded...");
                batch.clear();
            }
        }

        let n = flush_edge_batch(self.db, rel_type, src_label, dst_label, &batch)?;
        total += n;
        eprintln!("  {total} rows loaded...");

        Ok(total)
    }
}

// ── Private batch flush helpers ───────────────────────────────────────────────

/// Commit a batch of node rows in a single [`WriteTx`].
fn flush_node_batch(
    db: &crate::GraphDb,
    label: &str,
    headers: &[String],
    rows: &[Vec<String>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut tx = db.begin_write()?;
    let label_id = tx.get_or_create_label_id(label)?;

    for fields in rows {
        let named_props: Vec<(String, crate::Value)> = headers
            .iter()
            .zip(fields.iter())
            .filter_map(|(name, raw)| {
                if raw.is_empty() {
                    return None;
                }
                let val = parse_value(raw);
                Some((name.clone(), val))
            })
            .collect();

        tx.create_node_named(label_id, &named_props)?;
    }

    tx.commit()?;
    Ok(())
}

/// Execute one Cypher MATCH+CREATE per edge pair, each in its own transaction.
///
/// Returns the number of pairs successfully submitted (matches may still be
/// zero if the nodes don't exist, but that is not an error).
fn flush_edge_batch(
    db: &crate::GraphDb,
    rel_type: &str,
    src_label: &str,
    dst_label: &str,
    pairs: &[(String, String)],
) -> Result<u64> {
    if pairs.is_empty() {
        return Ok(0);
    }

    let mut created: u64 = 0;
    for (src_raw, dst_raw) in pairs {
        let src_val = cypher_literal(src_raw);
        let dst_val = cypher_literal(dst_raw);
        let q = format!(
            "MATCH (a:{src_label} {{id: {src_val}}}), \
             (b:{dst_label} {{id: {dst_val}}}) \
             CREATE (a)-[:{rel_type}]->(b)"
        );
        db.execute(&q)?;
        created += 1;
    }
    Ok(created)
}

// ── Value / literal helpers ───────────────────────────────────────────────────

/// Parse a raw CSV string into a [`Value`].
///
/// Tries integer, then float, then falls back to bytes (UTF-8 string).
fn parse_value(raw: &str) -> crate::Value {
    if let Ok(i) = raw.parse::<i64>() {
        return crate::Value::Int64(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return crate::Value::Float(f);
    }
    crate::Value::Bytes(raw.as_bytes().to_vec())
}

/// Format a raw CSV value as a Cypher literal token.
///
/// Integer strings become bare numbers; everything else becomes a
/// single-quoted string (with internal single-quotes escaped).
fn cypher_literal(raw: &str) -> String {
    if raw.parse::<i64>().is_ok() {
        return raw.to_owned();
    }
    if raw.parse::<f64>().is_ok() {
        return raw.to_owned();
    }
    // String literal: wrap in single quotes, escape embedded single quotes.
    let escaped = raw.replace('\'', "\\'");
    format!("'{escaped}'")
}
