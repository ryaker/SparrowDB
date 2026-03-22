//! Integration tests for the Neo4j → SparrowDB import bridge (SPA-148).
//!
//! Tests cover:
//!   - `import_nodes_csv`         — parse a small nodes CSV, verify node count
//!   - `import_relationships_csv` — parse a rels CSV, verify edge count
//!   - `import_roundtrip`         — import then query back via MATCH
//!
//! The import bridge operates at the `WriteTx` level via the public
//! `get_or_create_label_id` + `create_node_named` + `create_edge` API, which
//! is the same code path used by the CLI `import` subcommand.

use sparrowdb::{open, GraphDb};
use sparrowdb_common::NodeId;
use sparrowdb_execution::Value as ExecValue;
use std::collections::HashMap;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open db");
    (dir, db)
}

/// Minimal CSV parser matching the one in `sparrowdb-cli/src/main.rs`.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            }
            '"' => in_quotes = true,
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            other => current.push(other),
        }
    }
    fields.push(current);
    fields
}

fn primary_label(labels_cell: &str) -> &str {
    let stripped = labels_cell.trim_start_matches(':');
    stripped
        .split(':')
        .find(|s| !s.is_empty())
        .unwrap_or("Node")
}

/// Import nodes from a multi-line CSV string (header + data rows).
///
/// Returns a map from Neo4j string _id → SparrowDB NodeId.
fn import_nodes(db: &GraphDb, csv: &str) -> HashMap<String, NodeId> {
    let mut id_map: HashMap<String, NodeId> = HashMap::new();
    let mut lines = csv.lines();

    let header_line = lines.next().expect("nodes CSV must have a header");
    let headers = parse_csv_line(header_line);

    let id_col = headers
        .iter()
        .position(|h| h == "_id")
        .expect("missing _id");
    let labels_col = headers
        .iter()
        .position(|h| h == "_labels")
        .expect("missing _labels");
    let prop_cols: Vec<(usize, String)> = headers
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != id_col && *i != labels_col)
        .map(|(i, h)| (i, h.clone()))
        .collect();

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(line);
        let neo_id = fields[id_col].clone();
        let label = primary_label(&fields[labels_col]).to_string();

        let named_props: Vec<(String, sparrowdb::Value)> = prop_cols
            .iter()
            .filter_map(|(col_idx, prop_name)| {
                let raw = fields.get(*col_idx)?.as_str();
                if raw.is_empty() {
                    return None;
                }
                let val = if let Ok(i) = raw.parse::<i64>() {
                    sparrowdb::Value::Int64(i)
                } else {
                    sparrowdb::Value::Bytes(raw.as_bytes().to_vec())
                };
                Some((prop_name.clone(), val))
            })
            .collect();

        let mut tx = db.begin_write().expect("begin_write");
        let label_id = tx
            .get_or_create_label_id(&label)
            .expect("get_or_create_label_id");
        let node_id = tx
            .create_node_named(label_id, &named_props)
            .expect("create_node_named");
        tx.commit().expect("commit");
        id_map.insert(neo_id, node_id);
    }
    id_map
}

/// Import relationships from a multi-line CSV string using the supplied id_map.
fn import_rels(db: &GraphDb, csv: &str, id_map: &HashMap<String, NodeId>) -> u64 {
    let mut edge_count: u64 = 0;
    let mut lines = csv.lines();

    let header_line = lines.next().expect("rels CSV must have a header");
    let headers = parse_csv_line(header_line);
    let start_col = headers
        .iter()
        .position(|h| h == "_start")
        .expect("missing _start");
    let end_col = headers
        .iter()
        .position(|h| h == "_end")
        .expect("missing _end");
    let type_col = headers
        .iter()
        .position(|h| h == "_type")
        .expect("missing _type");

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(line);
        let src = *id_map
            .get(fields[start_col].as_str())
            .unwrap_or_else(|| panic!("unknown _start id: {}", fields[start_col]));
        let dst = *id_map
            .get(fields[end_col].as_str())
            .unwrap_or_else(|| panic!("unknown _end id: {}", fields[end_col]));
        let rel_type = fields[type_col].clone();

        let mut tx = db.begin_write().expect("begin_write");
        tx.create_edge(src, dst, &rel_type, HashMap::new())
            .expect("create_edge");
        tx.commit().expect("commit");
        edge_count += 1;
    }
    edge_count
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Parse a small nodes CSV and verify that the correct number of nodes are
/// created across the expected labels.
#[test]
fn import_nodes_csv() {
    let (_dir, db) = make_db();

    let nodes_csv = "\
_id,_labels,name,age
1,:Person,Alice,30
2,:Person,Bob,25
3,:Organization,Acme,
4,:Knowledge,GraphDBs,
5,:Technology,Rust,
";

    let id_map = import_nodes(&db, nodes_csv);

    // 5 nodes were imported.
    assert_eq!(id_map.len(), 5, "expected 5 nodes in id_map");

    // All 5 Neo4j ids are present.
    for id in &["1", "2", "3", "4", "5"] {
        assert!(id_map.contains_key(*id), "id_map missing key {id}");
    }

    // Verify counts via MATCH.
    let r = db.execute("MATCH (n:Person) RETURN COUNT(*)").unwrap();
    assert_eq!(r.rows[0][0], ExecValue::Int64(2), "expected 2 Person nodes");

    let r = db
        .execute("MATCH (n:Organization) RETURN COUNT(*)")
        .unwrap();
    assert_eq!(
        r.rows[0][0],
        ExecValue::Int64(1),
        "expected 1 Organization node"
    );

    let r = db.execute("MATCH (n:Knowledge) RETURN COUNT(*)").unwrap();
    assert_eq!(
        r.rows[0][0],
        ExecValue::Int64(1),
        "expected 1 Knowledge node"
    );

    let r = db.execute("MATCH (n:Technology) RETURN COUNT(*)").unwrap();
    assert_eq!(
        r.rows[0][0],
        ExecValue::Int64(1),
        "expected 1 Technology node"
    );
}

/// Parse a rels CSV and verify the correct number of edges are created.
#[test]
fn import_relationships_csv() {
    let (_dir, db) = make_db();

    let nodes_csv = "\
_id,_labels,name
1,:Person,Alice
2,:Person,Bob
3,:Organization,Acme
";

    let rels_csv = "\
_start,_end,_type
1,2,KNOWS
1,3,WORKS_AT
2,3,WORKS_AT
";

    let id_map = import_nodes(&db, nodes_csv);
    let edge_count = import_rels(&db, rels_csv, &id_map);

    assert_eq!(edge_count, 3, "expected 3 edges");

    // Verify via MATCH with edge pattern.
    let r = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN COUNT(*)")
        .unwrap();
    assert_eq!(r.rows[0][0], ExecValue::Int64(1), "expected 1 KNOWS edge");

    let r = db
        .execute("MATCH (a:Person)-[:WORKS_AT]->(b:Organization) RETURN COUNT(*)")
        .unwrap();
    assert_eq!(
        r.rows[0][0],
        ExecValue::Int64(2),
        "expected 2 WORKS_AT edges"
    );
}

/// Full round-trip: import KMS-schema nodes + relationships, then query back
/// the data via MATCH to confirm node properties and edge connectivity are
/// persisted correctly.
#[test]
fn import_roundtrip() {
    let (_dir, db) = make_db();

    // KMS-representative data: Knowledge nodes connected via RELATED_TO / ABOUT.
    let nodes_csv = "\
_id,_labels,title,score
10,:Knowledge,\"Graph Databases\",95
11,:Knowledge,\"Property Graphs\",88
12,:Person,Alice,
13,:Project,\"KMS\",
";

    let rels_csv = "\
_start,_end,_type
10,11,RELATED_TO
12,10,ABOUT
12,13,MENTIONS
";

    let id_map = import_nodes(&db, nodes_csv);
    let edge_count = import_rels(&db, rels_csv, &id_map);

    assert_eq!(id_map.len(), 4, "expected 4 nodes");
    assert_eq!(edge_count, 3, "expected 3 edges");

    // Query node properties.
    let r = db
        .execute("MATCH (n:Knowledge) RETURN n.title ORDER BY n.score DESC")
        .unwrap_or_else(|e| panic!("MATCH Knowledge failed: {e}"));
    assert_eq!(r.rows.len(), 2, "expected 2 Knowledge nodes");

    // Verify connectivity: Alice (Person) is ABOUT Graph Databases (Knowledge).
    let r = db
        .execute("MATCH (p:Person)-[:ABOUT]->(k:Knowledge) RETURN COUNT(*)")
        .unwrap();
    assert_eq!(r.rows[0][0], ExecValue::Int64(1), "1 ABOUT relationship");

    // Total edges in the graph.
    let r = db.execute("MATCH ()-[r]->() RETURN COUNT(*)").unwrap();
    assert_eq!(r.rows[0][0], ExecValue::Int64(3), "3 total edges");

    // Check that string property round-trip works (title contains a space).
    let r = db.execute("MATCH (n:Project) RETURN n.title").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0][0],
        ExecValue::String("KMS".to_string()),
        "Project title must round-trip correctly"
    );
}

/// Verify that integer properties survive the import round-trip without
/// type coercion.
#[test]
fn import_integer_properties() {
    let (_dir, db) = make_db();

    let nodes_csv = "\
_id,_labels,name,age,score
1,:Person,Alice,30,95
2,:Person,Bob,25,88
";

    let id_map = import_nodes(&db, nodes_csv);
    assert_eq!(id_map.len(), 2);

    let r = db
        .execute("MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "exactly 1 person with age=30");
    assert_eq!(
        r.rows[0][0],
        ExecValue::String("Alice".to_string()),
        "name must be Alice"
    );
}

/// Verify the `get_or_create_label_id` is idempotent: repeated calls with the
/// same label name must return the same id and not create duplicate label entries.
#[test]
fn get_or_create_label_id_is_idempotent() {
    let (_dir, db) = make_db();

    let id1 = {
        let mut tx = db.begin_write().unwrap();
        let id = tx.get_or_create_label_id("Person").unwrap();
        tx.commit().unwrap();
        id
    };

    let id2 = {
        let mut tx = db.begin_write().unwrap();
        let id = tx.get_or_create_label_id("Person").unwrap();
        tx.commit().unwrap();
        id
    };

    assert_eq!(id1, id2, "label id must be stable across transactions");
}
