//! Scan + Filter + Project operator tests — RED phase.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::operators::{Filter, LabelScan, Operator, Project};
use sparrowdb_execution::types::Value;
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};

fn setup_person_store(dir: &std::path::Path) -> (NodeStore, Catalog, u32) {
    let mut store = NodeStore::open(dir).expect("node store");
    let mut cat = Catalog::open(dir).expect("catalog");
    let label_id = cat.create_label("Person").expect("Person") as u32;
    // name col = 0, age col = 1
    // Store names as i64 hash (we'll use int for simplicity in unit tests)
    store
        .create_node(
            label_id,
            &[(0, StoreValue::Int64(1)), (1, StoreValue::Int64(30))],
        )
        .expect("Alice");
    store
        .create_node(
            label_id,
            &[(0, StoreValue::Int64(2)), (1, StoreValue::Int64(25))],
        )
        .expect("Bob");
    store
        .create_node(
            label_id,
            &[(0, StoreValue::Int64(3)), (1, StoreValue::Int64(40))],
        )
        .expect("Carol");
    (store, cat, label_id)
}

#[test]
fn label_scan_returns_all_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _cat, label_id) = setup_person_store(dir.path());

    let mut scan = LabelScan::new(&store, label_id, &[0, 1]);
    let mut total_rows = 0usize;
    while let Some(chunk) = scan.next_chunk().expect("scan ok") {
        for group in &chunk.groups {
            total_rows += group.len();
        }
    }
    assert_eq!(total_rows, 3, "should scan 3 Person nodes");
}

#[test]
fn filter_by_col_value() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _cat, label_id) = setup_person_store(dir.path());

    let mut scan = LabelScan::new(&store, label_id, &[0, 1]);
    let mut filter = Filter::new(&mut scan, "col_0", Value::Int64(2)); // Bob
    let mut matched = 0usize;
    while let Some(chunk) = filter.next_chunk().expect("filter ok") {
        for group in &chunk.groups {
            matched += group.len();
        }
    }
    assert_eq!(matched, 1, "only Bob should pass filter");
}

#[test]
fn project_selects_columns() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _cat, label_id) = setup_person_store(dir.path());

    let mut scan = LabelScan::new(&store, label_id, &[0, 1]);
    let mut proj = Project::new(&mut scan, vec!["col_0".to_string()]);
    let mut chunks = Vec::new();
    while let Some(chunk) = proj.next_chunk().expect("project ok") {
        chunks.push(chunk);
    }
    assert!(!chunks.is_empty());
    // Each group should only have col_0
    for chunk in &chunks {
        for group in &chunk.groups {
            assert!(group.has_column("col_0"), "projected chunk must have col_0");
            assert!(
                !group.has_column("col_1"),
                "projected chunk must not have col_1"
            );
        }
    }
}

/// #560 audit: `LabelScan` reads each column with `get_node_raw`, whose
/// zero-sentinel turns a property the node never stored into `Int64(0)`, and
/// `TypedVector::Int64` has no null channel to say otherwise. Fixture (col 1):
/// node1 = 0 (stored), node2 = 4, node3 = absent. `col_1 == 0` must match
/// node1 only; the hand-derived expectation is 1 row, the engine returns 2.
#[test]
#[ignore = "bug #562: LabelScan has no null channel; absent col reads as Int64(0)"]
fn label_scan_filter_zero_does_not_match_absent_column() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = NodeStore::open(dir.path()).expect("node store");
    let mut cat = Catalog::open(dir.path()).expect("catalog");
    let label_id = cat.create_label("W").expect("W") as u32;
    store
        .create_node(
            label_id,
            &[(0, StoreValue::Int64(1)), (1, StoreValue::Int64(0))],
        )
        .unwrap();
    store
        .create_node(
            label_id,
            &[(0, StoreValue::Int64(2)), (1, StoreValue::Int64(4))],
        )
        .unwrap();
    store
        .create_node(label_id, &[(0, StoreValue::Int64(3))])
        .unwrap();

    let mut scan = LabelScan::new(&store, label_id, &[0, 1]);
    let mut filter = Filter::new(&mut scan, "col_1", Value::Int64(0));
    let mut matched = 0usize;
    while let Some(chunk) = filter.next_chunk().expect("filter ok") {
        for group in &chunk.groups {
            matched += group.len();
        }
    }
    assert_eq!(matched, 1, "only node1 stores col_1 = 0");
}
