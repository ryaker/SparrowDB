//! Unit tests for Phase 2 pipeline operators (SPA-299, #338).
//!
//! Covers:
//! - T1: `ReadNodeProps` on empty input returns `Ok(None)`.
//! - T2: Single chunk, all rows live — reads correct property values.
//! - T3: Null handling — slots with missing property get null bitmap bit set.
//! - T4: Selection vector — pre-filtered rows produce zero I/O (filtered slots
//!   are absent from output columns' data).

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::col_id_of;
use sparrowdb_execution::chunk::{ColumnVector, DataChunk, COL_ID_SLOT};
use sparrowdb_execution::pipeline::{PipelineOperator, ReadNodeProps, ScanByLabel};
use sparrowdb_storage::node_store::{NodeStore, Value as StoreValue};
use std::sync::Arc;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a minimal test database.
/// Returns `(store, label_id)`.
///
/// Nodes written (in order, slots 0..2):
/// - slot 0: age=30, score=100
/// - slot 1: age=25, score=200
/// - slot 2: age=40, score=300
fn setup_three_nodes(dir: &std::path::Path) -> (NodeStore, u32) {
    let mut store = NodeStore::open(dir).expect("node store");
    let mut cat = Catalog::open(dir).expect("catalog");
    let label_id = cat.create_label("Person").expect("Person") as u32;

    let age_col = col_id_of("age");
    let score_col = col_id_of("score");

    store
        .create_node(
            label_id,
            &[
                (age_col, StoreValue::Int64(30)),
                (score_col, StoreValue::Int64(100)),
            ],
        )
        .expect("node 0");
    store
        .create_node(
            label_id,
            &[
                (age_col, StoreValue::Int64(25)),
                (score_col, StoreValue::Int64(200)),
            ],
        )
        .expect("node 1");
    store
        .create_node(
            label_id,
            &[
                (age_col, StoreValue::Int64(40)),
                (score_col, StoreValue::Int64(300)),
            ],
        )
        .expect("node 2");

    (store, label_id)
}

/// A `PipelineOperator` source that yields a single pre-built `DataChunk` then
/// returns `None`.  Mirrors `SingleChunkSource` inside the engine.
struct OnceSource(Option<DataChunk>);

impl PipelineOperator for OnceSource {
    fn next_chunk(&mut self) -> sparrowdb_common::Result<Option<DataChunk>> {
        Ok(self.0.take())
    }
}

// ── T1: ReadNodeProps on empty input returns None ─────────────────────────────

#[test]
fn read_node_props_empty_input_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let (store, label_id) = setup_three_nodes(dir.path());
    let store_arc = Arc::new(store);

    let age_col = col_id_of("age");

    // Source that immediately returns None.
    let empty_src = OnceSource(None);
    let mut rnp = ReadNodeProps::new(empty_src, store_arc, label_id, COL_ID_SLOT, vec![age_col]);

    let result = rnp.next_chunk().expect("no error");
    assert!(
        result.is_none(),
        "ReadNodeProps on empty input must return None"
    );
}

// ── T2: Single chunk, all rows live — reads correct property values ────────────

#[test]
fn read_node_props_all_live_reads_correct_values() {
    let dir = tempfile::tempdir().unwrap();
    let (store, label_id) = setup_three_nodes(dir.path());
    let store_arc = Arc::new(store);

    let age_col = col_id_of("age");
    let score_col = col_id_of("score");

    // Scan slots 0..3.
    let scan = ScanByLabel::new(3);
    let mut rnp = ReadNodeProps::new(
        scan,
        Arc::clone(&store_arc),
        label_id,
        COL_ID_SLOT,
        vec![age_col, score_col],
    );

    let chunk = rnp
        .next_chunk()
        .expect("no error")
        .expect("must have a chunk");

    assert_eq!(chunk.live_len(), 3, "all 3 rows must be live");

    let age_vec = chunk.find_column(age_col).expect("age column");
    let score_vec = chunk.find_column(score_col).expect("score column");

    // Collect live rows.
    let live: Vec<usize> = chunk.live_rows().collect();
    assert_eq!(live.len(), 3);

    // Slot 0: age=30, score=100.
    assert!(
        !age_vec.nulls.is_null(live[0]),
        "slot 0 age must be non-null"
    );
    assert!(
        !score_vec.nulls.is_null(live[0]),
        "slot 0 score must be non-null"
    );
    let age_val_0 = StoreValue::from_u64(age_vec.data[live[0]]);
    let score_val_0 = StoreValue::from_u64(score_vec.data[live[0]]);
    assert_eq!(age_val_0, StoreValue::Int64(30), "slot 0 age must be 30");
    assert_eq!(
        score_val_0,
        StoreValue::Int64(100),
        "slot 0 score must be 100"
    );

    // Slot 1: age=25, score=200.
    assert!(
        !age_vec.nulls.is_null(live[1]),
        "slot 1 age must be non-null"
    );
    let age_val_1 = StoreValue::from_u64(age_vec.data[live[1]]);
    let score_val_1 = StoreValue::from_u64(score_vec.data[live[1]]);
    assert_eq!(age_val_1, StoreValue::Int64(25), "slot 1 age must be 25");
    assert_eq!(
        score_val_1,
        StoreValue::Int64(200),
        "slot 1 score must be 200"
    );

    // Slot 2: age=40, score=300.
    let age_val_2 = StoreValue::from_u64(age_vec.data[live[2]]);
    let score_val_2 = StoreValue::from_u64(score_vec.data[live[2]]);
    assert_eq!(age_val_2, StoreValue::Int64(40), "slot 2 age must be 40");
    assert_eq!(
        score_val_2,
        StoreValue::Int64(300),
        "slot 2 score must be 300"
    );

    // Exhausted after one chunk.
    let next = rnp.next_chunk().expect("no error");
    assert!(next.is_none(), "must be exhausted after reading 3 slots");
}

// ── T3: Null handling — slots missing a property get the null bitmap bit set ──

#[test]
fn read_node_props_null_for_missing_property() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = NodeStore::open(dir.path()).expect("node store");
    let mut cat = Catalog::open(dir.path()).expect("catalog");
    let label_id = cat.create_label("Item").expect("Item") as u32;

    let name_col = col_id_of("name");
    let rare_col = col_id_of("rare_prop");

    // Node 0: has both name and rare_prop.
    store
        .create_node(
            label_id,
            &[
                (name_col, StoreValue::Int64(1)),
                (rare_col, StoreValue::Int64(42)),
            ],
        )
        .expect("node 0");
    // Node 1: has only name, NO rare_prop.
    store
        .create_node(label_id, &[(name_col, StoreValue::Int64(2))])
        .expect("node 1");
    // Node 2: has both.
    store
        .create_node(
            label_id,
            &[
                (name_col, StoreValue::Int64(3)),
                (rare_col, StoreValue::Int64(99)),
            ],
        )
        .expect("node 2");

    let store_arc = Arc::new(store);

    let scan = ScanByLabel::new(3);
    let mut rnp = ReadNodeProps::new(
        scan,
        Arc::clone(&store_arc),
        label_id,
        COL_ID_SLOT,
        vec![name_col, rare_col],
    );

    let chunk = rnp.next_chunk().expect("no error").expect("chunk");

    let live: Vec<usize> = chunk.live_rows().collect();
    assert_eq!(live.len(), 3);

    let rare_vec = chunk.find_column(rare_col).expect("rare_col column");

    // Slot 0: rare_prop present.
    assert!(
        !rare_vec.nulls.is_null(live[0]),
        "slot 0 rare_prop must be non-null"
    );
    assert_eq!(
        StoreValue::from_u64(rare_vec.data[live[0]]),
        StoreValue::Int64(42),
        "slot 0 rare_prop must be 42"
    );

    // Slot 1: rare_prop ABSENT — must be null.
    assert!(
        rare_vec.nulls.is_null(live[1]),
        "slot 1 rare_prop must be null (property was never written)"
    );

    // Slot 2: rare_prop present.
    assert!(
        !rare_vec.nulls.is_null(live[2]),
        "slot 2 rare_prop must be non-null"
    );
    assert_eq!(
        StoreValue::from_u64(rare_vec.data[live[2]]),
        StoreValue::Int64(99),
        "slot 2 rare_prop must be 99"
    );
}

// ── T4: Selection vector — pre-filtered rows produce zero I/O ─────────────────
//
// Build a 3-row chunk with a selection vector that keeps only slot 1.
// ReadNodeProps must:
//   - preserve the selection vector (live_len() == 1)
//   - only read storage for slot 1 — dead rows (0 and 2) get data = 0 (no I/O)
//   - the live row (slot 1, age=25) must have the correct non-null value
//
// The null bitmap for dead rows is NOT set (they are not read from storage at
// all; their data positions are left at the zero-initialized default).

#[test]
fn read_node_props_filtered_slots_produce_zero_io() {
    let dir = tempfile::tempdir().unwrap();
    let (store, label_id) = setup_three_nodes(dir.path());
    let store_arc = Arc::new(store);

    let age_col = col_id_of("age");

    // Build a DataChunk manually: 3 rows (slots 0, 1, 2) with sel = [1] (only slot 1 live).
    let slot_data: Vec<u64> = vec![0, 1, 2];
    let slot_col = ColumnVector::from_data(COL_ID_SLOT, slot_data);
    let mut chunk = DataChunk::from_columns(vec![slot_col]);
    // Filter: keep only row index 1 (slot value 1).
    chunk.filter_sel(|i| i == 1);
    assert_eq!(chunk.live_len(), 1, "only slot 1 should be live");

    let src = OnceSource(Some(chunk));
    let mut rnp = ReadNodeProps::new(
        src,
        Arc::clone(&store_arc),
        label_id,
        COL_ID_SLOT,
        vec![age_col],
    );

    let out = rnp
        .next_chunk()
        .expect("no error")
        .expect("chunk must be Some");

    // live_len() reflects the selection vector — still 1.
    assert_eq!(out.live_len(), 1, "selection vector must be preserved");

    let age_vec = out
        .find_column(age_col)
        .expect("age column must be present");

    // Dead rows (0 and 2) were never read from storage — their data is 0.
    assert_eq!(
        age_vec.data[0], 0,
        "dead row 0 must have zero data (no I/O)"
    );
    assert_eq!(
        age_vec.data[2], 0,
        "dead row 2 must have zero data (no I/O)"
    );

    // Row 1 is live — slot 1 has age=25, must be non-null with correct value.
    assert!(
        !age_vec.nulls.is_null(1),
        "live row 1 must be non-null in age column"
    );
    assert_eq!(
        StoreValue::from_u64(age_vec.data[1]),
        StoreValue::Int64(25),
        "live row 1 age must be 25"
    );
}

// ── T5: ChunkPredicate signed comparison ─────────────────────────────────────

// ── T5: ChunkPredicate signed comparison ─────────────────────────────────────

/// Regression test: `ChunkPredicate::Gt/Lt/Ge/Le` must use signed i64 ordering,
/// not raw u64.  Before the `raw_to_i64` fix, positive stored values
/// (e.g. `Int64(5)` = `0x0000_0000_0000_0005`) compared LESS THAN negative
/// literals (`Int64(-5)` = `0x00FF_FFFF_FFFF_FFFB`) under unsigned ordering,
/// making `WHERE n.score > -5` silently return no rows for any positive score.
#[test]
fn chunk_predicate_signed_comparison_positive_vs_negative() {
    use sparrowdb_execution::chunk::{ColumnVector, DataChunk};
    use sparrowdb_execution::pipeline::ChunkPredicate;
    use sparrowdb_storage::node_store::Value as StoreValue;

    let score_col_id = sparrowdb_common::col_id_of("score");

    // Two-row chunk: row 0 = score +5, row 1 = score -5.
    let chunk = DataChunk::from_columns(vec![ColumnVector::from_data(
        score_col_id,
        vec![
            StoreValue::Int64(5).to_u64(),
            StoreValue::Int64(-5).to_u64(),
        ],
    )]);

    // `score > -5`: row 0 passes (5 > -5), row 1 fails (-5 not > -5).
    let pred_gt = ChunkPredicate::Gt {
        col_id: score_col_id,
        rhs_raw: StoreValue::Int64(-5).to_u64(),
    };
    assert!(pred_gt.eval(&chunk, 0), "Int64(5) > Int64(-5) must be true");
    assert!(
        !pred_gt.eval(&chunk, 1),
        "Int64(-5) > Int64(-5) must be false"
    );

    // `score < 0`: row 0 fails (5 not < 0), row 1 passes (-5 < 0).
    let pred_lt = ChunkPredicate::Lt {
        col_id: score_col_id,
        rhs_raw: StoreValue::Int64(0).to_u64(),
    };
    assert!(
        !pred_lt.eval(&chunk, 0),
        "Int64(5) < Int64(0) must be false"
    );
    assert!(pred_lt.eval(&chunk, 1), "Int64(-5) < Int64(0) must be true");

    // `score >= -5`: both rows pass.
    let pred_ge = ChunkPredicate::Ge {
        col_id: score_col_id,
        rhs_raw: StoreValue::Int64(-5).to_u64(),
    };
    assert!(
        pred_ge.eval(&chunk, 0),
        "Int64(5) >= Int64(-5) must be true"
    );
    assert!(
        pred_ge.eval(&chunk, 1),
        "Int64(-5) >= Int64(-5) must be true"
    );

    // `score <= -1`: row 1 passes (-5 <= -1), row 0 fails (5 not <= -1).
    let pred_le = ChunkPredicate::Le {
        col_id: score_col_id,
        rhs_raw: StoreValue::Int64(-1).to_u64(),
    };
    assert!(
        !pred_le.eval(&chunk, 0),
        "Int64(5) <= Int64(-1) must be false"
    );
    assert!(
        pred_le.eval(&chunk, 1),
        "Int64(-5) <= Int64(-1) must be true"
    );
}
