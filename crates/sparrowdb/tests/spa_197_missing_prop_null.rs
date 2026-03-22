//! End-to-end regression test for SPA-197.
//!
//! When a node is created without a particular property (e.g. no `name` field),
//! a subsequent `MATCH (n) RETURN n.name` used to return `Int64(0)` because
//! `get_node_raw` returned `0u64` for absent column slots and `decode_raw_val(0)`
//! produced `Value::Int64(0)` (type tag 0x00 = Int64).
//!
//! The fix (SPA-197) routes all property reads through `get_node_raw_nullable`
//! via the new `read_node_props` helper, which treats absent/zero slots as
//! `None` and allows `project_row` / `build_row_vals` to return `Value::Null`
//! instead.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_execution::types::Value;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;

fn fresh_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("node store");
    let cat = Catalog::open(dir).expect("catalog");
    let csr = CsrForward::build(0, &[]);
    Engine::with_single_csr(store, cat, csr, dir)
}

/// CREATE a node with only `content` property (no `name`).
/// RETURN n.name must be `Value::Null`, not `Value::Int64(0)`.
#[test]
fn missing_string_property_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Use a short string (≤7 bytes) to stay within the inline-bytes encoding
    // limit (SPA-169) — longer strings are truncated by the storage layer.
    engine
        .execute("CREATE (n:Know {body: 'Fact'})")
        .expect("CREATE");

    let result = engine
        .execute("MATCH (n:Know) RETURN n.name, n.body")
        .expect("MATCH RETURN");

    assert_eq!(result.rows.len(), 1, "expected exactly 1 Know node");

    // n.name was never set — must be null, not integer 0 (SPA-197).
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "n.name was never set; expected Value::Null but got {:?} (SPA-197)",
        result.rows[0][0]
    );

    // n.body was set — must be the correct string.
    assert_eq!(
        result.rows[0][1],
        Value::String("Fact".to_string()),
        "n.body must be String('Fact')"
    );
}

/// Variation: multiple nodes, only some have a property.
/// Nodes that don't have it must return null; nodes that do must return the value.
///
/// Uses short strings (≤7 bytes) to stay within the inline-bytes storage limit.
#[test]
fn partial_property_coverage_returns_null_for_absent_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // Node 1 has both properties; node 2 has only body.
    engine
        .execute("CREATE (n:Item {name: 'Widget', body: 'small'})")
        .expect("CREATE 1");
    engine
        .execute("CREATE (n:Item {body: 'anon'})")
        .expect("CREATE 2");

    let result = engine
        .execute("MATCH (n:Item) RETURN n.name, n.body ORDER BY n.body")
        .expect("MATCH RETURN");

    assert_eq!(result.rows.len(), 2, "expected 2 Item nodes");

    // After ORDER BY n.body: "anon" < "small".
    let (nameless_name, _nameless_body) = (&result.rows[0][0], &result.rows[0][1]);
    let (widget_name, _widget_body) = (&result.rows[1][0], &result.rows[1][1]);

    assert_eq!(
        *nameless_name,
        Value::Null,
        "Node without name must return Value::Null (SPA-197)"
    );
    assert_eq!(
        *widget_name,
        Value::String("Widget".to_string()),
        "Node with name='Widget' must return Value::String('Widget')"
    );
}

/// Regression: missing integer property must also return null, not 0.
/// Note: avoid `n.count` which conflicts with the COUNT aggregate keyword.
#[test]
fn missing_integer_property_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (n:Item {body: 'hello'})")
        .expect("CREATE");

    let result = engine
        .execute("MATCH (n:Item) RETURN n.score")
        .expect("MATCH RETURN");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::Null,
        "missing integer property must be Null, not Int64(0) (SPA-197)"
    );
}
