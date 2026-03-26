//! Regression tests for SPA-241: intermediate node properties return wrong values
//! in chained 2-hop MATCH patterns.
//!
//! Before the fix, a query like:
//!   MATCH (a:Node)-[:E]->(m:Node)-[:E]->(b:Node) RETURN a.name, m.name, b.name
//!
//! returned m.name equal to a.name (or b.name) rather than the actual intermediate
//! node's property value. This happened because:
//!   1. col_ids_mid was only populated for the incoming hop case — forward-forward
//!      two-hop queries never read mid node properties at all.
//!   2. The flat two_hop() call discarded which mid node connected src to each fof,
//!      making it impossible to track mid→fof structure needed for property reads.
//!   3. project_fof_row() had no concept of the mid variable — any unrecognised var
//!      fell through to fof_props, producing wrong values.
//!
//! The fix:
//!   - Populates col_ids_mid for forward-forward patterns when mid_node_pat.var is set.
//!   - Switches to two_hop_factorized() which preserves mid_slot→fof_slots grouping.
//!   - Reads mid props per group and uses project_three_var_row() for projection.

use sparrowdb::open;
use sparrowdb_execution::types::Value;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Build: (alice:Node {name:"alice",age:30}) -[:E]-> (mid:Node {name:"mid",age:25}) -[:E]-> (bob:Node {name:"bob",age:20})
fn setup_simple_chain(db: &sparrowdb::GraphDb) {
    db.execute("CREATE (:Node {name: 'alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Node {name: 'mid', age: 25})").unwrap();
    db.execute("CREATE (:Node {name: 'bob', age: 20})").unwrap();

    db.execute("MATCH (a:Node {name: 'alice'}), (m:Node {name: 'mid'}) CREATE (a)-[:E]->(m)")
        .unwrap();
    db.execute("MATCH (m:Node {name: 'mid'}), (b:Node {name: 'bob'}) CREATE (m)-[:E]->(b)")
        .unwrap();
}

/// Core regression: m.name must be "mid", not "alice" or "bob".
#[test]
fn two_hop_intermediate_name_is_correct() {
    let (_dir, db) = make_db();
    setup_simple_chain(&db);

    let result = db
        .execute("MATCH (a:Node)-[:E]->(m:Node)-[:E]->(b:Node) RETURN a.name, m.name, b.name")
        .expect("two-hop query");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 result row, got {:?}",
        result.rows
    );
    let row = &result.rows[0];
    assert_eq!(
        row[0],
        Value::String("alice".to_string()),
        "a.name should be alice, got {:?}",
        row[0]
    );
    assert_eq!(
        row[1],
        Value::String("mid".to_string()),
        "m.name should be mid (not alice or bob), got {:?}",
        row[1]
    );
    assert_eq!(
        row[2],
        Value::String("bob".to_string()),
        "b.name should be bob, got {:?}",
        row[2]
    );
}

/// m.age must be 25, not 30 (alice's age) or 20 (bob's age).
#[test]
fn two_hop_intermediate_age_is_correct() {
    let (_dir, db) = make_db();
    setup_simple_chain(&db);

    let result = db
        .execute("MATCH (a:Node)-[:E]->(m:Node)-[:E]->(b:Node) RETURN m.age")
        .expect("two-hop age query");

    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly 1 row, got {:?}",
        result.rows
    );
    // age is stored as integer — accept either Int or Float representation.
    let age_val = &result.rows[0][0];
    let age_num = match age_val {
        Value::Int64(n) => *n as f64,
        Value::Float64(f) => *f,
        other => panic!("unexpected value type for m.age: {:?}", other),
    };
    assert!(
        (age_num - 25.0).abs() < 0.01,
        "m.age should be 25, got {}",
        age_num
    );
}

/// Three source nodes, each with 3 distinct intermediate nodes to a single terminal.
/// This catches off-by-one errors in the slot→index mapping across multiple groups.
#[test]
fn two_hop_multi_intermediate_correct_bindings() {
    let (_dir, db) = make_db();

    // Create: src1, src2, src3 — each with 3 mid nodes — all pointing to dst.
    for src_i in 1..=3usize {
        db.execute(&format!(
            "CREATE (:Hop {{name: 'src{}', kind: 'src'}})",
            src_i
        ))
        .unwrap();
    }
    for mid_i in 1..=9usize {
        db.execute(&format!(
            "CREATE (:Hop {{name: 'mid{}', kind: 'mid', idx: {}}})",
            mid_i, mid_i
        ))
        .unwrap();
    }
    db.execute("CREATE (:Hop {name: 'dst', kind: 'dst'})")
        .unwrap();

    // src1 -> mid1, mid2, mid3; src2 -> mid4, mid5, mid6; src3 -> mid7, mid8, mid9
    for src_i in 1..=3usize {
        for local_mid in 0..3usize {
            let mid_num = (src_i - 1) * 3 + local_mid + 1;
            db.execute(&format!(
                "MATCH (s:Hop {{name: 'src{}'}}), (m:Hop {{name: 'mid{}'}}) CREATE (s)-[:L]->(m)",
                src_i, mid_num
            ))
            .unwrap();
        }
    }
    // All mids -> dst
    for mid_i in 1..=9usize {
        db.execute(&format!(
            "MATCH (m:Hop {{name: 'mid{}'}}), (d:Hop {{name: 'dst'}}) CREATE (m)-[:L]->(d)",
            mid_i
        ))
        .unwrap();
    }

    let result = db
        .execute(
            "MATCH (s:Hop)-[:L]->(m:Hop)-[:L]->(d:Hop) \
             WHERE s.kind = 'src' AND m.kind = 'mid' AND d.kind = 'dst' \
             RETURN s.name, m.name, d.name",
        )
        .expect("multi-intermediate two-hop");

    // Should be 9 rows: 3 srcs × 3 mids each × 1 dst
    assert_eq!(
        result.rows.len(),
        9,
        "expected 9 rows (3 src × 3 mid), got {:?}",
        result.rows
    );

    // Every row: m.name must start with "mid", NOT "src" or "dst".
    for (i, row) in result.rows.iter().enumerate() {
        let m_name = match &row[1] {
            Value::String(s) => s.clone(),
            other => panic!("row {}: m.name is not a string: {:?}", i, other),
        };
        assert!(
            m_name.starts_with("mid"),
            "row {}: m.name should start with 'mid' but got '{}' (full row: {:?})",
            i,
            m_name,
            row
        );

        let s_name = match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("row {}: s.name is not a string: {:?}", i, other),
        };
        assert!(
            s_name.starts_with("src"),
            "row {}: s.name should start with 'src' but got '{}'",
            i,
            s_name
        );

        let d_name = match &row[2] {
            Value::String(s) => s.clone(),
            other => panic!("row {}: d.name is not a string: {:?}", i, other),
        };
        assert_eq!(
            d_name, "dst",
            "row {}: d.name should be 'dst' but got '{}'",
            i, d_name
        );
    }

    // Verify each mid1..mid9 appears exactly once.
    let mut mid_names: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::String(s) => s.clone(),
            other => panic!("m.name not a string: {:?}", other),
        })
        .collect();
    mid_names.sort();
    let expected: Vec<String> = (1..=9).map(|i| format!("mid{}", i)).collect();
    assert_eq!(
        mid_names, expected,
        "each mid node should appear exactly once"
    );
}

/// Verify b.uid != a.uid in the pattern from the issue report (Enron-style).
/// Uses uid property to confirm no property aliasing between src and intermediate.
#[test]
fn two_hop_uid_no_aliasing() {
    let (_dir, db) = make_db();

    // Create 3 person nodes with distinct uid values.
    db.execute("CREATE (:Person {uid: 1, name: 'A'})").unwrap();
    db.execute("CREATE (:Person {uid: 2, name: 'B'})").unwrap();
    db.execute("CREATE (:Person {uid: 3, name: 'C'})").unwrap();

    // A -[EMAILED]-> B -[EMAILED]-> C
    db.execute("MATCH (a:Person {uid: 1}), (b:Person {uid: 2}) CREATE (a)-[:EMAILED]->(b)")
        .unwrap();
    db.execute("MATCH (b:Person {uid: 2}), (c:Person {uid: 3}) CREATE (b)-[:EMAILED]->(c)")
        .unwrap();

    let result = db
        .execute(
            "MATCH (a:Person)-[:EMAILED]->(b:Person)-[:EMAILED]->(c:Person) \
             RETURN a.uid, b.uid, c.uid",
        )
        .expect("uid two-hop");

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row, got {:?}",
        result.rows
    );
    let row = &result.rows[0];

    // Extract uids as numbers.
    let uid = |v: &Value| -> i64 {
        match v {
            Value::Int64(n) => *n,
            Value::Float64(f) => *f as i64,
            other => panic!("unexpected uid value: {:?}", other),
        }
    };

    let a_uid = uid(&row[0]);
    let b_uid = uid(&row[1]);
    let c_uid = uid(&row[2]);

    assert_eq!(a_uid, 1, "a.uid should be 1, got {}", a_uid);
    assert_eq!(
        b_uid, 2,
        "b.uid should be 2, got {} (was it aliased to a.uid={}?)",
        b_uid, a_uid
    );
    assert_eq!(c_uid, 3, "c.uid should be 3, got {}", c_uid);
    assert_ne!(
        b_uid, a_uid,
        "b.uid must NOT equal a.uid (property aliasing bug)"
    );
    assert_ne!(b_uid, c_uid, "b.uid must NOT equal c.uid");
}
