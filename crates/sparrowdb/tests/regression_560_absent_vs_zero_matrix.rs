//! #560: a property ABSENT from a node must behave as NULL at every read
//! site, never as a stored `0`. A property legitimately stored as 0 must
//! still be 0 everywhere.
//!
//! Every expected value below is derived BY HAND from this fixture; none was
//! captured from engine output.
//!
//!   label W:  id | u        label V:  vid | k
//!             1  | 0 (stored)         1   | 0 (stored)
//!             2  | 4                  2   | 4
//!             3  | (absent)           3   | (absent)
//!
//!   A {name:'a'};  A -[:R]-> W1, W2, W3.
//!
//! openCypher: `u = 0`, `u <> 5`, `u < 1`, `u >= 0` are NULL (not true) for
//! the absent W3, so W3 never satisfies a comparison; only `IS NULL` selects it.
//! ORDER BY ascending places NULL last, descending places NULL first.

use sparrowdb::{open, GraphDb, NodeId};
use sparrowdb_execution::types::Value;

fn i(v: i64) -> Value {
    Value::Int64(v)
}

fn fixture() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).unwrap();
    for q in [
        "CREATE (:W {id:1, u:0})",
        "CREATE (:W {id:2, u:4})",
        "CREATE (:W {id:3})",
        "CREATE (:V {vid:1, k:0})",
        "CREATE (:V {vid:2, k:4})",
        "CREATE (:V {vid:3})",
        "CREATE (:A {name:'a'})",
        "MATCH (a:A),(w:W) CREATE (a)-[:R]->(w)",
    ] {
        db.execute(q).unwrap();
    }
    (dir, db)
}

fn rows(db: &GraphDb, q: &str) -> Result<Vec<Vec<Value>>, String> {
    db.execute(q).map(|r| r.rows).map_err(|e| e.to_string())
}

fn key(r: &[Value]) -> String {
    format!("{r:?}")
}

/// Compare ignoring row order (for cells without ORDER BY).
fn same_unordered(mut got: Vec<Vec<Value>>, mut want: Vec<Vec<Value>>) -> bool {
    got.sort_by_key(|r| key(r));
    want.sort_by_key(|r| key(r));
    got == want
}

struct Cell {
    name: &'static str,
    q: &'static str,
    want: Vec<Vec<Value>>,
    ordered: bool,
}

fn c(name: &'static str, q: &'static str, want: Vec<Vec<Value>>) -> Cell {
    Cell {
        name,
        q,
        want,
        ordered: false,
    }
}
fn co(name: &'static str, q: &'static str, want: Vec<Vec<Value>>) -> Cell {
    Cell {
        name,
        q,
        want,
        ordered: true,
    }
}

fn ids(v: &[i64]) -> Vec<Vec<Value>> {
    v.iter().map(|x| vec![i(*x)]).collect()
}

fn run(cells: Vec<Cell>) {
    let (_d, db) = fixture();
    let mut bad = Vec::new();
    for cell in cells {
        match rows(&db, cell.q) {
            Ok(got) => {
                let ok = if cell.ordered {
                    got == cell.want
                } else {
                    same_unordered(got.clone(), cell.want.clone())
                };
                if !ok {
                    bad.push(format!(
                        "WRONG [{}] {}\n   want {:?}\n   got  {:?}",
                        cell.name, cell.q, cell.want, got
                    ));
                }
            }
            Err(e) => bad.push(format!("ERROR [{}] {}\n   {e}", cell.name, cell.q)),
        }
    }
    assert!(
        bad.is_empty(),
        "{} cells wrong:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

/// The same property predicates over one entry shape. `pat` binds `w`.
fn predicate_cells(shape: &'static str, pat: &'static str) -> Vec<Cell> {
    // Static strings are required by Cell; build them once per shape with
    // Box::leak (test-only, a few dozen small strings).
    let mk = |name: &str, tail: &str| -> (&'static str, &'static str) {
        (
            Box::leak(format!("{shape}: {name}").into_boxed_str()),
            Box::leak(format!("{pat} {tail}").into_boxed_str()),
        )
    };
    let mut v = Vec::new();
    for (name, tail, want) in [
        ("WHERE u = 0", "WHERE w.u = 0 RETURN w.id", ids(&[1])),
        ("WHERE u = 4", "WHERE w.u = 4 RETURN w.id", ids(&[2])),
        ("WHERE nope = 0", "WHERE w.nope = 0 RETURN w.id", ids(&[])),
        ("WHERE u <> 5", "WHERE w.u <> 5 RETURN w.id", ids(&[1, 2])),
        ("WHERE u < 1", "WHERE w.u < 1 RETURN w.id", ids(&[1])),
        ("WHERE u <= 0", "WHERE w.u <= 0 RETURN w.id", ids(&[1])),
        ("WHERE u >= 0", "WHERE w.u >= 0 RETURN w.id", ids(&[1, 2])),
        ("WHERE u > -1", "WHERE w.u > -1 RETURN w.id", ids(&[1, 2])),
        (
            "WHERE u IS NULL",
            "WHERE w.u IS NULL RETURN w.id",
            ids(&[3]),
        ),
        (
            "WHERE u IS NOT NULL",
            "WHERE w.u IS NOT NULL RETURN w.id",
            ids(&[1, 2]),
        ),
        (
            "WHERE NOT u = 0",
            "WHERE NOT w.u = 0 RETURN w.id",
            ids(&[2]),
        ),
        ("COUNT(*) u = 0", "WHERE w.u = 0 RETURN COUNT(*)", ids(&[1])),
        ("COUNT(w.u)", "RETURN COUNT(w.u)", ids(&[2])),
        (
            "DISTINCT u",
            "RETURN DISTINCT w.u",
            vec![vec![i(0)], vec![i(4)], vec![Value::Null]],
        ),
        (
            "RETURN id,u",
            "RETURN w.id, w.u",
            vec![vec![i(1), i(0)], vec![i(2), i(4)], vec![i(3), Value::Null]],
        ),
    ] {
        let (n, q) = mk(name, tail);
        v.push(c(n, q, want));
    }
    // ORDER BY a projected key: NULL sorts last ascending, first descending.
    let (n, q) = mk("ORDER BY u ASC", "RETURN w.id, w.u ORDER BY w.u ASC");
    v.push(co(
        n,
        q,
        vec![vec![i(1), i(0)], vec![i(2), i(4)], vec![i(3), Value::Null]],
    ));
    let (n, q) = mk("ORDER BY u DESC", "RETURN w.id, w.u ORDER BY w.u DESC");
    v.push(co(
        n,
        q,
        vec![vec![i(3), Value::Null], vec![i(2), i(4)], vec![i(1), i(0)]],
    ));
    v
}

#[test]
fn single_pattern_control() {
    run(predicate_cells("single", "MATCH (w:W)"));
}

#[test]
fn multi_pattern_cross_product() {
    run(predicate_cells("multi", "MATCH (a:A),(w:W)"));
}

#[test]
fn one_hop() {
    run(predicate_cells("1hop", "MATCH (a:A)-[:R]->(w:W)"));
}

#[test]
fn multi_pattern_inline_filter() {
    run(vec![
        c(
            "multi {u:0}",
            "MATCH (a:A),(w:W {u:0}) RETURN w.id",
            ids(&[1]),
        ),
        c(
            "multi {u:0} COUNT",
            "MATCH (a:A),(w:W {u:0}) RETURN COUNT(*)",
            ids(&[1]),
        ),
        c(
            "multi {u:4}",
            "MATCH (a:A),(w:W {u:4}) RETURN w.id",
            ids(&[2]),
        ),
        c(
            "multi {nope:0}",
            "MATCH (a:A),(w:W {nope:0}) RETURN COUNT(*)",
            ids(&[0]),
        ),
        c(
            "multi {nope:0} rows",
            "MATCH (a:A),(w:W {nope:0}) RETURN w.id",
            ids(&[]),
        ),
        c(
            "multi {id:3,u:0}",
            "MATCH (a:A),(w:W {id:3, u:0}) RETURN w.id",
            ids(&[]),
        ),
        c(
            "multi {id:1,u:0}",
            "MATCH (a:A),(w:W {id:1, u:0}) RETURN w.id",
            ids(&[1]),
        ),
        c(
            "single {u:0} control",
            "MATCH (w:W {u:0}) RETURN COUNT(*)",
            ids(&[1]),
        ),
        c(
            "hop {u:0}",
            "MATCH (a:A)-[:R]->(w:W {u:0}) RETURN w.id",
            ids(&[1]),
        ),
        c(
            "hop {nope:0}",
            "MATCH (a:A)-[:R]->(w:W {nope:0}) RETURN w.id",
            ids(&[]),
        ),
    ]);
}

#[test]
fn joins_on_property_equality() {
    // W.u joins V.k: 0=0 (W1,V1) and 4=4 (W2,V2). W3/V3 are absent on both
    // sides and NULL = NULL is NULL, so they must NOT join.
    let want = vec![vec![i(1), i(1)], vec![i(2), i(2)]];
    run(vec![
        c(
            "join WHERE",
            "MATCH (w:W),(v:V) WHERE w.u = v.k RETURN w.id, v.vid",
            want.clone(),
        ),
        c(
            "join WHERE + A",
            "MATCH (a:A),(w:W),(v:V) WHERE w.u = v.k RETURN w.id, v.vid",
            want,
        ),
        // Absent on one side only: W3 (absent) vs V1 (stored 0) must not join.
        c(
            "join count",
            "MATCH (w:W),(v:V) WHERE w.u = v.k RETURN COUNT(*)",
            ids(&[2]),
        ),
    ]);
}

fn count_w(db: &GraphDb) -> i64 {
    match &db.execute("MATCH (w:W) RETURN COUNT(*)").unwrap().rows[0][0] {
        Value::Int64(n) => *n,
        o => panic!("{o:?}"),
    }
}

#[test]
fn set_target_filter() {
    let (_d, db) = fixture();
    db.execute("MATCH (w:W {u:0}) SET w.t = 1").unwrap();
    let r = rows(&db, "MATCH (w:W) WHERE w.t = 1 RETURN w.id").unwrap();
    assert_eq!(r, ids(&[1]), "SET {{u:0}} must touch only W1, got {r:?}");
    let (_d, db) = fixture();
    db.execute("MATCH (w:W {nope:0}) SET w.t = 1").unwrap();
    let r = rows(&db, "MATCH (w:W) WHERE w.t = 1 RETURN w.id").unwrap();
    assert!(r.is_empty(), "SET {{nope:0}} must touch nothing, got {r:?}");
}

#[test]
fn delete_target_filter() {
    let (_d, db) = fixture();
    db.execute("MATCH (w:W {u:0}) DETACH DELETE w").unwrap();
    let r = rows(&db, "MATCH (w:W) RETURN w.id").unwrap();
    assert!(same_unordered(r.clone(), ids(&[2, 3])), "got {r:?}");
    let (_d, db) = fixture();
    db.execute("MATCH (w:W {nope:0}) DETACH DELETE w").unwrap();
    assert_eq!(count_w(&db), 3);
}

#[test]
fn merge_on_zero_does_not_match_absent() {
    // MERGE (w:W {u:0}) matches W1 (stored 0): no new node.
    let (_d, db) = fixture();
    db.execute("MERGE (w:W {u:0})").unwrap();
    assert_eq!(count_w(&db), 3, "MERGE {{u:0}} must reuse W1");
    // MERGE (w:W {id:3, u:0}): W3 has id 3 but u ABSENT, so it does not match;
    // no W has (3,0) -> a 4th node is created.
    let (_d, db) = fixture();
    db.execute("MERGE (w:W {id:3, u:0})").unwrap();
    assert_eq!(count_w(&db), 4, "MERGE {{id:3,u:0}} must not match W3");
    // MERGE on a property no node has: creates a node.
    let (_d, db) = fixture();
    db.execute("MERGE (w:W {nope:0})").unwrap();
    assert_eq!(count_w(&db), 4, "MERGE {{nope:0}} must create");
}

#[test]
fn merge_return_absent_property_is_null() {
    let (_d, db) = fixture();
    let r = rows(&db, "MERGE (w:W {id:3}) RETURN w.id, w.u").unwrap();
    assert_eq!(r, vec![vec![i(3), Value::Null]], "MERGE RETURN absent u");
    let r = rows(&db, "MERGE (w:W {id:1}) RETURN w.id, w.u").unwrap();
    assert_eq!(r, vec![vec![i(1), i(0)]], "MERGE RETURN stored-0 u");
}

#[test]
fn read_tx_get_node_omits_absent_column() {
    let (_d, db) = fixture();
    let nid = |q: &str| match rows(&db, q).unwrap()[0][0] {
        Value::Int64(n) => NodeId(n as u64),
        ref o => panic!("{o:?}"),
    };
    let w1 = nid("MATCH (w:W {id:1}) RETURN id(w)");
    let w3 = nid("MATCH (w:W {id:3}) RETURN id(w)");
    let col_u = sparrowdb::fnv1a_col_id("u");
    let tx = db.begin_read().unwrap();
    let got1 = tx.get_node(w1, &[col_u]).unwrap();
    assert_eq!(got1.len(), 1, "W1 stores u=0: {got1:?}");
    let got3 = tx.get_node(w3, &[col_u]).unwrap();
    assert!(
        got3.is_empty(),
        "W3 has no u: must be omitted, got {got3:?}"
    );
}

#[test]
fn unique_constraint_sees_stored_zero() {
    // Stored 0 must conflict with another 0; absent must not conflict with 0.
    let (_d, db) = fixture();
    db.execute("CREATE CONSTRAINT ON (n:W) ASSERT n.u IS UNIQUE")
        .unwrap();
    assert!(
        db.execute("CREATE (:W {id:9, u:0})").is_err(),
        "u=0 already stored on W1: UNIQUE must reject"
    );
    assert!(
        db.execute("CREATE (:W {id:9, u:7})").is_ok(),
        "u=7 is new: UNIQUE must accept"
    );
}

#[test]
fn snapshot_reader_still_sees_property_absent_after_a_later_set() {
    // A reader pinned BEFORE `SET w.u = 7` on W3 must still see u absent, not a
    // fabricated before-image of Int64(0) (write_tx set_property).
    let (_d, db) = fixture();
    let w3 = match rows(&db, "MATCH (w:W {id:3}) RETURN id(w)").unwrap()[0][0] {
        Value::Int64(n) => NodeId(n as u64),
        ref o => panic!("{o:?}"),
    };
    let col_u = sparrowdb::fnv1a_col_id("u");
    let old_reader = db.begin_read().unwrap();
    db.execute("MATCH (w:W {id:3}) SET w.u = 7").unwrap();
    let before = old_reader.get_node(w3, &[col_u]).unwrap();
    assert!(
        before.is_empty(),
        "snapshot predates the SET; u must still be absent, got {before:?}"
    );
    let after = db.begin_read().unwrap().get_node(w3, &[col_u]).unwrap();
    assert_eq!(after.len(), 1, "new reader sees u = 7, got {after:?}");
    assert_eq!(after[0].1, sparrowdb_storage::node_store::Value::Int64(7));
}
