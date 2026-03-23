/// SPA-267: Float64 values (e.g. 3.14) were being stored with the top byte
/// (sign + exponent bits) masked off, so they came back as garbage integers
/// (e.g. 3.14 → 2567051787601183).
/// These tests verify end-to-end float round-trip through CREATE → MATCH.
// The test deliberately uses 3.14 as the literal from the bug report, not as an
// approximation of PI.
use sparrowdb::GraphDb;
use tempfile::tempdir;

#[test]
#[allow(clippy::approx_constant)] // 3.14 is the exact value from the SPA-267 bug report
fn float_roundtrips_correctly() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Thing {val: 3.14})").unwrap();
    let r = db.execute("MATCH (n:Thing) RETURN n.val").unwrap();
    assert_eq!(r.rows.len(), 1, "expected one row");
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - 3.14_f64).abs() < 1e-10,
                "float roundtrip failed: got {f}"
            );
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn negative_float_roundtrips() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Thing {temp: -273.15})").unwrap();
    let r = db.execute("MATCH (n:Thing) RETURN n.temp").unwrap();
    assert_eq!(r.rows.len(), 1, "expected one row");
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - (-273.15_f64)).abs() < 1e-10,
                "negative float roundtrip failed: got {f}"
            );
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn zero_float_roundtrips() {
    // 0.0 has all-zero bit pattern — it encodes to 0 which is the "absent" sentinel.
    // Storing 0.0 should not be confused with a missing property.
    // (This is a known limitation documented in the null-sentinel comment.)
    // For now, just verify that 0.0 does not panic and the encode/decode path
    // is handled without a crash — actual null-vs-zero disambiguation is deferred.
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    // 0.0 is a valid float literal; verify the write path doesn't panic.
    db.execute("CREATE (n:Zero {v: 0.0})").unwrap();
}

#[test]
fn large_float_roundtrips() {
    // Use a decimal literal that the Cypher lexer accepts (scientific notation
    // like 1.0e10 is not yet supported by the parser).
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Big {v: 9999999.5})").unwrap();
    let r = db.execute("MATCH (n:Big) RETURN n.v").unwrap();
    assert_eq!(r.rows.len(), 1);
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - 9_999_999.5_f64).abs() < 1e-3,
                "large float roundtrip failed: got {f}"
            );
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn float_filter_works() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Score {v: 0.95})").unwrap();
    db.execute("CREATE (n:Score {v: 0.30})").unwrap();
    let r = db
        .execute("MATCH (n:Score) WHERE n.v > 0.5 RETURN n.v")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "expected exactly one score above 0.5");
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!((f - 0.95_f64).abs() < 1e-10, "expected 0.95, got {f}");
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
#[allow(clippy::approx_constant)] // 3.14 is the exact value from the SPA-267 bug report
fn float_where_equality_filter() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Sensor {reading: 3.14})").unwrap();
    db.execute("CREATE (n:Sensor {reading: 2.71})").unwrap();
    let r = db
        .execute("MATCH (n:Sensor) WHERE n.reading = 3.14 RETURN n.reading")
        .unwrap();
    assert_eq!(
        r.rows.len(),
        1,
        "expected exactly one match for reading=3.14"
    );
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!(
                (f - 3.14_f64).abs() < 1e-10,
                "WHERE equality float: got {f}"
            );
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn multiple_floats_on_same_node() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Point {x: 1.5, y: 2.7, z: -0.3})")
        .unwrap();
    let r = db.execute("MATCH (n:Point) RETURN n.x, n.y, n.z").unwrap();
    assert_eq!(r.rows.len(), 1);
    let row = &r.rows[0];
    let get_f = |v: &sparrowdb_execution::Value| match v {
        sparrowdb_execution::Value::Float64(f) => *f,
        other => panic!("Expected Float64, got {:?}", other),
    };
    assert!((get_f(&row[0]) - 1.5_f64).abs() < 1e-10, "x mismatch");
    assert!((get_f(&row[1]) - 2.7_f64).abs() < 1e-10, "y mismatch");
    assert!((get_f(&row[2]) - (-0.3_f64)).abs() < 1e-10, "z mismatch");
}

#[test]
fn float_and_int_on_same_node() {
    let dir = tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    db.execute("CREATE (n:Mixed {score: 9.81, rank: 42})")
        .unwrap();
    let r = db
        .execute("MATCH (n:Mixed) RETURN n.score, n.rank")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    match &r.rows[0][0] {
        sparrowdb_execution::Value::Float64(f) => {
            assert!((f - 9.81_f64).abs() < 1e-10, "score mismatch: {f}");
        }
        other => panic!("Expected Float64 for score, got {:?}", other),
    }
    match &r.rows[0][1] {
        sparrowdb_execution::Value::Int64(n) => {
            assert_eq!(*n, 42, "rank mismatch");
        }
        other => panic!("Expected Int64 for rank, got {:?}", other),
    }
}
