//! End-to-end tests for temporal functions: datetime(), timestamp(), date(),
//! duration().
//!
//! All tests use a real tempdir + disk-backed engine, exercising the full
//! parse → bind → execute pipeline.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_execution::engine::Engine;
use sparrowdb_storage::csr::CsrForward;
use sparrowdb_storage::node_store::NodeStore;

fn fresh_engine(dir: &std::path::Path) -> Engine {
    let store = NodeStore::open(dir).expect("node store");
    let cat = Catalog::open(dir).expect("catalog");
    let csr = CsrForward::build(0, &[]);
    Engine::with_single_csr(store, cat, csr, dir)
}

// ── datetime() ────────────────────────────────────────────────────────────────

#[test]
fn datetime_returns_positive_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    let result = engine.execute("RETURN datetime()").expect("datetime()");
    assert_eq!(result.rows.len(), 1, "expected 1 row");

    match &result.rows[0][0] {
        sparrowdb_execution::Value::Int64(ms) => {
            // 1_000_000_000_000 ms ≈ 2001-09-09; any real clock is after this.
            assert!(
                *ms > 1_000_000_000_000,
                "datetime() should be > 1 trillion ms, got {ms}"
            );
        }
        other => panic!("expected Int64, got {other:?}"),
    }
}

// ── timestamp() ──────────────────────────────────────────────────────────────

#[test]
fn timestamp_alias() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    let result = engine.execute("RETURN timestamp()").expect("timestamp()");
    assert_eq!(result.rows.len(), 1, "expected 1 row");

    match &result.rows[0][0] {
        sparrowdb_execution::Value::Int64(ms) => {
            assert!(
                *ms > 1_000_000_000_000,
                "timestamp() should be > 1 trillion ms, got {ms}"
            );
        }
        other => panic!("expected Int64, got {other:?}"),
    }
}

// ── datetime() in CREATE / MATCH ──────────────────────────────────────────────

#[test]
fn datetime_in_create() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    engine
        .execute("CREATE (e:Event {ts: datetime()})")
        .expect("CREATE Event");

    let result = engine
        .execute("MATCH (e:Event) RETURN e.ts")
        .expect("MATCH Event");

    assert_eq!(result.rows.len(), 1, "expected 1 Event row");
    match &result.rows[0][0] {
        sparrowdb_execution::Value::Int64(ms) => {
            assert!(
                *ms > 1_000_000_000_000,
                "stored ts should be > 1 trillion ms, got {ms}"
            );
        }
        other => panic!("expected Int64 for e.ts, got {other:?}"),
    }
}

// ── date() ────────────────────────────────────────────────────────────────────

#[test]
fn date_returns_days() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    let result = engine.execute("RETURN date()").expect("date()");
    assert_eq!(result.rows.len(), 1, "expected 1 row");

    match &result.rows[0][0] {
        sparrowdb_execution::Value::Int64(days) => {
            // 2001-09-09 is day 11_574; any real clock is past this.
            assert!(
                *days > 11_574,
                "date() should be > 11574 days since epoch, got {days}"
            );
        }
        other => panic!("expected Int64, got {other:?}"),
    }
}

// ── duration() ────────────────────────────────────────────────────────────────

#[test]
fn duration_one_day() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    let result = engine
        .execute("RETURN duration('P1D')")
        .expect("duration('P1D')");
    assert_eq!(result.rows.len(), 1);

    match &result.rows[0][0] {
        sparrowdb_execution::Value::Int64(ms) => {
            assert_eq!(*ms, 86_400_000, "P1D should be 86400000 ms");
        }
        other => panic!("expected Int64, got {other:?}"),
    }
}

#[test]
fn duration_complex() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fresh_engine(dir.path());

    // P1DT2H = 1 day + 2 hours
    let result = engine
        .execute("RETURN duration('P1DT2H')")
        .expect("duration('P1DT2H')");
    assert_eq!(result.rows.len(), 1);

    let expected_ms = 86_400_000_i64 + 2 * 3_600_000;
    match &result.rows[0][0] {
        sparrowdb_execution::Value::Int64(ms) => {
            assert_eq!(*ms, expected_ms, "P1DT2H should be {expected_ms} ms");
        }
        other => panic!("expected Int64, got {other:?}"),
    }
}
