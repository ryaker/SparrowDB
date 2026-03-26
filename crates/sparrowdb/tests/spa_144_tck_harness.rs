//! SPA-144 — openCypher TCK compliance harness.
//!
//! This module defines a lightweight TCK-style scenario runner that mirrors the
//! structure of the openCypher Technology Compatibility Kit (Gherkin feature files)
//! without requiring the `gherkin` crate.  Each scenario is a struct with:
//!
//!   - `name`:   human-readable scenario title
//!   - `given`:  Cypher statements that set up the graph (CREATE nodes/edges)
//!   - `when`:   the Cypher query under test
//!   - `then`:   expected column names + expected rows (order-insensitive)
//!
//! The harness creates a fresh database per scenario, executes the setup, runs the
//! query, and asserts the result matches.  A summary report is printed at the end.

use sparrowdb::{open, GraphDb};
use sparrowdb_execution::types::Value;

// ── Scenario definition ─────────────────────────────────────────────────────

/// A single TCK scenario mirroring the openCypher Gherkin format.
struct TckScenario {
    /// Human-readable scenario name (maps to Gherkin `Scenario:` line).
    name: &'static str,
    /// Feature / category this scenario belongs to.
    feature: &'static str,
    /// `Given` step: Cypher statements that set up the graph.
    given: &'static [&'static str],
    /// `When` step: the Cypher query to execute.
    when: &'static str,
    /// `Then` step: expected column names.
    then_columns: &'static [&'static str],
    /// `Then` step: expected rows.  Each inner slice is one row of `Value`s.
    /// Rows are compared as *sets* (order-insensitive) unless `ordered` is true.
    then_rows: Vec<Vec<Value>>,
    /// Whether row order matters (e.g., when ORDER BY is used).
    ordered: bool,
}

// ── Harness ─────────────────────────────────────────────────────────────────

fn make_db() -> (tempfile::TempDir, GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

/// Run a single scenario, returning `Ok(())` on pass or `Err(msg)` on failure.
fn run_scenario(s: &TckScenario) -> Result<(), String> {
    let (_dir, db) = make_db();

    // Given: execute setup statements
    for (i, stmt) in s.given.iter().enumerate() {
        db.execute(stmt).map_err(|e| {
            format!(
                "[{}] Given step {} failed: {e}\n  statement: {stmt}",
                s.name, i
            )
        })?;
    }

    // When: execute the query
    let result = db
        .execute(s.when)
        .map_err(|e| format!("[{}] When step failed: {e}\n  query: {}", s.name, s.when))?;

    // Then: check columns
    if !s.then_columns.is_empty() {
        let actual_cols: Vec<&str> = result.columns.iter().map(|c| c.as_str()).collect();
        let expected_cols: Vec<&str> = s.then_columns.to_vec();
        if actual_cols != expected_cols {
            return Err(format!(
                "[{}] Column mismatch:\n  expected: {expected_cols:?}\n  actual:   {actual_cols:?}",
                s.name
            ));
        }
    }

    // Then: check row count
    if result.rows.len() != s.then_rows.len() {
        return Err(format!(
            "[{}] Row count mismatch: expected {}, got {}\n  actual rows: {:?}",
            s.name,
            s.then_rows.len(),
            result.rows.len(),
            result.rows
        ));
    }

    // Then: check row contents
    if s.ordered {
        // Ordered comparison
        for (i, (actual, expected)) in result.rows.iter().zip(s.then_rows.iter()).enumerate() {
            if !rows_match(actual, expected) {
                return Err(format!(
                    "[{}] Row {i} mismatch:\n  expected: {expected:?}\n  actual:   {actual:?}",
                    s.name
                ));
            }
        }
    } else {
        // Set comparison (order-insensitive)
        let mut unmatched_expected: Vec<&Vec<Value>> = s.then_rows.iter().collect();
        for actual_row in &result.rows {
            if let Some(pos) = unmatched_expected
                .iter()
                .position(|exp| rows_match(actual_row, exp))
            {
                unmatched_expected.remove(pos);
            } else {
                return Err(format!(
                    "[{}] Unexpected row: {actual_row:?}\n  expected rows: {:?}",
                    s.name, s.then_rows
                ));
            }
        }
        if !unmatched_expected.is_empty() {
            return Err(format!(
                "[{}] Missing expected rows: {unmatched_expected:?}",
                s.name
            ));
        }
    }

    Ok(())
}

/// Compare two rows, treating Float64 with tolerance.
fn rows_match(actual: &[Value], expected: &[Value]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    actual
        .iter()
        .zip(expected.iter())
        .all(|(a, e)| match (a, e) {
            (Value::Float64(af), Value::Float64(ef)) => (af - ef).abs() < 1e-9,
            _ => a == e,
        })
}

// ── Scenario catalogue ──────────────────────────────────────────────────────

fn all_scenarios() -> Vec<TckScenario> {
    vec![
        // ── Feature: Match ──────────────────────────────────────────────
        TckScenario {
            name: "Match all nodes",
            feature: "Match",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
            ],
            when: "MATCH (n:Person) RETURN n.name",
            then_columns: &["n.name"],
            then_rows: vec![
                vec![Value::String("Alice".into())],
                vec![Value::String("Bob".into())],
            ],
            ordered: false,
        },
        TckScenario {
            name: "Match with property equality filter",
            feature: "Match",
            given: &[
                "CREATE (n:Person {name: 'Alice', age: 30})",
                "CREATE (n:Person {name: 'Bob',   age: 25})",
                "CREATE (n:Person {name: 'Carol', age: 30})",
            ],
            when: "MATCH (n:Person {age: 30}) RETURN n.name",
            then_columns: &["n.name"],
            then_rows: vec![
                vec![Value::String("Alice".into())],
                vec![Value::String("Carol".into())],
            ],
            ordered: false,
        },
        TckScenario {
            name: "Match single relationship",
            feature: "Match",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
                "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
            ],
            when: "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
            then_columns: &["a.name", "b.name"],
            then_rows: vec![vec![
                Value::String("Alice".into()),
                Value::String("Bob".into()),
            ]],
            ordered: false,
        },
        // ── Feature: Where ──────────────────────────────────────────────
        TckScenario {
            name: "WHERE with equality",
            feature: "Where",
            given: &[
                "CREATE (n:Person {name: 'Alice', age: 30})",
                "CREATE (n:Person {name: 'Bob',   age: 25})",
            ],
            when: "MATCH (n:Person) WHERE n.age = 30 RETURN n.name",
            then_columns: &["n.name"],
            then_rows: vec![vec![Value::String("Alice".into())]],
            ordered: false,
        },
        TckScenario {
            name: "WHERE with greater-than comparison",
            feature: "Where",
            given: &[
                "CREATE (n:Person {name: 'Alice', age: 30})",
                "CREATE (n:Person {name: 'Bob',   age: 25})",
                "CREATE (n:Person {name: 'Carol', age: 35})",
            ],
            when: "MATCH (n:Person) WHERE n.age > 28 RETURN n.name",
            then_columns: &["n.name"],
            then_rows: vec![
                vec![Value::String("Alice".into())],
                vec![Value::String("Carol".into())],
            ],
            ordered: false,
        },
        TckScenario {
            name: "WHERE with less-than comparison",
            feature: "Where",
            given: &[
                "CREATE (n:Person {name: 'Alice', age: 30})",
                "CREATE (n:Person {name: 'Bob',   age: 25})",
                "CREATE (n:Person {name: 'Carol', age: 35})",
            ],
            when: "MATCH (n:Person) WHERE n.age < 30 RETURN n.name",
            then_columns: &["n.name"],
            then_rows: vec![vec![Value::String("Bob".into())]],
            ordered: false,
        },
        TckScenario {
            name: "WHERE with AND",
            feature: "Where",
            given: &[
                "CREATE (n:Person {name: 'Alice', age: 30})",
                "CREATE (n:Person {name: 'Bob',   age: 25})",
                "CREATE (n:Person {name: 'Carol', age: 30})",
            ],
            when: "MATCH (n:Person) WHERE n.age = 30 AND n.name = 'Alice' RETURN n.name",
            then_columns: &["n.name"],
            then_rows: vec![vec![Value::String("Alice".into())]],
            ordered: false,
        },
        // ── Feature: Return ─────────────────────────────────────────────
        TckScenario {
            name: "RETURN multiple properties",
            feature: "Return",
            given: &[
                "CREATE (n:Person {name: 'Alice', age: 30})",
            ],
            when: "MATCH (n:Person) RETURN n.name, n.age",
            then_columns: &["n.name", "n.age"],
            then_rows: vec![vec![
                Value::String("Alice".into()),
                Value::Int64(30),
            ]],
            ordered: false,
        },
        TckScenario {
            name: "RETURN DISTINCT removes duplicates",
            feature: "Return",
            given: &[
                "CREATE (n:Person {name: 'Alice', city: 'NYC'})",
                "CREATE (n:Person {name: 'Bob',   city: 'NYC'})",
                "CREATE (n:Person {name: 'Carol', city: 'LA'})",
            ],
            when: "MATCH (n:Person) RETURN DISTINCT n.city",
            then_columns: &["n.city"],
            then_rows: vec![
                vec![Value::String("NYC".into())],
                vec![Value::String("LA".into())],
            ],
            ordered: false,
        },
        // ── Feature: Aggregation ────────────────────────────────────────
        TckScenario {
            name: "COUNT(*) returns scalar total",
            feature: "Aggregation",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
                "CREATE (n:Person {name: 'Carol'})",
            ],
            when: "MATCH (n:Person) RETURN COUNT(*)",
            then_columns: &["count(*)"],
            then_rows: vec![vec![Value::Int64(3)]],
            ordered: false,
        },
        TckScenario {
            name: "COUNT(expr) counts matching nodes",
            feature: "Aggregation",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
            ],
            when: "MATCH (n:Person) RETURN COUNT(n)",
            then_columns: &["count(n)"],
            then_rows: vec![vec![Value::Int64(2)]],
            ordered: false,
        },
        // ── Feature: Limit ──────────────────────────────────────────────
        TckScenario {
            name: "LIMIT restricts result count",
            feature: "Limit",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
                "CREATE (n:Person {name: 'Carol'})",
                "CREATE (n:Person {name: 'Dave'})",
            ],
            when: "MATCH (n:Person) RETURN n.name LIMIT 2",
            then_columns: &["n.name"],
            // We only assert count, not specific rows (order unspecified)
            then_rows: vec![
                // Placeholder — we will do a count-only check for this scenario
                vec![Value::Null],
                vec![Value::Null],
            ],
            ordered: false,
        },
        // ── Feature: Skip ───────────────────────────────────────────────
        TckScenario {
            name: "SKIP skips leading rows",
            feature: "Skip",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
                "CREATE (n:Person {name: 'Carol'})",
                "CREATE (n:Person {name: 'Dave'})",
            ],
            when: "MATCH (n:Person) RETURN n.name SKIP 2",
            then_columns: &["n.name"],
            // Only check count — 4 nodes minus 2 skipped = 2 remaining
            then_rows: vec![vec![Value::Null], vec![Value::Null]],
            ordered: false,
        },
        // ── Feature: Create ─────────────────────────────────────────────
        TckScenario {
            name: "CREATE node then MATCH returns it",
            feature: "Create",
            given: &[
                "CREATE (n:Movie {title: 'The Matrix', year: 1999})",
            ],
            when: "MATCH (m:Movie) RETURN m.title, m.year",
            then_columns: &["m.title", "m.year"],
            then_rows: vec![vec![
                Value::String("The Matrix".into()),
                Value::Int64(1999),
            ]],
            ordered: false,
        },
        TckScenario {
            name: "CREATE relationship then traverse it",
            feature: "Create",
            given: &[
                "CREATE (n:Person {name: 'Keanu'})",
                "CREATE (n:Movie  {title: 'The Matrix'})",
                "MATCH (a:Person {name: 'Keanu'}), (m:Movie {title: 'The Matrix'}) CREATE (a)-[:ACTED_IN]->(m)",
            ],
            when: "MATCH (a:Person)-[:ACTED_IN]->(m:Movie) RETURN a.name, m.title",
            then_columns: &["a.name", "m.title"],
            then_rows: vec![vec![
                Value::String("Keanu".into()),
                Value::String("The Matrix".into()),
            ]],
            ordered: false,
        },
        // ── Feature: Multiple patterns ──────────────────────────────────
        TckScenario {
            name: "Two-hop traversal",
            feature: "MultiplePatterns",
            given: &[
                "CREATE (n:Person {name: 'Alice'})",
                "CREATE (n:Person {name: 'Bob'})",
                "CREATE (n:Person {name: 'Carol'})",
                "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
                "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
            ],
            when: "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, c.name",
            then_columns: &["a.name", "c.name"],
            then_rows: vec![vec![
                Value::String("Alice".into()),
                Value::String("Carol".into()),
            ]],
            ordered: false,
        },
    ]
}

// ── Special-case runners for LIMIT/SKIP (count-only) ────────────────────────

/// For LIMIT and SKIP scenarios we only assert row count since order is
/// unspecified without ORDER BY.
fn run_count_only_scenario(s: &TckScenario) -> Result<(), String> {
    let (_dir, db) = make_db();

    for (i, stmt) in s.given.iter().enumerate() {
        db.execute(stmt)
            .map_err(|e| format!("[{}] Given step {} failed: {e}", s.name, i))?;
    }

    let result = db
        .execute(s.when)
        .map_err(|e| format!("[{}] When step failed: {e}", s.name))?;

    if result.rows.len() != s.then_rows.len() {
        return Err(format!(
            "[{}] Row count mismatch: expected {}, got {}",
            s.name,
            s.then_rows.len(),
            result.rows.len()
        ));
    }

    Ok(())
}

// ── Test entry point ────────────────────────────────────────────────────────

#[test]
fn tck_scenario_runner() {
    let scenarios = all_scenarios();
    let total = scenarios.len();
    let mut passed = 0;
    let mut failed = 0;
    let mut failures: Vec<String> = Vec::new();

    for s in &scenarios {
        let is_count_only =
            s.name == "LIMIT restricts result count" || s.name == "SKIP skips leading rows";

        let result = if is_count_only {
            run_count_only_scenario(s)
        } else {
            run_scenario(s)
        };

        match result {
            Ok(()) => {
                passed += 1;
                eprintln!("  PASS  [{}] {}", s.feature, s.name);
            }
            Err(msg) => {
                failed += 1;
                eprintln!("  FAIL  [{}] {}", s.feature, s.name);
                eprintln!("        {msg}");
                failures.push(msg);
            }
        }
    }

    eprintln!();
    eprintln!("═══ TCK Summary ═══");
    eprintln!("  Total:  {total}");
    eprintln!("  Passed: {passed}");
    eprintln!("  Failed: {failed}");
    eprintln!("  Rate:   {:.1}%", (passed as f64 / total as f64) * 100.0);
    eprintln!("═══════════════════");

    if !failures.is_empty() {
        panic!(
            "{failed} TCK scenario(s) failed:\n{}",
            failures.join("\n\n")
        );
    }
}

// ── Individual scenario tests (for granular CI reporting) ───────────────────

#[test]
fn tck_match_all_nodes() {
    let s = &all_scenarios()[0];
    run_scenario(s).unwrap();
}

#[test]
fn tck_match_property_filter() {
    let s = &all_scenarios()[1];
    run_scenario(s).unwrap();
}

#[test]
fn tck_match_single_relationship() {
    let s = &all_scenarios()[2];
    run_scenario(s).unwrap();
}

#[test]
fn tck_where_equality() {
    let s = &all_scenarios()[3];
    run_scenario(s).unwrap();
}

#[test]
fn tck_where_greater_than() {
    let s = &all_scenarios()[4];
    run_scenario(s).unwrap();
}

#[test]
fn tck_where_less_than() {
    let s = &all_scenarios()[5];
    run_scenario(s).unwrap();
}

#[test]
fn tck_where_and() {
    let s = &all_scenarios()[6];
    run_scenario(s).unwrap();
}

#[test]
fn tck_return_multiple_properties() {
    let s = &all_scenarios()[7];
    run_scenario(s).unwrap();
}

#[test]
fn tck_return_distinct() {
    let s = &all_scenarios()[8];
    run_scenario(s).unwrap();
}

#[test]
fn tck_count_star() {
    let s = &all_scenarios()[9];
    run_scenario(s).unwrap();
}

#[test]
fn tck_count_expr() {
    let s = &all_scenarios()[10];
    run_scenario(s).unwrap();
}

#[test]
fn tck_limit() {
    let s = &all_scenarios()[11];
    run_count_only_scenario(s).unwrap();
}

#[test]
fn tck_skip() {
    let s = &all_scenarios()[12];
    run_count_only_scenario(s).unwrap();
}

#[test]
fn tck_create_node() {
    let s = &all_scenarios()[13];
    run_scenario(s).unwrap();
}

#[test]
fn tck_create_relationship() {
    let s = &all_scenarios()[14];
    run_scenario(s).unwrap();
}

#[test]
fn tck_two_hop_traversal() {
    let s = &all_scenarios()[15];
    run_scenario(s).unwrap();
}
