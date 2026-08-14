//! Security + fidelity tests for SPA-480 (S0): parameterized `CREATE`.
//!
//! Closes the last gap in the Cypher-injection story. `execute_with_params`
//! already covered MATCH/MERGE/SET (0.1.22/0.1.23); `CREATE` — the exact
//! shape of the original exploit — previously errored with "parameterized
//! MATCH...CREATE and standalone CREATE are not yet supported". This file
//! proves:
//!
//! 1. The original exploit (`CREATE (:User {name: "${evil}"})` built via
//!    string interpolation) is dead when the equivalent query is built with
//!    `execute_with_params` instead — for a battery of injection payloads,
//!    not just the one in the bug report.
//! 2. Every scalar parameter type (string, int, float, bool) round-trips
//!    through parameterized `CREATE` unchanged, derived by hand from the
//!    fixture (never captured from program output — see
//!    feedback_derive_expected_from_source in project memory).
//! 3. `null`/`List`/`Map` parameters bound into a CREATE property are
//!    rejected with a clear error rather than silently coerced to
//!    `Value::Int64(0)` (the #475 failure mode).
//! 4. Edge properties in both standalone `CREATE` and `MATCH...CREATE`
//!    accept `$param` the same way node properties do.
//!
//! Pre-fix behaviour (this repo's parent commit, before SPA-480): every
//! `execute_with_params` test below that targets `CREATE` or `MATCH...CREATE`
//! fails with `Error::InvalidArgument("... not yet supported ...")` rather
//! than injecting — i.e. pre-fix the attack surface was closed by refusing
//! to run, not by being safely parameterized. The meaningful proof that this
//! fix is real is `interpolated_form_actually_injects_demonstrating_the_bug`
//! below: the *interpolated* equivalent injects on both the parent commit
//! and this one (interpolation was never something this fix could patch —
//! it hands the attacker string straight to the parser), while the
//! parameterized form injects on neither, because pre-fix it errors and
//! post-fix it treats the value as inert data.

use sparrowdb::open;
use sparrowdb_execution::types::Value;
use std::collections::HashMap;

fn make_db() -> (tempfile::TempDir, sparrowdb::GraphDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open(dir.path()).expect("open");
    (dir, db)
}

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ── Security: the original exploit, verbatim ──────────────────────────────────

/// The exact scenario from the bug report:
/// ```js
/// const evil = '", role: "admin';
/// db.execute(`CREATE (:User {name: "${evil}"})`);
/// // -> node with name:"" AND an attacker-created role:"admin" property
/// ```
/// Parameterized, the same attacker string must land as one literal `name`
/// property and must not create a `role` property.
#[test]
fn injection_payload_stored_as_literal_no_role_property_created() {
    let (_dir, db) = make_db();
    let evil = "\", role: \"admin";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE with attacker string must succeed, not inject");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1, "exactly one User node must exist");
    assert_eq!(
        rows[0][0],
        Value::String(evil.to_string()),
        "name must equal the literal attacker string, unmodified"
    );

    let role_rows = db
        .execute("MATCH (n:User) RETURN n.role")
        .expect("MATCH")
        .rows;
    assert_eq!(role_rows.len(), 1);
    assert_eq!(
        role_rows[0][0],
        Value::Null,
        "no role property must have been created by the injection payload"
    );
}

/// A payload containing `}` and `)` must not close the property map or
/// the CREATE clause early.
#[test]
fn injection_payload_with_closing_brace_and_paren() {
    let (_dir, db) = make_db();
    let evil = "x\"}) CREATE (:Pwned {y:\"1";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE must succeed");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String(evil.to_string()));

    let pwned = db.execute("MATCH (n:Pwned) RETURN n").expect("MATCH").rows;
    assert_eq!(
        pwned.len(),
        0,
        "the embedded second CREATE clause must never have executed"
    );
}

/// A payload containing an embedded newline must not be treated as a
/// statement separator.
#[test]
fn injection_payload_with_newline() {
    let (_dir, db) = make_db();
    let evil = "line one\nCREATE (:Pwned)\nline two";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE must succeed");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String(evil.to_string()));
    assert_eq!(
        db.execute("MATCH (n:Pwned) RETURN n").unwrap().rows.len(),
        0
    );
}

/// A payload containing a `//` comment sequence must not truncate the rest
/// of the statement text at parse time (there is no statement text left to
/// truncate — the value is bound data).
#[test]
fn injection_payload_with_comment_sequence() {
    let (_dir, db) = make_db();
    let evil = "innocent // CREATE (:Pwned) RETURN n";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE must succeed");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String(evil.to_string()));
    assert_eq!(
        db.execute("MATCH (n:Pwned) RETURN n").unwrap().rows.len(),
        0
    );
}

/// A payload containing a double quote (the Cypher string delimiter used by
/// the exploit's `CREATE (:User {name: "..."})` shape).
#[test]
fn injection_payload_with_double_quote() {
    let (_dir, db) = make_db();
    let evil = "a\"b\"c";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE must succeed");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String(evil.to_string()));
}

/// A payload containing a single quote (the alternate Cypher string
/// delimiter).
#[test]
fn injection_payload_with_single_quote() {
    let (_dir, db) = make_db();
    let evil = "a'b'c";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE must succeed");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String(evil.to_string()));
}

/// A payload that is a complete, syntactically valid second statement must
/// not execute as one — no `:Pwned` node must exist afterward.
#[test]
fn injection_payload_full_second_statement_no_pwned_node() {
    let (_dir, db) = make_db();
    let evil = "x\"}) CREATE (:Pwned {y:\"1";

    db.execute_with_params(
        "CREATE (:User {name: $name})",
        params(&[("name", Value::String(evil.to_string()))]),
    )
    .expect("parameterized CREATE must succeed");

    assert_eq!(
        db.execute("MATCH (n:Pwned) RETURN n").unwrap().rows.len(),
        0,
        "no :Pwned node must exist"
    );
    assert_eq!(
        db.execute("MATCH (n:User) RETURN n").unwrap().rows.len(),
        1,
        "exactly one :User node must exist — the second CREATE never ran"
    );
}

// ── Security: prove the interpolated form is a genuinely different bug ────────

/// This is the negative control. String interpolation was never something
/// `execute_with_params` could patch — it hands the attacker string straight
/// to the Cypher parser before `execute_with_params` (or even `execute`) is
/// ever called. This test documents that the interpolated form still injects
/// (as designed — parameterization is the fix, not disabling interpolation)
/// while the parameterized form of the identical payload, tested above,
/// does not. This is the comparison the task asked for: proof that the
/// meaningful difference is parameterization, not some incidental change in
/// how strings are escaped.
#[test]
fn interpolated_form_actually_injects_demonstrating_the_bug() {
    let (_dir, db) = make_db();
    let evil = "\", role: \"admin";

    // The exact vulnerable pattern from the bug report: naive string
    // interpolation into a Cypher literal, executed via plain `execute()`.
    let cypher = format!("CREATE (:User {{name: \"{evil}\"}})");
    db.execute(&cypher)
        .expect("the interpolated (vulnerable) form parses and runs");

    let rows = db
        .execute("MATCH (n:User) RETURN n.name, n.role")
        .expect("MATCH")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        Value::String(String::new()),
        "interpolation truncated the name to an empty string, as the bug report describes"
    );
    assert_eq!(
        rows[0][1],
        Value::String("admin".to_string()),
        "interpolation let the attacker inject a role:\"admin\" property — \
         this is the vulnerability that parameterized CREATE exists to avoid"
    );
}

// ── Type fidelity: round-trip, values derived by hand from the fixture ────────

#[test]
fn roundtrip_string_param() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "CREATE (:Item {label: $v})",
        params(&[("v", Value::String("hello world".to_string()))]),
    )
    .expect("CREATE with string param");
    let rows = db.execute("MATCH (n:Item) RETURN n.label").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String("hello world".to_string()));
}

#[test]
fn roundtrip_int_param() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "CREATE (:Item {count: $v})",
        params(&[("v", Value::Int64(-42))]),
    )
    .expect("CREATE with int param");
    let rows = db.execute("MATCH (n:Item) RETURN n.count").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(-42));
}

#[test]
fn roundtrip_float_param() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "CREATE (:Item {ratio: $v})",
        params(&[("v", Value::Float64(12.375))]),
    )
    .expect("CREATE with float param");
    let rows = db.execute("MATCH (n:Item) RETURN n.ratio").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Float64(12.375));
}

/// Booleans are stored as `Int64(1)`/`Int64(0)` and read back the same way —
/// this is the pre-existing, documented round-trip behaviour for booleans
/// everywhere else in the codebase (`literal_to_value`); parameterized
/// CREATE deliberately matches it rather than inventing a divergent
/// convention. Fixing the underlying `Bool -> Int64` round-trip is out of
/// scope for SPA-480.
#[test]
fn roundtrip_bool_param_matches_existing_int64_convention() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "CREATE (:Item {active: $v})",
        params(&[("v", Value::Bool(true))]),
    )
    .expect("CREATE with bool param");
    let rows = db.execute("MATCH (n:Item) RETURN n.active").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        Value::Int64(1),
        "bool true round-trips as Int64(1), matching literal_to_value's convention"
    );
}

// ── Null / List / Map handling (#475 adjacency) ────────────────────────────────

/// A `$param` bound to `Value::Null` must be rejected with a clear error,
/// never silently written as `Value::Int64(0)` (the #475 failure mode).
/// This mirrors the pre-existing behaviour for a literal `null` in CREATE
/// (`CREATE (:X {p: null})` already errors on main) — one null convention,
/// not two.
#[test]
fn null_param_in_create_property_is_rejected_not_coerced_to_zero() {
    let (_dir, db) = make_db();
    let result = db.execute_with_params("CREATE (:Item {v: $v})", params(&[("v", Value::Null)]));
    assert!(
        result.is_err(),
        "a null-valued $param in a CREATE property must error"
    );

    // No node must have been created — the write must not have partially
    // succeeded with a coerced 0.
    assert_eq!(
        db.execute("MATCH (n:Item) RETURN n").unwrap().rows.len(),
        0,
        "the CREATE must not have run at all, not run with v silently set to 0"
    );
}

/// A `$param` bound to `Value::List` cannot be represented in a scalar
/// property column and must be rejected, never silently coerced to 0.
#[test]
fn list_param_in_create_property_is_rejected_not_coerced_to_zero() {
    let (_dir, db) = make_db();
    let result = db.execute_with_params(
        "CREATE (:Item {v: $v})",
        params(&[("v", Value::List(vec![Value::Int64(1), Value::Int64(2)]))]),
    );
    assert!(
        result.is_err(),
        "a List-valued $param in a CREATE property must error"
    );
    assert_eq!(db.execute("MATCH (n:Item) RETURN n").unwrap().rows.len(), 0);
}

/// A `$param` bound to `Value::Map` cannot be represented in a scalar
/// property column and must be rejected, never silently coerced to 0.
#[test]
fn map_param_in_create_property_is_rejected_not_coerced_to_zero() {
    let (_dir, db) = make_db();
    let result = db.execute_with_params(
        "CREATE (:Item {v: $v})",
        params(&[("v", Value::Map(vec![("k".to_string(), Value::Int64(1))]))]),
    );
    assert!(
        result.is_err(),
        "a Map-valued $param in a CREATE property must error"
    );
    assert_eq!(db.execute("MATCH (n:Item) RETURN n").unwrap().rows.len(), 0);
}

/// A `$param` referenced by the query but not supplied in the map must
/// error clearly rather than defaulting to anything.
#[test]
fn unbound_param_in_create_property_errors() {
    let (_dir, db) = make_db();
    let result = db.execute_with_params("CREATE (:Item {v: $missing})", HashMap::new());
    assert!(result.is_err(), "unbound $param must error");
}

/// A literal `$param` used with plain `execute()` (no params map) must
/// still error clearly, exactly as it did before SPA-480 — this path is
/// unreachable via the public API (params only arrive through
/// `execute_with_params`), but the resolver must fail closed rather than
/// silently zero the property if it is ever reached.
#[test]
fn param_reference_via_plain_execute_without_params_errors() {
    let (_dir, db) = make_db();
    let result = db.execute("CREATE (:Item {v: $v})");
    assert!(
        result.is_err(),
        "a $param reference with no params map supplied must error, not write 0"
    );
    assert_eq!(db.execute("MATCH (n:Item) RETURN n").unwrap().rows.len(), 0);
}

// ── Edge properties (standalone CREATE and MATCH...CREATE) ────────────────────

/// Standalone `CREATE (a)-[:R {weight: $w}]->(b)` with both endpoints new in
/// the same statement.
#[test]
fn standalone_create_edge_property_param_roundtrips() {
    let (_dir, db) = make_db();
    db.execute_with_params(
        "CREATE (a:Node {name: 'A'})-[:LINK {weight: $w}]->(b:Node {name: 'B'})",
        params(&[("w", Value::Int64(7))]),
    )
    .expect("standalone CREATE with edge $param");

    let rows = db
        .execute("MATCH (:Node {name: 'A'})-[r:LINK]->(:Node {name: 'B'}) RETURN r.weight")
        .expect("MATCH edge")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(7));
}

/// `MATCH ... CREATE` with a `$param` in the CREATE-clause edge property.
#[test]
fn match_create_edge_property_param_roundtrips() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Node {name: 'A'})").unwrap();
    db.execute("CREATE (:Node {name: 'B'})").unwrap();

    db.execute_with_params(
        "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) \
         CREATE (a)-[:LINK {weight: $w}]->(b)",
        params(&[("w", Value::Float64(2.5))]),
    )
    .expect("MATCH...CREATE with edge $param");

    let rows = db
        .execute("MATCH (:Node {name: 'A'})-[r:LINK]->(:Node {name: 'B'}) RETURN r.weight")
        .expect("MATCH edge")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Float64(2.5));
}

/// An injection payload bound into an edge property (via `MATCH...CREATE`)
/// must land as literal data, the same guarantee proven for node properties
/// above.
#[test]
fn match_create_edge_property_param_injection_payload_stored_literally() {
    let (_dir, db) = make_db();
    db.execute("CREATE (:Node {name: 'A'})").unwrap();
    db.execute("CREATE (:Node {name: 'B'})").unwrap();
    let evil = "x\"}]->(b) CREATE (:Pwned {y:\"1";

    db.execute_with_params(
        "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) \
         CREATE (a)-[:LINK {label: $l}]->(b)",
        params(&[("l", Value::String(evil.to_string()))]),
    )
    .expect("MATCH...CREATE with edge $param injection payload");

    let rows = db
        .execute("MATCH (:Node {name: 'A'})-[r:LINK]->(:Node {name: 'B'}) RETURN r.label")
        .expect("MATCH edge")
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String(evil.to_string()));
    assert_eq!(
        db.execute("MATCH (n:Pwned) RETURN n").unwrap().rows.len(),
        0
    );
}
