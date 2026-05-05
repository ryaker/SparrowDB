//! Integration test: spin up an HTTP server, send real HTTP requests with
//! `ureq`, verify the JSON envelope and status codes.
//!
//! Each test uses port `0` so the OS picks a free port — keeps tests
//! parallel-safe.

use sparrowdb::GraphDb;
use sparrowdb_server::{AuthConfig, Server, ServerConfig};
use std::net::SocketAddr;
use std::time::Duration;

const TOKEN: &str = "test-token-12345";

/// Spin up a server on a random loopback port and return its `SocketAddr`.
fn start_server(db: GraphDb, auth: AuthConfig) -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let cfg = ServerConfig { auth, workers: 2 };
    let server = Server::new(addr, cfg).expect("bind server");
    let local = server.local_addr();

    std::thread::spawn(move || server.serve(db));

    // Give the worker threads a moment to start consuming.
    std::thread::sleep(Duration::from_millis(50));
    local
}

fn url(addr: SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

// ── Tests ───────────────────────────────────────────────────────────

#[test]
fn health_is_unauthenticated() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::BearerToken(TOKEN.into()));

    // No Authorization header — should still succeed.
    let resp = ureq::get(&url(addr, "/health"))
        .timeout(Duration::from_secs(5))
        .call()
        .expect("GET /health");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.into_json().expect("parse json");
    assert_eq!(body["status"], "ok");
    assert!(body["version"].is_string());
}

#[test]
fn cypher_with_valid_token_executes() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::BearerToken(TOKEN.into()));

    // Seed the DB with a CREATE.
    let resp = ureq::post(&url(addr, "/cypher"))
        .set("Authorization", &format!("Bearer {TOKEN}"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_json(serde_json::json!({
            "query": "CREATE (n:Test {name: 'alice'}) RETURN n.name",
        }))
        .expect("POST /cypher");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.into_json().expect("parse json");
    assert_eq!(body["columns"], serde_json::json!(["n.name"]));
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n.name"], "alice");

    // Read it back.
    let resp = ureq::post(&url(addr, "/cypher"))
        .set("Authorization", &format!("Bearer {TOKEN}"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_json(serde_json::json!({
            "query": "MATCH (n:Test) RETURN n.name",
        }))
        .expect("POST /cypher (read)");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.into_json().unwrap();
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n.name"], "alice");
}

#[test]
fn cypher_without_auth_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::BearerToken(TOKEN.into()));

    // No Authorization header.
    let resp_err = ureq::post(&url(addr, "/cypher"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_json(serde_json::json!({ "query": "RETURN 1" }))
        .expect_err("expected 401");

    let resp = match resp_err {
        ureq::Error::Status(_, r) => r,
        ureq::Error::Transport(t) => panic!("transport error: {t}"),
    };
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.into_json().expect("parse json");
    assert!(body["error"].as_str().unwrap().contains("unauthorized"));
}

#[test]
fn cypher_with_malformed_json_returns_400() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::BearerToken(TOKEN.into()));

    let resp_err = ureq::post(&url(addr, "/cypher"))
        .set("Authorization", &format!("Bearer {TOKEN}"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_string("{ this is not valid json")
        .expect_err("expected 400");

    let resp = match resp_err {
        ureq::Error::Status(_, r) => r,
        ureq::Error::Transport(t) => panic!("transport error: {t}"),
    };
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.into_json().expect("parse json");
    assert!(body["error"].as_str().unwrap().contains("invalid JSON"));
}

#[test]
fn info_returns_structure() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::BearerToken(TOKEN.into()));

    // Create a node so we have a label registered.
    ureq::post(&url(addr, "/cypher"))
        .set("Authorization", &format!("Bearer {TOKEN}"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_json(serde_json::json!({
            "query": "CREATE (:Person {name: 'bob'})",
        }))
        .unwrap();

    let resp = ureq::get(&url(addr, "/info"))
        .set("Authorization", &format!("Bearer {TOKEN}"))
        .timeout(Duration::from_secs(5))
        .call()
        .expect("GET /info");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.into_json().expect("parse json");
    assert!(body["version"].is_string());
    assert!(body["labels"].is_array());
    assert!(body["relationship_types"].is_array());
    assert!(body["counts"]["nodes"].is_number());
    assert!(body["counts"]["edges"].is_number());

    let labels: Vec<String> = body["labels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(labels.iter().any(|l| l == "Person"));
}

#[test]
fn no_auth_rejected_on_non_loopback_bind() {
    // We don't actually bind to 0.0.0.0 — Server::new validates the policy
    // before opening the listener.
    let addr: SocketAddr = "0.0.0.0:7481".parse().unwrap();
    let cfg = ServerConfig {
        auth: AuthConfig::None,
        workers: 1,
    };
    let err = match Server::new(addr, cfg) {
        Ok(_) => panic!("must reject --no-auth on 0.0.0.0"),
        Err(e) => e,
    };
    assert!(
        err.contains("non-loopback"),
        "unexpected error message: {err}"
    );
}

#[test]
fn no_auth_allowed_on_loopback() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::None);

    let resp = ureq::post(&url(addr, "/cypher"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_json(serde_json::json!({
            "query": "RETURN 1 AS n",
        }))
        .expect("POST /cypher with no auth");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.into_json().unwrap();
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n"], 1);
}

#[test]
fn cypher_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let db = GraphDb::open(dir.path()).unwrap();
    let addr = start_server(db, AuthConfig::BearerToken(TOKEN.into()));

    let resp = ureq::post(&url(addr, "/cypher"))
        .set("Authorization", &format!("Bearer {TOKEN}"))
        .set("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .send_json(serde_json::json!({
            "query": "UNWIND $names AS name RETURN name",
            "params": { "names": ["alice", "bob", "carol"] },
        }))
        .expect("POST /cypher with params");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.into_json().unwrap();
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["name"], "alice");
    assert_eq!(rows[2]["name"], "carol");
}
