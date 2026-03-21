use std::process::Command;

#[test]
fn cli_checkpoint_creates_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let output = Command::new(env!("CARGO_BIN_EXE_sparrowdb"))
        .args(["checkpoint", "--db", db_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Checkpoint complete"),
        "expected 'Checkpoint complete' in stdout, got: {stdout}"
    );
}

#[test]
fn cli_query_not_implemented() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let output = Command::new(env!("CARGO_BIN_EXE_sparrowdb"))
        .args([
            "query",
            "--db",
            db_path.to_str().unwrap(),
            "MATCH (n) RETURN n",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("NotImplemented"),
        "expected 'NotImplemented' in stdout, got: {stdout}"
    );
}

#[test]
fn cli_info_returns_json() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let output = Command::new(env!("CARGO_BIN_EXE_sparrowdb"))
        .args(["info", "--db", db_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Validate it is parseable JSON
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("info output should be valid JSON");
    assert!(
        parsed.get("db_path").is_some(),
        "JSON should contain db_path"
    );
}
