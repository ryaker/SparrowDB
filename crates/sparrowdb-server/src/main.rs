//! `sparrowdb-server` — HTTP REST server for SparrowDB (Phase A).
//!
//! Boots a [`sparrowdb::GraphDb`] from a directory and serves the HTTP API
//! defined in `sparrowdb_server`.
//!
//! ```text
//! sparrowdb-server --db /path/to/db --port 7480 --token-file /etc/sparrow.token
//! ```

use clap::Parser;
use sparrowdb::GraphDb;
use sparrowdb_server::{AuthConfig, Server, ServerConfig};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "sparrowdb-server",
    about = "HTTP REST server for SparrowDB (Phase A)"
)]
struct Args {
    /// Path to the SparrowDB database directory.
    #[arg(long, env = "SPARROWDB_PATH")]
    db: PathBuf,

    /// Bind address (IP).  Default: `127.0.0.1`.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on.  Default: `7480`.
    #[arg(long, default_value_t = 7480)]
    port: u16,

    /// File containing the bearer token (single line, trimmed).
    /// Conflicts with `--no-auth`.  If omitted, falls back to
    /// `$SPARROWDB_HTTP_TOKEN`.
    #[arg(long)]
    token_file: Option<PathBuf>,

    /// Disable authentication entirely.
    /// Refused on any non-loopback bind address.
    #[arg(long, default_value_t = false)]
    no_auth: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sparrowdb_server=info".into()),
        )
        .init();

    let args = Args::parse();

    let ip: IpAddr = args
        .bind
        .parse()
        .map_err(|e| format!("invalid --bind `{}`: {e}", args.bind))?;
    let addr = SocketAddr::new(ip, args.port);

    let auth = resolve_auth(&args)?;
    let cfg = ServerConfig {
        auth,
        ..Default::default()
    };

    let db = GraphDb::open(&args.db)?;
    tracing::info!("opened database at {:?}", args.db);

    let server = Server::new(addr, cfg)?;
    tracing::info!("HTTP server bound to {}", server.local_addr());

    server.serve(db);
    Ok(())
}

/// Determine the authentication policy from CLI flags + env vars.
fn resolve_auth(args: &Args) -> Result<AuthConfig, String> {
    if args.no_auth {
        if args.token_file.is_some() {
            return Err("--no-auth conflicts with --token-file".into());
        }
        return Ok(AuthConfig::None);
    }

    if let Some(path) = &args.token_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("could not read --token-file {path:?}: {e}"))?;
        let token = raw.trim().to_owned();
        if token.is_empty() {
            return Err(format!("token file {path:?} is empty"));
        }
        return Ok(AuthConfig::BearerToken(token));
    }

    if let Ok(token) = std::env::var("SPARROWDB_HTTP_TOKEN") {
        let token = token.trim().to_owned();
        if !token.is_empty() {
            return Ok(AuthConfig::BearerToken(token));
        }
    }

    Err(
        "no authentication configured: pass --token-file <path>, set SPARROWDB_HTTP_TOKEN, \
         or pass --no-auth (loopback bind only)"
            .into(),
    )
}
