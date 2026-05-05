//! Authentication for the HTTP server.
//!
//! Phase A supports a single bearer token (or no auth, gated to loopback).

use std::net::{IpAddr, SocketAddr};

/// How the server authenticates incoming requests.
#[derive(Debug, Clone)]
pub enum AuthConfig {
    /// Require `Authorization: Bearer <token>` on every authenticated route.
    BearerToken(String),
    /// Disable authentication entirely.  Only valid when bound to loopback —
    /// see [`AuthConfig::validate_against_bind`].
    None,
}

impl AuthConfig {
    /// Reject a [`AuthConfig::None`] policy bound to a non-loopback address.
    ///
    /// The HTTP server must never accept unauthenticated requests on an
    /// externally reachable interface — that would expose the database to the
    /// open internet.  This check is enforced at construction time.
    pub fn validate_against_bind(&self, bind: SocketAddr) -> Result<(), String> {
        match self {
            AuthConfig::BearerToken(_) => Ok(()),
            AuthConfig::None => {
                if is_loopback_addr(bind.ip()) {
                    Ok(())
                } else {
                    Err(format!(
                        "refusing --no-auth on non-loopback address {bind}: \
                         bearer-token auth is required when binding to a non-loopback interface"
                    ))
                }
            }
        }
    }

    /// Check whether the supplied `Authorization` header value is acceptable.
    ///
    /// Returns `true` if auth is disabled, or the header carries a matching
    /// `Bearer <token>` value.  Comparison is constant-time-ish: it walks
    /// every byte of the expected token rather than short-circuiting on
    /// mismatch, to make timing-attack reasoning slightly easier.
    pub fn check_header(&self, header: Option<&str>) -> bool {
        match self {
            AuthConfig::None => true,
            AuthConfig::BearerToken(expected) => {
                let Some(value) = header else {
                    return false;
                };
                let Some(presented) = value.strip_prefix("Bearer ") else {
                    return false;
                };
                let presented = presented.trim();
                constant_time_eq(presented.as_bytes(), expected.as_bytes())
            }
        }
    }
}

/// Return `true` if `addr` is a loopback IP (127.0.0.0/8 or ::1).
pub fn is_loopback_addr(addr: IpAddr) -> bool {
    addr.is_loopback()
}

/// Constant-ish-time byte comparison.
///
/// Walks the longer of the two slices to avoid leaking the length of `expected`.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn loopback_v4_accepts_no_auth() {
        let cfg = AuthConfig::None;
        let addr: SocketAddr = "127.0.0.1:7480".parse().unwrap();
        assert!(cfg.validate_against_bind(addr).is_ok());
    }

    #[test]
    fn loopback_v6_accepts_no_auth() {
        let cfg = AuthConfig::None;
        let addr: SocketAddr = "[::1]:7480".parse().unwrap();
        assert!(cfg.validate_against_bind(addr).is_ok());
    }

    #[test]
    fn non_loopback_rejects_no_auth() {
        let cfg = AuthConfig::None;
        let addr: SocketAddr = "0.0.0.0:7480".parse().unwrap();
        assert!(cfg.validate_against_bind(addr).is_err());
        let addr: SocketAddr = "192.168.1.10:7480".parse().unwrap();
        assert!(cfg.validate_against_bind(addr).is_err());
    }

    #[test]
    fn non_loopback_accepts_token() {
        let cfg = AuthConfig::BearerToken("secret".into());
        let addr: SocketAddr = "0.0.0.0:7480".parse().unwrap();
        assert!(cfg.validate_against_bind(addr).is_ok());
    }

    #[test]
    fn token_check_matches() {
        let cfg = AuthConfig::BearerToken("secret".into());
        assert!(cfg.check_header(Some("Bearer secret")));
        assert!(!cfg.check_header(Some("Bearer wrong")));
        assert!(!cfg.check_header(Some("secret"))); // missing prefix
        assert!(!cfg.check_header(None));
    }

    #[test]
    fn no_auth_always_passes() {
        let cfg = AuthConfig::None;
        assert!(cfg.check_header(None));
        assert!(cfg.check_header(Some("Bearer anything")));
    }
}
