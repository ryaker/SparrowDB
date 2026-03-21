use sparrowdb_common::{Error, Result};

/// Stub query executor — full implementation in Phase 4b+.
pub struct Executor;

impl Executor {
    /// Create a new executor.
    pub fn new() -> Self {
        Executor
    }

    /// Execute a bound plan and return raw rows (stub).
    pub fn execute(&self) -> Result<Vec<Vec<u8>>> {
        Err(Error::Unimplemented)
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_type_exists() {
        let _e = Executor::new();
    }
}
