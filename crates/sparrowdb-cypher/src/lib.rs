use sparrowdb_common::{Error, Result};

/// Top-level Cypher statement variants — AST stub.
#[derive(Debug)]
pub enum Statement {
    Match,
    Create,
    Delete,
    Set,
    Return,
    Checkpoint,
    Optimize,
}

/// Stub Cypher parser — full implementation in Phase 4a.
pub struct Parser;

impl Parser {
    /// Create a new parser instance.
    pub fn new() -> Self {
        Parser
    }

    /// Parse a Cypher string into a `Statement`.
    pub fn parse(&self, _input: &str) -> Result<Statement> {
        Err(Error::Unimplemented)
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_type_exists() {
        let _p = Parser::new();
    }

    #[test]
    fn statement_variants_debug() {
        let s = Statement::Match;
        assert!(!format!("{s:?}").is_empty());
    }
}
