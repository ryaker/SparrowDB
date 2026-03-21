//! sparrowdb-cypher: Cypher lexer, parser, AST, and binder.

pub mod ast;
pub mod binder;
pub mod lexer;
pub mod parser;

pub use ast::Statement;
pub use binder::{bind, BoundStatement};
pub use parser::parse;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_type_exists() {
        // Smoke test: the parse function is callable.
        let _ = parse("CHECKPOINT");
    }

    #[test]
    fn statement_variants_debug() {
        let s = Statement::Checkpoint;
        assert!(!format!("{s:?}").is_empty());
    }
}
