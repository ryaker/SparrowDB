//! sparrowdb-execution: factorized execution engine.

pub mod engine;
pub mod join;
pub mod join_spill;
pub mod operators;
pub mod sort_spill;
pub mod types;

pub use engine::Engine;
pub use operators::UnwindOperator;
pub use types::{FactorizedChunk, QueryResult, Value, VectorGroup};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_type_exists() {
        // Smoke test: the Engine type is accessible.
        let _: fn() = || {
            let _ = std::mem::size_of::<Engine>();
        };
    }
}
