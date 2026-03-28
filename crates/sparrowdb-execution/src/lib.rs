//! sparrowdb-execution: factorized execution engine.

pub mod chunk;
pub mod engine;
pub mod functions;
pub mod join;
pub mod join_spill;
pub mod json;
pub mod operators;
pub mod parallel_bfs;
pub mod pipeline;
pub mod sort_spill;
pub mod types;

pub use engine::{DegreeStats, Engine, EngineBuilder};
pub use json::{query_result_to_json, value_to_json};
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
