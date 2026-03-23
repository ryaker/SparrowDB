//! Binder: resolves label names and relationship types against the Catalog.
//!
//! Returns `Err(Error::InvalidArgument)` for unknown labels or rel types,
//! and `Err(Error::Unimplemented)` for unsupported constructs.
//!
//! **CREATE vs MATCH semantics:**
//! - `MATCH` labels and rel-types must exist in the catalog (otherwise there is
//!   nothing to scan).
//! - `CREATE` labels and rel-types need NOT exist yet — they are auto-registered
//!   by the execution engine on first use (SPA-156).  The binder therefore skips
//!   existence checks for `CREATE` patterns.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::Result;

use crate::ast::{
    MatchMutateStatement, MatchOptionalMatchStatement, MatchStatement, MatchWithStatement,
    PathPattern, Statement,
};

/// A bound statement — the AST annotated with resolved catalog IDs.
///
/// For Phase 4a the bound form is the same AST; IDs are validated but not
/// replaced structurally.  The execution engine resolves IDs again at query
/// time using the same catalog reference.
#[derive(Debug, Clone)]
pub struct BoundStatement {
    pub inner: Statement,
}

/// Bind a parsed `Statement` against `catalog`.
///
/// Returns `Err` if:
/// - a MATCH label name is unknown (CREATE labels are auto-registered)
/// - any relationship type referenced in MATCH is unknown
/// - unsupported syntax is used
pub fn bind(stmt: Statement, catalog: &Catalog) -> Result<BoundStatement> {
    match &stmt {
        // CREATE: labels are auto-registered on execution — no existence check.
        Statement::Create(_c) => {}
        Statement::MatchCreate(mc) => {
            // MATCH patterns must reference existing labels.
            for pat in &mc.match_patterns {
                bind_path_pattern(pat, catalog)?;
            }
            // CREATE patterns: labels auto-registered — skip existence check.
        }
        Statement::Match(m) => bind_match(m, catalog)?,
        Statement::MatchWith(mw) => bind_match_with(mw, catalog)?,
        // UNWIND does not reference labels or rel types — nothing to bind.
        Statement::Unwind(_) => {}
        Statement::Union(_) => {}
        Statement::OptionalMatch(_om) => {
            // Label binding is deferred to execution time (SPA-134).
        }
        Statement::MatchMutate(_mm) => {
            // Label binding is deferred to execution time (SPA-134).
        }
        // MATCH…MERGE relationship: validate the MATCH patterns (labels must
        // exist); the rel type may not exist yet — merge_edge will create it.
        Statement::MatchMergeRel(mm) => {
            if mm.rel_type.is_empty() {
                return Err(sparrowdb_common::Error::InvalidArgument(
                    "MERGE relationship type must not be empty; \
                     use MERGE (a)-[r:TYPE]->(b)"
                        .into(),
                ));
            }
            for pat in &mm.match_patterns {
                bind_path_pattern(pat, catalog)?;
            }
        }
        // Pipeline: label binding is deferred to execution time (SPA-134).
        Statement::Pipeline(_) => {}
    }
    Ok(BoundStatement { inner: stmt })
}

fn bind_match_with(mw: &MatchWithStatement, catalog: &Catalog) -> Result<()> {
    for pat in &mw.match_patterns {
        bind_path_pattern(pat, catalog)?;
    }
    Ok(())
}

fn bind_match(m: &MatchStatement, catalog: &Catalog) -> Result<()> {
    for pat in &m.match_patterns {
        bind_path_pattern(pat, catalog)?;
    }
    Ok(())
}

fn bind_path_pattern(pat: &PathPattern, catalog: &Catalog) -> Result<()> {
    for node in &pat.nodes {
        if !node.labels.is_empty() {
            for label in &node.labels {
                let _ = catalog.get_label_id(label)?;
            }
        }
    }

    for rel in &pat.rels {
        if !rel.types.is_empty() {
            for rel_type in &rel.types {
                let _ = catalog.get_rel_type_id(rel_type)?;
            }
        }
    }
    Ok(())
}
