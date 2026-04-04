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
    MatchMergeRelStatement, MatchMutateStatement, MatchOptionalMatchStatement, MatchStatement,
    MatchWithStatement, PathPattern, Statement,
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
        // MERGE: validate that the label exists (or will be created at execution
        // time by merge_node).  We skip the strict "must exist" check so that
        // MERGE can act as a schema-creating operation, consistent with how
        // WriteTx::merge_node works (it calls create_label if missing).
        Statement::Merge(_) => {}
        // MATCH…MERGE relationship: validate the MATCH patterns (labels must
        // exist); the rel type may not exist yet — merge_edge will create it.
        Statement::MatchMergeRel(mm) => bind_match_merge_rel(mm, catalog)?,
        Statement::MatchMutate(mm) => bind_match_mutate(mm, catalog)?,
        // OPTIONAL MATCH: label/rel-type may not exist yet — that is exactly
        // the case that produces NULL rows.  Skip existence checks.
        Statement::OptionalMatch(_) => {}
        Statement::MatchOptionalMatch(mom) => bind_match_optional_match(mom, catalog)?,
        // UNION: bind both sides independently.
        Statement::Union(u) => {
            bind((*u.left).clone(), catalog)?;
            bind((*u.right).clone(), catalog)?;
        }
        Statement::Checkpoint | Statement::Optimize => {}
        // CALL: procedure name and args are validated at execution time by the
        // procedure dispatcher.  No catalog lookups are required here.
        Statement::Call(_) => {}
        // Pipeline: label binding is deferred to execution time (SPA-134).
        Statement::Pipeline(_) => {}
        Statement::CreateIndex { .. }
        | Statement::CreateConstraint { .. }
        | Statement::CreateFulltextIndex { .. }
        | Statement::CreateVectorIndex { .. }
        | Statement::DropIndex { .. } => {}
        // CALL { } subquery: label binding is deferred to the subquery's own
        // execution path, which recurses through bind/execute internally.
        Statement::CallSubquery { .. } => {}
    }
    Ok(BoundStatement { inner: stmt })
}

fn bind_match_with(mw: &MatchWithStatement, catalog: &Catalog) -> Result<()> {
    for pat in &mw.match_patterns {
        bind_path_pattern(pat, catalog)?;
    }
    Ok(())
}

fn bind_match_mutate(mm: &MatchMutateStatement, catalog: &Catalog) -> Result<()> {
    for pat in &mm.match_patterns {
        bind_path_pattern(pat, catalog)?;
    }
    // The mutation itself (SET/DELETE) targets variables already bound by
    // the MATCH patterns — no additional catalog lookups are needed here.
    Ok(())
}

fn bind_match_merge_rel(mm: &MatchMergeRelStatement, catalog: &Catalog) -> Result<()> {
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
    Ok(())
}

fn bind_match(m: &MatchStatement, catalog: &Catalog) -> Result<()> {
    for pat in &m.pattern {
        bind_path_pattern(pat, catalog)?;
    }
    Ok(())
}

fn bind_path_pattern(pat: &PathPattern, catalog: &Catalog) -> Result<()> {
    for node in &pat.nodes {
        for label in &node.labels {
            ensure_label(label, catalog)?;
        }
    }
    for rel in &pat.rels {
        if !rel.rel_type.is_empty() {
            ensure_rel_type(&rel.rel_type, catalog)?;
        }
    }
    Ok(())
}

fn ensure_label(name: &str, catalog: &Catalog) -> Result<()> {
    // SPA-245: unknown labels in MATCH patterns yield 0 rows at execution time
    // (standard Cypher semantics).  The binder no longer rejects them — only
    // I/O errors from the catalog are propagated.
    let _ = catalog.get_label(name)?;
    Ok(())
}

fn ensure_rel_type(_rel_type: &str, catalog: &Catalog) -> Result<()> {
    // SPA-245: unknown rel-types in MATCH patterns yield 0 rows at execution
    // time.  We still call list_rel_tables to propagate any I/O errors, but we
    // no longer return InvalidArgument when the type is absent.
    let _ = catalog.list_rel_tables()?;
    Ok(())
}

fn bind_match_optional_match(_mom: &MatchOptionalMatchStatement, _catalog: &Catalog) -> Result<()> {
    // Both the leading MATCH and OPTIONAL MATCH patterns skip strict existence
    // checks: if a label doesn't exist in the catalog the engine returns 0 rows
    // for the leading MATCH (Cypher semantics — no error) and NULL rows for the
    // OPTIONAL MATCH.  Suppressing the binder check here avoids a premature
    // "unknown label" error before the engine has a chance to handle it cleanly.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    fn make_catalog() -> (tempfile::TempDir, Catalog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cat = Catalog::open(dir.path()).expect("catalog open");
        let pid = cat.create_label("Person").expect("Person");
        cat.create_rel_table(pid, pid, "KNOWS").expect("KNOWS");
        (dir, cat)
    }

    #[test]
    fn bind_known_label_ok() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (n:Person) RETURN n.name").unwrap();
        bind(stmt, &cat).expect("bind must succeed");
    }

    /// SPA-245: unknown labels in MATCH patterns are no longer rejected by the
    /// binder — the execution engine returns 0 rows instead of an error.
    #[test]
    fn bind_unknown_label_ok() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (n:Ghost) RETURN n.name").unwrap();
        bind(stmt, &cat).expect("unknown label in MATCH must bind OK (SPA-245)");
    }

    #[test]
    fn bind_known_rel_ok() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name").unwrap();
        bind(stmt, &cat).expect("bind must succeed");
    }

    /// SPA-245: unknown rel-types in MATCH patterns are no longer rejected by
    /// the binder — the execution engine returns 0 rows instead of an error.
    #[test]
    fn bind_unknown_rel_ok() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (a:Person)-[:HATES]->(b:Person) RETURN b.name").unwrap();
        bind(stmt, &cat).expect("unknown rel-type in MATCH must bind OK (SPA-245)");
    }

    /// SPA-156: CREATE with a label that is not yet in the catalog must bind
    /// successfully (labels are auto-registered at execution time).
    #[test]
    fn bind_create_unknown_label_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cat = Catalog::open(dir.path()).expect("empty catalog");
        // "Ghost" is not registered — but CREATE should still bind OK.
        let stmt = parse("CREATE (n:Ghost {name: 'Casper'})").unwrap();
        bind(stmt, &cat).expect("CREATE with unknown label must bind OK");
    }

    /// SPA-156: CREATE with a label that is already registered should also succeed.
    #[test]
    fn bind_create_known_label_ok() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        bind(stmt, &cat).expect("CREATE with known label must bind OK");
    }
}
