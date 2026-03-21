//! Binder: resolves label names and relationship types against the Catalog.
//!
//! Returns `Err(Error::InvalidArgument)` for unknown labels or rel types,
//! and `Err(Error::Unimplemented)` for unsupported constructs.

use sparrowdb_catalog::catalog::Catalog;
use sparrowdb_common::{Error, Result};

use crate::ast::{CreateStatement, MatchStatement, PathPattern, Statement};

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
/// - any label name is unknown
/// - any relationship type is unknown
/// - unsupported syntax is used
pub fn bind(stmt: Statement, catalog: &Catalog) -> Result<BoundStatement> {
    match &stmt {
        Statement::Create(c) => bind_create(c, catalog)?,
        Statement::MatchCreate(mc) => {
            for pat in &mc.match_patterns {
                bind_path_pattern(pat, catalog)?;
            }
            bind_create(&mc.create, catalog)?;
        }
        Statement::Match(m) => bind_match(m, catalog)?,
        Statement::Checkpoint | Statement::Optimize => {}
    }
    Ok(BoundStatement { inner: stmt })
}

fn bind_create(create: &CreateStatement, catalog: &Catalog) -> Result<()> {
    for node in &create.nodes {
        for label in &node.labels {
            ensure_label(label, catalog)?;
        }
    }
    for (_src, rel, _dst) in &create.edges {
        ensure_rel_type(&rel.rel_type, catalog)?;
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
    match catalog.get_label(name)? {
        Some(_) => Ok(()),
        None => Err(Error::InvalidArgument(format!("unknown label: {name}"))),
    }
}

fn ensure_rel_type(rel_type: &str, catalog: &Catalog) -> Result<()> {
    // Rel tables are keyed by (src_label_id, dst_label_id, rel_type).
    // In the binder we just check that ANY rel table with this rel_type exists.
    let tables = catalog.list_rel_tables()?;
    if tables.iter().any(|(_, _, rt)| rt == rel_type) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "unknown relationship type: {rel_type}"
        )))
    }
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

    #[test]
    fn bind_unknown_label_err() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (n:Ghost) RETURN n.name").unwrap();
        assert!(bind(stmt, &cat).is_err());
    }

    #[test]
    fn bind_known_rel_ok() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name").unwrap();
        bind(stmt, &cat).expect("bind must succeed");
    }

    #[test]
    fn bind_unknown_rel_err() {
        let (_dir, cat) = make_catalog();
        let stmt = parse("MATCH (a:Person)-[:HATES]->(b:Person) RETURN b.name").unwrap();
        assert!(bind(stmt, &cat).is_err());
    }
}
