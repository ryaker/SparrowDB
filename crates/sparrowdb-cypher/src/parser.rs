//! Recursive-descent Cypher parser.
//!
//! Entry point: [`parse`].  Returns `Err(...)` — never panics — for
//! unsupported syntax.

use sparrowdb_common::{Error, Result};

use crate::ast::{
    BinOpKind, CallStatement, CreateStatement, EdgeDir, ExistsPattern, Expr, ListPredicateKind,
    Literal, MatchCreateStatement, MatchMergeRelStatement, MatchMutateStatement,
    MatchOptionalMatchStatement, MatchStatement, MergeStatement, Mutation, NodePattern,
    OptionalMatchStatement, PathPattern, PipelineStage, PipelineStatement, PropEntry, RelPattern,
    ReturnClause, ReturnItem, ShortestPathExpr, SortDir, Statement, UnionStatement,
    UnwindStatement, WithClause, WithItem,
};
use crate::lexer::{tokenize, Token};

/// Discriminated result of parsing one `ON CREATE SET …` / `ON MATCH SET …` clause.
enum OnClause {
    Create(Vec<Mutation>),
    Match(Vec<Mutation>),
}

/// Map a keyword `Token` variant to its canonical string representation.
///
/// Returns `None` for non-keyword tokens (e.g. `Ident`, `Integer`, punctuation).
fn token_keyword_name(tok: &Token) -> Option<&'static str> {
    match tok {
        Token::Match => Some("MATCH"),
        Token::Create => Some("CREATE"),
        Token::Return => Some("RETURN"),
        Token::Where => Some("WHERE"),
        Token::Not => Some("NOT"),
        Token::And => Some("AND"),
        Token::Or => Some("OR"),
        Token::Order => Some("ORDER"),
        Token::By => Some("BY"),
        Token::Asc => Some("ASC"),
        Token::Desc => Some("DESC"),
        Token::Limit => Some("LIMIT"),
        Token::Skip => Some("SKIP"),
        Token::Distinct => Some("DISTINCT"),
        Token::Optional => Some("OPTIONAL"),
        Token::Union => Some("UNION"),
        Token::Unwind => Some("UNWIND"),
        Token::Delete => Some("DELETE"),
        Token::Detach => Some("DETACH"),
        Token::Set => Some("SET"),
        Token::Merge => Some("MERGE"),
        Token::Checkpoint => Some("CHECKPOINT"),
        Token::Optimize => Some("OPTIMIZE"),
        Token::Contains => Some("CONTAINS"),
        Token::StartsWith => Some("STARTS"),
        Token::EndsWith => Some("ENDS"),
        Token::Count => Some("COUNT"),
        Token::Null => Some("NULL"),
        Token::True => Some("TRUE"),
        Token::False => Some("FALSE"),
        Token::As => Some("AS"),
        Token::With => Some("WITH"),
        Token::Exists => Some("EXISTS"),
        Token::In => Some("IN"),
        Token::Any => Some("ANY"),
        Token::All => Some("ALL"),
        Token::NoneKw => Some("NONE"),
        Token::Single => Some("SINGLE"),
        Token::Is => Some("IS"),
        Token::Call => Some("CALL"),
        Token::Yield => Some("YIELD"),
        Token::Case => Some("CASE"),
        Token::When => Some("WHEN"),
        Token::Then => Some("THEN"),
        Token::Else => Some("ELSE"),
        Token::End => Some("END"),
        Token::Index => Some("INDEX"),
        Token::On => Some("ON"),
        Token::Constraint => Some("CONSTRAINT"),
        Token::Assert => Some("ASSERT"),
        Token::Fulltext => Some("FULLTEXT"),
        Token::For => Some("FOR"),
        _ => None,
    }
}

/// Parse a Cypher statement string.  Returns `Err` for any unsupported or
/// malformed input; never panics.
pub fn parse(input: &str) -> Result<Statement> {
    if input.trim().is_empty() {
        return Err(Error::InvalidArgument("empty input".into()));
    }
    let tokens = tokenize(input)?;
    let mut p = Parser::new(tokens);
    let stmt = p.parse_statement()?;

    // Check for UNION / UNION ALL between two statements.
    let stmt = if matches!(p.peek(), Token::Union) {
        p.advance();
        let all = if matches!(p.peek(), Token::All)
            || matches!(p.peek(), Token::Ident(ref s) if s.to_uppercase() == "ALL")
        {
            p.advance();
            true
        } else {
            false
        };
        let right = p.parse_statement()?;
        Statement::Union(UnionStatement {
            left: Box::new(stmt),
            right: Box::new(right),
            all,
        })
    } else {
        stmt
    };

    // Consume optional trailing semicolon.
    if matches!(p.peek(), Token::Semicolon) {
        p.advance();
    }
    // All tokens must now be consumed.
    if !matches!(p.peek(), Token::Eof) {
        return Err(Error::InvalidArgument(format!(
            "unexpected trailing token: {:?}",
            p.peek()
        )));
    }
    Ok(stmt)
}

// ── Parser cursor ─────────────────────────────────────────────────────────────

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn peek2(&self) -> &Token {
        if self.pos + 1 < self.tokens.len() {
            &self.tokens[self.pos + 1]
        } else {
            &Token::Eof
        }
    }

    fn advance(&mut self) -> &Token {
        let tok = &self.tokens[self.pos];
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            other => Err(Error::InvalidArgument(format!(
                "expected identifier, got {:?}",
                other
            ))),
        }
    }

    /// Accept the next token as an identifier for a label or relationship type.
    ///
    /// In standard Cypher, backtick-escaped identifiers allow reserved words to
    /// be used as labels or relationship types.  After lexing, backtick-quoted
    /// words are already `Token::Ident`, but *unquoted* reserved words (e.g.
    /// `CONTAINS`, `ORDER`) are lexed as keyword tokens.  This helper accepts
    /// both `Token::Ident` and any keyword token and returns the string form,
    /// so that `:CONTAINS` and `` :`CONTAINS` `` both work.
    fn expect_label_or_type(&mut self) -> Result<String> {
        let tok = self.advance().clone();
        match tok {
            Token::Ident(s) => Ok(s),
            ref kw => token_keyword_name(kw)
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    Error::InvalidArgument(format!("expected label/type name, got {:?}", tok))
                }),
        }
    }

    fn expect_tok(&mut self, expected: &Token) -> Result<()> {
        let got = self.advance().clone();
        if &got == expected {
            Ok(())
        } else {
            Err(Error::InvalidArgument(format!(
                "expected {:?}, got {:?}",
                expected, got
            )))
        }
    }

    /// Consume the next token and return it as a property-name string.
    ///
    /// Cypher allows keywords to appear as property names when they follow a
    /// dot (`n.count`) or appear as map keys (`{count: 42}`).  This helper
    /// accepts any keyword token that is syntactically unambiguous in a
    /// property-name position and converts it to its lowercase string
    /// equivalent (SPA-265).
    fn advance_as_prop_name(&mut self) -> Result<String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            // Aggregate and common keywords that users legitimately use as
            // property names.
            Token::Count => Ok("count".into()),
            Token::With => Ok("with".into()),
            Token::As => Ok("as".into()),
            Token::In => Ok("in".into()),
            Token::Is => Ok("is".into()),
            Token::Not => Ok("not".into()),
            Token::And => Ok("and".into()),
            Token::Or => Ok("or".into()),
            Token::Contains => Ok("contains".into()),
            Token::StartsWith => Ok("starts".into()),
            Token::EndsWith => Ok("ends".into()),
            Token::Null => Ok("null".into()),
            Token::True => Ok("true".into()),
            Token::False => Ok("false".into()),
            Token::Set => Ok("set".into()),
            Token::Delete => Ok("delete".into()),
            Token::Detach => Ok("detach".into()),
            Token::Merge => Ok("merge".into()),
            Token::Match => Ok("match".into()),
            Token::Where => Ok("where".into()),
            Token::Return => Ok("return".into()),
            Token::Order => Ok("order".into()),
            Token::By => Ok("by".into()),
            Token::Asc => Ok("asc".into()),
            Token::Desc => Ok("desc".into()),
            Token::Limit => Ok("limit".into()),
            Token::Skip => Ok("skip".into()),
            Token::Distinct => Ok("distinct".into()),
            Token::Optional => Ok("optional".into()),
            Token::Union => Ok("union".into()),
            Token::Unwind => Ok("unwind".into()),
            Token::Create => Ok("create".into()),
            Token::Exists => Ok("exists".into()),
            Token::Any => Ok("any".into()),
            Token::All => Ok("all".into()),
            Token::NoneKw => Ok("none".into()),
            Token::Single => Ok("single".into()),
            Token::Call => Ok("call".into()),
            Token::Yield => Ok("yield".into()),
            Token::Case => Ok("case".into()),
            Token::When => Ok("when".into()),
            Token::Then => Ok("then".into()),
            Token::Else => Ok("else".into()),
            Token::End => Ok("end".into()),
            Token::Index => Ok("index".into()),
            Token::On => Ok("on".into()),
            Token::Constraint => Ok("constraint".into()),
            Token::Assert => Ok("assert".into()),
            Token::Fulltext => Ok("fulltext".into()),
            Token::For => Ok("for".into()),
            other => Err(Error::InvalidArgument(format!(
                "expected property name, got {:?}",
                other
            ))),
        }
    }
}

// ── Statement dispatch ────────────────────────────────────────────────────────

impl Parser {
    fn parse_statement(&mut self) -> Result<Statement> {
        match self.peek().clone() {
            Token::Match => self.parse_match_or_match_mutate(),
            Token::Create => self.parse_create(),
            Token::Merge => self.parse_merge(),
            Token::Checkpoint => {
                self.advance();
                Ok(Statement::Checkpoint)
            }
            Token::Optimize => {
                self.advance();
                Ok(Statement::Optimize)
            }
            Token::Optional => self.parse_optional_match(),
            Token::Union => Err(Error::InvalidArgument(
                "unexpected UNION: use 'MATCH ... RETURN ... UNION MATCH ... RETURN ...'".into(),
            )),
            Token::Unwind => self.parse_unwind(),
            // Standalone RETURN (no MATCH): `RETURN expr [AS alias], ...`
            Token::Return => self.parse_standalone_return(),
            // CALL procedure(args) YIELD col [RETURN ...]
            Token::Call => self.parse_call(),
            other => Err(Error::InvalidArgument(format!(
                "unexpected token at statement start: {:?}",
                other
            ))),
        }
    }

    /// Parse `RETURN expr [AS alias], ...` with no preceding MATCH clause.
    ///
    /// Emits a `Statement::Match` with an empty pattern list.  The execution
    /// engine detects the empty pattern and evaluates the RETURN items as
    /// pure scalar expressions (functions, literals, etc.).
    fn parse_standalone_return(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Return)?;
        let distinct = if matches!(self.peek(), Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };
        let items = self.parse_return_items()?;
        Ok(Statement::Match(MatchStatement {
            pattern: vec![],
            where_clause: None,
            return_clause: ReturnClause { items },
            order_by: vec![],
            skip: None,
            limit: None,
            distinct,
        }))
    }

    // ── MATCH (or MATCH ... CREATE / SET / DELETE) ────────────────────────────

    fn parse_match_or_match_mutate(&mut self) -> Result<Statement> {
        // Parse the MATCH clause first, then dispatch on the following keyword.
        self.expect_tok(&Token::Match)?;

        let patterns = self.parse_pattern_list()?;

        match self.peek().clone() {
            Token::Create => {
                // MATCH ... CREATE — used to add edges
                self.advance();
                let create = self.parse_create_body()?;
                Ok(Statement::MatchCreate(MatchCreateStatement {
                    match_patterns: patterns,
                    match_props: vec![],
                    create,
                }))
            }
            Token::Set => {
                // MATCH ... SET var.prop = expr [, var.prop = expr ...]
                self.advance();
                let mutations = self.parse_set_items()?;
                Ok(Statement::MatchMutate(MatchMutateStatement {
                    match_patterns: patterns,
                    where_clause: None,
                    mutations,
                }))
            }
            Token::Detach => {
                // MATCH ... DETACH DELETE var
                self.advance();
                self.expect_tok(&Token::Delete)?;
                let var = self.expect_ident()?;
                Ok(Statement::MatchMutate(MatchMutateStatement {
                    match_patterns: patterns,
                    where_clause: None,
                    mutations: vec![Mutation::Delete { var, detach: true }],
                }))
            }
            Token::Delete => {
                // MATCH ... DELETE var
                self.advance();
                let var = self.expect_ident()?;
                Ok(Statement::MatchMutate(MatchMutateStatement {
                    match_patterns: patterns,
                    where_clause: None,
                    mutations: vec![Mutation::Delete { var, detach: false }],
                }))
            }
            Token::Where => {
                // MATCH ... WHERE expr (SET|DELETE|RETURN)
                self.advance();
                let where_expr = self.parse_expr()?;
                match self.peek().clone() {
                    Token::Set => {
                        self.advance();
                        let mutations = self.parse_set_items()?;
                        Ok(Statement::MatchMutate(MatchMutateStatement {
                            match_patterns: patterns,
                            where_clause: Some(where_expr),
                            mutations,
                        }))
                    }
                    Token::Detach => {
                        // MATCH ... WHERE expr DETACH DELETE var
                        self.advance();
                        self.expect_tok(&Token::Delete)?;
                        let var = self.expect_ident()?;
                        Ok(Statement::MatchMutate(MatchMutateStatement {
                            match_patterns: patterns,
                            where_clause: Some(where_expr),
                            mutations: vec![Mutation::Delete { var, detach: true }],
                        }))
                    }
                    Token::Delete => {
                        self.advance();
                        let var = self.expect_ident()?;
                        Ok(Statement::MatchMutate(MatchMutateStatement {
                            match_patterns: patterns,
                            where_clause: Some(where_expr),
                            mutations: vec![Mutation::Delete { var, detach: false }],
                        }))
                    }
                    Token::With => {
                        // MATCH … WHERE … WITH … RETURN
                        self.parse_with_pipeline(patterns, Some(where_expr))
                    }
                    Token::Merge => {
                        // MATCH … WHERE … MERGE (a)-[r:TYPE]->(b)
                        self.parse_match_merge_rel_tail(patterns, Some(where_expr))
                    }
                    Token::Call => {
                        // MATCH … WHERE … CALL { } … RETURN — pipeline with WHERE guard.
                        self.advance(); // consume CALL
                        let call_stage = self.parse_call_subquery_stage()?;
                        self.parse_pipeline_continuation(
                            Some(patterns),
                            Some(where_expr),
                            None,
                            vec![call_stage],
                        )
                    }
                    _ => {
                        // Fall through to RETURN parsing with the parsed WHERE expr.
                        self.finish_match_return(patterns, Some(where_expr))
                    }
                }
            }
            Token::With => {
                // MATCH … WITH … RETURN pipeline
                self.parse_with_pipeline(patterns, None)
            }
            Token::Merge => {
                // MATCH … MERGE (a)-[r:TYPE]->(b) — find-or-create relationship (SPA-233)
                self.parse_match_merge_rel_tail(patterns, None)
            }
            Token::Return
            | Token::Order
            | Token::Skip
            | Token::Limit
            | Token::Eof
            | Token::Semicolon => self.finish_match_return(patterns, None),
            Token::Optional => {
                // MATCH … OPTIONAL MATCH … RETURN
                self.parse_match_optional_match_tail(patterns, None)
            }
            Token::Call => {
                // MATCH … CALL { } … RETURN — route into the pipeline engine.
                // Consume CALL and parse as a pipeline with the CALL as first stage.
                self.advance(); // consume CALL
                let call_stage = self.parse_call_subquery_stage()?;
                self.parse_pipeline_continuation(Some(patterns), None, None, vec![call_stage])
            }
            other => Err(Error::InvalidArgument(format!(
                "unexpected token after MATCH pattern: {:?}",
                other
            ))),
        }
    }

    // ── OPTIONAL MATCH (standalone) ───────────────────────────────────────────

    /// Parse `OPTIONAL MATCH pattern [WHERE expr] RETURN …`
    fn parse_optional_match(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Optional)?;
        self.expect_tok(&Token::Match)?;

        let patterns = self.parse_pattern_list()?;

        // Optional WHERE clause.
        let where_clause = if matches!(self.peek(), Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        // RETURN clause
        self.expect_tok(&Token::Return)?;
        let distinct = if matches!(self.peek(), Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };
        let items = self.parse_return_items()?;
        let return_clause = ReturnClause { items };

        // ORDER BY
        let order_by = if matches!(self.peek(), Token::Order) {
            self.advance();
            self.expect_tok(&Token::By)?;
            self.parse_order_by_items()?
        } else {
            vec![]
        };

        // SKIP
        let skip = if matches!(self.peek(), Token::Skip) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("SKIP must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after SKIP, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        // LIMIT
        let limit = if matches!(self.peek(), Token::Limit) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("LIMIT must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after LIMIT, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        Ok(Statement::OptionalMatch(OptionalMatchStatement {
            pattern: patterns,
            where_clause,
            return_clause,
            order_by,
            skip,
            limit,
            distinct,
        }))
    }

    // ── MATCH … OPTIONAL MATCH … RETURN ───────────────────────────────────────

    /// Parse the `OPTIONAL MATCH … RETURN` tail after `MATCH patterns` has been
    /// consumed.  `match_patterns` is already parsed; `match_where` is the
    /// WHERE predicate from the leading MATCH (if any).
    fn parse_match_optional_match_tail(
        &mut self,
        match_patterns: Vec<PathPattern>,
        match_where: Option<Expr>,
    ) -> Result<Statement> {
        // Consume OPTIONAL MATCH.
        self.expect_tok(&Token::Optional)?;
        self.expect_tok(&Token::Match)?;

        let optional_patterns = self.parse_pattern_list()?;

        // Optional WHERE clause on the OPTIONAL MATCH.
        let optional_where = if matches!(self.peek(), Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        // RETURN clause.
        self.expect_tok(&Token::Return)?;
        let distinct = if matches!(self.peek(), Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };
        let items = self.parse_return_items()?;
        let return_clause = ReturnClause { items };

        // ORDER BY
        let order_by = if matches!(self.peek(), Token::Order) {
            self.advance();
            self.expect_tok(&Token::By)?;
            self.parse_order_by_items()?
        } else {
            vec![]
        };

        // SKIP
        let skip = if matches!(self.peek(), Token::Skip) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("SKIP must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after SKIP, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        // LIMIT
        let limit = if matches!(self.peek(), Token::Limit) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("LIMIT must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after LIMIT, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        Ok(Statement::MatchOptionalMatch(MatchOptionalMatchStatement {
            match_patterns,
            match_where,
            optional_patterns,
            optional_where,
            return_clause,
            order_by,
            skip,
            limit,
            distinct,
        }))
    }

    /// Parse one or more `var.prop = expr` SET items separated by commas.
    ///
    /// Called after the `SET` keyword has already been consumed.
    /// Returns a `Vec<Mutation::Set>` with at least one item.
    fn parse_set_items(&mut self) -> Result<Vec<Mutation>> {
        let mut items = Vec::new();
        loop {
            let var = self.expect_ident()?;
            self.expect_tok(&Token::Dot)?;
            // Use advance_as_prop_name so keyword tokens (e.g. `match`, `count`)
            // are accepted as property names, consistent with SPA-265 behavior.
            let prop = self.advance_as_prop_name()?;
            self.expect_tok(&Token::Eq)?;
            let value = self.parse_expr()?;
            // Guard: the write helpers (expr_to_value / expr_to_value_with_params)
            // only materialise Expr::Literal values.  Any other expression (e.g.
            // arithmetic, property access) would be silently coerced to
            // Value::Int64(0) and corrupt data.  Reject them at parse time until
            // a full expression evaluator is wired into the SET path.
            if !matches!(value, Expr::Literal(_)) {
                return Err(Error::InvalidArgument(
                    "SET property value must be a literal or $parameter".into(),
                ));
            }
            items.push(Mutation::Set { var, prop, value });
            if matches!(self.peek(), Token::Comma) {
                self.advance(); // consume the comma
            } else {
                break;
            }
        }
        Ok(items)
    }

    /// Shared helper: finish parsing a MATCH … RETURN statement after the
    /// pattern list (and optional WHERE expr) have already been consumed.
    fn finish_match_return(
        &mut self,
        patterns: Vec<PathPattern>,
        pre_where: Option<Expr>,
    ) -> Result<Statement> {
        // If caller already parsed WHERE, use it; otherwise try to parse it now.
        let where_clause = if pre_where.is_some() { pre_where } else { None };

        // RETURN clause
        let (distinct, return_clause) = if matches!(self.peek(), Token::Return) {
            self.advance();
            let distinct = if matches!(self.peek(), Token::Distinct) {
                self.advance();
                true
            } else {
                false
            };
            let items = self.parse_return_items()?;
            (distinct, ReturnClause { items })
        } else {
            return Err(Error::InvalidArgument("expected RETURN clause".into()));
        };

        // ORDER BY
        let order_by = if matches!(self.peek(), Token::Order) {
            self.advance();
            self.expect_tok(&Token::By)?;
            self.parse_order_by_items()?
        } else {
            vec![]
        };

        // SKIP
        let skip = if matches!(self.peek(), Token::Skip) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("SKIP must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after SKIP, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        // LIMIT
        let limit = if matches!(self.peek(), Token::Limit) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("LIMIT must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after LIMIT, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        Ok(Statement::Match(MatchStatement {
            pattern: patterns,
            where_clause,
            return_clause,
            order_by,
            skip,
            limit,
            distinct,
        }))
    }

    // ── MATCH … WITH … RETURN pipeline ────────────────────────────────────────

    /// Parse `MATCH pattern [WHERE pred] WITH expr AS alias [, …] [WHERE pred] RETURN …`.
    fn parse_with_pipeline(
        &mut self,
        patterns: Vec<PathPattern>,
        match_where: Option<Expr>,
    ) -> Result<Statement> {
        use crate::ast::MatchWithStatement;

        // Consume WITH token.
        self.expect_tok(&Token::With)?;

        // Parse one or more `expr [AS alias]` items separated by commas.
        // `AS alias` is optional: `WITH n` is equivalent to `WITH n AS n`.
        let mut items: Vec<WithItem> = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if matches!(self.peek(), Token::As) {
                self.advance(); // consume AS
                self.expect_ident()?
            } else {
                // No AS — derive alias from the expression.
                // Only bare variable references have an obvious implicit alias.
                match &expr {
                    crate::ast::Expr::Var(name) => name.clone(),
                    _ => {
                        return Err(Error::InvalidArgument(
                            "WITH item without AS alias must be a bare variable (e.g. WITH n)"
                                .into(),
                        ))
                    }
                }
            };
            items.push(WithItem { expr, alias });
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Optional WHERE clause on the WITH stage.
        let with_where = if matches!(self.peek(), Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let with_clause = WithClause {
            items,
            where_clause: with_where,
        };

        // Optional ORDER BY / SKIP / LIMIT that belong to this WITH stage.
        // These are consumed before checking for a continuation clause (MATCH/WITH/UNWIND).
        let with_order_by = if matches!(self.peek(), Token::Order) {
            self.advance();
            self.expect_tok(&Token::By)?;
            self.parse_order_by_items()?
        } else {
            vec![]
        };

        let with_skip = if matches!(self.peek(), Token::Skip) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("SKIP must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after SKIP, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        let with_limit = if matches!(self.peek(), Token::Limit) {
            self.advance();
            match self.advance().clone() {
                Token::Integer(n) => {
                    if n < 0 {
                        return Err(Error::InvalidArgument("LIMIT must be non-negative".into()));
                    }
                    Some(n as u64)
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected integer after LIMIT, got {:?}",
                        other
                    )))
                }
            }
        } else {
            None
        };

        // Peek at the next token to decide: RETURN → simple MatchWith,
        // MATCH/WITH/UNWIND → multi-stage Pipeline (SPA-134).
        match self.peek().clone() {
            Token::Return => {
                // Simple single-WITH pipeline: MATCH … WITH … RETURN …
                self.advance(); // consume RETURN
                let distinct = if matches!(self.peek(), Token::Distinct) {
                    self.advance();
                    true
                } else {
                    false
                };
                let return_items = self.parse_return_items()?;
                let return_clause = ReturnClause {
                    items: return_items,
                };

                // ORDER BY / SKIP / LIMIT on the RETURN (i.e. not on the WITH).
                let order_by = if matches!(self.peek(), Token::Order) {
                    self.advance();
                    self.expect_tok(&Token::By)?;
                    self.parse_order_by_items()?
                } else {
                    with_order_by
                };

                let skip = if matches!(self.peek(), Token::Skip) {
                    self.advance();
                    match self.advance().clone() {
                        Token::Integer(n) => {
                            if n < 0 {
                                return Err(Error::InvalidArgument(
                                    "SKIP must be non-negative".into(),
                                ));
                            }
                            Some(n as u64)
                        }
                        other => {
                            return Err(Error::InvalidArgument(format!(
                                "expected integer after SKIP, got {:?}",
                                other
                            )))
                        }
                    }
                } else {
                    with_skip
                };

                let limit = if matches!(self.peek(), Token::Limit) {
                    self.advance();
                    match self.advance().clone() {
                        Token::Integer(n) => {
                            if n < 0 {
                                return Err(Error::InvalidArgument(
                                    "LIMIT must be non-negative".into(),
                                ));
                            }
                            Some(n as u64)
                        }
                        other => {
                            return Err(Error::InvalidArgument(format!(
                                "expected integer after LIMIT, got {:?}",
                                other
                            )))
                        }
                    }
                } else {
                    with_limit
                };

                Ok(Statement::MatchWith(MatchWithStatement {
                    match_patterns: patterns,
                    match_where,
                    with_clause,
                    return_clause,
                    order_by,
                    skip,
                    limit,
                    distinct,
                }))
            }
            // Continuation clause: MATCH, WITH, UNWIND, or CALL {} → build Pipeline.
            Token::Match | Token::With | Token::Unwind | Token::Call => {
                let first_with_stage = PipelineStage::With {
                    clause: with_clause,
                    order_by: with_order_by,
                    skip: with_skip,
                    limit: with_limit,
                };
                self.parse_pipeline_continuation(
                    Some(patterns),
                    match_where,
                    None,
                    vec![first_with_stage],
                )
            }
            other => Err(Error::InvalidArgument(format!(
                "expected RETURN, MATCH, WITH, UNWIND, or CALL after WITH clause, got {:?}",
                other
            ))),
        }
    }

    /// Parse the remainder of a multi-clause pipeline after the initial stages have
    /// been set up.  Accumulates additional MATCH / WITH / UNWIND stages until a
    /// RETURN clause terminates the pipeline.
    fn parse_pipeline_continuation(
        &mut self,
        leading_match: Option<Vec<PathPattern>>,
        leading_where: Option<Expr>,
        leading_unwind: Option<(crate::ast::Expr, String)>,
        mut stages: Vec<PipelineStage>,
    ) -> Result<Statement> {
        loop {
            match self.peek().clone() {
                Token::Match => {
                    // Parse a MATCH stage.
                    self.advance(); // consume MATCH
                    let patterns = self.parse_pattern_list()?;
                    let where_clause = if matches!(self.peek(), Token::Where) {
                        self.advance();
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };
                    stages.push(PipelineStage::Match {
                        patterns,
                        where_clause,
                    });
                }
                Token::With => {
                    // Parse a WITH stage.
                    self.advance(); // consume WITH
                    let mut items: Vec<WithItem> = Vec::new();
                    loop {
                        let expr = self.parse_expr()?;
                        self.expect_tok(&Token::As)?;
                        let alias = self.expect_ident()?;
                        items.push(WithItem { expr, alias });
                        if matches!(self.peek(), Token::Comma) {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    let where_clause = if matches!(self.peek(), Token::Where) {
                        self.advance();
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };
                    let clause = WithClause {
                        items,
                        where_clause,
                    };
                    // ORDER BY / SKIP / LIMIT on this intermediate WITH.
                    let order_by = if matches!(self.peek(), Token::Order) {
                        self.advance();
                        self.expect_tok(&Token::By)?;
                        self.parse_order_by_items()?
                    } else {
                        vec![]
                    };
                    let skip = if matches!(self.peek(), Token::Skip) {
                        self.advance();
                        match self.advance().clone() {
                            Token::Integer(n) => {
                                if n < 0 {
                                    return Err(Error::InvalidArgument(
                                        "SKIP must be non-negative".into(),
                                    ));
                                }
                                Some(n as u64)
                            }
                            other => {
                                return Err(Error::InvalidArgument(format!(
                                    "expected integer after SKIP, got {:?}",
                                    other
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    let limit = if matches!(self.peek(), Token::Limit) {
                        self.advance();
                        match self.advance().clone() {
                            Token::Integer(n) => {
                                if n < 0 {
                                    return Err(Error::InvalidArgument(
                                        "LIMIT must be non-negative".into(),
                                    ));
                                }
                                Some(n as u64)
                            }
                            other => {
                                return Err(Error::InvalidArgument(format!(
                                    "expected integer after LIMIT, got {:?}",
                                    other
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    stages.push(PipelineStage::With {
                        clause,
                        order_by,
                        skip,
                        limit,
                    });
                }
                Token::Unwind => {
                    // Parse an UNWIND stage: UNWIND alias_var AS new_alias.
                    self.advance(); // consume UNWIND
                                    // In pipeline context the "list" to unwind is a variable name.
                    let alias = self.expect_ident()?;
                    self.expect_tok(&Token::As)?;
                    let new_alias = self.expect_ident()?;
                    stages.push(PipelineStage::Unwind { alias, new_alias });
                }
                Token::Call => {
                    // Parse a CALL { } subquery stage inside a pipeline.
                    self.advance(); // consume CALL
                    let stage = self.parse_call_subquery_stage()?;
                    stages.push(stage);
                }
                Token::Return => {
                    // Terminal clause.
                    self.advance(); // consume RETURN
                    let distinct = if matches!(self.peek(), Token::Distinct) {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let return_items = self.parse_return_items()?;
                    let return_clause = ReturnClause {
                        items: return_items,
                    };
                    let return_order_by = if matches!(self.peek(), Token::Order) {
                        self.advance();
                        self.expect_tok(&Token::By)?;
                        self.parse_order_by_items()?
                    } else {
                        vec![]
                    };
                    let return_skip = if matches!(self.peek(), Token::Skip) {
                        self.advance();
                        match self.advance().clone() {
                            Token::Integer(n) => {
                                if n < 0 {
                                    return Err(Error::InvalidArgument(
                                        "SKIP must be non-negative".into(),
                                    ));
                                }
                                Some(n as u64)
                            }
                            other => {
                                return Err(Error::InvalidArgument(format!(
                                    "expected integer after SKIP, got {:?}",
                                    other
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    let return_limit = if matches!(self.peek(), Token::Limit) {
                        self.advance();
                        match self.advance().clone() {
                            Token::Integer(n) => {
                                if n < 0 {
                                    return Err(Error::InvalidArgument(
                                        "LIMIT must be non-negative".into(),
                                    ));
                                }
                                Some(n as u64)
                            }
                            other => {
                                return Err(Error::InvalidArgument(format!(
                                    "expected integer after LIMIT, got {:?}",
                                    other
                                )))
                            }
                        }
                    } else {
                        None
                    };
                    return Ok(Statement::Pipeline(PipelineStatement {
                        leading_match,
                        leading_where,
                        leading_unwind,
                        stages,
                        return_clause,
                        return_order_by,
                        return_skip,
                        return_limit,
                        distinct,
                    }));
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected MATCH, WITH, UNWIND, CALL, or RETURN in pipeline, got {:?}",
                        other
                    )))
                }
            }
        }
    }

    // ── MERGE ─────────────────────────────────────────────────────────────────

    /// Parse `MERGE (:Label {prop: val, ...})` and `MATCH...MERGE (a)-[:R]->(b)` patterns.
    ///
    /// Supports single-node MERGE and relationship MERGE via the match-merge path.
    fn parse_merge(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Merge)?;
        self.expect_tok(&Token::LParen)?;

        // Optional variable name — capture it so RETURN can reference it.
        let var = if let Token::Ident(_) = self.peek().clone() {
            if matches!(self.peek2(), Token::Colon) {
                // `n:Label` — capture variable name.
                let name = match self.advance().clone() {
                    Token::Ident(s) => s,
                    _ => unreachable!(),
                };
                name
            } else {
                // ambiguous or anonymous — treat as anonymous
                String::new()
            }
        } else {
            String::new()
        };

        // Label(s) — at least one required for MERGE.
        if !matches!(self.peek(), Token::Colon) {
            return Err(Error::InvalidArgument(
                "MERGE requires a label (e.g. MERGE (:Person {...}))".into(),
            ));
        }
        self.advance(); // consume ':'
        let label = self.expect_label_or_type()?;

        // Property map (optional but typical for MERGE).
        let props = if matches!(self.peek(), Token::LBrace) {
            self.parse_prop_map()?
        } else {
            vec![]
        };

        self.expect_tok(&Token::RParen)?;

        // Optional `ON CREATE SET …` / `ON MATCH SET …` clauses (in any order).
        let mut on_create_set: Vec<Mutation> = Vec::new();
        let mut on_match_set: Vec<Mutation> = Vec::new();
        while matches!(self.peek(), Token::On) {
            self.advance(); // consume ON
            let items = self.parse_on_clause()?;
            match items {
                OnClause::Create(mutations) => on_create_set.extend(mutations),
                OnClause::Match(mutations) => on_match_set.extend(mutations),
            }
        }

        // Optional RETURN clause.
        let return_clause = if matches!(self.peek(), Token::Return) {
            self.advance(); // consume RETURN
            let items = self.parse_return_items()?;
            Some(ReturnClause { items })
        } else {
            None
        };

        Ok(Statement::Merge(MergeStatement {
            var,
            label,
            props,
            return_clause,
            on_create_set,
            on_match_set,
        }))
    }

    // ── MATCH … MERGE relationship ────────────────────────────────────────────

    /// Parse the `MERGE (a)-[r:TYPE]->(b)` tail after a MATCH clause (SPA-233).
    fn parse_match_merge_rel_tail(
        &mut self,
        match_patterns: Vec<PathPattern>,
        where_clause: Option<Expr>,
    ) -> Result<Statement> {
        self.expect_tok(&Token::Merge)?;

        let src_node = self.parse_node_pattern()?;
        let src_var = src_node.var;

        if !matches!(self.peek(), Token::Dash | Token::Arrow | Token::LeftArrow) {
            return Err(Error::InvalidArgument(format!(
                "expected relationship pattern after node in MERGE, got {:?}",
                self.peek()
            )));
        }
        let rel_pat = self.parse_rel_pattern()?;
        if rel_pat.dir != EdgeDir::Outgoing {
            return Err(Error::InvalidArgument(
                "MERGE relationship pattern must use outgoing direction: (a)-[r:TYPE]->(b)".into(),
            ));
        }

        let dst_node = self.parse_node_pattern()?;
        let dst_var = dst_node.var;

        // Optional `ON CREATE SET …` / `ON MATCH SET …` clauses — parsed and ignored
        // for MATCH…MERGE relationship statements (relationship-level ON clauses are
        // not yet supported in the executor, but we parse them to avoid syntax errors).
        while matches!(self.peek(), Token::On) {
            self.advance(); // consume ON
            self.parse_on_clause()?;
        }

        Ok(Statement::MatchMergeRel(MatchMergeRelStatement {
            match_patterns,
            where_clause,
            src_var,
            rel_var: rel_pat.var,
            rel_type: rel_pat.rel_type,
            dst_var,
        }))
    }

    /// Parse an `ON CREATE SET …` or `ON MATCH SET …` clause after a MERGE.
    ///
    /// Called after the `ON` keyword has already been consumed.
    /// Returns the discriminated mutations so callers can route them to the
    /// correct `on_create_set` / `on_match_set` bucket.
    fn parse_on_clause(&mut self) -> Result<OnClause> {
        match self.peek().clone() {
            Token::Create => {
                self.advance(); // consume CREATE
                self.expect_tok(&Token::Set)?;
                let mutations = self.parse_set_items()?;
                Ok(OnClause::Create(mutations))
            }
            Token::Match => {
                self.advance(); // consume MATCH
                self.expect_tok(&Token::Set)?;
                let mutations = self.parse_set_items()?;
                Ok(OnClause::Match(mutations))
            }
            other => Err(Error::InvalidArgument(format!(
                "expected CREATE or MATCH after ON, got {:?}",
                other
            ))),
        }
    }

    // ── CREATE ────────────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Create)?;
        if matches!(self.peek(), Token::Index) {
            self.advance();
            return self.parse_create_index();
        }
        if matches!(self.peek(), Token::Constraint) {
            self.advance();
            return self.parse_create_constraint();
        }
        // CREATE FULLTEXT INDEX [name] FOR (n:Label) ON (n.prop)
        if matches!(self.peek(), Token::Fulltext) {
            self.advance(); // consume FULLTEXT
            return self.parse_create_fulltext_index();
        }
        let mut body = self.parse_create_body()?;
        // Check for optional RETURN clause (issue #366).
        if matches!(self.peek(), Token::Return) {
            self.advance(); // consume RETURN
            let items = self.parse_return_items()?;
            body.return_clause = Some(ReturnClause { items });
        }
        Ok(Statement::Create(body))
    }
    fn parse_create_index(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::On)?;
        self.expect_tok(&Token::Colon)?;
        let label = self.expect_label_or_type()?;
        self.expect_tok(&Token::LParen)?;
        let property = self.expect_ident()?;
        self.expect_tok(&Token::RParen)?;
        Ok(Statement::CreateIndex { label, property })
    }
    fn parse_create_constraint(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::On)?;
        self.expect_tok(&Token::LParen)?;
        let _var = self.expect_ident()?;
        self.expect_tok(&Token::Colon)?;
        let label = self.expect_label_or_type()?;
        self.expect_tok(&Token::RParen)?;
        match self.advance().clone() {
            Token::Assert => {}
            other => {
                return Err(Error::InvalidArgument(format!(
                    "expected ASSERT, got {:?}",
                    other
                )))
            }
        }
        let _prop_var = self.expect_ident()?;
        self.expect_tok(&Token::Dot)?;
        let property = self.expect_ident()?;
        self.expect_tok(&Token::Is)?;
        match self.advance().clone() {
            Token::Ident(ref s) if s.eq_ignore_ascii_case("UNIQUE") => {}
            other => {
                return Err(Error::InvalidArgument(format!(
                    "expected UNIQUE, got {:?}",
                    other
                )))
            }
        }
        Ok(Statement::CreateConstraint { label, property })
    }

    /// Parse `CREATE FULLTEXT INDEX [name] FOR (n:Label) ON (n.property)`.
    ///
    /// The optional `name` is an unquoted identifier that appears immediately
    /// after `INDEX`.  If the next token is `FOR` the name is omitted.
    ///
    /// Grammar:
    /// ```text
    /// CREATE FULLTEXT INDEX [<name>] FOR (<var>:<Label>) ON (<var>.<prop>)
    /// ```
    fn parse_create_fulltext_index(&mut self) -> Result<Statement> {
        // Expect "INDEX"
        self.expect_tok(&Token::Index)?;

        // Optional name: present if next token is an Ident *and* the one after
        // is not FOR (i.e. the name is followed by FOR).
        let name: Option<String> = match self.peek().clone() {
            Token::Ident(s) => {
                // Peek ahead: if the token after the ident is FOR, consume name.
                self.advance();
                Some(s)
            }
            Token::For => None,
            other => {
                return Err(Error::InvalidArgument(format!(
                    "CREATE FULLTEXT INDEX: expected index name or FOR, got {other:?}"
                )))
            }
        };

        // Expect FOR
        self.expect_tok(&Token::For)?;

        // Expect `(var:Label)`
        self.expect_tok(&Token::LParen)?;
        let _var = self.expect_ident()?; // e.g. "n" — consumed but not used
        self.expect_tok(&Token::Colon)?;
        let label = self.expect_label_or_type()?;
        self.expect_tok(&Token::RParen)?;

        // Expect ON
        self.expect_tok(&Token::On)?;

        // Expect `(var.property)`
        self.expect_tok(&Token::LParen)?;
        let _prop_var = self.expect_ident()?; // e.g. "n" — consumed but not used
        self.expect_tok(&Token::Dot)?;
        let property = self.advance_as_prop_name()?;
        self.expect_tok(&Token::RParen)?;

        Ok(Statement::CreateFulltextIndex {
            name,
            label,
            property,
        })
    }

    // ── UNWIND ────────────────────────────────────────────────────────────────

    /// Parse `UNWIND <expr> AS <var> RETURN <items>`.
    ///
    /// The list expression may be:
    /// - A list literal:    `[1, 2, 3]`
    /// - A parameter ref:   `$items`
    ///
    /// NOTE: `range(start, end)` function support is a TODO — it will be added
    /// when the function-call execution layer is extended.
    fn parse_unwind(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Unwind)?;
        let expr = self.parse_unwind_expr()?;
        self.expect_tok(&Token::As)?;
        let alias = self.expect_ident()?;

        // If the next token is WITH or MATCH, this is a pipeline:
        //   UNWIND … WITH … RETURN  (SPA-134)
        //   UNWIND … MATCH … RETURN (SPA-237)
        if matches!(self.peek(), Token::With | Token::Match) {
            return self.parse_pipeline_continuation(None, None, Some((expr, alias)), vec![]);
        }

        self.expect_tok(&Token::Return)?;
        let items = self.parse_return_items()?;
        Ok(Statement::Unwind(UnwindStatement {
            expr,
            alias,
            return_clause: ReturnClause { items },
        }))
    }

    /// Parse the list-producing expression for UNWIND.
    ///
    /// Accepts:
    /// - `[elem, ...]`  — list literal
    /// - `$param`       — parameter (evaluated at runtime to a list)
    /// - `fn(args)`     — function call that returns a list (e.g. `range(1, 5)`)
    fn parse_unwind_expr(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Token::LBracket => self.parse_list_literal(),
            Token::Param(p) => {
                self.advance();
                Ok(Expr::Literal(Literal::Param(p)))
            }
            Token::Ident(_) => {
                // May be a function call like range(1, 5).
                self.parse_atom()
            }
            other => Err(Error::InvalidArgument(format!(
                "UNWIND expects a list literal [..], $param, or a function call, got {:?}",
                other
            ))),
        }
    }

    /// Parse `[expr, expr, ...]` into `Expr::List`.
    fn parse_list_literal(&mut self) -> Result<Expr> {
        self.expect_tok(&Token::LBracket)?;
        let mut elems = Vec::new();
        if !matches!(self.peek(), Token::RBracket) {
            loop {
                elems.push(self.parse_expr()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        self.expect_tok(&Token::RBracket)?;
        Ok(Expr::List(elems))
    }

    fn parse_create_body(&mut self) -> Result<CreateStatement> {
        let mut nodes = Vec::new();
        let mut edges: Vec<(String, RelPattern, String)> = Vec::new();

        loop {
            if !matches!(self.peek(), Token::LParen) {
                break;
            }
            // Could be a node or the start of an edge
            let node = self.parse_node_pattern()?;
            let node_var = node.var.clone();

            if matches!(self.peek(), Token::Dash | Token::Arrow | Token::LeftArrow) {
                // Edge pattern: (a)-[:R]->(b)
                // Both endpoint nodes are always emitted so that the executor
                // can create them and resolve variable bindings for the edge.
                // Nodes without a variable name are anonymous and need not be
                // tracked, but they still get created.
                nodes.push(node);
                let rel = self.parse_rel_pattern()?;
                let dst_node = self.parse_node_pattern()?;
                let dst_var = dst_node.var.clone();
                edges.push((node_var, rel, dst_var));
                nodes.push(dst_node);
            } else {
                nodes.push(node);
            }

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        if nodes.is_empty() && edges.is_empty() {
            return Err(Error::InvalidArgument(
                "CREATE body must contain at least one node or edge pattern".into(),
            ));
        }

        Ok(CreateStatement {
            nodes,
            edges,
            return_clause: None,
        })
    }

    // ── Pattern list ──────────────────────────────────────────────────────────

    fn parse_pattern_list(&mut self) -> Result<Vec<PathPattern>> {
        let mut patterns = Vec::new();
        patterns.push(self.parse_path_pattern()?);
        while matches!(self.peek(), Token::Comma) {
            self.advance();
            patterns.push(self.parse_path_pattern()?);
        }
        Ok(patterns)
    }

    fn parse_path_pattern(&mut self) -> Result<PathPattern> {
        let mut nodes = Vec::new();
        let mut rels = Vec::new();

        nodes.push(self.parse_node_pattern()?);

        while matches!(self.peek(), Token::Dash | Token::Arrow | Token::LeftArrow) {
            // Check for variable-length paths: -[:R*n..m]->
            let rel = self.parse_rel_pattern()?;
            nodes.push(self.parse_node_pattern()?);
            rels.push(rel);
        }

        Ok(PathPattern { nodes, rels })
    }

    // ── Node pattern ──────────────────────────────────────────────────────────

    fn parse_node_pattern(&mut self) -> Result<NodePattern> {
        self.expect_tok(&Token::LParen)?;

        let var = match self.peek().clone() {
            Token::Ident(s) if !matches!(self.peek2(), Token::LParen) => {
                self.advance();
                s
            }
            _ => String::new(),
        };

        let mut labels = Vec::new();
        while matches!(self.peek(), Token::Colon) {
            self.advance();
            let label = self.expect_label_or_type()?;
            labels.push(label);
        }

        let props = if matches!(self.peek(), Token::LBrace) {
            self.parse_prop_map()?
        } else {
            vec![]
        };

        self.expect_tok(&Token::RParen)?;
        Ok(NodePattern { var, labels, props })
    }

    // ── Relationship pattern ──────────────────────────────────────────────────

    fn parse_rel_pattern(&mut self) -> Result<RelPattern> {
        // Syntax: -[:REL]-> or <-[:REL]- or -[:REL]-
        let incoming = if matches!(self.peek(), Token::LeftArrow) {
            self.advance();
            true
        } else if matches!(self.peek(), Token::Dash) {
            self.advance();
            false
        } else {
            return Err(Error::InvalidArgument(format!(
                "expected - or <- for relationship, got {:?}",
                self.peek()
            )));
        };

        // Supports: [r:REL_TYPE], [:REL_TYPE], [r], []
        self.expect_tok(&Token::LBracket)?;

        // SPA-195: parse optional variable name and optional rel type.
        //
        // Supported forms inside `[...]`:
        //   [r]          → var="r", rel_type=""      (variable only, no type filter)
        //   [:TYPE]      → var="",  rel_type="TYPE"  (type filter, no variable)
        //   [r:TYPE]     → var="r", rel_type="TYPE"  (both)
        //   [:TYPE*M..N] → variable-length with type
        //   [*]          → error (variable-length requires a rel type)
        let var = match self.peek().clone() {
            Token::Ident(s) if matches!(self.peek2(), Token::Colon) => {
                // `r:TYPE` — consume the variable identifier; colon handled below.
                self.advance();
                s
            }
            Token::Ident(s) if matches!(self.peek2(), Token::RBracket) => {
                // `[r]` — variable only, no rel-type constraint (SPA-198).
                self.advance();
                s
            }
            Token::Ident(s) if matches!(self.peek2(), Token::RBracket | Token::Star) => {
                // `r]` or `r*` — variable only, no rel type.
                // Variable-length without type (`[r*]`) is not valid Cypher;
                // emit a clear error rather than a confusing type-not-found one.
                self.advance();
                if matches!(self.peek(), Token::Star) {
                    return Err(Error::InvalidArgument(
                        "variable-length paths require a relationship type: \
                         use [r:R*] not [r*]"
                            .into(),
                    ));
                }
                self.expect_tok(&Token::RBracket)?;
                // Parse direction after `]`.
                let dir = if incoming {
                    if matches!(self.peek(), Token::Dash) {
                        self.advance();
                    } else {
                        return Err(Error::InvalidArgument(format!(
                            "expected '-' after ']' for incoming relationship, got {:?}",
                            self.peek()
                        )));
                    }
                    EdgeDir::Incoming
                } else if matches!(self.peek(), Token::Arrow) {
                    self.advance();
                    EdgeDir::Outgoing
                } else if matches!(self.peek(), Token::Dash) {
                    self.advance();
                    EdgeDir::Both
                } else {
                    return Err(Error::InvalidArgument(format!(
                        "expected '->' or '-' after ']' for outgoing/undirected \
                         relationship, got {:?}",
                        self.peek()
                    )));
                };
                return Ok(RelPattern {
                    var: s,
                    rel_type: String::new(),
                    dir,
                    min_hops: None,
                    max_hops: None,
                    props: vec![],
                });
            }
            _ => String::new(),
        };

        // Parse optional colon + rel type, or detect illegal bare star.
        let rel_type = if matches!(self.peek(), Token::Colon) {
            self.advance(); // consume ':'
            self.expect_label_or_type()?
        } else if matches!(self.peek(), Token::Star) {
            return Err(Error::InvalidArgument(
                "variable-length paths require a relationship type: use [:R*] not [*]".into(),
            ));
        } else if matches!(self.peek(), Token::RBracket) {
            // `[r]` or `[]` — no rel-type at all; leave rel_type empty.
            let rel_type = String::new();
            // Parse direction and return early.
            self.expect_tok(&Token::RBracket)?;
            let dir = if incoming {
                if matches!(self.peek(), Token::Dash) {
                    self.advance();
                } else {
                    return Err(Error::InvalidArgument(format!(
                        "expected '-' after ']' for incoming relationship, got {:?}",
                        self.peek()
                    )));
                }
                EdgeDir::Incoming
            } else if matches!(self.peek(), Token::Arrow) {
                self.advance();
                EdgeDir::Outgoing
            } else if matches!(self.peek(), Token::Dash) {
                self.advance();
                EdgeDir::Both
            } else {
                return Err(Error::InvalidArgument(format!(
                    "expected '->' or '-' after ']' for outgoing/undirected relationship, got {:?}",
                    self.peek()
                )));
            };
            return Ok(RelPattern {
                var,
                rel_type,
                dir,
                min_hops: None,
                max_hops: None,
                props: vec![],
            });
        } else {
            // No colon — rel type is unspecified (matches any relationship type).
            String::new()
        };

        // Parse optional inline property map: `[:R {key: val, ...}]`.
        // Props come after the rel type and before any hop spec or closing `]`.
        let rel_props: Vec<PropEntry> = if matches!(self.peek(), Token::LBrace) {
            self.parse_prop_map()?
        } else {
            vec![]
        };

        // Parse optional variable-length hop spec after rel type:
        //   [:R*]      -> min=1, max=unbounded (capped at 10 in engine)
        //   [:R*N]     -> min=N, max=N
        //   [:R*M..N]  -> min=M, max=N
        //   [:R*M..]   -> min=M, max=unbounded
        //   [:R*..N]   -> min=1, max=N
        let (min_hops, max_hops) = if matches!(self.peek(), Token::Star) {
            self.advance(); // consume '*'
            if matches!(self.peek(), Token::DotDot) {
                // [:R*..N]
                self.advance(); // consume '..'
                let max = match self.advance().clone() {
                    Token::Integer(n) if n >= 0 => n as u32,
                    other => {
                        return Err(Error::InvalidArgument(format!(
                            "expected integer after '..', got {:?}",
                            other
                        )))
                    }
                };
                (Some(1u32), Some(max))
            } else if let Token::Integer(n) = self.peek().clone() {
                let first = n as u32;
                self.advance(); // consume first integer
                if matches!(self.peek(), Token::DotDot) {
                    self.advance(); // consume '..'
                    if let Token::Integer(m) = self.peek().clone() {
                        let second = m as u32;
                        self.advance(); // consume second integer
                                        // [:R*M..N]
                        (Some(first), Some(second))
                    } else {
                        // [:R*M..] -> min=M, max=unbounded
                        (Some(first), None)
                    }
                } else {
                    // [:R*N] -> min=N, max=N
                    (Some(first), Some(first))
                }
            } else {
                // [:R*] -> min=1, max=unbounded
                (Some(1u32), None)
            }
        } else {
            (None, None)
        };

        self.expect_tok(&Token::RBracket)?;

        // -> or - (outgoing/undirected) or -
        let dir = if incoming {
            // <-[:R]- means incoming; the trailing '-' is required.
            if matches!(self.peek(), Token::Dash) {
                self.advance();
            } else {
                return Err(Error::InvalidArgument(format!(
                    "expected '-' after ']' for incoming relationship, got {:?}",
                    self.peek()
                )));
            }
            EdgeDir::Incoming
        } else {
            // -[:R]-> or -[:R]-; an arrow or dash is required.
            if matches!(self.peek(), Token::Arrow) {
                self.advance();
                EdgeDir::Outgoing
            } else if matches!(self.peek(), Token::Dash) {
                self.advance();
                EdgeDir::Both
            } else {
                return Err(Error::InvalidArgument(format!(
                    "expected '->' or '-' after ']' for outgoing/undirected relationship, got {:?}",
                    self.peek()
                )));
            }
        };

        Ok(RelPattern {
            var,
            rel_type,
            dir,
            min_hops,
            max_hops,
            props: rel_props,
        })
    }

    // ── Property map ──────────────────────────────────────────────────────────

    fn parse_prop_map(&mut self) -> Result<Vec<PropEntry>> {
        self.expect_tok(&Token::LBrace)?;
        let mut entries = Vec::new();

        if matches!(self.peek(), Token::RBrace) {
            self.advance();
            return Ok(entries);
        }

        loop {
            // SPA-265: property keys in map literals may be keyword tokens
            // (e.g. `{count: 42}`).
            let key = self.advance_as_prop_name()?;
            self.expect_tok(&Token::Colon)?;
            let value = self.parse_expr()?;
            entries.push(PropEntry { key, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_tok(&Token::RBrace)?;
        Ok(entries)
    }

    // ── Literals ──────────────────────────────────────────────────────────────

    fn parse_literal(&mut self) -> Result<Literal> {
        match self.advance().clone() {
            Token::Integer(n) => Ok(Literal::Int(n)),
            Token::Float(f) => Ok(Literal::Float(f)),
            Token::Str(s) => Ok(Literal::String(s)),
            Token::Param(p) => Ok(Literal::Param(p)),
            Token::Null => Ok(Literal::Null),
            Token::True => Ok(Literal::Bool(true)),
            Token::False => Ok(Literal::Bool(false)),
            other => Err(Error::InvalidArgument(format!(
                "expected literal, got {:?}",
                other
            ))),
        }
    }

    // ── Expressions ───────────────────────────────────────────────────────────

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;
        while matches!(self.peek(), Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expr()?;
        while matches!(self.peek(), Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr> {
        if matches!(self.peek(), Token::Not) {
            self.advance();
            // NOT (a)-[:R]->(b) — existence predicate, or NOT (expr) — parenthesized expr
            if matches!(self.peek(), Token::LParen) {
                // Try existence pattern first; fall back to parenthesized expr
                let saved_pos = self.pos;
                match self.parse_path_pattern() {
                    Ok(path) => return Ok(Expr::NotExists(Box::new(ExistsPattern { path }))),
                    Err(_) => {
                        self.pos = saved_pos; // restore position
                                              // fall through to parse as normal NOT expr
                    }
                }
            }
            let inner = self.parse_comparison()?;
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;

        // Handle `expr IS NULL` / `expr IS NOT NULL`
        if matches!(self.peek(), Token::Is) {
            self.advance(); // consume IS
            if matches!(self.peek(), Token::Not) {
                self.advance(); // consume NOT
                                // Expect NULL
                match self.peek().clone() {
                    Token::Null => {
                        self.advance();
                        return Ok(Expr::IsNotNull(Box::new(left)));
                    }
                    other => {
                        return Err(Error::InvalidArgument(format!(
                            "expected NULL after IS NOT, got {:?}",
                            other
                        )));
                    }
                }
            } else {
                // Expect NULL
                match self.peek().clone() {
                    Token::Null => {
                        self.advance();
                        return Ok(Expr::IsNull(Box::new(left)));
                    }
                    other => {
                        return Err(Error::InvalidArgument(format!(
                            "expected NULL after IS, got {:?}",
                            other
                        )));
                    }
                }
            }
        }

        // Handle `expr IN [...]`
        if matches!(self.peek(), Token::In) {
            self.advance(); // consume IN
            self.expect_tok(&Token::LBracket)?;
            let list = self.parse_in_list()?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: false,
            });
        }

        let op = match self.peek().clone() {
            Token::Eq => BinOpKind::Eq,
            Token::Neq => BinOpKind::Neq,
            Token::Lt => BinOpKind::Lt,
            Token::Le => BinOpKind::Le,
            Token::Gt => BinOpKind::Gt,
            Token::Ge => BinOpKind::Ge,
            Token::Contains => BinOpKind::Contains,
            Token::StartsWith => {
                self.advance();
                // STARTS WITH — the WITH keyword is mandatory.
                match self.peek().clone() {
                    Token::With | Token::Ident(_) => {
                        self.advance();
                    }
                    other => {
                        return Err(Error::InvalidArgument(format!(
                            "expected WITH after STARTS, got {:?}",
                            other
                        )));
                    }
                }
                let right = self.parse_atom()?;
                return Ok(Expr::BinOp {
                    left: Box::new(left),
                    op: BinOpKind::StartsWith,
                    right: Box::new(right),
                });
            }
            Token::EndsWith => {
                self.advance();
                // ENDS WITH — the WITH keyword is mandatory.
                match self.peek().clone() {
                    Token::With | Token::Ident(_) => {
                        self.advance();
                    }
                    other => {
                        return Err(Error::InvalidArgument(format!(
                            "expected WITH after ENDS, got {:?}",
                            other
                        )));
                    }
                }
                let right = self.parse_atom()?;
                return Ok(Expr::BinOp {
                    left: Box::new(left),
                    op: BinOpKind::EndsWith,
                    right: Box::new(right),
                });
            }
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_additive()?;
        Ok(Expr::BinOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    /// Parse additive expressions: `a + b`, `a - b`.
    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOpKind::Add,
                Token::Dash => BinOpKind::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Parse multiplicative expressions: `a * b`, `a / b`, `a % b`.
    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_atom()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOpKind::Mul,
                Token::Slash => BinOpKind::Div,
                Token::Percent => BinOpKind::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_atom()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Parse a comma-separated list of atom expressions up to `]`.
    /// Assumes `[` has already been consumed.  Returns the list and consumes `]`.
    fn parse_in_list(&mut self) -> Result<Vec<Expr>> {
        let mut items = Vec::new();
        if matches!(self.peek(), Token::RBracket) {
            self.advance(); // consume `]`
            return Ok(items); // empty list
        }
        loop {
            items.push(self.parse_atom()?);
            match self.peek().clone() {
                Token::Comma => {
                    self.advance();
                }
                Token::RBracket => {
                    self.advance();
                    break;
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected ',' or ']' in IN list, got {:?}",
                        other
                    )));
                }
            }
        }
        Ok(items)
    }

    fn parse_atom(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Token::Ident(var) => {
                // Could be var.prop, a function call fn(args), or just var.
                let next2 = self.peek2().clone();
                if matches!(next2, Token::Dot) {
                    self.advance(); // var
                    self.advance(); // .
                                    // SPA-265: property names may be keyword tokens (e.g. `n.count`).
                    let prop = self.advance_as_prop_name()?;
                    Ok(Expr::PropAccess { var, prop })
                } else if matches!(next2, Token::LParen) {
                    // Special-case shortestPath(…) and allShortestPaths(…) — SPA-136.
                    if var.to_lowercase() == "shortestpath"
                        || var.to_lowercase() == "allshortestpaths"
                    {
                        self.advance(); // consume function name
                        return self.parse_shortest_path_fn();
                    }
                    // Function call: name(arg, arg, ...)
                    self.advance(); // consume function name
                    self.advance(); // consume '('
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Token::RParen) {
                        loop {
                            args.push(self.parse_expr()?);
                            if matches!(self.peek(), Token::Comma) {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect_tok(&Token::RParen)?;
                    Ok(Expr::FnCall { name: var, args })
                } else {
                    self.advance();
                    Ok(Expr::Var(var))
                }
            }
            Token::Count => {
                self.advance();
                self.expect_tok(&Token::LParen)?;
                if self.peek() == &Token::Star {
                    // COUNT(*) — the well-known star form.
                    self.advance();
                    self.expect_tok(&Token::RParen)?;
                    Ok(Expr::CountStar)
                } else {
                    // COUNT(expr) — treat as a named aggregate FnCall.
                    let arg = self.parse_expr()?;
                    self.expect_tok(&Token::RParen)?;
                    Ok(Expr::FnCall {
                        name: "count".to_string(),
                        args: vec![arg],
                    })
                }
            }
            Token::Integer(_)
            | Token::Float(_)
            | Token::Str(_)
            | Token::Param(_)
            | Token::Null
            | Token::True
            | Token::False => {
                let lit = self.parse_literal()?;
                Ok(Expr::Literal(lit))
            }
            Token::LParen => {
                self.advance();
                let e = self.parse_expr()?;
                self.expect_tok(&Token::RParen)?;
                Ok(e)
            }
            // Inline list literal: [expr, expr, ...]
            Token::LBracket => self.parse_list_literal(),
            // List predicate: ANY(x IN list_expr WHERE predicate)
            Token::Any | Token::All | Token::NoneKw | Token::Single => {
                let kind = match self.advance().clone() {
                    Token::Any => ListPredicateKind::Any,
                    Token::All => ListPredicateKind::All,
                    Token::NoneKw => ListPredicateKind::None,
                    Token::Single => ListPredicateKind::Single,
                    _ => unreachable!(),
                };
                self.expect_tok(&Token::LParen)?;
                let variable = self.expect_ident()?;
                self.expect_tok(&Token::In)?;
                let list_expr = self.parse_expr()?;
                self.expect_tok(&Token::Where)?;
                let predicate = self.parse_expr()?;
                self.expect_tok(&Token::RParen)?;
                Ok(Expr::ListPredicate {
                    kind,
                    variable,
                    list_expr: Box::new(list_expr),
                    predicate: Box::new(predicate),
                })
            }
            // EXISTS { pattern } — positive existence subquery (SPA-137).
            Token::Exists => {
                self.advance(); // consume EXISTS
                self.expect_tok(&Token::LBrace)?;
                let path = self.parse_path_pattern()?;
                self.expect_tok(&Token::RBrace)?;
                Ok(Expr::ExistsSubquery(Box::new(ExistsPattern { path })))
            }
            // CASE WHEN cond THEN val [WHEN cond THEN val]* [ELSE val] END (SPA-138).
            Token::Case => {
                self.advance(); // consume CASE
                let mut branches: Vec<(Expr, Expr)> = Vec::new();
                let mut else_expr: Option<Box<Expr>> = None;
                let mut seen_when = false;
                let mut seen_else = false;
                loop {
                    match self.peek().clone() {
                        Token::When => {
                            if seen_else {
                                return Err(Error::InvalidArgument(
                                    "WHEN cannot follow ELSE in CASE expression".to_string(),
                                ));
                            }
                            self.advance(); // consume WHEN
                            let cond = self.parse_expr()?;
                            self.expect_tok(&Token::Then)?;
                            let val = self.parse_expr()?;
                            branches.push((cond, val));
                            seen_when = true;
                        }
                        Token::Else => {
                            if !seen_when {
                                return Err(Error::InvalidArgument(
                                    "ELSE requires at least one WHEN branch in CASE expression"
                                        .to_string(),
                                ));
                            }
                            if seen_else {
                                return Err(Error::InvalidArgument(
                                    "duplicate ELSE in CASE expression".to_string(),
                                ));
                            }
                            self.advance(); // consume ELSE
                            else_expr = Some(Box::new(self.parse_expr()?));
                            seen_else = true;
                        }
                        Token::End => {
                            if !seen_when {
                                return Err(Error::InvalidArgument(
                                    "CASE expression requires at least one WHEN branch".to_string(),
                                ));
                            }
                            self.advance(); // consume END
                            break;
                        }
                        other => {
                            return Err(Error::InvalidArgument(format!(
                                "expected WHEN, ELSE, or END in CASE expression, got {:?}",
                                other
                            )));
                        }
                    }
                }
                Ok(Expr::CaseWhen {
                    branches,
                    else_expr,
                })
            }
            // Unary minus: -expr (negates a numeric literal or sub-expression).
            Token::Dash => {
                self.advance();
                let inner = self.parse_atom()?;
                match inner {
                    Expr::Literal(Literal::Int(n)) => Ok(Expr::Literal(Literal::Int(-n))),
                    Expr::Literal(Literal::Float(f)) => Ok(Expr::Literal(Literal::Float(-f))),
                    // Wrap in a FnCall to negate: abs(0 - x) is wrong, use unary-minus fn.
                    // Instead, emit FnCall("_neg", [inner]) — handled by dispatch as negation.
                    // For now call a built-in no-op negation: use the integer math path.
                    other => Ok(Expr::FnCall {
                        name: "_neg".into(),
                        args: vec![other],
                    }),
                }
            }
            other => Err(Error::InvalidArgument(format!(
                "unexpected token in expression: {:?}",
                other
            ))),
        }
    }

    /// Parse `shortestPath((src)-[:REL*]->(dst))` — invoked from the Ident branch (SPA-136).
    fn parse_shortest_path_fn(&mut self) -> Result<Expr> {
        self.expect_tok(&Token::LParen)?;
        let path = self.parse_path_pattern()?;
        self.expect_tok(&Token::RParen)?;

        if path.nodes.len() != 2 || path.rels.len() != 1 {
            return Err(Error::InvalidArgument(
                "shortestPath() requires exactly one relationship pattern".into(),
            ));
        }
        let src_node = &path.nodes[0];
        let dst_node = &path.nodes[1];
        let rel = &path.rels[0];

        Ok(Expr::ShortestPath(Box::new(ShortestPathExpr {
            src_var: src_node.var.clone(),
            src_label: src_node.labels.first().cloned().unwrap_or_default(),
            src_props: src_node.props.clone(),
            dst_var: dst_node.var.clone(),
            dst_label: dst_node.labels.first().cloned().unwrap_or_default(),
            dst_props: dst_node.props.clone(),
            rel_type: rel.rel_type.clone(),
        })))
    }

    // ── RETURN items ──────────────────────────────────────────────────────────

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>> {
        if matches!(self.peek(), Token::Star) {
            self.advance();
            return Ok(vec![ReturnItem {
                expr: Expr::Var("*".into()),
                alias: None,
            }]);
        }

        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if matches!(self.peek(), Token::As) {
                self.advance();
                Some(self.expect_ident()?)
            } else {
                None
            };
            items.push(ReturnItem { expr, alias });
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(items)
    }

    // ── ORDER BY items ────────────────────────────────────────────────────────

    fn parse_order_by_items(&mut self) -> Result<Vec<(Expr, SortDir)>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let dir = match self.peek().clone() {
                Token::Desc => {
                    self.advance();
                    SortDir::Desc
                }
                Token::Asc => {
                    self.advance();
                    SortDir::Asc
                }
                _ => SortDir::Asc,
            };
            items.push((expr, dir));
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(items)
    }

    // ── CALL { } subquery (issue #290) ──────────────────────────────────────

    /// Parse a comma-separated list of import identifiers after `WITH` inside a
    /// subquery body.  The `WITH` keyword must already have been consumed.
    ///
    /// Returns the list of imported variable names.
    fn parse_subquery_import_list(&mut self) -> Result<Vec<String>> {
        let mut imports = Vec::new();
        loop {
            imports.push(self.expect_ident()?);
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(imports)
    }

    /// Entry point when we have already consumed `CALL` and see `{`.
    ///
    /// `imports` carries any variable names parsed from a preceding `WITH`
    /// clause (correlated form).  For unit subqueries `imports` is empty.
    fn parse_call_subquery(&mut self, imports: Vec<String>) -> Result<Statement> {
        self.expect_tok(&Token::LBrace)?;
        self.parse_call_subquery_body(imports)
    }

    /// Parse the body `[WITH imports] inner_stmt }` after the `{` has been consumed.
    ///
    /// Handles the optional `WITH var1, var2` import list at the start of the
    /// subquery body (correlated form).  After the closing `}`, also looks for
    /// an optional trailing `RETURN` clause for standalone usage.
    fn parse_call_subquery_body(&mut self, pre_imports: Vec<String>) -> Result<Statement> {
        // Parse optional WITH imports inside the brace (correlated form).
        let imports = if pre_imports.is_empty() && matches!(self.peek(), Token::With) {
            self.advance(); // consume WITH
            self.parse_subquery_import_list()?
        } else {
            pre_imports
        };

        let subquery = self.parse_statement()?;
        self.expect_tok(&Token::RBrace)?;

        // Optional trailing RETURN clause for standalone `CALL { } RETURN ...`.
        let (return_clause, return_distinct, return_order_by, return_skip, return_limit) =
            if matches!(self.peek(), Token::Return) {
                self.advance(); // consume RETURN
                let distinct = if matches!(self.peek(), Token::Distinct) {
                    self.advance();
                    true
                } else {
                    false
                };
                let items = self.parse_return_items()?;
                // Parse optional ORDER BY / SKIP / LIMIT modifiers.
                let order_by = if matches!(self.peek(), Token::Order) {
                    self.advance(); // consume ORDER
                    self.expect_tok(&Token::By)?;
                    self.parse_order_by_items()?
                } else {
                    vec![]
                };
                let skip = if matches!(self.peek(), Token::Skip) {
                    self.advance();
                    match self.advance().clone() {
                        Token::Integer(n) if n >= 0 => Some(n as u64),
                        _ => {
                            return Err(Error::InvalidArgument(
                                "SKIP expects a non-negative integer".into(),
                            ))
                        }
                    }
                } else {
                    None
                };
                let limit = if matches!(self.peek(), Token::Limit) {
                    self.advance();
                    match self.advance().clone() {
                        Token::Integer(n) if n > 0 => Some(n as u64),
                        _ => {
                            return Err(Error::InvalidArgument(
                                "LIMIT expects a positive integer".into(),
                            ))
                        }
                    }
                } else {
                    None
                };
                (
                    Some(ReturnClause { items }),
                    distinct,
                    order_by,
                    skip,
                    limit,
                )
            } else {
                (None, false, vec![], None, None)
            };

        Ok(Statement::CallSubquery {
            subquery: Box::new(subquery),
            imports,
            return_clause,
            return_order_by,
            return_skip,
            return_limit,
            return_distinct,
        })
    }

    /// Parse a `CALL { [WITH imports] inner_stmt }` as a `PipelineStage`.
    ///
    /// Called from `parse_pipeline_continuation` when a `CALL` token is seen.
    /// The leading `CALL` token must already have been consumed by the caller.
    fn parse_call_subquery_stage(&mut self) -> Result<PipelineStage> {
        // Check for correlated form: `CALL { WITH var1, var2 inner ... }`
        // The `CALL` has already been consumed; now we decide based on next token.
        //
        // Correlated: `CALL { WITH x MATCH ... }`
        // Unit:       `CALL { MATCH ... }`
        //
        // Both start with `{` here since `parse_pipeline_continuation` sees
        // the token *before* consuming CALL.
        self.expect_tok(&Token::LBrace)?;

        // Parse optional WITH imports inside the brace.
        let imports = if matches!(self.peek(), Token::With) {
            self.advance(); // consume WITH
            self.parse_subquery_import_list()?
        } else {
            vec![]
        };

        let subquery = self.parse_statement()?;
        self.expect_tok(&Token::RBrace)?;
        Ok(PipelineStage::CallSubquery {
            subquery: Box::new(subquery),
            imports,
        })
    }

    // ── CALL procedure(args) YIELD col [RETURN ...] ───────────────────────────

    /// Parse `CALL proc.name(args) YIELD col1, col2 [RETURN ...]`.
    ///
    /// The procedure name is a dotted identifier sequence, e.g.
    /// `db.index.fulltext.queryNodes`.  Arguments are a comma-separated list
    /// of expressions (string literals, parameters, etc.).  The `YIELD` clause
    /// names the columns the procedure produces; an optional `RETURN` clause
    /// projects them further.
    fn parse_call(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Call)?;

        // If the next token is `{`, this is a CALL { } subquery (issue #290),
        // not a procedure call.  The WITH import list (for correlated form) lives
        // *inside* the braces: `CALL { WITH n MATCH ... }`.
        if matches!(self.peek(), Token::LBrace) {
            return self.parse_call_subquery(vec![]);
        }

        // Parse dotted procedure name: ident (. ident)*
        // Use advance_as_prop_name for all segments so that keyword tokens like
        // `index` (Token::Index) are accepted within the dotted path
        // (e.g. `db.index.fulltext.queryNodes`).
        let mut proc_name = self.advance_as_prop_name()?;
        while matches!(self.peek(), Token::Dot) {
            self.advance(); // consume '.'
            let part = self.advance_as_prop_name()?;
            proc_name.push('.');
            proc_name.push_str(&part);
        }

        // Parse argument list: ( expr, expr, ... )
        self.expect_tok(&Token::LParen)?;
        let mut args = Vec::new();
        if !matches!(self.peek(), Token::RParen) {
            loop {
                args.push(self.parse_atom()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        self.expect_tok(&Token::RParen)?;

        // Parse YIELD col1, col2, ...
        let yield_columns = if matches!(self.peek(), Token::Yield) {
            self.advance(); // consume YIELD
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            cols
        } else {
            vec![]
        };

        // Optional trailing RETURN clause.
        let return_clause = if matches!(self.peek(), Token::Return) {
            self.advance(); // consume RETURN
            let distinct = if matches!(self.peek(), Token::Distinct) {
                self.advance();
                true
            } else {
                false
            };
            let _ = distinct; // not threaded through CallStatement yet — ignored
            let items = self.parse_return_items()?;
            Some(ReturnClause { items })
        } else {
            None
        };

        Ok(Statement::Call(CallStatement {
            procedure: proc_name,
            args,
            yield_columns,
            return_clause,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;

    #[test]
    fn parse_checkpoint_smoke() {
        assert!(matches!(parse("CHECKPOINT"), Ok(Statement::Checkpoint)));
    }

    #[test]
    fn parse_optimize_smoke() {
        assert!(matches!(parse("OPTIMIZE"), Ok(Statement::Optimize)));
    }

    #[test]
    fn parse_empty_fails() {
        assert!(parse("").is_err());
    }

    #[test]
    fn parse_optional_match_ok() {
        // OPTIONAL MATCH standalone is supported (SPA-131).
        let stmt = parse("OPTIONAL MATCH (n:Person) RETURN n.name").unwrap();
        assert!(matches!(stmt, Statement::OptionalMatch(_)));
    }

    #[test]
    fn parse_optional_match_missing_return_fails() {
        assert!(parse("OPTIONAL MATCH (n:Person)").is_err());
    }

    #[test]
    fn parse_create_node() {
        let stmt = parse("CREATE (n:Person {name: \"Alice\"})").unwrap();
        assert!(matches!(stmt, Statement::Create(_)));
    }

    #[test]
    fn parse_match_return() {
        let stmt = parse("MATCH (n:Person) RETURN n.name").unwrap();
        assert!(matches!(stmt, Statement::Match(_)));
    }
}
