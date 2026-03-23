use crate::ast::{
    BinOpKind, BinaryOp, ClosingClause, CallStatement, CreateStatement, EdgeDir, ExistsPattern, Expression, Expr, ListPredicateKind, Literal, MatchCreateStatement, MatchMergeRelStatement, MatchMutateStatement,
    MatchOptionalMatchStatement, MatchStatement, MergeStatement, Mutation, NodePattern, OptionalMatchStatement, PathPattern, PipelineStage, PipelineStatement, PropEntry, RelPattern, ReturnClause, ReturnItem, ShortestPathExpr, SortDir, Statement, UnionStatement, UnwindStatement, WithClause, WithItem,
};
use crate::lexer::{tokenize, Token};
use crate::lexer::{tokenize, Token};

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

// ── Parser cursor ─────────────────────────────────────────────────────────────

struct Parser {
    tokens: Vec<Token>,
    pos: usize,

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
                // MATCH ... SET var.prop = expr
                self.advance();
                let var = self.expect_ident()?;
                self.expect_tok(&Token::Dot)?;
                let prop = self.expect_ident()?;
                self.expect_tok(&Token::Eq)?;
                let value = self.parse_expr()?;
                Ok(Statement::MatchMutate(MatchMutateStatement {
                    match_patterns: patterns,
                    where_clause: None,
                    mutation: Mutation::Set { var, prop, value },
                }))
            }
            Token::Delete => {
                // MATCH ... DELETE var
                self.advance();
                let var = self.expect_ident()?;
                Ok(Statement::MatchMutate(MatchMutateStatement {
                    match_patterns: patterns,
                    where_clause: None,
                    mutation: Mutation::Delete { var },
                }))
            }
            Token::Where => {
                // MATCH ... WHERE expr (SET|DELETE|RETURN)
                self.advance();
                let where_expr = self.parse_expr()?;
                match self.peek().clone() {
                    Token::Set => {
                        self.advance();
                        let var = self.expect_ident()?;
                        self.expect_tok(&Token::Dot)?;
                        let prop = self.expect_ident()?;
                        self.expect_tok(&Token::Eq)?;
                        let value = self.parse_expr()?;
                        Ok(Statement::MatchMutate(MatchMutateStatement {
                            match_patterns: patterns,
                            where_clause: Some(where_expr),
                            mutation: Mutation::Set { var, prop, value },
                        }))
                    }
                    Token::Delete => {
                        self.advance();
                        let var = self.expect_ident()?;
                        Ok(Statement::MatchMutate(MatchMutateStatement {
                            match_patterns: patterns,
                            where_clause: Some(where_expr),
                            mutation: Mutation::Delete { var },
                        }))
                    }
                    Token::With => {
                        // MATCH … WHERE … WITH … RETURN
                        self.parse_with_pipeline(patterns, Some(where_expr))
                    }
                    _ => {
                        // Fall through to RETURN parsing with the parsed WHERE expr.
                        self.finish_match_return(patterns, Some(where_expr))
                    }
                }
            }
            Token::Merge => {
                // MATCH … MERGE (a)-[r:TYPE]->(b) — find-or-create relationship (SPA-233)
                self.parse_match_merge_rel_tail(patterns, None)
            }
            Token::With => {
                // MATCH … WITH … RETURN pipeline
                self.parse_with_pipeline(patterns, None)
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
            other => Err(Error::InvalidArgument(format!(
                "unexpected token after MATCH pattern: {:?}",
                other
            ))),
        }
    }

    /// Parse the `MERGE (a)-[r:TYPE]->(b)` tail after a MATCH clause.
    ///
    /// Syntax: `MERGE (src_var)-[rel_var:REL_TYPE]->(dst_var)`
    ///
    /// The `ON CREATE SET …` clause is parsed and silently discarded
    /// (relationship properties are not yet stored; SPA-233 scope).
    fn parse_match_merge_rel_tail(
        &mut self,
        match_patterns: Vec<PathPattern>,
        where_clause: Option<Expr>,
    ) -> Result<Statement> {
        self.expect_tok(&Token::Merge)?;

        // Parse `(src_var ...)` — the source node pattern.
        // We reuse parse_node_pattern which handles labels, props, and closing paren.
        let src_node = self.parse_node_pattern()?;
        let src_var = src_node.var;

        // Expect `-[...]->` or `<-[...]-` relationship pattern.
        // parse_rel_pattern itself consumes the leading `-` or `<-`.
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

        // Parse `(dst_var ...)` — the destination node pattern.
        let dst_node = self.parse_node_pattern()?;
        let dst_var = dst_node.var;

        // Optional `ON CREATE SET …` / `ON MATCH SET …` clauses — parse and discard
        // (relationship properties not stored in SPA-233 scope).
        // `ON` is not a reserved keyword, so it arrives as Token::Ident("ON").
        while matches!(self.peek(), Token::Ident(ref s) if s.eq_ignore_ascii_case("on")) {
            self.advance(); // consume ON
            self.skip_on_clause()?;
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

    /// Skip an `ON CREATE SET …` or `ON MATCH SET …` clause after a MERGE.
    ///
    /// The token stream is positioned right after the `ON` identifier.
    /// We consume tokens until we reach a top-level boundary: EOF, `;`, or
    /// a new top-level clause keyword (RETURN, WITH, another MERGE, or a new
    /// MATCH that is NOT preceded by CREATE/MATCH).
    ///
    /// Implementation: consume until we see `ON` (the next ON…SET clause)
    /// or a true top-level keyword.  The SET clause itself contains arbitrary
    /// expressions, so we skip all tokens until the next boundary.
    fn skip_on_clause(&mut self) -> Result<()> {
        // First skip the CREATE or MATCH keyword that follows ON.
        match self.peek().clone() {
            Token::Create | Token::Match => {
                self.advance();
            }
            other => {
                return Err(Error::InvalidArgument(format!(
                    "expected CREATE or MATCH after ON, got {:?}",
                    other
                )));
            }
        }
        // Now consume the rest of the `SET var.prop = expr` (or whatever follows)
        // until we hit a top-level boundary.
        loop {
            match self.peek().clone() {
                Token::Eof | Token::Semicolon | Token::Return | Token::With | Token::Merge => break,
                // Another `ON …` clause — stop here and let the outer loop handle it.
                Token::Ident(ref s) if s.eq_ignore_ascii_case("on") => break,
                _ => {
                    self.advance();
                }
            }
        }
        Ok(())
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

        // Parse one or more `expr AS alias` items separated by commas.
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
            // Continuation clause: MATCH, WITH, or UNWIND → build Pipeline.
            Token::Match | Token::With | Token::Unwind => {
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
                "expected RETURN, MATCH, WITH, or UNWIND after WITH clause, got {:?}",
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
