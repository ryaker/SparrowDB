//! Recursive-descent Cypher parser.
//!
//! Entry point: [`parse`].  Returns `Err(...)` — never panics — for
//! unsupported syntax.

use sparrowdb_common::{Error, Result};

use crate::ast::{
    BinOpKind, CreateStatement, EdgeDir, ExistsPattern, Expr, Literal, MatchCreateStatement,
    MatchMutateStatement, MatchOptionalMatchStatement, MatchStatement, MergeStatement, Mutation,
    NodePattern, OptionalMatchStatement, PathPattern, PropEntry, RelPattern, ReturnClause,
    ReturnItem, SortDir, Statement, UnionStatement, UnwindStatement,
};
use crate::lexer::{tokenize, Token};

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
        let all = if matches!(p.peek(), Token::Ident(ref s) if s.to_uppercase() == "ALL") {
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
            Token::With => {
                // MATCH … WITH … RETURN pipeline
                self.parse_with_pipeline(patterns, None)
            }
            Token::Return | Token::Order | Token::Limit | Token::Eof | Token::Semicolon => {
                self.finish_match_return(patterns, None)
            }
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
        use crate::ast::{MatchWithStatement, WithClause, WithItem};

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

        let with_clause = WithClause { items, where_clause: with_where };

        // RETURN clause.
        self.expect_tok(&Token::Return)?;
        let distinct = if matches!(self.peek(), Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };
        let return_items = self.parse_return_items()?;
        let return_clause = crate::ast::ReturnClause { items: return_items };

        // ORDER BY
        let order_by = if matches!(self.peek(), Token::Order) {
            self.advance();
            self.expect_tok(&Token::By)?;
            self.parse_order_by_items()?
        } else {
            vec![]
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

        Ok(Statement::MatchWith(MatchWithStatement {
            match_patterns: patterns,
            match_where,
            with_clause,
            return_clause,
            order_by,
            limit,
            distinct,
        }))
    }

    // ── MERGE ─────────────────────────────────────────────────────────────────

    /// Parse `MERGE (:Label {prop: val, ...})`.
    ///
    /// Only single-node MERGE (no paths) is supported.
    fn parse_merge(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Merge)?;
        self.expect_tok(&Token::LParen)?;

        // Optional variable name (we discard it — MERGE doesn't bind variables).
        if let Token::Ident(_) = self.peek().clone() {
            if !matches!(self.peek2(), Token::Colon | Token::RParen) {
                // ambiguous — treat as anonymous
            } else if matches!(self.peek2(), Token::Colon) {
                self.advance(); // consume var name
            }
        }

        // Label(s) — at least one required for MERGE.
        if !matches!(self.peek(), Token::Colon) {
            return Err(Error::InvalidArgument(
                "MERGE requires a label (e.g. MERGE (:Person {...}))".into(),
            ));
        }
        self.advance(); // consume ':'
        let label = match self.advance().clone() {
            Token::Ident(s) => s,
            other => {
                return Err(Error::InvalidArgument(format!(
                    "expected label name after ':', got {:?}",
                    other
                )))
            }
        };

        // Property map (optional but typical for MERGE).
        let props = if matches!(self.peek(), Token::LBrace) {
            self.parse_prop_map()?
        } else {
            vec![]
        };

        self.expect_tok(&Token::RParen)?;
        Ok(Statement::Merge(MergeStatement { label, props }))
    }

    // ── CREATE ────────────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect_tok(&Token::Create)?;
        let body = self.parse_create_body()?;
        Ok(Statement::Create(body))
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
                let rel = self.parse_rel_pattern()?;
                let dst_node = self.parse_node_pattern()?;
                let dst_var = dst_node.var.clone();
                edges.push((node_var, rel, dst_var));
                // dst node may be referenced but not re-created
                if !dst_node.labels.is_empty() || !dst_node.props.is_empty() {
                    nodes.push(dst_node);
                }
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

        Ok(CreateStatement { nodes, edges })
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
            let label = match self.advance().clone() {
                Token::Ident(s) => s,
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected label name, got {:?}",
                        other
                    )))
                }
            };
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

        // [ :REL_TYPE ]
        self.expect_tok(&Token::LBracket)?;

        let var = match self.peek().clone() {
            Token::Ident(s) if matches!(self.peek2(), Token::Colon) => {
                self.advance();
                s
            }
            _ => String::new(),
        };

        // Parse optional colon before rel type (or error on bare star).
        if matches!(self.peek(), Token::Colon) {
            self.advance();
        } else if matches!(self.peek(), Token::Star) {
            return Err(Error::InvalidArgument(
                "variable-length paths require a relationship type: use [:R*] not [*]".into(),
            ));
        }

        let rel_type = match self.advance().clone() {
            Token::Ident(s) => s,
            other => {
                return Err(Error::InvalidArgument(format!(
                    "expected relationship type, got {:?}",
                    other
                )))
            }
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
            let key = match self.advance().clone() {
                Token::Ident(s) => s,
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "expected property key, got {:?}",
                        other
                    )))
                }
            };
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
        let left = self.parse_atom()?;

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
        let right = self.parse_atom()?;
        Ok(Expr::BinOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
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
                    let prop = match self.advance().clone() {
                        Token::Ident(s) => s,
                        other => {
                            return Err(Error::InvalidArgument(format!(
                                "expected property name, got {:?}",
                                other
                            )))
                        }
                    };
                    Ok(Expr::PropAccess { var, prop })
                } else if matches!(next2, Token::LParen) {
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
                self.expect_tok(&Token::Star)?;
                self.expect_tok(&Token::RParen)?;
                Ok(Expr::CountStar)
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
