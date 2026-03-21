//! Cypher AST node types.
//!
//! Covers the minimum subset needed for UC-1 (social graph) and UC-3 (KMS)
//! queries as specified in docs/use-cases.md.

/// A parsed Cypher literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Param(String), // $param
    Null,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// A property map entry in a pattern: `{key: value}`.
#[derive(Debug, Clone, PartialEq)]
pub struct PropEntry {
    pub key: String,
    pub value: Literal,
}

/// A node pattern: `(var:Label {props})`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    /// Variable name (may be empty string for anonymous nodes).
    pub var: String,
    /// Labels declared on this node pattern (first = primary).
    pub labels: Vec<String>,
    /// Inline property predicates.
    pub props: Vec<PropEntry>,
}

/// Edge direction in a pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeDir {
    /// `(a)-[:R]->(b)` — outgoing from left node.
    Outgoing,
    /// `(a)<-[:R]-(b)` — incoming to left node.
    Incoming,
    /// `(a)-[:R]-(b)` — undirected.
    Both,
}

/// A relationship pattern: `-[:REL_TYPE]->`.
#[derive(Debug, Clone, PartialEq)]
pub struct RelPattern {
    /// Variable name (may be empty).
    pub var: String,
    /// Relationship type name.
    pub rel_type: String,
    /// Direction from the perspective of the path order.
    pub dir: EdgeDir,
}

/// A path pattern: a sequence of alternating nodes and relationships.
#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    pub nodes: Vec<NodePattern>,
    pub rels: Vec<RelPattern>,
}

/// An expression used in WHERE, RETURN, ORDER BY.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `variable.property`
    PropAccess { var: String, prop: String },
    /// Literal value.
    Literal(Literal),
    /// Binary comparison.
    BinOp {
        left: Box<Expr>,
        op: BinOpKind,
        right: Box<Expr>,
    },
    /// `NOT (a)-[:R]->(b)` — existence predicate negation.
    NotExists(Box<ExistsPattern>),
    /// `NOT expr`
    Not(Box<Expr>),
    /// `expr AND expr`
    And(Box<Expr>, Box<Expr>),
    /// `expr OR expr`
    Or(Box<Expr>, Box<Expr>),
    /// A variable reference (e.g. `RETURN k` without `.property`).
    Var(String),
    /// `COUNT(*)`
    CountStar,
    /// Function call (for aggregate stubs).
    FnCall { name: String, args: Vec<Expr> },
    /// A list literal: `[expr, expr, ...]`.
    List(Vec<Expr>),
}

/// An existence pattern used in `NOT (a)-[:R]->(b)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistsPattern {
    pub path: PathPattern,
}

/// Binary operator kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinOpKind {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    Contains,
    StartsWith,
    EndsWith,
    Or,
    And,
}

/// RETURN clause item.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    /// Optional `AS alias`.
    pub alias: Option<String>,
}

/// RETURN clause.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

/// CREATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateStatement {
    /// Nodes to create.
    pub nodes: Vec<NodePattern>,
    /// Edges to create (each is (left_var, rel, right_var)).
    pub edges: Vec<(String, RelPattern, String)>,
}

/// MATCH+CREATE statement (MATCH ... CREATE edge).
#[derive(Debug, Clone, PartialEq)]
pub struct MatchCreateStatement {
    pub match_patterns: Vec<PathPattern>,
    pub match_props: Vec<(String, Vec<PropEntry>)>, // (var, props) for filter
    pub create: CreateStatement,
}

/// MATCH statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchStatement {
    pub pattern: Vec<PathPattern>,
    pub where_clause: Option<Expr>,
    pub return_clause: ReturnClause,
    pub order_by: Vec<(Expr, SortDir)>,
    pub limit: Option<u64>,
    pub distinct: bool,
}

/// UNWIND statement: iterates a list expression, binding each element to an alias.
///
/// ```cypher
/// UNWIND [1, 2, 3] AS x RETURN x
/// UNWIND $items   AS item RETURN item
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UnwindStatement {
    /// The list expression to iterate.
    pub expr: Expr,
    /// Variable bound to each element.
    pub alias: String,
    /// The RETURN clause following UNWIND.
    pub return_clause: ReturnClause,
}

/// MERGE statement: find-or-create a single node.
///
/// `MERGE (:Label {prop: val, ...})`
#[derive(Debug, Clone, PartialEq)]
pub struct MergeStatement {
    /// The primary label to merge on.
    pub label: String,
    /// Identity properties used to locate or create the node.
    pub props: Vec<PropEntry>,
}

/// A mutation clause appended after a MATCH: SET or DELETE.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    /// `SET var.prop = expr`
    Set {
        var: String,
        prop: String,
        value: Expr,
    },
    /// `DELETE var`
    Delete { var: String },
}

/// MATCH … SET/DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchMutateStatement {
    /// The MATCH patterns (same structure as `MatchStatement::pattern`).
    pub match_patterns: Vec<PathPattern>,
    /// Optional WHERE predicate.
    pub where_clause: Option<Expr>,
    /// The mutation to apply to matched nodes.
    pub mutation: Mutation,
}

/// A single projection in a WITH clause: `expr AS alias`.
#[derive(Debug, Clone, PartialEq)]
pub struct WithItem {
    pub expr: Expr,
    pub alias: String,
}

/// WITH clause: materializes intermediate rows and optionally filters them.
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub items: Vec<WithItem>,
    pub where_clause: Option<Expr>,
}

/// MATCH … WITH … RETURN statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchWithStatement {
    pub match_patterns: Vec<PathPattern>,
    pub match_where: Option<Expr>,
    pub with_clause: WithClause,
    pub return_clause: ReturnClause,
    pub order_by: Vec<(Expr, SortDir)>,
    pub limit: Option<u64>,
    pub distinct: bool,
}

/// OPTIONAL MATCH statement (standalone).
///
/// Left-outer-join semantics: if no rows match (label missing or zero nodes),
/// returns exactly one row with NULL values for all RETURN columns.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionalMatchStatement {
    pub pattern: Vec<PathPattern>,
    pub where_clause: Option<Expr>,
    pub return_clause: ReturnClause,
    pub order_by: Vec<(Expr, SortDir)>,
    pub limit: Option<u64>,
    pub distinct: bool,
}

/// MATCH … OPTIONAL MATCH … RETURN statement.
///
/// For every row produced by the leading MATCH, attempt the OPTIONAL MATCH
/// sub-pattern.  If no sub-rows are found for a given leading row, emit that
/// leading row with NULL values for the OPTIONAL MATCH variables.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchOptionalMatchStatement {
    /// The leading MATCH patterns (must produce rows).
    pub match_patterns: Vec<PathPattern>,
    pub match_where: Option<Expr>,
    /// The OPTIONAL MATCH patterns (may produce NULLs).
    pub optional_patterns: Vec<PathPattern>,
    pub optional_where: Option<Expr>,
    /// Combined RETURN clause evaluated over both MATCH and OPTIONAL MATCH variables.
    pub return_clause: ReturnClause,
    pub order_by: Vec<(Expr, SortDir)>,
    pub limit: Option<u64>,
    pub distinct: bool,
}

/// UNION / UNION ALL — combine two complete queries.
///
/// When `all` is `false` duplicate rows are eliminated (UNION); when `true`
/// all rows from both sides are returned (UNION ALL).
#[derive(Debug, Clone, PartialEq)]
pub struct UnionStatement {
    pub left: Box<Statement>,
    pub right: Box<Statement>,
    pub all: bool,
}

/// Top-level statement variants.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Create(CreateStatement),
    MatchCreate(MatchCreateStatement),
    Match(MatchStatement),
    MatchWith(MatchWithStatement),
    Unwind(UnwindStatement),
    Merge(MergeStatement),
    MatchMutate(MatchMutateStatement),
    OptionalMatch(OptionalMatchStatement),
    MatchOptionalMatch(MatchOptionalMatchStatement),
    Union(UnionStatement),
    Checkpoint,
    Optimize,
}
