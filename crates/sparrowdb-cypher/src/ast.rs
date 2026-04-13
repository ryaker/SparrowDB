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
    pub value: Expr,
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
    /// Minimum hops for variable-length paths (None = exactly 1 hop, i.e. not variable-length).
    pub min_hops: Option<u32>,
    /// Maximum hops for variable-length paths (None = unbounded, capped at 10 internally).
    /// Only meaningful when `min_hops` is `Some`.
    pub max_hops: Option<u32>,
    /// Inline property predicates / values on the relationship (SPA-178).
    ///
    /// In a MATCH context these act as filters; in a CREATE context they set
    /// the initial property values on the new edge.
    pub props: Vec<PropEntry>,
}

/// A path pattern: a sequence of alternating nodes and relationships.
#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    pub nodes: Vec<NodePattern>,
    pub rels: Vec<RelPattern>,
}

/// Variants for list predicate expressions: ANY, ALL, NONE, SINGLE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPredicateKind {
    Any,
    All,
    None,
    Single,
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
    /// `expr IN [val, val, ...]` — membership test.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `ANY(x IN list_expr WHERE predicate)` — list predicate.
    ListPredicate {
        kind: ListPredicateKind,
        variable: String,
        list_expr: Box<Expr>,
        predicate: Box<Expr>,
    },
    /// `expr IS NULL`
    IsNull(Box<Expr>),
    /// `expr IS NOT NULL`
    IsNotNull(Box<Expr>),
    /// `EXISTS { (n)-[:R]->(:Label) }` — positive existence subquery (SPA-137).
    ExistsSubquery(Box<ExistsPattern>),
    /// `CASE WHEN cond THEN val ... [ELSE val] END` — conditional expression (SPA-138).
    CaseWhen {
        /// List of (condition, value) branches.
        branches: Vec<(Expr, Expr)>,
        /// Optional ELSE value (None = ELSE NULL).
        else_expr: Option<Box<Expr>>,
    },
    /// `shortestPath((a)-[:R*]->(b))` — shortest-path scalar (SPA-136).
    ShortestPath(Box<ShortestPathExpr>),
}

/// An existence pattern used in `NOT (a)-[:R]->(b)` or `EXISTS { pattern }`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistsPattern {
    pub path: PathPattern,
}

/// Shortest-path expression: `shortestPath((src:Label {props})-[:REL*]->(dst:Label {props}))`.
///
/// Returned as `Value::Int64(hop_count)`, or `Value::Null` when no path exists.
#[derive(Debug, Clone, PartialEq)]
pub struct ShortestPathExpr {
    pub src_var: String,
    pub src_label: String,
    pub src_props: Vec<PropEntry>,
    pub dst_var: String,
    pub dst_label: String,
    pub dst_props: Vec<PropEntry>,
    pub rel_type: String,
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
    /// Arithmetic: `a + b`
    Add,
    /// Arithmetic: `a - b`
    Sub,
    /// Arithmetic: `a * b`
    Mul,
    /// Arithmetic: `a / b`
    Div,
    /// Arithmetic: `a % b`
    Mod,
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
    /// Optional RETURN clause (issue #366).
    pub return_clause: Option<ReturnClause>,
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
    pub skip: Option<u64>,
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
    /// Variable name bound to the merged node (may be empty for anonymous MERGE).
    pub var: String,
    /// The primary label to merge on.
    pub label: String,
    /// Identity properties used to locate or create the node.
    pub props: Vec<PropEntry>,
    /// Optional RETURN clause projecting properties of the merged node.
    pub return_clause: Option<ReturnClause>,
    /// SET items applied only when the node is freshly created.
    pub on_create_set: Vec<Mutation>,
    /// SET items applied only when an existing node is matched.
    pub on_match_set: Vec<Mutation>,
}

/// MATCH … MERGE (a)-[r:TYPE]->(b) statement: find-or-create a relationship.
///
/// The MATCH clause binds node variables; the MERGE clause guarantees that
/// exactly one relationship of the given type exists between those nodes.
///
/// `MATCH (a:Label {prop: val}), (b:Label {prop: val}) MERGE (a)-[r:TYPE]->(b)`
#[derive(Debug, Clone, PartialEq)]
pub struct MatchMergeRelStatement {
    /// MATCH patterns used to bind the endpoint node variables.
    pub match_patterns: Vec<PathPattern>,
    /// Optional WHERE clause on the MATCH.
    pub where_clause: Option<Expr>,
    /// Variable bound to the left (source) node in the MERGE pattern.
    pub src_var: String,
    /// Variable bound to the relationship (may be empty for anonymous).
    pub rel_var: String,
    /// Relationship type to merge on.
    pub rel_type: String,
    /// Variable bound to the right (destination) node in the MERGE pattern.
    pub dst_var: String,
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
    /// `DELETE var` or `DETACH DELETE var`
    Delete {
        var: String,
        /// When `true`, all incident edges are removed before the node is
        /// deleted (equivalent to Cypher's `DETACH DELETE`).
        detach: bool,
    },
}

/// MATCH … SET/DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchMutateStatement {
    /// The MATCH patterns (same structure as `MatchStatement::pattern`).
    pub match_patterns: Vec<PathPattern>,
    /// Optional WHERE predicate.
    pub where_clause: Option<Expr>,
    /// The mutations to apply to matched nodes.
    ///
    /// For SET, there may be multiple items (comma-separated).
    /// For DELETE, this always contains exactly one `Mutation::Delete`.
    pub mutations: Vec<Mutation>,
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
    pub skip: Option<u64>,
    pub limit: Option<u64>,
    pub distinct: bool,
}

/// A single stage in a multi-clause pipeline (SPA-134).
///
/// Pipelines are sequences of clauses that pass intermediate row sets from
/// one stage to the next.  Each stage receives the projected rows from its
/// predecessor and either transforms them or re-traverses the graph.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    /// `WITH expr AS alias [, ...] [WHERE pred]` — project + optionally filter rows.
    /// Also carries optional ORDER BY / SKIP / LIMIT that are applied to the WITH output.
    With {
        clause: WithClause,
        order_by: Vec<(Expr, SortDir)>,
        skip: Option<u64>,
        limit: Option<u64>,
    },
    /// `MATCH pattern [WHERE pred]` — re-traverse the graph, constraining by the
    /// variables that were projected in the preceding WITH stage.
    Match {
        patterns: Vec<PathPattern>,
        where_clause: Option<Expr>,
    },
    /// `UNWIND alias_expr AS new_alias` — unwind a list variable from the preceding
    /// WITH into individual rows.
    Unwind { alias: String, new_alias: String },
    /// `CALL { [WITH var1, var2] inner_stmt }` — inline subquery stage (issue #290).
    ///
    /// For each outer row the `subquery` is executed (optionally seeded with the
    /// imported variables) and the subquery's output columns are appended to the
    /// outer row, producing a cross product / nested-loop join.
    CallSubquery {
        subquery: Box<Statement>,
        imports: Vec<String>,
    },
}

/// A multi-clause Cypher pipeline (SPA-134).
///
/// Covers patterns such as:
/// - `MATCH … WITH … MATCH … RETURN`
/// - `UNWIND … WITH … RETURN`
/// - `MATCH … WITH … ORDER BY … LIMIT … MATCH … RETURN`
/// - Three-stage `MATCH … WITH … MATCH … WITH … RETURN`
///
/// Execution is left-to-right: the first stage produces an initial row set
/// (from the leading MATCH or UNWIND), then each subsequent stage transforms
/// or re-traverses until the final RETURN clause produces the output.
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineStatement {
    /// Leading MATCH clause (if any). May be absent for UNWIND-led pipelines.
    pub leading_match: Option<Vec<PathPattern>>,
    /// Optional WHERE on the leading MATCH.
    pub leading_where: Option<Expr>,
    /// For UNWIND-led pipelines: the list expression and binding alias.
    pub leading_unwind: Option<(Expr, String)>,
    /// Ordered sequence of intermediate pipeline stages.
    pub stages: Vec<PipelineStage>,
    /// Final RETURN clause.
    pub return_clause: ReturnClause,
    /// Optional ORDER BY on the final RETURN.
    pub return_order_by: Vec<(Expr, SortDir)>,
    /// Optional SKIP on the final RETURN.
    pub return_skip: Option<u64>,
    /// Optional LIMIT on the final RETURN.
    pub return_limit: Option<u64>,
    /// Whether RETURN DISTINCT was requested.
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
    pub skip: Option<u64>,
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
    pub skip: Option<u64>,
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

/// A CALL procedure statement.
///
/// ```cypher
/// CALL db.index.fulltext.queryNodes('myIndex', $query) YIELD node
/// RETURN node
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CallStatement {
    /// Dotted procedure name, e.g. `"db.index.fulltext.queryNodes"`.
    pub procedure: String,
    /// Positional arguments.
    pub args: Vec<Expr>,
    /// Columns declared in `YIELD col1, col2`.
    pub yield_columns: Vec<String>,
    /// Optional trailing `RETURN` clause.
    pub return_clause: Option<ReturnClause>,
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
    /// MATCH … MERGE (a)-[r:TYPE]->(b) — find-or-create a relationship (SPA-233).
    MatchMergeRel(MatchMergeRelStatement),
    MatchMutate(MatchMutateStatement),
    OptionalMatch(OptionalMatchStatement),
    MatchOptionalMatch(MatchOptionalMatchStatement),
    Union(UnionStatement),
    Checkpoint,
    Optimize,
    /// CALL procedure dispatch (SPA-170 — KMS full-text search).
    Call(CallStatement),
    /// Multi-clause pipeline: MATCH/UNWIND … WITH … MATCH … WITH … RETURN (SPA-134).
    Pipeline(PipelineStatement),
    /// `CREATE INDEX ON :Label(property)` — build a property index (SPA-235).
    CreateIndex {
        label: String,
        property: String,
    },
    /// `CREATE CONSTRAINT ON (n:Label) ASSERT n.property IS UNIQUE` (SPA-234).
    CreateConstraint {
        label: String,
        property: String,
    },
    /// `CALL { subquery } [RETURN …]` — standalone unit subquery (issue #290).
    ///
    /// A unit subquery (no `imports`) is executed independently once and its
    /// output rows are extended with the optional trailing `RETURN` clause.
    ///
    /// When embedded inside a pipeline (as a `PipelineStage::CallSubquery`),
    /// this variant is **not** used — use `PipelineStage::CallSubquery` instead.
    CallSubquery {
        /// The statement inside `{ … }`.
        subquery: Box<Statement>,
        /// Variables imported from the outer scope via `CALL { WITH x, y … }`.
        imports: Vec<String>,
        /// Optional trailing `RETURN` clause projecting subquery columns.
        return_clause: Option<ReturnClause>,
        /// Optional `ORDER BY` on the outer RETURN (may be empty).
        return_order_by: Vec<(Expr, SortDir)>,
        /// Optional `SKIP` on the outer RETURN.
        return_skip: Option<u64>,
        /// Optional `LIMIT` on the outer RETURN.
        return_limit: Option<u64>,
        /// Whether the outer RETURN is `RETURN DISTINCT …`.
        return_distinct: bool,
    },
    /// `CREATE FULLTEXT INDEX [name] FOR (n:Label) ON (n.property)` — BM25 FTS index (issue #395).
    CreateFulltextIndex {
        /// Optional user-supplied index name (currently stored but not used for lookup).
        name: Option<String>,
        label: String,
        property: String,
    },
    /// `CREATE VECTOR INDEX [name] FOR (n:Label) ON (n.prop) OPTIONS { dimensions: N, similarity: 'cosine' }` (issue #394).
    CreateVectorIndex {
        /// Optional index name (ignored at runtime but parsed for compatibility).
        name: Option<String>,
        label: String,
        prop: String,
        dimensions: usize,
        similarity: String,
    },
    /// `DROP INDEX <name>` — remove a named property or vector index.
    DropIndex {
        name: String,
    },
}
