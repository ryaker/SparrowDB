//! Factorized execution core types.
//!
//! See spec Section 13 for semantics.

use std::collections::HashMap;

use sparrowdb_common::{EdgeId, NodeId};

/// A typed column vector (one column of data in a group).
#[derive(Debug, Clone)]
pub enum TypedVector {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Bool(Vec<bool>),
    String(Vec<String>),
    NodeRef(Vec<NodeId>),
    EdgeRef(Vec<EdgeId>),
}

impl TypedVector {
    pub fn len(&self) -> usize {
        match self {
            TypedVector::Int64(v) => v.len(),
            TypedVector::Float64(v) => v.len(),
            TypedVector::Bool(v) => v.len(),
            TypedVector::String(v) => v.len(),
            TypedVector::NodeRef(v) => v.len(),
            TypedVector::EdgeRef(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get value at index as a `Value`.
    pub fn get(&self, idx: usize) -> Value {
        match self {
            TypedVector::Int64(v) => Value::Int64(v[idx]),
            TypedVector::Float64(v) => Value::Float64(v[idx]),
            TypedVector::Bool(v) => Value::Bool(v[idx]),
            TypedVector::String(v) => Value::String(v[idx].clone()),
            TypedVector::NodeRef(v) => Value::NodeRef(v[idx]),
            TypedVector::EdgeRef(v) => Value::EdgeRef(v[idx]),
        }
    }
}

/// A scalar value (materialized from TypedVector for output).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    NodeRef(NodeId),
    EdgeRef(EdgeId),
    /// A list of values, produced by `collect()` aggregation.
    List(Vec<Value>),
}

impl Value {
    /// Evaluate `CONTAINS` predicate.
    pub fn contains(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::String(s), Value::String(p)) => s.contains(p.as_str()),
            _ => false,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
            Value::NodeRef(n) => write!(f, "node({})", n.0),
            Value::EdgeRef(e) => write!(f, "edge({})", e.0),
            Value::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
        }
    }
}

/// A vector group: one row-set with named typed columns and a multiplicity.
///
/// In factorized execution, `multiplicity` represents the number of implicit
/// copies of this group without materializing them.
#[derive(Debug, Clone)]
pub struct VectorGroup {
    /// Named column vectors.  All vectors must have the same length.
    pub columns: HashMap<String, TypedVector>,
    /// Logical multiplicity — how many times this group is counted.
    pub multiplicity: u64,
}

impl VectorGroup {
    pub fn new(multiplicity: u64) -> Self {
        VectorGroup {
            columns: HashMap::new(),
            multiplicity,
        }
    }

    pub fn add_column(&mut self, name: String, vec: TypedVector) {
        self.columns.insert(name, vec);
    }

    /// Number of rows in this group (length of any column; 0 if empty).
    pub fn len(&self) -> usize {
        self.columns.values().next().map(|v| v.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    /// Get value at row index from column `name`.
    pub fn get_value(&self, col: &str, row: usize) -> Option<Value> {
        self.columns.get(col).and_then(|v| {
            if row < v.len() {
                Some(v.get(row))
            } else {
                None
            }
        })
    }

    /// Logical row count (len * multiplicity).
    pub fn logical_row_count(&self) -> u64 {
        self.len() as u64 * self.multiplicity
    }
}

/// A factorized chunk: a batch of vector groups.
#[derive(Debug, Clone)]
pub struct FactorizedChunk {
    pub groups: Vec<VectorGroup>,
}

impl FactorizedChunk {
    pub fn new() -> Self {
        FactorizedChunk { groups: Vec::new() }
    }

    pub fn push_group(&mut self, group: VectorGroup) {
        self.groups.push(group);
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Total logical row count across all groups.
    pub fn logical_row_count(&self) -> u64 {
        self.groups.iter().map(|g| g.logical_row_count()).sum()
    }
}

impl Default for FactorizedChunk {
    fn default() -> Self {
        Self::new()
    }
}

/// Final materialized query result.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Named column headers, in the same order as values within each row.
    ///
    /// For `RETURN` queries these are the projected aliases (or expression
    /// text when no alias is given).  For `CALL` procedures these are the
    /// output column names declared by the procedure (e.g. `["type", "name",
    /// "properties"]` for `CALL db.schema()`).
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    pub fn empty(columns: Vec<String>) -> Self {
        QueryResult {
            columns,
            rows: Vec::new(),
        }
    }

    /// Return row `idx` as a `HashMap<column_name, Value>`.
    ///
    /// Returns `None` if `idx` is out of bounds.  Column names come from
    /// `self.columns`; if the columns list is shorter than the row, extra
    /// values are dropped.  If the columns list is longer than the row,
    /// missing values are absent from the map (they are never `Null`-padded).
    pub fn row_as_map(&self, idx: usize) -> Option<HashMap<String, Value>> {
        let row = self.rows.get(idx)?;
        Some(
            self.columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_group_len_matches_column_len() {
        let mut g = VectorGroup::new(1);
        g.add_column("x".into(), TypedVector::Int64(vec![1, 2, 3]));
        assert_eq!(g.len(), 3);
    }

    #[test]
    fn factorized_chunk_logical_row_count() {
        let mut chunk = FactorizedChunk::new();
        let mut g1 = VectorGroup::new(2);
        g1.add_column("a".into(), TypedVector::Int64(vec![1, 2]));
        let mut g2 = VectorGroup::new(3);
        g2.add_column("a".into(), TypedVector::Int64(vec![10]));
        chunk.push_group(g1);
        chunk.push_group(g2);
        // g1: 2 rows * 2 = 4; g2: 1 row * 3 = 3; total = 7
        assert_eq!(chunk.logical_row_count(), 7);
    }
}
